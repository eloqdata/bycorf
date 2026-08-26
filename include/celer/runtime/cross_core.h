/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CELER_RUNTIME_CROSS_CORE_H_
#define CELER_RUNTIME_CROSS_CORE_H_

#include <array>
#include <atomic>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

// <linux/fs.h> (pulled in transitively by liburing) defines BLOCK_SIZE as a
// macro, which collides with moodycamel's BLOCK_SIZE identifier. We don't use
// the filesystem constant, so drop it before including the queue.
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include "absl/status/status.h"
#include "celer/runtime/concurrentqueue.h"
#include "celer/runtime/task.h"

#ifndef CELER_ENABLE_SUBMIT_TASK_COUNT
#define CELER_ENABLE_SUBMIT_TASK_COUNT 0
#endif
#ifndef CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
#define CELER_ENABLE_CROSS_CORE_LATENCY_TRACE 0
#endif

namespace celer {

class Worker;
using WorkerId = std::uint16_t;

// A unit of cross-core work. It lives inside the awaiter, which lives in the
// caller's coroutine frame — so there is NO separate heap allocation. Only the
// pointer travels: origin -> target (request) -> origin (reply).
//
// Two separate queues (request / reply) mean the queue an item came from
// already tells us what to do with it — no phase tag, no dispatch function
// pointer. run_fn is the one unavoidable indirect call: it invokes the
// type-erased user closure, and only on the request leg.
struct RemoteWork {
  WorkerId origin_ = 0;
  std::coroutine_handle<> waiter_{};
  TaskClass task_class_ = TaskClass::kForeground;
  void (*run_fn_)(RemoteWork*) = nullptr;  // runs the user fn, stores result
  bool reply_deferred_ = false;
#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
  std::uint64_t request_post_ns_ = 0;
  std::uint64_t reply_post_ns_ = 0;
#endif
};

// Small one-way control message. It is copied into the target mailbox, so the
// sender does not have to keep an awaiter or heap allocation alive. Intended
// for ownership hand-backs such as returning a registered buffer to its owner.
struct RemoteNotification {
  void* context_ = nullptr;
  std::uint64_t value_ = 0;
  void (*run_fn_)(void*, std::uint64_t) noexcept = nullptr;
};

// wake_seq value meaning "the owner is parked": the owner CASes wake_seq to
// this before blocking; a producer bumps wake_seq after enqueuing and wakes the
// owner only if it reads this back (so a parked owner is woken exactly once per
// round).
inline constexpr std::uint32_t kWakeSeqParked = 1u << 31;

template <typename T, std::size_t Capacity = 256>
class alignas(64) SpscRing {
 public:
  static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0,
                "SPSC capacity must be a power of two");

  bool try_enqueue(T value) noexcept {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);
    const std::size_t next = (tail + 1) & (Capacity - 1);
    // head_ is written by the consumer. Avoid reading that remote cache line
    // until the producer's conservative snapshot says the ring may be full.
    if (next == cached_head_) {
      cached_head_ = head_.load(std::memory_order_acquire);
      if (next == cached_head_) {
        return false;
      }
    }
    entries_[tail] = std::move(value);
    tail_.store(next, std::memory_order_release);
    return true;
  }

  std::size_t try_dequeue_bulk(T* output, std::size_t maximum) noexcept {
    std::size_t head = head_.load(std::memory_order_relaxed);
    const std::size_t tail = tail_.load(std::memory_order_acquire);
    std::size_t count = 0;
    while (head != tail && count < maximum) {
      output[count++] = std::move(entries_[head]);
      head = (head + 1) & (Capacity - 1);
    }
    if (count != 0) {
      head_.store(head, std::memory_order_release);
    }
    return count;
  }

  bool empty() const noexcept {
    return head_.load(std::memory_order_relaxed) ==
           tail_.load(std::memory_order_acquire);
  }

 private:
  alignas(64) std::atomic<std::size_t> head_{0};
  alignas(64) std::atomic<std::size_t> tail_{0};
  std::size_t cached_head_ = 0;  // producer-owned snapshot of head_
  alignas(64) std::array<T, Capacity> entries_{};
};

// One SPSC lane for a fixed sender -> receiver pair.
struct alignas(64) CrossCoreLane {
  SpscRing<RemoteWork*> requests_;
  SpscRing<RemoteWork*> replies_;
  SpscRing<RemoteNotification> notifications_;
  // True while the receiver owns or is scheduled to drain this lane. A
  // producer that changes false -> true publishes its sender id in the
  // receiver's active-sender bitmap.
  std::atomic<bool> active_{false};

  bool empty() const noexcept {
    return requests_.empty() && replies_.empty() && notifications_.empty();
  }
};

// Keep each receiver bitmap word on its own cache line. Producers targeting
// different receivers (or different 64-sender groups) must not false-share.
struct alignas(64) ActiveSenderWord {
  std::atomic<std::uint64_t> bits_{0};
};

// One mailbox per worker, cache-line aligned so neighbours in the contiguous
// array never false-share their hot fields. Both queues are MPSC: every other
// worker may push, only the owning worker drains. ring_fd / wake_fd are how to
// wake it: MSG_RING to the io_uring ring, eventfd write as the legacy fallback.
struct alignas(64) WorkerMailbox {
  // Rare overflow path for a full bounded SPSC lane.
  moodycamel::ConcurrentQueue<RemoteWork*> requests_;  // others -> me: run here
  moodycamel::ConcurrentQueue<RemoteWork*>
      replies_;  // results coming back to me
  moodycamel::ConcurrentQueue<RemoteNotification> notifications_;
  std::atomic<bool> overflow_pending_{false};
  int ring_fd_ = -1;
  int wake_fd_ = -1;
  std::atomic<std::uint32_t> wake_seq_{0};
};

// All workers' mailboxes — a contiguous, fixed-size array (one indirection per
// access, no per-element pointer). Created by the Runtime BEFORE any worker
// thread starts, so cross-core posts never hit a missing mailbox.
class CrossCore {
 public:
  CrossCore() = default;  // empty until the Runtime knows the worker count
  explicit CrossCore(unsigned n)
      : size_(n),
        active_sender_word_count_((n + 63U) / 64U),
        mailboxes_(std::make_unique<WorkerMailbox[]>(n)),
        lanes_(
            std::make_unique<CrossCoreLane[]>(static_cast<std::size_t>(n) * n)),
        active_senders_(std::make_unique<ActiveSenderWord[]>(
            static_cast<std::size_t>(n) * active_sender_word_count_)) {}

  unsigned size() const noexcept { return size_; }
  unsigned active_sender_word_count() const noexcept {
    return active_sender_word_count_;
  }
  WorkerMailbox& mailbox(unsigned i) noexcept { return mailboxes_[i]; }
  CrossCoreLane& lane(unsigned receiver, unsigned sender) noexcept {
    return lanes_[static_cast<std::size_t>(receiver) * size_ + sender];
  }
  void ActivateSender(unsigned receiver, unsigned sender) noexcept {
    ActiveSenderWord& word =
        active_senders_[static_cast<std::size_t>(receiver) *
                            active_sender_word_count_ +
                        sender / 64U];
    word.bits_.fetch_or(std::uint64_t{1} << (sender % 64U),
                        std::memory_order_release);
  }
  std::uint64_t TakeActiveSenders(unsigned receiver, unsigned word) noexcept {
    auto& bits = active_senders_[static_cast<std::size_t>(receiver) *
                                     active_sender_word_count_ +
                                 word]
                     .bits_;
    // Busy polling calls this repeatedly. Keep the empty path read-only; a
    // stale zero only postpones a concurrently published bit to the next
    // drain round. The nonempty exchange acquires the producer's publication.
    if (bits.load(std::memory_order_relaxed) == 0) {
      return 0;
    }
    return bits.exchange(0, std::memory_order_acquire);
  }

 private:
  unsigned size_ = 0;
  unsigned active_sender_word_count_ = 0;
  std::unique_ptr<WorkerMailbox[]> mailboxes_;
  std::unique_ptr<CrossCoreLane[]> lanes_;
  std::unique_ptr<ActiveSenderWord[]> active_senders_;
};

// "Which worker is this thread", plus its per-round wake batch. Set at the top
// of Worker::Run. wake_pending/wake_list live here (not on Worker) so the hot
// cross-core post path marks wakes fully inline without needing Worker's
// definition; the owning worker drains wake_list once per loop in FlushWakes.
struct CurrentWorker {
  WorkerId id_ = 0;
  CrossCore* cross_core_ = nullptr;
  Worker* self_ = nullptr;
  std::vector<std::uint8_t> wake_pending_;  // per-target dedup flag
  std::vector<unsigned> wake_list_;         // targets marked this round
#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
  std::vector<std::uint64_t> wake_mark_ns_;  // first mark in the wake batch
#endif
};

#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
std::uint64_t CrossCoreTraceNowNanos() noexcept;
#endif

inline CurrentWorker& MutableThisWorker() noexcept {
  static thread_local CurrentWorker w;
  return w;
}
inline const CurrentWorker& ThisWorker() noexcept {
  return MutableThisWorker();
}

#if CELER_ENABLE_SUBMIT_TASK_COUNT
inline thread_local std::uint64_t g_local_submit_task_count = 0;

inline std::uint64_t LocalSubmitTaskCount() noexcept {
  return g_local_submit_task_count;
}
#endif

inline void SetThisWorker(WorkerId id, CrossCore* cross_core,
                          Worker* self) noexcept {
  CurrentWorker& w = MutableThisWorker();
  w.id_ = id;
  w.cross_core_ = cross_core;
  w.self_ = self;
  w.wake_pending_.assign(cross_core->size(), 0);
  w.wake_list_.clear();
#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
  w.wake_mark_ns_.assign(cross_core->size(), 0);
#endif
}

// Mark worker `target` to be woken at the end of the current loop iteration.
// Batched: the actual MSG_RING wake (one per parked target per round) is issued
// by FlushWakes. Inline on the hot post path — touches only thread-local state.
inline void MarkWakeWorker(unsigned target) noexcept {
  CurrentWorker& w = MutableThisWorker();
  if (target == w.id_) {
    return;  // never wake self
  }
  if (w.wake_pending_[target] == 0) {
    w.wake_pending_[target] = 1;
    w.wake_list_.push_back(target);
#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
    w.wake_mark_ns_[target] = CrossCoreTraceNowNanos();
#endif
  }
}

inline void ActivateCrossCoreLane(CrossCore* cc, unsigned receiver,
                                  unsigned sender,
                                  CrossCoreLane& lane) noexcept {
  if (!lane.active_.exchange(true, std::memory_order_acq_rel)) {
    cc->ActivateSender(receiver, sender);
  }
}

inline void PostRequest(CrossCore* cc, unsigned target,
                        RemoteWork* work) noexcept {
#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
  work->request_post_ns_ = CrossCoreTraceNowNanos();
#endif
  const unsigned sender = ThisWorker().id_;
  CrossCoreLane& lane = cc->lane(target, sender);
  if (lane.requests_.try_enqueue(work)) {
    ActivateCrossCoreLane(cc, target, sender, lane);
  } else {
    WorkerMailbox& mailbox = cc->mailbox(target);
    mailbox.requests_.enqueue(work);
    mailbox.overflow_pending_.store(true, std::memory_order_release);
  }
  MarkWakeWorker(target);
}

inline void PostReply(CrossCore* cc, WorkerId origin,
                      RemoteWork* work) noexcept {
#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
  work->reply_post_ns_ = CrossCoreTraceNowNanos();
#endif
  const unsigned sender = ThisWorker().id_;
  CrossCoreLane& lane = cc->lane(origin, sender);
  if (lane.replies_.try_enqueue(work)) {
    ActivateCrossCoreLane(cc, origin, sender, lane);
  } else {
    WorkerMailbox& mailbox = cc->mailbox(origin);
    mailbox.replies_.enqueue(work);
    mailbox.overflow_pending_.store(true, std::memory_order_release);
  }
  MarkWakeWorker(origin);
}

inline void PostNotification(CrossCore* cc, unsigned target,
                             RemoteNotification notification) noexcept {
  const unsigned sender = ThisWorker().id_;
  CrossCoreLane& lane = cc->lane(target, sender);
  if (lane.notifications_.try_enqueue(notification)) {
    ActivateCrossCoreLane(cc, target, sender, lane);
  } else {
    WorkerMailbox& mailbox = cc->mailbox(target);
    mailbox.notifications_.enqueue(notification);
    mailbox.overflow_pending_.store(true, std::memory_order_release);
  }
  MarkWakeWorker(target);
}

// Defined in worker.cpp, where Worker is complete. This keeps the generic
// cross-core awaiter independent of Worker's concrete scheduler layout.
void SpawnOnCurrentWorker(Task<absl::Status> task);

// Awaiter returned by SubmitTo. Runs fn on the target worker's thread and
// resumes the caller on the caller's (origin) worker. R must be default-
// constructible (true for all Redis return types: optional, absl::Status, bool,
// ...).
template <typename Fn>
class SubmitAwaiter : public RemoteWork {
 public:
  using R = std::invoke_result_t<Fn>;
  static_assert(std::is_default_constructible_v<R>,
                "SubmitTo fn must return a default-constructible value");

  SubmitAwaiter(unsigned target, Fn fn) : target_(target), fn_(std::move(fn)) {
    run_fn_ = &SubmitAwaiter::RunFn;
  }

  bool await_ready() noexcept {
    if (target_ == ThisWorker().id_) {  // local shard: run inline, no hop
      result_ = fn_();
      return true;
    }
    return false;
  }

  void await_suspend(std::coroutine_handle<> h) noexcept {
    waiter_ = h;
    origin_ = ThisWorker().id_;
    task_class_ = CurrentTaskClass();
    PostRequest(ThisWorker().cross_core_, target_, this);
  }

  R await_resume() { return std::move(result_); }

 private:
  static void RunFn(RemoteWork* base) {
    auto* self = static_cast<SubmitAwaiter*>(base);
    self->result_ = self->fn_();
  }

  unsigned target_;
  Fn fn_;
  R result_{};
};

// Run `fn` on `target` worker's thread; suspend the caller; resume it on its
// own worker once the result returns. fn must be a leaf op (no further
// cross-core calls, no blocking). If target is the current worker, fn runs
// inline.
template <typename Fn>
SubmitAwaiter<Fn> SubmitTo(unsigned target, Fn fn) {
  return SubmitAwaiter<Fn>(target, std::move(fn));
}

template <typename T>
struct IsTask : std::false_type {};

template <typename T>
struct IsTask<Task<T>> : std::true_type {
  using value_type = T;
};

// Awaiter for a coroutine operation owned by another worker. Unlike SubmitTo,
// the target function may suspend on that worker's io_uring operations. Its
// result is sent back only after the Task completes, and the caller is always
// resumed on its origin worker.
template <typename Fn>
class SubmitTaskAwaiter : public RemoteWork {
 public:
  using TaskType = std::invoke_result_t<Fn>;
  static_assert(IsTask<TaskType>::value,
                "SubmitTaskTo fn must return celer::Task<T>");
  using R = typename IsTask<TaskType>::value_type;

  SubmitTaskAwaiter(unsigned target, Fn fn)
      : target_(target), fn_(std::move(fn)) {
    run_fn_ = &SubmitTaskAwaiter::Start;
  }

  bool await_ready() const noexcept { return false; }

  void await_suspend(std::coroutine_handle<> handle) noexcept {
    waiter_ = handle;
    origin_ = ThisWorker().id_;
    task_class_ = CurrentTaskClass();
    if (target_ == origin_) {
      Start(this);
      return;
    }
    PostRequest(ThisWorker().cross_core_, target_, this);
  }

  R await_resume() { return std::move(*result_); }

 private:
  static void Start(RemoteWork* base) {
    auto* self = static_cast<SubmitTaskAwaiter*>(base);
    self->reply_deferred_ = true;
    SpawnOnCurrentWorker(self->Run());
  }

  Task<absl::Status> Run() {
    // A coroutine lambda's frame retains a pointer to its closure object; it
    // does not copy the captures into the coroutine frame. Move the closure
    // into this Run coroutine's frame before invoking it, so it remains alive
    // on the target worker across every suspension. Invoking a moved temporary
    // (or retaining it only in the origin worker's awaiter) is unsafe here.
    Fn target_fn = std::move(fn_);
    result_.emplace(co_await std::invoke(target_fn));
    PostReply(ThisWorker().cross_core_, origin_, this);
    co_return absl::OkStatus();
  }

  unsigned target_;
  Fn fn_;
  std::optional<R> result_;
};

// Run an asynchronous operation on target. fn is invoked on the target worker
// and must return Task<R>; awaiting SubmitTaskTo yields R on the origin worker.
template <typename Fn>
auto SubmitTaskTo(unsigned target, Fn fn) {
#if CELER_ENABLE_SUBMIT_TASK_COUNT
  if (target != ThisWorker().id_) {
    ++g_local_submit_task_count;
  }
#endif
  return SubmitTaskAwaiter<Fn>(target, std::move(fn));
}

}  // namespace celer

#endif  // CELER_RUNTIME_CROSS_CORE_H_
