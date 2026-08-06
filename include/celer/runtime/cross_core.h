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

#include <atomic>
#include <array>
#include <cstddef>
#include <coroutine>
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
#include "celer/runtime/concurrentqueue.h"
#include "celer/runtime/task.h"

namespace celer {

class Worker;

// A unit of cross-core work. It lives inside the awaiter, which lives in the
// caller's coroutine frame — so there is NO separate heap allocation. Only the
// pointer travels: origin -> target (request) -> origin (reply).
//
// Two separate queues (request / reply) mean the queue an item came from already
// tells us what to do with it — no phase tag, no dispatch function pointer.
// run_fn is the one unavoidable indirect call: it invokes the type-erased user
// closure, and only on the request leg.
struct RemoteWork {
  unsigned origin = 0;
  std::coroutine_handle<> waiter{};
  void (*run_fn)(RemoteWork*) = nullptr;  // runs the user fn, stores result
  bool reply_deferred = false;
};

// Small one-way control message. It is copied into the target mailbox, so the
// sender does not have to keep an awaiter or heap allocation alive. Intended
// for ownership hand-backs such as returning a registered buffer to its owner.
struct RemoteNotification {
  void* context = nullptr;
  std::uint64_t value = 0;
  void (*run_fn)(void*, std::uint64_t) noexcept = nullptr;
};

// wake_seq value meaning "the owner is parked": the owner CASes wake_seq to this
// before blocking; a producer bumps wake_seq after enqueuing and wakes the owner
// only if it reads this back (so a parked owner is woken exactly once per round).
inline constexpr std::uint32_t kWakeSeqParked = 1u << 31;

template <typename T, std::size_t Capacity = 256>
class alignas(64) SpscRing {
 public:
  static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0,
                "SPSC capacity must be a power of two");

  bool try_enqueue(T value) noexcept {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);
    const std::size_t next = (tail + 1) & (Capacity - 1);
    if (next == head_.load(std::memory_order_acquire)) {
      return false;
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
  alignas(64) std::array<T, Capacity> entries_{};
};

// One SPSC lane for a fixed sender -> receiver pair.
struct alignas(64) CrossCoreLane {
  SpscRing<RemoteWork*> requests;
  SpscRing<RemoteWork*> replies;
  SpscRing<RemoteNotification> notifications;
  std::atomic<bool> pending{false};

  bool empty() const noexcept {
    return requests.empty() && replies.empty() && notifications.empty();
  }
};

// One mailbox per worker, cache-line aligned so neighbours in the contiguous
// array never false-share their hot fields. Both queues are MPSC: every other
// worker may push, only the owning worker drains. ring_fd / wake_fd are how to
// wake it: MSG_RING to the io_uring ring, eventfd write as the legacy fallback.
struct alignas(64) WorkerMailbox {
  // Rare overflow path for a full bounded SPSC lane.
  moodycamel::ConcurrentQueue<RemoteWork*> requests;  // others -> me: run here
  moodycamel::ConcurrentQueue<RemoteWork*> replies;   // results coming back to me
  moodycamel::ConcurrentQueue<RemoteNotification> notifications;
  std::atomic<bool> overflow_pending{false};
  int ring_fd = -1;
  int wake_fd = -1;
  std::atomic<std::uint32_t> wake_seq{0};
};

// All workers' mailboxes — a contiguous, fixed-size array (one indirection per
// access, no per-element pointer). Created by the Runtime BEFORE any worker
// thread starts, so cross-core posts never hit a missing mailbox.
class CrossCore {
 public:
  CrossCore() = default;  // empty until the Runtime knows the worker count
  explicit CrossCore(unsigned n)
      : size_(n),
        mailboxes_(std::make_unique<WorkerMailbox[]>(n)),
        lanes_(std::make_unique<CrossCoreLane[]>(
            static_cast<std::size_t>(n) * n)) {}

  unsigned size() const noexcept { return size_; }
  WorkerMailbox& mailbox(unsigned i) noexcept { return mailboxes_[i]; }
  CrossCoreLane& lane(unsigned receiver, unsigned sender) noexcept {
    return lanes_[static_cast<std::size_t>(receiver) * size_ + sender];
  }

 private:
  unsigned size_ = 0;
  std::unique_ptr<WorkerMailbox[]> mailboxes_;
  std::unique_ptr<CrossCoreLane[]> lanes_;
};

// "Which worker is this thread", plus its per-round wake batch. Set at the top of
// Worker::Run. wake_pending/wake_list live here (not on Worker) so the hot
// cross-core post path marks wakes fully inline without needing Worker's
// definition; the owning worker drains wake_list once per loop in FlushWakes.
struct CurrentWorker {
  unsigned id = 0;
  CrossCore* cross_core = nullptr;
  Worker* self = nullptr;
  std::vector<std::uint8_t> wake_pending;  // per-target dedup flag
  std::vector<unsigned> wake_list;         // targets marked this round
};

inline CurrentWorker& MutableThisWorker() noexcept {
  static thread_local CurrentWorker w;
  return w;
}
inline const CurrentWorker& ThisWorker() noexcept { return MutableThisWorker(); }
inline void SetThisWorker(unsigned id, CrossCore* cross_core, Worker* self) noexcept {
  CurrentWorker& w = MutableThisWorker();
  w.id = id;
  w.cross_core = cross_core;
  w.self = self;
  w.wake_pending.assign(cross_core->size(), 0);
  w.wake_list.clear();
}

// Mark worker `target` to be woken at the end of the current loop iteration.
// Batched: the actual MSG_RING wake (one per parked target per round) is issued
// by FlushWakes. Inline on the hot post path — touches only thread-local state.
inline void MarkWakeWorker(unsigned target) noexcept {
  CurrentWorker& w = MutableThisWorker();
  if (target == w.id) {
    return;  // never wake self
  }
  if (w.wake_pending[target] == 0) {
    w.wake_pending[target] = 1;
    w.wake_list.push_back(target);
  }
}

inline void PostRequest(CrossCore* cc, unsigned target, RemoteWork* work) noexcept {
  const unsigned sender = ThisWorker().id;
  CrossCoreLane& lane = cc->lane(target, sender);
  if (lane.requests.try_enqueue(work)) {
    lane.pending.store(true, std::memory_order_release);
  } else {
    WorkerMailbox& mailbox = cc->mailbox(target);
    mailbox.requests.enqueue(work);
    mailbox.overflow_pending.store(true, std::memory_order_release);
  }
  MarkWakeWorker(target);
}

inline void PostReply(CrossCore* cc, unsigned origin, RemoteWork* work) noexcept {
  const unsigned sender = ThisWorker().id;
  CrossCoreLane& lane = cc->lane(origin, sender);
  if (lane.replies.try_enqueue(work)) {
    lane.pending.store(true, std::memory_order_release);
  } else {
    WorkerMailbox& mailbox = cc->mailbox(origin);
    mailbox.replies.enqueue(work);
    mailbox.overflow_pending.store(true, std::memory_order_release);
  }
  MarkWakeWorker(origin);
}

inline void PostNotification(CrossCore* cc, unsigned target,
                             RemoteNotification notification) noexcept {
  const unsigned sender = ThisWorker().id;
  CrossCoreLane& lane = cc->lane(target, sender);
  if (lane.notifications.try_enqueue(notification)) {
    lane.pending.store(true, std::memory_order_release);
  } else {
    WorkerMailbox& mailbox = cc->mailbox(target);
    mailbox.notifications.enqueue(notification);
    mailbox.overflow_pending.store(true, std::memory_order_release);
  }
  MarkWakeWorker(target);
}

// Defined in worker.cpp, where Worker is complete. This keeps the generic
// cross-core awaiter independent of Worker's concrete scheduler layout.
void SpawnOnCurrentWorker(Task<Status> task);

// Awaiter returned by SubmitTo. Runs fn on the target worker's thread and
// resumes the caller on the caller's (origin) worker. R must be default-
// constructible (true for all Redis return types: optional, Status, bool, ...).
template <typename Fn>
class SubmitAwaiter : public RemoteWork {
 public:
  using R = std::invoke_result_t<Fn>;
  static_assert(std::is_default_constructible_v<R>,
                "SubmitTo fn must return a default-constructible value");

  SubmitAwaiter(unsigned target, Fn fn) : target_(target), fn_(std::move(fn)) {
    run_fn = &SubmitAwaiter::RunFn;
  }

  bool await_ready() noexcept {
    if (target_ == ThisWorker().id) {  // local shard: run inline, no hop
      result_ = fn_();
      return true;
    }
    return false;
  }

  void await_suspend(std::coroutine_handle<> h) noexcept {
    waiter = h;
    origin = ThisWorker().id;
    PostRequest(ThisWorker().cross_core, target_, this);
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

// Run `fn` on `target` worker's thread; suspend the caller; resume it on its own
// worker once the result returns. fn must be a leaf op (no further cross-core
// calls, no blocking). If target is the current worker, fn runs inline.
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
    run_fn = &SubmitTaskAwaiter::Start;
  }

  bool await_ready() const noexcept { return false; }

  void await_suspend(std::coroutine_handle<> handle) noexcept {
    waiter = handle;
    origin = ThisWorker().id;
    if (target_ == origin) {
      Start(this);
      return;
    }
    PostRequest(ThisWorker().cross_core, target_, this);
  }

  R await_resume() { return std::move(*result_); }

 private:
  static void Start(RemoteWork* base) {
    auto* self = static_cast<SubmitTaskAwaiter*>(base);
    self->reply_deferred = true;
    SpawnOnCurrentWorker(self->Run());
  }

  Task<Status> Run() {
    // A coroutine lambda's frame retains a pointer to its closure object; it
    // does not copy the captures into the coroutine frame. Move the closure
    // into this Run coroutine's frame before invoking it, so it remains alive
    // on the target worker across every suspension. Invoking a moved temporary
    // (or retaining it only in the origin worker's awaiter) is unsafe here.
    Fn target_fn = std::move(fn_);
    result_.emplace(co_await std::invoke(target_fn));
    PostReply(ThisWorker().cross_core, origin, this);
    co_return Status::Ok();
  }

  unsigned target_;
  Fn fn_;
  std::optional<R> result_;
};

// Run an asynchronous operation on target. fn is invoked on the target worker
// and must return Task<R>; awaiting SubmitTaskTo yields R on the origin worker.
template <typename Fn>
auto SubmitTaskTo(unsigned target, Fn fn) {
  return SubmitTaskAwaiter<Fn>(target, std::move(fn));
}

}  // namespace celer

#endif  // CELER_RUNTIME_CROSS_CORE_H_
