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

#ifndef CELER_RUNTIME_SYNC_H_
#define CELER_RUNTIME_SYNC_H_

// Coroutine synchronization primitives. AsyncMutex, UnlockGuard, and
// AsyncNotification are worker-local: all suspensions and wake-ups must happen
// on one worker, so they need no atomics. CrossWorkerMutex and
// CoroutineBarrier resume each waiter on the worker where it suspended.

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <deque>
#include <mutex>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"

namespace celer {

class AsyncMutex {
 public:
  class LockAwaiter {
   public:
    explicit LockAwaiter(AsyncMutex* mutex) : mutex_(mutex) {}

    bool await_ready() noexcept {
      if (!mutex_->locked_) {
        mutex_->locked_ = true;
        return true;
      }
      return false;
    }

    bool await_suspend(std::coroutine_handle<> awaiting) {
      mutex_->waiters_.push_back(awaiting);
      return true;
    }

    void await_resume() noexcept {}

   private:
    AsyncMutex* mutex_;
  };

  LockAwaiter Lock() noexcept { return LockAwaiter(this); }

  void Unlock(Worker& worker) noexcept {
    if (waiters_.empty()) {
      locked_ = false;
      return;
    }
    std::coroutine_handle<> next = waiters_.front();
    waiters_.pop_front();
    worker.Enqueue(next);
  }

 private:
  bool locked_ = false;
  std::deque<std::coroutine_handle<>> waiters_;
};

// A FIFO coroutine mutex for state shared by multiple runtime workers.
// Contention suspends only the caller; the worker pthread remains available to
// run unrelated connections. The atomic queue guard is held only while moving
// waiter pointers and never while protected application code runs. The mutex
// and every queued coroutine frame must remain alive until that waiter resumes;
// cancellation must coordinate with lock acquisition rather than destroying a
// queued frame.
class CrossWorkerMutex {
  struct Waiter {
    Worker* worker_ = nullptr;
    std::coroutine_handle<> handle_{};
    Waiter* next_ = nullptr;
  };

 public:
  class LockAwaiter {
   public:
    LockAwaiter(CrossWorkerMutex* mutex, Worker* worker) noexcept
        : mutex_(mutex) {
      waiter_.worker_ = worker;
    }

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> awaiting) noexcept {
      queued_ = mutex_->AcquireOrQueue(&waiter_, awaiting);
      return queued_;
    }
    void await_resume() const noexcept {
      // A queued owner does not re-enter AcquireOrQueue after handoff. Touch
      // the queue guard once so protected writes published by Unlock are
      // visible independently of the runtime's scheduling transport.
      if (queued_) mutex_->SynchronizeAcquisition();
    }

   private:
    CrossWorkerMutex* mutex_ = nullptr;
    Waiter waiter_;
    bool queued_ = false;
  };

  class Guard {
   public:
    explicit Guard(CrossWorkerMutex* mutex) noexcept : mutex_(mutex) {}
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    ~Guard() { mutex_->Unlock(); }

   private:
    CrossWorkerMutex* mutex_ = nullptr;
  };

  CrossWorkerMutex() = default;
  CrossWorkerMutex(const CrossWorkerMutex&) = delete;
  CrossWorkerMutex& operator=(const CrossWorkerMutex&) = delete;

  LockAwaiter Lock(Worker& worker) noexcept {
    return LockAwaiter(this, &worker);
  }

 private:
  static void ResumeRemote(void* context, std::uint64_t value) noexcept {
    static_cast<Worker*>(context)->Enqueue(
        std::coroutine_handle<>::from_address(
            reinterpret_cast<void*>(static_cast<std::uintptr_t>(value))));
  }

  bool AcquireOrQueue(Waiter* waiter,
                      std::coroutine_handle<> awaiting) noexcept {
    waiter->handle_ = awaiting;
    waiter->next_ = nullptr;
    LockQueue();
    if (!held_) {
      held_ = true;
      UnlockQueue();
      return false;
    }
    if (waiters_tail_ == nullptr) {
      waiters_head_ = waiter;
    } else {
      waiters_tail_->next_ = waiter;
    }
    waiters_tail_ = waiter;
    UnlockQueue();
    return true;
  }

  void Unlock() noexcept {
    LockQueue();
    Waiter* wake = waiters_head_;
    if (wake == nullptr) {
      held_ = false;
      UnlockQueue();
      return;
    }
    waiters_head_ = wake->next_;
    if (waiters_head_ == nullptr) waiters_tail_ = nullptr;
    // Ownership transfers directly to the FIFO head. Keep held_ set so a
    // newcomer cannot overtake it before its coroutine runs.
    UnlockQueue();

    const CurrentWorker& current = ThisWorker();
    if (wake->worker_->id() == current.id_) {
      wake->worker_->Enqueue(wake->handle_);
    } else {
      PostNotification(
          current.cross_core_, wake->worker_->id(),
          RemoteNotification{
              .context_ = wake->worker_,
              .value_ = static_cast<std::uint64_t>(
                  reinterpret_cast<std::uintptr_t>(wake->handle_.address())),
              .run_fn_ = &CrossWorkerMutex::ResumeRemote,
          });
    }
  }

  void LockQueue() noexcept {
    while (queue_lock_.test_and_set(std::memory_order_acquire)) {
#if defined(__x86_64__) || defined(__i386__)
      __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
      asm volatile("yield" ::: "memory");
#else
      std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
    }
  }

  void UnlockQueue() noexcept { queue_lock_.clear(std::memory_order_release); }

  void SynchronizeAcquisition() noexcept {
    LockQueue();
    UnlockQueue();
  }

  std::atomic_flag queue_lock_ = ATOMIC_FLAG_INIT;
  bool held_ = false;
  Waiter* waiters_head_ = nullptr;
  Waiter* waiters_tail_ = nullptr;
};

class UnlockGuard {
 public:
  UnlockGuard(AsyncMutex* mutex, Worker* worker)
      : mutex_(mutex), worker_(worker) {}
  UnlockGuard(const UnlockGuard&) = delete;
  UnlockGuard& operator=(const UnlockGuard&) = delete;
  ~UnlockGuard() { mutex_->Unlock(*worker_); }

 private:
  AsyncMutex* mutex_;
  Worker* worker_;
};

class AsyncNotification {
 public:
  class Awaiter {
   public:
    explicit Awaiter(AsyncNotification* notification)
        : notification_(notification) {}

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> awaiting) {
      notification_->waiters_.push_back(awaiting);
      return true;
    }
    void await_resume() const noexcept {}

   private:
    AsyncNotification* notification_;
  };

  Awaiter Wait() noexcept { return Awaiter(this); }

  void NotifyAll(Worker& worker) {
    std::deque<std::coroutine_handle<>> waiters;
    waiters.swap(waiters_);
    for (std::coroutine_handle<> waiter : waiters) {
      worker.Enqueue(waiter);
    }
  }

 private:
  std::deque<std::coroutine_handle<>> waiters_;
};

class CoroutineBarrier {
 public:
  explicit CoroutineBarrier(unsigned participants)
      : participants_(participants) {}

  class Awaiter {
   public:
    Awaiter(CoroutineBarrier* barrier, Worker* worker)
        : barrier_(barrier), worker_(worker) {}

    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<> awaiting) {
      return barrier_->Arrive(worker_, awaiting);
    }
    absl::Status await_resume() { return barrier_->status(); }

   private:
    CoroutineBarrier* barrier_;
    Worker* worker_;
  };

  Awaiter Wait(Worker& worker) { return Awaiter(this, &worker); }

  void Abort(absl::Status status) {
    std::vector<Waiter> wake;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!status_.ok()) {
        return;
      }
      status_ = std::move(status);
      completed_ = true;
      wake.swap(waiters_);
    }
    Wake(wake);
  }

 private:
  struct Waiter {
    Worker* worker_ = nullptr;
    std::coroutine_handle<> handle_{};
  };

  static void ResumeRemote(void* context, std::uint64_t value) noexcept {
    static_cast<Worker*>(context)->Enqueue(
        std::coroutine_handle<>::from_address(
            reinterpret_cast<void*>(static_cast<std::uintptr_t>(value))));
  }

  bool Arrive(Worker* worker, std::coroutine_handle<> awaiting) {
    std::vector<Waiter> wake;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (completed_) {
        return false;
      }
      waiters_.push_back(Waiter{worker, awaiting});
      ++arrived_;
      if (arrived_ != participants_) {
        return true;
      }
      completed_ = true;
      wake.swap(waiters_);
    }
    Wake(wake);
    return true;
  }

  void Wake(const std::vector<Waiter>& waiters) {
    const CurrentWorker& current = ThisWorker();
    for (const Waiter& waiter : waiters) {
      if (waiter.worker_->id() == current.id_) {
        waiter.worker_->Enqueue(waiter.handle_);
      } else {
        PostNotification(
            current.cross_core_, waiter.worker_->id(),
            RemoteNotification{
                .context_ = waiter.worker_,
                .value_ = static_cast<std::uint64_t>(
                    reinterpret_cast<std::uintptr_t>(waiter.handle_.address())),
                .run_fn_ = &CoroutineBarrier::ResumeRemote,
            });
      }
    }
  }

  absl::Status status() {
    std::lock_guard<std::mutex> lock(mutex_);
    return status_;
  }

  unsigned participants_ = 0;
  unsigned arrived_ = 0;
  bool completed_ = false;
  absl::Status status_ = absl::OkStatus();
  std::mutex mutex_;
  std::vector<Waiter> waiters_;
};

}  // namespace celer

#endif  // CELER_RUNTIME_SYNC_H_
