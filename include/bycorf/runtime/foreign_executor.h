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

#ifndef BYCORF_RUNTIME_FOREIGN_EXECUTOR_H_
#define BYCORF_RUNTIME_FOREIGN_EXECUTOR_H_

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "bycorf/runtime/cross_core.h"

namespace bycorf {

class Runtime;

namespace detail {

// Runtime-owned state kept outside Worker and WorkerMailbox so enabling
// foreign ingress does not add bookkeeping to the data-plane event loop.
struct ForeignExecutorState {
  CrossCore* cross_core_ = nullptr;
  WorkerId worker_id_ = 0;
  std::atomic<std::size_t> pending_{0};
  std::atomic<bool> accepting_{true};
};

// Publishes directly to the target's MPSC mailbox. Unlike worker-to-worker
// posts, this path never reads ThisWorker TLS or touches an SPSC lane.
bool PostForeignNotification(ForeignExecutorState* state,
                             RemoteNotification notification) noexcept;
void CompleteForeignNotification(ForeignExecutorState* state) noexcept;

}  // namespace detail

// Copyable, non-owning ingress handle for one Runtime worker. Calls may come
// from arbitrary threads, including threads not created by Bycorf. The Runtime
// must outlive every copy and its foreign producers must stop before Runtime
// RequestStop begins. Adapter teardown should stop producers, call
// WaitUntilIdle(), and only then stop the Runtime.
class ForeignExecutor {
 public:
  ForeignExecutor() = default;

  bool valid() const noexcept { return state_ != nullptr; }
  WorkerId worker_id() const noexcept {
    return valid() ? state_->worker_id_ : WorkerId{0};
  }

  // Delivers a move-owned typed callable on the target worker. The callable
  // runs inline while the worker drains control notifications, so it must be
  // noexcept, bounded, and non-blocking. Returns false when the executor is
  // invalid, the Runtime is stopping, or queue/payload allocation fails.
  template <typename Notification>
    requires std::is_nothrow_invocable_r_v<void, std::decay_t<Notification>&>
  [[nodiscard]] bool Notify(Notification&& notification) const {
    using Owned = std::decay_t<Notification>;
    using Envelope = OwnedNotification<Owned>;
    if (!valid() || !state_->accepting_.load(std::memory_order_acquire)) {
      return false;
    }
    std::unique_ptr<Envelope> owned(new (std::nothrow) Envelope{
        state_, std::forward<Notification>(notification)});
    if (owned == nullptr) return false;

    RemoteNotification remote{
        .context_ = owned.get(),
        .value_ = 0,
        .run_fn_ = &Invoke<Owned>,
    };
    state_->pending_.fetch_add(1, std::memory_order_acq_rel);
    if (!detail::PostForeignNotification(state_, remote)) {
      detail::CompleteForeignNotification(state_);
      return false;
    }
    (void)owned.release();
    return true;
  }

  // Schedules an existing coroutine on the target worker's normal ready
  // queue. This is allocation-free and preserves foreground/background
  // classification. A false result means the handle was not accepted and
  // remains owned by the caller.
  [[nodiscard]] bool Resume(
      std::coroutine_handle<> continuation) const noexcept;

  // Waits until every notification accepted before this call has reached the
  // target worker. Callers must first stop all producers; otherwise a new post
  // can race the observed zero. This is intended for adapter teardown before
  // Runtime::RequestStop(), not for normal request synchronization.
  void WaitUntilIdle() const noexcept;

 private:
  friend class Runtime;
  explicit ForeignExecutor(detail::ForeignExecutorState* state) noexcept
      : state_(state) {}

  template <typename Notification>
  struct OwnedNotification {
    detail::ForeignExecutorState* state_;
    Notification notification_;
  };

  template <typename Notification>
  static void Invoke(void* context, std::uint64_t) noexcept {
    using Envelope = OwnedNotification<Notification>;
    std::unique_ptr<Envelope> owned(static_cast<Envelope*>(context));
    detail::ForeignExecutorState* state = owned->state_;
    owned->notification_();
    owned.reset();
    detail::CompleteForeignNotification(state);
  }

  detail::ForeignExecutorState* state_ = nullptr;
};

}  // namespace bycorf

#endif  // BYCORF_RUNTIME_FOREIGN_EXECUTOR_H_
