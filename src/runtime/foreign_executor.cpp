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

#include "bycorf/runtime/foreign_executor.h"

#include <unistd.h>

#include <cerrno>
#include <cstdint>

#include "bycorf/runtime/worker.h"

namespace bycorf {
namespace {

void EnqueueContinuation(void* context, std::uint64_t value) noexcept {
  // value carries Runtime-owned completion accounting without allocating a
  // wrapper around the coroutine handle.
  auto* state = reinterpret_cast<detail::ForeignExecutorState*>(value);
  ThisWorker().self_->Enqueue(std::coroutine_handle<>::from_address(context));
  detail::CompleteForeignNotification(state);
}

void WakeForeign(WorkerMailbox& mailbox) noexcept {
  const bool parked = mailbox.wake_seq_.fetch_add(
                          1, std::memory_order_acq_rel) == kWakeSeqParked;
  if (!parked) return;

  // eventfd coalesces wakeups. EAGAIN means an earlier wake is already
  // pending; EINTR is the only retryable failure. Other failures are possible
  // only after teardown has violated the executor lifetime contract.
  const std::uint64_t one = 1;
  while (::write(mailbox.wake_fd_, &one, sizeof(one)) < 0 && errno == EINTR) {
  }
}

}  // namespace

namespace detail {

bool PostForeignNotification(ForeignExecutorState* state,
                             RemoteNotification notification) noexcept {
  if (state == nullptr || state->cross_core_ == nullptr ||
      state->worker_id_ >= state->cross_core_->size() ||
      notification.run_fn_ == nullptr) {
    return false;
  }
  WorkerMailbox& mailbox = state->cross_core_->mailbox(state->worker_id_);
  if (!mailbox.notifications_.enqueue(notification)) {
    return false;
  }
  mailbox.overflow_pending_.store(true, std::memory_order_release);
  WakeForeign(mailbox);
  return true;
}

void CompleteForeignNotification(ForeignExecutorState* state) noexcept {
  if (state->pending_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    state->pending_.notify_all();
  }
}

}  // namespace detail

bool ForeignExecutor::Resume(
    std::coroutine_handle<> continuation) const noexcept {
  if (!continuation || !valid() ||
      !state_->accepting_.load(std::memory_order_acquire)) {
    return false;
  }
  state_->pending_.fetch_add(1, std::memory_order_acq_rel);
  if (detail::PostForeignNotification(
          state_,
          RemoteNotification{.context_ = continuation.address(),
                             .value_ = reinterpret_cast<std::uint64_t>(state_),
                             .run_fn_ = &EnqueueContinuation})) {
    return true;
  }
  detail::CompleteForeignNotification(state_);
  return false;
}

void ForeignExecutor::WaitUntilIdle() const noexcept {
  if (!valid()) return;
  std::size_t pending = state_->pending_.load(std::memory_order_acquire);
  while (pending != 0) {
    state_->pending_.wait(pending, std::memory_order_acquire);
    pending = state_->pending_.load(std::memory_order_acquire);
  }
}

}  // namespace bycorf
