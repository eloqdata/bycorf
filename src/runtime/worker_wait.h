// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "bycorf/runtime/worker.h"
#include "spdlog/spdlog.h"

namespace bycorf {

template <typename Observer>
bool Worker::WaitForWork(Observer&& observe) {
  CheckIdleConnections();
  WorkerMailbox& mb = cross_core_->mailbox(id_);
  bool parked = false;
  bool waited = false;
  const auto prepared = Network::PrepareWait();
  // Keep all exits inside this attempt so even partial preparation is cleaned
  // up. FinishWait errors participate in the normal worker failure path.
  const bool ok = [&] {
    if (!prepared.ok()) {
      spdlog::error("worker prepare wait failed: {}",
                    prepared.status().message());
      return false;
    }
    if (!*prepared) return true;
    observe(WaitPhase::kPrepared);

    // Snapshot BEFORE draining. Publication after the empty check changes the
    // sequence and defeats CAS; publication after CAS sends a persistent CQE.
    const auto seq = mb.wake_seq_.load(std::memory_order_acquire);
    bool work = KernelBackend().Poll();
    work |= Network::Poll();
    work |= PollStorage();
    work |= DrainCrossCore();
    observe(WaitPhase::kRechecked);
    if (work || HasRunnableWork() || NotifyWake() || Storage::NeedsPolling() ||
        !Network::CanWait())
      return true;

    auto expected = seq;
    if (!mb.wake_seq_.compare_exchange_strong(expected, kWakeSeqParked,
                                              std::memory_order_acq_rel,
                                              std::memory_order_relaxed))
      return true;
    parked = true;
    observe(WaitPhase::kParked);

    // Preparation and rechecking may queue polls, output, and peer wakes.
    // Accepted requests are not necessarily submitted: never sleep through
    // temporary enter backpressure or work left in the submission FIFO.
    FlushWakes();
    Network::Flush();
    const auto submitted = KernelBackend().Submit();
    if (!submitted.ok()) {
      if (absl::IsUnavailable(submitted)) return true;
      submit_failed_ = true;
      spdlog::error("worker submit before wait failed: {}",
                    submitted.message());
      return false;
    }
    observe(WaitPhase::kSubmitted);
    if (KernelBackend().HasPending() || HasRunnableWork() || NotifyWake() ||
        Storage::NeedsPolling() || !Network::CanWait())
      return true;

    const int timeout_ms =
        Network::WaitTimeout(options_.idle_timeout_ms_ > 0 ? 100 : -1);
    // BSD deadlines below the millisecond wait resolution are serviced by
    // the next worker round without issuing a zero-timeout wait syscall.
    if (timeout_ms == 0) return true;
    observe(WaitPhase::kWaiting);
    waited = true;
    return KernelBackend().Wait(timeout_ms);
  }();
  if (parked) mb.wake_seq_.store(0, std::memory_order_release);
  const auto finished = Network::FinishWait(waited);
  if (!finished.ok()) {
    spdlog::error("worker finish wait failed: {}", finished.message());
    return false;
  }
  return ok;
}

}  // namespace bycorf
