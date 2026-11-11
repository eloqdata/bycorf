// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <gtest/gtest.h>

#include "../src/runtime/worker_wait.h"

namespace bycorf {
class WorkerWaitTestPeer {
 public:
  using Phase = Worker::WaitPhase;
  template <typename Observer>
  static bool Wait(Worker& worker, Observer&& observer) {
    return worker.WaitForWork(std::forward<Observer>(observer));
  }
  static void Flush(Worker& worker) { worker.Flush(); }
  static bool Drain(Worker& worker) { return worker.DrainCrossCore(); }
  static bool Poll(Worker& worker) { return worker.Network::Poll(); }
  static void FlushNetwork(Worker& worker) { worker.Network::Flush(); }
  static IoUringBackend& Kernel(Worker& worker) {
    return worker.KernelBackend();
  }
  static bool BusyPoll(Worker& worker) {
    worker.options_.busy_poll_us_ = 1;
    worker.busy_poll_cycles_ = 1;
    return worker.BusyPoll();
  }
  static bool CanWait(Worker& worker) { return worker.Network::CanWait(); }
};
// Cancellation of an unsubmitted request produces a deferred software
// completion. This gives a deterministic ring event without timing sleeps or
// a real NIC, and checks both kernel-TCP and DPDK facade isolation.
inline void CheckNetworkRingIsolation(Worker& worker) {
  struct Completion : IoCompletion {
    unsigned calls = 0;
    void Complete(Worker&, int result, unsigned) override {
      ++calls;
      EXPECT_EQ(result, -ECANCELED);
    }
  } completion;
  auto& kernel = WorkerWaitTestPeer::Kernel(worker);
  const __kernel_timespec timeout{.tv_sec = 60, .tv_nsec = 0};
  ASSERT_TRUE(kernel.SubmitTimeout(timeout, &completion).ok());
  WorkerWaitTestPeer::FlushNetwork(worker);
  EXPECT_TRUE(kernel.HasPending()) << "network flush submitted the shared ring";
  ASSERT_TRUE(kernel.SubmitCancel(&completion).ok());
  (void)WorkerWaitTestPeer::Poll(worker);
  EXPECT_EQ(completion.calls, 0U) << "network poll dispatched ring work";
  // Drive terminal cancellation before releasing its tag even on regression.
  while (completion.calls == 0) {
    ASSERT_TRUE(kernel.Submit().ok());
    kernel.Poll();
  }
  EXPECT_EQ(completion.calls, 1U);
}
}  // namespace bycorf
