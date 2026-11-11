// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <thread>

#include "bycorf/runtime/foreign_executor.h"
#include "worker_wait_test_peer.h"

namespace bycorf {
namespace {
using Phase = WorkerWaitTestPeer::Phase;

class WorkerWait : public testing::Test {
 protected:
  void SetUp() override {
    for (unsigned id = 0; id < 2; ++id) {
      cc.mailbox(id).wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
      ASSERT_GE(cc.mailbox(id).wake_fd_, 0);
      workers[id].BindCrossCore(id, &cc);
      SetThisWorker(id, &cc, &workers[id]);
      const auto status = workers[id].Init({.recv_buffer_count_ = 0});
      ASSERT_TRUE(status.ok()) << status;
      WorkerWaitTestPeer::Flush(workers[id]);
    }
    SetThisWorker(0, &cc, &workers[0]);
    // Lost wakes fail the case instead of being rescued by a periodic timer.
    alarm(5);
  }
  void TearDown() override {
    alarm(0);
    for (unsigned id = 0; id < 2; ++id) {
      SetThisWorker(id, &cc, &workers[id]);
      workers[id].Shutdown();
      workers[id].DestroyDetachedTasks();
      close(cc.mailbox(id).wake_fd_);
    }
    MutableThisWorker() = {};
  }
  RemoteNotification Notification() {
    return {.context_ = &calls,
            .value_ = 0,
            .run_fn_ = +[](void* context, std::uint64_t) noexcept {
              ++*static_cast<unsigned*>(context);
            }};
  }
  void PostPeer() {
    SetThisWorker(1, &cc, &workers[1]);
    PostNotification(&cc, 0, Notification());
    WorkerWaitTestPeer::Flush(workers[1]);
    SetThisWorker(0, &cc, &workers[0]);
  }
  void PostForeign() {
    detail::ForeignExecutorState state{.cross_core_ = &cc, .worker_id_ = 0};
    std::thread producer([&] {
      EXPECT_TRUE(detail::PostForeignNotification(&state, Notification()));
    });
    producer.join();
  }
  CrossCore cc{2};
  Worker workers[2];
  unsigned calls = 0;
};

TEST_F(WorkerWait, BusyPollAdvancesSharedRing) {
  struct Completion : IoCompletion {
    unsigned calls = 0;
    void Complete(Worker&, int result, unsigned) override {
      ++calls;
      EXPECT_EQ(result, -ECANCELED);
    }
  } completion;
  auto& kernel = WorkerWaitTestPeer::Kernel(workers[0]);
  const __kernel_timespec timeout{.tv_sec = 60, .tv_nsec = 0};
  ASSERT_TRUE(kernel.SubmitTimeout(timeout, &completion).ok());
  ASSERT_TRUE(kernel.SubmitCancel(&completion).ok());
  EXPECT_TRUE(WorkerWaitTestPeer::BusyPoll(workers[0]));
  EXPECT_EQ(completion.calls, 1U);
  workers[0].Shutdown();
}

TEST_F(WorkerWait, NetworkFacadeDoesNotAdvanceSharedRing) {
  CheckNetworkRingIsolation(workers[0]);
}

#if BYCORF_ENABLE_RDMA
TEST_F(WorkerWait, RdmaFacadeUsesSharedRingForReadinessAndCancellation) {
  struct Completion : IoCompletion {
    unsigned calls = 0;
    int result = 0;
    void Complete(Worker&, int value, unsigned) override {
      ++calls;
      result = value;
    }
  } ready, cancelled;
  int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  ASSERT_GE(fd, 0);
  auto& kernel = WorkerWaitTestPeer::Kernel(workers[0]);
  ASSERT_TRUE(workers[0].SubmitRdmaEventPoll(fd, POLLIN, &ready).ok());
  const std::uint64_t one = 1;
  ASSERT_EQ(write(fd, &one, sizeof(one)), sizeof(one));
  // Only the existing kernel ring is driven. A separate RDMA ring would leave
  // this tag pending until the fixture's lost-wakeup alarm fired.
  while (!ready.calls) {
    ASSERT_TRUE(kernel.Submit().ok());
    kernel.Poll();
  }
  EXPECT_EQ(ready.calls, 1U);
  EXPECT_EQ(ready.result, POLLIN);
  std::uint64_t count;
  ASSERT_EQ(read(fd, &count, sizeof(count)), sizeof(count));
  ASSERT_TRUE(workers[0].SubmitRdmaEventPoll(fd, POLLIN, &cancelled).ok());
  ASSERT_TRUE(workers[0].CancelRdmaEventPoll(&cancelled).ok());
  while (!cancelled.calls) {
    ASSERT_TRUE(kernel.Submit().ok());
    kernel.Poll();
  }
  EXPECT_EQ(cancelled.calls, 1U);
  EXPECT_EQ(cancelled.result, -ECANCELED);
  close(fd);
}
#endif

TEST_F(WorkerWait, MailboxPublicationAfterEmptyCheckDefeatsPark) {
  for (bool foreign : {false, true}) {
    bool waiting = false;
    ASSERT_TRUE(WorkerWaitTestPeer::Wait(workers[0], [&](Phase phase) {
      if (phase == Phase::kRechecked) {
        if (foreign)
          PostForeign();
        else
          PostPeer();
      }
      waiting |= phase == Phase::kWaiting;
    }));
    EXPECT_FALSE(waiting);
    EXPECT_NE(cc.mailbox(0).wake_seq_.load(), kWakeSeqParked);
    EXPECT_TRUE(WorkerWaitTestPeer::Drain(workers[0]));
  }
  EXPECT_EQ(calls, 2U);
}

TEST_F(WorkerWait, PublicationAfterParkLeavesPersistentWake) {
  for (auto boundary : {Phase::kParked, Phase::kWaiting}) {
    for (bool foreign : {false, true}) {
      bool waiting = false;
      ASSERT_TRUE(WorkerWaitTestPeer::Wait(workers[0], [&](Phase phase) {
        if (phase == boundary) {
          if (foreign)
            PostForeign();
          else
            PostPeer();
        }
        waiting |= phase == Phase::kWaiting;
      }));
      EXPECT_TRUE(waiting);
      EXPECT_EQ(cc.mailbox(0).wake_seq_.load(), 0U);
      EXPECT_TRUE(WorkerWaitTestPeer::Drain(workers[0]));
    }
  }
  EXPECT_EQ(calls, 4U);
}

TEST_F(WorkerWait, StopConsumedDuringRecheckDoesNotSleepAgain) {
  bool waiting = false;
  ASSERT_TRUE(WorkerWaitTestPeer::Wait(workers[0], [&](Phase phase) {
    if (phase == Phase::kPrepared) workers[0].RequestStop();
    waiting |= phase == Phase::kWaiting;
  }));
  EXPECT_FALSE(waiting);
  EXPECT_TRUE(workers[0].NotifyWake());
  EXPECT_NE(cc.mailbox(0).wake_seq_.load(), kWakeSeqParked);
}

TEST_F(WorkerWait, StopAfterFinalCheckWakesInfiniteWait) {
  bool waiting = false;
  ASSERT_TRUE(WorkerWaitTestPeer::Wait(workers[0], [&](Phase phase) {
    if (phase == Phase::kWaiting) {
      waiting = true;
      std::thread producer([&] { workers[0].RequestStop(); });
      producer.join();
    }
  }));
  EXPECT_TRUE(waiting);
  EXPECT_TRUE(workers[0].NotifyWake());
  EXPECT_EQ(cc.mailbox(0).wake_seq_.load(), 0U);
}

Task<absl::Status> CountTask(unsigned& count) {
  ++count;
  co_return absl::OkStatus();
}

TEST_F(WorkerWait, RecheckAndSubmissionCannotStrandRunnableTasks) {
  for (auto boundary : {Phase::kRechecked, Phase::kSubmitted}) {
    bool waiting = false;
    ASSERT_TRUE(WorkerWaitTestPeer::Wait(workers[0], [&](Phase phase) {
      if (phase == boundary) workers[0].Spawn(CountTask(calls));
      waiting |= phase == Phase::kWaiting;
    }));
    EXPECT_FALSE(waiting);
    EXPECT_NE(cc.mailbox(0).wake_seq_.load(), kWakeSeqParked);
    EXPECT_TRUE(workers[0].RunOnce(false));
  }
  EXPECT_EQ(calls, 2U);
}

TEST_F(WorkerWait, UnsubmittedOperationVetoesFinalWait) {
  struct Completion : IoCompletion {
    void Complete(Worker&, int, unsigned) override {
      ADD_FAILURE() << "unsubmitted timeout must not complete";
    }
  } completion;
  bool waiting = false;
  ASSERT_TRUE(WorkerWaitTestPeer::Wait(workers[0], [&](Phase phase) {
    if (phase == Phase::kSubmitted) {
      const __kernel_timespec timeout{.tv_sec = 60, .tv_nsec = 0};
      EXPECT_TRUE(workers[0].SubmitTimeout(timeout, &completion).ok());
    }
    waiting |= phase == Phase::kWaiting;
  }));
  EXPECT_FALSE(waiting);
  EXPECT_EQ(cc.mailbox(0).wake_seq_.load(), 0U);
  // Quiesce the queued operation while its completion tag is still alive.
  workers[0].Shutdown();
}
}  // namespace
}  // namespace bycorf
