// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <coroutine>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <vector>

#include "bycorf/runtime/worker.h"

namespace bycorf {

// Exercise the real scheduler without initializing an I/O backend. A zero
// deadline stops after one dispatch; the maximum deadline drains all work.
class WorkerSchedulerTestPeer {
 public:
  static std::size_t Drain(Worker& worker, TaskClass task_class,
                           bool one = false) {
    const auto deadline = one ? 0 : std::numeric_limits<std::int64_t>::max();
    return task_class == TaskClass::kBackground
               ? worker.DrainBackgroundUntil(deadline)
               : worker.DrainReadyUntil(deadline);
  }

  static void MergeDeferred(Worker& worker) { worker.MergeDeferred(); }

  static void EnqueueRemote(Worker& worker, RemoteWork* work) {
    if (work->task_class_ == TaskClass::kBackground) {
      worker.background_remote_work_.push_back(work);
    } else {
      worker.foreground_remote_work_.push_back(work);
    }
  }
};

namespace {

Task<absl::Status> Record(std::vector<unsigned>* order, unsigned id,
                          TaskClass task_class, std::function<void()> action) {
  EXPECT_EQ(CurrentTaskClass(), task_class);
  order->push_back(id);
  if (action) action();
  co_return absl::OkStatus();
}

struct LoggedWork : RemoteWork {
  std::vector<unsigned>* order;
  unsigned id;
  std::function<void()> action;
};

class WorkerScheduler : public testing::TestWithParam<TaskClass> {
 protected:
  void TearDown() override {
    for (auto handle : frames_) {
      worker_.ForgetScheduling(handle);
      handle.destroy();
    }
  }

  std::coroutine_handle<> MakeTask(unsigned id,
                                   std::function<void()> action = {}) {
    auto handle =
        Record(&order_, id, GetParam(), std::move(action)).ReleaseHandle();
    frames_.push_back(handle);
    if (GetParam() == TaskClass::kBackground)
      worker_.RegisterBackground(handle);
    return handle;
  }

  void PostRemote(unsigned id, std::function<void()> action = {}) {
    auto work = std::make_unique<LoggedWork>();
    work->order = &order_;
    work->id = id;
    work->action = std::move(action);
    work->task_class_ = GetParam();
    // This test owns the records. No origin coroutine or reply transport is
    // involved in testing their dispatch and queue relocation.
    work->reply_deferred_ = true;
    work->run_fn_ = +[](RemoteWork* base) {
      auto& item = *static_cast<LoggedWork*>(base);
      EXPECT_EQ(CurrentTaskClass(), item.task_class_);
      item.order->push_back(item.id);
      if (item.action) item.action();
    };
    WorkerSchedulerTestPeer::EnqueueRemote(worker_, work.get());
    remote_.push_back(std::move(work));
  }

  std::size_t Drain(bool one = false) {
    return WorkerSchedulerTestPeer::Drain(worker_, GetParam(), one);
  }

  Worker worker_;
  std::vector<std::coroutine_handle<>> frames_;
  std::vector<std::unique_ptr<LoggedWork>> remote_;
  std::vector<unsigned> order_;
};

TEST_P(WorkerScheduler, DeferredMergePreservesFifoAcrossWrappedGrowth) {
  for (unsigned id = 0; id < 160; ++id) worker_.Enqueue(MakeTask(id));
  for (unsigned id = 240; id < 480; ++id) worker_.EnqueueNext(MakeTask(id));
  for (unsigned i = 0; i < 80; ++i) ASSERT_EQ(Drain(true), 1U);
  for (unsigned id = 160; id < 240; ++id) worker_.Enqueue(MakeTask(id));
  // Append deferred work behind a wrapped, partially consumed ready queue.
  WorkerSchedulerTestPeer::MergeDeferred(worker_);
  EXPECT_EQ(Drain(), 400U);
  std::vector<unsigned> expected(480);
  std::iota(expected.begin(), expected.end(), 0U);
  EXPECT_EQ(order_, expected);
  EXPECT_EQ(Drain(), 0U);
}

TEST_P(WorkerScheduler, ReentrantNextTurnWorkRemainsDeferred) {
  worker_.Enqueue(MakeTask(0, [this] {
    for (unsigned id = 2; id < 302; ++id) worker_.EnqueueNext(MakeTask(id));
    worker_.Enqueue(MakeTask(1));
  }));
  EXPECT_EQ(Drain(), 2U);
  EXPECT_EQ(order_, (std::vector<unsigned>{0, 1}));
  EXPECT_EQ(Drain(), 0U);
  WorkerSchedulerTestPeer::MergeDeferred(worker_);
  EXPECT_EQ(Drain(), 300U);
  std::vector<unsigned> expected(302);
  std::iota(expected.begin(), expected.end(), 0U);
  EXPECT_EQ(order_, expected);
}

TEST_P(WorkerScheduler, RemoteAndReadyAlternateAcrossGrowthAndShrink) {
  for (unsigned i = 0; i < 64; ++i) {
    worker_.Enqueue(MakeTask(i * 2));
    std::function<void()> grow;
    if (i == 0) {
      grow = [this] {
        // A remote callback grows both queues while the scheduler is draining.
        for (unsigned j = 64; j < 320; ++j) {
          worker_.Enqueue(MakeTask(j * 2));
          PostRemote(j * 2 + 1);
        }
      };
    }
    PostRemote(i * 2 + 1, std::move(grow));
  }
  EXPECT_EQ(Drain(), 640U);
  // Sustained small depths exercise pop-triggered contraction as well as
  // growth. Relocating slots must not invalidate the value being dispatched.
  for (unsigned i = 320; i < 1344; ++i) {
    worker_.Enqueue(MakeTask(i * 2));
    PostRemote(i * 2 + 1);
    ASSERT_EQ(Drain(), 2U);
  }
  std::vector<unsigned> expected;
  for (unsigned i = 0; i < 1344; ++i) {
    expected.push_back(i * 2 + 1);
    expected.push_back(i * 2);
  }
  EXPECT_EQ(order_, expected);
}

struct FrameLifetime {
  unsigned* destroyed;
  ~FrameLifetime() { ++*destroyed; }
};

Task<absl::Status> CompletedTask(std::unique_ptr<FrameLifetime> lifetime) {
  (void)lifetime;
  co_return absl::OkStatus();
}

TEST_P(WorkerScheduler, CompletedFrameOwnershipSurvivesQueueGrowth) {
  unsigned destroyed = 0;
  for (unsigned i = 0; i < 300; ++i) {
    auto lifetime = std::make_unique<FrameLifetime>();
    lifetime->destroyed = &destroyed;
    auto handle = CompletedTask(std::move(lifetime)).ReleaseHandle();
    handle.resume();
    ASSERT_TRUE(handle.done());
    if (GetParam() == TaskClass::kBackground)
      worker_.RegisterBackground(handle);
    worker_.Enqueue(handle, true);
  }
  EXPECT_EQ(destroyed, 0U);
  EXPECT_EQ(Drain(), 300U);
  EXPECT_EQ(destroyed, 300U);
  EXPECT_EQ(Drain(), 0U);
}

INSTANTIATE_TEST_SUITE_P(TaskClasses, WorkerScheduler,
                         testing::Values(TaskClass::kForeground,
                                         TaskClass::kBackground),
                         [](const testing::TestParamInfo<TaskClass>& info) {
                           return info.param == TaskClass::kForeground
                                      ? "Foreground"
                                      : "Background";
                         });

}  // namespace
}  // namespace bycorf
