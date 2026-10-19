// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef BYCORF_TESTS_RUNTIME_TEST_H_
#define BYCORF_TESTS_RUNTIME_TEST_H_

#include <gtest/gtest.h>

#include "bycorf/runtime/runtime.h"

namespace bycorf::test {

using WorkerCheck = Task<absl::Status> (*)(Worker&);

inline Task<absl::Status> CompleteCheck(Worker& worker, WorkerCheck check,
                                        bool& completed) {
  const auto status = co_await check(worker);
  EXPECT_TRUE(status.ok()) << status;
  completed = true;
  worker.RequestStop();
  co_return status;
}

// Keep coroutine frames and callbacks on their worker through teardown. A
// GoogleTest assertion in a coroutine must be nonfatal (EXPECT_*): ASSERT_*
// returns normally and cannot be used in a coroutine. Check returned statuses
// here as well, so an early co_return cannot silently pass a case.
inline void RunOnWorker(WorkerCheck check, WorkerOptions options = {}) {
  Runtime runtime;
  runtime.Start(
      1,
      [&](unsigned, Worker& worker) {
        const auto initialized = worker.Init(options);
        EXPECT_TRUE(initialized.ok()) << initialized;
        if (!initialized.ok()) return 1;
        bool completed = false;
        worker.Spawn(CompleteCheck(worker, check, completed));
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        EXPECT_TRUE(completed) << "worker stopped before the case completed";
        return completed ? 0 : 1;
      },
      /*pin_workers=*/false);
  runtime.WaitUntilStopped();
  EXPECT_EQ(runtime.exit_code(), 0);
}

}  // namespace bycorf::test

#endif  // BYCORF_TESTS_RUNTIME_TEST_H_
