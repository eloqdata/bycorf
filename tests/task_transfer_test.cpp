/*
 * Copyright (C) 2026 EloqData Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "bycorf/runtime/task.h"

namespace {
std::coroutine_handle<> pending;
unsigned live = 0;
unsigned background_registrations = 0;
unsigned forgotten = 0;
struct Lifetime {
  Lifetime() { ++live; }
  ~Lifetime() { --live; }
};
struct Suspend {
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> handle) noexcept {
    pending = handle;
  }
  void await_resume() const noexcept {}
};
using bycorf::Task;
Task<std::uint64_t> Leaf(std::uint64_t value) { co_return value; }
Task<std::uint64_t> Sequential(unsigned count) {
  Lifetime lifetime;
  std::uint64_t sum = 0;
  for (unsigned i = 0; i < count; ++i) sum += co_await Leaf(i);
  co_return sum;
}
Task<std::uint64_t> Nested(unsigned depth) {
  Lifetime lifetime;
  if (depth == 0) co_return 1;
  co_return 1 + co_await Nested(depth - 1);
}
Task<std::unique_ptr<unsigned>> Delayed(unsigned value) {
  Lifetime lifetime;
  co_await Suspend{};
  co_return std::make_unique<unsigned>(value);
}
Task<std::uint64_t> Mixed() {
  Lifetime lifetime;
  std::uint64_t sum = 0;
  for (unsigned i = 0; i < 20; ++i) {
    sum += co_await Sequential(10000);
    auto value = co_await Delayed(i);
    sum += *value;
  }
  co_return sum;
}
Task<std::uint64_t> CancelledParent() {
  Lifetime lifetime;
  co_return *co_await Delayed(7);
}
bool RunTask(Task<std::uint64_t> task, std::uint64_t expected) {
  auto handle = std::move(task).ReleaseHandle();
  handle.resume();
  while (!handle.done()) {
    if (!pending) {
      handle.destroy();
      return false;
    }
    std::exchange(pending, {}).resume();
  }
  const bool okay = handle.promise().value_ == expected;
  handle.destroy();
  return okay && live == 0 && !pending;
}
class TaskTransfer : public testing::Test {
 protected:
  void SetUp() override {
    pending = {};
    live = background_registrations = forgotten = 0;
    bycorf::MutableCurrentTaskClass() = bycorf::TaskClass::kForeground;
  }
  void TearDown() override {
    EXPECT_EQ(live, 0u);
    EXPECT_FALSE(pending);
    bycorf::MutableCurrentTaskClass() = bycorf::TaskClass::kForeground;
  }
};

TEST_F(TaskTransfer, SequentialAwaitsBoundNativeStack) {
  EXPECT_TRUE(RunTask(Sequential(100000), 4999950000ULL));
}

TEST_F(TaskTransfer, NestedAwaitsBoundNativeStack) {
  EXPECT_TRUE(RunTask(Nested(100000), 100001));
}

TEST_F(TaskTransfer, SuspensionsMoveOnlyResultsAndBackgroundHooks) {
  bycorf::MutableCurrentTaskClass() = bycorf::TaskClass::kBackground;
  EXPECT_TRUE(RunTask(Mixed(), 20ULL * 49995000 + 190));
  EXPECT_GT(background_registrations, 0u);
}

TEST_F(TaskTransfer, RootDestructionOwnsSuspendedChild) {
  auto task = CancelledParent();
  auto handle = std::move(task).ReleaseHandle();
  handle.resume();
  EXPECT_FALSE(handle.done());
  EXPECT_TRUE(pending);
  EXPECT_EQ(live, 2u);
  // The event owner cancels its outstanding wake before reclaiming the root.
  pending = {};
  handle.destroy();
  EXPECT_EQ(live, 0u);
}

TEST_F(TaskTransfer, DetachedCompletionDestroysRootOnce) {
  unsigned completions = 0;
  auto task = Sequential(100000);
  task.SetCompletionCallback(&completions,
                             [](void* context, auto handle) noexcept {
                               ++*static_cast<unsigned*>(context);
                               handle.destroy();
                             });
  std::move(task).ReleaseHandle().resume();
  EXPECT_EQ(completions, 1u);
  EXPECT_EQ(live, 0u);
  EXPECT_GT(forgotten, 0u);
}

TEST_F(TaskTransfer, ReentrantCompletionsRunEveryChild) {
  unsigned completions = 0;
  auto reentrant = Sequential(1000);
  reentrant.SetCompletionCallback(
      &completions, [](void* context, auto handle) noexcept {
        ++*static_cast<unsigned*>(context);
        handle.destroy();
        for (unsigned i = 0; i < 2; ++i) {
          auto child = Sequential(1000);
          child.SetCompletionCallback(context,
                                      [](void* result, auto done) noexcept {
                                        ++*static_cast<unsigned*>(result);
                                        done.destroy();
                                      });
          std::move(child).ReleaseHandle().resume();
        }
      });
  std::move(reentrant).ReleaseHandle().resume();
  EXPECT_EQ(completions, 3u);
  EXPECT_EQ(live, 0u);
}
}  // namespace

namespace bycorf {
// Isolate Task transfer/ownership from I/O. The real frame allocator is used;
// scheduler hooks are counted while the test drives explicit owner-local wakes.
void RegisterBackgroundTask(std::coroutine_handle<>) noexcept {
  ++background_registrations;
}
void ForgetTaskScheduling(std::coroutine_handle<>) noexcept { ++forgotten; }
}  // namespace bycorf

int main(int argc, char** argv) {
  constexpr char kChild[] = "--bycorf-bounded-stack";
  const bool child = argc > 1 && std::strcmp(argv[argc - 1], kChild) == 0;
  if (child) argv[--argc] = nullptr;
  // Preserve filters, shuffle/repeat, and XML output across exec; GoogleTest
  // removes its own flags from argv during initialization.
  std::vector<char*> arguments(argv, argv + argc);
  testing::InitGoogleTest(&argc, argv);
  if (child || GTEST_FLAG_GET(list_tests)) return RUN_ALL_TESTS();

  // The limit must precede exec so the native stack mapping starts bounded.
  // Discovery only lists cases and must never run this stress setup.
  const rlimit stack{512 * 1024, 512 * 1024};
  const rlimit core{0, 0};
  if (setrlimit(RLIMIT_STACK, &stack) || setrlimit(RLIMIT_CORE, &core)) {
    std::cerr << "cannot set bounded-stack test limits\n";
    return 1;
  }
  arguments.push_back(const_cast<char*>(kChild));
  arguments.push_back(nullptr);
  execv("/proc/self/exe", arguments.data());
  std::cerr << "cannot exec bounded-stack tests\n";
  return 1;
}
