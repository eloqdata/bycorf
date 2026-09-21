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

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <utility>

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
bool Run(Task<std::uint64_t> task, std::uint64_t expected) {
  auto handle = std::move(task).ReleaseHandle();
  handle.resume();
  while (!handle.done()) {
    if (!pending) return false;
    std::exchange(pending, {}).resume();
  }
  const bool okay = handle.promise().value_ == expected;
  handle.destroy();
  return okay && live == 0 && !pending;
}
int Check() {
  if (!Run(Sequential(100000), 4999950000ULL)) return 1;
  std::cout << "PASS 100000 sequential awaits\n";
  if (!Run(Nested(100000), 100001)) return 1;
  std::cout << "PASS 100000 nested awaits\n";
  bycorf::MutableCurrentTaskClass() = bycorf::TaskClass::kBackground;
  if (!Run(Mixed(), 20ULL * 49995000 + 190)) return 1;
  bycorf::MutableCurrentTaskClass() = bycorf::TaskClass::kForeground;
  if (background_registrations == 0) return 1;
  std::cout
      << "PASS real suspensions, move-only results and background hooks\n";
  {
    auto task = CancelledParent();
    auto handle = std::move(task).ReleaseHandle();
    handle.resume();
    if (handle.done() || !pending || live != 2) return 1;
    // The event owner cancels its outstanding wake before reclaiming the root.
    pending = {};
    handle.destroy();
    if (live != 0) return 1;
  }
  std::cout << "PASS suspended child ownership on root destruction\n";
  unsigned completions = 0;
  auto task = Sequential(100000);
  task.SetCompletionCallback(&completions,
                             [](void* context, auto handle) noexcept {
                               ++*static_cast<unsigned*>(context);
                               handle.destroy();
                             });
  std::move(task).ReleaseHandle().resume();
  if (completions != 1 || live != 0 || forgotten == 0) return 1;
  std::cout << "PASS detached completion destroys its root exactly once\n";
  completions = 0;
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
  if (completions != 3 || live != 0) return 1;
  std::cout << "PASS reentrant completions retain every pending transfer\n";
  return 0;
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
  if (argc == 2) return Check();
  const pid_t child = fork();
  if (child < 0) return 1;
  if (child == 0) {
    // Set before exec so the runtime's native stack starts with a bounded size.
    const rlimit stack{512 * 1024, 512 * 1024};
    const rlimit core{0, 0};
    if (setrlimit(RLIMIT_STACK, &stack) || setrlimit(RLIMIT_CORE, &core))
      _exit(127);
    execl(argv[0], argv[0], "--child", nullptr);
    _exit(127);
  }
  int status = 0;
  if (waitpid(child, &status, 0) != child) return 1;
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    std::cerr << "FAIL bounded-stack Task checks, wait status=" << status
              << '\n';
    return 1;
  }
  return 0;
}
