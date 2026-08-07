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

#ifndef CELER_RUNTIME_TASK_H_
#define CELER_RUNTIME_TASK_H_

#include <coroutine>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

#include "celer/base/status.h"
#include "celer/runtime/coroutine_frame_pool.h"

namespace celer {

enum class TaskClass : std::uint8_t {
  kForeground,
  kBackground,
};

inline TaskClass& MutableCurrentTaskClass() noexcept {
  static thread_local TaskClass task_class = TaskClass::kForeground;
  return task_class;
}

inline TaskClass CurrentTaskClass() noexcept {
  return MutableCurrentTaskClass();
}

// Implemented by Worker. Background membership follows nested Task frames so
// every later I/O, lock, or cross-core resume returns to the background queue.
void RegisterBackgroundTask(std::coroutine_handle<> handle) noexcept;
void ForgetTaskScheduling(std::coroutine_handle<> handle) noexcept;

template <typename T>
class Task {
 public:
  struct promise_type {
    using completion_fn =
        void (*)(void*, std::coroutine_handle<>) noexcept;

    std::optional<T> value_;
    std::coroutine_handle<> continuation_{};
    void* completion_context_ = nullptr;
    completion_fn completion_ = nullptr;
    // Index in the owning worker's detached-task registry (O(1) removal),
    // or kNotDetached while the task is owned by a Task object/awaiter.
    static constexpr std::uint32_t kNotDetached = 0xffffffffu;
    std::uint32_t detached_index_ = kNotDetached;

    static void* operator new(std::size_t size) {
      return detail::AllocateCoroutineFrame(size);
    }

    static void operator delete(void* frame, std::size_t) noexcept {
      detail::ReleaseCoroutineFrame(frame);
    }

    static void operator delete(void* frame) noexcept {
      detail::ReleaseCoroutineFrame(frame);
    }

    Task get_return_object() noexcept {
      return Task(std::coroutine_handle<promise_type>::from_promise(*this));
    }

    std::suspend_always initial_suspend() noexcept { return {}; }
    auto final_suspend() noexcept {
      struct FinalAwaiter {
        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<> await_suspend(handle_type handle) noexcept {
          auto& promise = handle.promise();
          if (promise.completion_ != nullptr) {
            promise.completion_(promise.completion_context_, handle);
            return std::noop_coroutine();
          }
          auto continuation = promise.continuation_;
          if (continuation) {
            return continuation;
          }
          return std::noop_coroutine();
        }

        void await_resume() noexcept {}
      };
      return FinalAwaiter{};
    }

    void return_value(T value) noexcept { value_ = std::move(value); }

    void unhandled_exception() noexcept { std::terminate(); }
  };

  using handle_type = std::coroutine_handle<promise_type>;

  Task() = default;
  explicit Task(handle_type handle) : handle_(handle) {}

  Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (handle_) {
        ForgetTaskScheduling(handle_);
        handle_.destroy();
      }
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  ~Task() {
    if (handle_) {
      ForgetTaskScheduling(handle_);
      handle_.destroy();
    }
  }

  bool valid() const noexcept { return static_cast<bool>(handle_); }
  bool done() const noexcept { return !handle_ || handle_.done(); }

  void SetCompletionCallback(void* context,
                             typename promise_type::completion_fn completion) noexcept {
    if (!handle_) {
      return;
    }
    handle_.promise().completion_context_ = context;
    handle_.promise().completion_ = completion;
  }

  // The awaiter only borrows the handle: ownership stays with the Task
  // object, whose destructor at the end of the co_await full expression (or
  // whenever the awaiting frame is destroyed while suspended here) destroys
  // the child frame. This is what lets shutdown reclamation of a suspended
  // coroutine cascade through its whole child chain.
  struct Awaiter {
    handle_type handle;

    bool await_ready() const noexcept { return !handle || handle.done(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
      handle.promise().continuation_ = awaiting;
      if (CurrentTaskClass() == TaskClass::kBackground) {
        RegisterBackgroundTask(handle);
      }
      return handle;
    }

    T await_resume() { return std::move(*handle.promise().value_); }
  };

  auto operator co_await() && noexcept { return Awaiter{handle_}; }

  T TakeResult() && { return std::move(*handle_.promise().value_); }
  handle_type ReleaseHandle() && noexcept { return std::exchange(handle_, {}); }

 private:
  handle_type handle_{};
};

}  // namespace celer

#endif  // CELER_RUNTIME_TASK_H_
