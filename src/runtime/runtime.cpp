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

#include "celer/runtime/runtime.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <sys/eventfd.h>
#include <unistd.h>

#include "celer/runtime/cross_core.h"

namespace celer {

class Runtime::Impl {
 public:
  Impl() {
    completion_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (completion_fd_ < 0) {
      throw std::runtime_error("failed to create runtime completion eventfd");
    }
  }

  ~Impl() {
    if (completion_fd_ >= 0) {
      close(completion_fd_);
    }
    // size() is 0 if Start() was never called — loop simply does nothing.
    for (unsigned i = 0; i < cross_core_.size(); ++i) {
      const int fd = cross_core_.mailbox(i).wake_fd;
      if (fd >= 0) {
        close(fd);
      }
    }
  }

  struct State {
    Worker worker;
    std::thread thread;
  };

  void Start(unsigned thread_count, WorkerMain main_fn) {
    if (started_) {
      throw std::logic_error("runtime already started");
    }
    if (thread_count == 0) {
      throw std::invalid_argument("thread_count must be >= 1");
    }
    if (thread_count > std::numeric_limits<WorkerId>::max()) {
      throw std::invalid_argument("thread_count exceeds WorkerId capacity");
    }

    started_ = true;

    // Build the cross-core mailboxes and their wake eventfds BEFORE any worker
    // thread starts, so a worker can be woken the moment it exists.
    cross_core_ = CrossCore(thread_count);
    for (unsigned i = 0; i < thread_count; ++i) {
      const int fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
      if (fd < 0) {
        throw std::runtime_error("failed to create worker wake eventfd");
      }
      cross_core_.mailbox(i).wake_fd = fd;
    }

    states_.reserve(thread_count);
    active_workers_.store(thread_count, std::memory_order_release);
    for (unsigned i = 0; i < thread_count; ++i) {
      auto state = std::make_unique<State>();
      State* raw = state.get();
      raw->worker.BindCrossCore(static_cast<WorkerId>(i), &cross_core_);
      raw->thread = std::thread([this, i, raw, main_fn] {
        int local_exit_code = main_fn(i, raw->worker);

        if (local_exit_code != 0) {
          int expected = 0;
          exit_code_.compare_exchange_strong(expected, local_exit_code,
                                             std::memory_order_acq_rel);
          RequestStop();
        }

        if (active_workers_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
          const std::uint64_t ready = 1;
          (void)write(completion_fd_, &ready, sizeof(ready));
          std::lock_guard<std::mutex> lk(mu_);
          stopped_ = true;
          cv_.notify_all();
          return;
        }

        std::lock_guard<std::mutex> lk(mu_);
        cv_.notify_all();
      });
      states_.push_back(std::move(state));
    }
  }

  void RequestStop() noexcept {
    if (!started_) {
      return;
    }
    if (stop_requested_.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    for (auto& state : states_) {
      state->worker.RequestStop();
    }
    std::lock_guard<std::mutex> lk(mu_);
    cv_.notify_all();
  }

  void WaitUntilStopped() {
    if (!started_) {
      return;
    }

    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait(lk, [this] {
        return stopped_ || active_workers_.load(std::memory_order_acquire) == 0;
      });
    }

    for (auto& state : states_) {
      if (state->thread.joinable()) {
        state->thread.join();
      }
    }
    stopped_ = true;
  }

  bool started() const noexcept {
    return started_;
  }

  bool stopped() const noexcept {
    return started_ && stopped_;
  }

  int exit_code() const noexcept {
    return exit_code_.load(std::memory_order_acquire);
  }

  int completion_fd() const noexcept {
    return completion_fd_;
  }

 private:
  // Declared first so it outlives the workers that hold pointers into it.
  CrossCore cross_core_;
  std::vector<std::unique_ptr<State>> states_;
  std::atomic<unsigned> active_workers_{0};
  std::atomic<int> exit_code_{0};
  std::atomic<bool> stop_requested_{false};
  std::mutex mu_;
  std::condition_variable cv_;
  bool started_ = false;
  bool stopped_ = false;
  int completion_fd_ = -1;
};

Runtime::Runtime() : impl_(std::make_unique<Impl>()) {
}

Runtime::Runtime(Runtime&&) noexcept = default;

Runtime& Runtime::operator=(Runtime&&) noexcept = default;

Runtime::~Runtime() {
  if (impl_ != nullptr) {
    impl_->RequestStop();
    impl_->WaitUntilStopped();
  }
}

void Runtime::Start(unsigned thread_count, WorkerMain main_fn) {
  impl_->Start(thread_count, std::move(main_fn));
}

void Runtime::RequestStop() noexcept {
  impl_->RequestStop();
}

void Runtime::WaitUntilStopped() {
  impl_->WaitUntilStopped();
}

bool Runtime::started() const noexcept {
  return impl_->started();
}

bool Runtime::stopped() const noexcept {
  return impl_->stopped();
}

int Runtime::exit_code() const noexcept {
  return impl_->exit_code();
}

int Runtime::completion_fd() const noexcept {
  return impl_->completion_fd();
}

}  // namespace celer
