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

#include "bycorf/runtime/runtime.h"

#include <pthread.h>
#include <sched.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <latch>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "bycorf/runtime/cross_core.h"

namespace bycorf {

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
      const int fd = cross_core_.mailbox(i).wake_fd_;
      if (fd >= 0) {
        close(fd);
      }
    }
  }

  struct State {
    Worker worker_;
    std::thread thread_;
  };

  void Start(unsigned thread_count, WorkerMain main_fn, bool pin_workers) {
    if (started_) {
      throw std::logic_error("runtime already started");
    }
    if (thread_count == 0) {
      throw std::invalid_argument("thread_count must be >= 1");
    }
    if (thread_count > std::numeric_limits<WorkerId>::max()) {
      throw std::invalid_argument("thread_count exceeds WorkerId capacity");
    }

    std::vector<unsigned> worker_cpus;
    if (pin_workers) {
      cpu_set_t allowed;
      CPU_ZERO(&allowed);
      if (pthread_getaffinity_np(pthread_self(), sizeof(allowed), &allowed) !=
          0) {
        throw std::runtime_error("failed to read process CPU affinity");
      }
      for (unsigned cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &allowed)) {
          worker_cpus.push_back(cpu);
        }
      }
      if (worker_cpus.size() < thread_count) {
        throw std::invalid_argument(
            "worker pinning requires at least one allowed CPU per worker");
      }
    }

    FreezeIoBackends();
#if BYCORF_KERNEL_BYPASS
    if (DpdkNetworkEnabled()) {
      const auto network = DpdkBackend::PrepareRuntime(thread_count);
      if (!network.ok())
        throw std::runtime_error(std::string(network.message()));
    }
#endif
    started_ = true;

    // Build the cross-core mailboxes and their wake eventfds BEFORE any worker
    // thread starts, so a worker can be woken the moment it exists.
    cross_core_ = CrossCore(thread_count);
    foreign_executors_.reserve(thread_count);
    for (unsigned i = 0; i < thread_count; ++i) {
      const int fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
      if (fd < 0) {
        throw std::runtime_error("failed to create worker wake eventfd");
      }
      cross_core_.mailbox(i).wake_fd_ = fd;
      auto foreign = std::make_unique<detail::ForeignExecutorState>();
      foreign->cross_core_ = &cross_core_;
      foreign->worker_id_ = static_cast<WorkerId>(i);
      foreign_executors_.push_back(std::move(foreign));
    }

    states_.reserve(thread_count);
    for (unsigned i = 0; i < thread_count; ++i) {
      auto state = std::make_unique<State>();
      state->worker_.BindCrossCore(static_cast<WorkerId>(i), &cross_core_);
      states_.push_back(std::move(state));
    }
    // Publish the complete worker table before a worker can fail and stop its
    // peers. The launch barrier also makes affinity failure an all-worker
    // startup failure, before a service enters any initialization barrier.
    active_workers_.store(thread_count, std::memory_order_release);
    auto launch = std::make_shared<std::latch>(thread_count + 1);
    auto affinity_failed = std::make_shared<std::atomic<bool>>(false);
    unsigned launched = 0;
    try {
      for (unsigned i = 0; i < thread_count; ++i) {
        State* raw = states_[i].get();
        raw->thread_ = std::thread([this, i, raw, main_fn, worker_cpus, launch,
                                    affinity_failed] {
          int local_exit_code = 0;
          if (!worker_cpus.empty()) {
            cpu_set_t affinity;
            CPU_ZERO(&affinity);
            CPU_SET(worker_cpus[i], &affinity);
            if (pthread_setaffinity_np(pthread_self(), sizeof(affinity),
                                       &affinity) != 0) {
              local_exit_code = 1;
              affinity_failed->store(true, std::memory_order_relaxed);
            }
          }
          launch->arrive_and_wait();
          if (affinity_failed->load(std::memory_order_relaxed)) {
            local_exit_code = 1;
          } else {
            local_exit_code = main_fn(i, raw->worker_);
          }
#if BYCORF_KERNEL_BYPASS
          if (DpdkNetworkEnabled() && local_exit_code != 0)
            DpdkBackend::AbortStartup(
                absl::InternalError("worker startup failed"));
          // FreeBSD sockets and EAL registrations belong to this native thread.
          // Teardown after main_fn must finish before the thread exits.
          if (DpdkNetworkEnabled()) raw->worker_.Shutdown();
#endif
          foreign_executors_[i]->accepting_.store(false,
                                                  std::memory_order_release);

          if (local_exit_code != 0) {
            int expected = 0;
            exit_code_.compare_exchange_strong(expected, local_exit_code,
                                               std::memory_order_acq_rel);
            RequestStop();
          }

          if (active_workers_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            const std::uint64_t ready = 1;
            const ssize_t result = write(completion_fd_, &ready, sizeof(ready));
            (void)result;
            std::lock_guard<std::mutex> lk(mu_);
            stopped_ = true;
            cv_.notify_all();
            return;
          }

          std::lock_guard<std::mutex> lk(mu_);
          cv_.notify_all();
        });
        ++launched;
      }
    } catch (...) {
      affinity_failed->store(true, std::memory_order_relaxed);
      active_workers_.fetch_sub(thread_count - launched,
                                std::memory_order_acq_rel);
      launch->count_down(thread_count - launched + 1);
      throw;
    }
    launch->count_down();
  }

  void RequestStop() noexcept {
    if (!started_) {
      return;
    }
    if (stop_requested_.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    for (auto& foreign : foreign_executors_) {
      foreign->accepting_.store(false, std::memory_order_release);
    }
    for (auto& state : states_) {
      state->worker_.RequestStop();
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
      if (state->thread_.joinable()) {
        state->thread_.join();
      }
    }
#if BYCORF_KERNEL_BYPASS
    if (DpdkNetworkEnabled()) DpdkBackend::StopRuntime();
#endif
    stopped_ = true;
  }

  bool started() const noexcept { return started_; }

  bool stopped() const noexcept { return started_ && stopped_; }

  int exit_code() const noexcept {
    return exit_code_.load(std::memory_order_acquire);
  }

  int completion_fd() const noexcept { return completion_fd_; }

  detail::ForeignExecutorState* ForeignState(WorkerId worker_id) {
    if (!started_) {
      throw std::logic_error("foreign executor requires a started runtime");
    }
    if (worker_id >= cross_core_.size()) {
      throw std::out_of_range("foreign executor worker id is out of range");
    }
    return foreign_executors_[worker_id].get();
  }

 private:
  // Declared first so it outlives the workers that hold pointers into it.
  CrossCore cross_core_;
  // Separate from WorkerMailbox so unused foreign ingress adds no work or
  // cache-line traffic to normal data-plane workers.
  std::vector<std::unique_ptr<detail::ForeignExecutorState>> foreign_executors_;
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

Runtime::Runtime() : impl_(std::make_unique<Impl>()) {}

Runtime::Runtime(Runtime&&) noexcept = default;

Runtime& Runtime::operator=(Runtime&&) noexcept = default;

Runtime::~Runtime() {
  if (impl_ != nullptr) {
    impl_->RequestStop();
    impl_->WaitUntilStopped();
  }
}

void Runtime::Start(unsigned thread_count, WorkerMain main_fn,
                    bool pin_workers) {
  impl_->Start(thread_count, std::move(main_fn), pin_workers);
}

void Runtime::RequestStop() noexcept { impl_->RequestStop(); }

void Runtime::WaitUntilStopped() { impl_->WaitUntilStopped(); }

bool Runtime::started() const noexcept { return impl_->started(); }

bool Runtime::stopped() const noexcept { return impl_->stopped(); }

int Runtime::exit_code() const noexcept { return impl_->exit_code(); }

int Runtime::completion_fd() const noexcept { return impl_->completion_fd(); }

bycorf::ForeignExecutor Runtime::GetForeignExecutor(WorkerId worker_id) {
  return bycorf::ForeignExecutor(impl_->ForeignState(worker_id));
}

}  // namespace bycorf
