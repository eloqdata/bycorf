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

#include "celer/net/server.h"

#include <thread>

#include "spdlog/spdlog.h"

namespace celer {

Server::~Server() {
  RequestStop();
  WaitUntilStopped();
}

void Server::AddService(Service* service) {
  if (service != nullptr) {
    services_.push_back(service);
  }
}

absl::Status Server::Start(const ServerOptions& options) {
  if (started_) [[unlikely]] {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "server already started");
  }
  if (options.thread_count_ == 0) [[unlikely]] {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "thread_count must be >= 1");
  }
  if (services_.empty()) [[unlikely]] {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "no services registered");
  }

  options_ = options;
  if (options_.bind_addresses_.empty()) {
    options_.bind_addresses_.push_back(options_.bind_ip_);
  }
  for (Service* service : services_) {
    service->Prepare(options_.thread_count_);
  }

  started_ = true;
  auto fn = [this](unsigned i, Worker& worker) { return RunWorker(i, worker); };
  runtime_.Start(options_.thread_count_, std::move(fn), options_.pin_workers_);
  return absl::OkStatus();
}

void Server::StopAccepting() noexcept {
  if (!started_) return;
  if (accepting_stopped_.exchange(true, std::memory_order_acq_rel)) return;
  for (Service* service : services_) {
    service->Stop();
  }
}

void Server::RequestStop() noexcept {
  if (!started_) return;
  if (stop_requested_.exchange(true, std::memory_order_acq_rel)) return;

  // Listener shutdown may have happened earlier during graceful draining.
  StopAccepting();
  runtime_.RequestStop();
}

void Server::WaitUntilStopped() {
  if (!started_) return;
  runtime_.WaitUntilStopped();
  stopped_ = true;
}

bool Server::stopped() const noexcept { return stopped_ || runtime_.stopped(); }

int Server::completion_fd() const noexcept { return runtime_.completion_fd(); }

int Server::exit_code() const noexcept { return runtime_.exit_code(); }

int Server::RunWorker(unsigned index, Worker& worker) {
  WorkerOptions worker_options;
  worker_options.idle_timeout_ms_ = options_.idle_timeout_ms_;
  worker_options.recv_buffer_count_ = options_.recv_buffer_count_;
  worker_options.ring_entries_ = options_.ring_entries_;
  worker_options.busy_poll_us_ = options_.busy_poll_us_;
  worker_options.foreground_budget_us_ = options_.foreground_budget_us_;
  worker_options.background_budget_us_ = options_.background_budget_us_;
  worker_options.background_warrant_percent_ =
      options_.background_warrant_percent_;
  worker_options.spdk_max_completions_per_poll_ =
      options_.spdk_max_completions_per_poll_;
  worker_options.spdk_foreground_pre_poll_us_ =
      options_.spdk_foreground_pre_poll_us_;

  auto init_status = worker.Init(worker_options);
  if (!init_status.ok()) [[unlikely]] {
    spdlog::error("worker[{}] init failed: {}", index, init_status.message());
    return 1;
  }

  ServiceContext ctx{
      .bind_addresses_ = options_.bind_addresses_,
      .reuse_port_ = options_.reuse_port_ && options_.thread_count_ > 1,
  };
  for (Service* service : services_) {
    worker.SpawnRoot(service->Run(worker, ctx));
  }

  worker.Run();

  // Two-phase shutdown: wait until every worker has left its event loop, so
  // no cross-core reference into another worker's coroutine frames can still
  // be exercised, then reclaim this worker's remaining frames on its own
  // thread — their destructors may touch thread-affine state such as buffer
  // pools. The ring is quiesced first so no in-flight kernel operation can
  // touch the freed frames.
  drained_workers_.fetch_add(1, std::memory_order_acq_rel);
  while (drained_workers_.load(std::memory_order_acquire) <
         options_.thread_count_) {
    std::this_thread::yield();
  }
  worker.Shutdown();
  worker.DestroyDetachedTasks();

  // A frame destroyed on one worker may release state owned by another.
  // Finalizers can reclaim worker-owned service state only after every such
  // destructor has run.
  reclaimed_workers_.fetch_add(1, std::memory_order_acq_rel);
  while (reclaimed_workers_.load(std::memory_order_acquire) <
         options_.thread_count_) {
    std::this_thread::yield();
  }

  // Unwind services in reverse registration order so a service may release
  // dependencies only after their later-registered consumers have finalized.
  for (auto service = services_.rbegin(); service != services_.rend();
       ++service) {
    (*service)->FinalizeWorker(worker);
  }

  if (!worker.stop_requested()) [[unlikely]] {
    return 1;
  }
  return 0;
}

}  // namespace celer
