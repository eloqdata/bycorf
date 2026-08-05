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

Status Server::Start(const ServerOptions& options) {
  if (started_) [[unlikely]] {
    return Status(StatusCode::kFailedPrecondition, "server already started");
  }
  if (options.thread_count == 0) [[unlikely]] {
    return Status(StatusCode::kInvalidArgument, "thread_count must be >= 1");
  }
  if (services_.empty()) [[unlikely]] {
    return Status(StatusCode::kFailedPrecondition, "no services registered");
  }

  options_ = options;
  for (Service* service : services_) {
    service->Prepare(options_.thread_count);
  }

  started_ = true;
  auto fn = [this](unsigned i, Worker& worker) { return RunWorker(i, worker); };
  runtime_.Start(options_.thread_count, std::move(fn));
  return Status::Ok();
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
  worker_options.idle_timeout_ms = options_.idle_timeout_ms;
  worker_options.recv_buffer_count = options_.recv_buffer_count;
  worker_options.ring_entries = options_.ring_entries;

  auto init_status = worker.Init(worker_options);
  if (!init_status.ok()) [[unlikely]] {
    spdlog::error("worker[{}] init failed: {}", index, init_status.message());
    return 1;
  }

  ServiceContext ctx{
      .bind_ip = options_.bind_ip,
      .reuse_port = options_.reuse_port && options_.thread_count > 1,
  };
  for (Service* service : services_) {
    worker.Spawn(service->Run(worker, ctx));
  }

  worker.Run();

  if (!worker.stop_requested()) [[unlikely]] {
    return 1;
  }
  return 0;
}

}  // namespace celer
