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

#ifndef CELER_NET_TCP_SERVER_INL_H_
#define CELER_NET_TCP_SERVER_INL_H_

#include "celer/base/log.h"
#include "celer/net/connection.h"
#include "celer/net/tcp_server.h"

namespace celer {

template <typename Handler>
TcpServer<Handler>::~TcpServer() {
  RequestStop();
  WaitUntilStopped();
}

template <typename Handler>
Status TcpServer<Handler>::Start(const TcpServerOptions& options, Handler handler) {
  if (started_) [[unlikely]] {
    return Status(StatusCode::kFailedPrecondition, "server already started");
  }
  if (options.thread_count == 0) [[unlikely]] {
    return Status(StatusCode::kInvalidArgument, "thread_count must be >= 1");
  }
  if (options.port == 0) [[unlikely]] {
    return Status(StatusCode::kInvalidArgument, "port must be non-zero");
  }

  options_ = options;
  handler_ = std::move(handler);
  runtimes_.reserve(options_.thread_count);
  for (unsigned i = 0; i < options_.thread_count; ++i) {
    runtimes_.push_back(std::make_unique<WorkerRuntime>());
  }

  started_ = true;
  auto fn = [this](unsigned i, Worker& worker) {
    return RunWorker(*runtimes_[i], i, worker);
  };
  runtime_.Start(options_.thread_count, std::move(fn));
  exit_code_ = runtime_.exit_code();
  return Status::Ok();
}

template <typename Handler>
void TcpServer<Handler>::RequestStop() noexcept {
  if (!started_) return;
  if (stop_requested_.exchange(true, std::memory_order_acq_rel)) return;

  for (const auto& r : runtimes_) {
    r->stop_requested.store(true, std::memory_order_release);
    r->listener.Close();
  }

  stop_thread_ = std::thread([this] {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(250);
    while (std::chrono::steady_clock::now() < deadline) {
      bool all_done = true;
      for (const auto& r : runtimes_) {
        if (!r->accept_loop_done.load(std::memory_order_acquire)) {
          all_done = false;
          break;
        }
      }
      if (all_done) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    runtime_.RequestStop();
  });
}

template <typename Handler>
void TcpServer<Handler>::WaitUntilStopped() {
  if (!started_) return;
  if (stop_thread_.joinable()) stop_thread_.join();
  runtime_.WaitUntilStopped();
  stopped_ = true;
}

template <typename Handler>
bool TcpServer<Handler>::started() const noexcept { return started_; }

template <typename Handler>
bool TcpServer<Handler>::stopped() const noexcept { return stopped_ || runtime_.stopped(); }

template <typename Handler>
int TcpServer<Handler>::completion_fd() const noexcept { return runtime_.completion_fd(); }

template <typename Handler>
int TcpServer<Handler>::exit_code() const noexcept { return exit_code_; }

template <typename Handler>
Task<Status> TcpServer<Handler>::RunSession(Worker& worker, Connection* connection) {
  TcpStream stream(connection);
  Status status = co_await handler_.HandleRequests(std::move(stream));

  if (connection != nullptr &&
      connection->state == ConnectionState::kActive &&
      !connection->closing) [[unlikely]] {
    if (status.ok()) {
      const CloseMode mode =
          connection->recv_eof ? CloseMode::kPeerClosed : CloseMode::kLocalClose;
      worker.BeginClose(connection, Status::Ok(), mode);
    } else {
      worker.BeginClose(connection, status, CloseMode::kLocalError);
    }
  }

  co_return status;
}

template <typename Handler>
Task<Status> TcpServer<Handler>::AcceptLoop(WorkerRuntime& rt, Worker& worker) {
  while (!rt.stop_requested.load(std::memory_order_acquire)) {
    auto accepted = co_await rt.listener.Accept();
    if (!accepted.ok()) [[unlikely]] {
      const auto code = accepted.status().code();
      if (rt.stop_requested.load(std::memory_order_acquire) ||
          code == StatusCode::kCancelled ||
          code == StatusCode::kFailedPrecondition) [[unlikely]] {
        rt.accept_loop_done.store(true, std::memory_order_release);
        co_return Status::Ok();
      }
      if (code != StatusCode::kUnavailable) {
        CELER_LOG_WARN << "accept failed: " << accepted.status().message();
      }
      continue;
    }

    worker.Spawn(RunSession(worker, *accepted));
  }

  rt.accept_loop_done.store(true, std::memory_order_release);
  co_return Status::Ok();
}

template <typename Handler>
int TcpServer<Handler>::RunWorker(WorkerRuntime& rt, unsigned index, Worker& worker) {
  WorkerOptions worker_options;
  worker_options.recv_mode = options_.recv_mode;
  worker_options.idle_timeout_ms = options_.idle_timeout_ms;
  worker_options.recv_buffer_count = options_.recv_buffer_count;

  auto init_status = worker.Init(worker_options);
  if (!init_status.ok()) [[unlikely]] {
    CELER_LOG_ERROR << "worker[" << index << "] init failed: " << init_status.message();
    return 1;
  }

  const bool reuse_port = options_.reuse_port && options_.thread_count > 1;
  auto bind_status = rt.listener.Bind(&worker, options_.bind_ip, options_.port,
                                      options_.backlog, reuse_port);
  if (!bind_status.ok()) [[unlikely]] {
    CELER_LOG_ERROR << "worker[" << index << "] bind failed: " << bind_status.message();
    worker.RequestStop();
    return 1;
  }

  worker.Spawn(AcceptLoop(rt, worker));
  worker.Run();

  if (!rt.stop_requested.load(std::memory_order_acquire)) [[unlikely]] {
    return 1;
  }
  return 0;
}

}  // namespace celer

#endif  // CELER_NET_TCP_SERVER_INL_H_
