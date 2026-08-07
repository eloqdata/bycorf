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

#include "celer/net/tcp_service.h"

#include <unistd.h>

#include "spdlog/spdlog.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"

namespace celer {

void TcpService::Prepare(unsigned thread_count) {
  thread_count_ = thread_count;
  next_connection_worker_.store(0, std::memory_order_relaxed);
  listeners_.clear();
  listeners_.reserve(thread_count);
  for (unsigned i = 0; i < thread_count; ++i) {
    listeners_.push_back(std::make_unique<TcpListener>());
  }
}

Status TcpService::StartSession(Worker& worker, Connection connection) {
  const int fd = connection.file.fd;
  if (fd < 0) {
    return Status(StatusCode::kInvalidArgument,
                  "accepted connection has an invalid fd");
  }
  if (worker.stop_requested()) {
    ::close(fd);
    return Status(StatusCode::kCancelled,
                  "target worker is stopping");
  }

  connection.worker = &worker;
  Connection* registered = worker.AddConnection(std::move(connection));
  if (registered == nullptr) {
    ::close(fd);
    return Status(StatusCode::kInternal,
                  "failed to register accepted connection");
  }
  worker.Spawn(RunSession(worker, registered));
  return Status::Ok();
}

void TcpService::Stop() noexcept {
  // Called from the Server's thread at shutdown; closing the listeners unblocks
  // the accept loops (their multishot accept completes with -ECANCELED).
  for (auto& listener : listeners_) {
    if (listener) {
      listener->Close();
    }
  }
}

Task<Status> TcpService::Run(Worker& worker, ServiceContext ctx) {
  TcpListener& listener = *listeners_[worker.id()];
  auto bind_status =
      listener.Bind(&worker, ctx.bind_ip, port_, backlog_, ctx.reuse_port);
  if (!bind_status.ok()) [[unlikely]] {
    spdlog::error("worker[{}] bind :{} failed: {}", worker.id(), port_,
                  bind_status.message());
    worker.RequestStop();
    co_return bind_status;
  }

  while (!worker.stop_requested()) {
    auto accepted = co_await listener.AcceptUnregistered();
    if (!accepted.ok()) [[unlikely]] {
      const auto code = accepted.status().code();
      if (worker.stop_requested() || code == StatusCode::kCancelled ||
          code == StatusCode::kFailedPrecondition) [[unlikely]] {
        break;
      }
      if (code != StatusCode::kUnavailable) {
        spdlog::warn("accept failed: {}", accepted.status().message());
      }
      continue;
    }

    const unsigned target = static_cast<unsigned>(
        next_connection_worker_.fetch_add(1, std::memory_order_relaxed) %
        thread_count_);
    Status started = co_await SubmitTo(
        target,
        [this, connection = std::move(*accepted)]() mutable -> Status {
          return StartSession(*ThisWorker().self, std::move(connection));
        });
    if (!started.ok() &&
        started.code() != StatusCode::kCancelled) [[unlikely]] {
      spdlog::warn("failed to start accepted connection on worker[{}]: {}",
                   target, started.message());
    }
  }

  co_return Status::Ok();
}

Task<Status> TcpService::RunSession(Worker& worker, Connection* connection) {
  TcpStream stream(connection);
  Status status = co_await Serve(std::move(stream));

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

}  // namespace celer
