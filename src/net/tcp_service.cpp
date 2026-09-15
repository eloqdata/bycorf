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

#include <unordered_set>

#include "absl/cleanup/cleanup.h"
#include "celer/net/socket_ops.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
#include "spdlog/spdlog.h"

namespace celer {

void TcpService::AddTlsEndpoint(std::uint16_t port,
                                std::shared_ptr<TlsContext> context) {
  if (port != 0 && context != nullptr) {
    endpoints_.push_back(Endpoint{port, std::move(context)});
  }
}

void TcpService::Prepare(unsigned thread_count) {
  thread_count_ = thread_count;
  next_connection_worker_.store(0, std::memory_order_relaxed);
  listeners_.clear();
  listeners_.resize(thread_count);
#ifdef CELER_WITH_DPDK
  stopping_ = false;
  control_executors_.resize(thread_count);
#endif
}

absl::Status TcpService::StartSession(Worker& worker, Connection connection,
                                      std::shared_ptr<TlsContext> tls) {
  const int fd = connection.file_.fd_;
  if (fd < 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "accepted connection has an invalid fd");
  }
  if (worker.stop_requested()) {
    detail::CloseSocket(fd);
    return absl::Status(absl::StatusCode::kCancelled,
                        "target worker is stopping");
  }

  connection.worker_ = &worker;
  Connection* registered = worker.AddConnection(std::move(connection));
  if (registered == nullptr) {
    detail::CloseSocket(fd);
    return absl::Status(absl::StatusCode::kInternal,
                        "failed to register accepted connection");
  }
  worker.Spawn(RunSession(worker, registered, std::move(tls)));
  return absl::OkStatus();
}

void TcpService::Stop() noexcept {
#ifdef CELER_WITH_DPDK
  std::lock_guard lock(stop_mutex_);
  stopping_ = true;
  // Native BSD socket operations must execute in their owner's VNET. The
  // existing foreign ingress handles both idle-ring wakeup and publication.
  for (unsigned i = 0; i < control_executors_.size(); ++i) {
    if (control_executors_[i].valid()) {
      (void)control_executors_[i].Notify([this, i]() noexcept {
        for (auto& bound : listeners_[i].values_)
          if (bound.listener_) bound.listener_->Close().IgnoreError();
      });
    }
  }
#else
  // Called from the Server's thread at shutdown; closing the listeners unblocks
  // the accept loops (their multishot accept completes with -ECANCELED).
  for (auto& worker : listeners_) {
    for (auto& bound : worker.values_) {
      if (bound.listener_) {
        bound.listener_->Close().IgnoreError();
      }
    }
  }
#endif
}

Task<absl::Status> TcpService::Run(Worker& worker, ServiceContext ctx) {
#ifdef CELER_WITH_DPDK
  {
    std::lock_guard lock(stop_mutex_);
    if (stopping_) co_return absl::OkStatus();
    control_executors_[worker.id()] = ctx.control_executor_;
  }
#endif
  WorkerListeners& owned = listeners_[worker.id()];
  std::unordered_set<std::string> seen;
  for (const Endpoint& endpoint : endpoints_) {
    for (const std::string& host : ctx.bind_addresses_) {
      auto addresses = ResolveTcpAddresses(host, endpoint.port_);
      if (!addresses.ok()) {
        spdlog::error("worker[{}] resolve {}:{} failed: {}", worker.id(), host,
                      endpoint.port_, addresses.status().message());
        worker.RequestStop();
        co_return addresses.status();
      }
      for (const ResolvedTcpAddress& address : *addresses) {
        const std::string key =
            address.display_ + (endpoint.tls_ == nullptr ? "/plain" : "/tls");
        if (!seen.insert(key).second) continue;
        auto listener = std::make_unique<TcpListener>();
        absl::Status bound =
            listener->Bind(&worker, address, backlog_, ctx.reuse_port_);
        if (!bound.ok()) {
          spdlog::error("worker[{}] bind {} failed: {}", worker.id(),
                        address.display_, bound.message());
          worker.RequestStop();
          co_return bound;
        }
        owned.values_.push_back(BoundListener{
            .listener_ = std::move(listener),
            .tls_ = endpoint.tls_,
            .display_ = address.display_,
        });
      }
    }
  }
  if (owned.values_.empty()) {
    worker.RequestStop();
    co_return absl::FailedPreconditionError(
        "TCP service has no configured endpoints");
  }
  for (std::size_t i = 1; i < owned.values_.size(); ++i) {
    worker.Spawn(AcceptLoop(worker, &owned.values_[i]));
  }
  co_return co_await AcceptLoop(worker, &owned.values_.front());
}

Task<absl::Status> TcpService::AcceptLoop(Worker& worker,
                                          BoundListener* bound) {
  TcpListener& listener = *bound->listener_;
  while (!worker.stop_requested()) {
    auto accepted = co_await listener.AcceptUnregistered();
    if (!accepted.ok()) [[unlikely]] {
      const auto code = accepted.status().code();
      if (worker.stop_requested() || code == absl::StatusCode::kCancelled ||
          code == absl::StatusCode::kFailedPrecondition) [[unlikely]] {
        break;
      }
      if (code != absl::StatusCode::kUnavailable) {
        spdlog::warn("accept failed: {}", accepted.status().message());
      }
      continue;
    }

    const int fd = accepted->file_.fd_;
    if (!AdmitConnection(fd, bound->tls_ != nullptr)) {
      detail::CloseSocket(fd);
      accepted->file_.fd_ = -1;
      continue;
    }

    // DPDK packet steering already chooses a stable connection owner. A BSD
    // socket cannot follow the kernel backend's accept-time fd redistribution.
    const unsigned target =
        detail::IsDpdkSocket(fd)
            ? worker.id()
            : static_cast<unsigned>(next_connection_worker_.fetch_add(
                                        1, std::memory_order_relaxed) %
                                    thread_count_);
    absl::Status started = co_await SubmitTo(
        target,
        [this, connection = std::move(*accepted),
         tls = bound->tls_]() mutable -> absl::Status {
          return StartSession(*ThisWorker().self_, std::move(connection),
                              std::move(tls));
        });
    if (!started.ok() && started.code() != absl::StatusCode::kCancelled)
        [[unlikely]] {
      spdlog::warn("failed to start accepted connection on worker[{}]: {}",
                   target, started.message());
    }
    if (!started.ok()) OnConnectionClosed();
  }

  co_return absl::OkStatus();
}

Task<absl::Status> TcpService::RunSession(Worker& worker,
                                          Connection* connection,
                                          std::shared_ptr<TlsContext> tls) {
  auto connection_slot = absl::MakeCleanup([this] { OnConnectionClosed(); });
  TcpStream stream(connection);
  absl::Status status;
  if (tls != nullptr) {
    status = co_await stream.StartTls(tls, true);
  }
  if (status.ok()) {
    status = co_await Serve(std::move(stream));
  }

  if (status.ok() && connection != nullptr &&
      connection->state_ == ConnectionState::kActive && !connection->closing_) {
    TcpStream closing_stream(connection);
    absl::Status shutdown = co_await closing_stream.ShutdownTls();
    if (!shutdown.ok()) status = shutdown;
  }

  if (connection != nullptr && connection->state_ == ConnectionState::kActive &&
      !connection->closing_) [[unlikely]] {
    if (status.ok()) {
      const CloseMode mode = connection->recv_eof_ ? CloseMode::kPeerClosed
                                                   : CloseMode::kLocalClose;
      worker.BeginClose(connection, absl::OkStatus(), mode);
    } else {
      worker.BeginClose(connection, status, CloseMode::kLocalError);
    }
  }

  co_return status;
}

}  // namespace celer
