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

#include "celer/net/tcp_server.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "celer/base/log.h"
#include "celer/net/connection.h"
#include "celer/net/tcp_listener.h"
#include "celer/runtime/worker.h"

namespace celer {

namespace {

struct WorkerServerRuntime {
  TcpListener listener;
  std::atomic<bool> stop_requested = false;
  std::atomic<bool> accept_loop_done = false;

  void BeginShutdown() {
    stop_requested.store(true, std::memory_order_release);
    listener.Close();
  }
};

}  // namespace

class TcpServerImpl {
 public:
  ~TcpServerImpl() {
    RequestStop();
    WaitUntilStopped();
  }

  Status Start(const TcpServerOptions& options, TcpConnectionHandler* handler) {
    if (started_) [[unlikely]] {
      return Status(StatusCode::kFailedPrecondition, "server already started");
    }
    if (handler == nullptr) [[unlikely]] {
      return Status(StatusCode::kInvalidArgument, "handler must not be null");
    }
    if (options.thread_count == 0) [[unlikely]] {
      return Status(StatusCode::kInvalidArgument, "thread_count must be >= 1");
    }
    if (options.port == 0) [[unlikely]] {
      return Status(StatusCode::kInvalidArgument, "port must be non-zero");
    }

    options_ = options;
    handler_ = handler;
    worker_runtimes_.reserve(options.thread_count);
    for (unsigned i = 0; i < options.thread_count; ++i) {
      worker_runtimes_.push_back(std::make_unique<WorkerServerRuntime>());
    }

    runtime_.Start(options.thread_count, [this](unsigned i, Worker& worker) {
      return RunServerWorker(*worker_runtimes_[i], i, worker);
    });
    started_ = true;
    return Status::Ok();
  }

  void RequestStop() noexcept {
    if (!started_) {
      return;
    }
    if (stop_requested_.exchange(true, std::memory_order_acq_rel)) [[unlikely]] {
      return;
    }

    for (const auto& runtime : worker_runtimes_) {
      runtime->BeginShutdown();
    }

    stop_thread_ = std::thread([this] {
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(250);
      while (std::chrono::steady_clock::now() < deadline) {
        bool all_accept_loops_done = true;
        for (const auto& runtime : worker_runtimes_) {
          if (!runtime->accept_loop_done.load(std::memory_order_acquire)) {
            all_accept_loops_done = false;
            break;
          }
        }
        if (all_accept_loops_done) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      runtime_.RequestStop();
    });
  }

  void WaitUntilStopped() {
    if (!started_) {
      return;
    }
    if (stop_thread_.joinable()) {
      stop_thread_.join();
    }
    runtime_.WaitUntilStopped();
    stopped_ = true;
  }

  bool started() const noexcept {
    return started_;
  }

  bool stopped() const noexcept {
    return stopped_ || runtime_.stopped();
  }

  int completion_fd() const noexcept {
    return runtime_.completion_fd();
  }

  int exit_code() const noexcept {
    return runtime_.exit_code();
  }

 private:
  Task<Status> RunHandlerSession(Worker& worker, Connection* connection) {
    TcpStream stream(connection);
    Status status = co_await handler_->HandleRequests(std::move(stream));

    if (connection != nullptr &&
        connection->state == ConnectionState::kActive &&
        !connection->closing) [[unlikely]] {
      if (status.ok()) {
        const CloseMode close_mode =
            connection->recv_eof ? CloseMode::kPeerClosed : CloseMode::kLocalClose;
        worker.BeginClose(connection, Status::Ok(), close_mode);
      } else {
        worker.BeginClose(connection, status, CloseMode::kLocalError);
      }
    }

    co_return status;
  }

  Task<Status> AcceptLoop(WorkerServerRuntime& runtime, Worker& worker) {
    while (!runtime.stop_requested.load(std::memory_order_acquire)) {
      auto accepted = co_await runtime.listener.Accept();
      if (!accepted.ok()) [[unlikely]] {
        const auto code = accepted.status().code();
        if (runtime.stop_requested.load(std::memory_order_acquire) ||
            code == StatusCode::kCancelled ||
            code == StatusCode::kFailedPrecondition) [[unlikely]] {
          runtime.accept_loop_done.store(true, std::memory_order_release);
          co_return Status::Ok();
        }
        if (code != StatusCode::kUnavailable) {
          CELER_LOG_WARN << "accept failed: " << accepted.status().message();
        }
        continue;
      }

      worker.Spawn(RunHandlerSession(worker, *accepted));
    }

    runtime.accept_loop_done.store(true, std::memory_order_release);
    co_return Status::Ok();
  }

  int RunServerWorker(WorkerServerRuntime& runtime, unsigned worker_index, Worker& worker) {
    WorkerOptions worker_options;
    worker_options.recv_mode = options_.recv_mode;
    worker_options.idle_timeout_ms = options_.idle_timeout_ms;

    auto init_status = worker.Init(worker_options);
    if (!init_status.ok()) [[unlikely]] {
      CELER_LOG_ERROR << "worker[" << worker_index
                      << "] init failed: " << init_status.message();
      return 1;
    }

    const bool reuse_port = options_.reuse_port && options_.thread_count > 1;
    auto bind_status = runtime.listener.Bind(&worker, options_.bind_ip, options_.port,
                                             options_.backlog, reuse_port);
    if (!bind_status.ok()) [[unlikely]] {
      CELER_LOG_ERROR << "worker[" << worker_index
                      << "] bind failed: " << bind_status.message();
      worker.RequestStop();
      return 1;
    }

    worker.Spawn(AcceptLoop(runtime, worker));
    worker.Run();

    if (!runtime.stop_requested.load(std::memory_order_acquire)) [[unlikely]] {
      return 1;
    }
    return 0;
  }

  Runtime runtime_;
  TcpServerOptions options_{};
  TcpConnectionHandler* handler_ = nullptr;
  std::vector<std::unique_ptr<WorkerServerRuntime>> worker_runtimes_;
  std::atomic<bool> stop_requested_{false};
  std::thread stop_thread_;
  bool started_ = false;
  bool stopped_ = false;
};

TcpServer::TcpServer() = default;

TcpServer::TcpServer(TcpServer&&) noexcept = default;

TcpServer& TcpServer::operator=(TcpServer&&) noexcept = default;

TcpServer::~TcpServer() {
  RequestStop();
  WaitUntilStopped();
}

Status TcpServer::Start(const TcpServerOptions& options, TcpConnectionHandler* handler) {
  if (!impl_) {
    impl_ = std::make_unique<TcpServerImpl>();
  }
  return impl_->Start(options, handler);
}

void TcpServer::RequestStop() noexcept {
  if (impl_) {
    impl_->RequestStop();
  }
}

void TcpServer::WaitUntilStopped() {
  if (impl_) {
    impl_->WaitUntilStopped();
  }
}

bool TcpServer::started() const noexcept {
  return impl_ && impl_->started();
}

bool TcpServer::stopped() const noexcept {
  return impl_ && impl_->stopped();
}

int TcpServer::completion_fd() const noexcept {
  return impl_ ? impl_->completion_fd() : -1;
}

int TcpServer::exit_code() const noexcept {
  return impl_ ? impl_->exit_code() : 0;
}

}  // namespace celer
