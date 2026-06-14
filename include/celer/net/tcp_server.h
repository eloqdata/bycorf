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

#ifndef CELER_NET_TCP_SERVER_H_
#define CELER_NET_TCP_SERVER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "celer/base/status.h"
#include "celer/net/tcp_listener.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/runtime.h"
#include "celer/runtime/task.h"
#include "celer/runtime/worker.h"

namespace celer {

struct TcpServerOptions {
  std::string bind_ip = "127.0.0.1";
  std::uint16_t port = 0;
  unsigned thread_count = 1;
  int backlog = 128;
  int idle_timeout_ms = -1;
  unsigned recv_buffer_count = 1024;  // multishot recv buffer-ring entries
  bool reuse_port = true;
  RecvMode recv_mode = kDefaultRecvMode;
};

// Handler concept: must have Task<Status> HandleRequests(TcpStream).

template <typename Handler>
class TcpServer {
 public:
  TcpServer() = default;
  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;
  TcpServer(TcpServer&&) noexcept = default;
  TcpServer& operator=(TcpServer&&) noexcept = default;

  ~TcpServer();

  Status Start(const TcpServerOptions& options, Handler handler);
  void RequestStop() noexcept;
  void WaitUntilStopped();

  bool started() const noexcept;
  bool stopped() const noexcept;
  int completion_fd() const noexcept;
  int exit_code() const noexcept;

 private:
  struct WorkerRuntime {
    TcpListener listener;
    std::atomic<bool> stop_requested = false;
    std::atomic<bool> accept_loop_done = false;
  };

  Task<Status> RunSession(Worker& worker, Connection* connection);
  Task<Status> AcceptLoop(WorkerRuntime& rt, Worker& worker);
  int RunWorker(WorkerRuntime& rt, unsigned index, Worker& worker);

  Runtime runtime_;
  TcpServerOptions options_{};
  Handler handler_{};
  std::vector<std::unique_ptr<WorkerRuntime>> runtimes_;
  std::atomic<bool> stop_requested_{false};
  std::thread stop_thread_;
  int exit_code_ = 0;
  bool started_ = false;
  bool stopped_ = false;
};

}  // namespace celer

#endif  // CELER_NET_TCP_SERVER_H_
