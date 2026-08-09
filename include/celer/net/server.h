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

#ifndef CELER_NET_SERVER_H_
#define CELER_NET_SERVER_H_

#include <atomic>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/net/service.h"
#include "celer/runtime/runtime.h"
#include "celer/runtime/worker.h"

namespace celer {

struct ServerOptions {
  std::string bind_ip = "0.0.0.0";
  unsigned thread_count = 1;
  bool reuse_port = true;
  int idle_timeout_ms = -1;
  // Multishot recv buffer-ring entries. Zero uses per-connection one-shot recv.
  unsigned recv_buffer_count = 1024;
  unsigned ring_entries = 256;  // io_uring SQ ring size
  unsigned busy_poll_us = 0;
  unsigned foreground_budget_us = 1000;
  unsigned background_budget_us = 50;
  unsigned background_warrant_percent = 10;
};

// Hosts one or more Services on a pool of thread-per-core workers. The Runtime
// owns the worker threads; the Server only spawns each registered service's Run
// on every worker and coordinates shutdown. It is transport-agnostic — what a
// service does on a worker (TCP accept loop, UDP datagram loop, ...) is the
// service's business.
class Server {
 public:
  Server() = default;
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  ~Server();

  // Register a service before Start(). Not owned; must outlive the Server.
  void AddService(Service* service);

  absl::Status Start(const ServerOptions& options);
  void StopAccepting() noexcept;
  void RequestStop() noexcept;
  void WaitUntilStopped();

  bool started() const noexcept { return started_; }
  bool stopped() const noexcept;
  int completion_fd() const noexcept;
  int exit_code() const noexcept;

 private:
  int RunWorker(unsigned index, Worker& worker);

  Runtime runtime_;
  ServerOptions options_{};
  std::vector<Service*> services_;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> accepting_stopped_{false};
  std::atomic<unsigned> drained_workers_{0};
  bool started_ = false;
  bool stopped_ = false;
};

}  // namespace celer

#endif  // CELER_NET_SERVER_H_
