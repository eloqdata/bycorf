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

#ifndef BYCORF_NET_SERVER_H_
#define BYCORF_NET_SERVER_H_

#include <atomic>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/net/service.h"
#include "bycorf/runtime/runtime.h"
#include "bycorf/runtime/worker.h"

namespace bycorf {

struct ServerOptions {
  std::string bind_ip_ = "0.0.0.0";
  // When non-empty, bind every service endpoint on every listed address.
  // Entries may be IPv4, IPv6, hostnames, or "*".
  std::vector<std::string> bind_addresses_;
  unsigned thread_count_ = 1;
  // Pin worker i to the i-th CPU in the process's inherited affinity mask.
  bool pin_workers_ = true;
  bool reuse_port_ = true;
  int idle_timeout_ms_ = -1;
  // Multishot recv buffer-ring entries. Zero uses per-connection one-shot recv.
  unsigned recv_buffer_count_ = 1024;
  unsigned ring_entries_ = 256;  // io_uring SQ ring size
  unsigned busy_poll_us_ = 0;
  unsigned foreground_budget_us_ = 1000;
  unsigned background_budget_us_ = 50;
  unsigned background_warrant_percent_ = 10;
  unsigned spdk_max_completions_per_poll_ = 0;
  unsigned spdk_foreground_pre_poll_us_ = 0;
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
  std::atomic<unsigned> reclaimed_workers_{0};
  bool started_ = false;
  bool stopped_ = false;
};

}  // namespace bycorf

#endif  // BYCORF_NET_SERVER_H_
