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

#ifndef CELER_NET_TCP_SERVICE_H_
#define CELER_NET_TCP_SERVICE_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/net/connection.h"
#include "celer/net/service.h"
#include "celer/net/tcp_listener.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/task.h"

namespace celer {

class Worker;

// A Service that listens on one TCP port and serves each accepted connection.
// It owns a listener per worker (bound with SO_REUSEPORT) and runs the accept
// loop; subclasses implement the per-connection protocol in Serve(). Concrete
// protocols (e.g. Redis) derive from this and only override Serve.
class TcpService : public Service {
 public:
  explicit TcpService(std::uint16_t port, int backlog = 128)
      : port_(port), backlog_(backlog) {}

  std::uint16_t port() const noexcept { return port_; }

  void Prepare(unsigned thread_count) override;
  Task<absl::Status> Run(Worker& worker, ServiceContext ctx) override;
  void Stop() noexcept override;

 protected:
  // Handles one accepted connection. The framework closes the connection after
  // this returns (unless already closed by the protocol).
  virtual Task<absl::Status> Serve(TcpStream stream) = 0;

 private:
  absl::Status StartSession(Worker& worker, Connection connection);
  Task<absl::Status> RunSession(Worker& worker, Connection* connection);

  std::uint16_t port_;
  int backlog_;
  std::vector<std::unique_ptr<TcpListener>> listeners_;  // one per worker, by id
  unsigned thread_count_ = 0;
  std::atomic<std::uint64_t> next_connection_worker_{0};

  // TODO: If long-lived connections develop uneven workloads after this
  // accept-time placement, add request-boundary live migration. It must first
  // cancel and drain the old worker's multishot recv and provided buffers.
};

}  // namespace celer

#endif  // CELER_NET_TCP_SERVICE_H_
