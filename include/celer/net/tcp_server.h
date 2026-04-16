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

#include <cstdint>
#include <string>

#include "celer/base/status.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/runtime.h"

namespace celer {

class TcpServerImpl;

struct TcpServerOptions {
  std::string bind_ip = "127.0.0.1";
  std::uint16_t port = 0;
  unsigned thread_count = 1;
  int backlog = 128;
  int idle_timeout_ms = -1;
  bool reuse_port = true;
  RecvMode recv_mode = kDefaultRecvMode;
};

class TcpConnectionHandler {
 public:
  virtual ~TcpConnectionHandler() = default;
  virtual Task<Status> HandleRequests(TcpStream stream) = 0;
};

class TcpServer {
 public:
  TcpServer();
  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;
  TcpServer(TcpServer&&) noexcept;
  TcpServer& operator=(TcpServer&&) noexcept;
  ~TcpServer();

  Status Start(const TcpServerOptions& options, TcpConnectionHandler* handler);
  void RequestStop() noexcept;
  void WaitUntilStopped();

  bool started() const noexcept;
  bool stopped() const noexcept;
  int completion_fd() const noexcept;
  int exit_code() const noexcept;

 private:
  std::unique_ptr<TcpServerImpl> impl_;
};

}  // namespace celer

#endif  // CELER_NET_TCP_SERVER_H_
