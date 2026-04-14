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

#ifndef CELER_NET_TCP_LISTENER_H_
#define CELER_NET_TCP_LISTENER_H_

#include <cstdint>
#include <string_view>

#include "celer/base/status.h"
#include "celer/net/connection.h"
#include "celer/runtime/task.h"

namespace celer {

class Worker;
class PollReadableOperation;

class TcpListener {
 public:
  TcpListener() = default;
  explicit TcpListener(Worker* worker) : worker_(worker) {}

  TcpListener(TcpListener&&) noexcept = default;
  TcpListener& operator=(TcpListener&&) noexcept = default;

  TcpListener(const TcpListener&) = delete;
  TcpListener& operator=(const TcpListener&) = delete;

  bool IsOpen() const noexcept;
  int NativeFd() const noexcept;

  Status Bind(Worker* worker, std::string_view ip, std::uint16_t port, int backlog = 128,
              bool reuse_port = false);
  Task<StatusOr<Connection*>> Accept();
  Status Close() noexcept;

 private:
  friend class PollReadableOperation;

  Worker* worker_ = nullptr;
  int fd_ = -1;
  bool closed_ = true;
  std::uint64_t next_generation_ = 1;
};

}  // namespace celer

#endif  // CELER_NET_TCP_LISTENER_H_
