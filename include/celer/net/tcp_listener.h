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

#include <sys/socket.h>

#include <coroutine>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/io/completion.h"
#include "celer/net/connection.h"
#include "celer/runtime/task.h"

namespace celer {

class Worker;
class TcpListener;

// Allocation-free awaitable for one accepted, not-yet-registered socket. It
// is embedded in the caller's coroutine frame rather than allocating a nested
// Task frame for every accepted connection.
class AcceptUnregisteredAwaitable final {
 public:
  explicit AcceptUnregisteredAwaitable(TcpListener* listener) noexcept
      : listener_(listener) {}

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::StatusOr<Connection> await_resume();

 private:
  TcpListener* listener_ = nullptr;
  std::optional<absl::Status> immediate_status_;
};

struct ResolvedTcpAddress {
  sockaddr_storage address_{};
  socklen_t length_ = 0;
  std::string display_;
};

absl::StatusOr<std::vector<ResolvedTcpAddress>> ResolveTcpAddresses(
    std::string_view host, std::uint16_t port);

class ListenerAcceptState final : public IoCompletion {
 public:
  ListenerAcceptState() = default;
  explicit ListenerAcceptState(TcpListener* listener) : listener_(listener) {}

  void Bind(TcpListener* listener) noexcept { listener_ = listener; }
  absl::Status Arm();
  absl::StatusOr<int> ConsumeAcceptedFd();
  void Complete(Worker& worker, int result, unsigned flags) override;
  void SetWaiter(std::coroutine_handle<> awaiting) { waiter_ = awaiting; }
  bool HasWaiter() const noexcept { return static_cast<bool>(waiter_); }
  bool HasAcceptedFd() const noexcept { return !accepted_fds_.empty(); }
  bool HasError() const noexcept { return !last_error_.ok(); }
  void CloseAllAcceptedFds() noexcept;

 private:
  TcpListener* listener_ = nullptr;
  std::deque<int> accepted_fds_;
  std::coroutine_handle<> waiter_{};
  absl::Status last_error_ = absl::OkStatus();
  bool armed_ = false;
};

class TcpListener {
 public:
  TcpListener() = default;
  explicit TcpListener(Worker* worker) : worker_(worker) {}

  TcpListener(TcpListener&&) noexcept = delete;
  TcpListener& operator=(TcpListener&&) noexcept = delete;

  TcpListener(const TcpListener&) = delete;
  TcpListener& operator=(const TcpListener&) = delete;

  bool IsOpen() const noexcept;
  int NativeFd() const noexcept;

  absl::Status Bind(Worker* worker, std::string_view ip, std::uint16_t port,
                    int backlog = 128, bool reuse_port = false);
  absl::Status Bind(Worker* worker, const ResolvedTcpAddress& address,
                    int backlog = 128, bool reuse_port = false);
  // Accept a socket without registering it with the accepting worker. This is
  // used by TcpService to hand a fresh socket to its selected owner before any
  // recv operation is armed.
  AcceptUnregisteredAwaitable AcceptUnregistered() noexcept;
  Task<absl::StatusOr<Connection*>> Accept();
  absl::Status Close() noexcept;

 private:
  friend class ListenerAcceptState;
  friend class AcceptUnregisteredAwaitable;

  Worker* worker_ = nullptr;
  int fd_ = -1;
  bool closed_ = true;
  std::uint64_t next_generation_ = 1;
  ListenerAcceptState accept_state_{this};
};

}  // namespace celer

#endif  // CELER_NET_TCP_LISTENER_H_
