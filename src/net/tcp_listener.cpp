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

#include "celer/net/tcp_listener.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

#include "celer/io/completion.h"
#include "celer/runtime/worker.h"

namespace celer {

namespace {

absl::Status ErrnoToStatus(int err, const char* operation) {
  std::string message = std::string(operation) + ": " + std::strerror(err) +
                        " (errno=" + std::to_string(err) + ")";
  switch (err) {
    case EAGAIN:
      return absl::Status(absl::StatusCode::kUnavailable, std::move(message));
    case ETIMEDOUT:
      return absl::Status(absl::StatusCode::kDeadlineExceeded,
                          std::move(message));
    case ECANCELED:
      return absl::Status(absl::StatusCode::kCancelled, std::move(message));
    case EINVAL:
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          std::move(message));
    case EBADF:
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          std::move(message));
    case EMFILE:
    case ENFILE:
      return absl::Status(absl::StatusCode::kResourceExhausted,
                          std::move(message));
    default:
      return absl::Status(absl::StatusCode::kUnknown, std::move(message));
  }
}

}  // namespace

absl::StatusOr<std::vector<ResolvedTcpAddress>> ResolveTcpAddresses(
    std::string_view host, std::uint16_t port) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_PASSIVE;

  const std::string host_text(host);
  const char* node = host.empty() || host == "*" ? nullptr : host_text.c_str();
  const std::string service = std::to_string(port);
  addrinfo* addresses = nullptr;
  const int result =
      ::getaddrinfo(node, service.c_str(), &hints, &addresses);
  if (result != 0) {
    return absl::InvalidArgumentError(
        std::string("cannot resolve bind address '") + host_text +
        "': " + ::gai_strerror(result));
  }

  std::vector<ResolvedTcpAddress> resolved;
  std::unordered_set<std::string> seen;
  for (addrinfo* current = addresses; current != nullptr;
       current = current->ai_next) {
    if ((current->ai_family != AF_INET && current->ai_family != AF_INET6) ||
        current->ai_addrlen > sizeof(sockaddr_storage)) {
      continue;
    }
    char numeric_host[NI_MAXHOST]{};
    char numeric_service[NI_MAXSERV]{};
    if (::getnameinfo(current->ai_addr, current->ai_addrlen, numeric_host,
                      sizeof(numeric_host), numeric_service,
                      sizeof(numeric_service),
                      NI_NUMERICHOST | NI_NUMERICSERV) != 0) {
      continue;
    }
    const std::string key = std::to_string(current->ai_family) + ":" +
                            numeric_host + ":" + numeric_service;
    if (!seen.insert(key).second) continue;

    ResolvedTcpAddress address;
    std::memcpy(&address.address_, current->ai_addr, current->ai_addrlen);
    address.length_ = static_cast<socklen_t>(current->ai_addrlen);
    address.display_ = current->ai_family == AF_INET6
                           ? std::string("[") + numeric_host + "]:" +
                                 numeric_service
                           : std::string(numeric_host) + ":" + numeric_service;
    resolved.push_back(std::move(address));
  }
  ::freeaddrinfo(addresses);
  if (resolved.empty()) {
    return absl::InvalidArgumentError(
        std::string("bind address has no IPv4 or IPv6 results: ") + host_text);
  }
  return resolved;
}

absl::Status ListenerAcceptState::Arm() {
  if (armed_) {
    return absl::OkStatus();
  }
  if (listener_ == nullptr || listener_->fd_ < 0) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "listener is closed");
  }
  if (listener_->worker_ == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "listener is not bound");
  }

  auto status = listener_->worker_->SubmitAcceptMultishot(listener_->fd_, this);
  if (!status.ok()) {
    return status;
  }
  armed_ = true;
  return absl::OkStatus();
}

absl::StatusOr<int> ListenerAcceptState::ConsumeAcceptedFd() {
  if (!accepted_fds_.empty()) {
    const int fd = accepted_fds_.front();
    accepted_fds_.pop_front();
    return fd;
  }

  if (!last_error_.ok()) {
    absl::Status status = last_error_;
    last_error_ = absl::OkStatus();
    return status;
  }

  return absl::Status(absl::StatusCode::kUnavailable,
                      "no accepted connection available");
}

void ListenerAcceptState::Complete(Worker& worker, int result, unsigned flags) {
  if (result >= 0) {
    accepted_fds_.push_back(result);
  } else if (result != -ECANCELED && result != -EBADF) {
    last_error_ = ErrnoToStatus(-result, "accept failed");
  } else if (listener_ != nullptr && listener_->closed_) {
    last_error_ = absl::Status(absl::StatusCode::kFailedPrecondition,
                               "listener is closed");
  }

  if ((flags & kCompletionMore) == 0) {
    armed_ = false;
    if (listener_ != nullptr && !listener_->closed_) {
      auto status = Arm();
      if (!status.ok()) {
        last_error_ = status;
      }
    }
  }

  if (waiter_) {
    auto waiter = waiter_;
    waiter_ = {};
    worker.Enqueue(waiter);
  }
}

void ListenerAcceptState::CloseAllAcceptedFds() noexcept {
  while (!accepted_fds_.empty()) {
    ::close(accepted_fds_.front());
    accepted_fds_.pop_front();
  }
}

bool AcceptUnregisteredAwaitable::await_suspend(
    std::coroutine_handle<> awaiting) {
  if (listener_ == nullptr) {
    immediate_status_ = absl::Status(absl::StatusCode::kInvalidArgument,
                                     "listener is not bound");
    return false;
  }
  if (listener_->fd_ < 0 || listener_->closed_) {
    immediate_status_ = absl::Status(absl::StatusCode::kFailedPrecondition,
                                     "listener is closed");
    return false;
  }
  if (listener_->worker_ == nullptr) {
    immediate_status_ = absl::Status(absl::StatusCode::kInvalidArgument,
                                     "listener is not bound");
    return false;
  }

  auto& state = listener_->accept_state_;
  if (state.HasAcceptedFd() || state.HasError()) {
    return false;
  }
  if (state.HasWaiter()) {
    immediate_status_ = absl::Status(absl::StatusCode::kFailedPrecondition,
                                     "concurrent accept is not allowed");
    return false;
  }

  const auto arm_status = state.Arm();
  if (!arm_status.ok()) {
    immediate_status_ = arm_status;
    return false;
  }

  state.SetWaiter(awaiting);
  return true;
}

absl::StatusOr<Connection> AcceptUnregisteredAwaitable::await_resume() {
  if (immediate_status_.has_value()) {
    return *immediate_status_;
  }
  auto accepted = listener_->accept_state_.ConsumeAcceptedFd();
  if (!accepted.ok()) return accepted.status();

  Connection connection;
  connection.file_.fd_ = *accepted;
  connection.closed_ = false;
  connection.generation_ = listener_->next_generation_++;
  return connection;
}

bool TcpListener::IsOpen() const noexcept { return !closed_ && fd_ >= 0; }

int TcpListener::NativeFd() const noexcept { return fd_; }

absl::Status TcpListener::Bind(Worker* worker, std::string_view ip,
                               std::uint16_t port, int backlog,
                               bool reuse_port) {
  auto addresses = ResolveTcpAddresses(ip, port);
  if (!addresses.ok()) return addresses.status();
  return Bind(worker, addresses->front(), backlog, reuse_port);
}

absl::Status TcpListener::Bind(Worker* worker,
                               const ResolvedTcpAddress& address, int backlog,
                               bool reuse_port) {
  if (IsOpen()) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "listener is already open");
  }
  if (worker == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "worker must not be null");
  }

  const int family = address.address_.ss_family;
  if (family != AF_INET && family != AF_INET6) {
    return absl::InvalidArgumentError("unsupported bind address family");
  }
  const int fd = ::socket(family, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return ErrnoToStatus(errno, "socket failed");
  }

  int reuse = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
    const auto status = ErrnoToStatus(errno, "setsockopt(SO_REUSEADDR) failed");
    ::close(fd);
    return status;
  }
  if (reuse_port &&
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) != 0) {
    const auto status = ErrnoToStatus(errno, "setsockopt(SO_REUSEPORT) failed");
    ::close(fd);
    return status;
  }

  if (family == AF_INET6) {
    int ipv6_only = 1;
    if (::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &ipv6_only,
                     sizeof(ipv6_only)) != 0) {
      const auto status = ErrnoToStatus(errno, "setsockopt(IPV6_V6ONLY) failed");
      ::close(fd);
      return status;
    }
  }

  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address.address_),
             address.length_) != 0) {
    const auto status = ErrnoToStatus(errno, "bind failed");
    ::close(fd);
    return status;
  }

  if (::listen(fd, backlog) != 0) {
    const auto status = ErrnoToStatus(errno, "listen failed");
    ::close(fd);
    return status;
  }

  const int current_flags = ::fcntl(fd, F_GETFL, 0);
  if (current_flags < 0 ||
      ::fcntl(fd, F_SETFL, current_flags | O_NONBLOCK) != 0) {
    const auto status = ErrnoToStatus(errno, "fcntl(O_NONBLOCK) failed");
    ::close(fd);
    return status;
  }

  worker_ = worker;
  fd_ = fd;
  closed_ = false;
  return absl::OkStatus();
}

AcceptUnregisteredAwaitable TcpListener::AcceptUnregistered() noexcept {
  return AcceptUnregisteredAwaitable(this);
}

Task<absl::StatusOr<Connection*>> TcpListener::Accept() {
  auto accepted = co_await AcceptUnregistered();
  if (!accepted.ok()) {
    co_return accepted.status();
  }

  Connection connection = std::move(*accepted);
  const int fd = connection.file_.fd_;
  connection.worker_ = worker_;
  Connection* registered = worker_->AddConnection(std::move(connection));
  if (registered == nullptr) {
    ::close(fd);
    co_return absl::Status(absl::StatusCode::kInternal,
                           "failed to register accepted connection");
  }
  co_return registered;
}

absl::Status TcpListener::Close() noexcept {
  if (closed_) {
    return absl::OkStatus();
  }

  closed_ = true;
  const int fd = fd_;
  fd_ = -1;
  accept_state_.CloseAllAcceptedFds();
  if (fd >= 0 && ::close(fd) != 0) {
    return ErrnoToStatus(errno, "close listener failed");
  }
  return absl::OkStatus();
}

}  // namespace celer
