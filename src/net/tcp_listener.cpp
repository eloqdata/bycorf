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
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "celer/io/completion.h"
#include "celer/runtime/worker.h"
#include "spdlog/spdlog.h"

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

absl::Status ValidateUnixSocketParent(std::string_view path) {
  const std::size_t slash = path.rfind('/');
  const std::string parent =
      slash == std::string_view::npos
          ? "."
          : (slash == 0 ? "/" : std::string(path.substr(0, slash)));
  struct stat metadata{};
  if (::stat(parent.c_str(), &metadata) != 0) {
    return ErrnoToStatus(errno, "inspect Unix listener parent failed");
  }
  if (!S_ISDIR(metadata.st_mode)) {
    return absl::FailedPreconditionError(
        "Unix listener parent is not a directory");
  }
  // Pathname ownership checks are only meaningful when another uid cannot
  // swap directory entries between lstat and unlink. The socket mode still
  // controls who may connect; this invariant controls who may replace it.
  if ((metadata.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
    return absl::FailedPreconditionError(
        "Unix listener parent must not be group/world-writable");
  }
  return absl::OkStatus();
}

bool SameFileIdentity(const struct stat& left, const struct stat& right) {
  return left.st_dev == right.st_dev && left.st_ino == right.st_ino;
}

absl::Status UnlinkSocketIfSame(const std::string& path,
                                const struct stat& expected) {
  struct stat current{};
  if (::lstat(path.c_str(), &current) != 0) {
    if (errno == ENOENT) return absl::OkStatus();
    return ErrnoToStatus(errno, "inspect Unix listener before unlink failed");
  }
  if (!S_ISSOCK(current.st_mode) || !SameFileIdentity(current, expected)) {
    return absl::FailedPreconditionError(
        "Unix listener pathname was replaced; refusing to unlink it");
  }
  if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
    return ErrnoToStatus(errno, "unlink Unix listener failed");
  }
  return absl::OkStatus();
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
  const int result = ::getaddrinfo(node, service.c_str(), &hints, &addresses);
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
    address.display_ =
        current->ai_family == AF_INET6
            ? std::string("[") + numeric_host + "]:" + numeric_service
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

  // Accepted TCP sockets otherwise keep Nagle enabled: a pipelined reply that
  // takes several flushes then stalls on Nagle x delayed ACK (~40ms per batch
  // on loopback). Match the outbound connections (see rpc.cpp), which all run
  // with TCP_NODELAY. Best-effort: this fd is always TCP, and a failure here
  // must not reject the connection.
  int domain = AF_UNSPEC;
  socklen_t domain_length = sizeof(domain);
  int one = 1;
  if (::getsockopt(*accepted, SOL_SOCKET, SO_DOMAIN, &domain, &domain_length) ==
          0 &&
      (domain == AF_INET || domain == AF_INET6) &&
      ::setsockopt(*accepted, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) !=
          0) {
    spdlog::warn("setsockopt(TCP_NODELAY) on accepted fd {} failed: {}",
                 *accepted, std::strerror(errno));
  }

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
      const auto status =
          ErrnoToStatus(errno, "setsockopt(IPV6_V6ONLY) failed");
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

absl::Status TcpListener::BindUnix(Worker* worker, std::string_view path,
                                   int backlog, std::uint32_t mode) {
  if (IsOpen()) {
    return absl::FailedPreconditionError("listener is already open");
  }
  if (worker == nullptr) {
    return absl::InvalidArgumentError("worker must not be null");
  }
  sockaddr_un address{};
  if (path.empty() || path.size() >= sizeof(address.sun_path)) {
    return absl::InvalidArgumentError("Unix socket path is empty or too long");
  }
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, path.data(), path.size());
  address.sun_path[path.size()] = '\0';
  if (const absl::Status parent = ValidateUnixSocketParent(path);
      !parent.ok()) {
    return parent;
  }

  struct stat existing{};
  if (::lstat(address.sun_path, &existing) == 0) {
    if (!S_ISSOCK(existing.st_mode)) {
      return absl::AlreadyExistsError(
          "Unix listener path exists and is not a socket");
    }
    const int probe = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (probe < 0) return ErrnoToStatus(errno, "Unix socket probe failed");
    const int connected = ::connect(
        probe, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
    const int connect_error = errno;
    ::close(probe);
    if (connected == 0) {
      return absl::AlreadyExistsError("Unix listener is already active");
    }
    if (connect_error != ECONNREFUSED && connect_error != ENOENT) {
      return ErrnoToStatus(connect_error, "Unix listener probe failed");
    }
    if (connect_error == ECONNREFUSED) {
      if (const absl::Status removed =
              UnlinkSocketIfSame(std::string(path), existing);
          !removed.ok()) {
        return removed;
      }
    }
  } else if (errno != ENOENT) {
    return ErrnoToStatus(errno, "inspect Unix listener path failed");
  }

  const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) return ErrnoToStatus(errno, "Unix socket failed");
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0) {
    const absl::Status status = ErrnoToStatus(errno, "Unix bind failed");
    ::close(fd);
    return status;
  }
  struct stat bound{};
  if (::lstat(address.sun_path, &bound) != 0) {
    const int inspect_error = errno;
    ::close(fd);
    return ErrnoToStatus(inspect_error, "inspect bound Unix listener failed");
  }
  if (!S_ISSOCK(bound.st_mode)) {
    ::close(fd);
    return absl::FailedPreconditionError(
        "Unix bind pathname was replaced before initialization");
  }
  if (::chmod(address.sun_path, static_cast<mode_t>(mode)) != 0) {
    const absl::Status status = ErrnoToStatus(errno, "Unix chmod failed");
    ::close(fd);
    (void)UnlinkSocketIfSame(std::string(path), bound);
    return status;
  }
  if (::listen(fd, backlog) != 0) {
    const absl::Status status = ErrnoToStatus(errno, "Unix listen failed");
    ::close(fd);
    (void)UnlinkSocketIfSame(std::string(path), bound);
    return status;
  }
  const int current_flags = ::fcntl(fd, F_GETFL, 0);
  if (current_flags < 0 ||
      ::fcntl(fd, F_SETFL, current_flags | O_NONBLOCK) != 0) {
    const absl::Status status =
        ErrnoToStatus(errno, "fcntl(O_NONBLOCK) failed");
    ::close(fd);
    (void)UnlinkSocketIfSame(std::string(path), bound);
    return status;
  }

  worker_ = worker;
  fd_ = fd;
  unix_path_ = std::string(path);
  unix_device_ = static_cast<std::uint64_t>(bound.st_dev);
  unix_inode_ = static_cast<std::uint64_t>(bound.st_ino);
  unix_identity_valid_ = true;
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
  absl::Status result = absl::OkStatus();
  if (fd >= 0 && ::close(fd) != 0) {
    result = ErrnoToStatus(errno, "close listener failed");
  }
  if (!unix_path_.empty()) {
    const std::string path = std::exchange(unix_path_, {});
    struct stat expected{};
    expected.st_dev = static_cast<dev_t>(unix_device_);
    expected.st_ino = static_cast<ino_t>(unix_inode_);
    const bool valid = std::exchange(unix_identity_valid_, false);
    unix_device_ = 0;
    unix_inode_ = 0;
    const absl::Status unlinked =
        valid ? UnlinkSocketIfSame(path, expected)
              : absl::FailedPreconditionError(
                    "Unix listener inode identity is unavailable");
    if (!unlinked.ok() && result.ok()) {
      result = unlinked;
    }
  }
  return result;
}

}  // namespace celer
