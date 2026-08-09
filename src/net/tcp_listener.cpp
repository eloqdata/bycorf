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
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>

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

class AcceptAwaitable final {
 public:
  explicit AcceptAwaitable(TcpListener* listener) : listener_(listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> awaiting) {
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

  absl::StatusOr<int> await_resume() {
    if (immediate_status_.has_value()) {
      return *immediate_status_;
    }
    return listener_->accept_state_.ConsumeAcceptedFd();
  }

 private:
  TcpListener* listener_ = nullptr;
  std::optional<absl::Status> immediate_status_;
};

bool TcpListener::IsOpen() const noexcept { return !closed_ && fd_ >= 0; }

int TcpListener::NativeFd() const noexcept { return fd_; }

absl::Status TcpListener::Bind(Worker* worker, std::string_view ip,
                               std::uint16_t port, int backlog,
                               bool reuse_port) {
  if (IsOpen()) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "listener is already open");
  }
  if (worker == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "worker must not be null");
  }

  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, std::string(ip).c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "invalid IPv4 address");
  }

  if (::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
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

Task<absl::StatusOr<Connection>> TcpListener::AcceptUnregistered() {
  auto accepted = co_await AcceptAwaitable(this);
  if (!accepted.ok()) {
    co_return accepted.status();
  }

  Connection connection;
  connection.file.fd = *accepted;
  connection.closed = false;
  connection.generation = next_generation_++;
  co_return connection;
}

Task<absl::StatusOr<Connection*>> TcpListener::Accept() {
  auto accepted = co_await AcceptUnregistered();
  if (!accepted.ok()) {
    co_return accepted.status();
  }

  Connection connection = std::move(*accepted);
  const int fd = connection.file.fd;
  connection.worker = worker_;
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
