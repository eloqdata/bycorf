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
#include <liburing.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>

#include "celer/runtime/operation.h"
#include "celer/runtime/worker.h"

namespace celer {

namespace {

Status ErrnoToStatus(int err, const char* operation) {
  std::string message = std::string(operation) + ": " + std::strerror(err) +
                        " (errno=" + std::to_string(err) + ")";
  switch (err) {
    case EAGAIN:
      return Status(StatusCode::kUnavailable, std::move(message));
    case ETIMEDOUT:
      return Status(StatusCode::kDeadlineExceeded, std::move(message));
    case ECANCELED:
      return Status(StatusCode::kCancelled, std::move(message));
    case EINVAL:
      return Status(StatusCode::kInvalidArgument, std::move(message));
    case EBADF:
      return Status(StatusCode::kFailedPrecondition, std::move(message));
    case EMFILE:
    case ENFILE:
      return Status(StatusCode::kResourceExhausted, std::move(message));
    default:
      return Status(StatusCode::kUnknown, std::move(message));
  }
}

}  // namespace

class PollReadableOperation final : public OperationBase {
 public:
  explicit PollReadableOperation(TcpListener* listener) : listener_(listener) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> awaiting) {
    awaiting_ = awaiting;

    if (listener_ == nullptr || listener_->fd_ < 0) {
      result_ = -EBADF;
      return false;
    }
    if (listener_->worker_ == nullptr) {
      result_ = -EINVAL;
      return false;
    }

    auto* sqe = listener_->worker_->AcquireSqe();
    if (sqe == nullptr) {
      result_ = -EAGAIN;
      return false;
    }

    io_uring_prep_poll_add(sqe, listener_->fd_, POLLIN);
    io_uring_sqe_set_data(sqe, this);
    return true;
  }

  Status await_resume() {
    if (result_ < 0) {
      return ErrnoToStatus(-result_, "listener poll failed");
    }
    return Status::Ok();
  }

  void Complete(Worker& worker, int result, unsigned flags) override {
    (void)flags;
    result_ = result;
    worker.Enqueue(awaiting_);
  }

 private:
  TcpListener* listener_ = nullptr;
  int result_ = -EINVAL;
};

bool TcpListener::IsOpen() const noexcept {
  return !closed_ && fd_ >= 0;
}

int TcpListener::NativeFd() const noexcept {
  return fd_;
}

Status TcpListener::Bind(Worker* worker, std::string_view ip, std::uint16_t port, int backlog,
                         bool reuse_port) {
  if (IsOpen()) {
    return Status(StatusCode::kFailedPrecondition, "listener is already open");
  }
  if (worker == nullptr) {
    return Status(StatusCode::kInvalidArgument, "worker must not be null");
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
    return Status(StatusCode::kInvalidArgument, "invalid IPv4 address");
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
  if (current_flags < 0 || ::fcntl(fd, F_SETFL, current_flags | O_NONBLOCK) != 0) {
    const auto status = ErrnoToStatus(errno, "fcntl(O_NONBLOCK) failed");
    ::close(fd);
    return status;
  }

  worker_ = worker;
  fd_ = fd;
  closed_ = false;
  return Status::Ok();
}

Task<StatusOr<Connection*>> TcpListener::Accept() {
  while (true) {
    sockaddr_storage addr{};
    socklen_t addr_len = sizeof(addr);
    const int accepted_fd =
        ::accept4(fd_, reinterpret_cast<sockaddr*>(&addr), &addr_len,
                  SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (accepted_fd >= 0) {
      Connection connection;
      connection.worker = worker_;
      connection.file.fd = accepted_fd;
      connection.recv_mode = worker_->recv_mode();
      connection.closed = false;
      connection.generation = next_generation_++;
      Connection* registered = worker_->AddConnection(std::move(connection));
      if (registered == nullptr) {
        ::close(accepted_fd);
        co_return Status(StatusCode::kInternal,
                         "failed to register accepted connection");
      }
      co_return registered;
    }

    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      co_return ErrnoToStatus(errno, "accept failed");
    }

    auto ready = co_await PollReadableOperation(this);
    if (!ready.ok()) {
      co_return ready;
    }
  }
}

Status TcpListener::Close() noexcept {
  if (closed_) {
    return Status::Ok();
  }

  closed_ = true;
  const int fd = fd_;
  fd_ = -1;
  if (fd >= 0 && ::close(fd) != 0) {
    return ErrnoToStatus(errno, "close listener failed");
  }
  return Status::Ok();
}

}  // namespace celer
