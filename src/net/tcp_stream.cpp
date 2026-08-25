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

#include "celer/net/tcp_stream.h"

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <optional>
#include <utility>
#include <vector>

#include "celer/io/completion.h"
#include "celer/net/tls.h"
#include "celer/runtime/worker.h"

namespace celer {

namespace {

absl::Status ErrnoToStatus(int err, const char* operation) {
  switch (err) {
    case EAGAIN:
      return absl::Status(absl::StatusCode::kUnavailable, operation);
    case ETIMEDOUT:
      return absl::Status(absl::StatusCode::kDeadlineExceeded, operation);
    case ECANCELED:
      return absl::Status(absl::StatusCode::kCancelled, operation);
    case EINVAL:
      return absl::Status(absl::StatusCode::kInvalidArgument, operation);
    case EBADF:
      return absl::Status(absl::StatusCode::kFailedPrecondition, operation);
    case ENOSYS:
      return absl::Status(absl::StatusCode::kUnimplemented, operation);
    default:
      return absl::Status(absl::StatusCode::kUnknown, operation);
  }
}

absl::StatusOr<std::string> FormatPeerAddress(int fd) {
  sockaddr_storage address{};
  socklen_t address_size = sizeof(address);
  if (::getpeername(fd, reinterpret_cast<sockaddr*>(&address), &address_size) !=
      0) {
    return ErrnoToStatus(errno, "getpeername failed");
  }
  char host[NI_MAXHOST];
  char service[NI_MAXSERV];
  const int result = ::getnameinfo(
      reinterpret_cast<const sockaddr*>(&address), address_size, host,
      sizeof(host), service, sizeof(service), NI_NUMERICHOST | NI_NUMERICSERV);
  if (result != 0) {
    return absl::UnknownError(gai_strerror(result));
  }
  if (address.ss_family == AF_INET6) {
    return std::string("[") + host + "]:" + service;
  }
  return std::string(host) + ":" + service;
}

class CancelRecvOperation final : public IoCompletion {
 public:
  explicit CancelRecvOperation(Connection* connection)
      : connection_(connection) {}

  bool await_ready() const noexcept {
    return connection_ == nullptr || !connection_->recv_armed_;
  }

  bool await_suspend(std::coroutine_handle<> awaiting) {
    awaiting_ = awaiting;
    if (connection_ == nullptr || connection_->worker_ == nullptr) {
      immediate_status_ =
          absl::InvalidArgumentError("recv cancel connection is not bound");
      return false;
    }
    absl::Status submitted =
        connection_->worker_->SubmitCancelRecv(connection_, this);
    if (!submitted.ok()) {
      immediate_status_ = std::move(submitted);
      return false;
    }
    return true;
  }

  absl::Status await_resume() noexcept {
    if (immediate_status_.has_value()) return *immediate_status_;
    // -ENOENT means the recv ended between await_ready and submission.
    if (result_ >= 0 || result_ == -ENOENT) return absl::OkStatus();
    return ErrnoToStatus(-result_, "recv cancel failed");
  }

  void Complete(Worker& worker, int result, unsigned flags) override {
    (void)flags;
    result_ = result;
    worker.Enqueue(awaiting_);
  }

 private:
  Connection* connection_ = nullptr;
  std::coroutine_handle<> awaiting_{};
  std::optional<absl::Status> immediate_status_;
  int result_ = 0;
};

}  // namespace

ReadOperation::ReadOperation(Connection* connection,
                             std::span<std::byte> buffer) noexcept
    : connection_(connection), buffer_(buffer) {}

bool ReadOperation::await_suspend(std::coroutine_handle<> awaiting) {
  awaiting_ = awaiting;

  if (buffer_.empty()) {
    immediate_result_ = std::size_t{0};
    return false;
  }
  if (connection_ == nullptr || connection_->worker_ == nullptr) {
    immediate_status_ =
        absl::Status(absl::StatusCode::kInvalidArgument, "stream is not bound");
    return false;
  }
  if (connection_->state_ != ConnectionState::kActive || connection_->closed_ ||
      connection_->closing_) {
    immediate_status_ = absl::Status(absl::StatusCode::kFailedPrecondition,
                                     "read on closed stream");
    return false;
  }
  if (!connection_->received_buffers_.empty() || connection_->recv_eof_ ||
      !connection_->last_error_.ok()) {
    return false;
  }
  if (connection_->read_inflight_ || connection_->read_waiter_) {
    immediate_status_ = absl::Status(absl::StatusCode::kFailedPrecondition,
                                     "concurrent read is not allowed");
    return false;
  }

  const auto arm_status = connection_->worker_->EnsureRecvArmed(connection_);
  if (!arm_status.ok()) {
    immediate_status_ = arm_status;
    return false;
  }

  connection_->read_inflight_ = true;
  connection_->read_waiter_ = awaiting_;
  return true;
}

absl::StatusOr<std::size_t> ReadOperation::await_resume() noexcept {
  if (immediate_status_.has_value()) return *immediate_status_;
  if (immediate_result_.has_value()) return *immediate_result_;
  if (connection_ == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "stream is not bound");
  }
  if (!connection_->last_error_.ok()) {
    absl::Status status = connection_->last_error_;
    connection_->last_error_ = absl::OkStatus();
    return status;
  }
  if (connection_->recv_eof_ && connection_->received_buffers_.empty()) {
    return std::size_t{0};
  }
  if (connection_->received_buffers_.empty()) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "no received data available");
  }

  auto& received = connection_->received_buffers_.front();
  const std::size_t available =
      static_cast<std::size_t>(received.size_ - received.offset_);
  const std::size_t to_copy = std::min(buffer_.size(), available);
  auto chunk = connection_->worker_->ViewMultishotBuffer(
      connection_, received.buffer_id_, received.offset_, to_copy);
  if (chunk.size() != to_copy) {
    return absl::Status(absl::StatusCode::kInternal,
                        "invalid multishot buffer view");
  }

  std::memcpy(buffer_.data(), chunk.data(), chunk.size());
  received.offset_ += static_cast<std::uint32_t>(to_copy);
  if (received.offset_ == received.size_) {
    const auto buffer_id = received.buffer_id_;
    connection_->received_buffers_.pop_front();
    connection_->worker_->ReleaseReceivedBuffer(connection_, buffer_id);
  }
  return to_copy;
}

void ReadOperation::Complete(Worker& worker, int result, unsigned flags) {
  (void)worker;
  (void)result;
  (void)flags;
}

WriteOperation::WriteOperation(Connection* connection,
                               std::span<const std::byte> buffer) noexcept
    : connection_(connection), buffer_(buffer) {}

bool WriteOperation::await_suspend(std::coroutine_handle<> awaiting) {
  awaiting_ = awaiting;

  if (connection_ == nullptr || connection_->worker_ == nullptr) {
    result_ = -EINVAL;
    return false;
  }
  if (connection_->state_ != ConnectionState::kActive || connection_->closed_ ||
      connection_->closing_) {
    result_ = -EBADF;
    return false;
  }
  if (connection_->write_inflight_) {
    result_ = -EINVAL;
    return false;
  }

  auto status =
      connection_->worker_->SubmitSend(connection_->file_, buffer_, this);
  if (!status.ok()) {
    result_ = -EAGAIN;
    return false;
  }

  connection_->write_inflight_ = true;
  connection_->inflight_ops_ += 1;
  return true;
}

absl::StatusOr<std::size_t> WriteOperation::await_resume() noexcept {
  if (result_ >= 0) return static_cast<std::size_t>(result_);
  return ErrnoToStatus(-result_, "send failed");
}

void WriteOperation::Complete(Worker& worker, int result, unsigned flags) {
  (void)flags;
  result_ = result;
  if (connection_ != nullptr) {
    connection_->write_inflight_ = false;
    connection_->inflight_ops_ -= 1;
  }
  worker.Enqueue(awaiting_);
}

WriteVOperation::WriteVOperation(Connection* connection,
                                 std::span<const iovec> buffers) noexcept
    : connection_(connection), buffers_(buffers) {}

bool WriteVOperation::await_suspend(std::coroutine_handle<> awaiting) {
  awaiting_ = awaiting;

  if (connection_ == nullptr || connection_->worker_ == nullptr) {
    result_ = -EINVAL;
    return false;
  }
  if (connection_->state_ != ConnectionState::kActive || connection_->closed_ ||
      connection_->closing_) {
    result_ = -EBADF;
    return false;
  }
  if (connection_->write_inflight_) {
    result_ = -EINVAL;
    return false;
  }

  message_.msg_iov = const_cast<iovec*>(buffers_.data());
  message_.msg_iovlen = buffers_.size();
  auto status =
      connection_->worker_->SubmitSendMsg(connection_->file_, &message_, this);
  if (!status.ok()) {
    result_ = -EAGAIN;
    return false;
  }

  connection_->write_inflight_ = true;
  connection_->inflight_ops_ += 1;
  return true;
}

absl::StatusOr<std::size_t> WriteVOperation::await_resume() noexcept {
  if (result_ >= 0) return static_cast<std::size_t>(result_);
  return ErrnoToStatus(-result_, "sendmsg failed");
}

void WriteVOperation::Complete(Worker& worker, int result, unsigned flags) {
  (void)flags;
  result_ = result;
  if (connection_ != nullptr) {
    connection_->write_inflight_ = false;
    connection_->inflight_ops_ -= 1;
  }
  worker.Enqueue(awaiting_);
}

bool TcpStream::IsOpen() const noexcept {
  return connection_ != nullptr &&
         connection_->state_ == ConnectionState::kActive &&
         !connection_->closed_ && connection_->file_.fd_ >= 0;
}

int TcpStream::NativeFd() const noexcept {
  return connection_ == nullptr ? -1 : connection_->file_.fd_;
}

absl::Status TcpStream::SetPeerDisconnectCallback(
    Connection::PeerDisconnectCallback callback, void* context) noexcept {
  if (connection_ == nullptr || connection_->worker_ == nullptr ||
      !IsOpen()) {
    return absl::FailedPreconditionError(
        "cannot observe disconnect on a closed stream");
  }
  connection_->peer_disconnect_callback_ = callback;
  connection_->peer_disconnect_context_ = context;
  absl::Status armed =
      connection_->worker_->EnsurePeerDisconnectPollArmed(connection_);
  if (!armed.ok()) {
    connection_->peer_disconnect_callback_ = nullptr;
    connection_->peer_disconnect_context_ = nullptr;
  }
  return armed;
}

void TcpStream::ClearPeerDisconnectCallback() noexcept {
  if (connection_ == nullptr) return;
  connection_->peer_disconnect_callback_ = nullptr;
  connection_->peer_disconnect_context_ = nullptr;
}

absl::StatusOr<std::string> TcpStream::PeerAddress() const {
  if (!IsOpen()) {
    return absl::FailedPreconditionError(
        "cannot inspect the peer of a closed stream");
  }
  return FormatPeerAddress(NativeFd());
}

absl::Status TcpStream::SetReadAhead(bool enabled) noexcept {
  if (connection_ == nullptr || connection_->worker_ == nullptr || !IsOpen()) {
    return absl::FailedPreconditionError(
        "cannot configure recv on a closed stream");
  }
  if (connection_->recv_armed_ || connection_->read_inflight_ ||
      connection_->read_waiter_ || !connection_->received_buffers_.empty()) {
    return absl::FailedPreconditionError(
        "recv read-ahead must be configured before the first read");
  }
  connection_->recv_mode_ =
      enabled ? RecvMode::kMultishot : RecvMode::kOneShot;
  return absl::OkStatus();
}

Task<absl::StatusOr<std::size_t>> TcpStream::ReadSome(
    std::span<std::byte> buffer) {
  if (tls_ != nullptr) {
    co_return co_await tls_->ReadSome(*this, buffer);
  }
  co_return co_await ReadRawSome(buffer);
}

ReadOperation TcpStream::ReadRawSome(std::span<std::byte> buffer) noexcept {
  return ReadOperation(connection_, buffer);
}

Task<absl::StatusOr<std::size_t>> TcpStream::WriteSome(
    std::span<const std::byte> buffer) {
  if (tls_ != nullptr) {
    co_return co_await tls_->WriteSome(*this, buffer);
  }
  co_return co_await WriteRawSome(buffer);
}

WriteOperation TcpStream::WriteRawSome(
    std::span<const std::byte> buffer) noexcept {
  return WriteOperation(connection_, buffer);
}

Task<absl::StatusOr<std::size_t>> TcpStream::WriteSomeV(
    std::span<const iovec> buffers) {
  if (tls_ != nullptr) {
    for (const iovec& buffer : buffers) {
      if (buffer.iov_len == 0) continue;
      co_return co_await tls_->WriteSome(
          *this,
          std::span<const std::byte>(
              static_cast<const std::byte*>(buffer.iov_base), buffer.iov_len));
    }
    co_return std::size_t{0};
  }
  co_return co_await WriteRawSomeV(buffers);
}

WriteVOperation TcpStream::WriteRawSomeV(
    std::span<const iovec> buffers) noexcept {
  return WriteVOperation(connection_, buffers);
}

Task<absl::Status> TcpStream::WriteRawAll(std::span<const std::byte> buffer) {
  std::size_t written = 0;
  while (written < buffer.size()) {
    auto result = co_await WriteRawSome(buffer.subspan(written));
    if (!result.ok()) co_return result.status();
    if (*result == 0) {
      co_return absl::InternalError("raw WriteSome returned 0");
    }
    written += *result;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> TcpStream::WriteAll(std::span<const std::byte> buffer) {
  std::size_t written = 0;
  while (written < buffer.size()) {
    auto result = co_await WriteSome(buffer.subspan(written));
    if (!result.ok()) {
      co_return result.status();
    }
    if (*result == 0) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "WriteSome returned 0");
    }
    written += *result;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> TcpStream::WriteAllV(std::span<const iovec> buffers) {
  std::vector<iovec> remaining;
  remaining.reserve(buffers.size());
  for (const iovec& buffer : buffers) {
    if (buffer.iov_len != 0) remaining.push_back(buffer);
  }

  std::size_t first = 0;
  while (first < remaining.size()) {
    auto result =
        co_await WriteSomeV(std::span<const iovec>(remaining).subspan(first));
    if (!result.ok()) co_return result.status();
    if (*result == 0) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "WriteSomeV returned 0");
    }
    std::size_t written = *result;
    while (first < remaining.size() && written >= remaining[first].iov_len) {
      written -= remaining[first].iov_len;
      ++first;
    }
    if (first < remaining.size() && written != 0) {
      remaining[first].iov_base =
          static_cast<std::byte*>(remaining[first].iov_base) + written;
      remaining[first].iov_len -= written;
    } else if (first == remaining.size() && written != 0) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "WriteSomeV wrote beyond the supplied buffers");
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> TcpStream::StartTls(
    const std::shared_ptr<TlsContext>& context, bool server,
    std::string_view peer_name) {
  if (connection_ == nullptr || connection_->worker_ == nullptr || !IsOpen()) {
    co_return absl::FailedPreconditionError(
        "cannot start TLS on a closed stream");
  }
  if (tls_ != nullptr) {
    co_return absl::FailedPreconditionError("TLS is already active");
  }
  if (connection_->recv_armed_ || connection_->read_inflight_ ||
      !connection_->received_buffers_.empty()) {
    co_return absl::FailedPreconditionError(
        "TLS must start before socket reads are armed");
  }
  auto state = TlsState::Create(context, server, peer_name);
  if (!state.ok()) co_return state.status();
  connection_->recv_mode_ = RecvMode::kOneShot;
  tls_ = std::move(*state);
  connection_->tls_state_ = tls_;
  absl::Status handshake = co_await tls_->Handshake(*this);
  if (!handshake.ok()) {
    connection_->tls_state_.reset();
    tls_.reset();
    co_return handshake;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> TcpStream::ShutdownTls() {
  if (tls_ == nullptr || !IsOpen()) co_return absl::OkStatus();
  co_return co_await tls_->Shutdown(*this);
}

std::shared_ptr<TlsState> TcpStream::TakeTlsState() noexcept {
  if (connection_ != nullptr) connection_->tls_state_.reset();
  return std::exchange(tls_, nullptr);
}

void TcpStream::AttachTlsState(std::shared_ptr<TlsState> state) noexcept {
  tls_ = std::move(state);
  if (tls_ != nullptr && connection_ != nullptr) {
    connection_->recv_mode_ = RecvMode::kOneShot;
    connection_->tls_state_ = tls_;
  }
}

Task<absl::Status> TcpStream::PauseRead() {
  if (connection_ == nullptr || connection_->worker_ == nullptr ||
      connection_->state_ != ConnectionState::kActive || connection_->closed_ ||
      connection_->closing_) {
    co_return absl::FailedPreconditionError(
        "cannot pause recv on a closed stream");
  }
  if (connection_->read_waiter_ || connection_->read_inflight_) {
    co_return absl::FailedPreconditionError(
        "cannot pause recv while a reader is waiting");
  }
  connection_->recv_paused_ = true;
  absl::Status cancelled = co_await CancelRecvOperation(connection_);
  if (!cancelled.ok()) co_return cancelled;
  while (connection_->recv_armed_) {
    co_await Yield(*connection_->worker_);
  }
  if (!connection_->received_buffers_.empty()) {
    co_return absl::FailedPreconditionError(
        "cannot hand off a stream with buffered input");
  }
  co_return absl::OkStatus();
}

absl::Status TcpStream::Close() noexcept {
  if (connection_ == nullptr) {
    return absl::OkStatus();
  }
  if (connection_->state_ != ConnectionState::kActive) {
    return absl::OkStatus();
  }
  connection_->worker_->BeginClose(connection_, absl::OkStatus(),
                                   CloseMode::kLocalClose);
  return absl::OkStatus();
}

}  // namespace celer
