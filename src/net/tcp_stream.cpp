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

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <optional>
#include <utility>
#include <vector>

#include "celer/io/completion.h"
#include "celer/net/socket_ops.h"
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
    case ECONNREFUSED:
    case EHOSTUNREACH:
    case ENETUNREACH:
      // Reachability failures surface on connect (and on a racing send); the
      // peer being down is a transient condition for the caller.
      return absl::Status(absl::StatusCode::kUnavailable, operation);
    default:
      return absl::Status(absl::StatusCode::kUnknown, operation);
  }
}

absl::StatusOr<std::string> FormatPeerAddress(int fd) {
  sockaddr_storage address{};
  socklen_t address_size = sizeof(address);
  if (detail::SocketPeerName(fd, reinterpret_cast<sockaddr*>(&address),
                             &address_size) != 0) {
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

ConnectOperation::ConnectOperation(Worker& worker, int fd,
                                   const sockaddr* address,
                                   socklen_t address_length,
                                   std::chrono::nanoseconds timeout) noexcept
    : worker_(&worker),
      fd_(fd),
      has_timeout_(timeout.count() > 0),
      timeout_done_(timeout.count() <= 0) {
  // The SQEs reference the address and the timespec until their CQEs arrive;
  // both live here in the awaiting frame, which outlives the operation.
  if (address != nullptr && address_length > 0 &&
      address_length <= sizeof(address_)) {
    std::memcpy(&address_, address, address_length);
    address_length_ = address_length;
  }
  const auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(timeout);
  const auto nanoseconds =
      std::chrono::duration_cast<std::chrono::nanoseconds>(timeout - seconds);
  timeout_.tv_sec = seconds.count();
  timeout_.tv_nsec = nanoseconds.count();
}

bool ConnectOperation::await_suspend(std::coroutine_handle<> awaiting) {
  awaiting_ = awaiting;
  if (fd_ < 0 || address_length_ == 0) {
    immediate_status_ = absl::Status(absl::StatusCode::kInvalidArgument,
                                     "invalid connect target");
    return false;
  }
  absl::Status status =
      worker_->SubmitConnect(fd_, reinterpret_cast<const sockaddr*>(&address_),
                             address_length_, static_cast<IoCompletion*>(this));
  if (!status.ok()) {
    immediate_status_ = std::move(status);
    return false;
  }
  if (!has_timeout_) {
    return true;
  }
  status = worker_->SubmitTimeout(timeout_, &timeout_tag_);
  if (!status.ok()) {
    // The connect is already in flight; its CQE must still be consumed before
    // this frame may be destroyed. Drop the deadline, pull the connect back,
    // and surface the submission failure once the connect CQE lands.
    timeout_done_ = true;
    immediate_status_ = std::move(status);
    CancelLoser(*worker_, static_cast<IoCompletion*>(this));
  }
  return true;
}

absl::Status ConnectOperation::await_resume() {
  if (immediate_status_.has_value()) {
    return std::move(*immediate_status_);
  }
  if (connect_result_ == 0) {
    return absl::OkStatus();
  }
  if (connect_result_ == -ECANCELED) {
    // Only the deadline cancels the connect, so the timeout won the race.
    return absl::Status(absl::StatusCode::kDeadlineExceeded,
                        "connect timed out");
  }
  return ErrnoToStatus(-connect_result_, "connect failed");
}

void ConnectOperation::Complete(Worker& worker, int result, unsigned flags) {
  (void)flags;
  connect_result_ = result;
  connect_done_ = true;
  if (!timeout_done_) {
    CancelLoser(worker, &timeout_tag_);
  }
  MaybeResume(worker);
}

void ConnectOperation::TimeoutTag::Complete(Worker& worker, int result,
                                            unsigned flags) {
  (void)flags;
  owner_->OnTimeoutComplete(worker, result);
}

void ConnectOperation::OnTimeoutComplete(Worker& worker, int result) {
  // The race outcome is inferred from the connect result; the timeout result
  // (-ETIME when it fired, -ECANCELED when the connect won) carries no state.
  (void)result;
  timeout_done_ = true;
  if (!connect_done_) {
    CancelLoser(worker, static_cast<IoCompletion*>(this));
  }
  MaybeResume(worker);
}

void ConnectOperation::CancelLoser(Worker& worker, IoCompletion* loser) {
  if (loser_cancel_submitted_) {
    return;
  }
  loser_cancel_submitted_ = true;
  // Best effort: if the cancel cannot be submitted (ring tearing down), the
  // loser's own CQE still arrives — a timeout always fires — so awaiting both
  // completions never deadlocks; the resume is merely late.
  (void)worker.SubmitCancel(loser);
}

void ConnectOperation::MaybeResume(Worker& worker) {
  // Resuming requires both CQEs: the loser is cancelled asynchronously, and an
  // early resume would let this frame be destroyed while the kernel still
  // holds an SQE referencing it. AcquireSqe dispatches nested completions on
  // its SQ-full retry, so a nested loser CQE can beat the outer winner handler
  // here — resumed_ guards against the resulting double-enqueue.
  if (connect_done_ && timeout_done_ && !resumed_) {
    resumed_ = true;
    worker.Enqueue(awaiting_);
  }
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
  if (connection_ == nullptr || connection_->worker_ == nullptr || !IsOpen()) {
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
  connection_->recv_mode_ = enabled ? RecvMode::kMultishot : RecvMode::kOneShot;
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

absl::StatusOr<std::vector<std::string>> TcpStream::PeerCertificateUriSans()
    const {
  if (tls_ == nullptr) {
    return absl::FailedPreconditionError("TLS is not active");
  }
  return tls_->PeerCertificateUriSans();
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

Task<absl::StatusOr<TcpStream>> ConnectTcp(Worker& worker, std::string_view ip,
                                           std::uint16_t port,
                                           std::chrono::nanoseconds timeout) {
  // Numeric endpoints only: DNS resolution does not belong on a worker loop.
  sockaddr_storage address{};
  socklen_t address_length = 0;
  int family = AF_UNSPEC;
  const std::string host(ip);
  sockaddr_in address4{};
  address4.sin_family = AF_INET;
  address4.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &address4.sin_addr) == 1) {
    family = AF_INET;
    std::memcpy(&address, &address4, sizeof(address4));
    address_length = sizeof(address4);
  } else {
    sockaddr_in6 address6{};
    address6.sin6_family = AF_INET6;
    address6.sin6_port = htons(port);
    if (::inet_pton(AF_INET6, host.c_str(), &address6.sin6_addr) == 1) {
      family = AF_INET6;
      std::memcpy(&address, &address6, sizeof(address6));
      address_length = sizeof(address6);
    }
  }
  if (family == AF_UNSPEC) {
    co_return absl::Status(absl::StatusCode::kInvalidArgument,
                           "connect target is not a numeric IPv4/IPv6 address");
  }

  // Nonblocking at creation: io_uring issues the connect on the worker, and
  // the registered Connection assumes nonblocking semantics (as accepted
  // sockets do).
  const int fd =
      ::socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    co_return ErrnoToStatus(errno, "socket creation failed");
  }
  int one = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
    const int error = errno;
    detail::CloseSocket(fd);
    co_return ErrnoToStatus(error, "setsockopt(TCP_NODELAY) failed");
  }

  ConnectOperation operation(worker, fd,
                             reinterpret_cast<const sockaddr*>(&address),
                             address_length, timeout);
  absl::Status connected = co_await operation;
  if (!connected.ok()) {
    // The operation retired fully (both CQEs consumed), so the ring no longer
    // references the fd and a plain close is deterministic cleanup.
    detail::CloseSocket(fd);
    co_return connected;
  }

  // Mirror the accept-side registration: hand the fd to the owning worker's
  // connection table, then wrap it as a TcpStream.
  Connection connection;
  connection.worker_ = &worker;
  connection.file_.fd_ = fd;
  connection.closed_ = false;
  Connection* registered = worker.AddConnection(std::move(connection));
  if (registered == nullptr) {
    detail::CloseSocket(fd);
    co_return absl::Status(absl::StatusCode::kInternal,
                           "failed to register outbound connection");
  }
  co_return TcpStream(registered);
}

}  // namespace celer
