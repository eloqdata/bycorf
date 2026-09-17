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

#ifndef BYCORF_NET_TCP_STREAM_H_
#define BYCORF_NET_TCP_STREAM_H_

#include <linux/time_types.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/io/completion.h"
#include "bycorf/net/connection.h"
#include "bycorf/runtime/task.h"

namespace bycorf {

class TlsContext;
class TlsState;

// Raw socket operations are awaitables rather than Task-returning wrappers,
// so they live directly in the awaiting coroutine frame.
class ReadOperation final : public IoCompletion {
 public:
  ReadOperation(Connection* connection, std::span<std::byte> buffer) noexcept;
  ReadOperation(const ReadOperation&) = delete;
  ReadOperation& operator=(const ReadOperation&) = delete;
  ReadOperation(ReadOperation&&) = delete;
  ReadOperation& operator=(ReadOperation&&) = delete;

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::StatusOr<std::size_t> await_resume() noexcept;
  void Complete(Worker& worker, int result, unsigned flags) override;

 private:
  Connection* connection_ = nullptr;
  std::span<std::byte> buffer_;
  std::optional<absl::Status> immediate_status_;
  std::optional<std::size_t> immediate_result_;
};

class WriteOperation final : public IoCompletion {
 public:
  WriteOperation(Connection* connection,
                 std::span<const std::byte> buffer) noexcept;
  WriteOperation(const WriteOperation&) = delete;
  WriteOperation& operator=(const WriteOperation&) = delete;
  WriteOperation(WriteOperation&&) = delete;
  WriteOperation& operator=(WriteOperation&&) = delete;

  bool await_ready() const noexcept { return buffer_.empty(); }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::StatusOr<std::size_t> await_resume() noexcept;
  void Complete(Worker& worker, int result, unsigned flags) override;

 private:
  Connection* connection_ = nullptr;
  std::span<const std::byte> buffer_;
  int result_ = 0;
};

class WriteVOperation final : public IoCompletion {
 public:
  WriteVOperation(Connection* connection,
                  std::span<const iovec> buffers) noexcept;
  WriteVOperation(const WriteVOperation&) = delete;
  WriteVOperation& operator=(const WriteVOperation&) = delete;
  WriteVOperation(WriteVOperation&&) = delete;
  WriteVOperation& operator=(WriteVOperation&&) = delete;

  bool await_ready() const noexcept { return buffers_.empty(); }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::StatusOr<std::size_t> await_resume() noexcept;
  void Complete(Worker& worker, int result, unsigned flags) override;

 private:
  Connection* connection_ = nullptr;
  std::span<const iovec> buffers_;
  msghdr message_{};
  int result_ = 0;
};

// Outbound async connect (IORING_OP_CONNECT) on a not-yet-registered fd, raced
// against an optional deadline. The awaitable owns both SQEs; the first
// completion to arrive submits io_uring_prep_cancel for the loser, and the
// awaiting coroutine resumes only after BOTH completions are consumed — so the
// kernel never references this frame after await_resume. A non-positive
// timeout disables the deadline. Success resolves to OkStatus; a lost race to
// kDeadlineExceeded.
class ConnectOperation final : public IoCompletion {
 public:
  ConnectOperation(Worker& worker, int fd, const sockaddr* address,
                   socklen_t address_length,
                   std::chrono::nanoseconds timeout) noexcept;
  ConnectOperation(const ConnectOperation&) = delete;
  ConnectOperation& operator=(const ConnectOperation&) = delete;
  ConnectOperation(ConnectOperation&&) = delete;
  ConnectOperation& operator=(ConnectOperation&&) = delete;

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::Status await_resume();
  void Complete(Worker& worker, int result, unsigned flags) override;

 private:
  // The timeout SQE carries this tag (not `this`) so its CQE is
  // distinguishable from the connect CQE at dispatch.
  class TimeoutTag final : public IoCompletion {
   public:
    explicit TimeoutTag(ConnectOperation* owner) noexcept : owner_(owner) {}
    void Complete(Worker& worker, int result, unsigned flags) override;

   private:
    ConnectOperation* owner_ = nullptr;
  };

  void OnTimeoutComplete(Worker& worker, int result);
  void CancelLoser(Worker& worker, IoCompletion* loser);
  void MaybeResume(Worker& worker);

  Worker* worker_ = nullptr;
  int fd_ = -1;
  sockaddr_storage address_{};
  socklen_t address_length_ = 0;
  __kernel_timespec timeout_{};
  TimeoutTag timeout_tag_{this};
  std::optional<absl::Status> immediate_status_;
  int connect_result_ = 0;
  bool has_timeout_ = false;
  bool connect_done_ = false;
  // Invariant: true ⟺ no timeout CQE is outstanding. Set from whether the
  // deadline is armed at construction — arming the deadline while this stays
  // true skips the loser cancel and lets the timeout CQE dispatch into a
  // destroyed frame.
  bool timeout_done_ = false;
  bool loser_cancel_submitted_ = false;
  bool resumed_ = false;
};

class TcpStream {
 public:
  TcpStream() = default;
  explicit TcpStream(Connection* connection)
      : connection_(connection),
        tls_(connection == nullptr ? nullptr : connection->tls_state_) {}

  TcpStream(TcpStream&&) noexcept = default;
  TcpStream& operator=(TcpStream&&) noexcept = default;

  TcpStream(const TcpStream&) = delete;
  TcpStream& operator=(const TcpStream&) = delete;

  bool IsOpen() const noexcept;
  int NativeFd() const noexcept;
  // Observe transport disconnect without consuming application bytes. The
  // callback runs on this connection's worker and remains installed until it
  // is cleared or the one-shot disconnect event fires.
  absl::Status SetPeerDisconnectCallback(
      Connection::PeerDisconnectCallback callback, void* context) noexcept;
  void ClearPeerDisconnectCallback() noexcept;
  // Numeric peer endpoint. IPv6 uses [host]:port so callers can pass the
  // result back as an unambiguous endpoint.
  absl::StatusOr<std::string> PeerAddress() const;

  // Select whether the transport may prefetch multiple receive buffers while
  // the application is not reading. Must be configured before the first read.
  // Disabling read-ahead provides socket-level backpressure for protocols
  // with their own bounded ingress queues.
  absl::Status SetReadAhead(bool enabled) noexcept;

  Task<absl::StatusOr<std::size_t>> ReadSome(std::span<std::byte> buffer);
  Task<absl::StatusOr<std::size_t>> WriteSome(
      std::span<const std::byte> buffer);
  Task<absl::StatusOr<std::size_t>> WriteSomeV(std::span<const iovec> buffers);
  Task<absl::Status> WriteAll(std::span<const std::byte> buffer);
  Task<absl::Status> WriteAllV(std::span<const iovec> buffers);

  Task<absl::Status> StartTls(const std::shared_ptr<TlsContext>& context,
                              bool server, std::string_view peer_name = {});
  Task<absl::Status> ShutdownTls();
  bool IsTls() const noexcept { return tls_ != nullptr; }
  // Returns all URI subjectAltName values from the verified peer
  // certificate. Available only after a successful TLS handshake.
  absl::StatusOr<std::vector<std::string>> PeerCertificateUriSans() const;

  // A paused stream may transfer its TLS state alongside a duplicated fd.
  // The caller must ensure no read/write coroutine is still in flight.
  std::shared_ptr<TlsState> TakeTlsState() noexcept;
  void AttachTlsState(std::shared_ptr<TlsState> state) noexcept;

  // Stop and drain the connection's recv operation without closing its fd.
  // This is required before moving a live socket to another worker because a
  // multishot recv otherwise remains attached to the old worker and can steal
  // bytes after the fd is duplicated.
  Task<absl::Status> PauseRead();

  absl::Status Close() noexcept;

 private:
  friend class TlsState;

  ReadOperation ReadRawSome(std::span<std::byte> buffer) noexcept;
  WriteOperation WriteRawSome(std::span<const std::byte> buffer) noexcept;
  WriteVOperation WriteRawSomeV(std::span<const iovec> buffers) noexcept;
  Task<absl::Status> WriteRawAll(std::span<const std::byte> buffer);

  Connection* connection_ = nullptr;
  std::shared_ptr<TlsState> tls_;
};

// Connect a TCP stream to a numeric IPv4/IPv6 address (no DNS). Creates the
// socket (TCP_NODELAY, nonblocking), drives IORING_OP_CONNECT under the
// deadline, and on success registers the fd as a Connection on `worker` —
// mirroring the accept-side registration flow. Must be awaited on `worker`.
// On any failure the fd is closed; a lost deadline yields kDeadlineExceeded.
Task<absl::StatusOr<TcpStream>> ConnectTcp(Worker& worker, std::string_view ip,
                                           std::uint16_t port,
                                           std::chrono::nanoseconds timeout);

}  // namespace bycorf

#endif  // BYCORF_NET_TCP_STREAM_H_
