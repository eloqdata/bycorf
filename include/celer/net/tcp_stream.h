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

#ifndef CELER_NET_TCP_STREAM_H_
#define CELER_NET_TCP_STREAM_H_

#include <sys/socket.h>
#include <sys/uio.h>

#include <coroutine>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "celer/io/completion.h"
#include "celer/net/connection.h"
#include "celer/runtime/task.h"

namespace celer {

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

}  // namespace celer

#endif  // CELER_NET_TCP_STREAM_H_
