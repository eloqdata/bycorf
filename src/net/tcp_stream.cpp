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

#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <optional>

#include "celer/io/completion.h"
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

class ReadOperation final : public IoCompletion {
 public:
  ReadOperation(Connection* connection, std::span<std::byte> buffer)
      : connection_(connection), buffer_(buffer) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> awaiting) {
    awaiting_ = awaiting;

    if (buffer_.empty()) {
      immediate_result_ = std::size_t{0};
      return false;
    }
    if (connection_ == nullptr || connection_->worker == nullptr) {
      immediate_status_ = absl::Status(absl::StatusCode::kInvalidArgument,
                                       "stream is not bound");
      return false;
    }
    if (connection_->state != ConnectionState::kActive || connection_->closed ||
        connection_->closing) {
      immediate_status_ = absl::Status(absl::StatusCode::kFailedPrecondition,
                                       "read on closed stream");
      return false;
    }

    if (!connection_->received_buffers.empty() || connection_->recv_eof ||
        !connection_->last_error.ok()) {
      return false;
    }

    if (connection_->read_inflight || connection_->read_waiter) {
      immediate_status_ = absl::Status(absl::StatusCode::kFailedPrecondition,
                                       "concurrent read is not allowed");
      return false;
    }

    const auto arm_status = connection_->worker->EnsureRecvArmed(connection_);
    if (!arm_status.ok()) {
      immediate_status_ = arm_status;
      return false;
    }

    connection_->read_inflight = true;
    connection_->read_waiter = awaiting_;
    return true;
  }

  absl::StatusOr<std::size_t> await_resume() noexcept {
    if (immediate_status_.has_value()) {
      return *immediate_status_;
    }
    if (immediate_result_.has_value()) {
      return *immediate_result_;
    }

    if (connection_ == nullptr) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "stream is not bound");
    }

    if (!connection_->last_error.ok()) {
      absl::Status status = connection_->last_error;
      connection_->last_error = absl::OkStatus();
      return status;
    }

    if (connection_->recv_eof && connection_->received_buffers.empty()) {
      return std::size_t{0};
    }

    if (connection_->received_buffers.empty()) {
      return absl::Status(absl::StatusCode::kUnavailable,
                          "no received data available");
    }

    auto& received = connection_->received_buffers.front();
    const std::size_t available =
        static_cast<std::size_t>(received.size - received.offset);
    const std::size_t to_copy = std::min(buffer_.size(), available);
    auto chunk = connection_->worker->ViewMultishotBuffer(
        connection_, received.buffer_id, received.offset, to_copy);
    if (chunk.size() != to_copy) {
      return absl::Status(absl::StatusCode::kInternal,
                          "invalid multishot buffer view");
    }

    std::memcpy(buffer_.data(), chunk.data(), chunk.size());
    received.offset += static_cast<std::uint32_t>(to_copy);
    if (received.offset == received.size) {
      const auto buffer_id = received.buffer_id;
      connection_->received_buffers.pop_front();
      connection_->worker->ReleaseReceivedBuffer(connection_, buffer_id);
    }
    return to_copy;
  }

  void Complete(Worker& worker, int result, unsigned flags) override {
    (void)worker;
    (void)result;
    (void)flags;
  }

 private:
  Connection* connection_ = nullptr;
  std::span<std::byte> buffer_;
  std::optional<absl::Status> immediate_status_;
  std::optional<std::size_t> immediate_result_;
};

class WriteOperation final : public IoCompletion {
 public:
  WriteOperation(Connection* connection, std::span<const std::byte> buffer)
      : connection_(connection), buffer_(buffer) {}

  bool await_ready() const noexcept { return buffer_.empty(); }

  bool await_suspend(std::coroutine_handle<> awaiting) {
    awaiting_ = awaiting;

    if (connection_ == nullptr || connection_->worker == nullptr) {
      result_ = -EINVAL;
      return false;
    }
    if (connection_->state != ConnectionState::kActive || connection_->closed ||
        connection_->closing) {
      result_ = -EBADF;
      return false;
    }
    if (connection_->write_inflight) {
      result_ = -EINVAL;
      return false;
    }

    auto status =
        connection_->worker->SubmitSend(connection_->file, buffer_, this);
    if (!status.ok()) {
      result_ = -EAGAIN;
      return false;
    }

    connection_->write_inflight = true;
    connection_->inflight_ops += 1;
    submitted_ = true;
    return true;
  }

  absl::StatusOr<std::size_t> await_resume() noexcept {
    if (!submitted_) {
      if (result_ >= 0) {
        return static_cast<std::size_t>(result_);
      }
      return ErrnoToStatus(-result_, "send failed");
    }

    if (result_ >= 0) {
      return static_cast<std::size_t>(result_);
    }
    return ErrnoToStatus(-result_, "send failed");
  }

  void Complete(Worker& worker, int result, unsigned flags) override {
    (void)flags;
    result_ = result;
    if (connection_ != nullptr) {
      connection_->write_inflight = false;
      connection_->inflight_ops -= 1;
    }
    worker.Enqueue(awaiting_);
  }

 private:
  Connection* connection_ = nullptr;
  std::span<const std::byte> buffer_;
  int result_ = 0;
  bool submitted_ = false;
};

}  // namespace

bool TcpStream::IsOpen() const noexcept {
  return connection_ != nullptr &&
         connection_->state == ConnectionState::kActive &&
         !connection_->closed && connection_->file.fd >= 0;
}

int TcpStream::NativeFd() const noexcept {
  return connection_ == nullptr ? -1 : connection_->file.fd;
}

Task<absl::StatusOr<std::size_t>> TcpStream::ReadSome(
    std::span<std::byte> buffer) {
  co_return co_await ReadOperation(connection_, buffer);
}

Task<absl::StatusOr<std::size_t>> TcpStream::WriteSome(
    std::span<const std::byte> buffer) {
  co_return co_await WriteOperation(connection_, buffer);
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

absl::Status TcpStream::Close() noexcept {
  if (connection_ == nullptr) {
    return absl::OkStatus();
  }
  if (connection_->state != ConnectionState::kActive) {
    return absl::OkStatus();
  }
  connection_->worker->BeginClose(connection_, absl::OkStatus(),
                                  CloseMode::kLocalClose);
  return absl::OkStatus();
}

}  // namespace celer
