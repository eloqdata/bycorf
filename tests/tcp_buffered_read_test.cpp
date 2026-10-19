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

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>

#include "absl/cleanup/cleanup.h"
#include "bycorf/io/storage.h"
#include "bycorf/net/tcp_stream.h"
#include "runtime_test.h"

namespace {

using namespace std::chrono_literals;

absl::Status Check(bool passed, std::string_view message) {
  // Keep the last completed assertion visible if CTest kills a stalled read.
  std::cout << (passed ? "PASS " : "FAIL ") << message << std::endl;
  return passed ? absl::OkStatus() : absl::InternalError(message);
}

absl::Status Send(int fd, std::string_view bytes) {
  while (!bytes.empty()) {
    const auto sent = send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (sent <= 0) return absl::InternalError("send failed");
    bytes.remove_prefix(sent);
  }
  return absl::OkStatus();
}

std::size_t QueuedBytes(const bycorf::Connection& connection) {
  std::size_t bytes = 0;
  for (std::uint32_t i = 0; i < connection.received_buffers_.size(); ++i) {
    const auto buffer = connection.received_buffers_[i];
    bytes += buffer.size_ - buffer.offset_;
  }
  return bytes;
}

bycorf::Task<absl::Status> SendLater(bycorf::Worker& worker, int fd) {
  auto waited = co_await bycorf::SleepFor(worker, 1ms);
  if (!waited.ok()) co_return waited;
  co_return Send(fd, "later");
}

bycorf::Task<absl::Status> CheckReads(bycorf::Worker& worker) {
  int sockets[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                 sockets) != 0)
    co_return absl::InternalError("socketpair failed");
  absl::Cleanup close_peer = [&] { close(sockets[1]); };
  bycorf::Connection initial;
  initial.worker_ = &worker;
  initial.file_.fd_ = sockets[0];
  initial.closed_ = false;
  auto* connection = worker.AddConnection(std::move(initial));
  bycorf::TcpStream stream(connection);
  auto storage = stream.BorrowStorage();
  absl::Cleanup close_stream = [&] { (void)stream.Close(); };
  std::string payload(32 * 1024, '\0');
  for (std::size_t i = 0; i < payload.size(); ++i)
    payload[i] = static_cast<char>(i * 31 + i / 257);
  // Use distinct views of the one-shot backing store to make queue boundaries
  // deterministic, including on kernels without provided-buffer support. The
  // reads still use the real backend's checked views and recycling path. Live
  // socket receives below separately cover suspension and terminal events.
  connection->recv_mode_ = bycorf::RecvMode::kOneShot;
  connection->read_buffer_.resize(payload.size());
  std::memcpy(connection->read_buffer_.data(), payload.data(), payload.size());
  for (std::uint32_t offset = 0; offset < payload.size(); offset += 4096) {
    connection->received_buffers_.push_back(
        {.buffer_id_ = 0, .size_ = offset + 4096, .offset_ = offset});
  }

  std::array<std::byte, 64 * 1024> output;
  auto read = co_await stream.ReadSomeBuffered({});
  auto status = Check(
      read.ok() && *read == 0 && QueuedBytes(*connection) == payload.size(),
      "empty destination preserves queued data");
  if (!status.ok()) co_return status;

  const auto first_size = connection->received_buffers_.front().size_;
  const auto partial = first_size / 2;
  read = co_await stream.ReadSome(std::span(output).first(partial));
  status = Check(read.ok() && *read == partial,
                 "ordinary read leaves a partial front buffer");
  if (!status.ok()) co_return status;
  std::string received(reinterpret_cast<const char*>(output.data()), *read);
  const auto& queue = connection->received_buffers_;
  const auto third_partial = (queue[2].size_ - queue[2].offset_) / 2;
  const auto third_offset = queue[2].offset_ + third_partial;
  const auto limit = queue[0].size_ - queue[0].offset_ + queue[1].size_ -
                     queue[1].offset_ + third_partial;
  read = co_await stream.ReadSomeBuffered(std::span(output).first(limit));
  status =
      Check(read.ok() && *read == limit &&
                connection->received_buffers_.front().offset_ == third_offset,
            "buffered read crosses buffers and stops within the last");
  if (!status.ok()) co_return status;
  received.append(reinterpret_cast<const char*>(output.data()), *read);

  const auto front_remaining = connection->received_buffers_.front().size_ -
                               connection->received_buffers_.front().offset_;
  read = co_await stream.ReadSome(output);
  status = Check(read.ok() && *read == front_remaining &&
                     !connection->received_buffers_.empty(),
                 "ordinary ReadSome still consumes only the front buffer");
  if (!status.ok()) co_return status;
  received.append(reinterpret_cast<const char*>(output.data()), *read);

  const auto queued = QueuedBytes(*connection);
  connection->last_error_ = absl::UnavailableError("injected receive error");
  read = co_await stream.ReadSomeBuffered(output);
  status = Check(absl::IsUnavailable(read.status()) &&
                     QueuedBytes(*connection) == queued &&
                     connection->last_error_.ok(),
                 "pending receive error preserves queued data");
  if (!status.ok()) co_return status;

  // A later invalid view must not discard the successfully consumed prefix.
  // Restore the real buffer identity after observing the deferred error.
  status = Check(connection->received_buffers_.size() >= 2,
                 "multiple remaining buffers for partial-error check");
  if (!status.ok()) co_return status;
  const auto saved_buffer = connection->received_buffers_[1];
  connection->received_buffers_[1].buffer_id_ =
      std::numeric_limits<std::uint16_t>::max();
  const auto valid_prefix = connection->received_buffers_[0].size_ -
                            connection->received_buffers_[0].offset_;
  read = co_await stream.ReadSomeBuffered(output);
  const bool prefix_ok = read.ok() && *read == valid_prefix;
  if (prefix_ok)
    received.append(reinterpret_cast<const char*>(output.data()), *read);
  auto failed = co_await stream.ReadSome(output);
  // The preceding valid descriptor was consumed. Reacquire the invalid front
  // rather than retaining a reference across a potentially shrinking pop.
  if (prefix_ok) connection->received_buffers_.front() = saved_buffer;
  status = Check(prefix_ok && absl::IsInternal(failed.status()),
                 "partial progress is returned before a later buffer error");
  if (!status.ok()) co_return status;

  // Deterministically cover data and EOF already queued together without
  // relying on the kernel to deliver both completions in a particular batch.
  connection->recv_eof_ = true;
  read = co_await stream.ReadSomeBuffered(output);
  status = Check(read.ok() && *read == payload.size() - received.size() &&
                     connection->received_buffers_.empty(),
                 "queued data precedes EOF without filling output");
  if (!status.ok()) co_return status;
  received.append(reinterpret_cast<const char*>(output.data()), *read);
  status =
      Check(received == payload, "all bytes retain order across read modes");
  if (!status.ok()) co_return status;
  read = co_await stream.ReadSomeBuffered(output);
  status = Check(read.ok() && *read == 0,
                 "queued EOF returns zero after buffered data drains");
  if (!status.ok()) co_return status;

  connection->recv_eof_ = false;
  connection->recv_mode_ = bycorf::RecvMode::kMultishot;
  connection->read_buffer_.clear();
  // No bytes are queued now: the first read must suspend, but once the peer
  // sends five bytes it must return without waiting for the rest of output.
  worker.Spawn(SendLater(worker, sockets[1]));
  read = co_await stream.ReadSomeBuffered(output);
  status =
      Check(read.ok() && *read == 5 &&
                std::string_view(reinterpret_cast<const char*>(output.data()),
                                 *read) == "later",
            "buffered read waits only for initial data");
  if (!status.ok()) co_return status;

  status = Send(sockets[1], "before eof");
  if (!status.ok()) co_return status;
  read = co_await stream.ReadSomeBuffered(output);
  status =
      Check(read.ok() && *read == 10 &&
                std::string_view(reinterpret_cast<const char*>(output.data()),
                                 *read) == "before eof",
            "live receive returns the peer payload");
  if (!status.ok()) co_return status;
  // Older kernels can lose the AF_UNIX multishot EOF wake when send and
  // shutdown run back-to-back (Linux fix a68ed2df7213). Consume the payload
  // first so this tests live EOF independently of that kernel race; queued
  // data plus EOF ordering is covered explicitly above.
  if (shutdown(sockets[1], SHUT_WR) != 0)
    co_return absl::InternalError("peer shutdown failed");
  read = co_await stream.ReadSomeBuffered(output);
  co_return Check(read.ok() && *read == 0,
                  "live EOF returns zero after the queue drains");
}

absl::Status CheckCloseDoesNotShrink(bycorf::Worker& worker) {
  int sockets[2];
  if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                 sockets) != 0)
    return absl::InternalError("socketpair failed");
  absl::Cleanup close_peer = [&] { close(sockets[1]); };
  bycorf::Connection initial;
  initial.worker_ = &worker;
  initial.file_.fd_ = sockets[0];
  initial.closed_ = false;
  initial.recv_mode_ = bycorf::RecvMode::kOneShot;
  auto* connection = worker.AddConnection(std::move(initial));
  bycorf::TcpStream stream(connection);
  auto storage = stream.BorrowStorage();
  absl::Cleanup close_stream = [&] { (void)stream.Close(); };
  connection->read_buffer_.resize(1);
  auto& queue = connection->received_buffers_;
  const bycorf::ReceivedBuffer buffer{.buffer_id_ = 0, .size_ = 1};
  constexpr std::uint32_t capacity = 64;
  for (std::uint32_t i = 0; i < capacity; ++i) queue.push_back(buffer);
  while (queue.size() > capacity / 4) queue.pop_front();
  // Synthetic one-shot views do not borrow provided buffers. Leave the queue
  // one low-water pop before shrinking: the old close loop allocated 32 new
  // descriptors here even though it was about to discard every descriptor.
  for (std::uint32_t i = 0; i < capacity - 2; ++i) {
    queue.pop_front();
    queue.push_back(buffer);
  }
  auto status =
      Check(queue.size() == capacity / 4 && queue.capacity() == capacity,
            "close regression starts near the shrink threshold");
  if (!status.ok()) return status;
  status = stream.Close();
  if (!status.ok()) return status;
  return Check(
      connection->closed_ && queue.empty() && queue.capacity() == capacity,
      "close discards queued buffers without shrinking storage");
}

TEST(TcpBufferedRead, QueuedAndLiveReads) {
  bycorf::test::RunOnWorker(CheckReads);
}

TEST(TcpBufferedRead, CloseDoesNotShrink) {
  bycorf::test::RunOnWorker(
      [](bycorf::Worker& worker) -> bycorf::Task<absl::Status> {
        co_return CheckCloseDoesNotShrink(worker);
      });
}

}  // namespace
