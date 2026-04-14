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

#include "celer/runtime/worker.h"

#include <liburing.h>
#include <linux/io_uring.h>

#include <chrono>
#include <cstring>
#include <cstdint>
#include <algorithm>

#include "celer/base/log.h"
#include "celer/runtime/operation.h"

namespace celer {

namespace {

constexpr std::uintptr_t kMultishotTag = 1;

std::int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void* EncodeMultishotData(Connection* connection) {
  return reinterpret_cast<void*>(reinterpret_cast<std::uintptr_t>(connection) | kMultishotTag);
}

bool IsMultishotData(void* data) {
  return (reinterpret_cast<std::uintptr_t>(data) & kMultishotTag) != 0;
}

Connection* DecodeMultishotConnection(void* data) {
  return reinterpret_cast<Connection*>(reinterpret_cast<std::uintptr_t>(data) & ~kMultishotTag);
}

}  // namespace

Worker::~Worker() {
  Shutdown();
}

Status Worker::Init(const WorkerOptions& options) {
  if (initialized_) {
    return Status::Ok();
  }

  const int rc = io_uring_queue_init(options.ring_entries, &ring_, 0);
  if (rc < 0) {
    return Status(StatusCode::kInternal, "io_uring_queue_init failed");
  }

  options_ = options;
  if (options_.recv_mode == RecvMode::kMultishot) {
    const auto status = InitMultishotRecv();
    if (!status) {
      io_uring_queue_exit(&ring_);
      return Status(StatusCode::kInternal, "multishot recv setup failed");
    }
  }
  initialized_ = true;
  return Status::Ok();
}

void Worker::Shutdown() {
  if (!initialized_) {
    return;
  }
  if (multishot_ring_.ring != nullptr) {
    io_uring_free_buf_ring(&ring_, multishot_ring_.ring, multishot_ring_.entries,
                           MultishotBufferRing::kGroupId);
    multishot_ring_ = {};
  }
  io_uring_queue_exit(&ring_);
  initialized_ = false;
}

io_uring_sqe* Worker::AcquireSqe() {
  if (!initialized_) {
    return nullptr;
  }

  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe != nullptr) {
    return sqe;
  }

  if (io_uring_submit(&ring_) < 0) {
    return nullptr;
  }
  return io_uring_get_sqe(&ring_);
}

Status Worker::Submit() {
  if (!initialized_) {
    return Status(StatusCode::kFailedPrecondition, "worker is not initialized");
  }

  const int rc = io_uring_submit(&ring_);
  if (rc < 0) {
    return Status(StatusCode::kInternal, "io_uring_submit failed");
  }
  return Status::Ok();
}

void Worker::Spawn(Task<Status> task) {
  auto handle = std::move(task).ReleaseHandle();
  if (handle) {
    Enqueue(handle, true);
  }
}

Connection* Worker::AddConnection(Connection connection) {
  const int fd = connection.file.fd;
  if (fd < 0) {
    return nullptr;
  }
  auto owned = std::make_unique<Connection>(std::move(connection));
  Connection* raw = owned.get();
  raw->id = next_connection_id_++;
  raw->last_active_ms = NowMs();
  connections_[raw->id] = std::move(owned);
  return raw;
}

void Worker::BeginClose(Connection* connection, Status reason, CloseMode mode) noexcept {
  if (connection == nullptr) {
    return;
  }
  if (connection->state == ConnectionState::kRetired) {
    return;
  }

  if (!reason.ok() || connection->last_error.ok()) {
    connection->last_error = std::move(reason);
  }

  connection->closing = true;
  connection->state = ConnectionState::kClosing;

  if (connection->file.fd >= 0) {
    const int fd = connection->file.fd;
    connection->file.fd = -1;
    connection->closed = true;
    ::close(fd);
  } else {
    connection->closed = true;
  }

  if (mode != CloseMode::kPeerClosed) {
    connection->recv_eof = false;
  }

  WakeReader(connection);
  RetireConnection(connection);
}

void Worker::RetireConnection(Connection* connection) {
  if (connection == nullptr) {
    return;
  }
  if (connection->retired) {
    return;
  }
  connection->retired = true;
  connection->closing = true;
  connection->state = connection->recv_armed || connection->inflight_ops != 0 ||
                              connection->read_waiter || connection->read_inflight ||
                              connection->write_inflight || !connection->received_buffers.empty()
                          ? ConnectionState::kDraining
                          : ConnectionState::kRetired;
  DiscardReceivedBuffers(connection);
  retired_connection_ids_.push_back(connection->id);
}

void Worker::Enqueue(std::coroutine_handle<> handle, bool destroy_when_done) {
  if (handle) {
    ready_.push_back(ReadyTask{.handle = handle, .destroy_when_done = destroy_when_done});
  }
}

void Worker::DrainReady() {
  while (!ready_.empty()) {
    auto ready = ready_.front();
    ready_.pop_front();
    auto handle = ready.handle;
    if (handle.done()) {
      if (ready.destroy_when_done) {
        handle.destroy();
      }
      continue;
    }
    handle.resume();
    if (handle.done()) {
      if (ready.destroy_when_done) {
        handle.destroy();
      }
    }
  }
}

bool Worker::InitMultishotRecv() {
  multishot_ring_.entries = 256;
  multishot_ring_.buffer_size = 4096;

  int err = 0;
  multishot_ring_.ring = io_uring_setup_buf_ring(
      &ring_, multishot_ring_.entries, MultishotBufferRing::kGroupId, 0, &err);
  if (multishot_ring_.ring == nullptr) {
    return false;
  }

  multishot_ring_.mask = io_uring_buf_ring_mask(multishot_ring_.entries);
  multishot_ring_.storage.resize(multishot_ring_.entries * multishot_ring_.buffer_size);

  io_uring_buf_ring_init(multishot_ring_.ring);
  for (unsigned i = 0; i < multishot_ring_.entries; ++i) {
    void* addr = multishot_ring_.storage.data() + i * multishot_ring_.buffer_size;
    io_uring_buf_ring_add(multishot_ring_.ring, addr, multishot_ring_.buffer_size,
                          static_cast<unsigned short>(i), multishot_ring_.mask, i);
  }
  io_uring_buf_ring_advance(multishot_ring_.ring, static_cast<int>(multishot_ring_.entries));
  return true;
}

Status Worker::EnsureRecvArmed(Connection* connection) {
  if (connection == nullptr) {
    return Status(StatusCode::kInvalidArgument, "connection must not be null");
  }
  if (connection->recv_mode != RecvMode::kMultishot) {
    return Status(StatusCode::kUnimplemented, "recv mode is not multishot");
  }
  if (connection->recv_armed || connection->closed || connection->closing) {
    return Status::Ok();
  }

  auto* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return Status(StatusCode::kUnavailable, "failed to acquire recv multishot sqe");
  }

  io_uring_prep_recv_multishot(
      sqe,
      connection->file.is_fixed ? static_cast<int>(connection->file.fixed_index)
                                : connection->file.fd,
      nullptr,
      0,
      0);
  sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
  if (connection->file.is_fixed) {
    sqe->flags |= IOSQE_FIXED_FILE;
  }
  sqe->flags |= IOSQE_BUFFER_SELECT;
  sqe->buf_group = MultishotBufferRing::kGroupId;
  io_uring_sqe_set_data(sqe, EncodeMultishotData(connection));

  connection->recv_armed = true;
  connection->inflight_ops += 1;
  return Status::Ok();
}

void Worker::RecycleMultishotBuffer(std::uint16_t buffer_id) {
  if (multishot_ring_.ring == nullptr || buffer_id >= multishot_ring_.entries) {
    return;
  }

  void* addr = multishot_ring_.storage.data() + buffer_id * multishot_ring_.buffer_size;
  io_uring_buf_ring_add(multishot_ring_.ring, addr, multishot_ring_.buffer_size, buffer_id,
                        multishot_ring_.mask, 0);
  io_uring_buf_ring_advance(multishot_ring_.ring, 1);
}

void Worker::WakeReader(Connection* connection) {
  if (connection != nullptr && connection->read_waiter) {
    auto waiter = connection->read_waiter;
    connection->read_waiter = {};
    connection->read_inflight = false;
    Enqueue(waiter);
  }
}

bool Worker::CanReclaim(const Connection& connection) const noexcept {
  return connection.state != ConnectionState::kActive &&
         connection.inflight_ops == 0 &&
         !connection.read_waiter &&
         !connection.read_inflight &&
         !connection.write_inflight &&
         !connection.recv_armed &&
         connection.received_buffers.empty();
}

void Worker::ReclaimConnections() {
  if (retired_connection_ids_.empty()) {
    return;
  }

  std::vector<std::uint64_t> still_retired;
  still_retired.reserve(retired_connection_ids_.size());

  for (std::uint64_t connection_id : retired_connection_ids_) {
    auto it = connections_.find(connection_id);
    if (it == connections_.end()) {
      continue;
    }

    Connection* connection = it->second.get();
    if (!CanReclaim(*connection)) {
      connection->state = ConnectionState::kDraining;
      still_retired.push_back(connection_id);
      continue;
    }

    connection->state = ConnectionState::kRetired;
    connections_.erase(it);
  }

  retired_connection_ids_ = std::move(still_retired);
}

void Worker::HandleMultishotRecv(Connection* connection, io_uring_cqe* cqe) {
  if (connection == nullptr) {
    return;
  }

  const bool has_buffer = (cqe->flags & IORING_CQE_F_BUFFER) != 0;
  std::uint16_t buffer_id = 0;
  if (has_buffer) {
    buffer_id = static_cast<std::uint16_t>(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
  }

  if (cqe->res > 0 && has_buffer) {
    connection->last_active_ms = NowMs();
    connection->received_buffers.push_back(
        ReceivedBuffer{.buffer_id = buffer_id, .size = static_cast<std::uint32_t>(cqe->res)});
  } else if (cqe->res == 0) {
    connection->recv_eof = true;
  } else if (cqe->res < 0 && cqe->res != -ECANCELED) {
    connection->last_error = Status(StatusCode::kUnknown, "recv multishot failed");
  }

  if (has_buffer && cqe->res <= 0) {
    RecycleMultishotBuffer(buffer_id);
  }

  if ((cqe->flags & IORING_CQE_F_MORE) == 0) {
    connection->recv_armed = false;
    if (connection->inflight_ops > 0) {
      connection->inflight_ops -= 1;
    }
    if (connection->state == ConnectionState::kActive &&
        !connection->closed && !connection->closing && !connection->recv_eof) {
      auto status = EnsureRecvArmed(connection);
      if (!status.ok()) {
        connection->last_error = status;
      }
    } else if (connection->state != ConnectionState::kActive) {
      connection->state = ConnectionState::kDraining;
    }
  }

  WakeReader(connection);
}

std::span<const std::byte> Worker::ViewMultishotBuffer(std::uint16_t buffer_id,
                                                       std::size_t offset,
                                                       std::size_t length) const {
  if (multishot_ring_.ring == nullptr || buffer_id >= multishot_ring_.entries ||
      offset > multishot_ring_.buffer_size ||
      length > multishot_ring_.buffer_size - offset) {
    return {};
  }

  const auto* base =
      multishot_ring_.storage.data() + buffer_id * multishot_ring_.buffer_size + offset;
  return std::span<const std::byte>(base, length);
}

void Worker::ReleaseReceivedBuffer(Connection* connection, std::uint16_t buffer_id) {
  if (connection == nullptr) {
    return;
  }
  RecycleMultishotBuffer(buffer_id);
}

void Worker::DiscardReceivedBuffers(Connection* connection) {
  if (connection == nullptr) {
    return;
  }

  while (!connection->received_buffers.empty()) {
    auto received = connection->received_buffers.front();
    connection->received_buffers.pop_front();
    RecycleMultishotBuffer(received.buffer_id);
  }
}

void Worker::CheckIdleConnections() {
  if (options_.idle_timeout_ms <= 0) {
    return;
  }

  const std::int64_t now_ms = NowMs();
  for (auto& [connection_id, owned] : connections_) {
    (void)connection_id;
    Connection* connection = owned.get();
    if (connection->retired || connection->closed || connection->last_active_ms <= 0) {
      continue;
    }
    if (now_ms - connection->last_active_ms < options_.idle_timeout_ms) {
      continue;
    }

    connection->last_error =
        Status(StatusCode::kDeadlineExceeded, "connection idle timeout");
    BeginClose(connection, connection->last_error, CloseMode::kIdleTimeout);
  }
}

bool Worker::DrainCompletions() {
  unsigned head = 0;
  io_uring_cqe* cqe = nullptr;
  unsigned processed = 0;

  io_uring_for_each_cqe(&ring_, head, cqe) {
    void* data = io_uring_cqe_get_data(cqe);
    if (IsMultishotData(data)) {
      HandleMultishotRecv(DecodeMultishotConnection(data), cqe);
    } else {
      auto* op = static_cast<OperationBase*>(data);
      if (op != nullptr) {
        op->Complete(*this, cqe->res, cqe->flags);
      }
    }
    ++processed;
  }

  if (processed != 0) {
    io_uring_cq_advance(&ring_, processed);
    return true;
  }
  return false;
}

bool Worker::RunOnce(bool wait_for_completion) {
  if (!initialized_) {
    return false;
  }

  DrainReady();
  if (DrainCompletions()) {
    DrainReady();
    return true;
  }

  const auto submit_status = Submit();
  if (!submit_status.ok()) {
    CELER_LOG_ERROR << "worker submit failed: " << submit_status.message();
    return false;
  }

  if (!wait_for_completion) {
    CheckIdleConnections();
    ReclaimConnections();
    return DrainCompletions();
  }

  io_uring_cqe* cqe = nullptr;
  int rc = 0;
  if (options_.idle_timeout_ms > 0) {
    __kernel_timespec timeout{
        .tv_sec = 0,
        .tv_nsec = 100 * 1000 * 1000,
    };
    rc = io_uring_wait_cqe_timeout(&ring_, &cqe, &timeout);
    if (rc == -ETIME) {
      CheckIdleConnections();
      ReclaimConnections();
      return true;
    }
  } else {
    rc = io_uring_wait_cqe(&ring_, &cqe);
  }
  if (rc < 0) {
    CELER_LOG_ERROR << "worker wait_cqe failed rc=" << rc;
    return false;
  }

  void* data = io_uring_cqe_get_data(cqe);
  if (IsMultishotData(data)) {
    HandleMultishotRecv(DecodeMultishotConnection(data), cqe);
  } else {
    auto* op = static_cast<OperationBase*>(data);
    if (op != nullptr) {
      op->Complete(*this, cqe->res, cqe->flags);
    }
  }
  io_uring_cqe_seen(&ring_, cqe);

  DrainCompletions();
  DrainReady();
  CheckIdleConnections();
  ReclaimConnections();
  return true;
}

void Worker::Run() {
  stopping_ = false;
  while (!stopping_) {
    if (!RunOnce(true)) {
      CELER_LOG_ERROR << "worker loop exiting because RunOnce returned false";
      break;
    }
    ReclaimConnections();
  }

  for (auto& [connection_id, owned] : connections_) {
    (void)connection_id;
    BeginClose(owned.get(),
               Status(StatusCode::kCancelled, "worker shutdown"),
               CloseMode::kWorkerShutdown);
  }
  while (!connections_.empty() && RunOnce(false)) {
  }
}

}  // namespace celer
