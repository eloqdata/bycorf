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

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>

#include "spdlog/spdlog.h"

namespace celer {

namespace {

std::int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

} // namespace

Worker::~Worker() { Shutdown(); }

Status Worker::Init(const WorkerOptions &options) {
  if (initialized_) {
    return Status::Ok();
  }
  // The Runtime binds a CrossCore (and its wake eventfds) for every worker
  // before Run(); a worker only ever runs through it, so cross_core_ is an
  // invariant.
  if (cross_core_ == nullptr) {
    return Status(StatusCode::kFailedPrecondition,
                  "worker requires BindCrossCore before Init");
  }
  options_ = options;

  IoBackendOptions backend_options;
  backend_options.ring_entries = options.ring_entries;
  backend_options.recv_buffer_count = options.recv_buffer_count;
  // Runtime pre-created the eventfd; the backend reuses it (does not own it).
  auto status =
      backend_.Init(backend_options, this, cross_core_->mailbox(id_).wake_fd);
  if (!status.ok()) {
    return status;
  }

  // Advertise our ring so other workers can wake us via MSG_RING.
  cross_core_->mailbox(id_).ring_fd = backend_.WakeHandle();
  initialized_ = true;
  return Status::Ok();
}

void Worker::Shutdown() {
  if (!initialized_) {
    return;
  }
  backend_.Shutdown();
  initialized_ = false;
}

void Worker::Spawn(Task<Status> task) {
  task.SetCompletionCallback(
      this, [](void* context, std::coroutine_handle<> completed) noexcept {
        static_cast<Worker*>(context)->Enqueue(completed, true);
      });
  auto handle = std::move(task).ReleaseHandle();
  if (handle) {
    if (stop_requested_.load(std::memory_order_acquire) ||
        stopping_.load(std::memory_order_acquire)) {
      handle.destroy();
      return;
    }
    Enqueue(handle);
  }
}

void SpawnOnCurrentWorker(Task<Status> task) {
  ThisWorker().self->Spawn(std::move(task));
}

void Worker::RequestStop() noexcept {
  stop_requested_.store(true, std::memory_order_release);
  backend_.WakeSelf();
}

bool Worker::NotifyWake() noexcept {
  if (stop_requested_.load(std::memory_order_acquire)) {
    stopping_.store(true, std::memory_order_release);
  }
  return stopping_.load(std::memory_order_acquire);
}

Connection *Worker::AddConnection(Connection connection) {
  const int fd = connection.file.fd;
  if (fd < 0) {
    return nullptr;
  }
  auto owned = std::make_unique<Connection>(std::move(connection));
  Connection *raw = owned.get();
  raw->id = next_connection_id_++;
  raw->last_active_ms = NowMs();
  connections_[raw->id] = std::move(owned);
  return raw;
}

void Worker::BeginClose(Connection *connection, Status reason,
                        CloseMode mode) noexcept {
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

  // Resume any reader waiting on this connection (deferred via the ready
  // queue).
  if (connection->read_waiter) {
    auto waiter = connection->read_waiter;
    connection->read_waiter = {};
    connection->read_inflight = false;
    Enqueue(waiter);
  }
  RetireConnection(connection);
}

void Worker::RetireConnection(Connection *connection) {
  if (connection == nullptr) {
    return;
  }
  if (connection->retired) {
    return;
  }
  connection->retired = true;
  connection->closing = true;
  connection->state = connection->recv_armed || connection->inflight_ops != 0 ||
                              connection->read_waiter ||
                              connection->read_inflight ||
                              connection->write_inflight ||
                              !connection->received_buffers.empty()
                          ? ConnectionState::kDraining
                          : ConnectionState::kRetired;
  DiscardReceivedBuffers(connection);
  retired_connection_ids_.push_back(connection->id);
}

void Worker::Enqueue(std::coroutine_handle<> handle, bool destroy_when_done) {
  if (handle) {
    ready_.push_back(
        ReadyTask{.handle = handle, .destroy_when_done = destroy_when_done});
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
  }
}

bool Worker::CanReclaim(const Connection &connection) const noexcept {
  return connection.state != ConnectionState::kActive &&
         connection.inflight_ops == 0 && !connection.read_waiter &&
         !connection.read_inflight && !connection.write_inflight &&
         !connection.recv_armed && connection.received_buffers.empty();
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

    Connection *connection = it->second.get();
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

void Worker::DiscardReceivedBuffers(Connection *connection) {
  if (connection == nullptr) {
    return;
  }
  while (!connection->received_buffers.empty()) {
    auto received = connection->received_buffers.front();
    connection->received_buffers.pop_front();
    backend_.ReleaseRecvBuffer(connection, received.buffer_id);
  }
}

void Worker::CheckIdleConnections() {
  if (options_.idle_timeout_ms <= 0) {
    return;
  }

  const std::int64_t now_ms = NowMs();
  for (auto &[connection_id, owned] : connections_) {
    (void)connection_id;
    Connection *connection = owned.get();
    if (connection->retired || connection->closed ||
        connection->last_active_ms <= 0) {
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

bool Worker::DrainCrossCore() {
  WorkerMailbox &mb = cross_core_->mailbox(id_);
  RemoteWork *batch[64];
  RemoteNotification notifications[64];

  // One bounded batch each, NOT drain-to-empty: leftover work is picked up on
  // the next loop iteration so io completions are not starved under load.
  const std::size_t nreq = mb.requests.try_dequeue_bulk(batch, 64);
  for (std::size_t i = 0; i < nreq; ++i) {
    RemoteWork *work = batch[i];
    work->run_fn(work); // run fn, store result in awaiter
    if (!work->reply_deferred) {
      PostReply(cross_core_, work->origin, work); // handed off; don't touch after
    }
  }

  const std::size_t nrep = mb.replies.try_dequeue_bulk(batch, 64);
  for (std::size_t i = 0; i < nrep; ++i) {
    batch[i]->waiter.resume(); // don't touch after resume
  }

  const std::size_t nnotifications =
      mb.notifications.try_dequeue_bulk(notifications, 64);
  for (std::size_t i = 0; i < nnotifications; ++i) {
    RemoteNotification &notification = notifications[i];
    notification.run_fn(notification.context, notification.value);
  }

  return nreq > 0 || nrep > 0 || nnotifications > 0;
}

void Worker::FlushWakes() {
  CurrentWorker &w = MutableThisWorker();
  if (w.wake_list.empty()) {
    return;
  }
  for (unsigned target : w.wake_list) {
    w.wake_pending[target] = 0;
    WorkerMailbox &mb = cross_core_->mailbox(target);
    if (mb.wake_seq.fetch_add(1, std::memory_order_acq_rel) == kWakeSeqParked) {
      backend_.WakeRemote(mb.ring_fd);
    }
  }
  w.wake_list.clear();
}

void Worker::Flush() {
  FlushWakes();
  const auto status = backend_.Submit();
  if (!status.ok()) [[unlikely]] {
    spdlog::error("worker submit failed: {}", status.message());
  }
}

bool Worker::RunOnce(bool wait_for_completion) {
  if (!initialized_) [[unlikely]] {
    return false;
  }

  // 1. Drain every source of work: pending coroutines, the cross-core mailbox,
  //    and reaped io completions (Poll also re-arms recv). DrainReady then
  //    resumes everything they enqueued — it loops until the ready queue is
  //    empty, so chained resumes are handled here too.
  bool did_work = !ready_.empty();
  did_work |= DrainCrossCore();
  did_work |= backend_.Poll();
  DrainReady();

  // 2. The single submit point: flush queued SQEs (sends, recv re-arms) and
  //    batched cross-core wakes. Completions dispatched while parked are
  //    resumed by the next iteration, which always reaches here before it can
  //    block.
  Flush();
  ReclaimConnections();

  if (!wait_for_completion) {
    return did_work; // shutdown drain: stop once no progress is left
  }
  if (did_work) {
    return true; // had work; loop again without parking
  }

  // 3. Idle — park. Check idle timeouts only here, off the hot path. The
  // wake_seq
  //    handshake closes the lost-wakeup race: snapshot it, recheck the mailbox
  //    once, then CAS to the parked sentinel. The recheck catches work already
  //    enqueued; the CAS catches work published after the snapshot (a
  //    producer's fetch_add changes wake_seq, so the CAS fails and we loop
  //    instead of park).
  CheckIdleConnections();
  WorkerMailbox &mb = cross_core_->mailbox(id_);
  const std::uint32_t seq = mb.wake_seq.load(std::memory_order_acquire);
  if (DrainCrossCore()) {
    return true; // raced: work arrived; next iteration drains + submits it
  }
  std::uint32_t expected = seq;
  if (!mb.wake_seq.compare_exchange_strong(expected, kWakeSeqParked,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
    return true; // a producer published work; loop again instead of parking
  }

  const int timeout_ms = options_.idle_timeout_ms > 0 ? 100 : -1;
  const bool ok = backend_.Wait(timeout_ms);       // blocks; dispatches on wake
  mb.wake_seq.store(0, std::memory_order_release); // leave the parked state
  return ok; // next iteration drains what Wait dispatched
}

void Worker::Run() {
  SetThisWorker(id_, cross_core_, this);
  stop_requested_.store(false, std::memory_order_release);
  stopping_.store(false, std::memory_order_release);
  while (!stopping_.load(std::memory_order_acquire)) {
    if (!RunOnce(true)) [[unlikely]] {
      spdlog::error("worker loop exiting because RunOnce returned false");
      break;
    }
    ReclaimConnections();
  }

  for (auto &[connection_id, owned] : connections_) {
    (void)connection_id;
    BeginClose(owned.get(), Status(StatusCode::kCancelled, "worker shutdown"),
               CloseMode::kWorkerShutdown);
  }
  while (!connections_.empty() && RunOnce(false)) {
  }
}

} // namespace celer
