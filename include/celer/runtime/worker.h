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

#ifndef CELER_RUNTIME_WORKER_H_
#define CELER_RUNTIME_WORKER_H_

#include <coroutine>
#include <cstddef>
#include <deque>
#include <memory>
#include <atomic>
#include <span>
#include <utility>
#include <unordered_map>
#include <vector>

#include "celer/io/completion.h"
#include "celer/io/net_backend.h"
#include "celer/io/storage.h"
#include "celer/net/connection.h"
#include "celer/base/status.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/task.h"

namespace celer {

class TcpServerImpl;

struct WorkerOptions {
  unsigned ring_entries = 256;          // io_uring SQ ring size
  // Multishot recv buffer-ring entries. Zero uses per-connection one-shot recv.
  unsigned recv_buffer_count = 1024;
  int idle_timeout_ms = -1;
};

// The per-core scheduler: ready queue, connection table, cross-core mailbox and
// the run loop. All io_uring mechanism lives in the backend it owns; the Worker
// only forwards typed submissions to it and reacts to completions.
class Worker {
 public:
  struct ReadyTask {
    std::coroutine_handle<> handle{};
    bool destroy_when_done = false;
  };

  Worker() = default;
  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  ~Worker();

  Status Init(const WorkerOptions& options = {});
  void Shutdown();

  // Wire this worker into the cross-core mailbox set before Run(). Called by the
  // Runtime, which creates the CrossCore (and eventfds) before any thread starts.
  void BindCrossCore(unsigned id, CrossCore* cross_core) noexcept {
    id_ = id;
    cross_core_ = cross_core;
  }
  unsigned id() const noexcept { return id_; }

  // Resume a coroutine on this worker's ready queue.
  void Enqueue(std::coroutine_handle<> handle, bool destroy_when_done = false);

  bool RunOnce(bool wait_for_completion);
  void Run();
  void RequestStop() noexcept;
  void Stop() noexcept { RequestStop(); }
  bool stop_requested() const noexcept { return stop_requested_.load(std::memory_order_acquire); }

  // Stop-signal handler, invoked by the backend when the wake eventfd fires.
  // Returns true once the worker should leave its loop.
  bool NotifyWake() noexcept;

  // Typed io submissions, forwarded to the backend (keeps io_uring out of the
  // net layer). recv multishot is driven by EnsureRecvArmed; its completions are
  // handled inside the backend, which calls back into Enqueue to resume readers.
  Status SubmitSend(const RegisteredFile& file, std::span<const std::byte> buffer,
                    IoCompletion* tag) {
    return backend_.SubmitSend(file, buffer, tag);
  }
  Status SubmitAcceptMultishot(int listen_fd, IoCompletion* tag) {
    return backend_.SubmitAcceptMultishot(listen_fd, tag);
  }
  Status EnsureRecvArmed(Connection* connection) {
    return backend_.StartRecvMultishot(connection);
  }
  std::span<const std::byte> ViewMultishotBuffer(
      const Connection* connection, std::uint16_t buffer_id,
      std::size_t offset, std::size_t length) const {
    return backend_.ViewRecvBuffer(connection, buffer_id, offset, length);
  }
  void ReleaseReceivedBuffer(Connection* connection, std::uint16_t buffer_id) {
    backend_.ReleaseRecvBuffer(connection, buffer_id);
  }

  Status RegisterFixedFiles(unsigned count) {
    return backend_.RegisterFixedFiles(count);
  }
  Status RegisterBuffers(std::span<const iovec> buffers) {
    return backend_.RegisterBuffers(buffers);
  }
  Status SubmitOpenDirect(std::string_view path, int flags, mode_t mode,
                          FixedFile file, IoCompletion* tag) {
    return backend_.SubmitOpenDirect(path, flags, mode, file, tag);
  }
  Status SubmitCloseDirect(FixedFile file, IoCompletion* tag) {
    return backend_.SubmitCloseDirect(file, tag);
  }
  Status SubmitReadFixed(FixedFile file, FixedBuffer buffer,
                         std::uint64_t offset, IoCompletion* tag) {
    return backend_.SubmitReadFixed(file, buffer, offset, tag);
  }
  Status SubmitRead(FixedFile file, std::span<std::byte> buffer,
                    std::uint64_t offset, IoCompletion* tag) {
    return backend_.SubmitRead(file, buffer, offset, tag);
  }
  Status SubmitWrite(FixedFile file, std::span<const std::byte> buffer,
                     std::uint64_t offset, IoCompletion* tag) {
    return backend_.SubmitWrite(file, buffer, offset, tag);
  }
  Status SubmitWriteFixed(FixedFile file, FixedBuffer buffer,
                          std::uint64_t offset, IoCompletion* tag) {
    return backend_.SubmitWriteFixed(file, buffer, offset, tag);
  }
  Status SubmitFdatasync(FixedFile file, IoCompletion* tag) {
    return backend_.SubmitFdatasync(file, tag);
  }
  Status SubmitTimeout(const __kernel_timespec& timeout, IoCompletion* tag) {
    return backend_.SubmitTimeout(timeout, tag);
  }

  Connection* AddConnection(Connection connection);
  void BeginClose(Connection* connection, Status reason, CloseMode mode) noexcept;
  void RetireConnection(Connection* connection);

  // Schedule a fire-and-forget session coroutine on this worker (e.g. a service's
  // accept loop or a per-connection session). The frame is destroyed on completion.
  void Spawn(Task<Status> task);

 private:
  void DrainReady();
  void Flush();        // FlushWakes() + backend_.Submit()
  void FlushWakes();   // wake every marked, parked target once
  void CheckIdleConnections();
  bool CanReclaim(const Connection& connection) const noexcept;
  void ReclaimConnections();
  void DiscardReceivedBuffers(Connection* connection);
  bool DrainCrossCore();

  NetBackend backend_{};
  bool initialized_ = false;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> stopping_{false};
  WorkerOptions options_{};
  std::deque<ReadyTask> ready_;
  std::unordered_map<std::uint64_t, std::unique_ptr<Connection>> connections_;
  std::vector<std::uint64_t> retired_connection_ids_;
  std::uint64_t next_connection_id_ = 1;
  unsigned id_ = 0;
  CrossCore* cross_core_ = nullptr;
};

}  // namespace celer

#endif  // CELER_RUNTIME_WORKER_H_
