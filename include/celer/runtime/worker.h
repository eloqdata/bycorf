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

#include <liburing.h>

#include "celer/net/connection.h"
#include "celer/base/status.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/task.h"

namespace celer {

class TcpServerImpl;

struct WorkerOptions {
  unsigned ring_entries = 256;          // io_uring SQ ring size
  unsigned recv_buffer_count = 1024;    // multishot recv buffer-ring entries
  RecvMode recv_mode = kDefaultRecvMode;
  int idle_timeout_ms = -1;
};

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

  // Wake worker `target` if it is parked (MSG_RING via this worker's ring, or
  // eventfd fallback). Called from the current worker's thread.
  void WakeRemote(unsigned target) noexcept;

  io_uring_sqe* AcquireSqe();
  Status Submit();
  void Enqueue(std::coroutine_handle<> handle, bool destroy_when_done = false);
  bool RunOnce(bool wait_for_completion);
  void Run();
  void RequestStop() noexcept;
  void Stop() noexcept { RequestStop(); }
  bool stop_requested() const noexcept { return stop_requested_.load(std::memory_order_acquire); }
  RecvMode recv_mode() const noexcept { return options_.recv_mode; }
  Status EnsureRecvArmed(Connection* connection);
  Connection* AddConnection(Connection connection);
  void BeginClose(Connection* connection, Status reason, CloseMode mode) noexcept;
  void RetireConnection(Connection* connection);
  std::span<const std::byte> ViewMultishotBuffer(std::uint16_t buffer_id,
                                                 std::size_t offset,
                                                 std::size_t length) const;
  void ReleaseReceivedBuffer(Connection* connection, std::uint16_t buffer_id);

 private:
  struct MultishotBufferRing {
    static constexpr std::uint16_t kGroupId = 1;

    io_uring_buf_ring* ring = nullptr;
    std::vector<std::byte> storage;
    unsigned entries = 0;
    unsigned buffer_size = 0;
    int mask = 0;
  };

  void DrainReady();
  bool DrainCompletions();
  void CheckIdleConnections();
  bool CanReclaim(const Connection& connection) const noexcept;
  void ReclaimConnections();
  void DiscardReceivedBuffers(Connection* connection);
  bool InitMultishotRecv();
  bool ArmWakePoll();
  void HandleWakePoll();
  void HandleMultishotRecv(Connection* connection, io_uring_cqe* cqe);
  void RecycleMultishotBuffer(std::uint16_t buffer_id);
  void WakeReader(Connection* connection);
  void Spawn(Task<Status> task);
  bool DrainCrossCore();

  template <typename H>
  friend class TcpServer;
  io_uring ring_{};
  bool initialized_ = false;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> stopping_{false};
  WorkerOptions options_{};
  std::deque<ReadyTask> ready_;
  std::unordered_map<std::uint64_t, std::unique_ptr<Connection>> connections_;
  std::vector<std::uint64_t> retired_connection_ids_;
  std::uint64_t next_connection_id_ = 1;
  MultishotBufferRing multishot_ring_{};
  int wake_event_fd_ = -1;
  bool wake_poll_armed_ = false;
  bool owns_wake_fd_ = true;
  unsigned id_ = 0;
  CrossCore* cross_core_ = nullptr;
};

}  // namespace celer

#endif  // CELER_RUNTIME_WORKER_H_
