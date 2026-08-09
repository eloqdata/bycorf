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

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <deque>
#include <memory>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/io/completion.h"
#include "celer/io/net_backend.h"
#include "celer/io/storage.h"
#include "celer/net/connection.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/task.h"

namespace celer {

class TcpServerImpl;

struct WorkerOptions {
  unsigned ring_entries_ = 256;  // io_uring SQ ring size
  // Multishot recv buffer-ring entries. Zero uses per-connection one-shot recv.
  unsigned recv_buffer_count_ = 1024;
  int idle_timeout_ms_ = -1;
  unsigned busy_poll_us_ = 0;
  // Match Dragonfly's normal/background fiber scheduler defaults. These are
  // cooperative budgets: a coroutine must suspend or call Yield to be
  // preemptible.
  unsigned foreground_budget_us_ = 1000;
  unsigned background_budget_us_ = 50;
  unsigned background_warrant_percent_ = 10;
};

// The per-core scheduler: ready queue, connection table, cross-core mailbox and
// the run loop. All io_uring mechanism lives in the backend it owns; the Worker
// only forwards typed submissions to it and reacts to completions.
class Worker {
 public:
  struct WakeStats {
    std::uint64_t checks_ = 0;
    std::uint64_t sent_ = 0;
  };

  struct SchedulerStats {
    std::uint64_t rounds_ = 0;
    std::uint64_t foreground_resumes_ = 0;
    std::uint64_t background_resumes_ = 0;
    std::uint64_t round_cycles_ = 0;
    std::uint64_t foreground_cycles_ = 0;
    std::uint64_t background_cycles_ = 0;
    std::uint64_t max_round_cycles_ = 0;
    std::uint64_t max_foreground_cycles_ = 0;
    std::uint64_t max_background_cycles_ = 0;
    std::uint64_t foreground_overruns_ = 0;
    std::uint64_t background_overruns_ = 0;
    double cycles_per_second_ = 0.0;
  };

  struct ReadyTask {
    std::coroutine_handle<> handle_{};
    bool destroy_when_done_ = false;
  };

  Worker() = default;
  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  ~Worker();

  absl::Status Init(const WorkerOptions& options = {});
  void Shutdown();

  // Wire this worker into the cross-core mailbox set before Run(). Called by
  // the Runtime, which creates the CrossCore (and eventfds) before any thread
  // starts.
  void BindCrossCore(WorkerId id, CrossCore* cross_core) noexcept {
    id_ = id;
    cross_core_ = cross_core;
  }
  WorkerId id() const noexcept { return id_; }

  // Resume a coroutine on this worker's ready queue.
  void Enqueue(std::coroutine_handle<> handle, bool destroy_when_done = false);

  // Resume a coroutine after the worker has returned to its event loop and
  // polled cross-core work and I/O completions once.
  void EnqueueNext(std::coroutine_handle<> handle);

  void RegisterBackground(std::coroutine_handle<> handle);
  void ForgetScheduling(std::coroutine_handle<> handle) noexcept;
  void RegisterDetached(std::coroutine_handle<> handle);
  void ForgetDetached(std::coroutine_handle<> handle) noexcept;
  void DestroyDetachedTasks() noexcept;
  bool IsBackground(std::coroutine_handle<> handle) const noexcept;
  bool BackgroundBudgetExpired() const noexcept;

  bool RunOnce(bool wait_for_completion);
  void Run();
  void RequestStop() noexcept;
  void Stop() noexcept { RequestStop(); }
  bool stop_requested() const noexcept {
    return stop_requested_.load(std::memory_order_acquire);
  }

  // Stop-signal handler, invoked by the backend when the wake eventfd fires.
  // Returns true once the worker should leave its loop.
  bool NotifyWake() noexcept;
  WakeStats TakeWakeStats() noexcept {
    WakeStats result{wake_checks_, wake_sent_};
    wake_checks_ = 0;
    wake_sent_ = 0;
    return result;
  }
  SchedulerStats TakeSchedulerStats() noexcept;

  // Typed io submissions, forwarded to the backend (keeps io_uring out of the
  // net layer). recv multishot is driven by EnsureRecvArmed; its completions
  // are handled inside the backend, which calls back into Enqueue to resume
  // readers.
  absl::Status SubmitSend(const RegisteredFile& file,
                          std::span<const std::byte> buffer,
                          IoCompletion* tag) {
    return backend_.SubmitSend(file, buffer, tag);
  }
  absl::Status SubmitAcceptMultishot(int listen_fd, IoCompletion* tag) {
    return backend_.SubmitAcceptMultishot(listen_fd, tag);
  }
  absl::Status EnsureRecvArmed(Connection* connection) {
    return backend_.StartRecvMultishot(connection);
  }
  std::span<const std::byte> ViewMultishotBuffer(const Connection* connection,
                                                 std::uint16_t buffer_id,
                                                 std::size_t offset,
                                                 std::size_t length) const {
    return backend_.ViewRecvBuffer(connection, buffer_id, offset, length);
  }
  void ReleaseReceivedBuffer(Connection* connection, std::uint16_t buffer_id) {
    backend_.ReleaseRecvBuffer(connection, buffer_id);
  }

  absl::Status RegisterFixedFiles(unsigned count) {
    return backend_.RegisterFixedFiles(count);
  }
  absl::Status RegisterBuffers(std::span<const iovec> buffers) {
    return backend_.RegisterBuffers(buffers);
  }
  absl::Status SubmitOpenDirect(std::string_view path, int flags, mode_t mode,
                                FixedFile file, IoCompletion* tag) {
    return backend_.SubmitOpenDirect(path, flags, mode, file, tag);
  }
  absl::Status SubmitCloseDirect(FixedFile file, IoCompletion* tag) {
    return backend_.SubmitCloseDirect(file, tag);
  }
  absl::Status SubmitReadFixed(FixedFile file, FixedBuffer buffer,
                               std::uint64_t offset, IoCompletion* tag) {
    return backend_.SubmitReadFixed(file, buffer, offset, tag);
  }
  absl::Status SubmitRead(FixedFile file, std::span<std::byte> buffer,
                          std::uint64_t offset, IoCompletion* tag) {
    return backend_.SubmitRead(file, buffer, offset, tag);
  }
  absl::Status SubmitWrite(FixedFile file, std::span<const std::byte> buffer,
                           std::uint64_t offset, IoCompletion* tag) {
    return backend_.SubmitWrite(file, buffer, offset, tag);
  }
  absl::Status SubmitWriteFixed(FixedFile file, FixedBuffer buffer,
                                std::uint64_t offset, IoCompletion* tag) {
    return backend_.SubmitWriteFixed(file, buffer, offset, tag);
  }
  absl::Status SubmitFdatasync(FixedFile file, IoCompletion* tag) {
    return backend_.SubmitFdatasync(file, tag);
  }
  absl::Status SubmitTimeout(const __kernel_timespec& timeout,
                             IoCompletion* tag) {
    return backend_.SubmitTimeout(timeout, tag);
  }

  Connection* AddConnection(Connection connection);
  void BeginClose(Connection* connection, absl::Status reason,
                  CloseMode mode) noexcept;
  void RetireConnection(Connection* connection);

  // Schedule a fire-and-forget session coroutine on this worker (e.g. a
  // service's accept loop or a per-connection session). The frame is destroyed
  // on completion.
  void Spawn(Task<absl::Status> task);
  // Spawn a long-lived root (service loop). Tracked so the worker can destroy
  // its frame at shutdown if it never completes (e.g. suspended in accept).
  void SpawnRoot(Task<absl::Status> task);

  // Schedule cooperative background work. Membership follows nested Task frames
  // and all subsequent resume paths.
  void SpawnBackground(Task<absl::Status> task);

 private:
  std::size_t DrainReadyUntil(std::int64_t deadline_cycles);
  std::size_t DrainBackgroundUntil(std::int64_t deadline_cycles);
  void RunRemoteWork(RemoteWork* work);
  void ResumeReady(ReadyTask ready, TaskClass task_class);
  void MergeDeferred();
  bool ShouldRunBackground() const noexcept;
  void MaybeResetRuntimeWindow() noexcept;
  void RecordRound(std::int64_t start_cycles) noexcept;
  void Flush();       // FlushWakes() + backend_.Submit()
  void FlushWakes();  // wake every marked, parked target once
  void CheckIdleConnections();
  bool BusyPoll();
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
  std::deque<ReadyTask> next_ready_;
  std::deque<RemoteWork*> foreground_remote_work_;
  std::deque<ReadyTask> background_ready_;
  std::deque<ReadyTask> next_background_ready_;
  std::deque<RemoteWork*> background_remote_work_;
  std::vector<void*> background_tasks_;
  // Root handles of Spawn/SpawnBackground tasks still alive. A task that
  // completes is removed when its frame is destroyed; anything left when the
  // run loop exits is destroyed by DestroyDetachedTasks so shutdown does not
  // leak coroutine frames suspended on I/O that will never complete.
  std::vector<void*> detached_tasks_;
  bool foreground_remote_turn_ = true;
  bool background_remote_turn_ = true;
  std::unordered_map<std::uint64_t, std::unique_ptr<Connection>> connections_;
  std::vector<std::uint64_t> retired_connection_ids_;
  std::uint64_t next_connection_id_ = 1;
  WorkerId id_ = 0;
  CrossCore* cross_core_ = nullptr;
  std::uint64_t wake_checks_ = 0;
  std::uint64_t wake_sent_ = 0;
  std::uint64_t foreground_runtime_cycles_ = 0;
  std::uint64_t background_runtime_cycles_ = 0;
  std::uint64_t foreground_budget_cycles_ = 0;
  std::uint64_t background_budget_cycles_ = 0;
  std::uint64_t busy_poll_cycles_ = 0;
  std::uint64_t runtime_window_cycles_ = 0;
  std::int64_t background_deadline_cycles_ = 0;
  double cycle_frequency_ = 0.0;
  SchedulerStats scheduler_stats_{};
};

class YieldAwaiter {
 public:
  explicit YieldAwaiter(Worker* worker) noexcept : worker_(worker) {}

  bool await_ready() const noexcept {
    return CurrentTaskClass() == TaskClass::kBackground &&
           !worker_->BackgroundBudgetExpired();
  }
  void await_suspend(std::coroutine_handle<> handle) const noexcept {
    worker_->EnqueueNext(handle);
  }
  void await_resume() const noexcept {}

 private:
  Worker* worker_;
};

inline YieldAwaiter Yield(Worker& worker) noexcept {
  return YieldAwaiter(&worker);
}

}  // namespace celer

#endif  // CELER_RUNTIME_WORKER_H_
