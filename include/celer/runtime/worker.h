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

#include <sys/socket.h>

#include <array>
#include <atomic>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/io/completion.h"
#include "celer/io/net_backend.h"
#include "celer/io/spdk_storage.h"
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
  // Zero preserves SPDK's process-all-ready-completions behavior. A non-zero
  // value bounds each worker-loop poll so foreground work gets another turn.
  unsigned spdk_max_completions_per_poll_ = 0;
  // Run a small foreground slice after cross-core/network polling but before
  // polling SPDK. Zero preserves the original ordering.
  unsigned spdk_foreground_pre_poll_us_ = 0;
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
    std::uint64_t storage_poll_calls_ = 0;
    std::uint64_t storage_poll_empty_ = 0;
    std::uint64_t storage_completions_ = 0;
    std::uint64_t storage_max_completions_ = 0;
    std::uint64_t storage_poll_cycles_ = 0;
    std::uint64_t storage_max_poll_cycles_ = 0;
    double cycles_per_second_ = 0.0;
  };

  struct LatencySampleStats {
    static constexpr std::array<std::uint64_t, 24> kBucketUpperUs{
        1,  2,   3,   4,   5,   8,   10,  15,   20,   30,   40,   50,
        75, 100, 150, 200, 300, 500, 750, 1000, 1500, 2000, 5000, 10000};
    std::uint64_t count_ = 0;
    std::uint64_t sum_ns_ = 0;
    std::uint64_t max_ns_ = 0;
    std::array<std::uint64_t, kBucketUpperUs.size()> buckets_{};

    void Add(std::uint64_t nanoseconds) noexcept;
    double AverageUs() const noexcept;
    std::uint64_t PercentileUpperUs(double percentile) const noexcept;
  };

  struct CrossCoreLatencyStats {
    LatencySampleStats wake_batch_wait_;
    LatencySampleStats parked_wake_batch_wait_;
    LatencySampleStats request_queue_;
    LatencySampleStats reply_queue_;
  };

  struct StorageIoStats {
    std::uint64_t read_operations_ = 0;
    std::uint64_t read_bytes_ = 0;
    std::uint64_t write_operations_ = 0;
    std::uint64_t write_bytes_ = 0;
    std::uint64_t fdatasync_operations_ = 0;
    // Bytes submitted before successful fdatasync barriers. This is logical
    // durability throughput, not an NVMe-reported physical transfer count.
    std::uint64_t fdatasync_bytes_ = 0;
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

  // Scheduler knobs are changed on the owning worker through SubmitTaskTo.
  // Updating both the reader-facing option and its precomputed cycle budget
  // there keeps RunOnce lock-free while allowing CONFIG SET at runtime.
  absl::Status SetForegroundBudgetUs(unsigned microseconds) noexcept;
  absl::Status SetBackgroundBudgetUs(unsigned microseconds) noexcept;
  absl::Status SetBackgroundWarrantPercent(unsigned percent) noexcept;
  void SetSpdkMaxCompletionsPerPoll(unsigned completions) noexcept;
  unsigned foreground_budget_us() const noexcept {
    return options_.foreground_budget_us_;
  }
  unsigned background_budget_us() const noexcept {
    return options_.background_budget_us_;
  }
  unsigned background_warrant_percent() const noexcept {
    return options_.background_warrant_percent_;
  }
  unsigned spdk_max_completions_per_poll() const noexcept {
    return options_.spdk_max_completions_per_poll_;
  }

  bool RunOnce(bool wait_for_completion);
  // Run until stopped, honoring requests made before entry. A stopped Worker
  // is not restarted by calling Run again.
  void Run();
  // Thread-safe and sticky for this Worker's lifetime, including startup.
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
  CrossCoreLatencyStats TakeCrossCoreLatencyStats() noexcept {
    return std::exchange(cross_core_latency_stats_, CrossCoreLatencyStats{});
  }

  // Storage completions run on the owning worker. These counters and the
  // per-file durability watermarks therefore need no atomics; metrics
  // collection copies them on that worker through SubmitTo.
  StorageIoStats storage_io_stats() const noexcept { return storage_io_stats_; }
  void RecordStorageReadCompletion(std::size_t bytes) noexcept;
  void RecordStorageWriteCompletion(std::size_t bytes) noexcept;
  std::uint64_t StorageWriteSubmissionBytes(FixedFile file) const noexcept;
  void RecordFdatasyncCompletion(FixedFile file,
                                 std::uint64_t write_bytes) noexcept;

  // Typed io submissions, forwarded to the backend (keeps io_uring out of the
  // net layer). recv multishot is driven by EnsureRecvArmed; its completions
  // are handled inside the backend, which calls back into Enqueue to resume
  // readers.
  absl::Status SubmitSend(const RegisteredFile& file,
                          std::span<const std::byte> buffer,
                          IoCompletion* tag) {
    return backend_.SubmitSend(file, buffer, tag);
  }
  absl::Status SubmitSendMsg(const RegisteredFile& file, const msghdr* message,
                             IoCompletion* tag) {
    return backend_.SubmitSendMsg(file, message, tag);
  }
  absl::Status SubmitAcceptMultishot(int listen_fd, IoCompletion* tag) {
    return backend_.SubmitAcceptMultishot(listen_fd, tag);
  }
  // Async connect on a raw fd (pre-registration); see ConnectTcp.
  absl::Status SubmitConnect(int fd, const sockaddr* address,
                             socklen_t address_length, IoCompletion* tag) {
    return backend_.SubmitConnect(fd, address, address_length, tag);
  }
  // Best-effort cancel of the in-flight SQE tagged `target`; owner-thread
  // only (the ring is single-issuer).
  absl::Status SubmitCancel(IoCompletion* target) {
    return backend_.SubmitCancel(target);
  }
  absl::Status EnsureRecvArmed(Connection* connection) {
    return backend_.StartRecvMultishot(connection);
  }
  absl::Status SubmitCancelRecv(Connection* connection, IoCompletion* tag) {
    return backend_.SubmitCancelRecv(connection, tag);
  }
  absl::Status EnsurePeerDisconnectPollArmed(Connection* connection) {
    return backend_.StartPeerDisconnectPoll(connection);
  }
  absl::Status CancelPeerDisconnectPoll(Connection* connection) {
    return backend_.CancelPeerDisconnectPoll(connection);
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
#ifdef CELER_WITH_SPDK_STORAGE
    absl::Status status = storage_backend_.RegisterFixedFiles(count);
#else
    absl::Status status = backend_.RegisterFixedFiles(count);
#endif
    if (status.ok()) {
      storage_file_io_stats_.assign(count, StorageFileIoStats{});
    }
    return status;
  }
  absl::Status RegisterBuffers(std::span<const iovec> buffers) {
#ifdef CELER_WITH_SPDK_STORAGE
    return storage_backend_.RegisterBuffers(buffers);
#else
    return backend_.RegisterBuffers(buffers);
#endif
  }
  absl::Status SubmitOpenDirect(std::string_view path, int flags, mode_t mode,
                                FixedFile file, IoCompletion* tag) {
#ifdef CELER_WITH_SPDK_STORAGE
    return storage_backend_.SubmitOpenDirect(path, flags, mode, file, tag);
#else
    return backend_.SubmitOpenDirect(path, flags, mode, file, tag);
#endif
  }
  absl::Status SubmitCloseDirect(FixedFile file, IoCompletion* tag) {
#ifdef CELER_WITH_SPDK_STORAGE
    return storage_backend_.SubmitCloseDirect(file, tag);
#else
    return backend_.SubmitCloseDirect(file, tag);
#endif
  }
  absl::Status SubmitReadFixed(FixedFile file, FixedBuffer buffer,
                               std::uint64_t offset, IoCompletion* tag) {
#ifdef CELER_WITH_SPDK_STORAGE
    return storage_backend_.SubmitReadFixed(file, buffer, offset, tag);
#else
    return backend_.SubmitReadFixed(file, buffer, offset, tag);
#endif
  }
  absl::Status SubmitRead(FixedFile file, std::span<std::byte> buffer,
                          std::uint64_t offset, IoCompletion* tag) {
#ifdef CELER_WITH_SPDK_STORAGE
    return storage_backend_.SubmitRead(file, buffer, offset, tag);
#else
    return backend_.SubmitRead(file, buffer, offset, tag);
#endif
  }
  absl::Status SubmitWrite(FixedFile file, std::span<const std::byte> buffer,
                           std::uint64_t offset, IoCompletion* tag) {
#ifdef CELER_WITH_SPDK_STORAGE
    absl::Status status =
        storage_backend_.SubmitWrite(file, buffer, offset, tag);
#else
    absl::Status status = backend_.SubmitWrite(file, buffer, offset, tag);
#endif
    if (status.ok() && file.index_ < storage_file_io_stats_.size()) {
      storage_file_io_stats_[file.index_].submitted_write_bytes_ +=
          buffer.size();
    }
    return status;
  }
  absl::Status SubmitWriteFixed(FixedFile file, FixedBuffer buffer,
                                std::uint64_t offset, IoCompletion* tag) {
#ifdef CELER_WITH_SPDK_STORAGE
    absl::Status status =
        storage_backend_.SubmitWriteFixed(file, buffer, offset, tag);
#else
    absl::Status status = backend_.SubmitWriteFixed(file, buffer, offset, tag);
#endif
    if (status.ok() && file.index_ < storage_file_io_stats_.size()) {
      storage_file_io_stats_[file.index_].submitted_write_bytes_ +=
          buffer.size_;
    }
    return status;
  }
  absl::Status SubmitFdatasync(FixedFile file, IoCompletion* tag) {
#ifdef CELER_WITH_SPDK_STORAGE
    return storage_backend_.SubmitFdatasync(file, tag);
#else
    return backend_.SubmitFdatasync(file, tag);
#endif
  }
  absl::Status SubmitTimeout(const __kernel_timespec& timeout,
                             IoCompletion* tag) {
    return backend_.SubmitTimeout(timeout, tag);
  }

  Connection* AddConnection(Connection connection);
  std::uint64_t ActiveConnectionCount() const noexcept {
    return active_connection_count_;
  }
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

  // Schedule cooperative background work. Membership follows nested Task
  // frames and all subsequent resume paths. If still suspended at shutdown,
  // the coroutine frame is reclaimed after all workers and io_uring have
  // quiesced.
  void SpawnBackground(Task<absl::Status> task);

 private:
  struct StorageFileIoStats {
    std::uint64_t submitted_write_bytes_ = 0;
    std::uint64_t durable_write_bytes_ = 0;
  };

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
#ifdef CELER_WITH_SPDK_STORAGE
  bool PollStorage();
#endif
  bool CanReclaim(const Connection& connection) const noexcept;
  void ReclaimConnections();
  void DiscardReceivedBuffers(Connection* connection);
  bool DrainCrossCore();

  NetBackend backend_{};
#ifdef CELER_WITH_SPDK_STORAGE
  SpdkStorageBackend storage_backend_{};
#endif
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
  std::uint64_t active_connection_count_ = 0;
  WorkerId id_ = 0;
  CrossCore* cross_core_ = nullptr;
  std::uint64_t wake_checks_ = 0;
  std::uint64_t wake_sent_ = 0;
  std::uint64_t foreground_runtime_cycles_ = 0;
  std::uint64_t background_runtime_cycles_ = 0;
  std::uint64_t foreground_budget_cycles_ = 0;
  std::uint64_t background_budget_cycles_ = 0;
  std::uint64_t busy_poll_cycles_ = 0;
  std::uint64_t spdk_foreground_pre_poll_cycles_ = 0;
  std::uint64_t runtime_window_cycles_ = 0;
  std::int64_t background_deadline_cycles_ = 0;
  double cycle_frequency_ = 0.0;
  SchedulerStats scheduler_stats_{};
  CrossCoreLatencyStats cross_core_latency_stats_{};
  StorageIoStats storage_io_stats_{};
  std::vector<StorageFileIoStats> storage_file_io_stats_;
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
