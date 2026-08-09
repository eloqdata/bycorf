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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/base/internal/cycleclock.h"
#include "spdlog/spdlog.h"

namespace celer {

namespace {

std::int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

std::int64_t CycleNow() noexcept {
  return absl::base_internal::CycleClock::Now();
}

double CycleFrequency() noexcept {
  return absl::base_internal::CycleClock::Frequency();
}

std::uint64_t CyclesFromMicroseconds(double frequency,
                                     unsigned microseconds) noexcept {
  return std::max<std::uint64_t>(
      1, static_cast<std::uint64_t>(
             frequency * static_cast<double>(microseconds) / 1'000'000.0));
}

class TaskClassGuard {
 public:
  explicit TaskClassGuard(TaskClass task_class) noexcept
      : previous_(std::exchange(MutableCurrentTaskClass(), task_class)) {}
  ~TaskClassGuard() { MutableCurrentTaskClass() = previous_; }

 private:
  TaskClass previous_;
};

}  // namespace

void RegisterBackgroundTask(std::coroutine_handle<> handle) noexcept {
  if (ThisWorker().self != nullptr) {
    ThisWorker().self->RegisterBackground(handle);
  }
}

void ForgetTaskScheduling(std::coroutine_handle<> handle) noexcept {
  if (ThisWorker().self != nullptr) {
    ThisWorker().self->ForgetScheduling(handle);
  }
}

Worker::~Worker() { Shutdown(); }

absl::Status Worker::Init(const WorkerOptions &options) {
  if (initialized_) {
    return absl::OkStatus();
  }
  // The Runtime binds a CrossCore (and its wake eventfds) for every worker
  // before Run(); a worker only ever runs through it, so cross_core_ is an
  // invariant.
  if (cross_core_ == nullptr) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "worker requires BindCrossCore before Init");
  }
  if (options.foreground_budget_us == 0 || options.background_budget_us == 0 ||
      options.background_warrant_percent > 100) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "worker scheduler budgets must be positive and background "
        "warrant must be <= 100%");
  }
  options_ = options;
  cycle_frequency_ = CycleFrequency();
  foreground_budget_cycles_ =
      CyclesFromMicroseconds(cycle_frequency_, options_.foreground_budget_us);
  background_budget_cycles_ =
      CyclesFromMicroseconds(cycle_frequency_, options_.background_budget_us);
  busy_poll_cycles_ =
      CyclesFromMicroseconds(cycle_frequency_, options_.busy_poll_us);
  runtime_window_cycles_ = CyclesFromMicroseconds(cycle_frequency_, 10'000);
  scheduler_stats_.cycles_per_second = cycle_frequency_;

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
  return absl::OkStatus();
}

void Worker::Shutdown() {
  if (!initialized_) {
    return;
  }
  backend_.Shutdown();
  initialized_ = false;
}

void Worker::Spawn(Task<absl::Status> task) {
  task.SetCompletionCallback(
      this, [](void *context, std::coroutine_handle<> completed) noexcept {
        static_cast<Worker *>(context)->Enqueue(completed, true);
      });
  auto handle = std::move(task).ReleaseHandle();
  if (handle) {
    ForgetScheduling(handle);
    if (stop_requested_.load(std::memory_order_acquire) ||
        stopping_.load(std::memory_order_acquire)) {
      handle.destroy();
      return;
    }
    RegisterDetached(handle);
    Enqueue(handle);
  }
}

void Worker::SpawnRoot(Task<absl::Status> task) {
  task.SetCompletionCallback(
      this, [](void *context, std::coroutine_handle<> completed) noexcept {
        static_cast<Worker *>(context)->Enqueue(completed, true);
      });
  auto handle = std::move(task).ReleaseHandle();
  if (!handle) {
    return;
  }
  if (stop_requested_.load(std::memory_order_acquire) ||
      stopping_.load(std::memory_order_acquire)) {
    handle.destroy();
    return;
  }
  RegisterDetached(handle);
  Enqueue(handle);
}

void Worker::SpawnBackground(Task<absl::Status> task) {
  task.SetCompletionCallback(
      this, [](void *context, std::coroutine_handle<> completed) noexcept {
        static_cast<Worker *>(context)->Enqueue(completed, true);
      });
  auto handle = std::move(task).ReleaseHandle();
  if (!handle) {
    return;
  }
  if (stop_requested_.load(std::memory_order_acquire) ||
      stopping_.load(std::memory_order_acquire)) {
    handle.destroy();
    return;
  }
  RegisterBackground(handle);
  RegisterDetached(handle);
  Enqueue(handle);
}

void SpawnOnCurrentWorker(Task<absl::Status> task) {
  if (CurrentTaskClass() == TaskClass::kBackground) {
    ThisWorker().self->SpawnBackground(std::move(task));
  } else {
    ThisWorker().self->Spawn(std::move(task));
  }
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

void Worker::BeginClose(Connection *connection, absl::Status reason,
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
  if (!handle) {
    return;
  }
  ReadyTask ready{.handle = handle, .destroy_when_done = destroy_when_done};
  if (IsBackground(handle)) {
    background_ready_.push_back(ready);
  } else {
    ready_.push_back(ready);
  }
}

void Worker::EnqueueNext(std::coroutine_handle<> handle) {
  if (!handle) {
    return;
  }
  if (IsBackground(handle)) {
    next_background_ready_.push_back(ReadyTask{.handle = handle});
  } else {
    next_ready_.push_back(ReadyTask{.handle = handle});
  }
}

void Worker::RegisterBackground(std::coroutine_handle<> handle) {
  if (!handle || IsBackground(handle)) {
    return;
  }
  background_tasks_.push_back(handle.address());
}

void Worker::ForgetScheduling(std::coroutine_handle<> handle) noexcept {
  if (!handle || background_tasks_.empty()) {
    return;
  }
  const auto found = std::find(background_tasks_.begin(),
                               background_tasks_.end(), handle.address());
  if (found != background_tasks_.end()) {
    *found = background_tasks_.back();
    background_tasks_.pop_back();
  }
}

namespace {

using SpawnPromise = Task<absl::Status>::promise_type;

SpawnPromise &PromiseOf(void *address) noexcept {
  return std::coroutine_handle<SpawnPromise>::from_address(address).promise();
}

}  // namespace

void Worker::RegisterDetached(std::coroutine_handle<> handle) {
  PromiseOf(handle.address()).detached_index_ =
      static_cast<std::uint32_t>(detached_tasks_.size());
  detached_tasks_.push_back(handle.address());
}

void Worker::ForgetDetached(std::coroutine_handle<> handle) noexcept {
  if (!handle) {
    return;
  }
  SpawnPromise &promise = PromiseOf(handle.address());
  const std::uint32_t index = promise.detached_index_;
  if (index == SpawnPromise::kNotDetached) {
    return;
  }
  promise.detached_index_ = SpawnPromise::kNotDetached;
  detached_tasks_[index] = detached_tasks_.back();
  detached_tasks_.pop_back();
  if (index != detached_tasks_.size()) {
    PromiseOf(detached_tasks_[index]).detached_index_ = index;
  }
}

void Worker::DestroyDetachedTasks() noexcept {
  // Destroying a root frame runs its destructors, which release any child
  // Task frames it owns; the io_uring ring must already be quiesced so no
  // in-flight kernel operation can touch the freed frames.
  std::vector<void *> tasks = std::move(detached_tasks_);
  detached_tasks_.clear();
  for (void *address : tasks) {
    std::coroutine_handle<>::from_address(address).destroy();
  }
}

bool Worker::IsBackground(std::coroutine_handle<> handle) const noexcept {
  if (!handle || background_tasks_.empty()) {
    return false;
  }
  return std::find(background_tasks_.begin(), background_tasks_.end(),
                   handle.address()) != background_tasks_.end();
}

bool Worker::BackgroundBudgetExpired() const noexcept {
  return background_deadline_cycles_ == 0 ||
         CycleNow() >= background_deadline_cycles_;
}

void Worker::ResumeReady(ReadyTask ready, TaskClass task_class) {
  auto handle = ready.handle;
  if (handle.done()) {
    if (ready.destroy_when_done) {
      ForgetScheduling(handle);
      ForgetDetached(handle);
      handle.destroy();
    }
    return;
  }
  TaskClassGuard task_class_guard(task_class);
  handle.resume();
}

std::size_t Worker::DrainReadyUntil(std::int64_t deadline_cycles) {
  std::size_t resumed = 0;
  while (!ready_.empty() || !foreground_remote_work_.empty()) {
    const bool run_remote = !foreground_remote_work_.empty() &&
                            (ready_.empty() || foreground_remote_turn_);
    if (run_remote) {
      RemoteWork *work = foreground_remote_work_.front();
      foreground_remote_work_.pop_front();
      TaskClassGuard task_class_guard(TaskClass::kForeground);
      RunRemoteWork(work);
    } else {
      ReadyTask ready = ready_.front();
      ready_.pop_front();
      ResumeReady(ready, TaskClass::kForeground);
    }
    foreground_remote_turn_ = !foreground_remote_turn_;
    ++resumed;
    if (CycleNow() >= deadline_cycles) {
      break;
    }
  }
  return resumed;
}

std::size_t Worker::DrainBackgroundUntil(std::int64_t deadline_cycles) {
  std::size_t resumed = 0;
  background_deadline_cycles_ = deadline_cycles;
  while (!background_ready_.empty() || !background_remote_work_.empty()) {
    const bool run_remote =
        !background_remote_work_.empty() &&
        (background_ready_.empty() || background_remote_turn_);
    if (run_remote) {
      RemoteWork *work = background_remote_work_.front();
      background_remote_work_.pop_front();
      TaskClassGuard task_class_guard(TaskClass::kBackground);
      RunRemoteWork(work);
    } else {
      ReadyTask ready = background_ready_.front();
      background_ready_.pop_front();
      ResumeReady(ready, TaskClass::kBackground);
    }
    background_remote_turn_ = !background_remote_turn_;
    ++resumed;
    if (CycleNow() >= deadline_cycles) {
      break;
    }
  }
  background_deadline_cycles_ = 0;
  return resumed;
}

void Worker::MergeDeferred() {
  while (!next_ready_.empty()) {
    ready_.push_back(next_ready_.front());
    next_ready_.pop_front();
  }
  while (!next_background_ready_.empty()) {
    background_ready_.push_back(next_background_ready_.front());
    next_background_ready_.pop_front();
  }
}

bool Worker::ShouldRunBackground() const noexcept {
  if (background_ready_.empty() && background_remote_work_.empty()) {
    return false;
  }
  if (ready_.empty() && foreground_remote_work_.empty()) {
    return true;
  }
  if (foreground_runtime_cycles_ == 0) {
    return true;
  }
  const std::uint64_t total =
      foreground_runtime_cycles_ + background_runtime_cycles_;
  const std::uint64_t warrant =
      total * options_.background_warrant_percent / 100;
  return background_runtime_cycles_ <= warrant;
}

void Worker::MaybeResetRuntimeWindow() noexcept {
  const std::uint64_t total =
      foreground_runtime_cycles_ + background_runtime_cycles_;
  if (total >= runtime_window_cycles_ &&
      (background_runtime_cycles_ <= foreground_runtime_cycles_ ||
       total >= 5 * runtime_window_cycles_)) {
    foreground_runtime_cycles_ = 0;
    background_runtime_cycles_ = 0;
  }
}

void Worker::RecordRound(std::int64_t start_cycles) noexcept {
  const std::uint64_t elapsed =
      static_cast<std::uint64_t>(CycleNow() - start_cycles);
  ++scheduler_stats_.rounds;
  scheduler_stats_.round_cycles += elapsed;
  scheduler_stats_.max_round_cycles =
      std::max(scheduler_stats_.max_round_cycles, elapsed);
}

Worker::SchedulerStats Worker::TakeSchedulerStats() noexcept {
  SchedulerStats result = std::exchange(scheduler_stats_, SchedulerStats{});
  result.cycles_per_second = cycle_frequency_;
  scheduler_stats_.cycles_per_second = cycle_frequency_;
  return result;
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

    connection->last_error = absl::Status(absl::StatusCode::kDeadlineExceeded,
                                          "connection idle timeout");
    BeginClose(connection, connection->last_error, CloseMode::kIdleTimeout);
  }
}

void Worker::RunRemoteWork(RemoteWork *work) {
  work->run_fn(work);
  if (!work->reply_deferred) {
    PostReply(cross_core_, work->origin, work);
  }
}

bool Worker::DrainCrossCore() {
  WorkerMailbox &mb = cross_core_->mailbox(id_);
  RemoteWork *batch[64];
  RemoteNotification notifications[64];
  std::size_t nreq = 0;
  std::size_t nrep = 0;
  std::size_t nnotifications = 0;
  const auto schedule_request = [this](RemoteWork *work) {
    if (work->task_class == TaskClass::kBackground) {
      background_remote_work_.push_back(work);
    } else {
      foreground_remote_work_.push_back(work);
    }
  };

  // The bounded SPSC lane is the normal path. Only touch the old MPSC queues
  // when a producer reported a full lane.
  if (mb.overflow_pending.exchange(false, std::memory_order_acq_rel)) {
    const std::size_t overflow_requests =
        mb.requests.try_dequeue_bulk(batch, 64);
    for (std::size_t i = 0; i < overflow_requests; ++i) {
      schedule_request(batch[i]);
    }
    nreq += overflow_requests;

    const std::size_t overflow_replies = mb.replies.try_dequeue_bulk(batch, 64);
    for (std::size_t i = 0; i < overflow_replies; ++i) {
      Enqueue(batch[i]->waiter);
    }
    nrep += overflow_replies;

    const std::size_t overflow_notifications =
        mb.notifications.try_dequeue_bulk(notifications, 64);
    for (std::size_t i = 0; i < overflow_notifications; ++i) {
      RemoteNotification &notification = notifications[i];
      notification.run_fn(notification.context, notification.value);
    }
    nnotifications += overflow_notifications;

    if (overflow_requests == 64 || overflow_replies == 64 ||
        overflow_notifications == 64) {
      mb.overflow_pending.store(true, std::memory_order_release);
    }
  }

  // Scan one independent pending flag per sender lane. Producers never contend
  // with each other, and empty lanes avoid touching all three ring indices.
  // One bounded batch per message kind across all lanes keeps io fair.
  for (unsigned sender = 0; sender < cross_core_->size(); ++sender) {
    CrossCoreLane &lane = cross_core_->lane(id_, sender);
    if (!lane.pending.load(std::memory_order_acquire) ||
        !lane.pending.exchange(false, std::memory_order_acq_rel)) {
      continue;
    }

    if (nreq < 64) {
      const std::size_t count =
          lane.requests.try_dequeue_bulk(batch, 64 - nreq);
      for (std::size_t i = 0; i < count; ++i) {
        schedule_request(batch[i]);
      }
      nreq += count;
    }

    if (nrep < 64) {
      const std::size_t count = lane.replies.try_dequeue_bulk(batch, 64 - nrep);
      for (std::size_t i = 0; i < count; ++i) {
        Enqueue(batch[i]->waiter);
      }
      nrep += count;
    }

    if (nnotifications < 64) {
      const std::size_t count = lane.notifications.try_dequeue_bulk(
          notifications, 64 - nnotifications);
      for (std::size_t i = 0; i < count; ++i) {
        RemoteNotification &notification = notifications[i];
        notification.run_fn(notification.context, notification.value);
      }
      nnotifications += count;
    }
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
    ++wake_checks_;
    if (mb.wake_seq.fetch_add(1, std::memory_order_acq_rel) == kWakeSeqParked) {
      ++wake_sent_;
      backend_.WakeRemote(mb.ring_fd);
    }
  }
  w.wake_list.clear();
}

bool Worker::BusyPoll() {
  if (options_.busy_poll_us == 0) {
    return false;
  }
  const std::int64_t deadline =
      CycleNow() + static_cast<std::int64_t>(busy_poll_cycles_);
  do {
    bool did_work = DrainCrossCore();
    did_work |= backend_.Poll();
    if (did_work) {
      const std::int64_t round_start = CycleNow();
      MergeDeferred();
      const std::int64_t foreground_start = CycleNow();
      const std::size_t foreground_resumes =
          DrainReadyUntil(foreground_start +
                          static_cast<std::int64_t>(foreground_budget_cycles_));
      const std::uint64_t foreground_cycles =
          static_cast<std::uint64_t>(CycleNow() - foreground_start);
      if (foreground_resumes != 0) {
        foreground_runtime_cycles_ += foreground_cycles;
        scheduler_stats_.foreground_resumes += foreground_resumes;
        scheduler_stats_.foreground_cycles += foreground_cycles;
        scheduler_stats_.max_foreground_cycles =
            std::max(scheduler_stats_.max_foreground_cycles, foreground_cycles);
        scheduler_stats_.foreground_overruns +=
            foreground_cycles > foreground_budget_cycles_;
      }
      Flush();
      ReclaimConnections();
      MaybeResetRuntimeWindow();
      RecordRound(round_start);
      return true;
    }
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#else
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
  } while (CycleNow() < deadline);
  return false;
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

  const std::int64_t round_start = CycleNow();

  // 1. Poll external work once, then run online coroutines within their normal
  //    budget. Deferred Yield continuations are appended after fresh I/O and
  //    cross-core work so yielding really returns control to the event loop.
  bool did_work =
      !ready_.empty() || !next_ready_.empty() ||
      !foreground_remote_work_.empty() || !background_ready_.empty() ||
      !next_background_ready_.empty() || !background_remote_work_.empty();
  did_work |= DrainCrossCore();
  did_work |= backend_.Poll();
  MergeDeferred();

  const std::int64_t foreground_start = CycleNow();
  const std::size_t foreground_resumes = DrainReadyUntil(
      foreground_start + static_cast<std::int64_t>(foreground_budget_cycles_));
  const std::uint64_t foreground_cycles =
      static_cast<std::uint64_t>(CycleNow() - foreground_start);
  if (foreground_resumes != 0) {
    did_work = true;
    foreground_runtime_cycles_ += foreground_cycles;
    scheduler_stats_.foreground_resumes += foreground_resumes;
    scheduler_stats_.foreground_cycles += foreground_cycles;
    scheduler_stats_.max_foreground_cycles =
        std::max(scheduler_stats_.max_foreground_cycles, foreground_cycles);
    scheduler_stats_.foreground_overruns +=
        foreground_cycles > foreground_budget_cycles_;
  }

  // 2. Give maintenance a bounded slice when the worker is otherwise idle or
  //    its rolling CPU share is below the warrant. Background tasks remain
  //    cooperative and may overrun only until their next Yield checkpoint.
  if (ShouldRunBackground()) {
    const std::int64_t background_start = CycleNow();
    const std::size_t background_resumes =
        DrainBackgroundUntil(background_start + static_cast<std::int64_t>(
                                                    background_budget_cycles_));
    const std::uint64_t background_cycles =
        static_cast<std::uint64_t>(CycleNow() - background_start);
    if (background_resumes != 0) {
      did_work = true;
      background_runtime_cycles_ += background_cycles;
      scheduler_stats_.background_resumes += background_resumes;
      scheduler_stats_.background_cycles += background_cycles;
      scheduler_stats_.max_background_cycles =
          std::max(scheduler_stats_.max_background_cycles, background_cycles);
      scheduler_stats_.background_overruns +=
          background_cycles > background_budget_cycles_;
    }
  }

  // 3. The single submit point: flush queued SQEs (sends, recv re-arms) and
  //    batched cross-core wakes. Completions dispatched while parked are
  //    resumed by the next iteration, which always reaches here before it can
  //    block.
  Flush();
  ReclaimConnections();
  MaybeResetRuntimeWindow();
  RecordRound(round_start);

  if (!wait_for_completion) {
    return did_work;  // shutdown drain: stop once no progress is left
  }
  if (did_work) {
    return true;  // had work; loop again without parking
  }

  if (BusyPoll()) {
    return true;
  }

  // 4. Idle — park. Check idle timeouts only here, off the hot path. The
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
    return true;  // raced: work arrived; next iteration drains + submits it
  }
  std::uint32_t expected = seq;
  if (!mb.wake_seq.compare_exchange_strong(expected, kWakeSeqParked,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
    return true;  // a producer published work; loop again instead of parking
  }

  const int timeout_ms = options_.idle_timeout_ms > 0 ? 100 : -1;
  const bool ok = backend_.Wait(timeout_ms);  // blocks; dispatches on wake
  mb.wake_seq.store(0, std::memory_order_release);  // leave the parked state
  return ok;  // next iteration drains what Wait dispatched
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
    BeginClose(owned.get(),
               absl::Status(absl::StatusCode::kCancelled, "worker shutdown"),
               CloseMode::kWorkerShutdown);
  }
  while (!connections_.empty() && RunOnce(false)) {
  }
  // Frames of still-suspended detached tasks are reclaimed by the runtime
  // after every worker thread has joined (DestroyDetachedTasks): destroying
  // them here could race with another worker still holding cross-core
  // references into those frames.
}

}  // namespace celer
