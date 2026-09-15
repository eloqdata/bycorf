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
#include <bit>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

#include "absl/base/internal/cycleclock.h"
#include "celer/net/socket_ops.h"
#include "celer/runtime/cycle_clock.h"
#if defined(__x86_64__)
#include "absl/base/internal/sysinfo.h"
#endif
#include "spdlog/spdlog.h"

namespace celer {

#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
std::uint64_t CrossCoreTraceNowNanos() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
#endif

namespace {

std::int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

std::int64_t CycleNow() noexcept {
#if defined(__x86_64__)
  std::uint64_t low = 0;
  std::uint64_t high = 0;
  __asm__ volatile("rdtsc" : "=a"(low), "=d"(high) : : "memory");
  return static_cast<std::int64_t>((high << 32U) | low);
#elif defined(__aarch64__)
  std::int64_t counter = 0;
  __asm__ volatile("mrs %0, cntvct_el0" : "=r"(counter) : : "memory");
  return counter;
#else
  return absl::base_internal::CycleClock::Now();
#endif
}

double CycleFrequency() noexcept {
#if defined(__x86_64__)
  return absl::base_internal::NominalCPUFrequency();
#elif defined(__aarch64__)
  std::uint64_t frequency = 0;
  __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(frequency));
  return static_cast<double>(frequency);
#else
  return absl::base_internal::CycleClock::Frequency();
#endif
}

const char* CycleCounterName() noexcept {
#if defined(__x86_64__)
  return "rdtsc";
#elif defined(__aarch64__)
  return "cntvct_el0";
#else
  return "absl::CycleClock";
#endif
}

const char* CycleFrequencySourceName() noexcept {
#if defined(__x86_64__)
  return "absl::NominalCPUFrequency";
#elif defined(__aarch64__)
  return "cntfrq_el0";
#else
  return "absl::CycleClock::Frequency";
#endif
}

void LogCycleCounterInfo(double frequency) {
  static std::once_flag once;
  std::call_once(once, [frequency] {
    spdlog::info(
        "runtime cycle-counter={} frequency-source={} frequency={:.0f} Hz "
        "({:.3f} MHz) ticks-per-us={:.3f}",
        CycleCounterName(), CycleFrequencySourceName(), frequency,
        frequency / 1'000'000.0, frequency / 1'000'000.0);
  });
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

std::uint64_t ReadCycleCounter() noexcept {
  return static_cast<std::uint64_t>(CycleNow());
}

double CycleCounterFrequency() noexcept { return CycleFrequency(); }

void RegisterBackgroundTask(std::coroutine_handle<> handle) noexcept {
  if (ThisWorker().self_ != nullptr) {
    ThisWorker().self_->RegisterBackground(handle);
  }
}

void ForgetTaskScheduling(std::coroutine_handle<> handle) noexcept {
  if (ThisWorker().self_ != nullptr) {
    ThisWorker().self_->ForgetScheduling(handle);
  }
}

Worker::~Worker() { Shutdown(); }

absl::Status Worker::Init(const WorkerOptions& options) {
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
  if (options.foreground_budget_us_ == 0 ||
      options.background_budget_us_ == 0 ||
      options.background_warrant_percent_ > 100) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "worker scheduler budgets must be positive and background "
        "warrant must be <= 100%");
  }
  FreezeIoBackends();
  options_ = options;
  cycle_frequency_ = CycleFrequency();
  LogCycleCounterInfo(cycle_frequency_);
  foreground_budget_cycles_ =
      CyclesFromMicroseconds(cycle_frequency_, options_.foreground_budget_us_);
  background_budget_cycles_ =
      CyclesFromMicroseconds(cycle_frequency_, options_.background_budget_us_);
  busy_poll_cycles_ =
      CyclesFromMicroseconds(cycle_frequency_, options_.busy_poll_us_);
  spdk_foreground_pre_poll_cycles_ = CyclesFromMicroseconds(
      cycle_frequency_, options_.spdk_foreground_pre_poll_us_);
  runtime_window_cycles_ = CyclesFromMicroseconds(cycle_frequency_, 10'000);
  scheduler_stats_.cycles_per_second_ = cycle_frequency_;

  IoBackendOptions backend_options;
  backend_options.ring_entries_ = options.ring_entries_;
  backend_options.recv_buffer_count_ = options.recv_buffer_count_;
  // Runtime pre-created the eventfd; the backend reuses it (does not own it).
  auto status =
      backend_.Init(backend_options, this, cross_core_->mailbox(id_).wake_fd_);
  if (!status.ok()) {
    return status;
  }
#ifdef CELER_WITH_SPDK_STORAGE
  if (SpdkStorageEnabled()) status = storage_backend_.Init(this);
  if (!status.ok()) {
    backend_.Shutdown();
    return status;
  }
#endif

  // Advertise our ring so other workers can wake us via MSG_RING.
  cross_core_->mailbox(id_).ring_fd_ = backend_.WakeHandle();
  initialized_ = true;
  return absl::OkStatus();
}

void Worker::Shutdown() {
  if (!initialized_) {
    return;
  }
#ifdef CELER_WITH_SPDK_STORAGE
  storage_backend_.Shutdown();
#endif
  backend_.Shutdown();
  initialized_ = false;
}

void Worker::Spawn(Task<absl::Status> task) {
  task.SetCompletionCallback(
      this, [](void* context, std::coroutine_handle<> completed) noexcept {
        static_cast<Worker*>(context)->Enqueue(completed, true);
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
      this, [](void* context, std::coroutine_handle<> completed) noexcept {
        static_cast<Worker*>(context)->Enqueue(completed, true);
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
      this, [](void* context, std::coroutine_handle<> completed) noexcept {
        static_cast<Worker*>(context)->Enqueue(completed, true);
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
    ThisWorker().self_->SpawnBackground(std::move(task));
  } else {
    ThisWorker().self_->Spawn(std::move(task));
  }
}

void Worker::RequestStop() noexcept {
  stop_requested_.store(true, std::memory_order_release);
  // Runtime creates this descriptor before launch and closes it after join.
  // Stop may race backend initialization/teardown, so do not read mutable
  // backend state from the requesting thread.
  if (cross_core_) {
    const std::uint64_t wake = 1;
    (void)::write(cross_core_->mailbox(id_).wake_fd_, &wake, sizeof(wake));
  }
}

bool Worker::NotifyWake() noexcept {
  if (stop_requested_.load(std::memory_order_acquire)) {
    stopping_.store(true, std::memory_order_release);
  }
  return stopping_.load(std::memory_order_acquire);
}

Connection* Worker::AddConnection(Connection connection) {
  const int fd = connection.file_.fd_;
  if (fd < 0) {
    return nullptr;
  }
  auto owned = std::make_unique<Connection>(std::move(connection));
  Connection* raw = owned.get();
  raw->id_ = next_connection_id_++;
  raw->last_active_ms_ = NowMs();
  connections_[raw->id_] = std::move(owned);
  ++active_connection_count_;
  return raw;
}

void Worker::BeginClose(Connection* connection, absl::Status reason,
                        CloseMode mode) noexcept {
  if (connection == nullptr) {
    return;
  }
  if (connection->state_ == ConnectionState::kRetired) {
    return;
  }

  if (connection->state_ == ConnectionState::kActive) {
    assert(active_connection_count_ != 0);
    --active_connection_count_;
  }
  if (!reason.ok() || connection->last_error_.ok()) {
    connection->last_error_ = std::move(reason);
  }

  connection->closing_ = true;
  connection->state_ = ConnectionState::kClosing;

  // Stop the independent peer-disconnect observer before retiring the
  // Connection storage. Its completion owns one inflight operation and will
  // make the connection reclaimable.
  (void)backend_.CancelPeerDisconnectPoll(connection);

  if (connection->file_.fd_ >= 0) {
    const int fd = connection->file_.fd_;
    connection->file_.fd_ = -1;
    connection->closed_ = true;
    detail::CloseSocket(fd);
  } else {
    connection->closed_ = true;
  }

  if (mode != CloseMode::kPeerClosed) {
    connection->recv_eof_ = false;
  }

  // Resume any reader waiting on this connection (deferred via the ready
  // queue).
  if (connection->read_waiter_) {
    auto waiter = connection->read_waiter_;
    connection->read_waiter_ = {};
    connection->read_inflight_ = false;
    Enqueue(waiter);
  }
  RetireConnection(connection);
}

void Worker::RetireConnection(Connection* connection) {
  if (connection == nullptr) {
    return;
  }
  if (connection->retired_) {
    return;
  }
  connection->retired_ = true;
  connection->closing_ = true;
  connection->state_ =
      connection->recv_armed_ || connection->inflight_ops_ != 0 ||
              connection->read_waiter_ || connection->read_inflight_ ||
              connection->write_inflight_ ||
              !connection->received_buffers_.empty()
          ? ConnectionState::kDraining
          : ConnectionState::kRetired;
  DiscardReceivedBuffers(connection);
  retired_connection_ids_.push_back(connection->id_);
}

void Worker::Enqueue(std::coroutine_handle<> handle, bool destroy_when_done) {
  if (!handle) {
    return;
  }
  ReadyTask ready{.handle_ = handle, .destroy_when_done_ = destroy_when_done};
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
    next_background_ready_.push_back(ReadyTask{.handle_ = handle});
  } else {
    next_ready_.push_back(ReadyTask{.handle_ = handle});
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

SpawnPromise& PromiseOf(void* address) noexcept {
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
  SpawnPromise& promise = PromiseOf(handle.address());
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
  std::vector<void*> tasks = std::move(detached_tasks_);
  detached_tasks_.clear();
  for (void* address : tasks) {
    const std::coroutine_handle<> handle =
        std::coroutine_handle<>::from_address(address);
    ForgetScheduling(handle);
    handle.destroy();
  }
  // Destroying a root cascades through its owned child Task frames. Clear any
  // scheduling-only entries that belonged to frames already reclaimed above.
  background_tasks_.clear();
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

absl::Status Worker::SetForegroundBudgetUs(unsigned microseconds) noexcept {
  if (microseconds == 0) {
    return absl::InvalidArgumentError(
        "foreground budget must be a positive number of microseconds");
  }
  options_.foreground_budget_us_ = microseconds;
  foreground_budget_cycles_ =
      CyclesFromMicroseconds(cycle_frequency_, microseconds);
  return absl::OkStatus();
}

absl::Status Worker::SetBackgroundBudgetUs(unsigned microseconds) noexcept {
  if (microseconds == 0) {
    return absl::InvalidArgumentError(
        "background budget must be a positive number of microseconds");
  }
  options_.background_budget_us_ = microseconds;
  background_budget_cycles_ =
      CyclesFromMicroseconds(cycle_frequency_, microseconds);
  return absl::OkStatus();
}

absl::Status Worker::SetBackgroundWarrantPercent(unsigned percent) noexcept {
  if (percent == 0 || percent > 100) {
    return absl::InvalidArgumentError(
        "background warrant percent must be between 1 and 100");
  }
  options_.background_warrant_percent_ = percent;
  return absl::OkStatus();
}

void Worker::SetSpdkMaxCompletionsPerPoll(unsigned completions) noexcept {
  options_.spdk_max_completions_per_poll_ = completions;
}

void Worker::ResumeReady(ReadyTask ready, TaskClass task_class) {
  auto handle = ready.handle_;
  if (handle.done()) {
    if (ready.destroy_when_done_) {
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
      RemoteWork* work = foreground_remote_work_.front();
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
      RemoteWork* work = background_remote_work_.front();
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
      total * options_.background_warrant_percent_ / 100;
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
  ++scheduler_stats_.rounds_;
  scheduler_stats_.round_cycles_ += elapsed;
  scheduler_stats_.max_round_cycles_ =
      std::max(scheduler_stats_.max_round_cycles_, elapsed);
}

Worker::SchedulerStats Worker::TakeSchedulerStats() noexcept {
  SchedulerStats result = std::exchange(scheduler_stats_, SchedulerStats{});
  result.cycles_per_second_ = cycle_frequency_;
  scheduler_stats_.cycles_per_second_ = cycle_frequency_;
  return result;
}

void Worker::LatencySampleStats::Add(std::uint64_t nanoseconds) noexcept {
  ++count_;
  sum_ns_ += nanoseconds;
  max_ns_ = std::max(max_ns_, nanoseconds);
  const std::uint64_t microseconds = (nanoseconds + 999) / 1000;
  const auto it = std::lower_bound(kBucketUpperUs.begin(), kBucketUpperUs.end(),
                                   microseconds);
  const std::size_t index =
      it == kBucketUpperUs.end()
          ? kBucketUpperUs.size() - 1
          : static_cast<std::size_t>(it - kBucketUpperUs.begin());
  ++buckets_[index];
}

double Worker::LatencySampleStats::AverageUs() const noexcept {
  return count_ == 0 ? 0.0
                     : static_cast<double>(sum_ns_) /
                           (1000.0 * static_cast<double>(count_));
}

std::uint64_t Worker::LatencySampleStats::PercentileUpperUs(
    double percentile) const noexcept {
  if (count_ == 0) {
    return 0;
  }
  const std::uint64_t target = static_cast<std::uint64_t>(
      static_cast<double>(count_) * percentile + 0.999999);
  std::uint64_t cumulative = 0;
  for (std::size_t i = 0; i < buckets_.size(); ++i) {
    cumulative += buckets_[i];
    if (cumulative >= target) {
      return kBucketUpperUs[i];
    }
  }
  return kBucketUpperUs.back();
}

void Worker::RecordStorageReadCompletion(std::size_t bytes) noexcept {
  ++storage_io_stats_.read_operations_;
  storage_io_stats_.read_bytes_ += bytes;
}

void Worker::RecordStorageWriteCompletion(std::size_t bytes) noexcept {
  ++storage_io_stats_.write_operations_;
  storage_io_stats_.write_bytes_ += bytes;
}

std::uint64_t Worker::StorageWriteSubmissionBytes(
    FixedFile file) const noexcept {
  if (file.index_ >= storage_file_io_stats_.size()) [[unlikely]] {
    return 0;
  }
  return storage_file_io_stats_[file.index_].submitted_write_bytes_;
}

void Worker::RecordFdatasyncCompletion(FixedFile file,
                                       std::uint64_t write_bytes) noexcept {
  ++storage_io_stats_.fdatasync_operations_;
  if (file.index_ >= storage_file_io_stats_.size()) [[unlikely]] {
    return;
  }
  StorageFileIoStats& stats = storage_file_io_stats_[file.index_];
  if (write_bytes > stats.durable_write_bytes_) {
    storage_io_stats_.fdatasync_bytes_ +=
        write_bytes - stats.durable_write_bytes_;
    stats.durable_write_bytes_ = write_bytes;
  }
}

bool Worker::CanReclaim(const Connection& connection) const noexcept {
  return connection.state_ != ConnectionState::kActive &&
         connection.inflight_ops_ == 0 && !connection.read_waiter_ &&
         !connection.read_inflight_ && !connection.write_inflight_ &&
         !connection.recv_armed_ && connection.received_buffers_.empty();
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
      connection->state_ = ConnectionState::kDraining;
      still_retired.push_back(connection_id);
      continue;
    }

    connection->state_ = ConnectionState::kRetired;
    connections_.erase(it);
  }

  retired_connection_ids_ = std::move(still_retired);
}

void Worker::DiscardReceivedBuffers(Connection* connection) {
  if (connection == nullptr) {
    return;
  }
  while (!connection->received_buffers_.empty()) {
    auto received = connection->received_buffers_.front();
    connection->received_buffers_.pop_front();
    backend_.ReleaseRecvBuffer(connection, received.buffer_id_);
  }
}

void Worker::CheckIdleConnections() {
  if (options_.idle_timeout_ms_ <= 0) {
    return;
  }

  const std::int64_t now_ms = NowMs();
  for (auto& [connection_id, owned] : connections_) {
    (void)connection_id;
    Connection* connection = owned.get();
    if (connection->retired_ || connection->closed_ ||
        connection->last_active_ms_ <= 0) {
      continue;
    }
    if (now_ms - connection->last_active_ms_ < options_.idle_timeout_ms_) {
      continue;
    }

    connection->last_error_ = absl::Status(absl::StatusCode::kDeadlineExceeded,
                                           "connection idle timeout");
    BeginClose(connection, connection->last_error_, CloseMode::kIdleTimeout);
  }
}

void Worker::RunRemoteWork(RemoteWork* work) {
  work->run_fn_(work);
  if (!work->reply_deferred_) {
    PostReply(cross_core_, work->origin_, work);
  }
}

bool Worker::DrainCrossCore() {
  WorkerMailbox& mb = cross_core_->mailbox(id_);
  RemoteWork* batch[64];
  // Both dequeue paths assign all fields of [0, count); never read the unused
  // suffix. Default initialization intentionally leaves this scratch untouched.
  RemoteNotification notifications[64];
  std::size_t nreq = 0;
  std::size_t nrep = 0;
  std::size_t nnotifications = 0;
  const auto schedule_request = [this](RemoteWork* work) {
#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
    const std::uint64_t now = CrossCoreTraceNowNanos();
    if (work->request_post_ns_ != 0 && now >= work->request_post_ns_) {
      cross_core_latency_stats_.request_queue_.Add(now -
                                                   work->request_post_ns_);
    }
#endif
    if (work->task_class_ == TaskClass::kBackground) {
      background_remote_work_.push_back(work);
    } else {
      foreground_remote_work_.push_back(work);
    }
  };
  const auto schedule_reply = [this](RemoteWork* work) {
#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
    const std::uint64_t now = CrossCoreTraceNowNanos();
    if (work->reply_post_ns_ != 0 && now >= work->reply_post_ns_) {
      cross_core_latency_stats_.reply_queue_.Add(now - work->reply_post_ns_);
    }
#endif
    Enqueue(work->waiter_);
  };

  // The bounded SPSC lane is the normal path. Only touch the old MPSC queues
  // when a producer reported a full lane. A stale false from the read-only
  // fast path only defers draining; the wake-sequence handshake prevents the
  // receiver from parking past a producer that published overflow work.
  if (mb.overflow_pending_.load(std::memory_order_relaxed) &&
      mb.overflow_pending_.exchange(false, std::memory_order_acquire)) {
    const std::size_t overflow_requests =
        mb.requests_.try_dequeue_bulk(batch, 64);
    for (std::size_t i = 0; i < overflow_requests; ++i) {
      schedule_request(batch[i]);
    }
    nreq += overflow_requests;

    const std::size_t overflow_replies =
        mb.replies_.try_dequeue_bulk(batch, 64);
    for (std::size_t i = 0; i < overflow_replies; ++i) {
      schedule_reply(batch[i]);
    }
    nrep += overflow_replies;

    const std::size_t overflow_notifications =
        mb.notifications_.try_dequeue_bulk(notifications, 64);
    for (std::size_t i = 0; i < overflow_notifications; ++i) {
      RemoteNotification& notification = notifications[i];
      notification.run_fn_(notification.context_, notification.value_);
    }
    nnotifications += overflow_notifications;

    if (overflow_requests == 64 || overflow_replies == 64 ||
        overflow_notifications == 64) {
      mb.overflow_pending_.store(true, std::memory_order_release);
    }
  }

  // Producers publish only inactive -> active lane transitions. At 64 workers
  // the receiver checks one bitmap word instead of 64 distant lane flags.
  for (unsigned word = 0; word < cross_core_->active_sender_word_count();
       ++word) {
    std::uint64_t active = cross_core_->TakeActiveSenders(id_, word);
    while (active != 0) {
      const unsigned bit = std::countr_zero(active);
      active &= active - 1;
      const unsigned sender = word * 64U + bit;
      if (sender >= cross_core_->size()) {
        continue;
      }

      CrossCoreLane& lane = cross_core_->lane(id_, sender);
      // Keep active=true while draining. A producer that races with the drain
      // may therefore coalesce into this visit. Clearing followed by an empty
      // check closes the case where it arrived after that message kind's tail
      // snapshot and did not publish another active bit.
      if (nreq < 64) {
        const std::size_t count =
            lane.requests_.try_dequeue_bulk(batch, 64 - nreq);
        for (std::size_t i = 0; i < count; ++i) {
          schedule_request(batch[i]);
        }
        nreq += count;
      }

      if (nrep < 64) {
        const std::size_t count =
            lane.replies_.try_dequeue_bulk(batch, 64 - nrep);
        for (std::size_t i = 0; i < count; ++i) {
          schedule_reply(batch[i]);
        }
        nrep += count;
      }

      if (nnotifications < 64) {
        const std::size_t count = lane.notifications_.try_dequeue_bulk(
            notifications, 64 - nnotifications);
        for (std::size_t i = 0; i < count; ++i) {
          RemoteNotification& notification = notifications[i];
          notification.run_fn_(notification.context_, notification.value_);
        }
        nnotifications += count;
      }

      // The producer publishes the ring entry before its active=true RMW. If
      // it observed this lane already active, it deliberately did not set a
      // second sender bit. Acquire that RMW while closing the lane so the
      // empty recheck cannot miss the entry and strand a pending inactive
      // lane forever.
      (void)lane.active_.exchange(false, std::memory_order_acq_rel);
      if (!lane.empty() &&
          !lane.active_.exchange(true, std::memory_order_acq_rel)) {
        cross_core_->ActivateSender(id_, sender);
      }
    }
  }

  return nreq > 0 || nrep > 0 || nnotifications > 0;
}

void Worker::FlushWakes() {
  CurrentWorker& w = MutableThisWorker();
  if (w.wake_list_.empty()) {
    return;
  }
  for (unsigned target : w.wake_list_) {
    // The first post was published immediately. If this round added more,
    // republish once after the burst: this preserves the receiver's lane-close
    // handshake without an active-state RMW for every intervening message.
    if (w.wake_pending_[target] == 2) {
      PublishCrossCoreLane(target);
    }
    w.wake_pending_[target] = 0;
    WorkerMailbox& mb = cross_core_->mailbox(target);
    ++wake_checks_;
    const bool parked =
        mb.wake_seq_.fetch_add(1, std::memory_order_acq_rel) == kWakeSeqParked;
#if CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
    const std::uint64_t now = CrossCoreTraceNowNanos();
    const std::uint64_t mark = w.wake_mark_ns_[target];
    if (mark != 0 && now >= mark) {
      const std::uint64_t wait = now - mark;
      cross_core_latency_stats_.wake_batch_wait_.Add(wait);
      if (parked) {
        cross_core_latency_stats_.parked_wake_batch_wait_.Add(wait);
      }
    }
    w.wake_mark_ns_[target] = 0;
#endif
    if (parked) {
      ++wake_sent_;
      backend_.WakeRemote(mb.ring_fd_);
    }
  }
  w.wake_list_.clear();
}

bool Worker::BusyPoll() {
  if (options_.busy_poll_us_ == 0) {
    return false;
  }
  const std::int64_t deadline =
      CycleNow() + static_cast<std::int64_t>(busy_poll_cycles_);
  do {
    bool did_work = DrainCrossCore();
    did_work |= backend_.Poll();
#ifdef CELER_WITH_SPDK_STORAGE
    did_work |= PollStorage();
#endif
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
        scheduler_stats_.foreground_resumes_ += foreground_resumes;
        scheduler_stats_.foreground_cycles_ += foreground_cycles;
        scheduler_stats_.max_foreground_cycles_ = std::max(
            scheduler_stats_.max_foreground_cycles_, foreground_cycles);
        scheduler_stats_.foreground_overruns_ +=
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

#ifdef CELER_WITH_SPDK_STORAGE
bool Worker::PollStorage() {
  if (!SpdkStorageEnabled()) return false;
  const std::int64_t start = CycleNow();
  const SpdkPollResult result =
      storage_backend_.Poll(options_.spdk_max_completions_per_poll_);
  const std::uint64_t elapsed = static_cast<std::uint64_t>(CycleNow() - start);
  ++scheduler_stats_.storage_poll_calls_;
  scheduler_stats_.storage_poll_empty_ += result.completions_ == 0;
  scheduler_stats_.storage_completions_ += result.completions_;
  scheduler_stats_.storage_max_completions_ = std::max<std::uint64_t>(
      scheduler_stats_.storage_max_completions_, result.completions_);
  scheduler_stats_.storage_poll_cycles_ += elapsed;
  scheduler_stats_.storage_max_poll_cycles_ =
      std::max(scheduler_stats_.storage_max_poll_cycles_, elapsed);
  return result.did_work_;
}
#endif

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
#ifdef CELER_WITH_SPDK_STORAGE
  if (SpdkStorageEnabled() && options_.spdk_foreground_pre_poll_us_ != 0 &&
      (!ready_.empty() || !next_ready_.empty() ||
       !foreground_remote_work_.empty())) {
    MergeDeferred();
    const std::int64_t pre_poll_start = CycleNow();
    const std::size_t pre_poll_resumes =
        DrainReadyUntil(pre_poll_start + static_cast<std::int64_t>(
                                             spdk_foreground_pre_poll_cycles_));
    if (pre_poll_resumes != 0) {
      did_work = true;
      const std::uint64_t pre_poll_cycles =
          static_cast<std::uint64_t>(CycleNow() - pre_poll_start);
      foreground_runtime_cycles_ += pre_poll_cycles;
      scheduler_stats_.foreground_resumes_ += pre_poll_resumes;
      scheduler_stats_.foreground_cycles_ += pre_poll_cycles;
      scheduler_stats_.max_foreground_cycles_ =
          std::max(scheduler_stats_.max_foreground_cycles_, pre_poll_cycles);
      scheduler_stats_.foreground_overruns_ +=
          pre_poll_cycles > spdk_foreground_pre_poll_cycles_;
    }
  }
  did_work |= PollStorage();
#endif
  MergeDeferred();

  const std::int64_t foreground_start = CycleNow();
  const std::size_t foreground_resumes = DrainReadyUntil(
      foreground_start + static_cast<std::int64_t>(foreground_budget_cycles_));
  const std::uint64_t foreground_cycles =
      static_cast<std::uint64_t>(CycleNow() - foreground_start);
  if (foreground_resumes != 0) {
    did_work = true;
    foreground_runtime_cycles_ += foreground_cycles;
    scheduler_stats_.foreground_resumes_ += foreground_resumes;
    scheduler_stats_.foreground_cycles_ += foreground_cycles;
    scheduler_stats_.max_foreground_cycles_ =
        std::max(scheduler_stats_.max_foreground_cycles_, foreground_cycles);
    scheduler_stats_.foreground_overruns_ +=
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
      scheduler_stats_.background_resumes_ += background_resumes;
      scheduler_stats_.background_cycles_ += background_cycles;
      scheduler_stats_.max_background_cycles_ =
          std::max(scheduler_stats_.max_background_cycles_, background_cycles);
      scheduler_stats_.background_overruns_ +=
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

#ifdef CELER_WITH_SPDK_STORAGE
  // SPDK is a polled-mode driver and has no fd that can wake io_uring. Keep
  // driving its completion queues while device I/O is outstanding.
  if (storage_backend_.HasOutstanding()) {
    return true;
  }
#endif

  // 4. Idle — park. Check idle timeouts only here, off the hot path. The
  // wake_seq
  //    handshake closes the lost-wakeup race: snapshot it, recheck the mailbox
  //    once, then CAS to the parked sentinel. The recheck catches work already
  //    enqueued; the CAS catches work published after the snapshot (a
  //    producer's fetch_add changes wake_seq, so the CAS fails and we loop
  //    instead of park).
  CheckIdleConnections();
  WorkerMailbox& mb = cross_core_->mailbox(id_);
  const std::uint32_t seq = mb.wake_seq_.load(std::memory_order_acquire);
  if (DrainCrossCore()) {
    return true;  // raced: work arrived; next iteration drains + submits it
  }
  std::uint32_t expected = seq;
  if (!mb.wake_seq_.compare_exchange_strong(expected, kWakeSeqParked,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
    return true;  // a producer published work; loop again instead of parking
  }

  const int timeout_ms = options_.idle_timeout_ms_ > 0 ? 100 : -1;
  const bool ok = backend_.Wait(timeout_ms);  // blocks; dispatches on wake
  mb.wake_seq_.store(0, std::memory_order_release);  // leave the parked state
  return ok;  // next iteration drains what Wait dispatched
}

void Worker::Run() {
  SetThisWorker(id_, cross_core_, this);
  // A peer may fail, or the server may stop, while this worker initializes.
  // Preserve that request instead of consuming its wake and restarting work.
  (void)NotifyWake();
  while (!stopping_.load(std::memory_order_acquire)) {
    if (!RunOnce(true)) [[unlikely]] {
      spdlog::error("worker loop exiting because RunOnce returned false");
      break;
    }
    ReclaimConnections();
  }

  for (auto& [connection_id, owned] : connections_) {
    (void)connection_id;
    BeginClose(owned.get(),
               absl::Status(absl::StatusCode::kCancelled, "worker shutdown"),
               CloseMode::kWorkerShutdown);
  }
  while (!connections_.empty() && RunOnce(false)) {
  }
  // Server reclaims still-suspended frames on their owning threads after all
  // workers leave their loops. Reclaiming them here could race with a peer
  // that still holds cross-core references into those frames.
}

}  // namespace celer
