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

#include "celer/io/storage.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <new>
#include <string>
#include <utility>

#include "celer/io/spdk_storage.h"
#include "celer/runtime/worker.h"

namespace celer {
#ifndef CELER_WITH_SPDK_STORAGE

bool IsSpdkStoragePath(std::string_view path) noexcept {
  return path.starts_with("spdk://");
}

absl::StatusOr<SpdkStorageDeviceInfo> ProbeSpdkStorage(std::string_view path) {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "SPDK storage path requires CELER_KERNEL_BYPASS=ON: " +
                          std::string(path));
}

absl::Status ReadSpdkStorage(std::string_view, std::span<std::byte>,
                             std::uint64_t) {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "SPDK storage is not compiled in");
}

absl::Status WriteSpdkStorage(std::string_view, std::span<const std::byte>,
                              std::uint64_t, bool) {
  return absl::Status(absl::StatusCode::kUnimplemented,
                      "SPDK storage is not compiled in");
}

void ReleaseSpdkStorageMetadataQpairs() noexcept {}

void* AllocateStorageBuffer(std::size_t bytes, std::size_t alignment) noexcept {
  FreezeIoBackends();
  return ::operator new[](bytes, std::align_val_t(alignment), std::nothrow);
}

void FreeStorageBuffer(void* buffer, std::size_t alignment) noexcept {
  ::operator delete[](buffer, std::align_val_t(alignment));
}

#endif  // CELER_WITH_SPDK_STORAGE

namespace {

absl::Status StorageError(int error, const char* operation) {
  std::string message = std::string(operation) + ": " + std::strerror(error) +
                        " (errno=" + std::to_string(error) + ")";
  switch (error) {
    case EAGAIN:
    case EBUSY:
      return absl::Status(absl::StatusCode::kUnavailable, std::move(message));
    case ECANCELED:
      return absl::Status(absl::StatusCode::kCancelled, std::move(message));
    case EINVAL:
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          std::move(message));
    case EBADF:
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          std::move(message));
    case ENOSPC:
    case EMFILE:
    case ENFILE:
    case ENOMEM:
      return absl::Status(absl::StatusCode::kResourceExhausted,
                          std::move(message));
    case ENOENT:
      return absl::Status(absl::StatusCode::kNotFound, std::move(message));
    default:
      return absl::Status(absl::StatusCode::kUnknown, std::move(message));
  }
}

}  // namespace

bool OneShotIoAwaitable::Suspend(std::coroutine_handle<> awaiting,
                                 absl::Status status) {
  awaiting_ = awaiting;
  if (!status.ok()) {
    immediate_status_.emplace(std::move(status));
    return false;
  }
  return true;
}

absl::StatusOr<int> OneShotIoAwaitable::Resume(const char* operation) {
  if (immediate_status_.has_value()) {
    return std::move(*immediate_status_);
  }
  if (result_ < 0) {
    return StorageError(-result_, operation);
  }
  return result_;
}

void OneShotIoAwaitable::Complete(Worker& worker, int result, unsigned flags) {
  (void)flags;
  result_ = result;
  worker.Enqueue(awaiting_);
}

SizeIoAwaitable::SizeIoAwaitable(Worker& worker, FixedFile file,
                                 FixedBuffer buffer, std::uint64_t offset,
                                 Operation operation) noexcept
    : worker_(&worker),
      file_(file),
      buffer_(buffer),
      offset_(offset),
      operation_(operation) {}

bool SizeIoAwaitable::await_suspend(std::coroutine_handle<> awaiting) {
  absl::Status status;
  switch (operation_) {
    case Operation::kRead:
      status = worker_->SubmitRead(
          file_, std::span<std::byte>(buffer_.data_, buffer_.size_), offset_,
          this);
      break;
    case Operation::kReadFixed:
      status = worker_->SubmitReadFixed(file_, buffer_, offset_, this);
      break;
    case Operation::kWrite:
      status = worker_->SubmitWrite(
          file_, std::span<const std::byte>(buffer_.data_, buffer_.size_),
          offset_, this);
      break;
    case Operation::kWriteFixed:
      status = worker_->SubmitWriteFixed(file_, buffer_, offset_, this);
      break;
  }
  return Suspend(awaiting, std::move(status));
}

absl::StatusOr<std::size_t> SizeIoAwaitable::await_resume() {
  const char* operation = nullptr;
  switch (operation_) {
    case Operation::kRead:
      operation = "read failed";
      break;
    case Operation::kReadFixed:
      operation = "fixed-buffer read failed";
      break;
    case Operation::kWrite:
      operation = "write failed";
      break;
    case Operation::kWriteFixed:
      operation = "fixed-buffer write failed";
      break;
  }
  auto result = Resume(operation);
  if (!result.ok()) {
    return result.status();
  }
  const std::size_t bytes = static_cast<std::size_t>(*result);
  switch (operation_) {
    case Operation::kRead:
    case Operation::kReadFixed:
      worker_->RecordStorageReadCompletion(bytes);
      break;
    case Operation::kWrite:
    case Operation::kWriteFixed:
      worker_->RecordStorageWriteCompletion(bytes);
      break;
  }
  return bytes;
}

OpenFixedFileAwaitable::OpenFixedFileAwaitable(Worker& worker, std::string path,
                                               int flags, mode_t mode,
                                               FixedFile file)
    : worker_(&worker),
      path_(std::move(path)),
      flags_(flags),
      mode_(mode),
      file_(file) {}

bool OpenFixedFileAwaitable::await_suspend(std::coroutine_handle<> awaiting) {
  return Suspend(awaiting,
                 worker_->SubmitOpenDirect(path_, flags_, mode_, file_, this));
}

absl::Status OpenFixedFileAwaitable::await_resume() {
  auto result = Resume("open fixed O_DIRECT file failed");
  return result.ok() ? absl::OkStatus() : result.status();
}

FileStatusAwaitable::FileStatusAwaitable(Worker& worker, FixedFile file,
                                         Operation operation) noexcept
    : worker_(&worker), file_(file), operation_(operation) {}

bool FileStatusAwaitable::await_suspend(std::coroutine_handle<> awaiting) {
  absl::Status status = operation_ == Operation::kClose
                            ? worker_->SubmitCloseDirect(file_, this)
                            : worker_->SubmitFdatasync(file_, this);
  if (status.ok() && operation_ == Operation::kFdatasync) {
    durability_target_bytes_ = worker_->StorageWriteSubmissionBytes(file_);
  }
  return Suspend(awaiting, std::move(status));
}

absl::Status FileStatusAwaitable::await_resume() {
  auto result =
      Resume(operation_ == Operation::kClose ? "close fixed file failed"
                                             : "fixed-file fdatasync failed");
  if (result.ok() && operation_ == Operation::kFdatasync) {
    worker_->RecordFdatasyncCompletion(file_, durability_target_bytes_);
  }
  return result.ok() ? absl::OkStatus() : result.status();
}

TimeoutAwaitable::TimeoutAwaitable(Worker& worker,
                                   std::chrono::nanoseconds duration) noexcept
    : worker_(&worker), duration_(duration) {
  const auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(duration);
  const auto nanoseconds =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration - seconds);
  timeout_.tv_sec = seconds.count();
  timeout_.tv_nsec = nanoseconds.count();
}

bool TimeoutAwaitable::await_suspend(std::coroutine_handle<> awaiting) {
  if (duration_.count() <= 0) {
    return Suspend(awaiting, absl::Status(absl::StatusCode::kInvalidArgument,
                                          "sleep duration must be positive"));
  }
  return Suspend(awaiting, worker_->SubmitTimeout(timeout_, this));
}

absl::Status TimeoutAwaitable::await_resume() {
  if (has_immediate_status()) {
    return TakeImmediateStatus();
  }
  if (result() == -ETIME || result() == 0) {
    return absl::OkStatus();
  }
  return StorageError(-result(), "io_uring timeout failed");
}

void CancellableTimerState::Complete(Worker& worker, int result,
                                     unsigned flags) {
  (void)flags;
  completed_ = true;
  result_ = result;
  if (waiter_) {
    worker.Enqueue(waiter_);
  }
}

void TimerCancelHandle::Cancel() const noexcept {
  if (state_ == nullptr) {
    return;
  }
  state_->cancelled_.store(true, std::memory_order_release);
  // io_uring_prep_cancel must be issued by the owning worker (the ring is
  // single-issuer). From any other thread the flag alone carries the cancel;
  // the pending fire then resolves as a kCancelled no-op at its deadline.
  if (ThisWorker().self_ == state_->worker_ && !state_->completed_) {
    (void)state_->worker_->SubmitCancel(state_.get());
  }
}

CancellableTimerAwaitable::CancellableTimerAwaitable(
    Worker& worker, std::chrono::nanoseconds duration) noexcept
    : worker_(&worker),
      duration_(duration),
      state_(std::make_shared<CancellableTimerState>(&worker)) {
  const auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(duration);
  const auto nanoseconds =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration - seconds);
  timeout_.tv_sec = seconds.count();
  timeout_.tv_nsec = nanoseconds.count();
}

bool CancellableTimerAwaitable::await_suspend(
    std::coroutine_handle<> awaiting) {
  state_->waiter_ = awaiting;
  if (duration_.count() <= 0) {
    immediate_status_.emplace(absl::Status(absl::StatusCode::kInvalidArgument,
                                           "sleep duration must be positive"));
    return false;
  }
  // A cancel that landed before the co_await never touches the ring.
  if (state_->cancelled_.load(std::memory_order_acquire)) {
    immediate_status_.emplace(
        absl::Status(absl::StatusCode::kCancelled, "timer cancelled"));
    return false;
  }
  absl::Status status = worker_->SubmitTimeout(timeout_, state_.get());
  if (!status.ok()) {
    immediate_status_.emplace(std::move(status));
    return false;
  }
  return true;
}

absl::Status CancellableTimerAwaitable::await_resume() {
  if (immediate_status_.has_value()) {
    return std::move(*immediate_status_);
  }
  // The cancelled flag wins over the raw CQE result: a fire that raced with
  // the cancel (or arrived after a foreign-thread cancel) is a no-op.
  if (state_->cancelled_.load(std::memory_order_acquire)) {
    return absl::Status(absl::StatusCode::kCancelled, "timer cancelled");
  }
  if (state_->result_ == -ETIME || state_->result_ == 0) {
    return absl::OkStatus();
  }
  return StorageError(-state_->result_, "io_uring timeout failed");
}

OpenFixedFileAwaitable OpenFixedFile(Worker& worker, std::string path,
                                     int flags, mode_t mode, FixedFile file) {
  return OpenFixedFileAwaitable(worker, std::move(path), flags, mode, file);
}

FileStatusAwaitable CloseFixedFile(Worker& worker, FixedFile file) {
  return FileStatusAwaitable(worker, file,
                             FileStatusAwaitable::Operation::kClose);
}

SizeIoAwaitable ReadFixed(Worker& worker, FixedFile file, FixedBuffer buffer,
                          std::uint64_t offset) {
  return SizeIoAwaitable(worker, file, buffer, offset,
                         SizeIoAwaitable::Operation::kReadFixed);
}

SizeIoAwaitable Read(Worker& worker, FixedFile file,
                     std::span<std::byte> buffer, std::uint64_t offset) {
  return SizeIoAwaitable(
      worker, file,
      FixedBuffer{.data_ = buffer.data(), .size_ = buffer.size(), .index_ = 0},
      offset, SizeIoAwaitable::Operation::kRead);
}

SizeIoAwaitable Write(Worker& worker, FixedFile file,
                      std::span<const std::byte> buffer, std::uint64_t offset) {
  return SizeIoAwaitable(
      worker, file,
      FixedBuffer{.data_ = const_cast<std::byte*>(buffer.data()),
                  .size_ = buffer.size(),
                  .index_ = 0},
      offset, SizeIoAwaitable::Operation::kWrite);
}

SizeIoAwaitable WriteFixed(Worker& worker, FixedFile file, FixedBuffer buffer,
                           std::uint64_t offset) {
  return SizeIoAwaitable(worker, file, buffer, offset,
                         SizeIoAwaitable::Operation::kWriteFixed);
}

FileStatusAwaitable Fdatasync(Worker& worker, FixedFile file) {
  return FileStatusAwaitable(worker, file,
                             FileStatusAwaitable::Operation::kFdatasync);
}

TimeoutAwaitable SleepFor(Worker& worker, std::chrono::nanoseconds duration) {
  return TimeoutAwaitable(worker, duration);
}

CancellableTimerAwaitable CancellableSleepFor(
    Worker& worker, std::chrono::nanoseconds duration) {
  return CancellableTimerAwaitable(worker, duration);
}

}  // namespace celer
