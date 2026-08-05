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
#include <string>
#include <utility>

#include "celer/runtime/worker.h"

namespace celer {
namespace {

Status StorageError(int error, const char* operation) {
  std::string message = std::string(operation) + ": " + std::strerror(error) +
                        " (errno=" + std::to_string(error) + ")";
  switch (error) {
    case EAGAIN:
    case EBUSY:
      return Status(StatusCode::kUnavailable, std::move(message));
    case ECANCELED:
      return Status(StatusCode::kCancelled, std::move(message));
    case EINVAL:
      return Status(StatusCode::kInvalidArgument, std::move(message));
    case EBADF:
      return Status(StatusCode::kFailedPrecondition, std::move(message));
    case ENOSPC:
    case EMFILE:
    case ENFILE:
    case ENOMEM:
      return Status(StatusCode::kResourceExhausted, std::move(message));
    case ENOENT:
      return Status(StatusCode::kNotFound, std::move(message));
    default:
      return Status(StatusCode::kUnknown, std::move(message));
  }
}

}  // namespace

bool OneShotIoAwaitable::Suspend(std::coroutine_handle<> awaiting,
                                 Status status) {
  awaiting_ = awaiting;
  if (!status.ok()) {
    immediate_status_.emplace(std::move(status));
    return false;
  }
  return true;
}

StatusOr<int> OneShotIoAwaitable::Resume(const char* operation) {
  if (immediate_status_.has_value()) {
    return std::move(*immediate_status_);
  }
  if (result_ < 0) {
    return StorageError(-result_, operation);
  }
  return result_;
}

void OneShotIoAwaitable::Complete(Worker& worker, int result,
                                  unsigned flags) {
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
  Status status;
  switch (operation_) {
    case Operation::kRead:
      status = worker_->SubmitRead(
          file_, std::span<std::byte>(buffer_.data, buffer_.size), offset_,
          this);
      break;
    case Operation::kReadFixed:
      status = worker_->SubmitReadFixed(file_, buffer_, offset_, this);
      break;
    case Operation::kWrite:
      status = worker_->SubmitWrite(
          file_, std::span<const std::byte>(buffer_.data, buffer_.size),
          offset_, this);
      break;
    case Operation::kWriteFixed:
      status = worker_->SubmitWriteFixed(file_, buffer_, offset_, this);
      break;
  }
  return Suspend(awaiting, std::move(status));
}

StatusOr<std::size_t> SizeIoAwaitable::await_resume() {
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
  return static_cast<std::size_t>(*result);
}

OpenFixedFileAwaitable::OpenFixedFileAwaitable(
    Worker& worker, std::string path, int flags, mode_t mode, FixedFile file)
    : worker_(&worker),
      path_(std::move(path)),
      flags_(flags),
      mode_(mode),
      file_(file) {}

bool OpenFixedFileAwaitable::await_suspend(
    std::coroutine_handle<> awaiting) {
  return Suspend(awaiting, worker_->SubmitOpenDirect(
                               path_, flags_, mode_, file_, this));
}

Status OpenFixedFileAwaitable::await_resume() {
  auto result = Resume("open fixed O_DIRECT file failed");
  return result.ok() ? Status::Ok() : result.status();
}

FileStatusAwaitable::FileStatusAwaitable(Worker& worker, FixedFile file,
                                         Operation operation) noexcept
    : worker_(&worker), file_(file), operation_(operation) {}

bool FileStatusAwaitable::await_suspend(std::coroutine_handle<> awaiting) {
  Status status = operation_ == Operation::kClose
                      ? worker_->SubmitCloseDirect(file_, this)
                      : worker_->SubmitFdatasync(file_, this);
  return Suspend(awaiting, std::move(status));
}

Status FileStatusAwaitable::await_resume() {
  auto result = Resume(operation_ == Operation::kClose
                           ? "close fixed file failed"
                           : "fixed-file fdatasync failed");
  return result.ok() ? Status::Ok() : result.status();
}

TimeoutAwaitable::TimeoutAwaitable(
    Worker& worker, std::chrono::milliseconds duration) noexcept
    : worker_(&worker), duration_(duration) {
  const auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(duration);
  const auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
      duration - seconds);
  timeout_.tv_sec = seconds.count();
  timeout_.tv_nsec = nanoseconds.count();
}

bool TimeoutAwaitable::await_suspend(std::coroutine_handle<> awaiting) {
  if (duration_.count() <= 0) {
    return Suspend(awaiting,
                   Status(StatusCode::kInvalidArgument,
                          "sleep duration must be positive"));
  }
  return Suspend(awaiting, worker_->SubmitTimeout(timeout_, this));
}

Status TimeoutAwaitable::await_resume() {
  if (has_immediate_status()) {
    return TakeImmediateStatus();
  }
  if (result() == -ETIME || result() == 0) {
    return Status::Ok();
  }
  return StorageError(-result(), "io_uring timeout failed");
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
      FixedBuffer{.data = buffer.data(), .size = buffer.size(), .index = 0},
      offset, SizeIoAwaitable::Operation::kRead);
}

SizeIoAwaitable Write(Worker& worker, FixedFile file,
                      std::span<const std::byte> buffer,
                      std::uint64_t offset) {
  return SizeIoAwaitable(
      worker, file,
      FixedBuffer{.data = const_cast<std::byte*>(buffer.data()),
                  .size = buffer.size(),
                  .index = 0},
      offset, SizeIoAwaitable::Operation::kWrite);
}

SizeIoAwaitable WriteFixed(Worker& worker, FixedFile file,
                           FixedBuffer buffer, std::uint64_t offset) {
  return SizeIoAwaitable(worker, file, buffer, offset,
                         SizeIoAwaitable::Operation::kWriteFixed);
}

FileStatusAwaitable Fdatasync(Worker& worker, FixedFile file) {
  return FileStatusAwaitable(worker, file,
                             FileStatusAwaitable::Operation::kFdatasync);
}

TimeoutAwaitable SleepFor(Worker& worker,
                          std::chrono::milliseconds duration) {
  return TimeoutAwaitable(worker, duration);
}

}  // namespace celer
