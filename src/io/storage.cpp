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
#include <coroutine>
#include <cstring>
#include <optional>
#include <string>
#include <utility>

#include "celer/io/completion.h"
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

template <typename Submit>
class StorageOperation final : public IoCompletion {
 public:
  StorageOperation(const char* operation, Submit submit)
      : operation_(operation), submit_(std::move(submit)) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> awaiting) {
    awaiting_ = awaiting;
    Status status = submit_(this);
    if (!status.ok()) {
      immediate_status_.emplace(std::move(status));
      return false;
    }
    return true;
  }

  StatusOr<int> await_resume() {
    if (immediate_status_.has_value()) {
      return std::move(*immediate_status_);
    }
    if (result_ < 0) {
      return StorageError(-result_, operation_);
    }
    return result_;
  }

  void Complete(Worker& worker, int result, unsigned flags) override {
    (void)flags;
    result_ = result;
    worker.Enqueue(awaiting_);
  }

 private:
  const char* operation_;
  Submit submit_;
  std::optional<Status> immediate_status_;
  int result_ = 0;
};

class TimeoutOperation final : public IoCompletion {
 public:
  TimeoutOperation(Worker& worker, std::chrono::milliseconds duration)
      : worker_(&worker) {
    const auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(duration);
    const auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
        duration - seconds);
    timeout_.tv_sec = seconds.count();
    timeout_.tv_nsec = nanoseconds.count();
  }

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> awaiting) {
    awaiting_ = awaiting;
    Status status = worker_->SubmitTimeout(timeout_, this);
    if (!status.ok()) {
      immediate_status_.emplace(std::move(status));
      return false;
    }
    return true;
  }

  Status await_resume() {
    if (immediate_status_.has_value()) {
      return std::move(*immediate_status_);
    }
    if (result_ == -ETIME || result_ == 0) {
      return Status::Ok();
    }
    return StorageError(-result_, "io_uring timeout failed");
  }

  void Complete(Worker& worker, int result, unsigned flags) override {
    (void)flags;
    result_ = result;
    worker.Enqueue(awaiting_);
  }

 private:
  Worker* worker_ = nullptr;
  __kernel_timespec timeout_{};
  std::optional<Status> immediate_status_;
  int result_ = 0;
};

template <typename Submit>
StorageOperation<Submit> MakeStorageOperation(const char* operation,
                                              Submit submit) {
  return StorageOperation<Submit>(operation, std::move(submit));
}

}  // namespace

Task<Status> OpenFixedFile(Worker& worker, std::string path, int flags,
                           mode_t mode, FixedFile file) {
  auto result = co_await MakeStorageOperation(
      "open fixed O_DIRECT file failed",
      [&worker, &path, flags, mode, file](IoCompletion* completion) {
        return worker.SubmitOpenDirect(path, flags, mode, file, completion);
      });
  if (!result.ok()) {
    co_return result.status();
  }
  co_return Status::Ok();
}

Task<Status> CloseFixedFile(Worker& worker, FixedFile file) {
  auto result = co_await MakeStorageOperation(
      "close fixed file failed",
      [&worker, file](IoCompletion* completion) {
        return worker.SubmitCloseDirect(file, completion);
      });
  if (!result.ok()) {
    co_return result.status();
  }
  co_return Status::Ok();
}

Task<StatusOr<std::size_t>> ReadFixed(Worker& worker, FixedFile file,
                                      FixedBuffer buffer,
                                      std::uint64_t offset) {
  auto result = co_await MakeStorageOperation(
      "fixed-buffer read failed",
      [&worker, file, buffer, offset](IoCompletion* completion) {
        return worker.SubmitReadFixed(file, buffer, offset, completion);
      });
  if (!result.ok()) {
    co_return result.status();
  }
  co_return static_cast<std::size_t>(*result);
}

Task<StatusOr<std::size_t>> Read(Worker& worker, FixedFile file,
                                 std::span<std::byte> buffer,
                                 std::uint64_t offset) {
  auto result = co_await MakeStorageOperation(
      "read failed",
      [&worker, file, buffer, offset](IoCompletion* completion) {
        return worker.SubmitRead(file, buffer, offset, completion);
      });
  if (!result.ok()) {
    co_return result.status();
  }
  co_return static_cast<std::size_t>(*result);
}

Task<StatusOr<std::size_t>> Write(Worker& worker, FixedFile file,
                                  std::span<const std::byte> buffer,
                                  std::uint64_t offset) {
  auto result = co_await MakeStorageOperation(
      "write failed",
      [&worker, file, buffer, offset](IoCompletion* completion) {
        return worker.SubmitWrite(file, buffer, offset, completion);
      });
  if (!result.ok()) {
    co_return result.status();
  }
  co_return static_cast<std::size_t>(*result);
}

Task<StatusOr<std::size_t>> WriteFixed(Worker& worker, FixedFile file,
                                       FixedBuffer buffer,
                                       std::uint64_t offset) {
  auto result = co_await MakeStorageOperation(
      "fixed-buffer write failed",
      [&worker, file, buffer, offset](IoCompletion* completion) {
        return worker.SubmitWriteFixed(file, buffer, offset, completion);
      });
  if (!result.ok()) {
    co_return result.status();
  }
  co_return static_cast<std::size_t>(*result);
}

Task<Status> Fdatasync(Worker& worker, FixedFile file) {
  auto result = co_await MakeStorageOperation(
      "fixed-file fdatasync failed",
      [&worker, file](IoCompletion* completion) {
        return worker.SubmitFdatasync(file, completion);
      });
  if (!result.ok()) {
    co_return result.status();
  }
  co_return Status::Ok();
}

Task<Status> SleepFor(Worker& worker, std::chrono::milliseconds duration) {
  if (duration.count() <= 0) {
    co_return Status(StatusCode::kInvalidArgument,
                     "sleep duration must be positive");
  }
  co_return co_await TimeoutOperation(worker, duration);
}

}  // namespace celer
