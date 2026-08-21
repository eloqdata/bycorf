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

#ifndef CELER_IO_STORAGE_H_
#define CELER_IO_STORAGE_H_

#include <linux/time_types.h>
#include <sys/types.h>

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>

#include "absl/status/statusor.h"
#include "celer/io/completion.h"

namespace celer {

class Worker;

// Ring-local file-table reference. It is intentionally not an OS descriptor.
struct FixedFile {
  std::uint32_t index_ = 0;
};

// A range inside an iovec registered with this worker's ring.
struct FixedBuffer {
  std::byte* data_ = nullptr;
  std::size_t size_ = 0;
  std::uint16_t index_ = 0;
};

// Shared completion state for one-shot operations. Concrete awaitables are
// embedded directly in their caller's coroutine frame.
class OneShotIoAwaitable : public IoCompletion {
 public:
  OneShotIoAwaitable(const OneShotIoAwaitable&) = delete;
  OneShotIoAwaitable& operator=(const OneShotIoAwaitable&) = delete;
  OneShotIoAwaitable(OneShotIoAwaitable&&) = delete;
  OneShotIoAwaitable& operator=(OneShotIoAwaitable&&) = delete;

  void Complete(Worker& worker, int result, unsigned flags) override;

 protected:
  OneShotIoAwaitable() = default;

  bool Suspend(std::coroutine_handle<> awaiting, absl::Status status);
  absl::StatusOr<int> Resume(const char* operation);
  bool has_immediate_status() const noexcept {
    return immediate_status_.has_value();
  }
  absl::Status TakeImmediateStatus() { return std::move(*immediate_status_); }
  int result() const noexcept { return result_; }

 private:
  std::optional<absl::Status> immediate_status_;
  int result_ = 0;
};

class SizeIoAwaitable final : public OneShotIoAwaitable {
 public:
  enum class Operation : std::uint8_t {
    kRead,
    kReadFixed,
    kWrite,
    kWriteFixed,
  };

  SizeIoAwaitable(Worker& worker, FixedFile file, FixedBuffer buffer,
                  std::uint64_t offset, Operation operation) noexcept;

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::StatusOr<std::size_t> await_resume();

 private:
  Worker* worker_ = nullptr;
  FixedFile file_{};
  FixedBuffer buffer_{};
  std::uint64_t offset_ = 0;
  Operation operation_ = Operation::kRead;
};

class OpenFixedFileAwaitable final : public OneShotIoAwaitable {
 public:
  OpenFixedFileAwaitable(Worker& worker, std::string path, int flags,
                         mode_t mode, FixedFile file);

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::Status await_resume();

 private:
  Worker* worker_ = nullptr;
  std::string path_;
  int flags_ = 0;
  mode_t mode_ = 0;
  FixedFile file_{};
};

class FileStatusAwaitable final : public OneShotIoAwaitable {
 public:
  enum class Operation : std::uint8_t {
    kClose,
    kFdatasync,
  };

  FileStatusAwaitable(Worker& worker, FixedFile file,
                      Operation operation) noexcept;

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::Status await_resume();

 private:
  Worker* worker_ = nullptr;
  FixedFile file_{};
  Operation operation_ = Operation::kClose;
  std::uint64_t durability_target_bytes_ = 0;
};

class TimeoutAwaitable final : public OneShotIoAwaitable {
 public:
  TimeoutAwaitable(Worker& worker, std::chrono::nanoseconds duration) noexcept;

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> awaiting);
  absl::Status await_resume();

 private:
  Worker* worker_ = nullptr;
  std::chrono::nanoseconds duration_{};
  __kernel_timespec timeout_{};
};

OpenFixedFileAwaitable OpenFixedFile(Worker& worker, std::string path,
                                     int flags, mode_t mode, FixedFile file);
FileStatusAwaitable CloseFixedFile(Worker& worker, FixedFile file);
SizeIoAwaitable ReadFixed(Worker& worker, FixedFile file, FixedBuffer buffer,
                          std::uint64_t offset);
SizeIoAwaitable Read(Worker& worker, FixedFile file,
                     std::span<std::byte> buffer, std::uint64_t offset);
SizeIoAwaitable Write(Worker& worker, FixedFile file,
                      std::span<const std::byte> buffer, std::uint64_t offset);
SizeIoAwaitable WriteFixed(Worker& worker, FixedFile file, FixedBuffer buffer,
                           std::uint64_t offset);
FileStatusAwaitable Fdatasync(Worker& worker, FixedFile file);
TimeoutAwaitable SleepFor(Worker& worker, std::chrono::nanoseconds duration);

}  // namespace celer

#endif  // CELER_IO_STORAGE_H_
