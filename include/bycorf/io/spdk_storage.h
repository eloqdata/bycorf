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

#ifndef BYCORF_IO_SPDK_STORAGE_H_
#define BYCORF_IO_SPDK_STORAGE_H_

#include <sys/uio.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "bycorf/io/backend_options.h"
#include "bycorf/io/completion.h"
#include "bycorf/io/storage.h"

struct spdk_nvme_qpair;
struct spdk_nvme_cpl;

namespace bycorf {

class Worker;

struct SpdkStorageDeviceInfo {
  std::size_t io_alignment_ = 0;
  std::uint64_t size_bytes_ = 0;
  std::string controller_id_;
  unsigned io_queue_count_ = 0;
};

struct SpdkPollResult {
  bool did_work_ = false;
  std::uint32_t completions_ = 0;
};

// SPDK paths use spdk://<PCI-domain>:<bus>:<device>.<function>/<nsid>, for
// example spdk://69f9:00:00.0/1.  The namespace defaults to 1.
bool IsSpdkStoragePath(std::string_view path) noexcept;
absl::StatusOr<SpdkStorageDeviceInfo> ProbeSpdkStorage(std::string_view path);
absl::Status ReadSpdkStorage(std::string_view path, std::span<std::byte> output,
                             std::uint64_t offset);
absl::Status WriteSpdkStorage(std::string_view path,
                              std::span<const std::byte> input,
                              std::uint64_t offset, bool flush);
// Releases the temporary qpairs used by synchronous metadata I/O. Call after
// metadata preparation and before allocating per-worker qpairs.
void ReleaseSpdkStorageMetadataQpairs() noexcept;

// Storage buffers use ordinary aligned memory or pinned DMA memory according
// to the frozen process selection. Free must use the same allocation regime.
void* AllocateStorageBuffer(std::size_t bytes, std::size_t alignment) noexcept;
void FreeStorageBuffer(void* buffer, std::size_t alignment) noexcept;

#if BYCORF_KERNEL_BYPASS

class SpdkStorageBackend {
 public:
  SpdkStorageBackend() = default;
  SpdkStorageBackend(const SpdkStorageBackend&) = delete;
  SpdkStorageBackend& operator=(const SpdkStorageBackend&) = delete;
  ~SpdkStorageBackend();

  absl::Status Init(Worker* worker);
  void Shutdown();

  absl::Status RegisterFixedFiles(unsigned count);
  absl::Status RegisterBuffers(std::span<const iovec> buffers);
  absl::Status SubmitOpenDirect(std::string_view path, int flags, mode_t mode,
                                FixedFile file, IoCompletion* tag);
  absl::Status SubmitCloseDirect(FixedFile file, IoCompletion* tag);
  absl::Status SubmitReadFixed(FixedFile file, FixedBuffer buffer,
                               std::uint64_t offset, IoCompletion* tag);
  absl::Status SubmitRead(FixedFile file, std::span<std::byte> buffer,
                          std::uint64_t offset, IoCompletion* tag);
  absl::Status SubmitWrite(FixedFile file, std::span<const std::byte> buffer,
                           std::uint64_t offset, IoCompletion* tag);
  absl::Status SubmitWriteFixed(FixedFile file, FixedBuffer buffer,
                                std::uint64_t offset, IoCompletion* tag);
  absl::Status SubmitFdatasync(FixedFile file, IoCompletion* tag);

  SpdkPollResult Poll(unsigned max_completions = 0);
  bool HasOutstanding() const noexcept {
    return outstanding_ != 0 || !pending_completions_.empty();
  }
  void CompleteRequest(IoCompletion* tag, int result);

 private:
  struct AsyncRequest {
    SpdkStorageBackend* backend_ = nullptr;
    IoCompletion* tag_ = nullptr;
    int success_result_ = 0;
    AsyncRequest* next_ = nullptr;
  };

  struct OpenFile {
    void* device_ = nullptr;
    spdk_nvme_qpair* qpair_ = nullptr;
  };

  struct ControllerChannel {
    void* controller_ = nullptr;
    spdk_nvme_qpair* qpair_ = nullptr;
    unsigned open_files_ = 0;
  };

  absl::Status SubmitIo(FixedFile file, void* buffer, std::size_t bytes,
                        std::uint64_t offset, bool write, IoCompletion* tag);
  OpenFile* Lookup(FixedFile file);
  AsyncRequest* AcquireRequest() noexcept;
  void ReleaseRequest(AsyncRequest* request) noexcept;
  static void CompleteAsync(void* context, const spdk_nvme_cpl* completion);

  Worker* worker_ = nullptr;
  std::vector<OpenFile> files_;
  std::vector<ControllerChannel> channels_;
  std::vector<std::pair<IoCompletion*, int>> pending_completions_;
  std::vector<AsyncRequest> request_pool_;
  AsyncRequest* free_requests_ = nullptr;
  std::size_t next_poll_channel_ = 0;
  std::size_t outstanding_ = 0;
  bool initialized_ = false;
};

#endif  // BYCORF_KERNEL_BYPASS

}  // namespace bycorf

#endif  // BYCORF_IO_SPDK_STORAGE_H_
