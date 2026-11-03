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

#ifndef BYCORF_IO_STORAGE_BACKEND_H_
#define BYCORF_IO_STORAGE_BACKEND_H_

#include <cassert>

#include "bycorf/io/backend_options.h"
#include "bycorf/io/io_uring_backend.h"
#if BYCORF_KERNEL_BYPASS
#include "bycorf/io/spdk_storage.h"
#endif

namespace bycorf {

// Routes worker-owned storage operations using the frozen process selection.
// Borrows the worker's kernel backend; neither initializes nor shuts down its
// ring. The worker passes the ring directly to submissions, avoiding a stored
// pointer load, and keeps it alive until both adapters have stopped.
// Accepted operations retain caller-owned tags/buffers until completion.
class StorageBackend {
 public:
  struct PollResult {
    bool did_work_ = false;
    std::uint32_t completions_ = 0;
  };

  StorageBackend() = default;
  StorageBackend(const StorageBackend&) = delete;
  StorageBackend& operator=(const StorageBackend&) = delete;

  // Initialize after the shared ring, on the owning worker thread.
  absl::Status Init(Worker* worker) {
#if BYCORF_KERNEL_BYPASS
    if (SpdkStorageEnabled()) {
      return spdk_.Init(worker);
    }
#else
    (void)worker;
#endif
    return absl::OkStatus();
  }
  // Drain the polled backend before the worker tears down the shared ring.
  // Kernel storage resources retire with the ring, after all kernel I/O stops.
  void Shutdown() {
#if BYCORF_KERNEL_BYPASS
    spdk_.Shutdown();
#endif
  }

  // Kernel storage completions are driven by the shared ring's event loop.
  // Only a backend without a kernel wake source needs separate polling and
  // must keep the worker from parking while requests/completions are pending.
  bool NeedsPolling() const noexcept {
#if BYCORF_KERNEL_BYPASS
    return spdk_.IsInitialized() && spdk_.HasOutstanding();
#else
    return false;
#endif
  }
  // Call only after NeedsPolling returns true, on the owning worker. A zero
  // limit preserves the backend's default completion budget.
  PollResult Poll(unsigned max_completions = 0) {
#if BYCORF_KERNEL_BYPASS
    // The worker calls Poll only after NeedsPolling, so no second selection
    // check is needed on this hot path.
    assert(spdk_.IsInitialized());
    const auto result = spdk_.Poll(max_completions);
    return {result.did_work_, result.completions_};
#else
    (void)max_completions;
#endif
    return {};
  }

  absl::Status RegisterFixedFiles(IoUringBackend& kernel, unsigned count) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.RegisterFixedFiles(count);
    }
#endif
    return kernel.RegisterFixedFiles(count);
  }
  absl::Status RegisterBuffers(IoUringBackend& kernel,
                               std::span<const iovec> buffers) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.RegisterBuffers(buffers);
    }
#endif
    return kernel.RegisterBuffers(buffers);
  }
  absl::Status SubmitOpenDirect(IoUringBackend& kernel, std::string_view path,
                                int flags, mode_t mode, FixedFile file,
                                IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitOpenDirect(path, flags, mode, file, tag);
    }
#endif
    return kernel.SubmitOpenDirect(path, flags, mode, file, tag);
  }
  absl::Status SubmitCloseDirect(IoUringBackend& kernel, FixedFile file,
                                 IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitCloseDirect(file, tag);
    }
#endif
    return kernel.SubmitCloseDirect(file, tag);
  }
  absl::Status SubmitReadFixed(IoUringBackend& kernel, FixedFile file,
                               FixedBuffer buffer, std::uint64_t offset,
                               IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitReadFixed(file, buffer, offset, tag);
    }
#endif
    return kernel.SubmitReadFixed(file, buffer, offset, tag);
  }
  absl::Status SubmitRead(IoUringBackend& kernel, FixedFile file,
                          std::span<std::byte> buffer, std::uint64_t offset,
                          IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitRead(file, buffer, offset, tag);
    }
#endif
    return kernel.SubmitRead(file, buffer, offset, tag);
  }
  absl::Status SubmitWrite(IoUringBackend& kernel, FixedFile file,
                           std::span<const std::byte> buffer,
                           std::uint64_t offset, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitWrite(file, buffer, offset, tag);
    }
#endif
    return kernel.SubmitWrite(file, buffer, offset, tag);
  }
  absl::Status SubmitWriteFixed(IoUringBackend& kernel, FixedFile file,
                                FixedBuffer buffer, std::uint64_t offset,
                                IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitWriteFixed(file, buffer, offset, tag);
    }
#endif
    return kernel.SubmitWriteFixed(file, buffer, offset, tag);
  }
  absl::Status SubmitFdatasync(IoUringBackend& kernel, FixedFile file,
                               IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitFdatasync(file, tag);
    }
#endif
    return kernel.SubmitFdatasync(file, tag);
  }

 private:
#if BYCORF_KERNEL_BYPASS
  // Resolve callback ownership through worker TLS without adding an owner
  // pointer to every I/O request.
  friend class SpdkStorageBackend;
  // Default construction acquires no device resources. Successful Init marks
  // this backend active; reuse that state instead of a second selection flag.
  // Storage submissions require a successfully initialized, live worker.
  SpdkStorageBackend spdk_;
#endif
};
}  // namespace bycorf

#endif
