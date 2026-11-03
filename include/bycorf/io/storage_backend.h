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

// CRTP storage facade. Owner derives non-virtually from this specialization and
// grants it access to KernelBackend(). The shared ring is reached by a fixed
// address adjustment, without a stored owner/ring pointer. Owner initializes
// and shuts down the ring, which must outlive the facade. Accepted operations
// retain caller-owned tags/buffers until completion.
template <typename Owner>
class StorageBackend {
 public:
  struct PollResult {
    bool did_work_ = false;
    std::uint32_t completions_ = 0;
  };

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

  absl::Status RegisterFixedFiles(unsigned count) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.RegisterFixedFiles(count);
    }
#endif
    return Kernel().RegisterFixedFiles(count);
  }
  absl::Status RegisterBuffers(std::span<const iovec> buffers) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.RegisterBuffers(buffers);
    }
#endif
    return Kernel().RegisterBuffers(buffers);
  }
  absl::Status SubmitOpenDirect(std::string_view path, int flags, mode_t mode,
                                FixedFile file, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitOpenDirect(path, flags, mode, file, tag);
    }
#endif
    return Kernel().SubmitOpenDirect(path, flags, mode, file, tag);
  }
  absl::Status SubmitCloseDirect(FixedFile file, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitCloseDirect(file, tag);
    }
#endif
    return Kernel().SubmitCloseDirect(file, tag);
  }
  absl::Status SubmitReadFixed(FixedFile file, FixedBuffer buffer,
                               std::uint64_t offset, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitReadFixed(file, buffer, offset, tag);
    }
#endif
    return Kernel().SubmitReadFixed(file, buffer, offset, tag);
  }
  absl::Status SubmitRead(FixedFile file, std::span<std::byte> buffer,
                          std::uint64_t offset, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitRead(file, buffer, offset, tag);
    }
#endif
    return Kernel().SubmitRead(file, buffer, offset, tag);
  }
  absl::Status SubmitWrite(FixedFile file, std::span<const std::byte> buffer,
                           std::uint64_t offset, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitWrite(file, buffer, offset, tag);
    }
#endif
    return Kernel().SubmitWrite(file, buffer, offset, tag);
  }
  absl::Status SubmitWriteFixed(FixedFile file, FixedBuffer buffer,
                                std::uint64_t offset, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitWriteFixed(file, buffer, offset, tag);
    }
#endif
    return Kernel().SubmitWriteFixed(file, buffer, offset, tag);
  }
  absl::Status SubmitFdatasync(FixedFile file, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (spdk_.IsInitialized()) {
      return spdk_.SubmitFdatasync(file, tag);
    }
#endif
    return Kernel().SubmitFdatasync(file, tag);
  }

 private:
  // Only the declared owner may construct the facade; a standalone base
  // would make the CRTP downcast invalid. Never access Owner in base teardown.
  friend Owner;
  StorageBackend() = default;
  ~StorageBackend() = default;

  IoUringBackend& Kernel() noexcept {
    return static_cast<Owner&>(*this).KernelBackend();
  }
  const IoUringBackend& Kernel() const noexcept {
    return static_cast<const Owner&>(*this).KernelBackend();
  }

#if BYCORF_KERNEL_BYPASS
  // Resolve callback ownership through worker TLS without adding an owner
  // pointer to every I/O request.
  friend class SpdkBackend;
  // Default construction acquires no device resources. Successful Init marks
  // this backend active; reuse that state instead of a second selection flag.
  // Storage submissions require a successfully initialized, live worker.
  SpdkBackend spdk_;
#endif
};
}  // namespace bycorf

#endif
