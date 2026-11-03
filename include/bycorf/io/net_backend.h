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

#ifndef BYCORF_IO_NET_BACKEND_H_
#define BYCORF_IO_NET_BACKEND_H_
#include "bycorf/io/backend_options.h"
#include "bycorf/io/io_uring_backend.h"
#if BYCORF_KERNEL_BYPASS
#include <optional>

#include "bycorf/io/dpdk_backend.h"
#endif

namespace bycorf {
// Routes networking through the worker-owned kernel ring or a DPDK adapter
// borrowing that same ring. Selection stays fixed after Init; the worker owns
// ring initialization and shutdown, including storage, timers and wakeups.
class NetBackend {
 public:
  // The worker passes its ring directly so inlined kernel submissions do not
  // load a stored ring pointer. DPDK borrows it for its adapter lifetime.
  // Initialize networking after the worker initializes the shared ring.
  absl::Status Init(IoUringBackend& kernel, const IoBackendOptions& options,
                    Worker* worker) {
#if BYCORF_KERNEL_BYPASS
    if (DpdkNetworkEnabled()) {
      dpdk_.emplace(kernel);
      return dpdk_->Init(options, worker);
    }
#endif
    (void)kernel;
    (void)options;
    (void)worker;
    return absl::OkStatus();
  }
  // Release DPDK peers waiting for startup if shared-ring initialization fails.
  void AbortStartup(const absl::Status& status) {
#if BYCORF_KERNEL_BYPASS
    if (DpdkNetworkEnabled()) DpdkBackend::AbortStartup(status);
#else
    (void)status;
#endif
  }
  // Stop networking while the borrowed ring is still alive.
  void Shutdown() {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) {
      dpdk_->Shutdown();
      return;
    }
#endif
  }
  absl::Status SubmitSend(IoUringBackend& kernel, const RegisteredFile& file,
                          std::span<const std::byte> buffer,
                          IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitSend(file, buffer, tag);
#endif
    return kernel.SubmitSend(file, buffer, tag);
  }
  absl::Status SubmitSendMsg(IoUringBackend& kernel, const RegisteredFile& file,
                             const msghdr* message, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitSendMsg(file, message, tag);
#endif
    return kernel.SubmitSendMsg(file, message, tag);
  }
  absl::Status SubmitAcceptMultishot(IoUringBackend& kernel, int listen_fd,
                                     IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitAcceptMultishot(listen_fd, tag);
#endif
    return kernel.SubmitAcceptMultishot(listen_fd, tag);
  }
  absl::Status SubmitConnect(IoUringBackend& kernel, int fd,
                             const sockaddr* address, socklen_t address_length,
                             IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitConnect(fd, address, address_length, tag);
#endif
    return kernel.SubmitConnect(fd, address, address_length, tag);
  }
  absl::Status SubmitCancel(IoUringBackend& kernel, IoCompletion* target) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitCancel(target);
#endif
    return kernel.SubmitCancel(target);
  }
  // Cancel kernel socket operations that have not acquired their fd reference
  // yet. Native DPDK handles must be excluded by the caller.
  void CancelUnsubmittedForFd(IoUringBackend& kernel, int fd) noexcept {
    kernel.CancelUnsubmittedForFd(fd);
  }
  absl::Status StartRecvMultishot(IoUringBackend& kernel,
                                  Connection* connection) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->StartRecvMultishot(connection);
#endif
    return kernel.StartRecvMultishot(connection);
  }
  // Null tag requests cancellation without a separate completion consumer.
  absl::Status SubmitCancelRecv(IoUringBackend& kernel, Connection* connection,
                                IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitCancelRecv(connection, tag);
#endif
    return kernel.SubmitCancelRecv(connection, tag);
  }
  absl::Status StartPeerDisconnectPoll(IoUringBackend& kernel,
                                       Connection* connection) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->StartPeerDisconnectPoll(connection);
#endif
    return kernel.StartPeerDisconnectPoll(connection);
  }
  absl::Status CancelPeerDisconnectPoll(IoUringBackend& kernel,
                                        Connection* connection) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->CancelPeerDisconnectPoll(connection);
#endif
    return kernel.CancelPeerDisconnectPoll(connection);
  }
  std::span<const std::byte> ViewRecvBuffer(const IoUringBackend& kernel,
                                            const Connection* connection,
                                            std::uint16_t buffer_id,
                                            std::size_t offset,
                                            std::size_t length) const {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_)
      return dpdk_->ViewRecvBuffer(connection, buffer_id, offset, length);
#endif
    return kernel.ViewRecvBuffer(connection, buffer_id, offset, length);
  }
  void ReleaseRecvBuffer(IoUringBackend& kernel, Connection* connection,
                         std::uint16_t buffer_id) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->ReleaseRecvBuffer(connection, buffer_id);
#endif
    return kernel.ReleaseRecvBuffer(connection, buffer_id);
  }
  absl::Status Submit(IoUringBackend& kernel) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->Submit();
#endif
    return kernel.Submit();
  }
  bool Poll(IoUringBackend& kernel) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->Poll();
#endif
    return kernel.Poll();
  }
  bool Wait(IoUringBackend& kernel, int timeout_ms) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->Wait(timeout_ms);
#endif
    return kernel.Wait(timeout_ms);
  }

 private:
#if BYCORF_KERNEL_BYPASS
  // Inline the small adapter: packet/stream submissions already follow its
  // implementation pointer, so a separate heap adapter adds a dependent load
  // to every call. The optional only activates the selected network path.
  std::optional<DpdkBackend> dpdk_;
#endif
};
}  // namespace bycorf
#endif
