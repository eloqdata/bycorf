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
// CRTP network facade. Owner derives non-virtually from this specialization and
// grants it access to KernelBackend(), which returns the owner's shared ring.
// Downcasting computes a fixed address adjustment; no owner/ring pointer is
// stored here. Owner must keep the ring alive through adapter destruction.
template <typename Owner>
class NetBackend {
 public:
  // Initialize networking after the owner initializes the shared ring. DPDK
  // borrows that ring for its adapter lifetime.
  absl::Status Init(const IoBackendOptions& options, Worker* worker) {
#if BYCORF_KERNEL_BYPASS
    if (DpdkNetworkEnabled()) {
      dpdk_.emplace(Kernel());
      return dpdk_->Init(options, worker);
    }
#endif
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
  absl::Status SubmitSend(const RegisteredFile& file,
                          std::span<const std::byte> buffer,
                          IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitSend(file, buffer, tag);
#endif
    return Kernel().SubmitSend(file, buffer, tag);
  }
  absl::Status SubmitSendMsg(const RegisteredFile& file, const msghdr* message,
                             IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitSendMsg(file, message, tag);
#endif
    return Kernel().SubmitSendMsg(file, message, tag);
  }
  absl::Status SubmitAcceptMultishot(int listen_fd, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitAcceptMultishot(listen_fd, tag);
#endif
    return Kernel().SubmitAcceptMultishot(listen_fd, tag);
  }
  absl::Status SubmitConnect(int fd, const sockaddr* address,
                             socklen_t address_length, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitConnect(fd, address, address_length, tag);
#endif
    return Kernel().SubmitConnect(fd, address, address_length, tag);
  }
  absl::Status SubmitCancel(IoCompletion* target) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitCancel(target);
#endif
    return Kernel().SubmitCancel(target);
  }
  // Cancel kernel socket operations that have not acquired their fd reference
  // yet. Native DPDK handles must be excluded by the caller.
  void CancelUnsubmittedForFd(int fd) noexcept {
    Kernel().CancelUnsubmittedForFd(fd);
  }
  absl::Status StartRecvMultishot(Connection* connection) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->StartRecvMultishot(connection);
#endif
    return Kernel().StartRecvMultishot(connection);
  }
  // Null tag requests cancellation without a separate completion consumer.
  absl::Status SubmitCancelRecv(Connection* connection, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitCancelRecv(connection, tag);
#endif
    return Kernel().SubmitCancelRecv(connection, tag);
  }
  absl::Status StartPeerDisconnectPoll(Connection* connection) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->StartPeerDisconnectPoll(connection);
#endif
    return Kernel().StartPeerDisconnectPoll(connection);
  }
  absl::Status CancelPeerDisconnectPoll(Connection* connection) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->CancelPeerDisconnectPoll(connection);
#endif
    return Kernel().CancelPeerDisconnectPoll(connection);
  }
  std::span<const std::byte> ViewRecvBuffer(const Connection* connection,
                                            std::uint16_t buffer_id,
                                            std::size_t offset,
                                            std::size_t length) const {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_)
      return dpdk_->ViewRecvBuffer(connection, buffer_id, offset, length);
#endif
    return Kernel().ViewRecvBuffer(connection, buffer_id, offset, length);
  }
  void ReleaseRecvBuffer(Connection* connection, std::uint16_t buffer_id) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->ReleaseRecvBuffer(connection, buffer_id);
#endif
    return Kernel().ReleaseRecvBuffer(connection, buffer_id);
  }
  // Advance only native network work. The owner drives the shared kernel
  // ring separately, including when kernel TCP is selected.
  void Flush() {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) dpdk_->Flush();
#endif
  }
  bool Poll() {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->Poll();
#endif
    return false;
  }
  // Owner-thread preparation is always paired with FinishWait, including
  // errors/vetoes. Kernel sockets already have persistent completion sources.
  absl::StatusOr<bool> PrepareWait() {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->PrepareWait();
#endif
    return true;
  }
  // Recheck after Poll and submission; this does not consume completions.
  bool CanWait() const {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->CanWait();
#endif
    return true;
  }
  int WaitTimeout(int timeout_ms) const {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->WaitTimeout(timeout_ms);
#endif
    return timeout_ms;
  }
  absl::Status FinishWait(bool waited) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->FinishWait(waited);
#endif
    (void)waited;
    return absl::OkStatus();
  }

 private:
  // Only the declared owner may construct the facade; a standalone base
  // would make the CRTP downcast invalid. Never access Owner in base teardown.
  friend Owner;
  NetBackend() = default;
  ~NetBackend() = default;

  IoUringBackend& Kernel() noexcept {
    return static_cast<Owner&>(*this).KernelBackend();
  }
  const IoUringBackend& Kernel() const noexcept {
    return static_cast<const Owner&>(*this).KernelBackend();
  }

#if BYCORF_KERNEL_BYPASS
  // Inline the small adapter: packet/stream submissions already follow its
  // implementation pointer, so a separate heap adapter adds a dependent load
  // to every call. The optional only activates the selected network path.
  std::optional<DpdkBackend> dpdk_;
#endif
};
}  // namespace bycorf
#endif
