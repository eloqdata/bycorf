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
// One kernel ring per worker, shared by kernel sockets, storage, timers and
// wakeups. An optional DPDK adapter borrows that ring. Backend selection stays
// fixed after Init, so calls use a predictable branch without virtual dispatch.
class NetBackend {
 public:
  absl::Status Init(const IoBackendOptions& options, Worker* worker,
                    int wake_fd) {
#if BYCORF_KERNEL_BYPASS
    if (DpdkNetworkEnabled()) {
      dpdk_.emplace(kernel_);
      return dpdk_->Init(options, worker, wake_fd);
    }
#endif
    return kernel_.Init(options, worker, wake_fd);
  }
  void Shutdown() {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) {
      dpdk_->Shutdown();
      return;
    }
#endif
    kernel_.Shutdown();
  }
  int WakeHandle() const noexcept { return kernel_.WakeHandle(); }
  absl::Status SubmitSend(const RegisteredFile& file,
                          std::span<const std::byte> buffer,
                          IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitSend(file, buffer, tag);
#endif
    return kernel_.SubmitSend(file, buffer, tag);
  }
  absl::Status SubmitSendMsg(const RegisteredFile& file, const msghdr* message,
                             IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitSendMsg(file, message, tag);
#endif
    return kernel_.SubmitSendMsg(file, message, tag);
  }
  absl::Status SubmitAcceptMultishot(int listen_fd, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitAcceptMultishot(listen_fd, tag);
#endif
    return kernel_.SubmitAcceptMultishot(listen_fd, tag);
  }
  absl::Status SubmitConnect(int fd, const sockaddr* address,
                             socklen_t address_length, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitConnect(fd, address, address_length, tag);
#endif
    return kernel_.SubmitConnect(fd, address, address_length, tag);
  }
  absl::Status SubmitCancel(IoCompletion* target) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitCancel(target);
#endif
    return kernel_.SubmitCancel(target);
  }
  absl::Status StartRecvMultishot(Connection* connection) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->StartRecvMultishot(connection);
#endif
    return kernel_.StartRecvMultishot(connection);
  }
  absl::Status SubmitCancelRecv(Connection* connection, IoCompletion* tag) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->SubmitCancelRecv(connection, tag);
#endif
    return kernel_.SubmitCancelRecv(connection, tag);
  }
  absl::Status StartPeerDisconnectPoll(Connection* connection) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->StartPeerDisconnectPoll(connection);
#endif
    return kernel_.StartPeerDisconnectPoll(connection);
  }
  absl::Status CancelPeerDisconnectPoll(Connection* connection) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->CancelPeerDisconnectPoll(connection);
#endif
    return kernel_.CancelPeerDisconnectPoll(connection);
  }
  std::span<const std::byte> ViewRecvBuffer(const Connection* connection,
                                            std::uint16_t buffer_id,
                                            std::size_t offset,
                                            std::size_t length) const {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_)
      return dpdk_->ViewRecvBuffer(connection, buffer_id, offset, length);
#endif
    return kernel_.ViewRecvBuffer(connection, buffer_id, offset, length);
  }
  void ReleaseRecvBuffer(Connection* connection, std::uint16_t buffer_id) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->ReleaseRecvBuffer(connection, buffer_id);
#endif
    return kernel_.ReleaseRecvBuffer(connection, buffer_id);
  }
  absl::Status RegisterFixedFiles(unsigned count) {
    return kernel_.RegisterFixedFiles(count);
  }
  absl::Status RegisterBuffers(std::span<const iovec> buffers) {
    return kernel_.RegisterBuffers(buffers);
  }
  void UnregisterStorageResources() {
    return kernel_.UnregisterStorageResources();
  }
  absl::Status SubmitOpenDirect(std::string_view path, int flags, mode_t mode,
                                FixedFile file, IoCompletion* tag) {
    return kernel_.SubmitOpenDirect(path, flags, mode, file, tag);
  }
  absl::Status SubmitCloseDirect(FixedFile file, IoCompletion* tag) {
    return kernel_.SubmitCloseDirect(file, tag);
  }
  absl::Status SubmitReadFixed(FixedFile file, FixedBuffer buffer,
                               std::uint64_t offset, IoCompletion* tag) {
    return kernel_.SubmitReadFixed(file, buffer, offset, tag);
  }
  absl::Status SubmitRead(FixedFile file, std::span<std::byte> buffer,
                          std::uint64_t offset, IoCompletion* tag) {
    return kernel_.SubmitRead(file, buffer, offset, tag);
  }
  absl::Status SubmitWrite(FixedFile file, std::span<const std::byte> buffer,
                           std::uint64_t offset, IoCompletion* tag) {
    return kernel_.SubmitWrite(file, buffer, offset, tag);
  }
  absl::Status SubmitWriteFixed(FixedFile file, FixedBuffer buffer,
                                std::uint64_t offset, IoCompletion* tag) {
    return kernel_.SubmitWriteFixed(file, buffer, offset, tag);
  }
  absl::Status SubmitFdatasync(FixedFile file, IoCompletion* tag) {
    return kernel_.SubmitFdatasync(file, tag);
  }
  absl::Status SubmitTimeout(const __kernel_timespec& timeout,
                             IoCompletion* tag) {
    return kernel_.SubmitTimeout(timeout, tag);
  }
  absl::Status SubmitPoll(int fd, unsigned events, IoCompletion* tag) {
    return kernel_.SubmitPoll(fd, events, tag);
  }
  absl::Status Submit() {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->Submit();
#endif
    return kernel_.Submit();
  }
  bool Poll() {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->Poll();
#endif
    return kernel_.Poll();
  }
  bool Wait(int timeout_ms) {
#if BYCORF_KERNEL_BYPASS
    if (dpdk_) return dpdk_->Wait(timeout_ms);
#endif
    return kernel_.Wait(timeout_ms);
  }
  void WakeRemote(int peer_ring_fd) noexcept {
    return kernel_.WakeRemote(peer_ring_fd);
  }
  void WakeSelf() noexcept { return kernel_.WakeSelf(); }

 private:
  // Declaration order ensures the borrowing adapter dies before its ring.
  IoUringBackend kernel_;
#if BYCORF_KERNEL_BYPASS
  // Inline the small adapter: packet/stream submissions already follow its
  // implementation pointer, so a separate heap adapter adds a dependent load
  // to every call. The optional only activates the selected network path.
  std::optional<DpdkBackend> dpdk_;
#endif
};
}  // namespace bycorf
#endif
