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

#ifndef CELER_IO_DPDK_BACKEND_H_
#define CELER_IO_DPDK_BACKEND_H_

#include <memory>

#include "celer/io/io_uring_backend.h"

namespace celer {
// One instance per existing Worker. io_uring retains storage, timers and
// MSG_RING wakeups; accepted IPv4 TCP streams run in the worker's FreeBSD VNET.
class DpdkBackend : public IoUringBackend {
 public:
  DpdkBackend();
  ~DpdkBackend();
  // Called once before Runtime creates worker threads. Configures one DPDK
  // port and bounded software queues using CELER_DPDK_* environment settings.
  static absl::Status PrepareRuntime(unsigned workers);
  // Release peers waiting for worker 0 if any worker fails during startup.
  static void AbortStartup(const absl::Status& reason);
  static void StopRuntime();
  absl::Status Init(const IoBackendOptions&, Worker*, int wake_fd);
  void Shutdown();
  bool Poll();
  bool Wait(int timeout_ms);
  absl::Status Submit();
  absl::Status SubmitSend(const RegisteredFile&, std::span<const std::byte>,
                          IoCompletion*);
  absl::Status SubmitSendMsg(const RegisteredFile&, const msghdr*,
                             IoCompletion*);
  absl::Status SubmitAcceptMultishot(int fd, IoCompletion*);
  absl::Status SubmitCancel(IoCompletion*);
  absl::Status StartRecvMultishot(Connection*);
  absl::Status SubmitCancelRecv(Connection*, IoCompletion*);
  absl::Status StartPeerDisconnectPoll(Connection*);
  absl::Status CancelPeerDisconnectPoll(Connection*);
  std::span<const std::byte> ViewRecvBuffer(const Connection*, std::uint16_t,
                                            std::size_t, std::size_t) const;
  void ReleaseRecvBuffer(Connection*, std::uint16_t);

  // Socket helpers require the owning worker thread. Return host errno values.
  static int Listen(const sockaddr* address, socklen_t length, int backlog);
  static int Close(int handle) noexcept;
  static int PeerName(int handle, sockaddr*, socklen_t*) noexcept;

 private:
  class Impl;
  static thread_local Impl* current_;
  std::unique_ptr<Impl> impl_;
};
}  // namespace celer

#endif
