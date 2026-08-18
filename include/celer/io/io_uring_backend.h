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

#ifndef CELER_IO_IO_URING_BACKEND_H_
#define CELER_IO_IO_URING_BACKEND_H_

#include <liburing.h>
#include <sys/uio.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/io/completion.h"
#include "celer/io/storage.h"
#include "celer/net/connection.h"

namespace celer {

class Worker;

struct IoBackendOptions {
  unsigned ring_entries_ = 256;  // io_uring SQ ring size
  // Multishot recv buffer-ring entries. Zero uses per-connection one-shot recv.
  unsigned recv_buffer_count_ = 1024;
  unsigned recv_buffer_size_ = 4096;
  int idle_timeout_ms_ = -1;
};

// The io_uring network backend: a concrete (non-virtual) class selected at
// compile time, so all calls inline — zero abstraction overhead. It owns the
// ring, the provided buffer ring, the wake eventfd, SQE/CQE handling, multishot
// recv + its re-arm, and cross-core MSG_RING wake. io_uring symbols live ONLY
// here. The backend may touch Connection (celer-internal) and uses the Worker
// for scheduler actions (resume a reader via Enqueue, observe a stop signal).
class IoUringBackend {
 public:
  IoUringBackend() = default;
  IoUringBackend(const IoUringBackend&) = delete;
  IoUringBackend& operator=(const IoUringBackend&) = delete;
  ~IoUringBackend();

  // wake_fd: a pre-created eventfd (from the cross-core mailbox), or -1 to own
  // one.
  absl::Status Init(const IoBackendOptions& options, Worker* worker,
                    int wake_fd);
  void Shutdown();

  int WakeHandle() const noexcept {
    return ring_.ring_fd;
  }  // peers' MSG_RING target

  // Typed submissions. On completion the backend invokes tag->Complete(worker,
  // result, flags) — send (one-shot) and accept (multishot) both go through
  // this path; the backend is agnostic to which. recv does NOT use Complete:
  // the backend updates the Connection directly and resumes its reader.
  absl::Status SubmitSend(const RegisteredFile& file,
                          std::span<const std::byte> buffer, IoCompletion* tag);
  absl::Status SubmitSendMsg(const RegisteredFile& file,
                             const msghdr* message, IoCompletion* tag);
  absl::Status SubmitAcceptMultishot(int listen_fd,
                                     IoCompletion* tag);  // multishot
  absl::Status StartRecvMultishot(
      Connection* connection);  // multishot, idempotent
  absl::Status SubmitCancelRecv(Connection* connection, IoCompletion* tag);

  std::span<const std::byte> ViewRecvBuffer(const Connection* connection,
                                            std::uint16_t buffer_id,
                                            std::size_t offset,
                                            std::size_t length) const;
  void ReleaseRecvBuffer(Connection* connection, std::uint16_t buffer_id);

  absl::Status RegisterFixedFiles(unsigned count);
  absl::Status RegisterBuffers(std::span<const iovec> buffers);
  void UnregisterStorageResources();
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
  absl::Status SubmitTimeout(const __kernel_timespec& timeout,
                             IoCompletion* tag);

  // Event loop.
  absl::Status Submit();
  bool Poll();                // reap completions, dispatch, then re-arm recvs
  bool Wait(int timeout_ms);  // block for a completion, dispatch, drain

  // Cross-core: wake a peer worker that is parked (caller checks wake_seq).
  void WakeRemote(int peer_ring_fd) noexcept;
  // Wake THIS backend from another thread (stop path).
  void WakeSelf() noexcept;

 private:
  struct MultishotBufferRing {
    static constexpr std::uint16_t kGroupId = 1;
    io_uring_buf_ring* ring_ = nullptr;
    std::vector<std::byte> storage_;
    unsigned entries_ = 0;
    unsigned buffer_size_ = 0;
    int mask_ = 0;
  };

  io_uring_sqe* AcquireSqe();
  bool InitMultishotRecv();
  bool ArmWakePoll();
  void HandleWakePoll();
  void HandleMultishotRecv(Connection* connection, io_uring_cqe* cqe);
  void RecycleMultishotBuffer(std::uint16_t buffer_id);
  void DispatchCqe(io_uring_cqe* cqe);
  bool
  ReapCompletions();  // reap + dispatch only (no re-arm; used by AcquireSqe)
  void DrainRecvRearm();

  io_uring ring_{};
  bool initialized_ = false;
  Worker* worker_ = nullptr;
  IoBackendOptions options_{};
  MultishotBufferRing multishot_ring_{};
  bool recv_multishot_enabled_ = false;
  int wake_event_fd_ = -1;
  bool wake_poll_armed_ = false;
  bool owns_wake_fd_ = true;
  std::vector<Connection*> recv_rearm_queue_;
  bool fixed_files_registered_ = false;
  bool buffers_registered_ = false;
};

}  // namespace celer

#endif  // CELER_IO_IO_URING_BACKEND_H_
