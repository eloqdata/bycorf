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

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <liburing.h>

#include "celer/base/status.h"
#include "celer/io/completion.h"
#include "celer/net/connection.h"

namespace celer {

class Worker;

struct IoBackendOptions {
  unsigned ring_entries = 256;        // io_uring SQ ring size
  unsigned recv_buffer_count = 1024;  // multishot recv buffer-ring entries
  unsigned recv_buffer_size = 4096;
  int idle_timeout_ms = -1;
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

  // wake_fd: a pre-created eventfd (from the cross-core mailbox), or -1 to own one.
  Status Init(const IoBackendOptions& options, Worker* worker, int wake_fd);
  void Shutdown();

  int WakeHandle() const noexcept { return ring_.ring_fd; }  // peers' MSG_RING target

  // Typed submissions. On completion the backend invokes tag->Complete(worker,
  // result, flags) — send (one-shot) and accept (multishot) both go through this
  // path; the backend is agnostic to which. recv does NOT use Complete: the
  // backend updates the Connection directly and resumes its reader.
  Status SubmitSend(const RegisteredFile& file, std::span<const std::byte> buffer,
                    IoCompletion* tag);
  Status SubmitAcceptMultishot(int listen_fd, IoCompletion* tag);  // multishot
  Status StartRecvMultishot(Connection* connection);              // multishot, idempotent

  std::span<const std::byte> ViewRecvBuffer(std::uint16_t buffer_id, std::size_t offset,
                                            std::size_t length) const;
  void ReleaseRecvBuffer(std::uint16_t buffer_id);

  // Event loop.
  Status Submit();
  bool Poll();                // reap completions, dispatch, then re-arm recvs
  bool Wait(int timeout_ms);  // block for a completion, dispatch, drain

  // Cross-core: wake a peer worker that is parked (caller checks wake_seq).
  void WakeRemote(int peer_ring_fd) noexcept;
  // Wake THIS backend from another thread (stop path).
  void WakeSelf() noexcept;

 private:
  struct MultishotBufferRing {
    static constexpr std::uint16_t kGroupId = 1;
    io_uring_buf_ring* ring = nullptr;
    std::vector<std::byte> storage;
    unsigned entries = 0;
    unsigned buffer_size = 0;
    int mask = 0;
  };

  io_uring_sqe* AcquireSqe();
  bool InitMultishotRecv();
  bool ArmWakePoll();
  void HandleWakePoll();
  void HandleMultishotRecv(Connection* connection, io_uring_cqe* cqe);
  void RecycleMultishotBuffer(std::uint16_t buffer_id);
  void DispatchCqe(io_uring_cqe* cqe);
  bool ReapCompletions();  // reap + dispatch only (no re-arm; used by AcquireSqe)
  void DrainRecvRearm();

  io_uring ring_{};
  bool initialized_ = false;
  Worker* worker_ = nullptr;
  IoBackendOptions options_{};
  MultishotBufferRing multishot_ring_{};
  int wake_event_fd_ = -1;
  bool wake_poll_armed_ = false;
  bool owns_wake_fd_ = true;
  std::vector<Connection*> recv_rearm_queue_;
};

}  // namespace celer

#endif  // CELER_IO_IO_URING_BACKEND_H_
