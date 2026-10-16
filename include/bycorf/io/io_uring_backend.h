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

#ifndef BYCORF_IO_IO_URING_BACKEND_H_
#define BYCORF_IO_IO_URING_BACKEND_H_

#include <liburing.h>
#include <sys/uio.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <span>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/io/completion.h"
#include "bycorf/io/storage.h"
#include "bycorf/net/connection.h"

namespace bycorf {

class Worker;

struct IoBackendOptions {
  unsigned ring_entries_ = 256;  // io_uring SQ ring size
  // Multishot recv buffer-ring entries. Zero uses per-connection one-shot recv.
  unsigned recv_buffer_count_ = 1024;
  unsigned recv_buffer_size_ = 4096;
  int idle_timeout_ms_ = -1;
};

// The worker-owned kernel I/O backend, shared by networking and storage.
// Concrete submissions avoid virtual dispatch. It owns the
// ring, the provided buffer ring, the wake eventfd, SQE/CQE handling, multishot
// recv + its re-arm, and cross-core MSG_RING wake. io_uring symbols live ONLY
// here. The backend may touch Connection (bycorf-internal) and uses the Worker
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

  // Success accepts an operation, including time waiting for SQ space. Callers
  // retain tags, buffers and indirect arguments until terminal completion.
  // Submitting never invokes a completion inline. Ordinary allocation failure
  // is fatal, as for the worker's other queues.
  // On completion the backend invokes tag->Complete(worker,
  // result, flags) — send (one-shot) and accept (multishot) both go through
  // this path; the backend is agnostic to which. recv does NOT use Complete:
  // the backend updates the Connection directly and resumes its reader.
  absl::Status SubmitSend(const RegisteredFile& file,
                          std::span<const std::byte> buffer, IoCompletion* tag);
  absl::Status SubmitSendMsg(const RegisteredFile& file, const msghdr* message,
                             IoCompletion* tag);
  absl::Status SubmitAcceptMultishot(int listen_fd,
                                     IoCompletion* tag);  // multishot
  // Async connect on a raw fd whose socket is not registered as a Connection
  // yet (registration happens in ConnectTcp after a successful completion).
  // `address` is referenced by the SQE until the CQE arrives; callers keep it
  // alive by embedding it in the awaitable.
  absl::Status SubmitConnect(int fd, const sockaddr* address,
                             socklen_t address_length, IoCompletion* tag);
  // Cancel a queued operation locally or best-effort cancel its in-flight SQE.
  // Both paths deliver the target's terminal completion asynchronously. The
  // cancel request's own CQE is dropped (kCancelRequestTag); the target's CQE
  // still arrives — normally or with -ECANCELED — so the owner MUST wait for
  // it before reclaiming state the SQE referenced. -ENOENT (already done) is
  // an expected outcome, not an error.
  absl::Status SubmitCancel(IoCompletion* target);
  absl::Status StartRecvMultishot(
      Connection* connection);  // multishot, idempotent
  // A null tag discards the cancel request's CQE; the receive's own terminal
  // CQE still drains its connection reference before reclamation.
  absl::Status SubmitCancelRecv(Connection* connection, IoCompletion* tag);
  absl::Status StartPeerDisconnectPoll(Connection* connection);
  absl::Status CancelPeerDisconnectPoll(Connection* connection);

  // Retire unsubmitted kernel socket operations before closing/reusing a raw
  // fd. Their cancellation completions are deferred to Poll; already consumed
  // SQEs retain the kernel's file reference. Fixed-file operations are
  // excluded.
  void CancelUnsubmittedForFd(int fd) noexcept;

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
  // Observe a host descriptor (e.g. a DPDK RX notification) through the same
  // CQ as storage and MSG_RING. The caller retains tag until completion/cancel.
  absl::Status SubmitPoll(int fd, unsigned events, IoCompletion* tag);

  // Event loop.
  absl::Status Submit();
  bool Poll();                // reap completions, dispatch, then re-arm recvs
  bool Wait(int timeout_ms);  // block for a completion, dispatch, drain
  // Software work or SQEs still requiring an enter must prevent worker sleep.
  bool HasPending() const noexcept;

  // Cross-core: wake a peer worker that is parked (caller checks wake_seq).
  void WakeRemote(int peer_ring_fd) noexcept;
  // Wake THIS backend from another thread (stop path).
  void WakeSelf() noexcept;

 private:
  // Backend-owned submission state. Pointees remain caller-owned, just as for
  // an SQE waiting in the real ring. Prepare the entire descriptor before
  // publishing it so queue draining never observes a partially built request.
  struct PendingIo {
    explicit PendingIo(const io_uring_sqe& sqe) noexcept;
    io_uring_sqe descriptor() const noexcept;

    // Linux's SQE contains a zero-length extension array, so store its object
    // representation rather than embedding that non-standard member in C++.
    std::array<std::byte, sizeof(io_uring_sqe)> descriptor_;
  };
  struct PendingCompletion {
    std::uint64_t user_data_;
    int result_;
  };

  struct MultishotBufferRing {
    static constexpr std::uint16_t kGroupId = 1;
    io_uring_buf_ring* ring_ = nullptr;
    std::vector<std::byte> storage_;
    unsigned entries_ = 0;
    unsigned buffer_size_ = 0;
    int mask_ = 0;
  };

  io_uring_sqe* AcquireSqe();
  absl::Status SubmitOrQueue(const io_uring_sqe& prepared);
  absl::Status QueuePrepared(const io_uring_sqe& prepared);
  absl::Status SubmitSlow();
  bool PollDeferred();
  bool DrainPending();
  bool DrainPendingCompletions();
  template <typename Predicate>
  bool CancelUnsubmittedMatching(Predicate matches, int result = -ECANCELED);
  void RetireUnsubmittedCancels(std::uint64_t target);
  absl::Status CancelByData(void* target, void* completion);
  bool InitMultishotRecv();
  bool ArmWakePoll();
  void HandleWakePoll();
  void HandleMultishotRecv(Connection* connection, io_uring_cqe* cqe);
  void HandlePeerDisconnect(Connection* connection, io_uring_cqe* cqe);
  void RecycleMultishotBuffer(std::uint16_t buffer_id);
  void DispatchCqe(io_uring_cqe* cqe);
  bool ReapCompletions();
  void DrainRecvRearm();

  io_uring ring_{};
  // Pack flags into the alignment gap before pointer-sized state. The rarely
  // used ownership/registration flags then add no gaps between hot fields.
  bool initialized_ = false;
  bool may_have_unsubmitted_cancels_ = false;
  bool recv_multishot_enabled_ = false;
  bool wake_poll_armed_ = false;
  bool owns_wake_fd_ = true;
  bool fixed_files_registered_ = false;
  bool buffers_registered_ = false;
  Worker* worker_ = nullptr;

  // All three queues are checked by the event loop even when empty. Keep their
  // metadata together ahead of receive-buffer storage and setup options.
  std::deque<PendingIo> pending_io_;
  std::vector<PendingCompletion> pending_completions_;
  std::vector<Connection*> recv_rearm_queue_;

  MultishotBufferRing multishot_ring_{};
  int wake_event_fd_ = -1;
  IoBackendOptions options_{};
};

}  // namespace bycorf

#endif  // BYCORF_IO_IO_URING_BACKEND_H_
