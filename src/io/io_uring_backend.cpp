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

#include "celer/io/io_uring_backend.h"

#include <linux/io_uring.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>

#include "celer/runtime/worker.h"
#include "spdlog/spdlog.h"

namespace celer {

namespace {

constexpr std::uintptr_t kRecvTag = 1;
constexpr std::uintptr_t kPeerDisconnectTag = 2;
constexpr std::uintptr_t kConnectionTagMask = 3;
static_assert(alignof(Connection) > kConnectionTagMask);
int kWakePollTag = 0;       // CQE user_data sentinel: wake eventfd poll
int kCrossCoreWakeTag = 0;  // CQE user_data sentinel: cross-core MSG_RING wake
int kPeerDisconnectCancelTag = 0;

std::int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void* EncodeMultishotData(Connection* connection) {
  return reinterpret_cast<void*>(reinterpret_cast<std::uintptr_t>(connection) |
                                 kRecvTag);
}

bool IsMultishotData(void* data) {
  return (reinterpret_cast<std::uintptr_t>(data) & kConnectionTagMask) ==
         kRecvTag;
}

Connection* DecodeMultishotConnection(void* data) {
  return reinterpret_cast<Connection*>(reinterpret_cast<std::uintptr_t>(data) &
                                       ~kConnectionTagMask);
}

void* EncodePeerDisconnectData(Connection* connection) {
  return reinterpret_cast<void*>(reinterpret_cast<std::uintptr_t>(connection) |
                                 kPeerDisconnectTag);
}

bool IsPeerDisconnectData(void* data) {
  return (reinterpret_cast<std::uintptr_t>(data) & kConnectionTagMask) ==
         kPeerDisconnectTag;
}

Connection* DecodePeerDisconnectConnection(void* data) {
  return reinterpret_cast<Connection*>(reinterpret_cast<std::uintptr_t>(data) &
                                       ~kConnectionTagMask);
}

}  // namespace

IoUringBackend::~IoUringBackend() { Shutdown(); }

absl::Status IoUringBackend::Init(const IoBackendOptions& options,
                                  Worker* worker, int wake_fd) {
  if (initialized_) {
    return absl::OkStatus();
  }
  worker_ = worker;
  options_ = options;

  io_uring_params params{};
  params.flags = IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_COOP_TASKRUN |
                 IORING_SETUP_TASKRUN_FLAG | IORING_SETUP_SINGLE_ISSUER;
  const int rc =
      io_uring_queue_init_params(options.ring_entries_, &ring_, &params);
  if (rc < 0) {
    if (rc == -ENOMEM) {
      return absl::Status(
          absl::StatusCode::kResourceExhausted,
          "io_uring setup exhausted locked memory; raise memlock");
    }
    return absl::Status(absl::StatusCode::kInternal,
                        "io_uring_queue_init_params failed");
  }
  // We rely on IORING_FEAT_NODROP: the kernel queues overflowed completions
  // instead of dropping them (and makes submit return -EBUSY as backpressure).
  if ((params.features & IORING_FEAT_NODROP) == 0) {
    io_uring_queue_exit(&ring_);
    return absl::Status(
        absl::StatusCode::kFailedPrecondition,
        "io_uring lacks IORING_FEAT_NODROP (kernel >= 5.5 required)");
  }

  if (wake_fd >= 0) {
    wake_event_fd_ = wake_fd;
    owns_wake_fd_ = false;
  } else {
    wake_event_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    owns_wake_fd_ = true;
    if (wake_event_fd_ < 0) {
      io_uring_queue_exit(&ring_);
      return absl::Status(absl::StatusCode::kInternal, "eventfd failed");
    }
  }

  initialized_ = true;
  // Resource registration must happen before any SQE is prepared. In
  // particular, IORING_REGISTER_PBUF_RING may fail with EINVAL when the wake
  // poll SQE is already pending in the submission queue.
  if (!InitMultishotRecv()) {
    initialized_ = false;
    if (owns_wake_fd_) {
      ::close(wake_event_fd_);
    }
    wake_event_fd_ = -1;
    io_uring_queue_exit(&ring_);
    return absl::Status(absl::StatusCode::kInternal,
                        "multishot recv setup failed");
  }
  if (!ArmWakePoll()) {
    if (multishot_ring_.ring_ != nullptr) {
      io_uring_free_buf_ring(&ring_, multishot_ring_.ring_,
                             multishot_ring_.entries_,
                             MultishotBufferRing::kGroupId);
    }
    initialized_ = false;
    if (owns_wake_fd_) {
      ::close(wake_event_fd_);
    }
    wake_event_fd_ = -1;
    io_uring_queue_exit(&ring_);
    multishot_ring_ = {};
    recv_multishot_enabled_ = false;
    return absl::Status(absl::StatusCode::kInternal,
                        "worker wake poll setup failed");
  }
  return absl::OkStatus();
}

void IoUringBackend::Shutdown() {
  if (!initialized_) {
    return;
  }
  if (multishot_ring_.ring_ != nullptr) {
    io_uring_free_buf_ring(&ring_, multishot_ring_.ring_,
                           multishot_ring_.entries_,
                           MultishotBufferRing::kGroupId);
  }
  UnregisterStorageResources();
  wake_poll_armed_ = false;
  if (wake_event_fd_ >= 0) {
    if (owns_wake_fd_) {
      ::close(wake_event_fd_);
    }
    wake_event_fd_ = -1;
  }
  io_uring_queue_exit(&ring_);
  multishot_ring_ = {};
  recv_multishot_enabled_ = false;
  initialized_ = false;
}

absl::Status IoUringBackend::RegisterFixedFiles(unsigned count) {
  if (!initialized_ || count == 0 || fixed_files_registered_) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "invalid fixed-file table registration");
  }
  const int rc = io_uring_register_files_sparse(&ring_, count);
  if (rc < 0) {
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "io_uring_register_files_sparse failed");
  }
  fixed_files_registered_ = true;
  return absl::OkStatus();
}

absl::Status IoUringBackend::RegisterBuffers(std::span<const iovec> buffers) {
  if (!initialized_ || buffers.empty() || buffers_registered_) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "invalid registered-buffer setup");
  }
  const int rc = io_uring_register_buffers(
      &ring_, buffers.data(), static_cast<unsigned>(buffers.size()));
  if (rc < 0) {
    return absl::Status(absl::StatusCode::kResourceExhausted,
                        "io_uring_register_buffers failed; raise memlock");
  }
  buffers_registered_ = true;
  return absl::OkStatus();
}

void IoUringBackend::UnregisterStorageResources() {
  if (buffers_registered_) {
    io_uring_unregister_buffers(&ring_);
    buffers_registered_ = false;
  }
  if (fixed_files_registered_) {
    io_uring_unregister_files(&ring_);
    fixed_files_registered_ = false;
  }
}

absl::Status IoUringBackend::SubmitOpenDirect(std::string_view path, int flags,
                                              mode_t mode, FixedFile file,
                                              IoCompletion* tag) {
  if (!fixed_files_registered_ || path.empty()) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "fixed-file table is not registered");
  }
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire open sqe");
  }
  io_uring_prep_openat_direct(sqe, AT_FDCWD, path.data(), flags, mode,
                              file.index_);
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitCloseDirect(FixedFile file,
                                               IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire close sqe");
  }
  io_uring_prep_close_direct(sqe, file.index_);
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitReadFixed(FixedFile file, FixedBuffer buffer,
                                             std::uint64_t offset,
                                             IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire read sqe");
  }
  io_uring_prep_read_fixed(sqe, static_cast<int>(file.index_), buffer.data_,
                           static_cast<unsigned>(buffer.size_), offset,
                           buffer.index_);
  sqe->flags |= IOSQE_FIXED_FILE;
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitRead(FixedFile file,
                                        std::span<std::byte> buffer,
                                        std::uint64_t offset,
                                        IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire read sqe");
  }
  io_uring_prep_read(sqe, static_cast<int>(file.index_), buffer.data(),
                     static_cast<unsigned>(buffer.size()), offset);
  sqe->flags |= IOSQE_FIXED_FILE;
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitWrite(FixedFile file,
                                         std::span<const std::byte> buffer,
                                         std::uint64_t offset,
                                         IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire write sqe");
  }
  io_uring_prep_write(sqe, static_cast<int>(file.index_), buffer.data(),
                      static_cast<unsigned>(buffer.size()), offset);
  sqe->flags |= IOSQE_FIXED_FILE;
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitWriteFixed(FixedFile file,
                                              FixedBuffer buffer,
                                              std::uint64_t offset,
                                              IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire write sqe");
  }
  io_uring_prep_write_fixed(sqe, static_cast<int>(file.index_), buffer.data_,
                            static_cast<unsigned>(buffer.size_), offset,
                            buffer.index_);
  sqe->flags |= IOSQE_FIXED_FILE;
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitFdatasync(FixedFile file,
                                             IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire fsync sqe");
  }
  io_uring_prep_fsync(sqe, static_cast<int>(file.index_),
                      IORING_FSYNC_DATASYNC);
  sqe->flags |= IOSQE_FIXED_FILE;
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitTimeout(const __kernel_timespec& timeout,
                                           IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire timeout sqe");
  }
  io_uring_prep_timeout(sqe, const_cast<__kernel_timespec*>(&timeout), 0, 0);
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

io_uring_sqe* IoUringBackend::AcquireSqe() {
  if (!initialized_) {
    return nullptr;
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe != nullptr) [[likely]] {
    return sqe;
  }
  // SQ ring full (rare). Loop submit (frees SQ slots) + reap (frees CQ space)
  // until an SQE is available — never returns null. We reap with
  // ReapCompletions (NOT Poll): Poll would run DrainRecvRearm, whose
  // StartRecvMultishot calls back into AcquireSqe and could recurse. Re-arm is
  // deferred to Poll proper.
  for (;;) {
    io_uring_submit_and_get_events(&ring_);
    sqe = io_uring_get_sqe(&ring_);
    if (sqe != nullptr) {
      return sqe;
    }
    ReapCompletions();
  }
}

absl::Status IoUringBackend::Submit() {
  if (!initialized_) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "backend is not initialized");
  }
  const int rc = io_uring_submit_and_get_events(&ring_);
  if (rc == -EBUSY) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "io_uring completion ring busy");
  }
  if (rc < 0) {
    return absl::Status(absl::StatusCode::kInternal,
                        "io_uring_submit_and_get_events failed");
  }
  return absl::OkStatus();
}

bool IoUringBackend::InitMultishotRecv() {
  if (options_.recv_buffer_count_ == 0) {
    recv_multishot_enabled_ = false;
    spdlog::info(
        "provided-buffer ring disabled; using per-connection io_uring recv");
    return true;
  }

  multishot_ring_.entries_ = options_.recv_buffer_count_;
  multishot_ring_.buffer_size_ = options_.recv_buffer_size_;

  int err = 0;
  multishot_ring_.ring_ = io_uring_setup_buf_ring(
      &ring_, multishot_ring_.entries_, MultishotBufferRing::kGroupId, 0, &err);
  if (multishot_ring_.ring_ == nullptr) {
    spdlog::warn(
        "io_uring_setup_buf_ring failed: {} ({}); falling back to "
        "per-connection io_uring recv",
        err, std::strerror(err < 0 ? -err : err));
    multishot_ring_ = {};
    recv_multishot_enabled_ = false;
    return true;
  }

  multishot_ring_.mask_ = io_uring_buf_ring_mask(multishot_ring_.entries_);
  multishot_ring_.storage_.resize(multishot_ring_.entries_ *
                                  multishot_ring_.buffer_size_);

  io_uring_buf_ring_init(multishot_ring_.ring_);
  for (unsigned i = 0; i < multishot_ring_.entries_; ++i) {
    void* addr =
        multishot_ring_.storage_.data() + i * multishot_ring_.buffer_size_;
    io_uring_buf_ring_add(
        multishot_ring_.ring_, addr, multishot_ring_.buffer_size_,
        static_cast<unsigned short>(i), multishot_ring_.mask_, i);
  }
  io_uring_buf_ring_advance(multishot_ring_.ring_,
                            static_cast<int>(multishot_ring_.entries_));
  recv_multishot_enabled_ = true;
  return true;
}

bool IoUringBackend::ArmWakePoll() {
  if (wake_event_fd_ < 0 || wake_poll_armed_) {
    return wake_event_fd_ >= 0;
  }
  auto* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return false;
  }
  io_uring_prep_poll_add(sqe, wake_event_fd_, POLLIN);
  io_uring_sqe_set_data(sqe, &kWakePollTag);
  wake_poll_armed_ = true;
  return true;
}

void IoUringBackend::HandleWakePoll() {
  wake_poll_armed_ = false;
  if (wake_event_fd_ >= 0) {
    std::uint64_t value = 0;
    while (::read(wake_event_fd_, &value, sizeof(value)) == sizeof(value)) {
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      spdlog::warn("worker wake read failed errno={}", errno);
    }
  }
  const bool stopping = worker_->NotifyWake();
  if (!stopping && !ArmWakePoll()) {
    spdlog::warn("failed to re-arm worker wake poll");
  }
}

absl::Status IoUringBackend::SubmitSend(const RegisteredFile& file,
                                        std::span<const std::byte> buffer,
                                        IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire send sqe");
  }
  io_uring_prep_send(
      sqe, file.is_fixed_ ? static_cast<int>(file.fixed_index_) : file.fd_,
      buffer.data(), static_cast<unsigned>(buffer.size()), 0);
  if (file.is_fixed_) {
    sqe->flags |= IOSQE_FIXED_FILE;
  }
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitSendMsg(const RegisteredFile& file,
                                           const msghdr* message,
                                           IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire sendmsg sqe");
  }
  io_uring_prep_sendmsg(
      sqe, file.is_fixed_ ? static_cast<int>(file.fixed_index_) : file.fd_,
      message, 0);
  if (file.is_fixed_) {
    sqe->flags |= IOSQE_FIXED_FILE;
  }
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitAcceptMultishot(int listen_fd,
                                                   IoCompletion* tag) {
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire accept sqe");
  }
  io_uring_prep_multishot_accept(sqe, listen_fd, nullptr, nullptr,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC);
  io_uring_sqe_set_data(sqe, tag);  // plain IoCompletion* — same path as send
  return absl::OkStatus();
}

absl::Status IoUringBackend::StartRecvMultishot(Connection* connection) {
  if (connection == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "connection must not be null");
  }
  if (connection->recv_armed_ || connection->closed_ || connection->closing_ ||
      connection->recv_paused_) {
    return absl::OkStatus();  // already armed / not arm-able (idempotent)
  }
  const bool use_multishot =
      recv_multishot_enabled_ &&
      connection->recv_mode_ == RecvMode::kMultishot;
  if (!use_multishot && !connection->received_buffers_.empty()) {
    return absl::OkStatus();
  }
  auto* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire recv multishot sqe");
  }
  const int fd = connection->file_.is_fixed_
                     ? static_cast<int>(connection->file_.fixed_index_)
                     : connection->file_.fd_;
  if (use_multishot) {
    io_uring_prep_recv_multishot(sqe, fd, nullptr, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = MultishotBufferRing::kGroupId;
  } else {
    if (connection->read_buffer_.empty()) {
      connection->read_buffer_.resize(options_.recv_buffer_size_);
    }
    io_uring_prep_recv(sqe, fd, connection->read_buffer_.data(),
                       static_cast<unsigned>(connection->read_buffer_.size()),
                       0);
  }
  sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
  if (connection->file_.is_fixed_) {
    sqe->flags |= IOSQE_FIXED_FILE;
  }
  io_uring_sqe_set_data(sqe, EncodeMultishotData(connection));

  connection->recv_armed_ = true;
  connection->inflight_ops_ += 1;
  return absl::OkStatus();
}

absl::Status IoUringBackend::SubmitCancelRecv(Connection* connection,
                                              IoCompletion* tag) {
  if (connection == nullptr || tag == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "invalid recv cancel request");
  }
  auto* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::Status(absl::StatusCode::kUnavailable,
                        "failed to acquire recv cancel sqe");
  }
  io_uring_prep_cancel(sqe, EncodeMultishotData(connection), 0);
  io_uring_sqe_set_data(sqe, tag);
  return absl::OkStatus();
}

absl::Status IoUringBackend::StartPeerDisconnectPoll(
    Connection* connection) {
  if (connection == nullptr) {
    return absl::InvalidArgumentError("connection must not be null");
  }
  if (connection->peer_disconnect_poll_armed_ || connection->closed_ ||
      connection->closing_) {
    return absl::OkStatus();
  }
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::UnavailableError(
        "failed to acquire peer disconnect poll sqe");
  }
  const int fd = connection->file_.is_fixed_
                     ? static_cast<int>(connection->file_.fixed_index_)
                     : connection->file_.fd_;
  io_uring_prep_poll_add(
      sqe, fd, static_cast<unsigned>(POLLERR | POLLHUP | POLLRDHUP));
  if (connection->file_.is_fixed_) sqe->flags |= IOSQE_FIXED_FILE;
  io_uring_sqe_set_data(sqe, EncodePeerDisconnectData(connection));
  connection->peer_disconnect_poll_armed_ = true;
  connection->peer_disconnect_poll_cancel_requested_ = false;
  ++connection->inflight_ops_;
  return absl::OkStatus();
}

absl::Status IoUringBackend::CancelPeerDisconnectPoll(
    Connection* connection) {
  if (connection == nullptr || !connection->peer_disconnect_poll_armed_ ||
      connection->peer_disconnect_poll_cancel_requested_) {
    return absl::OkStatus();
  }
  io_uring_sqe* sqe = AcquireSqe();
  if (sqe == nullptr) {
    return absl::UnavailableError(
        "failed to acquire peer disconnect cancel sqe");
  }
  io_uring_prep_cancel(sqe, EncodePeerDisconnectData(connection), 0);
  io_uring_sqe_set_data(sqe, &kPeerDisconnectCancelTag);
  connection->peer_disconnect_poll_cancel_requested_ = true;
  return absl::OkStatus();
}

void IoUringBackend::RecycleMultishotBuffer(std::uint16_t buffer_id) {
  if (multishot_ring_.ring_ == nullptr ||
      buffer_id >= multishot_ring_.entries_) {
    return;
  }
  void* addr = multishot_ring_.storage_.data() +
               buffer_id * multishot_ring_.buffer_size_;
  io_uring_buf_ring_add(multishot_ring_.ring_, addr,
                        multishot_ring_.buffer_size_, buffer_id,
                        multishot_ring_.mask_, 0);
  io_uring_buf_ring_advance(multishot_ring_.ring_, 1);
}

std::span<const std::byte> IoUringBackend::ViewRecvBuffer(
    const Connection* connection, std::uint16_t buffer_id, std::size_t offset,
    std::size_t length) const {
  if (connection != nullptr && recv_multishot_enabled_ &&
      connection->recv_mode_ == RecvMode::kMultishot) {
    if (buffer_id >= multishot_ring_.entries_ ||
        offset > multishot_ring_.buffer_size_ ||
        length > multishot_ring_.buffer_size_ - offset) {
      return {};
    }
    const auto* base = multishot_ring_.storage_.data() +
                       buffer_id * multishot_ring_.buffer_size_ + offset;
    return std::span<const std::byte>(base, length);
  }
  if (connection == nullptr || buffer_id != 0 ||
      offset > connection->read_buffer_.size() ||
      length > connection->read_buffer_.size() - offset) {
    return {};
  }
  return std::span<const std::byte>(connection->read_buffer_.data() + offset,
                                    length);
}

void IoUringBackend::ReleaseRecvBuffer(Connection* connection,
                                       std::uint16_t buffer_id) {
  if (connection != nullptr && recv_multishot_enabled_ &&
      connection->recv_mode_ == RecvMode::kMultishot) {
    RecycleMultishotBuffer(buffer_id);
  }
}

void IoUringBackend::HandleMultishotRecv(Connection* connection,
                                         io_uring_cqe* cqe) {
  if (connection == nullptr) {
    return;
  }
  const bool has_buffer = (cqe->flags & IORING_CQE_F_BUFFER) != 0;
  const bool use_multishot =
      recv_multishot_enabled_ &&
      connection->recv_mode_ == RecvMode::kMultishot;
  std::uint16_t buffer_id = 0;
  if (has_buffer) {
    buffer_id =
        static_cast<std::uint16_t>(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
  }
  bool notify_reader = false;

  if (cqe->res > 0 && (has_buffer || !use_multishot)) {
    connection->last_active_ms_ = NowMs();
    connection->received_buffers_.push_back(
        ReceivedBuffer{.buffer_id_ = buffer_id,
                       .size_ = static_cast<std::uint32_t>(cqe->res)});
    notify_reader = true;
  } else if (cqe->res == 0) {
    connection->recv_eof_ = true;
    notify_reader = true;
  } else if (cqe->res < 0 && cqe->res != -ECANCELED && cqe->res != -ENOBUFS) {
    connection->last_error_ =
        absl::Status(absl::StatusCode::kUnknown, "recv multishot failed");
    notify_reader = true;
  } else if (cqe->res > 0) {
    connection->last_error_ = absl::Status(
        absl::StatusCode::kInternal, "recv completion missing selected buffer");
    notify_reader = true;
  }
  // -ENOBUFS is transient: retain the waiting reader and let the
  // F_MORE-cleared path re-arm after other readers return their buffers.

  if (has_buffer && cqe->res <= 0) {
    RecycleMultishotBuffer(buffer_id);
  }

  if ((cqe->flags & IORING_CQE_F_MORE) == 0) {
    connection->recv_armed_ = false;
    if (connection->inflight_ops_ > 0) {
      connection->inflight_ops_ -= 1;
    }
    if (use_multishot && !connection->recv_paused_ &&
        connection->state_ == ConnectionState::kActive &&
        !connection->closed_ && !connection->closing_ &&
        !connection->recv_eof_) {
      // Defer re-arm out of completion dispatch (DrainRecvRearm runs after the
      // reap loop) so AcquireSqe is never reached from inside it.
      if (!connection->needs_recv_rearm_) {
        connection->needs_recv_rearm_ = true;
        recv_rearm_queue_.push_back(connection);
      }
    } else if (connection->state_ != ConnectionState::kActive) {
      connection->state_ = ConnectionState::kDraining;
    }
  }

  // Resume the waiting reader (deferred via the worker's ready queue —
  // completion dispatch must not run coroutines inline).
  if (notify_reader && connection->read_waiter_) {
    auto waiter = connection->read_waiter_;
    connection->read_waiter_ = {};
    connection->read_inflight_ = false;
    worker_->Enqueue(waiter);
  }
}

void IoUringBackend::HandlePeerDisconnect(Connection* connection,
                                          io_uring_cqe* cqe) {
  if (connection == nullptr) return;
  connection->peer_disconnect_poll_armed_ = false;
  connection->peer_disconnect_poll_cancel_requested_ = false;
  if (connection->inflight_ops_ != 0) --connection->inflight_ops_;
  if (cqe->res < 0 || connection->state_ != ConnectionState::kActive) return;
  if ((cqe->res & (POLLERR | POLLHUP | POLLRDHUP)) == 0) return;
  if (connection->peer_disconnect_callback_ != nullptr) {
    connection->peer_disconnect_callback_(
        connection->peer_disconnect_context_);
  }
}

void IoUringBackend::DispatchCqe(io_uring_cqe* cqe) {
  void* data = io_uring_cqe_get_data(cqe);
  if (data == &kWakePollTag) {
    HandleWakePoll();
  } else if (data == &kCrossCoreWakeTag) {
    // Cross-core wake marker; the worker drains its mailbox separately.
  } else if (data == &kPeerDisconnectCancelTag) {
    // The cancelled poll's own CQE retires the connection operation. The
    // cancel request completion carries no additional state.
  } else if (IsMultishotData(data)) {
    HandleMultishotRecv(DecodeMultishotConnection(data), cqe);
  } else if (IsPeerDisconnectData(data)) {
    HandlePeerDisconnect(DecodePeerDisconnectConnection(data), cqe);
  } else {
    auto* op = static_cast<IoCompletion*>(data);
    if (op != nullptr) {
      // Translate native flags to neutral CompletionFlags (multishot accept
      // needs "more" to decide re-arm; one-shot send never carries it).
      const unsigned flags =
          (cqe->flags & IORING_CQE_F_MORE) ? kCompletionMore : kCompletionNone;
      op->Complete(*worker_, cqe->res, flags);
    }
  }
}

bool IoUringBackend::ReapCompletions() {
  unsigned head = 0;
  io_uring_cqe* cqe = nullptr;
  unsigned processed = 0;
  io_uring_for_each_cqe(&ring_, head, cqe) {
    DispatchCqe(cqe);
    ++processed;
  }
  if (processed != 0) {
    io_uring_cq_advance(&ring_, processed);
    return true;
  }
  return false;
}

void IoUringBackend::DrainRecvRearm() {
  if (recv_rearm_queue_.empty()) {
    return;
  }
  std::vector<Connection*> batch;
  batch.swap(recv_rearm_queue_);
  for (Connection* connection : batch) {
    connection->needs_recv_rearm_ = false;
    if (connection->state_ == ConnectionState::kActive &&
        !connection->closed_ && !connection->closing_ &&
        !connection->recv_eof_ && !connection->recv_paused_) {
      auto status = StartRecvMultishot(connection);
      if (!status.ok()) {
        connection->last_error_ = status;
      }
    }
  }
}

bool IoUringBackend::Poll() {
  const bool processed = ReapCompletions();
  if (processed) {
    DrainRecvRearm();
  }
  return processed;
}

bool IoUringBackend::Wait(int timeout_ms) {
  io_uring_cqe* cqe = nullptr;
  int rc = 0;
  if (timeout_ms >= 0) {
    __kernel_timespec ts{
        .tv_sec = timeout_ms / 1000,
        .tv_nsec = static_cast<long long>(timeout_ms % 1000) * 1000 * 1000,
    };
    rc = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
    if (rc == -ETIME) {
      return true;  // tick: no completion, caller handles idle/reclaim
    }
  } else {
    rc = io_uring_wait_cqe(&ring_, &cqe);
  }
  if (rc == -EINTR || rc == -EAGAIN) {
    // A signal (debugger attach, profiler, timer) or a transient kernel
    // shortage aborts the wait without a completion. Neither is fatal: the
    // ring is intact and the caller re-enters on the next round.
    return true;
  }
  if (rc < 0) {
    spdlog::error("backend wait_cqe failed rc={}", rc);
    return false;
  }
  DispatchCqe(cqe);
  io_uring_cqe_seen(&ring_, cqe);
  Poll();  // drain any other ready completions + re-arm recvs
  return true;
}

void IoUringBackend::WakeRemote(int peer_ring_fd) noexcept {
  io_uring_sqe* sqe = AcquireSqe();  // never null while initialized
  io_uring_prep_msg_ring(sqe, peer_ring_fd, 0,
                         reinterpret_cast<std::uintptr_t>(&kCrossCoreWakeTag),
                         0);
  io_uring_sqe_set_data(sqe, &kCrossCoreWakeTag);
}

void IoUringBackend::WakeSelf() noexcept {
  if (wake_event_fd_ < 0) {
    return;
  }
  const std::uint64_t one = 1;
  const ssize_t result = ::write(wake_event_fd_, &one, sizeof(one));
  (void)result;
}

}  // namespace celer
