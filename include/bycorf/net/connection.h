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

#ifndef BYCORF_NET_CONNECTION_H_
#define BYCORF_NET_CONNECTION_H_

#include <coroutine>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <limits>
#include <memory>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/status/statusor.h"

namespace bycorf {

class Worker;
class TlsState;

enum class ConnectionState : std::uint8_t {
  kActive,
  kClosing,
  kDraining,
  kRetired,
};

enum class CloseMode : std::uint8_t {
  kLocalClose,
  kPeerClosed,
  kLocalError,
  kIdleTimeout,
  kWorkerShutdown,
};

enum class RecvMode : std::uint8_t {
  kMultishot,
  kOneShot,
};

struct RegisteredFile {
  int fd_ = -1;
  std::uint32_t fixed_index_ = 0;
  bool is_fixed_ = false;
};

struct ReceivedBuffer {
  std::uint16_t buffer_id_ = 0;
  std::uint32_t size_ = 0;
  std::uint32_t offset_ = 0;
};

struct Connection {
  using PeerDisconnectCallback = void (*)(void*) noexcept;

  Worker* worker_ = nullptr;
  std::uint64_t id_ = 0;
  RegisteredFile file_;
  ConnectionState state_ = ConnectionState::kActive;
  std::uint64_t generation_ = 0;
  std::int64_t last_active_ms_ = 0;

  bool closing_ = false;
  bool closed_ = true;
  bool retired_ = false;
  bool read_inflight_ = false;
  bool write_inflight_ = false;
  // Live connection handoff pauses recv before the fd is duplicated onto a
  // different worker.  In particular, an armed multishot recv must not keep
  // consuming bytes from the shared socket after the handoff.
  bool recv_paused_ = false;

  std::uint32_t inflight_ops_ = 0;
  // Session coroutines borrow their Connection storage for their whole
  // lifetime: BeginClose/RetireConnection still close the fd and wake waiters
  // immediately, but ReclaimConnections skips storage while it is borrowed, so
  // a session that resumes after an external close can never dereference
  // reclaimed memory. Borrow and release run on the owning worker thread (or
  // before the first await that could observe retirement).
  std::uint32_t storage_borrows_ = 0;
  absl::Status last_error_ = absl::OkStatus();

  std::vector<std::byte> read_buffer_;
  std::deque<ReceivedBuffer> received_buffers_;
  std::coroutine_handle<> read_waiter_{};
  bool recv_armed_ = false;
  bool needs_recv_rearm_ =
      false;  // re-arm deferred out of the completion handler
  bool recv_eof_ = false;
  bool peer_disconnect_poll_armed_ = false;
  bool peer_disconnect_poll_cancel_requested_ = false;
  PeerDisconnectCallback peer_disconnect_callback_ = nullptr;
  void* peer_disconnect_context_ = nullptr;
  RecvMode recv_mode_ = RecvMode::kMultishot;
  std::shared_ptr<TlsState> tls_state_;
  void* protocol_context_ = nullptr;
};

// Keep an already-live Connection's storage valid across coroutine suspension.
// Call on its owning worker before spawning/suspending the borrower, and pair
// every borrow with ReleaseConnectionStorage on that same worker. This does not
// keep the transport open or extend the lifetime of the worker itself. Null
// pointers and counter overflow fail-stop rather than permit premature reclaim.
inline void BorrowConnectionStorage(Connection* connection) noexcept {
  if (connection == nullptr || connection->storage_borrows_ ==
                                   std::numeric_limits<std::uint32_t>::max()) {
    std::abort();
  }
  ++connection->storage_borrows_;
}

// Release one storage borrow after all of its Connection users have unwound.
// Reclamation remains deferred to the worker's retired-connection sweep; other
// borrows and pending I/O must also drain. Null pointers and counter underflow
// fail-stop. Calls must be balanced with BorrowConnectionStorage.
inline void ReleaseConnectionStorage(Connection* connection) noexcept {
  if (connection == nullptr || connection->storage_borrows_ == 0) {
    std::abort();
  }
  --connection->storage_borrows_;
}

// Worker-local RAII borrow for a session that can outlive transport closure.
// The worker must remain alive until this guard and all other users unwind.
[[nodiscard]] inline auto MakeConnectionStorageBorrow(
    Connection* connection) noexcept {
  BorrowConnectionStorage(connection);
  return absl::MakeCleanup(
      [connection]() noexcept { ReleaseConnectionStorage(connection); });
}

using ConnectionStorageBorrow = decltype(MakeConnectionStorageBorrow(nullptr));

}  // namespace bycorf

#endif  // BYCORF_NET_CONNECTION_H_
