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

#ifndef CELER_NET_CONNECTION_H_
#define CELER_NET_CONNECTION_H_

#include <coroutine>
#include <cstdint>
#include <deque>
#include <memory>
#include <vector>

#include "absl/status/statusor.h"

namespace celer {

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
  absl::Status last_error_ = absl::OkStatus();

  std::vector<std::byte> read_buffer_;
  std::deque<ReceivedBuffer> received_buffers_;
  std::coroutine_handle<> read_waiter_{};
  bool recv_armed_ = false;
  bool needs_recv_rearm_ =
      false;  // re-arm deferred out of the completion handler
  bool recv_eof_ = false;
  RecvMode recv_mode_ = RecvMode::kMultishot;
  std::shared_ptr<TlsState> tls_state_;
  void* protocol_context_ = nullptr;
};

}  // namespace celer

#endif  // CELER_NET_CONNECTION_H_
