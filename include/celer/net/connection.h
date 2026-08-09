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
#include <vector>

#include "absl/status/statusor.h"

namespace celer {

class Worker;

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

struct RegisteredFile {
  int fd = -1;
  std::uint32_t fixed_index = 0;
  bool is_fixed = false;
};

struct ReceivedBuffer {
  std::uint16_t buffer_id = 0;
  std::uint32_t size = 0;
  std::uint32_t offset = 0;
};

struct Connection {
  Worker* worker = nullptr;
  std::uint64_t id = 0;
  RegisteredFile file;
  ConnectionState state = ConnectionState::kActive;
  std::uint64_t generation = 0;
  std::int64_t last_active_ms = 0;

  bool closing = false;
  bool closed = true;
  bool retired = false;
  bool read_inflight = false;
  bool write_inflight = false;

  std::uint32_t inflight_ops = 0;
  absl::Status last_error = absl::OkStatus();

  std::vector<std::byte> read_buffer;
  std::deque<ReceivedBuffer> received_buffers;
  std::coroutine_handle<> read_waiter{};
  bool recv_armed = false;
  bool needs_recv_rearm = false;  // re-arm deferred out of the completion handler
  bool recv_eof = false;
  void* protocol_context = nullptr;
};

}  // namespace celer

#endif  // CELER_NET_CONNECTION_H_
