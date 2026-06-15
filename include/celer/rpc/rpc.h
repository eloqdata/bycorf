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

#ifndef CELER_RPC_RPC_H_
#define CELER_RPC_RPC_H_

#include <coroutine>
#include <cstdint>
#include <deque>
#include <functional>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "celer/base/status.h"
#include "celer/net/tcp_service.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/task.h"

namespace celer::rpc {

using Bytes = std::vector<std::byte>;
using BytesView = std::span<const std::byte>;

// Fixed 16-byte wire header (no padding), followed by `len` payload bytes. This is
// a same-host benchmark transport, so native byte order is fine.
struct WireHeader {
  std::uint64_t req_id;
  std::uint32_t len;
  std::uint16_t verb;
  std::uint8_t type;  // kRequest / kResponse
  std::uint8_t pad;
};
static_assert(sizeof(WireHeader) == 16, "WireHeader must be 16 bytes");

enum : std::uint8_t { kRequest = 0, kResponse = 1 };

// Synchronous verb handler (fast path): request payload -> response payload. It
// runs inline in the connection's read loop, so it must not block. Registered
// before Start and read-only afterwards, hence safely shared across all workers.
using Handler = std::function<Bytes(BytesView)>;

// An RPC server: a TcpService that frames requests, dispatches by verb to a
// registered handler, and writes back the framed responses (batched per read).
class RpcServer : public TcpService {
 public:
  explicit RpcServer(std::uint16_t port) : TcpService(port) {}

  void OnVerb(std::uint16_t verb, Handler handler);  // call before Start

 protected:
  Task<Status> Serve(TcpStream stream) override;

 private:
  std::unordered_map<std::uint16_t, Handler> handlers_;
};

// A multiplexing RPC client connection owned by one worker. Many Calls may be in
// flight; responses are correlated to their waiters by req_id. Not thread-safe by
// design — each worker uses its own RpcClient (shared-nothing).
class RpcClient {
 public:
  RpcClient() = default;
  RpcClient(const RpcClient&) = delete;
  RpcClient& operator=(const RpcClient&) = delete;

  // Connect to (ip, port) on the current worker and start the response reader.
  // Uses a blocking connect() at setup (cold path); the data path is async.
  Task<Status> Connect(std::string_view ip, std::uint16_t port);

  // Send (verb, payload); suspend until the matching response arrives.
  Task<StatusOr<Bytes>> Call(std::uint16_t verb, BytesView payload);

  bool connected() const noexcept { return stream_.IsOpen(); }
  void Close() noexcept { stream_.Close(); }

 private:
  struct Pending {
    std::coroutine_handle<> waiter{};
    Bytes result;
    Status status = Status::Ok();
    bool done = false;
  };

  void SendFrame(const WireHeader& header, BytesView payload);  // enqueue + kick
  Task<Status> WriteLoop();   // single writer: serializes concurrent requests
  Task<Status> ReadLoop();    // reads responses, resumes pending by req_id

  TcpStream stream_;
  std::deque<Bytes> out_;
  bool writing_ = false;
  std::unordered_map<std::uint64_t, Pending*> pending_;
  std::uint64_t next_req_id_ = 1;
};

}  // namespace celer::rpc

#endif  // CELER_RPC_RPC_H_
