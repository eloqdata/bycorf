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

#pragma once

#include "bycorf/net/tcp_stream.h"

namespace bycorf {

// One attempt, owned by the connecting worker. The caller retains this object
// until ConnectTcpCancellable returns. Cancel is idempotent, including before
// the call starts and after it finishes; it must run on the same worker.
// Cancellation is a request, not a join: await the connection task before
// destroying this object or the worker. It never cancels an established stream.
class TcpConnectCancellation {
 public:
  TcpConnectCancellation() = default;
  TcpConnectCancellation(const TcpConnectCancellation&) = delete;
  TcpConnectCancellation& operator=(const TcpConnectCancellation&) = delete;

  void Cancel() noexcept;
  bool cancelled() const noexcept { return cancelled_; }

 private:
  friend class CancellableConnectAwaitable;
  Worker* worker_ = nullptr;
  IoCompletion* operation_ = nullptr;
  bool cancelled_ = false;
};

// Like ConnectTcp, with owner-worker cancellation and complete retirement of
// both connect and deadline before returning. A cancellation observed before
// return wins over a racing connection success, which is then closed.
Task<absl::StatusOr<TcpStream>> ConnectTcpCancellable(
    Worker& worker, std::string_view ip, std::uint16_t port,
    std::chrono::nanoseconds timeout, TcpConnectCancellation* cancellation);

}  // namespace bycorf
