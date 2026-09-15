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

#include "celer/net/socket_ops.h"

#include "celer/io/dpdk_backend.h"

namespace celer::detail {
bool IsDpdkSocket(int handle) noexcept { return handle >= 0x40000000; }
int CloseSocket(int handle) noexcept {
  return IsDpdkSocket(handle) ? DpdkBackend::Close(handle) : ::close(handle);
}
int SocketPeerName(int handle, sockaddr* address, socklen_t* length) noexcept {
  return IsDpdkSocket(handle) ? DpdkBackend::PeerName(handle, address, length)
                              : ::getpeername(handle, address, length);
}
}  // namespace celer::detail
