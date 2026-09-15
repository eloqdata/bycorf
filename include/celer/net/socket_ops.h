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

#ifndef CELER_NET_SOCKET_OPS_H_
#define CELER_NET_SOCKET_OPS_H_

#include <sys/socket.h>
#include <unistd.h>

namespace celer::detail {
#ifdef CELER_WITH_DPDK
// DPDK listener/connection handles inhabit a disjoint integer range. They are
// local to their worker and cannot be passed to host syscalls or transferred.
bool IsDpdkSocket(int handle) noexcept;
int CloseSocket(int handle) noexcept;
int SocketPeerName(int handle, sockaddr* address, socklen_t* length) noexcept;
#else
inline bool IsDpdkSocket(int) noexcept { return false; }
inline int CloseSocket(int fd) noexcept { return ::close(fd); }
inline int SocketPeerName(int fd, sockaddr* address,
                          socklen_t* length) noexcept {
  return ::getpeername(fd, address, length);
}
#endif
}  // namespace celer::detail

#endif
