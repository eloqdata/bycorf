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

#include "internal.h"
#include <sys/mbuf.h>
#include <sys/mutex.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/sockopt.h>
#include <sys/sockbuf.h>
#include <sys/uio.h>
#include <net/vnet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

static void assert_owner(struct socket* so) {
  if (so->so_vnet != curvnet)
    panic("FreeBSD socket used from a different worker");
}
int bycorf_bsd_listen(uint32_t address, uint16_t port, int backlog,
                      struct socket** out) {
  *out = NULL;
  struct socket* so;
  int error = socreate(AF_INET, &so, SOCK_STREAM, IPPROTO_TCP,
                       curthread->td_ucred, curthread);
  if (error) return error;
  so->so_state |= SS_NBIO;
  so->so_options |= SO_REUSEADDR;
  struct sockaddr_in addr = {.sin_len = sizeof(addr),
                             .sin_family = AF_INET,
                             .sin_port = htons(port),
                             .sin_addr.s_addr = address};
  error = sobind(so, (struct sockaddr*)&addr, curthread);
  if (!error) error = solisten(so, backlog, curthread);
  if (error) {
    soclose(so);
    return error;
  }
  *out = so;
  return 0;
}
int bycorf_bsd_accept(struct socket* listener, struct socket** out) {
  assert_owner(listener);
  *out = NULL;
  struct socket* so;
  SOLISTEN_LOCK(listener);
  int error = solisten_dequeue(listener, &so, SOCK_NONBLOCK);
  if (error) return error;
  struct sockaddr_in address = {.sin_len = sizeof(address)};
  error = soaccept(so, (struct sockaddr*)&address);
  if (error) {
    soclose(so);
    return error;
  }
  // Match Bycorf's kernel sockets: small Redis replies are sent immediately.
  int enabled = 1;
  struct sockopt option = {.sopt_dir = SOPT_SET,
                           .sopt_level = IPPROTO_TCP,
                           .sopt_name = TCP_NODELAY,
                           .sopt_val = &enabled,
                           .sopt_valsize = sizeof(enabled),
                           .sopt_td = curthread};
  error = sosetopt(so, &option);
  if (error) {
    soclose(so);
    return error;
  }
  *out = so;
  return 0;
}
int bycorf_bsd_open_client(uint32_t address, uint16_t local_port,
                           struct socket** out) {
  *out = NULL;
  struct socket* so;
  int error = socreate(AF_INET, &so, SOCK_STREAM, IPPROTO_TCP,
                       curthread->td_ucred, curthread);
  if (error) return error;
  so->so_state |= SS_NBIO;
  // Each worker uses a disjoint source-port stripe. TIME_WAIT stays in the
  // same VNET, and replies can be routed without a shared mutable flow table.
  struct sockaddr_in local = {.sin_len = sizeof(local),
                               .sin_family = AF_INET,
                               .sin_port = htons(local_port),
                               .sin_addr.s_addr = address};
  error = sobind(so, (struct sockaddr*)&local, curthread);
  int enabled = 1;
  struct sockopt option = {.sopt_dir = SOPT_SET,
                           .sopt_level = IPPROTO_TCP,
                           .sopt_name = TCP_NODELAY,
                           .sopt_val = &enabled,
                           .sopt_valsize = sizeof(enabled),
                           .sopt_td = curthread};
  if (!error) error = sosetopt(so, &option);
  if (error) {
    soclose(so);
    return error;
  }
  *out = so;
  return 0;
}
int bycorf_bsd_connect(struct socket* so, uint32_t address, uint16_t port) {
  assert_owner(so);
  struct sockaddr_in remote = {.sin_len = sizeof(remote),
                                .sin_family = AF_INET,
                                .sin_port = htons(port),
                                .sin_addr.s_addr = address};
  return soconnect(so, (struct sockaddr*)&remote, curthread);
}
int bycorf_bsd_connect_status(struct socket* so) {
  assert_owner(so);
  if (so->so_error) return so->so_error;
  if (so->so_state & SS_ISCONNECTED) return 0;
  if (so->so_state & SS_ISCONNECTING) return EAGAIN;
  return ECONNABORTED;
}
int bycorf_bsd_receive(struct socket* so, void* buffer, size_t size,
                       size_t* received) {
  assert_owner(so);
  *received = 0;
  if (size > INT_MAX) return EINVAL;
  struct iovec iov = {.iov_base = buffer, .iov_len = size};
  struct uio uio = {.uio_iov = &iov,
                    .uio_iovcnt = 1,
                    .uio_resid = size,
                    .uio_segflg = UIO_SYSSPACE,
                    .uio_rw = UIO_READ,
                    .uio_td = curthread};
  int flags = MSG_DONTWAIT;
  int error = soreceive(so, NULL, &uio, NULL, NULL, &flags);
  *received = size - uio.uio_resid;
  // The native API can return EWOULDBLOCK after consuming part of a stream.
  return *received ? 0 : error;
}
int bycorf_bsd_send(struct socket* so, const void* buffer, size_t size,
                    size_t* sent) {
  assert_owner(so);
  *sent = 0;
  if (size > INT_MAX) return EINVAL;
  struct iovec iov = {.iov_base = __DECONST(void*, buffer), .iov_len = size};
  struct uio uio = {.uio_iov = &iov,
                    .uio_iovcnt = 1,
                    .uio_resid = size,
                    .uio_segflg = UIO_SYSSPACE,
                    .uio_rw = UIO_WRITE,
                    .uio_td = curthread};
  int error = sosend(so, NULL, &uio, NULL, NULL, MSG_DONTWAIT | MSG_NOSIGNAL,
                     curthread);
  *sent = size - uio.uio_resid;
  return *sent ? 0 : error;
}
int bycorf_bsd_close(struct socket* so) {
  assert_owner(so);
  return soclose(so);
}
int bycorf_bsd_peer(struct socket* so, uint32_t* address, uint16_t* port) {
  assert_owner(so);
  struct sockaddr_in peer = {.sin_len = sizeof(peer)};
  int error = sopeeraddr(so, (struct sockaddr*)&peer);
  if (!error) {
    *address = peer.sin_addr.s_addr;
    *port = ntohs(peer.sin_port);
  }
  return error;
}
int bycorf_bsd_disconnected(struct socket* so) {
  assert_owner(so);
  return so->so_error || (so->so_rcv.sb_state & SBS_CANTRCVMORE);
}
