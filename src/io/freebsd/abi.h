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

#ifndef CELER_FREEBSD_ABI_H_
#define CELER_FREEBSD_ABI_H_

#ifdef _KERNEL
#include <sys/types.h>
#else
#include <stddef.h>
#include <stdint.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// The only boundary between the private FreeBSD namespace and the Linux host.
// Callbacks stay alive until all worker stacks have stopped. Packet callbacks
// must copy/retain bytes before returning: FreeBSD reclaims their mbuf
// afterward.
struct celer_bsd_host {
  void* (*allocate)(size_t size, size_t alignment);
  void (*release)(void* pointer);
  uint64_t (*monotonic_ns)(void);
  uint64_t (*realtime_ns)(void);
  void (*random_bytes)(void* buffer, size_t length);
  void (*log)(const char* bytes, size_t length);
  void (*abort_process)(void);
};

struct celer_bsd_interface {
  uint32_t address;
  uint32_t netmask;
  uint32_t gateway;
  unsigned char mac[6];
  uint16_t mtu;
  int (*transmit)(void* context, const void* bytes, size_t length);
  void* context;
};

// Initialize process-wide kernel services on worker 0 before other workers
// attach. Each API below must run on the thread that owns its stack/socket.
// Capacity may be queried before initialization, without a worker context.
unsigned celer_bsd_max_workers(void);
int celer_bsd_initialize(const struct celer_bsd_host* host, unsigned workers);
int celer_bsd_attach_worker(unsigned worker);
int celer_bsd_attach_interface(const struct celer_bsd_interface* config);
void celer_bsd_input(const void* bytes, size_t length);
void celer_bsd_poll(void);
uint64_t celer_bsd_deadline_ns(void);

// Socket pointers are opaque user-stack handles, never Linux file descriptors.
// Errors are positive FreeBSD errno values; the host adapter translates them.
struct socket;
int celer_bsd_listen(uint32_t address, uint16_t port, int backlog,
                     struct socket** result);
int celer_bsd_accept(struct socket* listener, struct socket** result);
int celer_bsd_receive(struct socket* socket, void* buffer, size_t size,
                      size_t* received);
int celer_bsd_send(struct socket* socket, const void* buffer, size_t size,
                   size_t* sent);
int celer_bsd_close(struct socket* socket);
int celer_bsd_peer(struct socket* socket, uint32_t* address, uint16_t* port);
int celer_bsd_disconnected(struct socket* socket);

#ifdef __cplusplus
}
#endif

#endif
