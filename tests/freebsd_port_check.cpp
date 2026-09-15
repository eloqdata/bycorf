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

// Standalone TCP echo fixture for the private BSD ABI. Linux TAP descriptors
// arrive from the Python harness, so this also runs under QEMU user emulation
// without requiring emulated DPDK or io_uring support.
#include <arpa/inet.h>
#include <poll.h>
#include <signal.h>
#include <sys/random.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <future>
#include <thread>
#include <vector>

#include "../src/io/freebsd/abi.h"

namespace {
// The bridge returns FreeBSD errno values, independently of host libc errno.
constexpr int kBsdWouldBlock = 35;
std::atomic<bool> stopping{false};
static_assert(std::atomic<bool>::is_always_lock_free);

void Check(bool ok, const char* operation, int error = 0) {
  if (ok) return;
  std::fprintf(stderr, "%s failed: %d\n", operation, error);
  std::abort();
}
void* Allocate(size_t size, size_t alignment) {
  void* result = nullptr;
  if (posix_memalign(&result, alignment, size)) return nullptr;
  // Detect accidental reliance on zero-filled host memory.
  std::memset(result, 0xa5, size);
  return result;
}
uint64_t Now(clockid_t clock) {
  timespec time{};
  Check(clock_gettime(clock, &time) == 0, "clock_gettime", errno);
  return uint64_t(time.tv_sec) * 1000000000 + time.tv_nsec;
}
void Random(void* buffer, size_t size) {
  auto* bytes = static_cast<char*>(buffer);
  while (size) {
    ssize_t n = getrandom(bytes, size, 0);
    if (n < 0 && errno == EINTR) continue;
    Check(n > 0, "getrandom", errno);
    bytes += n;
    size -= n;
  }
}
struct Session {
  struct socket* socket;
  std::array<char, 65536> bytes;
  size_t size = 0;
  size_t offset = 0;
};
struct Lane {
  unsigned worker;
  int fd;
  struct socket* listener = nullptr;
  std::vector<Session> sessions{};
  uint64_t accepted = 0;
  uint64_t echoed = 0;

  void Attach() {
    celer_bsd_interface interface{};
    interface.address = htonl(0xc6130002 + (worker << 8));
    interface.netmask = inet_addr("255.255.255.0");
    interface.mac[0] = 2;
    interface.mac[4] = worker;
    interface.mac[5] = 2;
    interface.mtu = 1500;
    interface.context = this;
    interface.transmit = [](void* context, const void* bytes, size_t length) {
      auto* lane = static_cast<Lane*>(context);
      ssize_t n;
      do {
        n = write(lane->fd, bytes, length);
      } while (n < 0 && errno == EINTR);
      // A full TAP queue may drop a frame, as a bounded NIC queue does. TCP
      // must recover; never retain an mbuf's borrowed bytes across callbacks.
      return n == static_cast<ssize_t>(length) ? 0 : 55;  // BSD ENOBUFS
    };
    int error = celer_bsd_attach_interface(&interface);
    Check(error == 0, "attach interface", error);
    error = celer_bsd_listen(interface.address, 16390, 128, &listener);
    Check(error == 0, "listen", error);
    std::fprintf(stderr, "READY worker %u\n", worker);
  }

  void Run() {
    while (!stopping.load(std::memory_order_relaxed)) {
      for (unsigned i = 0; i < 64; ++i) {
        std::array<char, 2048> frame;
        ssize_t n = read(fd, frame.data(), frame.size());
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && errno == EAGAIN) break;
        Check(n > 0, "TAP read", errno);
        celer_bsd_input(frame.data(), n);
      }
      celer_bsd_poll();
      for (unsigned i = 0; i < 16 && sessions.size() < 128; ++i) {
        struct socket* socket = nullptr;
        int error = celer_bsd_accept(listener, &socket);
        if (error == kBsdWouldBlock) break;
        Check(error == 0, "accept", error);
        sessions.push_back(Session{socket, {}});
        ++accepted;
      }
      for (auto it = sessions.begin(); it != sessions.end();) {
        if (it->offset == it->size) {
          int error = celer_bsd_receive(it->socket, it->bytes.data(),
                                        it->bytes.size(), &it->size);
          it->offset = 0;
          if (error == kBsdWouldBlock) {
            ++it;
            continue;
          }
          Check(error == 0, "receive", error);
          if (it->size == 0) {
            Check(celer_bsd_close(it->socket) == 0, "close session");
            it = sessions.erase(it);
            continue;
          }
        }
        size_t sent = 0;
        int error = celer_bsd_send(it->socket, it->bytes.data() + it->offset,
                                   it->size - it->offset, &sent);
        Check(error == 0 || error == kBsdWouldBlock, "send", error);
        it->offset += sent;
        echoed += sent;
        ++it;
      }
      // This fixture's bounded wait drives TCP timers even without packets.
      // Production parking and queue forwarding belong to dpdk_smoke.py.
      pollfd notification{fd, POLLIN, 0};
      poll(&notification, 1, 1);
    }
    for (auto& session : sessions)
      Check(celer_bsd_close(session.socket) == 0, "close live session");
    Check(celer_bsd_close(listener) == 0, "close listener");
    std::fprintf(stderr, "DONE worker %u: accepted=%lu echoed=%lu\n", worker,
                 accepted, echoed);
  }
};
}  // namespace

int main(int argc, char** argv) {
  Check(argc == 3, "usage: freebsd_port_check TAP_FD_0 TAP_FD_1");
  signal(SIGTERM, [](int) { stopping.store(true, std::memory_order_relaxed); });
  celer_bsd_host host{Allocate,
                      std::free,
                      [] { return Now(CLOCK_MONOTONIC); },
                      [] { return Now(CLOCK_REALTIME); },
                      Random,
                      [](const char* bytes, size_t length) {
                        std::fwrite(bytes, 1, length, stderr);
                      },
                      std::abort};
  int error = celer_bsd_initialize(&host, 2);
  Check(error == 0, "initialize", error);
  Lane first{0, std::atoi(argv[1])};
  Lane second{1, std::atoi(argv[2])};
  first.Attach();
  std::promise<void> attached;
  auto ready = attached.get_future();
  std::thread peer([&] {
    int attach_error = celer_bsd_attach_worker(1);
    Check(attach_error == 0, "attach worker", attach_error);
    second.Attach();
    attached.set_value();
    second.Run();
  });
  // SYSINIT/VNET attachment is serialized; data processing thereafter runs
  // concurrently, retaining each context on the thread that created it.
  ready.get();
  first.Run();
  peer.join();
}
