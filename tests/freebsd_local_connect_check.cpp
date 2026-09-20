// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

// Real BSD sockets and packet input, without DPDK, TAP, or kernel TCP.
#include <arpa/inet.h>
#include <sys/random.h>
#include <time.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <vector>

#include "../src/io/freebsd/abi.h"

namespace {
std::deque<std::vector<unsigned char>> packets;
unsigned wire_packets = 0;
constexpr uint32_t kAddress = 0xc6130002;

void Check(bool ok, const char* what, int error = 0) {
  if (ok) return;
  std::fprintf(stderr, "FAIL %s: BSD errno %d\n", what, error);
  std::exit(1);
}
uint64_t Now(clockid_t clock) {
  timespec t{};
  Check(clock_gettime(clock, &t) == 0, "clock");
  return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}
void Pump() {
  // Queueing, including to this same worker, prevents recursive TCP input
  // while the emitting socket still holds its PCB lock.
  unsigned budget = 4096;
  while (!packets.empty() && budget--) {
    auto frame = std::move(packets.front());
    packets.pop_front();
    bycorf_bsd_local_input(frame.data(), frame.size());
  }
  Check(packets.empty(), "bounded local packet progress");
  bycorf_bsd_poll();
}
void Exchange(struct socket* source, struct socket* target) {
  std::array<unsigned char, 32769> expected{}, actual{};
  for (size_t i = 0; i < expected.size(); ++i) expected[i] = i * 37;
  size_t sent = 0, received = 0;
  const auto deadline = Now(CLOCK_MONOTONIC) + 2000000000;
  while (received < actual.size() && Now(CLOCK_MONOTONIC) < deadline) {
    size_t n = 0;
    if (sent < expected.size()) {
      const int error = bycorf_bsd_send(source, expected.data() + sent,
                                        expected.size() - sent, &n);
      Check(error == 0 || error == 35, "local send", error);
      sent += n;
    }
    Pump();
    const int error = bycorf_bsd_receive(target, actual.data() + received,
                                         actual.size() - received, &n);
    Check(error == 0 || error == 35, "local receive", error);
    received += n;
  }
  Check(sent == expected.size() && received == actual.size() &&
            actual == expected,
        "segmented binary local payload");
}
}  // namespace

int main() {
  bycorf_bsd_host host{
      [](size_t n, size_t alignment) -> void* {
        void* p = nullptr;
        if (posix_memalign(&p, alignment, n)) return nullptr;
        std::memset(p, 0xa5, n);
        return p;
      },
      std::free,
      [] { return Now(CLOCK_MONOTONIC); },
      [] { return Now(CLOCK_REALTIME); },
      [](void* p, size_t n) {
        auto* bytes = static_cast<unsigned char*>(p);
        while (n) {
          auto used = getrandom(bytes, n, 0);
          if (used < 0 && errno == EINTR) continue;
          Check(used > 0, "random");
          bytes += used;
          n -= used;
        }
      },
      [](const char* p, size_t n) { std::fwrite(p, 1, n, stderr); },
      std::abort};
  Check(bycorf_bsd_initialize(&host, 1) == 0, "initialize");
  bycorf_bsd_interface interface{};
  interface.address = htonl(kAddress);
  interface.netmask = htonl(0xffffff00);
  interface.mac[0] = 2;
  interface.mac[5] = 2;
  interface.mtu = 1500;
  interface.transmit = [](void*, const void* data, size_t n) {
    auto* bytes = static_cast<const unsigned char*>(data);
    const uint32_t destination = htonl(kAddress);
    if (n >= 34 && bytes[12] == 8 && bytes[13] == 0 &&
        std::memcmp(bytes + 30, &destination, 4) == 0)
      packets.emplace_back(bytes, bytes + n);
    else
      ++wire_packets;
    return 0;
  };
  Check(bycorf_bsd_attach_interface(&interface) == 0, "attach");
  struct socket *listener = nullptr, *client = nullptr, *accepted = nullptr;
  Check(bycorf_bsd_listen(interface.address, 16390, 16, &listener) == 0,
        "listen");
  const auto before = wire_packets;
  Check(bycorf_bsd_open_client(interface.address, 40000, &client) == 0,
        "open client");
  int error = bycorf_bsd_connect(client, interface.address, 16390);
  Check(error == 0 || error == 36, "connect same address/port", error);
  Check(packets.size() == 1, "local SYN queued");
  // The identical locally sourced SYN is invalid on the physical interface.
  // Keeping source validation there must not prevent trusted loopback input.
  bycorf_bsd_input(packets.front().data(), packets.front().size());
  Check(packets.size() == 1, "physical input rejects a spoofed local source");
  Check(bycorf_bsd_accept(listener, &accepted) == 35 && !accepted,
        "physical local-source packet does not create a connection");
  Pump();
  error = bycorf_bsd_connect_status(client);
  Check(error == 0, "local handshake", error);
  Check(bycorf_bsd_accept(listener, &accepted) == 0, "local accept");
  Exchange(client, accepted);
  Exchange(accepted, client);
  Check(bycorf_bsd_close(accepted) == 0, "close accepted");
  Pump();
  unsigned char byte;
  size_t n = 1;
  Check(bycorf_bsd_receive(client, &byte, 1, &n) == 0 && n == 0, "local EOF");
  Check(bycorf_bsd_close(client) == 0, "close client");
  Pump();
  Check(bycorf_bsd_open_client(interface.address, 40001, &client) == 0,
        "open refused client");
  error = bycorf_bsd_connect(client, interface.address, 16391);
  Check(error == 0 || error == 36, "start refused connect", error);
  Pump();
  Check(bycorf_bsd_connect_status(client) == 61, "local ECONNREFUSED");
  Check(bycorf_bsd_close(client) == 0, "close refused client");
  Check(bycorf_bsd_close(listener) == 0, "close listener");
  Check(wire_packets == before, "local TCP never reaches physical output");
  std::puts(
      "PASS BSD local connect, binary duplex transfer, EOF, refusal and "
      "physical source validation");
}
