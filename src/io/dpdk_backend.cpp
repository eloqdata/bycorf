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

#include "bycorf/io/dpdk_backend.h"

#include <arpa/inet.h>
#include <poll.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_ring.h>
#include <sys/random.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bycorf/io/dpdk_environment.h"
#include "bycorf/net/socket_ops.h"
#include "bycorf/runtime/worker.h"
#include "freebsd/abi.h"
#include "freebsd_errno.h"

namespace bycorf {
namespace {
using BsdSocket = struct ::socket;
constexpr unsigned kBurst = 32;
constexpr unsigned kQueueSize = 8192;
constexpr unsigned kMaxWorkers = BYCORF_DPDK_MAX_WORKERS;
static_assert(kMaxWorkers > 0 && kMaxWorkers < RTE_MAX_LCORE);
constexpr int kFirstHandle = 0x40000000;
// Bit 30 distinguishes BSD handles from kernel fds; the remaining positive
// bits partition the namespace by owner. Fixed 24-bit slots overflowed signed
// handles at worker 64. Leave the last slot unused so increment never wraps.
constexpr unsigned kHandleBits = 30 - std::bit_width(kMaxWorkers - 1);
constexpr unsigned kHandleMask = (1U << kHandleBits) - 1;
constexpr std::size_t kMaxFrame = 1518;
// rte_ring_create reserves pointer-sized slots; the element API below stores
// their address representation as the uint64_t type used by DPDK's copy code.
static_assert(sizeof(void*) == sizeof(std::uint64_t));

std::uint64_t ClockNs(clockid_t clock) {
  timespec time{};
  clock_gettime(clock, &time);
  return std::uint64_t(time.tv_sec) * 1'000'000'000 + time.tv_nsec;
}
std::uint64_t NowNs() { return ClockNs(CLOCK_MONOTONIC); }
std::uint64_t WallNs() { return ClockNs(CLOCK_REALTIME); }
void* Allocate(std::size_t bytes, std::size_t alignment) {
  void* pointer = nullptr;
  return posix_memalign(&pointer, alignment, bytes) ? nullptr : pointer;
}
void Random(void* buffer, std::size_t length) {
  auto* bytes = static_cast<unsigned char*>(buffer);
  while (length) {
    const ssize_t n = getrandom(bytes, length, 0);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) std::abort();
    bytes += n;
    length -= n;
  }
}
void Log(const char* bytes, std::size_t size) {
  std::fwrite(bytes, 1, size, stderr);
}
const bycorf_bsd_host kHost{Allocate, std::free, NowNs,     WallNs,
                            Random,   Log,       std::abort};

struct alignas(64) Lane {
  rte_ring* rx = nullptr;
  rte_ring* tx = nullptr;
  std::atomic<bool> parked{false};
  int ring_fd = -1;
};
struct Fabric {
  std::mutex mutex;
  std::condition_variable attached;
  bool prepared = false, stopped = false, bsd_ready = false;
  absl::Status failure;
  unsigned workers = 0, queues = 0;
  std::atomic<unsigned> ready{0};
  std::array<Lane, kMaxWorkers> lanes;
  std::uint16_t port = RTE_MAX_ETHPORTS;
  rte_mempool* pool = nullptr;
  bycorf_bsd_interface interface{};
  bool adaptive = false, tap = false, rss_owner = false;
} fabric;

std::uint16_t Read16(const unsigned char* p) {
  return (std::uint16_t(p[0]) << 8) | p[1];
}
// Hash steering assigns every segment to the same software owner. RSS steering
// instead trusts the receiving queue: its mapping must stay fixed while any TCP
// connection is live, because each worker owns an independent TCP stack.
// Fragmented IP and VLAN frames are intentionally outside this MTU-1500 test
// profile: guessing transport ports in a noninitial fragment would misroute it.
int Destination(const unsigned char* frame, std::size_t length,
                unsigned rx_owner) {
  if (length < 14) return -1;
  const auto type = Read16(frame + 12);
  if (type == 0x0806) return -2;  // ARP updates every worker's neighbour cache.
  if (type != 0x0800 || length < 34 || (frame[14] >> 4) != 4) return -1;
  const unsigned ihl = (frame[14] & 15) * 4;
  if (ihl < 20 || length < 14 + ihl || (Read16(frame + 20) & 0x3fff)) return -1;
  if (frame[23] == 1) return -2;  // ICMP errors can concern any local TCP flow.
  if (frame[23] != 6 || length < 14 + ihl + 20) return -1;
  if (fabric.rss_owner) return static_cast<int>(rx_owner);
  std::uint32_t hash = 2166136261u;
  for (unsigned i = 26; i < 34; ++i) hash = (hash ^ frame[i]) * 16777619u;
  for (unsigned i = 14 + ihl; i < 18 + ihl; ++i)
    hash = (hash ^ frame[i]) * 16777619u;
  hash ^= hash >> 16;
  return static_cast<int>(hash % fabric.workers);
}

void FreeRing(rte_ring*& ring) {
  if (!ring) return;
  std::uint64_t address;
  while (!rte_ring_dequeue_elem(ring, &address, sizeof(address)))
    rte_pktmbuf_free(reinterpret_cast<rte_mbuf*>(address));
  rte_ring_free(ring);
  ring = nullptr;
}
void StopPort() {
  if (fabric.port != RTE_MAX_ETHPORTS) {
    rte_eth_dev_stop(fabric.port);
    rte_eth_dev_close(fabric.port);
    fabric.port = RTE_MAX_ETHPORTS;
  }
  for (auto& lane : fabric.lanes) {
    FreeRing(lane.rx);
    FreeRing(lane.tx);
  }
  if (fabric.pool) {
    rte_mempool_free(fabric.pool);
    fabric.pool = nullptr;
  }
}
absl::Status DeviceError(const char* operation, int rc) {
  return absl::FailedPreconditionError(std::string(operation) + ": " +
                                       rte_strerror(rc < 0 ? -rc : rc));
}
}  // namespace

class DpdkBackend::Impl {
 public:
  explicit Impl(DpdkBackend& parent) : backend(parent) {}
  DpdkBackend& backend;
  Worker* worker = nullptr;
  unsigned id = 0;
  bool initialized = false, polling = false;
  std::size_t buffer_size = 4096;
  int next_handle = kFirstHandle;
  std::unordered_map<int, BsdSocket*> sockets;
  struct Request {
    int fd;
    IoCompletion* tag;
    std::span<const std::byte> bytes;
    const msghdr* message = nullptr;
  };
  struct Completion {
    IoCompletion* tag;
    int result;
    unsigned flags;
  };
  std::vector<Request> accepts, sends;
  std::vector<Connection*> reads, disconnects;
  std::vector<Completion> completions;
  std::uint64_t rx = 0, tx = 0, forwarded_rx = 0, forwarded_tx = 0, drops = 0;
  std::uint64_t waits = 0, arms = 0, notifications = 0;
  bool rx_can_wait = false;

  struct Notification : IoCompletion {
    Impl* owner = nullptr;
    int fd = -1;
    bool armed = false;
    void Complete(Worker&, int result, unsigned) override {
      armed = false;
      if (result < 0) {
        owner->rx_can_wait = false;
        return;
      }
      ++owner->notifications;
      // TAP's notification descriptor IS its packet stream. Reading an
      // eventfd word from it would consume/corrupt a packet. Hardware eventfds
      // are acknowledged here, then RX interrupts are disabled before polling.
      if (!fabric.tap) {
        std::uint64_t value;
        (void)::read(fd, &value, sizeof(value));
      }
    }
  } notification;

  BsdSocket* Find(int handle) {
    const auto found = sockets.find(handle);
    return found == sockets.end() ? nullptr : found->second;
  }
  int Own(BsdSocket* so) {
    if ((next_handle & kHandleMask) == kHandleMask) {
      bycorf_bsd_close(so);
      errno = EMFILE;
      return -1;
    }
    const int handle = next_handle++;
    sockets.emplace(handle, so);
    return handle;
  }
  void QueueCompletion(IoCompletion* tag, int result,
                       unsigned flags = kCompletionNone) {
    completions.push_back({tag, result, flags});
  }
  void Wake(unsigned target) {
    Lane& lane = fabric.lanes[target];
    // Publication precedes this exchange. Wait marks parked before rechecking
    // its rings, so work either prevents the wait or has a MSG_RING wake
    // queued.
    if (target != id && lane.parked.exchange(false, std::memory_order_acq_rel))
      backend.kernel_.WakeRemote(lane.ring_fd);
  }
  bool Enqueue(rte_ring* ring, rte_mbuf* packet, unsigned target) {
    // DPDK's inline ring copy accesses eight-byte elements as uint64_t. Passing
    // a pointer object/array through its void** API violates C++ strict
    // aliasing and lets optimized builds consume uninitialized packet pointers.
    // Store integer addresses at this boundary; ownership still moves only on
    // success.
    std::uint64_t address = reinterpret_cast<std::uintptr_t>(packet);
    if (rte_ring_enqueue_elem(ring, &address, sizeof(address))) {
      ++drops;
      rte_pktmbuf_free(packet);
      return false;
    }
    Wake(target);
    return true;
  }
  void Input(rte_mbuf* packet) {
    std::array<unsigned char, kMaxFrame> scratch;
    const std::size_t length = rte_pktmbuf_pkt_len(packet);
    const void* bytes =
        length <= scratch.size()
            ? rte_pktmbuf_read(packet, 0, length, scratch.data())
            : nullptr;
    if (bytes) {
      bycorf_bsd_input(bytes, length);
      ++rx;
    } else
      ++drops;
    rte_pktmbuf_free(packet);
  }
  void Route(rte_mbuf* packet) {
    std::array<unsigned char, kMaxFrame> scratch;
    const std::size_t length = rte_pktmbuf_pkt_len(packet);
    const auto* bytes = static_cast<const unsigned char*>(
        length <= scratch.size()
            ? rte_pktmbuf_read(packet, 0, length, scratch.data())
            : nullptr);
    const int target = bytes ? Destination(bytes, length, id) : -1;
    if (target == -2) {
      for (unsigned i = 0; i < fabric.workers; ++i) {
        if (i == id) continue;
        auto* copy = rte_pktmbuf_clone(packet, fabric.pool);
        if (copy) {
          Enqueue(fabric.lanes[i].rx, copy, i);
          ++forwarded_rx;
        } else
          ++drops;
      }
      Input(packet);
    } else if (target == static_cast<int>(id))
      Input(packet);
    else if (target >= 0) {
      Enqueue(fabric.lanes[target].rx, packet, target);
      ++forwarded_rx;
    } else {
      ++drops;
      rte_pktmbuf_free(packet);
    }
  }
  static int Transmit(void* context, const void* data, std::size_t length) {
    auto& self = *static_cast<Impl*>(context);
    const auto* bytes = static_cast<const unsigned char*>(data);
    // All VNETs learn broadcast control traffic, but only worker 0 responds to
    // ARP requests or echo requests for the shared address. ARP requests made
    // by any worker remain necessary to resolve its outgoing neighbour cache.
    if (self.id && length >= 42 && Read16(bytes + 12) == 0x0806 &&
        Read16(bytes + 20) == 2)
      return 0;
    if (self.id && length >= 35 && Read16(bytes + 12) == 0x0800 &&
        bytes[23] == 1 && bytes[34] == 0)
      return 0;
    if (length > kMaxFrame) {
      ++self.drops;
      return 40;
    }  // BSD EMSGSIZE.
    rte_mbuf* packet = rte_pktmbuf_alloc(fabric.pool);
    if (!packet) {
      ++self.drops;
      return 55;
    }  // BSD ENOBUFS.
    void* output = rte_pktmbuf_append(packet, length);
    if (!output) {
      rte_pktmbuf_free(packet);
      ++self.drops;
      return 55;
    }
    std::memcpy(output, data, length);
    const unsigned target = self.id % fabric.queues;
    if (target != self.id) ++self.forwarded_tx;
    return self.Enqueue(fabric.lanes[target].tx, packet, target) ? 0 : 55;
  }
  bool FlushTx() {
    if (id >= fabric.queues) return false;
    std::uint64_t items[kBurst];
    const unsigned n = rte_ring_dequeue_burst_elem(
        fabric.lanes[id].tx, items, sizeof(items[0]), kBurst, nullptr);
    if (!n) return false;
    rte_mbuf* packets[kBurst];
    for (unsigned i = 0; i < n; ++i)
      packets[i] = reinterpret_cast<rte_mbuf*>(items[i]);
    const unsigned sent = rte_eth_tx_burst(fabric.port, id, packets, n);
    tx += sent;
    // TX pressure is bounded. Unaccepted packets remain application-owned;
    // dropping and freeing them lets native TCP retransmit without a leak or
    // an unbounded retry queue that could prevent shutdown.
    for (unsigned i = sent; i < n; ++i) {
      rte_pktmbuf_free(packets[i]);
      ++drops;
    }
    return true;
  }
  bool Packets() {
    if (fabric.ready.load(std::memory_order_acquire) != fabric.workers)
      return false;
    bool work = false;
    if (id < fabric.queues) {
      rte_mbuf* packets[kBurst];
      const unsigned n = rte_eth_rx_burst(fabric.port, id, packets, kBurst);
      for (unsigned i = 0; i < n; ++i) Route(packets[i]);
      work |= n != 0;
    }
    std::uint64_t items[kBurst];
    const unsigned n = rte_ring_dequeue_burst_elem(
        fabric.lanes[id].rx, items, sizeof(items[0]), kBurst, nullptr);
    for (unsigned i = 0; i < n; ++i)
      Input(reinterpret_cast<rte_mbuf*>(items[i]));
    work |= n != 0;
    bycorf_bsd_poll();
    return FlushTx() || work;
  }
  void FinishRead(Connection* connection) {
    connection->recv_armed_ = false;
    --connection->inflight_ops_;
    if (connection->read_waiter_) {
      auto waiter = std::exchange(connection->read_waiter_, {});
      connection->read_inflight_ = false;
      worker->Enqueue(waiter);
    }
  }
  bool SocketCompletions() {
    bool work = false;
    for (std::size_t i = 0; i < accepts.size();) {
      const auto request = accepts[i];
      BsdSocket* listener = Find(request.fd);
      BsdSocket* accepted = nullptr;
      const int error =
          listener ? BsdError(bycorf_bsd_accept(listener, &accepted)) : EBADF;
      if (error == EAGAIN) {
        ++i;
        continue;
      }
      if (!error) {
        const int handle = Own(accepted);
        QueueCompletion(request.tag, handle < 0 ? -errno : handle,
                        kCompletionMore);
        ++i;
      } else {
        accepts.erase(accepts.begin() + i);
        QueueCompletion(request.tag, -error);
      }
      work = true;
    }
    for (std::size_t i = 0; i < sends.size();) {
      const auto request = sends[i];
      BsdSocket* so = Find(request.fd);
      std::size_t sent = 0;
      int error = so ? 0 : EBADF;
      if (so && request.message) {
        // Sendv exposes a stream, so completing after a prefix is valid. The
        // caller owns iovecs until Complete and resubmits any unsent suffix.
        for (std::size_t v = 0; v < request.message->msg_iovlen; ++v) {
          const auto& iov = request.message->msg_iov[v];
          std::size_t part = 0;
          const auto size = std::min<std::size_t>(iov.iov_len, INT_MAX - sent);
          error = BsdError(bycorf_bsd_send(so, iov.iov_base, size, &part));
          sent += part;
          if (error || part < iov.iov_len || sent >= INT_MAX) break;
        }
      } else if (so) {
        error = BsdError(bycorf_bsd_send(
            so, request.bytes.data(),
            std::min<std::size_t>(request.bytes.size(), INT_MAX), &sent));
      }
      if (error == EAGAIN && !sent) {
        ++i;
        continue;
      }
      sends.erase(sends.begin() + i);
      QueueCompletion(request.tag, sent ? static_cast<int>(sent) : -error);
      work = true;
    }
    for (std::size_t i = 0; i < reads.size();) {
      Connection* c = reads[i];
      BsdSocket* so = Find(c->file_.fd_);
      std::size_t got = 0;
      const int error =
          so && !c->closing_
              ? BsdError(bycorf_bsd_receive(so, c->read_buffer_.data(),
                                            c->read_buffer_.size(), &got))
              : ECANCELED;
      if (error == EAGAIN) {
        ++i;
        continue;
      }
      reads.erase(reads.begin() + i);
      if (got) {
        c->received_buffers_.push_back({0, static_cast<std::uint32_t>(got), 0});
        c->last_active_ms_ = NowNs() / 1'000'000;
      } else if (!error)
        c->recv_eof_ = true;
      else if (error != ECANCELED)
        c->last_error_ = absl::UnavailableError(std::string("FreeBSD recv: ") +
                                                std::strerror(error));
      FinishRead(c);
      work = true;
    }
    for (std::size_t i = 0; i < disconnects.size();) {
      Connection* c = disconnects[i];
      BsdSocket* so = Find(c->file_.fd_);
      const bool cancel =
          c->peer_disconnect_poll_cancel_requested_ || c->closing_ || !so;
      if (!cancel && !bycorf_bsd_disconnected(so)) {
        ++i;
        continue;
      }
      disconnects.erase(disconnects.begin() + i);
      c->peer_disconnect_poll_armed_ = false;
      c->peer_disconnect_poll_cancel_requested_ = false;
      --c->inflight_ops_;
      if (!cancel && c->peer_disconnect_callback_)
        c->peer_disconnect_callback_(c->peer_disconnect_context_);
      work = true;
    }
    // Callbacks may submit or cancel another operation. Finish iteration over
    // all pending containers first; new completions are delivered next round.
    std::vector<Completion> batch;
    batch.swap(completions);
    work |= !batch.empty();
    for (const auto& item : batch)
      item.tag->Complete(*worker, item.result, item.flags);
    return work;
  }
};

thread_local DpdkBackend::Impl* DpdkBackend::current_ = nullptr;

DpdkBackend::DpdkBackend(IoUringBackend& kernel)
    : kernel_(kernel), impl_(std::make_unique<Impl>(*this)) {}
DpdkBackend::~DpdkBackend() { Shutdown(); }

absl::Status DpdkBackend::PrepareRuntime(unsigned workers) {
  std::lock_guard lock(fabric.mutex);
  if (fabric.prepared)
    return absl::FailedPreconditionError(
        "DPDK prototype supports one Runtime per process");
  if (!workers || workers > kMaxWorkers)
    return absl::InvalidArgumentError("DPDK build supports 1.." +
                                      std::to_string(kMaxWorkers) + " workers");
  if (workers > bycorf_bsd_max_workers())
    return absl::FailedPreconditionError(
        "FreeBSD worker capacity does not match the DPDK backend; rebuild");
  auto status = EnsureDpdkEnvironment();
  if (!status.ok()) return status;
  // Main/control and other registered threads consume slots too. Check before
  // configuring the port; registration still detects a concurrent claimant.
  unsigned occupied = 0;
  rte_lcore_iterate(
      [](unsigned, void* count) {
        ++*static_cast<unsigned*>(count);
        return 0;
      },
      &occupied);
  const unsigned available = RTE_MAX_LCORE - occupied;
  if (workers > available)
    return absl::ResourceExhaustedError(
        "DPDK needs " + std::to_string(workers) + " worker lcore slots, but " +
        std::to_string(available) +
        " remain; increase build capacity or "
        "release other EAL registrations");
  fabric.workers = workers;
  const char* mode = std::getenv("BYCORF_DPDK_MODE");
  if (mode && std::strcmp(mode, "poll") && std::strcmp(mode, "adaptive"))
    return absl::InvalidArgumentError(
        "BYCORF_DPDK_MODE must be poll or adaptive");
  fabric.adaptive = mode && std::string_view(mode) == "adaptive";
  const char* steering = std::getenv("BYCORF_DPDK_RX_STEERING");
  if (steering && std::strcmp(steering, "hash") && std::strcmp(steering, "rss"))
    return absl::InvalidArgumentError(
        "BYCORF_DPDK_RX_STEERING must be hash or rss");
  fabric.rss_owner = steering && std::string_view(steering) == "rss";
  const char* address = std::getenv("BYCORF_DPDK_IP");
  const char* netmask = std::getenv("BYCORF_DPDK_NETMASK");
  const char* gateway = std::getenv("BYCORF_DPDK_GATEWAY");
  if (inet_pton(AF_INET, address ? address : "198.18.0.2",
                &fabric.interface.address) != 1 ||
      inet_pton(AF_INET, netmask ? netmask : "255.255.255.0",
                &fabric.interface.netmask) != 1 ||
      (gateway && inet_pton(AF_INET, gateway, &fabric.interface.gateway) != 1))
    return absl::InvalidArgumentError("invalid DPDK IPv4 configuration");
  if (rte_eth_dev_count_avail() != 1)
    return absl::FailedPreconditionError(
        "DPDK prototype requires exactly one allowlisted physical or virtual "
        "port");
  // Use the same unowned-port view as count_avail(). A netvsc port owns its
  // accelerated VF, which may have a lower port ID; configuring that child
  // directly bypasses the parent PMD's fallback and device lifecycle.
  fabric.port = static_cast<std::uint16_t>(
      rte_eth_find_next_owned_by(0, RTE_ETH_DEV_NO_OWNER));
  rte_eth_dev_info info{};
  int rc = rte_eth_dev_info_get(fabric.port, &info);
  if (rc) return DeviceError("ethdev info", rc);
  fabric.tap =
      info.driver_name && std::string_view(info.driver_name) == "net_tap";
  unsigned requested = workers;
  if (const char* queues = std::getenv("BYCORF_DPDK_QUEUES")) {
    char* end = nullptr;
    const unsigned long value = std::strtoul(queues, &end, 10);
    if (!*queues || *end || !value || value > workers)
      return absl::InvalidArgumentError(
          "BYCORF_DPDK_QUEUES must be between 1 and the worker count");
    requested = value;
  }
  fabric.queues = std::min(
      {requested, unsigned(info.max_rx_queues), unsigned(info.max_tx_queues)});
  if (!fabric.queues)
    return absl::FailedPreconditionError("port has no usable RX/TX queue pair");
  rte_eth_conf config{};
  config.intr_conf.rxq = fabric.adaptive;
  const auto rss = info.flow_type_rss_offloads & RTE_ETH_RSS_NONFRAG_IPV4_TCP;
  // Do not silently collapse connections onto a subset of workers. RSS mode
  // removes software redistribution and needs a stable TCP-aware queue owner.
  // A single worker needs no hashing; multi-worker PMDs must provide TCP RSS.
  if (fabric.rss_owner && (fabric.queues != workers || (workers > 1 && !rss)))
    return absl::FailedPreconditionError(
        "RSS steering requires one RX/TX pair per worker and IPv4 TCP RSS");
  if (fabric.queues > 1 && rss) {
    config.rxmode.mq_mode = RTE_ETH_MQ_RX_RSS;
    config.rx_adv_conf.rss_conf.rss_hf = rss;
  }
  rc =
      rte_eth_dev_configure(fabric.port, fabric.queues, fabric.queues, &config);
  if (rc && fabric.adaptive) {
    config.intr_conf.rxq = 0;
    fabric.adaptive = false;
    std::fprintf(stderr, "DPDK RX interrupts unavailable; using polling\n");
    rc = rte_eth_dev_configure(fabric.port, fabric.queues, fabric.queues,
                               &config);
  }
  if (rc) {
    StopPort();
    return DeviceError("ethdev configure", rc);
  }
  std::uint16_t rx_desc = 512, tx_desc = 512;
  rc = rte_eth_dev_adjust_nb_rx_tx_desc(fabric.port, &rx_desc, &tx_desc);
  if (rc) {
    StopPort();
    return DeviceError("ethdev descriptor counts", rc);
  }
  // Preserve the small-runtime pool size; larger queue sets must also fit
  // posted descriptors and worker-local caches, with a burst left to forward.
  constexpr unsigned kCacheSize = 256;
  const unsigned needed = fabric.queues * (unsigned(rx_desc) + tx_desc) +
                          workers * (kCacheSize + kBurst) + kBurst;
  const unsigned packets = std::max(65535U, std::bit_ceil(needed + 1) - 1);
  fabric.pool =
      rte_pktmbuf_pool_create("bycorf_packets", packets, kCacheSize, 0,
                              RTE_MBUF_DEFAULT_BUF_SIZE, SOCKET_ID_ANY);
  if (!fabric.pool) {
    StopPort();
    return DeviceError("packet pool", rte_errno);
  }
  for (unsigned i = 0; i < workers; ++i) {
    fabric.lanes[i].rx =
        rte_ring_create(("bycorf_rx_" + std::to_string(i)).c_str(), kQueueSize,
                        SOCKET_ID_ANY, RING_F_SC_DEQ);
    fabric.lanes[i].tx =
        rte_ring_create(("bycorf_tx_" + std::to_string(i)).c_str(), kQueueSize,
                        SOCKET_ID_ANY, RING_F_SC_DEQ);
    if (!fabric.lanes[i].rx || !fabric.lanes[i].tx) {
      StopPort();
      return DeviceError("forwarding rings", rte_errno);
    }
  }
  for (unsigned q = 0; q < fabric.queues; ++q) {
    rc = rte_eth_rx_queue_setup(fabric.port, q, rx_desc, SOCKET_ID_ANY, nullptr,
                                fabric.pool);
    if (!rc)
      rc = rte_eth_tx_queue_setup(fabric.port, q, tx_desc, SOCKET_ID_ANY,
                                  nullptr);
    if (rc) {
      StopPort();
      return DeviceError("ethdev queues", rc);
    }
  }
  rte_ether_addr mac{};
  rc = rte_eth_macaddr_get(fabric.port, &mac);
  std::uint16_t mtu = 0;
  if (!rc) rc = rte_eth_dev_get_mtu(fabric.port, &mtu);
  // A fixed-MTU virtual PMD (net_ring) need not implement mtu_set. Accept its
  // existing value when it already matches the stack's frame-size contract.
  if (!rc && mtu != 1500) rc = rte_eth_dev_set_mtu(fabric.port, 1500);
  if (!rc) rc = rte_eth_dev_start(fabric.port);
  if (rc) {
    StopPort();
    return DeviceError("ethdev start", rc);
  }
  std::memcpy(fabric.interface.mac, mac.addr_bytes, 6);
  fabric.interface.mtu = 1500;
  fabric.prepared = true;
  std::fprintf(
      stderr,
      "DPDK %s: %u workers, %u RX/TX pairs, %s, IPv4 %s, RX steering %s\n",
      info.driver_name, workers, fabric.queues,
      fabric.adaptive ? "adaptive" : "poll", address ? address : "198.18.0.2",
      fabric.rss_owner ? "rss" : "hash");
  return absl::OkStatus();
}

void DpdkBackend::StopRuntime() {
  std::lock_guard lock(fabric.mutex);
  if (!fabric.prepared || fabric.stopped) return;
  StopPort();
  fabric.stopped = true;
}
void DpdkBackend::AbortStartup(const absl::Status& reason) {
  if (reason.ok()) return;
  std::lock_guard lock(fabric.mutex);
  if (fabric.failure.ok()) fabric.failure = reason;
  fabric.attached.notify_all();
}
absl::Status DpdkBackend::Init(const IoBackendOptions& options, Worker* worker,
                               int wake_fd) {
  if (impl_->initialized) return absl::OkStatus();
  if (!fabric.prepared || fabric.stopped)
    return absl::FailedPreconditionError(
        "DPDK Runtime must prepare before worker initialization");
  auto kernel_options = options;
  kernel_options.recv_buffer_count_ = 0;
  auto status = kernel_.Init(kernel_options, worker, wake_fd);
  if (!status.ok()) {
    AbortStartup(status);
    return status;
  }
  impl_->worker = worker;
  impl_->id = worker->id();
  impl_->buffer_size = options.recv_buffer_size_;
  // Embed the owner in the handle so an accidental transfer cannot alias an
  // unrelated socket that happens to occupy the same slot on another worker.
  impl_->next_handle = kFirstHandle | (impl_->id << kHandleBits);
  current_ = impl_.get();
  if (rte_thread_register()) {
    status = DeviceError("register worker with EAL", rte_errno);
    AbortStartup(status);
    kernel_.Shutdown();
    current_ = nullptr;
    return status;
  }
  std::unique_lock lock(fabric.mutex);
  const unsigned id = impl_->id;
  int error = 0;
  if (id == 0 && fabric.failure.ok()) {
    error = bycorf_bsd_initialize(&kHost, fabric.workers);
    fabric.bsd_ready = error == 0;
    if (error)
      fabric.failure = DeviceError("FreeBSD initialization", BsdError(error));
    fabric.attached.notify_all();
  } else if (id != 0) {
    fabric.attached.wait(
        lock, [] { return fabric.bsd_ready || !fabric.failure.ok(); });
    if (fabric.failure.ok()) error = bycorf_bsd_attach_worker(id);
  }
  if (!error && fabric.failure.ok()) {
    auto config = fabric.interface;
    config.transmit = Impl::Transmit;
    config.context = impl_.get();
    error = bycorf_bsd_attach_interface(&config);
  }
  if (error)
    fabric.failure = DeviceError("FreeBSD worker setup", BsdError(error));
  if (!fabric.failure.ok()) {
    fabric.attached.notify_all();
    rte_thread_unregister();
    current_ = nullptr;
    kernel_.Shutdown();
    return fabric.failure;
  }
  fabric.lanes[id].ring_fd = kernel_.WakeHandle();
  impl_->notification.owner = impl_.get();
  impl_->rx_can_wait = id >= fabric.queues;
  if (fabric.adaptive && id < fabric.queues) {
    impl_->notification.fd = rte_eth_dev_rx_intr_ctl_q_get_fd(fabric.port, id);
    if (impl_->notification.fd >= 0) {
      impl_->rx_can_wait =
          fabric.tap || rte_eth_dev_rx_intr_disable(fabric.port, id) == 0;
    }
    if (!impl_->rx_can_wait)
      std::fprintf(stderr,
                   "DPDK worker %u RX queue lacks interrupt support; polling\n",
                   id);
  }
  impl_->initialized = true;
  fabric.ready.fetch_add(1, std::memory_order_release);
  return absl::OkStatus();
}

void DpdkBackend::Shutdown() {
  if (!impl_ || !impl_->initialized) return;
  if (current_ != impl_.get())
    std::abort();  // Runtime tears down on the owning thread.
  fabric.lanes[impl_->id].parked.store(false, std::memory_order_release);
  for (auto [handle, so] : impl_->sockets) bycorf_bsd_close(so);
  impl_->sockets.clear();
  impl_->SocketCompletions();
  impl_->FlushTx();
  std::fprintf(
      stderr,
      "DPDK worker %u: rx=%llu tx=%llu forward_rx=%llu forward_tx=%llu "
      "drops=%llu waits=%llu arms=%llu notifications=%llu\n",
      impl_->id, (unsigned long long)impl_->rx, (unsigned long long)impl_->tx,
      (unsigned long long)impl_->forwarded_rx,
      (unsigned long long)impl_->forwarded_tx, (unsigned long long)impl_->drops,
      (unsigned long long)impl_->waits, (unsigned long long)impl_->arms,
      (unsigned long long)impl_->notifications);
  kernel_.Shutdown();
  rte_thread_unregister();
  current_ = nullptr;
  impl_->initialized = false;
}
bool DpdkBackend::Poll() {
  if (!impl_->initialized || impl_->polling) return false;
  impl_->polling = true;
  const bool kernel = kernel_.Poll();
  const bool packets = impl_->Packets();
  const bool sockets = impl_->SocketCompletions();
  impl_->polling = false;
  return kernel || packets || sockets;
}
absl::Status DpdkBackend::Submit() {
  if (impl_->initialized) impl_->FlushTx();
  return kernel_.Submit();
}
bool DpdkBackend::Wait(int timeout_ms) {
  if (!fabric.adaptive || !impl_->rx_can_wait ||
      fabric.ready.load(std::memory_order_acquire) != fabric.workers)
    return true;
  const unsigned id = impl_->id;
  Lane& lane = fabric.lanes[id];
  lane.parked.store(true, std::memory_order_release);
  bool enabled = false;
  if (id < fabric.queues) {
    if (!fabric.tap) {
      enabled = rte_eth_dev_rx_intr_enable(fabric.port, id) == 0;
      if (!enabled) {
        lane.parked.store(false);
        impl_->rx_can_wait = false;
        return true;
      }
    }
    ++impl_->arms;
    if (!impl_->notification.armed) {
      const auto submitted = kernel_.SubmitPoll(impl_->notification.fd, POLLIN,
                                                &impl_->notification);
      if (!submitted.ok()) {
        lane.parked.store(false);
        if (enabled) rte_eth_dev_rx_intr_disable(fabric.port, id);
        return true;
      }
      impl_->notification.armed = true;
    }
  }
  // Arm first, then recheck packet/software queues and submit the poll. A
  // packet arriving in either gap leaves a readable fd or a MSG_RING CQE.
  bool ok = true;
  if (!Poll() && rte_ring_empty(lane.rx) && rte_ring_empty(lane.tx) &&
      impl_->completions.empty()) {
    const auto submitted = kernel_.Submit();
    if (!submitted.ok())
      ok = false;
    else {
      const auto now = NowNs(), deadline = bycorf_bsd_deadline_ns();
      if (deadline != UINT64_MAX) {
        const auto delay = deadline <= now
                               ? 0
                               : std::min<std::uint64_t>(
                                     (deadline - now) / 1'000'000, INT_MAX);
        timeout_ms = timeout_ms < 0
                         ? delay
                         : std::min(timeout_ms, static_cast<int>(delay));
      }
      // A sub-millisecond timer is already due for the millisecond wait API.
      // Let the next worker round service it without a zero-timeout syscall.
      if (timeout_ms != 0) {
        ++impl_->waits;
        ok = kernel_.Wait(timeout_ms);
      }
    }
  }
  lane.parked.store(false, std::memory_order_release);
  if (enabled) rte_eth_dev_rx_intr_disable(fabric.port, id);
  return ok;
}

absl::Status DpdkBackend::SubmitSend(const RegisteredFile& file,
                                     std::span<const std::byte> bytes,
                                     IoCompletion* tag) {
  if (!detail::IsDpdkSocket(file.fd_))
    return kernel_.SubmitSend(file, bytes, tag);
  if (!tag) return absl::InvalidArgumentError("send completion is null");
  impl_->sends.push_back({file.fd_, tag, bytes});
  return absl::OkStatus();
}
absl::Status DpdkBackend::SubmitSendMsg(const RegisteredFile& file,
                                        const msghdr* message,
                                        IoCompletion* tag) {
  if (!detail::IsDpdkSocket(file.fd_))
    return kernel_.SubmitSendMsg(file, message, tag);
  if (!tag || !message || message->msg_control || message->msg_name)
    return absl::InvalidArgumentError("unsupported DPDK sendmsg request");
  impl_->sends.push_back({file.fd_, tag, {}, message});
  return absl::OkStatus();
}
absl::Status DpdkBackend::SubmitAcceptMultishot(int fd, IoCompletion* tag) {
  if (!detail::IsDpdkSocket(fd)) return kernel_.SubmitAcceptMultishot(fd, tag);
  if (!tag || !impl_->Find(fd))
    return absl::InvalidArgumentError("invalid DPDK accept request");
  impl_->accepts.push_back({fd, tag, {}});
  return absl::OkStatus();
}
absl::Status DpdkBackend::SubmitCancel(IoCompletion* target) {
  for (auto* requests : {&impl_->accepts, &impl_->sends}) {
    for (auto at = requests->begin(); at != requests->end(); ++at) {
      if (at->tag != target) continue;
      requests->erase(at);
      impl_->QueueCompletion(target, -ECANCELED);
      return absl::OkStatus();
    }
  }
  return kernel_.SubmitCancel(target);
}
absl::Status DpdkBackend::StartRecvMultishot(Connection* c) {
  if (!c || !detail::IsDpdkSocket(c->file_.fd_))
    return kernel_.StartRecvMultishot(c);
  if (c->recv_armed_ || c->closed_ || c->closing_ || c->recv_paused_ ||
      !c->received_buffers_.empty())
    return absl::OkStatus();
  c->recv_mode_ = RecvMode::kOneShot;
  if (c->read_buffer_.empty()) c->read_buffer_.resize(impl_->buffer_size);
  c->recv_armed_ = true;
  ++c->inflight_ops_;
  impl_->reads.push_back(c);
  return absl::OkStatus();
}
absl::Status DpdkBackend::SubmitCancelRecv(Connection* c, IoCompletion* tag) {
  const auto at = std::find(impl_->reads.begin(), impl_->reads.end(), c);
  if (at == impl_->reads.end()) return kernel_.SubmitCancelRecv(c, tag);
  impl_->reads.erase(at);
  impl_->FinishRead(c);
  impl_->QueueCompletion(tag, 0);
  return absl::OkStatus();
}
absl::Status DpdkBackend::StartPeerDisconnectPoll(Connection* c) {
  if (!c || !detail::IsDpdkSocket(c->file_.fd_))
    return kernel_.StartPeerDisconnectPoll(c);
  if (c->peer_disconnect_poll_armed_ || c->closed_ || c->closing_)
    return absl::OkStatus();
  c->peer_disconnect_poll_armed_ = true;
  ++c->inflight_ops_;
  impl_->disconnects.push_back(c);
  return absl::OkStatus();
}
absl::Status DpdkBackend::CancelPeerDisconnectPoll(Connection* c) {
  if (std::find(impl_->disconnects.begin(), impl_->disconnects.end(), c) ==
      impl_->disconnects.end())
    return kernel_.CancelPeerDisconnectPoll(c);
  c->peer_disconnect_poll_cancel_requested_ = true;
  return absl::OkStatus();
}
std::span<const std::byte> DpdkBackend::ViewRecvBuffer(
    const Connection* c, std::uint16_t id, std::size_t offset,
    std::size_t length) const {
  return kernel_.ViewRecvBuffer(c, id, offset, length);
}
void DpdkBackend::ReleaseRecvBuffer(Connection* c, std::uint16_t id) {
  kernel_.ReleaseRecvBuffer(c, id);
}

int DpdkBackend::Listen(const sockaddr* address, socklen_t length,
                        int backlog) {
  if (!current_ || !address || address->sa_family != AF_INET ||
      length < sizeof(sockaddr_in)) {
    errno = EAFNOSUPPORT;
    return -1;
  }
  const auto* ipv4 = reinterpret_cast<const sockaddr_in*>(address);
  BsdSocket* listener = nullptr;
  const int error = BsdError(bycorf_bsd_listen(
      ipv4->sin_addr.s_addr, ntohs(ipv4->sin_port), backlog, &listener));
  if (error) {
    errno = error;
    return -1;
  }
  return current_->Own(listener);
}
int DpdkBackend::Close(int handle) noexcept {
  if (!current_) {
    errno = EBADF;
    return -1;
  }
  BsdSocket* so = current_->Find(handle);
  if (!so) {
    errno = EBADF;
    return -1;
  }
  current_->sockets.erase(handle);
  const int error = BsdError(bycorf_bsd_close(so));
  if (error) {
    errno = error;
    return -1;
  }
  return 0;
}
int DpdkBackend::PeerName(int handle, sockaddr* address,
                          socklen_t* length) noexcept {
  if (!current_ || !address || !length || *length < sizeof(sockaddr_in)) {
    errno = EINVAL;
    return -1;
  }
  BsdSocket* so = current_->Find(handle);
  if (!so) {
    errno = EBADF;
    return -1;
  }
  sockaddr_in value{};
  value.sin_family = AF_INET;
  std::uint16_t port;
  const int error =
      BsdError(bycorf_bsd_peer(so, &value.sin_addr.s_addr, &port));
  if (error) {
    errno = error;
    return -1;
  }
  value.sin_port = htons(port);
  std::memcpy(address, &value, sizeof(value));
  *length = sizeof(value);
  return 0;
}
}  // namespace bycorf
