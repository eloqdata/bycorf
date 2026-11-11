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

#include <gtest/gtest.h>
#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_ring.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>

#include "bycorf/net/server.h"
#include "bycorf/runtime/foreign_executor.h"
#include "worker_wait_test_peer.h"

namespace {
std::string_view scenario;
int notification_fd = -1;
std::atomic<unsigned> enables{0};
std::atomic<unsigned> disables{0};
std::atomic<unsigned> configured{0};
std::atomic<bool> injected{false};
std::atomic<bool> race_done{false};
std::atomic<unsigned> deadlines{0};

void CheckWaitRace(bycorf::Worker& worker) {
  using Peer = bycorf::WorkerWaitTestPeer;
  using Phase = Peer::Phase;
  if (scenario == "race-facade") {
    bycorf::CheckNetworkRingIsolation(worker);
    injected = true;
    race_done.store(true, std::memory_order_release);
    return;
  }
  bool reached = false, waiting = false;
  unsigned delivered = 0;
  const auto boundary =
      scenario == "race-notification" ? Phase::kWaiting
      : scenario == "race-stop"       ? Phase::kPrepared
      : (scenario == "race-submit-queue" || scenario == "race-deadline")
          ? Phase::kSubmitted
          : Phase::kRechecked;
  const char* queue_name =
      scenario == "race-tx-queue" ? "bycorf_tx_0" : "bycorf_rx_0";
  // Other workers may still be attaching their VNET. Retry only until this
  // worker can actually prepare notifications; no timeout rescues its wait.
  const auto limit = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!reached && std::chrono::steady_clock::now() < limit) {
    const auto before = enables.load();
    EXPECT_TRUE(Peer::Wait(worker, [&](Phase phase) {
      waiting |= phase == Phase::kWaiting;
      if (phase != boundary) return;
      reached = true;
      if (scenario == "race-notification") {
        const std::uint64_t one = 1;
        EXPECT_EQ(write(notification_fd, &one, sizeof(one)), sizeof(one));
      } else if (scenario == "race-stop") {
        worker.RequestStop();
      } else if (scenario == "race-mailbox") {
        bycorf::detail::ForeignExecutorState ingress{
            .cross_core_ = bycorf::ThisWorker().cross_core_, .worker_id_ = 0};
        std::thread producer([&] {
          EXPECT_TRUE(bycorf::detail::PostForeignNotification(
              &ingress, {.context_ = &delivered,
                         .value_ = 0,
                         .run_fn_ = +[](void* p, std::uint64_t) noexcept {
                           ++*static_cast<unsigned*>(p);
                         }}));
        });
        producer.join();
      } else if (scenario != "race-deadline") {
        // Zero-length input is deliberately dropped by Input, but still
        // exercises the real software RX queue and mbuf ownership.
        auto* packet = rte_pktmbuf_alloc(rte_mempool_lookup("bycorf_packets"));
        EXPECT_NE(packet, nullptr);
        if (packet) {
          auto address = reinterpret_cast<std::uintptr_t>(packet);
          const int rc = rte_ring_enqueue_elem(rte_ring_lookup(queue_name),
                                               &address, sizeof(address));
          EXPECT_EQ(rc, 0);
          if (rc) rte_pktmbuf_free(packet);
        }
      }
    }));
    if (reached) {
      EXPECT_GT(enables.load(), before);
      EXPECT_EQ(enables.load(), disables.load());
      EXPECT_FALSE(Peer::CanWait(worker));
    }
  }
  EXPECT_TRUE(reached);
  EXPECT_EQ(waiting, scenario == "race-notification");
  if (scenario == "race-mailbox") {
    EXPECT_TRUE(Peer::Drain(worker));
    EXPECT_EQ(delivered, 1U);
  }
  if (scenario == "race-notification") {
    std::uint64_t counter;
    EXPECT_EQ(read(notification_fd, &counter, sizeof(counter)), -1);
    EXPECT_EQ(errno, EAGAIN);  // The actual notification CQE was dispatched.
  }
  if (scenario == "race-deadline") EXPECT_GT(deadlines.load(), 0U);
  if (scenario == "race-queue" || scenario == "race-submit-queue" ||
      scenario == "race-tx-queue") {
    EXPECT_FALSE(rte_ring_empty(rte_ring_lookup(queue_name)));
    EXPECT_TRUE(Peer::Poll(worker));
    EXPECT_TRUE(rte_ring_empty(rte_ring_lookup(queue_name)));
  }
  injected = reached;
  race_done.store(true, std::memory_order_release);
}

class IdleService final : public bycorf::Service {
 public:
  void Prepare(unsigned) override {}
  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (scenario.starts_with("race-") && worker.id() == 0)
      CheckWaitRace(worker);
    co_return absl::OkStatus();
  }
  void Stop() noexcept override {}
};
}  // namespace

// Keep fault injection outside production code. The ring PMD owns no physical
// NIC, and eventfd emulates the notification descriptor for runtime failures.
extern "C" int __real_rte_eth_dev_info_get(uint16_t, rte_eth_dev_info*);
extern "C" int __wrap_rte_eth_dev_info_get(uint16_t port,
                                           rte_eth_dev_info* info) {
  const int rc = __real_rte_eth_dev_info_get(port, info);
  if (!rc && scenario == "mlx5-empty") info->driver_name = "mlx5_pci";
  return rc;
}
extern "C" int __real_rte_eth_dev_configure(uint16_t, uint16_t, uint16_t,
                                            const rte_eth_conf*);
extern "C" int __wrap_rte_eth_dev_configure(uint16_t port, uint16_t rx,
                                            uint16_t tx,
                                            const rte_eth_conf* conf) {
  ++configured;
  if (scenario == "configure" && conf->intr_conf.rxq) {
    injected = true;
    return -ENOTSUP;
  }
  rte_eth_conf ring_conf = *conf;
  ring_conf.intr_conf.rxq = 0;
  return __real_rte_eth_dev_configure(port, rx, tx, &ring_conf);
}
extern "C" int __wrap_rte_eth_dev_rx_intr_ctl_q_get_fd(uint16_t, uint16_t) {
  if (scenario == "descriptor") {
    injected = true;
    return -1;
  }
  return notification_fd;
}
extern "C" int __wrap_rte_eth_dev_rx_intr_enable(uint16_t, uint16_t) {
  const unsigned count = ++enables;
  if (scenario == "enable" || (scenario == "runtime-enable" && count > 1)) {
    injected = true;
    return -ENOTSUP;
  }
  return 0;
}
extern "C" int __wrap_rte_eth_dev_rx_intr_disable(uint16_t, uint16_t) {
  const unsigned count = ++disables;
  if (scenario == "mlx5-empty") {
    injected = true;
    return -EAGAIN;
  }
  if (scenario == "disable" || (scenario == "runtime-disable" && count > 1)) {
    injected = true;
    return -EIO;
  }
  return 0;
}

extern "C" std::uint64_t __real_bycorf_bsd_deadline_ns();
extern "C" std::uint64_t __wrap_bycorf_bsd_deadline_ns() {
  ++deadlines;
  return scenario == "race-deadline" ? 0 : __real_bycorf_bsd_deadline_ns();
}

namespace {
int RunScenario(std::string_view selected) {
  scenario = selected;
  const bool poll = scenario == "poll";
  const bool race = scenario.starts_with("race-");
  const bool mlx5 = scenario == "mlx5-empty";
  const bool runtime = scenario.starts_with("runtime-");
  // Ignore inherited hardware configuration; each process gets its own EAL.
  setenv("BYCORF_EAL_ARGS", "--no-pci --no-huge --vdev=net_ring0", 1);
  setenv("BYCORF_DPDK_MODE", poll ? "poll" : "adaptive", 1);
  setenv("BYCORF_DPDK_QUEUES", "1", 1);
  setenv("BYCORF_DPDK_RX_STEERING", "hash", 1);
  setenv("BYCORF_DPDK_IP", "198.18.0.2", 1);
  setenv("BYCORF_DPDK_NETMASK", "255.255.255.0", 1);
  unsetenv("BYCORF_DPDK_GATEWAY");
  setenv("BYCORF_DPDK_MEMORY_MB", "512", 1);
  notification_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (notification_fd < 0 ||
      !bycorf::ConfigureIoBackends({.dpdk_network = true}).ok())
    return 2;
  IdleService service;
  bycorf::Server server;
  server.AddService(&service);
  const auto status = server.Start({.thread_count_ = 2, .pin_workers_ = false});
  if (!poll && !runtime && !mlx5 && !race) {
    if (status.ok() || !injected || configured != 1 ||
        status.message().find("BYCORF_DPDK_MODE=poll") ==
            std::string_view::npos) {
      std::cerr << "expected actionable startup rejection: " << status << '\n';
      return 1;
    }
    std::cout << status << '\n';
  } else {
    if (!status.ok()) {
      std::cerr << status << '\n';
      return 1;
    }
    if (mlx5) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (race) {
      const auto limit =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (!race_done.load(std::memory_order_acquire) &&
             std::chrono::steady_clock::now() < limit)
        std::this_thread::yield();
    }
    if (poll || mlx5 || race) server.RequestStop();
    // A CTest timeout catches peers stranded at the all-worker barriers.
    server.WaitUntilStopped();
    if (poll && (server.exit_code() != 0 || enables != 0)) return 1;
    if (mlx5 && (server.exit_code() != 0 || enables < 2 || !injected)) return 1;
    if (race &&
        (server.exit_code() != 0 || !injected || testing::Test::HasFailure()))
      return 1;
    if (runtime && (server.exit_code() == 0 || !injected)) return 1;
  }
  close(notification_fd);
  std::cout << "PASS " << scenario << '\n';
  return 0;
}

class DpdkAdaptiveDeathTest : public testing::TestWithParam<const char*> {};

TEST_P(DpdkAdaptiveDeathTest, HandlesInterruptFailure) {
  // EAL, backend selection, and FreeBSD bootstrap have process lifetime.
  // GoogleTest discovery must only list cases, never initialize those states.
  const auto selected = GetParam();
  ASSERT_EXIT(std::exit(RunScenario(selected)), testing::ExitedWithCode(0), "");
}

INSTANTIATE_TEST_SUITE_P(Scenarios, DpdkAdaptiveDeathTest,
                         testing::Values("configure", "descriptor", "enable",
                                         "disable", "poll", "runtime-enable",
                                         "runtime-disable", "mlx5-empty",
                                         "race-queue", "race-tx-queue",
                                         "race-submit-queue", "race-mailbox",
                                         "race-notification", "race-stop",
                                         "race-deadline", "race-facade"),
                         [](const testing::TestParamInfo<const char*>& info) {
                           std::string name = info.param;
                           for (auto& character : name)
                             if (character == '-') character = '_';
                           return name;
                         });
}  // namespace
