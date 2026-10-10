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

#include <rte_ethdev.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <thread>

#include "bycorf/net/server.h"

namespace {
std::string_view scenario;
int notification_fd = -1;
std::atomic<unsigned> enables{0};
std::atomic<unsigned> disables{0};
std::atomic<unsigned> configured{0};
std::atomic<bool> injected{false};

class IdleService final : public bycorf::Service {
 public:
  void Prepare(unsigned) override {}
  bycorf::Task<absl::Status> Run(bycorf::Worker&,
                                 bycorf::ServiceContext) override {
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

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  scenario = argv[1];
  const bool poll = scenario == "poll";
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
  if (!poll && !runtime && !mlx5) {
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
    if (poll || mlx5) server.RequestStop();
    // A CTest timeout catches peers stranded at the all-worker barriers.
    server.WaitUntilStopped();
    if (poll && (server.exit_code() != 0 || enables != 0)) return 1;
    if (mlx5 && (server.exit_code() != 0 || enables < 2 || !injected)) return 1;
    if (runtime && (server.exit_code() == 0 || !injected)) return 1;
  }
  close(notification_fd);
  std::cout << "PASS " << scenario << '\n';
}
