// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include <rte_ethdev.h>
#include <rte_malloc.h>

#include <cstdlib>
#include <iostream>
#include <string_view>

#include "bycorf/io/backend_options.h"
#include "bycorf/io/dpdk_backend.h"

namespace {
std::string_view scenario;
bool checking = false;
unsigned heap_queries = 0;
}  // namespace

extern "C" int __real_rte_eth_dev_info_get(uint16_t, rte_eth_dev_info*);
extern "C" int __wrap_rte_eth_dev_info_get(uint16_t port,
                                           rte_eth_dev_info* info) {
  const int rc = __real_rte_eth_dev_info_get(port, info);
  if (rc == 0) info->driver_name = "net_netvsc";
  return rc;
}

extern "C" int __real_rte_malloc_get_socket_stats(int,
                                                  rte_malloc_socket_stats*);
extern "C" int __wrap_rte_malloc_get_socket_stats(
    int numa, rte_malloc_socket_stats* stats) {
  if (!checking) return __real_rte_malloc_get_socket_stats(numa, stats);
  ++heap_queries;
  if (scenario == "stats-error") return -1;
  *stats = {};
  stats->heap_totalsz_bytes = 512 * 1024 * 1024;
  stats->heap_allocsz_bytes = 256 * 1024 * 1024;
  stats->heap_freesz_bytes =
      scenario == "exhausted" || scenario == "spdk-exhausted"
          ? 1024
          : 256 * 1024 * 1024;
  stats->greatest_free_size =
      scenario == "fragmented" ? 4096 : stats->heap_freesz_bytes;
  return 0;
}

namespace {
int RunScenario(std::string_view name) {
  scenario = name;
  if (name == "disabled" || name == "spdk-only") {
    if (!bycorf::ConfigureIoBackends({.spdk_storage = name == "spdk-only"})
             .ok())
      return 2;
    if (!bycorf::DpdkBackend::CheckMemoryBudget().ok() || heap_queries != 0)
      return 1;
    return 0;
  }
  setenv("BYCORF_EAL_ARGS", "--no-pci --no-huge --vdev=net_ring0", 1);
  setenv("BYCORF_DPDK_MEMORY_MB", "512", 1);
  setenv("BYCORF_DPDK_MODE", "poll", 1);
  setenv("BYCORF_DPDK_QUEUES", "1", 1);
  setenv("BYCORF_DPDK_RX_STEERING", "hash", 1);
  setenv("BYCORF_DPDK_IP", "198.18.0.2", 1);
  setenv("BYCORF_DPDK_NETMASK", "255.255.255.0", 1);
  unsetenv("BYCORF_DPDK_GATEWAY");
  if (!bycorf::ConfigureIoBackends(
           {.dpdk_network = true, .spdk_storage = name == "spdk-exhausted"})
           .ok())
    return 2;
  if (bycorf::DpdkBackend::CheckMemoryBudget().ok()) return 3;
  const auto prepared = bycorf::DpdkBackend::PrepareRuntime(2);
  if (!prepared.ok()) {
    std::cerr << prepared << '\n';
    return 4;
  }
  // Consume/fail the snapshot only after real fixed network allocations.
  // This models the final check after another EAL user (e.g. SPDK) starts.
  checking = name != "real-heap";
  const auto status = bycorf::DpdkBackend::CheckMemoryBudget();
  const bool queried = !checking || heap_queries != 0;
  checking = false;
  bycorf::DpdkBackend::StopRuntime();
  if (!queried) return 8;
  if (name == "pass" || name == "real-heap") return status.ok() ? 0 : 5;
  if (name == "stats-error") return absl::IsUnavailable(status) ? 0 : 6;
  if (!absl::IsResourceExhausted(status) ||
      status.message().find("BYCORF_DPDK_MEMORY_MB") == std::string_view::npos)
    return 7;
  return 0;
}

class DpdkMemoryStartupDeathTest : public testing::TestWithParam<const char*> {
};
TEST_P(DpdkMemoryStartupDeathTest, ChecksAfterFixedAllocations) {
  const auto selected = GetParam();
  const std::string_view name = selected;
  const char* log =
      name == "disabled" || name == "spdk-only" ? ""
      : name == "stats-error" ? "DPDK memory statistics failed numa="
      : name == "pass" || name == "real-heap" ? "DPDK memory budget passed"
                                              : "shortfall_bytes=";
  ASSERT_EXIT(std::exit(RunScenario(selected)), testing::ExitedWithCode(0),
              log);
}
INSTANTIATE_TEST_SUITE_P(Scenarios, DpdkMemoryStartupDeathTest,
                         testing::Values("disabled", "spdk-only", "real-heap",
                                         "pass", "exhausted", "spdk-exhausted",
                                         "fragmented", "stats-error"));
}  // namespace
