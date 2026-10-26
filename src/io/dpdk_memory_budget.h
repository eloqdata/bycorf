// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef BYCORF_IO_DPDK_MEMORY_BUDGET_H_
#define BYCORF_IO_DPDK_MEMORY_BUDGET_H_

#include <cstdint>
#include <filesystem>
#include <span>
#include <string>

#include "absl/status/status.h"

namespace bycorf::detail {

// Remaining allocation allowance, not the memory already occupied by pools,
// queues, or SPDK. This admission policy covers the fixed startup topology;
// it neither reserves these bytes nor permits runtime device/pool expansion.
struct DpdkMemoryBudget {
  std::uint64_t vmbus_bytes = 0;
  std::uint64_t rndis_bytes = 0;
  std::uint64_t mlx5_cache_bytes = 0;
  std::uint64_t margin_bytes = 0;
  std::uint64_t largest_allocation_bytes = 0;
  std::uint64_t total_bytes() const {
    return vmbus_bytes + rndis_bytes + mlx5_cache_bytes + margin_bytes;
  }
};

struct DpdkHeapStats {
  int numa = 0;
  std::uint64_t total_bytes = 0;
  std::uint64_t allocated_bytes = 0;
  std::uint64_t free_bytes = 0;
  std::uint64_t largest_free_bytes = 0;
};

// pool_chunks bounds the registered ranges of Bycorf's ordinary mbuf pool.
// netvsc includes its optional accelerated mlx5 child, which shares that pool.
DpdkMemoryBudget EstimateDpdkMemoryBudget(unsigned workers, unsigned queues,
                                          unsigned pool_chunks, bool netvsc,
                                          bool mlx5);

// SOCKET_ID_ANY allocations can originate on different workers. Require the
// full allowance on each populated heap rather than adding remote fragments
// together. Empty heaps are not promised as capacity; EAL can use an existing
// heap on another node. An absent snapshot fails closed.
absl::Status ValidateDpdkMemoryBudget(const DpdkMemoryBudget& budget,
                                      std::span<const DpdkHeapStats> heaps);

// Best-effort diagnostics only: unallocated host pages are never credited to
// the EAL allowance. The root parameter also permits tests without host sysfs.
std::string DescribeHostHugepages(
    const std::filesystem::path& sys_root = "/sys");

}  // namespace bycorf::detail

#endif
