// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include "dpdk_memory_budget.h"

#include <algorithm>
#include <bit>
#include <charconv>
#include <fstream>
#include <optional>
#include <string_view>
#include <vector>

#include "absl/strings/str_cat.h"

namespace bycorf::detail {
namespace {
constexpr std::uint64_t kKiB = 1024;
constexpr std::uint64_t kMiB = 1024 * kKiB;

std::optional<std::uint64_t> ReadCount(const std::filesystem::path& path) {
  std::ifstream input(path);
  std::string text;
  if (!(input >> text)) return std::nullopt;
  std::uint64_t value;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size())
    return std::nullopt;
  return value;
}

std::string Count(const std::filesystem::path& path) {
  const auto value = ReadCount(path);
  return value ? std::to_string(*value) : "unavailable";
}

std::vector<std::filesystem::path> Directories(
    const std::filesystem::path& root) {
  std::vector<std::filesystem::path> paths;
  std::error_code error;
  std::filesystem::directory_iterator it(root, error), end;
  while (!error && it != end) {
    if (it->is_directory(error)) paths.push_back(it->path());
    it.increment(error);
  }
  std::sort(paths.begin(), paths.end());
  return paths;
}
}  // namespace

DpdkMemoryBudget EstimateDpdkMemoryBudget(unsigned workers, unsigned queues,
                                          unsigned pool_chunks, bool netvsc,
                                          bool mlx5) {
  DpdkMemoryBudget budget;
  if (netvsc) {
    // Pinned netvsc: VMBus tlen is uint16_t in 8-byte units (<512 KiB),
    // hn_process_events grows to len + len/4 (<640 KiB). Charge both old and
    // new images for every channel, even though the old image is smaller.
    constexpr auto kEventImage = 640 * kKiB;
    budget.vmbus_bytes = std::uint64_t{queues} * 2 * kEventImage;
    budget.largest_allocation_bytes = kEventImage;
    // Current fixed RNDIS query/set payloads each fit within 4 KiB. A query
    // owns a request and completion simultaneously, allocated BEFORE the
    // pending-request check. Allow every worker plus the main and EAL control
    // threads to own a pair; TCP connection count does not enter this budget.
    budget.rndis_bytes = (std::uint64_t{workers} + 2) * 8 * kKiB;
  }
  if (mlx5 || netvsc) {
    // mlx5 merges adjacent mempool chunks, so their count is an upper bound
    // on MRs. The pinned PMD starts each queue's cache at 256 entries (one
    // sentinel), doubles it, and can fall back to shared lookup on failure.
    // Budget the old/new tables so successful growth cannot eat netvsc's
    // allowance. 32 bytes covers the 64-bit MR-cache entry including padding.
    const auto entries = std::bit_ceil(std::uint64_t{pool_chunks} + 1);
    if (entries > 256) {
      const auto image = entries * 32;
      budget.mlx5_cache_bytes = std::uint64_t{queues} * 2 * image;
      budget.largest_allocation_bytes =
          std::max(budget.largest_allocation_bytes, image);
    }
  }
  // Explicit operational headroom for heap headers/alignment and fragmentation;
  // not a proof against arbitrary allocation histories or new devices. Fixed
  // queues and mbufs are already charged in the sampled heap and are excluded.
  const auto growth = budget.total_bytes();
  budget.margin_bytes = std::max(2 * kMiB, (growth + 3) / 4);
  // The heap's largest free block must also fit alignment padding and the
  // allocator header; comparing it only with the requested payload is too weak.
  budget.largest_allocation_bytes =
      std::max(budget.largest_allocation_bytes, 4 * kKiB) + 4 * kKiB;
  return budget;
}

absl::Status ValidateDpdkMemoryBudget(const DpdkMemoryBudget& budget,
                                      std::span<const DpdkHeapStats> heaps) {
  bool populated = false;
  std::string failures;
  for (const auto& heap : heaps) {
    if (heap.total_bytes == 0) continue;
    populated = true;
    const auto required = budget.total_bytes();
    const auto deficit =
        required > heap.free_bytes ? required - heap.free_bytes : 0;
    const auto block_deficit =
        budget.largest_allocation_bytes > heap.largest_free_bytes
            ? budget.largest_allocation_bytes - heap.largest_free_bytes
            : 0;
    if (deficit == 0 && block_deficit == 0) continue;
    absl::StrAppend(
        &failures, " numa=", heap.numa, " available_bytes=", heap.free_bytes,
        " required_bytes=", required, " shortfall_bytes=", deficit,
        " largest_free_block_bytes=", heap.largest_free_bytes,
        " largest_required_block_bytes=", budget.largest_allocation_bytes,
        " block_shortfall_bytes=", block_deficit, ";");
  }
  if (!populated)
    return absl::FailedPreconditionError(
        "DPDK memory budget: no populated EAL heap statistics");
  if (failures.empty()) return absl::OkStatus();
  return absl::ResourceExhaustedError(absl::StrCat(
      "DPDK memory budget check failed:", failures,
      " increase BYCORF_DPDK_MEMORY_MB or the EAL per-NUMA memory allocation",
      " and provision enough hugepages on those nodes; host free pages are not",
      " included in this check"));
}

std::string DescribeHostHugepages(const std::filesystem::path& sys_root) {
  std::string result;
  auto append = [&](const std::filesystem::path& directory,
                    std::string_view numa, bool global) {
    for (const auto& pages : Directories(directory)) {
      const auto name = pages.filename().string();
      if (!name.starts_with("hugepages-") || !name.ends_with("kB")) continue;
      const auto size = std::string_view(name).substr(10, name.size() - 12);
      std::uint64_t size_kib;
      const auto [end, error] =
          std::from_chars(size.data(), size.data() + size.size(), size_kib);
      if (error != std::errc{} || end != size.data() + size.size()) continue;
      absl::StrAppend(&result, "DPDK host hugepages numa=", numa,
                      " page_size_kib=", size_kib,
                      " total_pages=", Count(pages / "nr_hugepages"),
                      " free_pages=", Count(pages / "free_hugepages"));
      // Reservations are global; Linux does not assign their NUMA node until
      // fault time. Do not subtract a global reservation from each node.
      if (global)
        absl::StrAppend(&result,
                        " reserved_pages=", Count(pages / "resv_hugepages"));
      result += '\n';
    }
  };
  append(sys_root / "kernel/mm/hugepages", "all", true);
  for (const auto& node : Directories(sys_root / "devices/system/node")) {
    const auto name = node.filename().string();
    if (name.starts_with("node")) append(node / "hugepages", name, false);
  }
  if (result.empty()) result = "DPDK host hugepages statistics unavailable\n";
  return result;
}

}  // namespace bycorf::detail
