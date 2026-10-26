// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include "dpdk_memory_budget.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <limits>

namespace bycorf::detail {
namespace {
constexpr std::uint64_t kMiB = 1024 * 1024;

TEST(DpdkMemoryBudgetTest, FixedPacketPoolDoesNotNeedAnotherPoolAllowance) {
  const auto small = EstimateDpdkMemoryBudget(1, 1, 1, false, false);
  const auto large = EstimateDpdkMemoryBudget(128, 128, 4096, false, false);
  EXPECT_EQ(small.total_bytes(), 2 * kMiB);
  EXPECT_EQ(large.total_bytes(), small.total_bytes());
}

TEST(DpdkMemoryBudgetTest, NetvscIncludesReallocationAndConcurrentControls) {
  const auto budget = EstimateDpdkMemoryBudget(8, 4, 1, true, false);
  EXPECT_EQ(budget.vmbus_bytes, 4 * 2 * 640 * 1024);
  EXPECT_EQ(budget.rndis_bytes, 10 * 8 * 1024);
  EXPECT_EQ(budget.mlx5_cache_bytes, 0);
  EXPECT_EQ(budget.largest_allocation_bytes, 644 * 1024);
  const auto more_workers = EstimateDpdkMemoryBudget(16, 4, 1, true, false);
  EXPECT_EQ(more_workers.vmbus_bytes, budget.vmbus_bytes);
  EXPECT_GT(more_workers.rndis_bytes, budget.rndis_bytes);
}

TEST(DpdkMemoryBudgetTest, Mlx5CountsSentinelDoublingAndBothTables) {
  EXPECT_EQ(EstimateDpdkMemoryBudget(4, 4, 255, false, true).mlx5_cache_bytes,
            0);
  const auto budget = EstimateDpdkMemoryBudget(4, 4, 256, false, true);
  EXPECT_EQ(budget.mlx5_cache_bytes, 4 * 2 * 512 * 32);
  EXPECT_EQ(budget.largest_allocation_bytes, 512 * 32 + 4096);
  EXPECT_EQ(EstimateDpdkMemoryBudget(4, 4, 256, true, false).mlx5_cache_bytes,
            budget.mlx5_cache_bytes);
}

TEST(DpdkMemoryBudgetTest, LargeTopologyUsesWideArithmetic) {
  const auto budget = EstimateDpdkMemoryBudget(
      1023, 1023, std::numeric_limits<unsigned>::max(), true, true);
  EXPECT_GT(budget.mlx5_cache_bytes, std::uint64_t{1} << 32);
  EXPECT_EQ(budget.margin_bytes, (budget.vmbus_bytes + budget.rndis_bytes +
                                  budget.mlx5_cache_bytes + 3) /
                                     4);
}

TEST(DpdkMemoryBudgetTest, UsesRemainingHeapAfterOtherStartupAllocations) {
  const auto budget = EstimateDpdkMemoryBudget(8, 4, 1, true, false);
  std::array heaps{
      DpdkHeapStats{.numa = 3,
                    .total_bytes = 1024 * kMiB,
                    .allocated_bytes = 900 * kMiB,
                    .free_bytes = budget.total_bytes(),
                    .largest_free_bytes = budget.largest_allocation_bytes}};
  EXPECT_TRUE(ValidateDpdkMemoryBudget(budget, heaps).ok());
  --heaps[0].free_bytes;
  const auto failure = ValidateDpdkMemoryBudget(budget, heaps);
  EXPECT_TRUE(absl::IsResourceExhausted(failure));
  EXPECT_NE(failure.message().find("numa=3"), std::string_view::npos);
  EXPECT_NE(failure.message().find("shortfall_bytes=1 "),
            std::string_view::npos);
}

TEST(DpdkMemoryBudgetTest, RemoteCapacityCannotHideLocalShortfall) {
  const auto budget = EstimateDpdkMemoryBudget(8, 4, 1, true, false);
  const std::array heaps{DpdkHeapStats{.numa = 0,
                                       .total_bytes = 1024 * kMiB,
                                       .free_bytes = 512 * kMiB,
                                       .largest_free_bytes = 256 * kMiB},
                         DpdkHeapStats{.numa = 1,
                                       .total_bytes = 1024 * kMiB,
                                       .free_bytes = 1024,
                                       .largest_free_bytes = 1024}};
  const auto failure = ValidateDpdkMemoryBudget(budget, heaps);
  EXPECT_TRUE(absl::IsResourceExhausted(failure));
  EXPECT_NE(failure.message().find("numa=1"), std::string_view::npos);
}

TEST(DpdkMemoryBudgetTest, EnoughTotalButFragmentedHeapFails) {
  const auto budget = EstimateDpdkMemoryBudget(8, 4, 1, true, false);
  const std::array heaps{
      DpdkHeapStats{.total_bytes = 1024 * kMiB,
                    .free_bytes = 128 * kMiB,
                    .largest_free_bytes = budget.largest_allocation_bytes - 1}};
  const auto failure = ValidateDpdkMemoryBudget(budget, heaps);
  EXPECT_TRUE(absl::IsResourceExhausted(failure));
  EXPECT_NE(failure.message().find("block_shortfall_bytes=1"),
            std::string_view::npos);
}

TEST(DpdkMemoryBudgetTest, MissingOrEmptyStatisticsCannotPass) {
  const auto budget = EstimateDpdkMemoryBudget(1, 1, 1, false, false);
  EXPECT_FALSE(ValidateDpdkMemoryBudget(budget, {}).ok());
  const std::array empty{DpdkHeapStats{}};
  EXPECT_FALSE(ValidateDpdkMemoryBudget(budget, empty).ok());
  const std::array populated{DpdkHeapStats{},
                             DpdkHeapStats{.numa = 1,
                                           .total_bytes = 128 * kMiB,
                                           .free_bytes = 64 * kMiB,
                                           .largest_free_bytes = 32 * kMiB}};
  EXPECT_TRUE(ValidateDpdkMemoryBudget(budget, populated).ok());
}

class HostHugepageTest : public testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/bycorf-hugepages-XXXXXX";
    const char* created = mkdtemp(pattern);
    ASSERT_NE(created, nullptr);
    root = created;
  }
  void TearDown() override {
    if (!root.empty()) std::filesystem::remove_all(root);
  }
  void Write(std::string_view relative, std::string_view value) {
    const auto path = root / relative;
    std::filesystem::create_directories(path.parent_path());
    std::ofstream(path) << value;
  }
  std::filesystem::path root;
};

TEST_F(HostHugepageTest, ReportsGlobalReservationsAndPerNodePageSizes) {
  Write("kernel/mm/hugepages/hugepages-2048kB/nr_hugepages", "4096\n");
  Write("kernel/mm/hugepages/hugepages-2048kB/free_hugepages", "256\n");
  Write("kernel/mm/hugepages/hugepages-2048kB/resv_hugepages", "16\n");
  Write("devices/system/node/node2/hugepages/hugepages-1048576kB/nr_hugepages",
        "8\n");
  Write(
      "devices/system/node/node2/hugepages/hugepages-1048576kB/free_hugepages",
      "3\n");
  const auto text = DescribeHostHugepages(root);
  EXPECT_NE(text.find("numa=all page_size_kib=2048 total_pages=4096 "
                      "free_pages=256 reserved_pages=16"),
            std::string::npos);
  EXPECT_NE(
      text.find(
          "numa=node2 page_size_kib=1048576 total_pages=8 free_pages=3\n"),
      std::string::npos);
}

TEST_F(HostHugepageTest, MissingAndMalformedValuesAreNotReportedAsZero) {
  EXPECT_NE(DescribeHostHugepages(root).find("unavailable"), std::string::npos);
  Write("kernel/mm/hugepages/hugepages-2048kB/free_hugepages", "-1\n");
  const auto text = DescribeHostHugepages(root);
  EXPECT_NE(text.find("total_pages=unavailable free_pages=unavailable "
                      "reserved_pages=unavailable"),
            std::string::npos);
}
}  // namespace
}  // namespace bycorf::detail
