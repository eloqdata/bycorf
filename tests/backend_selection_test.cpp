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

#include <cstdlib>
#include <cstring>

#include "bycorf/io/backend_options.h"
#include "bycorf/io/dpdk_environment.h"
#include "bycorf/io/spdk_storage.h"

namespace {
// Backend selection is process-global and becomes immutable on allocation.
// Subprocesses keep these cases independent even under --gtest_shuffle/repeat;
// no production reset hook or device initialization is needed.
TEST(BackendSelectionDeathTest, DefaultsToKernelAndIoUring) {
  ASSERT_EXIT(([] {
                EXPECT_FALSE(bycorf::DpdkNetworkEnabled());
                EXPECT_FALSE(bycorf::SpdkStorageEnabled());
                std::exit(testing::Test::HasFailure() ? 1 : 0);
              }()),
              testing::ExitedWithCode(0), "");
}

TEST(BackendSelectionDeathTest, CompiledCapabilitiesSelectIndependently) {
  ASSERT_EXIT(([] {
                const bycorf::IoBackends supported[] = {
                    {.dpdk_network = true},
                    {.spdk_storage = true},
                    {.dpdk_network = true, .spdk_storage = true},
                };
                for (const auto options : supported) {
                  const auto status = bycorf::ConfigureIoBackends(options);
#if BYCORF_KERNEL_BYPASS
                  EXPECT_TRUE(status.ok()) << status;
                  EXPECT_EQ(bycorf::DpdkNetworkEnabled(), options.dpdk_network);
                  EXPECT_EQ(bycorf::SpdkStorageEnabled(), options.spdk_storage);
#else
                  EXPECT_TRUE(absl::IsUnimplemented(status)) << status;
                  EXPECT_FALSE(bycorf::DpdkNetworkEnabled());
                  EXPECT_FALSE(bycorf::SpdkStorageEnabled());
#endif
                }
                EXPECT_TRUE(bycorf::ConfigureIoBackends({}).ok());
                std::exit(testing::Test::HasFailure() ? 1 : 0);
              }()),
              testing::ExitedWithCode(0), "");
}

TEST(BackendSelectionDeathTest, AllocationFreezesBackendAndAllocator) {
  ASSERT_EXIT(
      ([] {
        EXPECT_TRUE(bycorf::ConfigureIoBackends({}).ok());
        void* memory = bycorf::AllocateStorageBuffer(4096, 4096);
        EXPECT_NE(memory, nullptr);
        if (memory) std::memset(memory, 0xa5, 4096);
        EXPECT_FALSE(bycorf::ConfigureIoBackends({.dpdk_network = true}).ok());
        EXPECT_FALSE(bycorf::ConfigureIoBackends({.spdk_storage = true}).ok());
        EXPECT_TRUE(bycorf::ConfigureIoBackends({}).ok());
        bycorf::FreeStorageBuffer(memory, 4096);
        std::exit(testing::Test::HasFailure() ? 1 : 0);
      }()),
      testing::ExitedWithCode(0), "");
}
}  // namespace
