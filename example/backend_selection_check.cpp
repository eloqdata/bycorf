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

#include <cstddef>
#include <cstring>
#include <iostream>

#include "bycorf/io/backend_options.h"
#include "bycorf/io/dpdk_environment.h"
#include "bycorf/io/spdk_storage.h"

// No device access: even an all-capability binary must allocate ordinary memory
// with the default selection, and may never change allocator identity later.
int main() {
  if (bycorf::DpdkNetworkEnabled() || bycorf::SpdkStorageEnabled()) return 1;
#if BYCORF_KERNEL_BYPASS
  // Compiling both capabilities must still allow independent startup choices.
  // Selection alone must not initialize EAL or access a device.
  const bycorf::IoBackends supported[] = {
      {.dpdk_network = true},
      {.spdk_storage = true},
      {.dpdk_network = true, .spdk_storage = true},
  };
  for (const auto options : supported) {
    if (!bycorf::ConfigureIoBackends(options).ok() ||
        bycorf::DpdkNetworkEnabled() != options.dpdk_network ||
        bycorf::SpdkStorageEnabled() != options.spdk_storage)
      return 9;
  }
#else
  if (bycorf::ConfigureIoBackends({.dpdk_network = true}).ok()) return 2;
  if (bycorf::ConfigureIoBackends({.spdk_storage = true}).ok()) return 3;
#endif
  if (!bycorf::ConfigureIoBackends({}).ok()) return 4;
  void* memory = bycorf::AllocateStorageBuffer(4096, 4096);
  if (!memory) return 5;
  std::memset(memory, 0xa5, 4096);
  if (bycorf::ConfigureIoBackends({.dpdk_network = true}).ok()) return 6;
  if (bycorf::ConfigureIoBackends({.spdk_storage = true}).ok()) return 7;
  if (!bycorf::ConfigureIoBackends({}).ok()) return 8;
  bycorf::FreeStorageBuffer(memory, 4096);
  std::cout
      << "default kernel/uring; inactive DMA; immutable selection: PASS\n";
}
