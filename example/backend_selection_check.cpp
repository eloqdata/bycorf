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

#include "celer/io/backend_options.h"
#include "celer/io/dpdk_environment.h"
#include "celer/io/spdk_storage.h"

// No device access: even an all-capability binary must allocate ordinary memory
// with the default selection, and may never change allocator identity later.
int main() {
  if (celer::DpdkNetworkEnabled() || celer::SpdkStorageEnabled()) return 1;
#ifndef CELER_WITH_DPDK
  if (celer::ConfigureIoBackends({.dpdk_network = true}).ok()) return 2;
#endif
#ifndef CELER_WITH_SPDK_STORAGE
  if (celer::ConfigureIoBackends({.spdk_storage = true}).ok()) return 3;
#endif
  if (!celer::ConfigureIoBackends({}).ok()) return 4;
  void* memory = celer::AllocateStorageBuffer(4096, 4096);
  if (!memory) return 5;
  std::memset(memory, 0xa5, 4096);
  if (celer::ConfigureIoBackends({.dpdk_network = true}).ok()) return 6;
  if (celer::ConfigureIoBackends({.spdk_storage = true}).ok()) return 7;
  if (!celer::ConfigureIoBackends({}).ok()) return 8;
  celer::FreeStorageBuffer(memory, 4096);
  std::cout
      << "default kernel/uring; inactive DMA; immutable selection: PASS\n";
}
