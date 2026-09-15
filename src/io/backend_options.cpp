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

#include "celer/io/backend_options.h"

#include <atomic>
#include <mutex>

namespace celer {
namespace detail {
IoBackends io_backends;
}
namespace {
std::mutex options_mutex;
std::atomic<bool> frozen{false};
}  // namespace
absl::Status ConfigureIoBackends(IoBackends options) {
  std::lock_guard lock(options_mutex);
#ifndef CELER_WITH_DPDK
  if (options.dpdk_network)
    return absl::UnimplementedError("DPDK network support is not compiled in");
#endif
#ifndef CELER_WITH_SPDK_STORAGE
  if (options.spdk_storage)
    return absl::UnimplementedError("SPDK storage support is not compiled in");
#endif
  if (frozen.load(std::memory_order_relaxed)) {
    if (options == detail::io_backends) return absl::OkStatus();
    return absl::FailedPreconditionError(
        "I/O backends are fixed for the process lifetime");
  }
  detail::io_backends = options;
  return absl::OkStatus();
}
void FreezeIoBackends() {
  // Buffer allocation can occur on an I/O path. Once published, selection is
  // immutable: acquire its visibility without contending on a global mutex.
  if (frozen.load(std::memory_order_acquire)) return;
  std::lock_guard lock(options_mutex);
  frozen.store(true, std::memory_order_release);
}
}  // namespace celer
