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

#include "bycorf/io/dpdk_environment.h"

#include <pthread.h>
#include <sched.h>
#include <spdk/env.h>

#include <climits>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <string>

#include "bycorf/io/backend_options.h"

namespace bycorf {
absl::Status EnsureDpdkEnvironment() {
  FreezeIoBackends();
  if (!DpdkNetworkEnabled() && !SpdkStorageEnabled())
    return absl::FailedPreconditionError(
        "DPDK network and SPDK storage are disabled");
  static std::once_flag once;
  static absl::Status result;
  std::call_once(once, [] {
    cpu_set_t original;
    if (pthread_getaffinity_np(pthread_self(), sizeof(original), &original)) {
      result =
          absl::InternalError("cannot save affinity before EAL initialization");
      return;
    }
    unsigned cpu = 0;
    while (cpu < CPU_SETSIZE && !CPU_ISSET(cpu, &original)) ++cpu;
    // EAL lcore 0 is only its control context. Existing Bycorf worker threads
    // register themselves later; EAL never launches an RX polling worker.
    const std::string mapping = "0@" + std::to_string(cpu);
    spdk_env_opts options{};
    options.opts_size = sizeof(options);
    spdk_env_opts_init(&options);
    options.name = "bycorf";
    options.core_mask = nullptr;
    options.lcore_map = mapping.c_str();
    options.unlink_hugepage = true;
    std::string extra;
    if (const char* args = std::getenv("BYCORF_EAL_ARGS")) extra = args;
#if BYCORF_KERNEL_BYPASS
    // Virtual-device testing is the explicit default. A physical-device run
    // supplies BYCORF_EAL_ARGS with its allowlist and hugepage configuration.
    if (DpdkNetworkEnabled() && extra.empty()) {
      options.no_huge = true;
      options.no_pci = true;
      options.mem_size = 512;
      extra = "--vdev=net_tap0,iface=bycorfdp0,mac=02:00:00:00:00:02";
    }
#endif
    // SPDK synthesizes some EAL flags from its options, including --no-huge
    // discovered in env_context. DPDK's argument parser rejects duplicates.
    // Normalize these flags into options and forward every other token once.
    std::istringstream arguments(extra);
    std::string token, forwarded;
    while (arguments >> token) {
      if (token == "--no-huge") {
        options.no_huge = true;
      } else if (token == "--no-pci") {
        options.no_pci = true;
      } else if (token == "-m" || token.starts_with("-m")) {
        std::string memory = token.substr(2);
        if (memory.empty()) arguments >> memory;
        char* end = nullptr;
        const unsigned long value = std::strtoul(memory.c_str(), &end, 10);
        if (memory.empty() || *end || !value || value > INT_MAX) {
          result =
              absl::InvalidArgumentError("EAL -m requires a positive MiB size");
          return;
        }
        options.mem_size = value;
      } else {
        if (!forwarded.empty()) forwarded += ' ';
        forwarded += token;
      }
    }
    extra = std::move(forwarded);
    if (options.no_huge && options.mem_size < 0) options.mem_size = 512;
    if (const char* memory = std::getenv("BYCORF_DPDK_MEMORY_MB")) {
      char* end = nullptr;
      const unsigned long value = std::strtoul(memory, &end, 10);
      if (!*memory || *end || !value || value > INT_MAX) {
        result = absl::InvalidArgumentError(
            "BYCORF_DPDK_MEMORY_MB must be a positive integer");
        return;
      }
      options.mem_size = value;
    }
    options.env_context = extra.empty() ? nullptr : extra.data();
    options.unlink_hugepage = !options.no_huge;
    const int rc = spdk_env_init(&options);
    const int restored =
        pthread_setaffinity_np(pthread_self(), sizeof(original), &original);
    if (rc < 0)
      result = absl::FailedPreconditionError(
          "SPDK/DPDK environment initialization failed; inspect EAL "
          "diagnostics");
    else if (restored)
      result = absl::InternalError(
          "EAL initialized but affinity restoration failed");
    else
      result = absl::OkStatus();
  });
  return result;
}
}  // namespace bycorf
