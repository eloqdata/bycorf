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

#include <atomic>
#include <cstdlib>
#include <iostream>

#include "bycorf/runtime/runtime.h"

// One worker fails before backend initialization, while its peer may already
// be waiting for FreeBSD bootstrap. Neither ordering may strand the runtime.
int main(int argc, char** argv) {
  const unsigned failed_worker = argc > 1 ? std::atoi(argv[1]) : 0;
  if (failed_worker > 1) return 2;
#if BYCORF_KERNEL_BYPASS
  if (!bycorf::ConfigureIoBackends({.dpdk_network = true}).ok()) return 1;
#endif
  bycorf::Runtime runtime;
  std::atomic<unsigned> entered{0};
  std::atomic<bool> rejected{false};
  runtime.Start(2, [&](unsigned index, bycorf::Worker& worker) {
    ++entered;
    bycorf::WorkerOptions options;
    if (index == failed_worker) options.foreground_budget_us_ = 0;
    const auto status = worker.Init(options);
    if (!status.ok()) {
      if (index == failed_worker) rejected = true;
      return 1;
    }
    worker.Run();
    worker.Shutdown();
    return 0;
  });
  runtime.WaitUntilStopped();
  if (entered != 2 || !rejected || runtime.exit_code() == 0) return 1;
  std::cout << "startup failure released both workers\n";
  return 0;
}
