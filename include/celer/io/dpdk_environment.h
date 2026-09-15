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

#ifndef CELER_IO_DPDK_ENVIRONMENT_H_
#define CELER_IO_DPDK_ENVIRONMENT_H_

#include "absl/status/status.h"

namespace celer {
// Initializes SPDK's environment and its one shared DPDK EAL once per process.
// The calling thread's affinity is restored; no EAL worker threads are
// launched. CELER_EAL_ARGS supplies additional EAL arguments before the first
// call.
absl::Status EnsureDpdkEnvironment();
}  // namespace celer

#endif
