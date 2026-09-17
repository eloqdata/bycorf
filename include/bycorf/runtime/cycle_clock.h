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

#ifndef BYCORF_RUNTIME_CYCLE_CLOCK_H_
#define BYCORF_RUNTIME_CYCLE_CLOCK_H_

#include <cstdint>

namespace bycorf {

// Low-overhead monotonic counter used for runtime scheduling and hot-path
// latency measurement. Convert deltas with CycleCounterFrequency().
std::uint64_t ReadCycleCounter() noexcept;
double CycleCounterFrequency() noexcept;

}  // namespace bycorf

#endif  // BYCORF_RUNTIME_CYCLE_CLOCK_H_
