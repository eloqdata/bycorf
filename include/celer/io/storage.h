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

#ifndef CELER_IO_STORAGE_H_
#define CELER_IO_STORAGE_H_

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <sys/types.h>

#include "celer/base/status.h"
#include "celer/runtime/task.h"

namespace celer {

class Worker;

// Ring-local file-table reference. It is intentionally not an OS descriptor.
struct FixedFile {
  std::uint32_t index = 0;
};

// A range inside an iovec registered with this worker's ring.
struct FixedBuffer {
  std::byte* data = nullptr;
  std::size_t size = 0;
  std::uint16_t index = 0;
};

// Coroutine wrappers around Worker's ring-local fixed-file/fixed-buffer
// submissions. They suspend only until the corresponding CQE is dispatched;
// no blocking filesystem calls or helper threads are involved.
Task<Status> OpenFixedFile(Worker& worker, std::string path, int flags,
                           mode_t mode, FixedFile file);
Task<Status> CloseFixedFile(Worker& worker, FixedFile file);
Task<StatusOr<std::size_t>> ReadFixed(Worker& worker, FixedFile file,
                                      FixedBuffer buffer,
                                      std::uint64_t offset);
Task<StatusOr<std::size_t>> Read(Worker& worker, FixedFile file,
                                 std::span<std::byte> buffer,
                                 std::uint64_t offset);
Task<StatusOr<std::size_t>> WriteFixed(Worker& worker, FixedFile file,
                                       FixedBuffer buffer,
                                       std::uint64_t offset);
Task<Status> Fdatasync(Worker& worker, FixedFile file);

}  // namespace celer

#endif  // CELER_IO_STORAGE_H_
