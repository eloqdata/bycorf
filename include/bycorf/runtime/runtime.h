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

#ifndef BYCORF_RUNTIME_RUNTIME_H_
#define BYCORF_RUNTIME_RUNTIME_H_

#include <cstddef>
#include <functional>
#include <memory>
#include <span>

#include "bycorf/runtime/foreign_executor.h"
#include "bycorf/runtime/worker.h"

namespace bycorf {

class Runtime {
 public:
  using WorkerMain = std::function<int(unsigned, Worker&)>;

  Runtime();
  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;
  Runtime(Runtime&&) noexcept;
  Runtime& operator=(Runtime&&) noexcept;
  ~Runtime();

  // Empty cpu_ids uses the caller's allowed CPUs; otherwise the supplied CPU
  // list is cycled over workers. Repeated IDs deliberately share a logical CPU.
  // Every ID must belong to the inherited affinity mask. Explicit placement
  // requires pin_workers; setup failures prevent all worker mains from running.
  void Start(unsigned thread_count, WorkerMain main_fn, bool pin_workers = true,
             std::span<const unsigned> cpu_ids = {});
  // Request shutdown after Start returns, even if a worker has not entered
  // Init or Run yet. WorkerMain must eventually observe stop or return.
  void RequestStop() noexcept;
  void WaitUntilStopped();

  bool started() const noexcept;
  bool stopped() const noexcept;
  int completion_fd() const noexcept;
  int exit_code() const noexcept;

  // Returns a copyable, non-owning handle for posting to one worker from
  // threads outside this Runtime. Start must have completed and worker_id must
  // name an existing worker; misuse throws std::logic_error/out_of_range.
  bycorf::ForeignExecutor GetForeignExecutor(WorkerId worker_id);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bycorf

#endif  // BYCORF_RUNTIME_RUNTIME_H_
