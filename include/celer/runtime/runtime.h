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

#ifndef CELER_RUNTIME_RUNTIME_H_
#define CELER_RUNTIME_RUNTIME_H_

#include <cstddef>
#include <functional>
#include <memory>

#include "celer/runtime/worker.h"

namespace celer {

class Runtime {
 public:
  using WorkerMain = std::function<int(unsigned, Worker&)>;

  Runtime();
  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;
  Runtime(Runtime&&) noexcept;
  Runtime& operator=(Runtime&&) noexcept;
  ~Runtime();

  void Start(unsigned thread_count, WorkerMain main_fn,
             bool pin_workers = true);
  void RequestStop() noexcept;
  void WaitUntilStopped();

  bool started() const noexcept;
  bool stopped() const noexcept;
  int completion_fd() const noexcept;
  int exit_code() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace celer

#endif  // CELER_RUNTIME_RUNTIME_H_
