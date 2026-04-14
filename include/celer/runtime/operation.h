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

#ifndef CELER_RUNTIME_OPERATION_H_
#define CELER_RUNTIME_OPERATION_H_

#include <coroutine>

namespace celer {

class Worker;

class OperationBase {
 public:
  virtual ~OperationBase() = default;

  virtual void Complete(Worker& worker, int result, unsigned flags) = 0;

 protected:
  std::coroutine_handle<> awaiting_{};
};

}  // namespace celer

#endif  // CELER_RUNTIME_OPERATION_H_
