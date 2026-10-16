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

#ifndef BYCORF_IO_COMPLETION_H_
#define BYCORF_IO_COMPLETION_H_

#include <coroutine>

namespace bycorf {

class Worker;

// Backend-neutral completion flags. The io backend translates its native flags
// (e.g. io_uring's IORING_CQE_F_MORE) into these before invoking Complete(), so
// upper layers never see backend-specific symbols.
enum CompletionFlags : unsigned {
  kCompletionNone = 0,
  kCompletionMore = 1u << 0,  // a multishot op will deliver further completions
};

// Backend-neutral completion callback. The io backend invokes Complete() when
// an operation submitted with this object as its tag finishes. `result` is the
// op result (>=0 byte count / fd, <0 -errno); `flags` is a CompletionFlags
// bitset. Each storage operation needs its own tag until completion, including
// time spent queued for backend capacity. Complete() may reuse the tag for the
// next operation or destroy it; backends must read all per-operation state
// before dispatching and must not access the tag afterwards.
class IoCompletion {
 public:
  virtual ~IoCompletion() = default;

  virtual void Complete(Worker& worker, int result, unsigned flags) = 0;

 protected:
  std::coroutine_handle<> awaiting_{};

 private:
  friend class SpdkStorageBackend;

  // Status-only storage completions need the expected successful byte count
  // (zero for flush). Keep it with the caller-owned tag so submitted commands
  // need no separately allocated backend callback context.
  int success_result_ = 0;
};

}  // namespace bycorf

#endif  // BYCORF_IO_COMPLETION_H_
