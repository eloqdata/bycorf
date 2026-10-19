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

#ifndef BYCORF_NET_RECEIVED_BUFFER_QUEUE_H_
#define BYCORF_NET_RECEIVED_BUFFER_QUEUE_H_

#include <cstdint>

#include "bycorf/base/inline_ring_queue.h"

namespace bycorf {

struct ReceivedBuffer {
  std::uint16_t buffer_id_ = 0;
  std::uint32_t size_ = 0;
  std::uint32_t offset_ = 0;
};

// Owner-worker FIFO of receive descriptors, not the network bytes themselves.
// Callers own recycling backend buffers, including before clear(). References
// must not survive push/pop or moving the queue.
// The inline descriptor and mutable counters occupy the first 32 bytes. In the
// audited x86 layout the queue starts at Connection+96, so these fit one cache
// line when the Connection is 64-byte aligned. The heap owner shares storage
// with the inline descriptor.
// Recheck allocator addresses and throughput/tails after layout changes.
using ReceivedBufferQueue = InlineRingQueue<ReceivedBuffer, 1>;

}  // namespace bycorf
#endif  // BYCORF_NET_RECEIVED_BUFFER_QUEUE_H_
