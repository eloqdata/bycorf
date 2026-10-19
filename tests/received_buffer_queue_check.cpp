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

#include <cstdlib>
#include <deque>
#include <iostream>
#include <random>
#include <utility>

#include "bycorf/net/received_buffer_queue.h"

namespace {
void Check(bool ok) {
  if (!ok) {
    std::cerr << "received buffer queue check failed\n";
    std::abort();
  }
}
bool Equal(const bycorf::ReceivedBuffer& a, const bycorf::ReceivedBuffer& b) {
  return a.buffer_id_ == b.buffer_id_ && a.size_ == b.size_ &&
         a.offset_ == b.offset_;
}
void Compare(const bycorf::ReceivedBufferQueue& queue,
             const std::deque<bycorf::ReceivedBuffer>& reference) {
  Check(queue.size() == reference.size());
  for (std::uint32_t i = 0; i < queue.size(); ++i)
    Check(Equal(queue[i], reference[i]));
}
}  // namespace

int main() {
  bycorf::ReceivedBufferQueue queue;
  Check(queue.empty() && queue.capacity() == 0);
  std::deque<bycorf::ReceivedBuffer> reference;
  std::mt19937 random(20261019);
  // Alternate sustained growth and draining, with partial front consumption.
  // This exercises wrapped growth and contraction, not just an empty queue.
  for (unsigned phase = 0; phase < 12; ++phase) {
    for (unsigned step = 0; step < 10000; ++step) {
      const auto value = static_cast<std::uint32_t>(random());
      if (queue.empty() || value % 100 < (phase % 2 == 0 ? 60 : 40)) {
        bycorf::ReceivedBuffer buffer{
            .buffer_id_ = static_cast<std::uint16_t>(value),
            .size_ = value % 65536 + 10,
            .offset_ = value % 10};
        queue.push_back(buffer);
        reference.push_back(buffer);
      } else {
        Check(Equal(queue.front(), reference.front()));
        queue.pop_front();
        reference.pop_front();
      }
      if (!queue.empty() && step % 7 == 0) {
        ++queue.front().offset_;
        ++reference.front().offset_;
      }
      if (step % 113 == 0) Compare(queue, reference);
    }
    Compare(queue, reference);
  }
  while (queue.size() < 256) {
    queue.push_back({.buffer_id_ = 42, .size_ = 100, .offset_ = 7});
    reference.push_back({.buffer_id_ = 42, .size_ = 100, .offset_ = 7});
  }
  const auto peak = queue.capacity();
  while (queue.size() > 3) {
    queue.pop_front();
    reference.pop_front();
  }
  for (unsigned i = 0; i < peak * 3; ++i) {
    queue.push_back({.buffer_id_ = 7, .size_ = 200, .offset_ = 11});
    reference.push_back({.buffer_id_ = 7, .size_ = 200, .offset_ = 11});
    queue.pop_front();
    reference.pop_front();
    Compare(queue, reference);
  }
  Check(queue.capacity() < peak && queue.capacity() >= queue.size());
  auto moved = std::move(queue);
  Check(queue.empty() && queue.capacity() == 0);
  Compare(moved, reference);
  queue.push_back({.buffer_id_ = 99});
  queue = std::move(moved);
  Check(moved.empty() && moved.capacity() == 0);
  Compare(queue, reference);
  queue.clear();
  Check(queue.empty());
  for (unsigned i = 0; i < 10000; ++i) {
    queue.push_back({.buffer_id_ = 1, .size_ = 32});
    Check(queue.front().size_ == 32);
    queue.pop_front();
  }
  Check(queue.capacity() == 8);
  std::cout << "PASS FIFO, wrap, growth, delayed shrink, partial offsets, "
               "move, clear\n";
}
