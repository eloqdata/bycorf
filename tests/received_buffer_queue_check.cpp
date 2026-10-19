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
#include <source_location>
#include <utility>

#include "bycorf/net/received_buffer_queue.h"

namespace {
void Check(bool ok,
           std::source_location location = std::source_location::current()) {
  if (!ok) {
    std::cerr << "received buffer queue check failed at line "
              << location.line() << "\n";
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
void CheckInlineAndHeapMoves() {
  for (bool source_heap : {false, true}) {
    for (bool destination_heap : {false, true}) {
      bycorf::ReceivedBufferQueue destination;
      for (unsigned i = 0; i < (destination_heap ? 4u : 1u); ++i)
        destination.push_back({.buffer_id_ = 99});
      std::deque<bycorf::ReceivedBuffer> expected;
      {
        bycorf::ReceivedBufferQueue source;
        for (unsigned i = 0; i < (source_heap ? 4u : 1u); ++i) {
          bycorf::ReceivedBuffer value{
              .buffer_id_ = static_cast<std::uint16_t>(i),
              .size_ = 100 + i,
              .offset_ = i};
          source.push_back(value);
          expected.push_back(value);
        }
        destination = std::move(source);
        Check(source.empty() && source.capacity() == 1);
        // Reusing and destroying the source must not alias an inline element.
        source.push_back({.buffer_id_ = 77, .size_ = 7});
        Compare(destination, expected);
      }
      Compare(destination, expected);
      bycorf::ReceivedBufferQueue constructed(std::move(destination));
      Check(destination.empty() && destination.capacity() == 1);
      destination.push_back({.buffer_id_ = 88});
      Compare(constructed, expected);
      auto& same = constructed;
      constructed = std::move(same);
      Compare(constructed, expected);
    }
  }
  bycorf::ReceivedBufferQueue small;
  for (unsigned cycle = 0; cycle < 100; ++cycle) {
    small.push_back({.buffer_id_ = 1});
    Check(small.capacity() == 1);
    small.push_back({.buffer_id_ = 2});
    Check(small.capacity() == 8);
    small.pop_front();
    Check(small.front().buffer_id_ == 2 && small.capacity() == 8);
    small.pop_front();
    Check(small.empty() && small.capacity() == 8);
    for (unsigned i = 0; i < 6; ++i) {
      small.push_back({.buffer_id_ = 3});
      small.pop_front();
    }
    Check(small.empty() && small.capacity() == 1);
  }
}
// No default construction is needed, even for over-aligned values. Its copy
// constructor and destructor remain trivial.
struct alignas(32) Value {
  Value() = delete;
  explicit Value(std::uint64_t v) : value(v) {}
  bool operator==(const Value&) const = default;
  std::uint64_t value;
};

template <typename T, std::uint32_t N>
void CheckGenericQueue() {
  using Queue = bycorf::InlineRingQueue<T, N>;
  auto compare = [](const Queue& q, const std::deque<T>& expected) {
    Check(q.size() == expected.size());
    Check(q.capacity() >= q.size());
    if (!q.empty()) Check(q.front() == expected.front());
    for (std::uint32_t i = 0; i < q.size(); ++i) Check(q[i] == expected[i]);
  };
  // Exercise wrapped inline and heap sources against both destination modes.
  for (bool source_heap : {false, true}) {
    for (bool destination_heap : {false, true}) {
      Queue destination;
      for (unsigned i = 0; i < (destination_heap ? N + 1 : N); ++i)
        destination.push_back(T(999));
      std::deque<T> expected;
      {
        Queue source;
        const auto count = source_heap ? N + 1 : N;
        for (unsigned i = 0; i < count; ++i) {
          source.push_back(T(i));
          expected.push_back(T(i));
        }
        for (unsigned i = 0; i < count * 3 + 1; ++i) {
          source.pop_front();
          expected.pop_front();
          source.push_back(T(100 + i));
          expected.push_back(T(100 + i));
        }
        destination = std::move(source);
        Check(source.empty() && source.capacity() == N);
        source.push_back(T(777));
        compare(destination, expected);
      }
      compare(destination, expected);
      Queue constructed(std::move(destination));
      Check(destination.empty() && destination.capacity() == N);
      destination.push_back(T(888));
      compare(constructed, expected);
      auto& same = constructed;
      constructed = std::move(same);
      compare(constructed, expected);
      // Argument aliases a live value when growth relocates the source.
      while (constructed.size() < constructed.capacity()) {
        constructed.push_back(T(42));
        expected.push_back(T(42));
      }
      expected.push_back(constructed.front());
      constructed.push_back(constructed.front());
      compare(constructed, expected);
    }
  }
  Queue queue;
  std::deque<T> expected;
  std::mt19937 random(20261019 + N);
  for (unsigned phase = 0; phase < 8; ++phase) {
    for (unsigned step = 0; step < 4000; ++step) {
      auto v = random();
      if (queue.empty() || v % 100 < (phase % 2 == 0 ? 60 : 40)) {
        queue.push_back(T(v));
        expected.push_back(T(v));
      } else {
        queue.pop_front();
        expected.pop_front();
      }
      if (step % 113 == 0) compare(queue, expected);
    }
    compare(queue, expected);
  }
  while (queue.size() < N * 4) {
    queue.push_back(T(42));
    expected.push_back(T(42));
  }
  const auto peak = queue.capacity();
  while (queue.size() > N) {
    queue.pop_front();
    expected.pop_front();
  }
  // Shrink with N live values, including wrapped sources. Pop before push so
  // the push never resets the final heap-to-inline low-water count.
  for (unsigned i = 0; i < peak * 4; ++i) {
    queue.pop_front();
    expected.pop_front();
    queue.push_back(T(i));
    expected.push_back(T(i));
    compare(queue, expected);
  }
  Check(queue.capacity() == N);
  queue.push_back(T(123));
  Check(queue.capacity() > N);
  const auto capacity = queue.capacity();
  queue.clear();
  Check(queue.empty() && queue.capacity() == capacity);
  for (unsigned i = 0; i < capacity * 2; ++i) {
    queue.push_back(T(i));
    Check(queue.front() == T(i));
    queue.pop_front();
  }
  Check(queue.capacity() == N);
}

template <std::uint32_t N>
void CheckGenericTypes() {
  CheckGenericQueue<std::uint64_t, N>();
  CheckGenericQueue<Value, N>();
}
}  // namespace

int main() {
  CheckGenericTypes<1>();
  CheckGenericTypes<2>();
  CheckGenericTypes<4>();
  CheckGenericTypes<8>();
  CheckGenericTypes<16>();
  CheckGenericTypes<32>();
  CheckInlineAndHeapMoves();
  bycorf::ReceivedBufferQueue queue;
  Check(queue.empty() && queue.capacity() == 1);
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
  Check(queue.empty() && queue.capacity() == 1);
  Compare(moved, reference);
  queue.push_back({.buffer_id_ = 99});
  queue = std::move(moved);
  Check(moved.empty() && moved.capacity() == 1);
  Compare(queue, reference);
  queue.clear();
  Check(queue.empty());
  for (unsigned i = 0; i < 10000; ++i) {
    queue.push_back({.buffer_id_ = 1, .size_ = 32});
    Check(queue.front().size_ == 32);
    queue.pop_front();
  }
  Check(queue.capacity() == 1);
  std::cout
      << "PASS FIFO, wrap, growth, delayed shrink, partial offsets, "
         "move, clear, generic types and inline capacities 1/2/4/8/16/32\n";
}
