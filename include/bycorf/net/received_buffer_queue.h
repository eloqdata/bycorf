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

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory>
#include <new>
#include <utility>

namespace bycorf {

struct ReceivedBuffer {
  std::uint16_t buffer_id_ = 0;
  std::uint32_t size_ = 0;
  std::uint32_t offset_ = 0;
};

// Owner-worker FIFO of receive descriptors, not the network bytes themselves.
// Resizing preserves buffer IDs and partial-read offsets; callers still own
// recycling the referenced backend buffers. Never retain an element reference
// across push_back/pop_front: either operation can relocate the descriptors.
class ReceivedBufferQueue {
 public:
  ReceivedBufferQueue() = default;
  ReceivedBufferQueue(const ReceivedBufferQueue&) = delete;
  ReceivedBufferQueue& operator=(const ReceivedBufferQueue&) = delete;
  ReceivedBufferQueue(ReceivedBufferQueue&& other) noexcept {
    *this = std::move(other);
  }
  ReceivedBufferQueue& operator=(ReceivedBufferQueue&& other) noexcept {
    if (this != &other) {
      data_ = std::move(other.data_);
      head_ = std::exchange(other.head_, 0);
      size_ = std::exchange(other.size_, 0);
      capacity_ = std::exchange(other.capacity_, 0);
      low_water_pops_ = std::exchange(other.low_water_pops_, 0);
    }
    return *this;
  }

  bool empty() const noexcept { return size_ == 0; }
  std::uint32_t size() const noexcept { return size_; }
  std::uint32_t capacity() const noexcept { return capacity_; }
  ReceivedBuffer& operator[](std::uint32_t index) noexcept {
    assert(index < size_);
    return data_[(head_ + index) & (capacity_ - 1)];
  }
  const ReceivedBuffer& operator[](std::uint32_t index) const noexcept {
    assert(index < size_);
    return data_[(head_ + index) & (capacity_ - 1)];
  }
  ReceivedBuffer& front() noexcept {
    assert(!empty());
    return data_[head_];
  }
  const ReceivedBuffer& front() const noexcept {
    assert(!empty());
    return data_[head_];
  }

  void push_back(ReceivedBuffer value) {
    if (size_ == capacity_) Grow();
    data_[(head_ + size_) & (capacity_ - 1)] = value;
    ++size_;
    if (size_ > capacity_ / 4) low_water_pops_ = 0;
  }

  void pop_front() noexcept {
    assert(!empty());
    head_ = (head_ + 1) & (capacity_ - 1);
    --size_;
    if (capacity_ > kMinCapacity && size_ <= capacity_ / 4) {
      // Require a full capacity's worth of low-water pops before halving.
      // Brief bursts and ordinary empty/nonempty alternation at the minimum
      // capacity do not allocate. Shrinking is best-effort: reads and close
      // cleanup must keep working even when allocation fails.
      if (++low_water_pops_ == capacity_) Shrink();
    } else {
      low_water_pops_ = 0;
    }
  }

  // Like deque::clear, this discards descriptors without returning backend
  // buffers. Production close paths must recycle each buffer before clearing.
  void clear() noexcept { head_ = size_ = low_water_pops_ = 0; }

 private:
  // Keep allocation and descriptor relocation out of inlined push/pop callers.
  // These paths are rare; inlining them bloats the ordinary receive hot path.
  [[gnu::noinline]] void Grow() {
    // Bound both doubling and head + index arithmetic. Allocation failure
    // leaves every queued descriptor intact, as with deque::push_back.
    if (capacity_ == kMaxCapacity) throw std::bad_alloc();
    const auto capacity = capacity_ == 0 ? kMinCapacity : capacity_ * 2;
    Reallocate(std::make_unique<ReceivedBuffer[]>(capacity), capacity);
  }

  [[gnu::noinline]] void Shrink() noexcept {
    low_water_pops_ = 0;
    const auto capacity = capacity_ / 2;
    std::unique_ptr<ReceivedBuffer[]> data(new (std::nothrow)
                                               ReceivedBuffer[capacity]);
    if (data) Reallocate(std::move(data), capacity);
  }

  void Reallocate(std::unique_ptr<ReceivedBuffer[]> data,
                  std::uint32_t capacity) noexcept {
    if (size_ != 0) {
      const auto first = std::min(size_, capacity_ - head_);
      std::copy_n(data_.get() + head_, first, data.get());
      std::copy_n(data_.get(), size_ - first, data.get() + first);
    }
    data_ = std::move(data);
    head_ = 0;
    capacity_ = capacity;
    low_water_pops_ = 0;
  }

  static constexpr std::uint32_t kMinCapacity = 8;
  static constexpr std::uint32_t kMaxCapacity = 1u << 30;
  std::unique_ptr<ReceivedBuffer[]> data_;
  std::uint32_t head_ = 0;
  std::uint32_t size_ = 0;
  std::uint32_t capacity_ = 0;
  std::uint32_t low_water_pops_ = 0;
};

}  // namespace bycorf
#endif  // BYCORF_NET_RECEIVED_BUFFER_QUEUE_H_
