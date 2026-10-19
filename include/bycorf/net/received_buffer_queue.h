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
#include <type_traits>
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
  // User-provided so value-initializing the queue does not zero the unused
  // inline slot before its first descriptor is constructed.
  ReceivedBufferQueue() noexcept {}
  ReceivedBufferQueue(const ReceivedBufferQueue&) = delete;
  ReceivedBufferQueue& operator=(const ReceivedBufferQueue&) = delete;
  ReceivedBufferQueue(ReceivedBufferQueue&& other) noexcept {
    *this = std::move(other);
  }
  ReceivedBufferQueue& operator=(ReceivedBufferQueue&& other) noexcept {
    if (this != &other) {
      if (other.capacity_ == kInlineCapacity) {
        // Inline elements belong to the queue object and must be copied before
        // the moved-from queue can be reused or destroyed.
        data_.reset();
        if (other.size_ != 0)
          std::construct_at(&inline_.value_, other.inline_.value_);
      } else {
        data_ = std::move(other.data_);
      }
      head_ = std::exchange(other.head_, 0);
      size_ = std::exchange(other.size_, 0);
      capacity_ = std::exchange(other.capacity_, kInlineCapacity);
      low_water_pops_ = std::exchange(other.low_water_pops_, 0);
    }
    return *this;
  }

  bool empty() const noexcept { return size_ == 0; }
  std::uint32_t size() const noexcept { return size_; }
  std::uint32_t capacity() const noexcept { return capacity_; }
  ReceivedBuffer& operator[](std::uint32_t index) noexcept {
    assert(index < size_);
    return Storage()[(head_ + index) & (capacity_ - 1)].value_;
  }
  const ReceivedBuffer& operator[](std::uint32_t index) const noexcept {
    assert(index < size_);
    return Storage()[(head_ + index) & (capacity_ - 1)].value_;
  }
  ReceivedBuffer& front() noexcept {
    assert(!empty());
    return Storage()[head_].value_;
  }
  const ReceivedBuffer& front() const noexcept {
    assert(!empty());
    return Storage()[head_].value_;
  }

  void push_back(ReceivedBuffer value) {
    if (size_ == capacity_) Grow();
    std::construct_at(&Storage()[(head_ + size_) & (capacity_ - 1)].value_,
                      value);
    ++size_;
    if (capacity_ > kInlineCapacity && size_ > LowWaterLimit())
      low_water_pops_ = 0;
  }

  void pop_front() noexcept {
    assert(!empty());
    head_ = (head_ + 1) & (capacity_ - 1);
    --size_;
    // Inline storage cannot shrink; its low-water count stays zero.
    if (capacity_ == kInlineCapacity) return;
    if (size_ <= LowWaterLimit()) {
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
  // An empty slot has no active value. Construct only live descriptors, both
  // inline and on the heap; a union's empty constructor avoids
  // value-initializing every unused slot during growth. Trivial descriptors
  // need no destruction when popped, cleared, overwritten, or released with the
  // slot array.
  union Slot {
    ReceivedBuffer value_;
    Slot() noexcept {}
  };
  static_assert(std::is_trivially_copyable_v<ReceivedBuffer>);
  static_assert(std::is_trivially_destructible_v<Slot>);

  Slot* Storage() noexcept {
    return capacity_ == kInlineCapacity ? &inline_ : data_.get();
  }
  const Slot* Storage() const noexcept {
    return capacity_ == kInlineCapacity ? &inline_ : data_.get();
  }

  std::uint32_t LowWaterLimit() const noexcept {
    // The smallest heap allocation contracts directly to the inline slot.
    // Only count a sustained depth that actually fits the next capacity.
    return capacity_ == kMinHeapCapacity ? kInlineCapacity : capacity_ / 4;
  }

  // Keep allocation and descriptor relocation out of inlined push/pop callers.
  [[gnu::noinline]] void Grow() {
    // Allocation failure leaves the source descriptors and ownership intact.
    if (capacity_ == kMaxCapacity) throw std::bad_alloc();
    const auto capacity =
        capacity_ == kInlineCapacity ? kMinHeapCapacity : capacity_ * 2;
    auto data = std::make_unique<Slot[]>(capacity);
    CopyTo(data.get());
    data_ = std::move(data);
    head_ = 0;
    capacity_ = capacity;
    low_water_pops_ = 0;
  }

  [[gnu::noinline]] void Shrink() noexcept {
    low_water_pops_ = 0;
    const auto capacity =
        capacity_ == kMinHeapCapacity ? kInlineCapacity : capacity_ / 2;
    if (capacity == kInlineCapacity) {
      CopyTo(&inline_);
      data_.reset();
    } else {
      std::unique_ptr<Slot[]> data(new (std::nothrow) Slot[capacity]);
      if (!data) return;
      CopyTo(data.get());
      data_ = std::move(data);
    }
    head_ = 0;
    capacity_ = capacity;
  }

  void CopyTo(Slot* destination) const noexcept {
    const auto* source = Storage();
    const auto first = std::min(size_, capacity_ - head_);
    for (std::uint32_t i = 0; i < first; ++i)
      std::construct_at(&destination[i].value_, source[head_ + i].value_);
    for (std::uint32_t i = first; i < size_; ++i)
      std::construct_at(&destination[i].value_, source[i - first].value_);
  }

  static constexpr std::uint32_t kInlineCapacity = 1;
  static constexpr std::uint32_t kMinHeapCapacity = 8;
  static constexpr std::uint32_t kMaxCapacity = 1u << 30;
  // The inline descriptor and frequently written counters occupy 28 bytes.
  // Keep the heap pointer after them so the usual depth-one path need not
  // access it. In the audited x86 layout, this queue starts at Connection+96:
  // these 28 bytes fit one cache line if the Connection is 64-byte aligned.
  // Recheck allocator addresses and throughput/tails after layout changes.
  Slot inline_;
  std::uint32_t head_ = 0;
  std::uint32_t size_ = 0;
  std::uint32_t capacity_ = kInlineCapacity;
  std::uint32_t low_water_pops_ = 0;
  std::unique_ptr<Slot[]> data_;
};

}  // namespace bycorf
#endif  // BYCORF_NET_RECEIVED_BUFFER_QUEUE_H_
