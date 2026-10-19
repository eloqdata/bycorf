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

#ifndef BYCORF_BASE_INLINE_RING_QUEUE_H_
#define BYCORF_BASE_INLINE_RING_QUEUE_H_

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace bycorf {

// Single-threaded resizable FIFO for trivially copyable values. InlineCapacity
// must be a positive power of two; heap storage starts at max(8, 2 * inline).
// Push/pop and moves may relocate elements and invalidate references. This
// container does not manage resources referenced by its values.
template <typename T, std::uint32_t InlineCapacity>
class InlineRingQueue {
  static_assert(
      std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T> &&
          std::is_nothrow_copy_constructible_v<T>,
      "InlineRingQueue requires trivial values with nonthrowing copy");
  static_assert(InlineCapacity > 0 &&
                    (InlineCapacity & (InlineCapacity - 1)) == 0 &&
                    InlineCapacity <= (1u << 29),
                "InlineCapacity must be a power of two in [1, 2^29]");

 public:
  // User-provided so value-initializing the queue does not zero the unused
  // inline slots before their first values are constructed.
  InlineRingQueue() noexcept {}
  ~InlineRingQueue() {
    if (capacity_ > kInlineCapacity) std::destroy_at(&storage_.data_);
  }
  InlineRingQueue(const InlineRingQueue&) = delete;
  InlineRingQueue& operator=(const InlineRingQueue&) = delete;
  InlineRingQueue(InlineRingQueue&& other) noexcept {
    *this = std::move(other);
  }
  InlineRingQueue& operator=(InlineRingQueue&& other) noexcept {
    if (this != &other) {
      if (other.capacity_ == kInlineCapacity) {
        // Inline elements belong to the queue object and must be copied before
        // the moved-from queue can be reused or destroyed.
        if (capacity_ > kInlineCapacity) {
          std::destroy_at(&storage_.data_);
          std::construct_at(&storage_.inline_);
        }
        other.CopyTo(storage_.inline_.slots_);
        head_ = 0;
      } else {
        if (capacity_ == kInlineCapacity)
          std::construct_at(&storage_.data_, std::move(other.storage_.data_));
        else
          storage_.data_ = std::move(other.storage_.data_);
        head_ = other.head_;
        std::destroy_at(&other.storage_.data_);
        std::construct_at(&other.storage_.inline_);
      }
      other.head_ = 0;
      size_ = std::exchange(other.size_, 0);
      capacity_ = std::exchange(other.capacity_, kInlineCapacity);
      low_water_pops_ = std::exchange(other.low_water_pops_, 0);
    }
    return *this;
  }

  bool empty() const noexcept { return size_ == 0; }
  std::uint32_t size() const noexcept { return size_; }
  std::uint32_t capacity() const noexcept { return capacity_; }
  T& operator[](std::uint32_t index) noexcept {
    assert(index < size_);
    return Storage()[(head_ + index) & (capacity_ - 1)].value_;
  }
  const T& operator[](std::uint32_t index) const noexcept {
    assert(index < size_);
    return Storage()[(head_ + index) & (capacity_ - 1)].value_;
  }
  T& front() noexcept {
    assert(!empty());
    return Storage()[head_].value_;
  }
  const T& front() const noexcept {
    assert(!empty());
    return Storage()[head_].value_;
  }

  void push_back(T value) {
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

  // Retains heap capacity. Callers must release any resources referenced by
  // the values before discarding them.
  void clear() noexcept { head_ = size_ = low_water_pops_ = 0; }

 private:
  // An empty slot has no active value. Construct only live values, both
  // inline and on the heap; a union's empty constructor avoids
  // value-initializing every unused slot during growth. Trivial values
  // need no destruction when popped, cleared, overwritten, or released with the
  // slot array.
  union Slot {
    T value_;
    Slot() noexcept {}
  };
  static_assert(std::is_trivially_destructible_v<Slot>);

  struct InlineStorage {
    Slot slots_[InlineCapacity];
    // User-provided to avoid zeroing unused inline slots on activation.
    InlineStorage() noexcept {}
  };
  union StorageUnion {
    InlineStorage inline_;
    std::unique_ptr<Slot[]> data_;
    StorageUnion() noexcept : inline_() {}
    // capacity_ selects the active member; the queue destroys heap ownership.
    ~StorageUnion() {}
  };

  Slot* Storage() noexcept {
    return capacity_ == kInlineCapacity ? storage_.inline_.slots_
                                        : storage_.data_.get();
  }
  const Slot* Storage() const noexcept {
    return capacity_ == kInlineCapacity ? storage_.inline_.slots_
                                        : storage_.data_.get();
  }

  std::uint32_t LowWaterLimit() const noexcept {
    // The smallest heap allocation contracts directly to the inline slot.
    // Only count a sustained depth that actually fits the next capacity.
    return capacity_ == kMinHeapCapacity ? kInlineCapacity : capacity_ / 4;
  }

  // Keep allocation and value relocation out of inlined push/pop callers.
  [[gnu::noinline]] void Grow() {
    // Allocation failure leaves the source values and ownership intact.
    if (capacity_ == kMaxCapacity) throw std::bad_alloc();
    const auto capacity =
        capacity_ == kInlineCapacity ? kMinHeapCapacity : capacity_ * 2;
    auto data = std::make_unique<Slot[]>(capacity);
    CopyTo(data.get());
    // Allocation and copying finish before replacing the active union member.
    if (capacity_ == kInlineCapacity)
      std::construct_at(&storage_.data_, std::move(data));
    else
      storage_.data_ = std::move(data);
    head_ = 0;
    capacity_ = capacity;
    low_water_pops_ = 0;
  }

  [[gnu::noinline]] void Shrink() noexcept {
    low_water_pops_ = 0;
    const auto capacity =
        capacity_ == kMinHeapCapacity ? kInlineCapacity : capacity_ / 2;
    if (capacity == kInlineCapacity) {
      // Keep the source array alive locally before its owner is overwritten by
      // inline slots. CopyFrom uses the old head/capacity until copying ends.
      auto data = std::move(storage_.data_);
      std::destroy_at(&storage_.data_);
      std::construct_at(&storage_.inline_);
      CopyFrom(data.get(), storage_.inline_.slots_);
    } else {
      std::unique_ptr<Slot[]> data(new (std::nothrow) Slot[capacity]);
      if (!data) return;
      CopyTo(data.get());
      storage_.data_ = std::move(data);
    }
    head_ = 0;
    capacity_ = capacity;
  }

  void CopyTo(Slot* destination) const noexcept {
    CopyFrom(Storage(), destination);
  }

  void CopyFrom(const Slot* source, Slot* destination) const noexcept {
    const auto first = std::min(size_, capacity_ - head_);
    for (std::uint32_t i = 0; i < first; ++i)
      std::construct_at(&destination[i].value_, source[head_ + i].value_);
    for (std::uint32_t i = first; i < size_; ++i)
      std::construct_at(&destination[i].value_, source[i - first].value_);
  }

  static constexpr std::uint32_t kInlineCapacity = InlineCapacity;
  static constexpr std::uint32_t kMinHeapCapacity =
      std::max(8u, InlineCapacity * 2);
  static constexpr std::uint32_t kMaxCapacity = 1u << 30;
  // Inline values and heap ownership are mutually exclusive. capacity_ is the
  // discriminator; every transition explicitly starts/ends member lifetimes.
  StorageUnion storage_;
  std::uint32_t head_ = 0;
  std::uint32_t size_ = 0;
  std::uint32_t capacity_ = kInlineCapacity;
  std::uint32_t low_water_pops_ = 0;
};

}  // namespace bycorf
#endif  // BYCORF_BASE_INLINE_RING_QUEUE_H_
