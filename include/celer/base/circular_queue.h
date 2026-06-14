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

#ifndef CELER_BASE_CIRCULAR_QUEUE_H_
#define CELER_BASE_CIRCULAR_QUEUE_H_

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <utility>

namespace celer {

inline void ValidateCapacity(std::size_t capacity) {
  if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
    throw std::invalid_argument("CircularQueue capacity must be a power of two");
  }
}

// Single-threaded growable ring buffer (power-of-two capacity, masked indexing).
template <typename T>
class CircularQueue {
 public:
  explicit CircularQueue(std::size_t capacity = 8) : head_(0), cnt_(0) {
    ValidateCapacity(capacity);
    capacity_ = capacity;
    mask_ = capacity_ - 1;
    vec_ = std::make_unique<T[]>(capacity_);
  }

  CircularQueue(CircularQueue&& rhs) noexcept {
    head_ = rhs.head_;
    cnt_ = rhs.cnt_;
    capacity_ = rhs.capacity_;
    mask_ = rhs.mask_;
    vec_ = std::move(rhs.vec_);
  }

  CircularQueue& operator=(CircularQueue&& rhs) noexcept {
    if (this != &rhs) {
      head_ = rhs.head_;
      cnt_ = rhs.cnt_;
      capacity_ = rhs.capacity_;
      mask_ = rhs.mask_;
      vec_ = std::move(rhs.vec_);
    }
    return *this;
  }

  CircularQueue(const CircularQueue& rhs) = delete;
  CircularQueue& operator=(const CircularQueue& rhs) = delete;

  ~CircularQueue() = default;

  void Reset(std::size_t new_cap) {
    head_ = 0;
    cnt_ = 0;
    ValidateCapacity(new_cap);
    capacity_ = new_cap;
    mask_ = capacity_ - 1;
    vec_ = std::make_unique<T[]>(capacity_);
  }

  void Enqueue(const T& item) {
    if (__builtin_expect(cnt_ != capacity_, 1)) {
      std::size_t tail = (head_ + cnt_) & mask_;
      vec_[tail] = item;
      ++cnt_;
      return;
    }
    Grow();
    std::size_t tail = (head_ + cnt_) & mask_;
    vec_[tail] = item;
    ++cnt_;
  }

  void Enqueue(T&& item) {
    if (__builtin_expect(cnt_ != capacity_, 1)) {
      std::size_t tail = (head_ + cnt_) & mask_;
      vec_[tail] = std::move(item);
      ++cnt_;
      return;
    }
    Grow();
    std::size_t tail = (head_ + cnt_) & mask_;
    vec_[tail] = std::move(item);
    ++cnt_;
  }

  void Dequeue() {
    assert(cnt_ > 0);
    head_ = (head_ + 1) & mask_;
    --cnt_;
  }

  T& Peek() { return vec_[head_]; }

  std::size_t Size() const { return cnt_; }
  std::size_t Capacity() const { return capacity_; }
  bool Empty() const { return cnt_ == 0; }

  T& Get(std::size_t index) const { return vec_[(head_ + index) & mask_]; }

 private:
  void Grow() {
    std::size_t new_capacity = capacity_ << 1;
    std::unique_ptr<T[]> new_vec = std::make_unique<T[]>(new_capacity);
    std::size_t end = 0;
    for (std::size_t idx = head_; idx < capacity_; ++idx, ++end) {
      new_vec[end] = std::move(vec_[idx]);
    }
    for (std::size_t idx = 0; idx < head_; ++idx, ++end) {
      new_vec[end] = std::move(vec_[idx]);
    }
    capacity_ = new_capacity;
    mask_ = capacity_ - 1;
    head_ = 0;
    vec_ = std::move(new_vec);
  }

  std::unique_ptr<T[]> vec_;
  std::size_t head_;
  std::size_t cnt_;
  std::size_t capacity_;
  std::size_t mask_;
};

}  // namespace celer

#endif  // CELER_BASE_CIRCULAR_QUEUE_H_
