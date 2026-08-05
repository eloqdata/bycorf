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

#ifndef CELER_RUNTIME_COROUTINE_FRAME_POOL_H_
#define CELER_RUNTIME_COROUTINE_FRAME_POOL_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <new>

namespace celer::detail {

// Coroutine frames are allocated and released on runtime threads at a high
// rate. Cache freed frames on the thread that destroys them. This remains safe
// when a frame is created on one worker and destroyed on another, and avoids a
// synchronized global freelist.
class CoroutineFramePool {
 public:
  CoroutineFramePool() = default;
  CoroutineFramePool(const CoroutineFramePool&) = delete;
  CoroutineFramePool& operator=(const CoroutineFramePool&) = delete;

  ~CoroutineFramePool() {
    for (Block* head : free_) {
      while (head != nullptr) {
        Block* next = head->next;
        ::operator delete(head);
        head = next;
      }
    }
  }

  void* Allocate(std::size_t requested) {
    const std::size_t class_index = ClassIndex(requested);
    Block* block = nullptr;
    if (class_index < kClassCount) {
      block = free_[class_index];
      if (block != nullptr) {
        free_[class_index] = block->next;
        --count_[class_index];
        cached_bytes_ -= ClassBytes(class_index);
      } else {
        block = static_cast<Block*>(
            ::operator new(sizeof(Block) + ClassBytes(class_index)));
      }
      block->class_index = static_cast<std::uint16_t>(class_index);
    } else {
      block = static_cast<Block*>(::operator new(sizeof(Block) + requested));
      block->class_index = kUncachedClass;
    }
    block->next = nullptr;
    return block + 1;
  }

  void Release(void* frame) noexcept {
    if (frame == nullptr) {
      return;
    }
    Block* block = static_cast<Block*>(frame) - 1;
    const std::size_t class_index = block->class_index;
    if (class_index >= kClassCount) {
      ::operator delete(block);
      return;
    }

    const std::size_t bytes = ClassBytes(class_index);
    if (count_[class_index] >= kMaxFramesPerClass ||
        cached_bytes_ + bytes > kMaxCachedBytes) {
      ::operator delete(block);
      return;
    }
    block->next = free_[class_index];
    free_[class_index] = block;
    ++count_[class_index];
    cached_bytes_ += bytes;
  }

 private:
  struct alignas(std::max_align_t) Block {
    Block* next = nullptr;
    std::uint16_t class_index = 0;
  };

  static constexpr std::size_t kMinClassBytes = 64;
  static constexpr std::size_t kClassCount = 11;
  static constexpr std::size_t kMaxFramesPerClass = 64;
  static constexpr std::size_t kMaxCachedBytes = 4 * 1024 * 1024;
  static constexpr std::uint16_t kUncachedClass = UINT16_MAX;

  static std::size_t ClassBytes(std::size_t class_index) noexcept {
    return kMinClassBytes << class_index;
  }

  static std::size_t ClassIndex(std::size_t requested) noexcept {
    std::size_t bytes = kMinClassBytes;
    for (std::size_t i = 0; i < kClassCount; ++i, bytes <<= 1) {
      if (requested <= bytes) {
        return i;
      }
    }
    return kClassCount;
  }

  std::array<Block*, kClassCount> free_{};
  std::array<std::size_t, kClassCount> count_{};
  std::size_t cached_bytes_ = 0;
};

inline thread_local CoroutineFramePool g_coroutine_frame_pool;

inline void* AllocateCoroutineFrame(std::size_t size) {
  return g_coroutine_frame_pool.Allocate(size);
}

inline void ReleaseCoroutineFrame(void* frame) noexcept {
  g_coroutine_frame_pool.Release(frame);
}

}  // namespace celer::detail

#endif  // CELER_RUNTIME_COROUTINE_FRAME_POOL_H_
