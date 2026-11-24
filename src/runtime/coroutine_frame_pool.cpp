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

#include "bycorf/runtime/coroutine_frame_pool.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <limits>

#include "bycorf/base/tlsf_range_allocator.h"

namespace bycorf::detail {

struct CoroutineFramePool::Arena {
  static constexpr std::size_t kBytes = 4 * 1024 * 1024;
  static constexpr std::size_t kUnit = 64;
  static constexpr std::uint32_t kMaxCachedUnits = 1024 / kUnit;
  static constexpr std::uint32_t kCacheBudgetUnits = 32 * 1024 / kUnit;

  struct alignas(std::max_align_t) Block {
    union {
      Arena* owner;
      Block* next;
    };
    std::uint32_t units;
  };
  static_assert(kUnit % alignof(Block) == 0);
  static inline Block closed{};
  alignas(Block) std::byte storage[kBytes];
  TlsfRangeAllocator::Metadata metadata[kBytes / kUnit];
  TlsfRangeAllocator ranges;
  // The owner keeps these blocks reserved in ranges for exact-size reuse.
  // They have no live frames and do not keep a detached arena alive.
  Block* recent[kMaxCachedUnits]{};
  std::uint32_t cached_units = 0;
  std::uint32_t live = 0;
  std::atomic<Block*> returned{nullptr};
  std::atomic<std::uint32_t> detached_refs{0};

  Arena() {
    static_assert(kBytes / kUnit <= TlsfRangeAllocator::kMaxCapacity);
    if (!ranges.Init(metadata)) std::abort();
  }

  void ReturnRange(Block* b) noexcept {
    const auto offset = (reinterpret_cast<std::uintptr_t>(b) -
                         reinterpret_cast<std::uintptr_t>(storage)) /
                        kUnit;
    ranges.Free({static_cast<std::uint32_t>(offset), b->units});
  }

  void FlushCache() noexcept {
    for (Block*& head : recent) {
      while (head) {
        Block* block = head;
        head = block->next;
        ReturnRange(block);
      }
    }
    cached_units = 0;
  }

  void Free(Block* b) noexcept {
    --live;
    if (b->units <= kMaxCachedUnits &&
        cached_units + b->units <= kCacheBudgetUnits) {
      cached_units += b->units;
      b->next = recent[b->units - 1];
      recent[b->units - 1] = b;
      return;
    }
    ReturnRange(b);
  }

  void Drain() noexcept {
    if (!returned.load(std::memory_order_relaxed)) return;
    Block* b = returned.exchange(nullptr, std::memory_order_acquire);
    while (b) {
      Block* next = b->next;
      Free(b);
      b = next;
    }
  }

  // Only the owner updates live and the range allocator. At owner exit,
  // transfer lifetime accounting to the outstanding frames and one teardown
  // reference before publishing closed. Late returners only drop a reference.
  void Detach() noexcept {
    detached_refs.store(live + 1, std::memory_order_relaxed);
    auto* b = returned.exchange(&closed, std::memory_order_acq_rel);
    std::uint32_t released = 1;
    while (b) {
      ++released;
      b = b->next;
    }
    DropDetached(released);
  }

  void DropDetached(std::uint32_t count) noexcept {
    if (detached_refs.fetch_sub(count, std::memory_order_acq_rel) == count)
      delete this;
  }

  void ReturnRemote(Block* b) noexcept {
    auto* head = returned.load(std::memory_order_acquire);
    while (head != &closed) {
      b->next = head;
      if (returned.compare_exchange_weak(head, b, std::memory_order_acq_rel,
                                         std::memory_order_acquire))
        return;
    }
    DropDetached(1);
  }
};

CoroutineFramePool::~CoroutineFramePool() {
  if (arena_) arena_->Detach();
}

void* CoroutineFramePool::Allocate(std::size_t requested) {
  using Block = Arena::Block;
  if (requested > std::numeric_limits<std::size_t>::max() - sizeof(Block))
    throw std::bad_alloc();
  const auto bytes = sizeof(Block) + requested;
  if (bytes <= Arena::kBytes) {
    if (!arena_) arena_ = new (std::nothrow) Arena;
    if (arena_) {
      arena_->Drain();
      const auto units =
          static_cast<std::uint32_t>((bytes + Arena::kUnit - 1) / Arena::kUnit);
      if (units <= Arena::kMaxCachedUnits) {
        auto*& slot = arena_->recent[units - 1];
        if (auto* b = slot) {
          slot = b->next;
          arena_->cached_units -= units;
          b->owner = arena_;
          ++arena_->live;
          return b + 1;
        }
      }
      auto range = arena_->ranges.Allocate(units);
      if (!range && arena_->cached_units != 0) {
        // Return every cached range so adjacent blocks can satisfy this
        // request.
        arena_->FlushCache();
        range = arena_->ranges.Allocate(units);
      }
      if (range) {
        void* start = arena_->storage + range.offset * Arena::kUnit;
        auto* block = ::new (start) Block{{arena_}, units};
        ++arena_->live;
        return block + 1;
      }
    }
  }
  // Frame creation is synchronous. Oversize, fragmentation and exhaustion
  // use a standalone allocation, which can be freed by any thread.
  auto* block = ::new (::operator new(bytes)) Block{{nullptr}, 0};
  return block + 1;
}

void CoroutineFramePool::Release(void* frame) noexcept {
  if (!frame) return;
  auto* block = static_cast<Arena::Block*>(frame) - 1;
  Arena* owner = block->owner;
  if (!owner) {
    ::operator delete(block);
  } else if (owner == arena_) {
    owner->Free(block);
  } else {
    owner->ReturnRemote(block);
  }
}

}  // namespace bycorf::detail
