// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

struct socket;

namespace bycorf::detail {

// Worker-local socket identities. Close removes the transport immediately,
// but a number cannot be reused until every queued operation/completion drops
// its Pin. This lifetime rule replaces cumulative, never-reused handle IDs;
// no generation counter or cross-worker reference count is needed.
class DpdkSocketTable {
 public:
  class Pin {
   public:
    Pin() = default;
    Pin(const Pin& other) noexcept : table_(other.table_), slot_(other.slot_) {
      if (table_) ++table_->slots_[slot_].pins;
    }
    Pin& operator=(const Pin& other) noexcept {
      if (this != &other) {
        Pin copy(other);
        *this = std::move(copy);
      }
      return *this;
    }
    Pin(Pin&& other) noexcept
        : table_(std::exchange(other.table_, nullptr)), slot_(other.slot_) {}
    Pin& operator=(Pin&& other) noexcept {
      if (this != &other) {
        Reset();
        table_ = std::exchange(other.table_, nullptr);
        slot_ = other.slot_;
      }
      return *this;
    }
    ~Pin() { Reset(); }

    // A closed socket stays invalid even while its number is retained.
    struct socket* get() const noexcept {
      return table_ ? table_->slots_[slot_].socket : nullptr;
    }
    explicit operator bool() const noexcept { return table_ != nullptr; }
    void Reset() noexcept {
      if (auto* table = std::exchange(table_, nullptr)) {
        auto& slot = table->slots_[slot_];
        assert(slot.pins != 0);
        if (--slot.pins == 0 && slot.socket == nullptr) table->Recycle(slot_);
      }
    }

   private:
    friend class DpdkSocketTable;
    Pin(DpdkSocketTable* table, std::uint32_t slot) noexcept
        : table_(table), slot_(slot) {
      ++table_->slots_[slot_].pins;
    }
    DpdkSocketTable* table_ = nullptr;
    std::uint32_t slot_ = 0;
  };

  DpdkSocketTable() = default;
  DpdkSocketTable(const DpdkSocketTable&) = delete;
  DpdkSocketTable& operator=(const DpdkSocketTable&) = delete;
  ~DpdkSocketTable() { AssertDrained(); }

  // Initialization/reinitialization requires drained operations and sockets.
  void Configure(int first, std::uint32_t capacity) {
    AssertDrained();
    assert(first >= 0 &&
           capacity <= std::uint32_t(std::numeric_limits<int>::max() - first));
    first_ = first;
    capacity_ = capacity;
    slots_.clear();
    free_ = kNoSlot;
  }

  // Does not take socket ownership on exhaustion or allocation failure.
  int Insert(struct socket* socket) {
    assert(socket != nullptr);
    std::uint32_t index = free_;
    if (index == kNoSlot) {
      if (slots_.size() == capacity_) return -1;
      index = static_cast<std::uint32_t>(slots_.size());
      slots_.emplace_back();
    } else {
      free_ = slots_[index].next_free;
    }
    auto& slot = slots_[index];
    assert(slot.socket == nullptr && slot.pins == 0);
    slot.socket = socket;
    return first_ + static_cast<int>(index);
  }

  struct socket* Find(int handle) const noexcept {
    const auto index = Index(handle);
    return index < slots_.size() ? slots_[index].socket : nullptr;
  }
  Pin Acquire(int handle) noexcept {
    return Find(handle) ? Pin(this, Index(handle)) : Pin{};
  }

  // Returns the transport to close. Recycling itself never allocates, and
  // live pins defer recycling until after their callbacks have returned.
  struct socket* Close(int handle) noexcept {
    const auto index = Index(handle);
    if (index >= slots_.size()) return nullptr;
    auto& slot = slots_[index];
    auto* socket = std::exchange(slot.socket, nullptr);
    if (socket && slot.pins == 0) Recycle(index);
    return socket;
  }
  template <class CloseSocket>
  void CloseAll(CloseSocket close_socket) {
    for (std::uint32_t i = 0; i < slots_.size(); ++i) {
      if (auto* socket = Close(first_ + static_cast<int>(i)))
        close_socket(socket);
    }
  }

 private:
  static constexpr auto kNoSlot = std::numeric_limits<std::uint32_t>::max();
  struct Slot {
    struct socket* socket = nullptr;
    std::size_t pins = 0;
    std::uint32_t next_free = kNoSlot;
  };
  std::uint32_t Index(int handle) const noexcept {
    return handle < first_ ? kNoSlot
                           : static_cast<std::uint32_t>(handle - first_);
  }
  void Recycle(std::uint32_t index) noexcept {
    slots_[index].next_free = free_;
    free_ = index;
  }
  void AssertDrained() const noexcept {
    for ([[maybe_unused]] const auto& slot : slots_)
      assert(slot.socket == nullptr && slot.pins == 0);
  }
  int first_ = 0;
  std::uint32_t capacity_ = 0;
  std::uint32_t free_ = kNoSlot;
  // Retain metadata at the peak number of live/draining sockets to keep reuse
  // allocation-free. Closed slots hold neither transport nor payload buffers.
  std::vector<Slot> slots_;
};

}  // namespace bycorf::detail
