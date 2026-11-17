// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#include "bycorf/base/local_shared_ptr.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <thread>

namespace bycorf {
namespace {
struct Tracked {
  explicit Tracked(int& destroyed) : destroyed(destroyed) {}
  ~Tracked() { ++destroyed; }
  int& destroyed;
};

TEST(LocalSharedPtr, StrongCopiesMovesAndConstConversion) {
  int destroyed = 0;
  auto first = MakeLocalShared<Tracked>(destroyed);
  auto second = first;
  LocalSharedPtr<const Tracked> view = first;
  EXPECT_EQ(first.use_count(), 3U);
  auto moved = std::move(second);
  EXPECT_FALSE(second);
  EXPECT_EQ(moved.get(), first.get());
  EXPECT_EQ(first.use_count(), 3U);
  first.reset();
  moved = nullptr;
  EXPECT_EQ(destroyed, 0);
  EXPECT_EQ(view.use_count(), 1U);
  view.reset();
  EXPECT_EQ(destroyed, 1);
}

TEST(LocalSharedPtr, WeakLockDoesNotResurrectDestroyedObject) {
  int destroyed = 0;
  auto first = MakeLocalShared<Tracked>(destroyed);
  LocalWeakPtr<Tracked> weak = first;
  auto copy = weak;
  auto moved = std::move(copy);
  EXPECT_TRUE(copy.expired());
  auto retained = moved.lock();
  first.reset();
  EXPECT_EQ(destroyed, 0);
  EXPECT_FALSE(weak.expired());
  retained.reset();
  EXPECT_EQ(destroyed, 1);
  EXPECT_TRUE(weak.expired());
  EXPECT_FALSE(moved.lock());
  weak.reset();
  moved.reset();
  EXPECT_EQ(destroyed, 1);
}

struct SelfObserved {
  LocalWeakPtr<SelfObserved> self;
  bool& destroyed;
  explicit SelfObserved(bool& destroyed) : destroyed(destroyed) {}
  ~SelfObserved() {
    EXPECT_TRUE(self.expired());
    EXPECT_FALSE(self.lock());
    // The implicit weak reference must protect the control while destruction
    // drops the last explicit weak reference.
    self.reset();
    destroyed = true;
  }
};
TEST(LocalSharedPtr, SelfWeakReferenceCanReleaseDuringDestruction) {
  bool destroyed = false;
  auto state = MakeLocalShared<SelfObserved>(destroyed);
  state->self = state;
  state.reset();
  EXPECT_TRUE(destroyed);
}

struct Incomplete;
struct OpaqueHandle {
  LocalSharedPtr<Incomplete> strong;
  LocalWeakPtr<Incomplete> weak;
  ~OpaqueHandle() = default;
};
// Instantiate destruction while the pointee is still incomplete, as in the
// private worker facade and public RC handles.
void DestroyOpaque(OpaqueHandle* handle) { delete handle; }
struct Incomplete {
  int& destroyed;
  explicit Incomplete(int& destroyed) : destroyed(destroyed) {}
  ~Incomplete() { ++destroyed; }
};
TEST(LocalSharedPtr, IncompletePointeeDestruction) {
  int destroyed = 0;
  auto* handle = new OpaqueHandle;
  handle->strong = MakeLocalShared<Incomplete>(destroyed);
  handle->weak = handle->strong;
  DestroyOpaque(handle);
  EXPECT_EQ(destroyed, 1);
}

struct Throwing {
  Tracked member;
  explicit Throwing(int& destroyed) : member(destroyed) {
    throw std::runtime_error("constructor failed");
  }
};
TEST(LocalSharedPtr, ConstructionFailureUnwindsMembers) {
  int destroyed = 0;
  EXPECT_THROW(MakeLocalShared<Throwing>(destroyed), std::runtime_error);
  EXPECT_EQ(destroyed, 1);
}

#ifndef NDEBUG
TEST(LocalSharedPtrDeathTest, ForeignThreadCopyAndReleaseAreRejected) {
  auto state = MakeLocalShared<int>(7);
  EXPECT_DEATH(
      {
        std::thread other([&] { auto copy = state; });
        other.join();
      },
      "outside its owner thread");
  EXPECT_DEATH(
      {
        std::thread other([&] { state.reset(); });
        other.join();
      },
      "outside its owner thread");
}
TEST(LocalSharedPtrDeathTest, ForeignThreadWeakLockAndReleaseAreRejected) {
  auto state = MakeLocalShared<int>(7);
  LocalWeakPtr<int> weak = state;
  EXPECT_DEATH(
      {
        std::thread other([&] { auto retained = weak.lock(); });
        other.join();
      },
      "outside its owner thread");
  state.reset();
  EXPECT_DEATH(
      {
        std::thread other([&] { weak.reset(); });
        other.join();
      },
      "outside its owner thread");
}
#endif
}  // namespace
}  // namespace bycorf
