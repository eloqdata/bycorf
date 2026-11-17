// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>
#ifndef NDEBUG
#include <thread>
#endif

namespace bycorf {
namespace local_shared_detail {
// Same allocating-thread contract as Lavik's LocalSharedPtr. The erased
// control also supports incomplete types and weak references for RC handles.
struct Control {
  std::size_t strong = 1;
  // One implicit weak reference belongs to the entire strong-owner cohort.
  // It keeps the control alive while T's destructor releases its own weak refs.
  std::size_t weak = 1;
  void (*destroy)(Control*) noexcept;
  void (*deallocate)(Control*) noexcept;
#ifndef NDEBUG
  const std::thread::id owner = std::this_thread::get_id();
#endif
  void AssertOwner() const noexcept {
#ifndef NDEBUG
    assert(owner == std::this_thread::get_id() &&
           "LocalSharedPtr used outside its owner thread");
#endif
  }
  void RetainStrong() noexcept {
    AssertOwner();
    assert(strong && strong != std::numeric_limits<std::size_t>::max());
    ++strong;
  }
  void RetainWeak() noexcept {
    AssertOwner();
    assert(weak && weak != std::numeric_limits<std::size_t>::max());
    ++weak;
  }
  void ReleaseWeak() noexcept {
    AssertOwner();
    if (--weak == 0) deallocate(this);
  }
  void ReleaseStrong() noexcept {
    AssertOwner();
    if (--strong == 0) {
      destroy(this);
      ReleaseWeak();
    }
  }
};

template <typename T>
struct Allocation final : Control {
  // Object destruction and allocation release happen at different times when
  // weak observers survive. Keeping T in a union avoids double destruction.
  union {
    T value;
  };
  template <typename... Args>
  explicit Allocation(Args&&... args)
      : Control{.destroy =
                    [](Control* c) noexcept {
                      std::destroy_at(&static_cast<Allocation*>(c)->value);
                    },
                .deallocate =
                    [](Control* c) noexcept {
                      delete static_cast<Allocation*>(c);
                    }},
        value(std::forward<Args>(args)...) {}
  ~Allocation() {}
};
}  // namespace local_shared_detail

template <typename T>
class LocalWeakPtr;
template <typename T>
class LocalSharedPtr;
template <typename T, typename... Args>
LocalSharedPtr<T> MakeLocalShared(Args&&... args);

// Non-atomic shared ownership confined to the allocating thread, including
// access, copying, moving and destruction, even after the worker shuts down.
// Coroutine frames carrying these handles must also be destroyed there.
// Debug builds check thread affinity before accessing the control block.
// Supports incomplete types and T -> const T; no aliasing, raw adoption,
// arrays, custom deleters or cross-thread handoff. Use MakeLocalShared.
template <typename T>
class LocalSharedPtr {
  static_assert(std::is_object_v<T> && !std::is_array_v<T> &&
                !std::is_volatile_v<T>);
  template <typename U>
  static constexpr bool kCompatible =
      std::is_same_v<std::remove_const_t<T>, std::remove_const_t<U>> &&
      std::is_convertible_v<U*, T*>;

 public:
  constexpr LocalSharedPtr() noexcept = default;
  constexpr LocalSharedPtr(std::nullptr_t) noexcept {}
  LocalSharedPtr(const LocalSharedPtr& other) noexcept
      : control_(other.CheckedControl()), value_(other.value_) {
    if (control_) control_->RetainStrong();
  }
  template <typename U>
    requires kCompatible<U>
  LocalSharedPtr(const LocalSharedPtr<U>& other) noexcept
      : control_(other.CheckedControl()), value_(other.value_) {
    if (control_) control_->RetainStrong();
  }
  LocalSharedPtr(LocalSharedPtr&& other) noexcept
      : control_(other.CheckedControl()), value_(other.value_) {
    other.control_ = nullptr;
    other.value_ = nullptr;
  }
  ~LocalSharedPtr() { reset(); }
  LocalSharedPtr& operator=(LocalSharedPtr other) noexcept {
    swap(other);
    return *this;
  }
  void reset() noexcept {
    auto* control = CheckedControl();
    control_ = nullptr;
    value_ = nullptr;
    if (control) control->ReleaseStrong();
  }
  void swap(LocalSharedPtr& other) noexcept {
    (void)CheckedControl();
    (void)other.CheckedControl();
    std::swap(control_, other.control_);
    std::swap(value_, other.value_);
  }
  T* get() const noexcept {
    (void)CheckedControl();
    return value_;
  }
  T* operator->() const noexcept {
    assert(value_);
    return get();
  }
  T& operator*() const noexcept { return *operator->(); }
  explicit operator bool() const noexcept { return get() != nullptr; }
  std::size_t use_count() const noexcept {
    auto* control = CheckedControl();
    return control ? control->strong : 0;
  }
  friend bool operator==(const LocalSharedPtr& a,
                         const LocalSharedPtr& b) noexcept {
    return a.get() == b.get();
  }
  friend bool operator==(const LocalSharedPtr& a, std::nullptr_t) noexcept {
    return !a;
  }

 private:
  template <typename U>
  friend class LocalSharedPtr;
  template <typename U>
  friend class LocalWeakPtr;
  template <typename U, typename... Args>
  friend LocalSharedPtr<U> MakeLocalShared(Args&&...);
  LocalSharedPtr(local_shared_detail::Control* control, T* value) noexcept
      : control_(control), value_(value) {}
  local_shared_detail::Control* CheckedControl() const noexcept {
    if (control_) control_->AssertOwner();
    return control_;
  }
  local_shared_detail::Control* control_ = nullptr;
  T* value_ = nullptr;
};

// Non-owning observation with the same thread confinement as LocalSharedPtr.
// lock() retains a live object; after the last strong release it returns empty.
// Weak handles keep only the allocation/control alive, never the object.
template <typename T>
class LocalWeakPtr {
 public:
  constexpr LocalWeakPtr() noexcept = default;
  LocalWeakPtr(const LocalSharedPtr<T>& strong) noexcept
      : control_(strong.CheckedControl()), value_(strong.value_) {
    if (control_) control_->RetainWeak();
  }
  LocalWeakPtr(const LocalWeakPtr& other) noexcept
      : control_(other.CheckedControl()), value_(other.value_) {
    if (control_) control_->RetainWeak();
  }
  LocalWeakPtr(LocalWeakPtr&& other) noexcept
      : control_(other.CheckedControl()), value_(other.value_) {
    other.control_ = nullptr;
    other.value_ = nullptr;
  }
  ~LocalWeakPtr() { reset(); }
  LocalWeakPtr& operator=(LocalWeakPtr other) noexcept {
    swap(other);
    return *this;
  }
  void swap(LocalWeakPtr& other) noexcept {
    (void)CheckedControl();
    (void)other.CheckedControl();
    std::swap(control_, other.control_);
    std::swap(value_, other.value_);
  }
  void reset() noexcept {
    auto* control = CheckedControl();
    control_ = nullptr;
    value_ = nullptr;
    if (control) control->ReleaseWeak();
  }
  bool expired() const noexcept {
    auto* control = CheckedControl();
    return !control || control->strong == 0;
  }
  LocalSharedPtr<T> lock() const noexcept {
    if (expired()) return {};
    control_->RetainStrong();
    return LocalSharedPtr<T>(control_, value_);
  }

 private:
  local_shared_detail::Control* CheckedControl() const noexcept {
    if (control_) control_->AssertOwner();
    return control_;
  }
  local_shared_detail::Control* control_ = nullptr;
  T* value_ = nullptr;
};

// Constructs object and control in one allocation. Constructor failure frees
// that allocation; T's destructor must not throw. The last strong release
// destroys T and the last weak release frees the allocation.
template <typename T, typename... Args>
LocalSharedPtr<T> MakeLocalShared(Args&&... args) {
  auto* block = new local_shared_detail::Allocation<std::remove_const_t<T>>(
      std::forward<Args>(args)...);
  return LocalSharedPtr<T>(block, std::addressof(block->value));
}
}  // namespace bycorf
