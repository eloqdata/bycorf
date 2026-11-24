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

#ifndef BYCORF_RUNTIME_COROUTINE_FRAME_POOL_H_
#define BYCORF_RUNTIME_COROUTINE_FRAME_POOL_H_

#include <cstddef>
#include <new>

namespace bycorf::detail {

// Each allocating thread owns a variable-range arena. Release may be called
// on another thread, including after the allocating thread has exited.
class CoroutineFramePool {
 public:
  CoroutineFramePool() = default;
  CoroutineFramePool(const CoroutineFramePool&) = delete;
  CoroutineFramePool& operator=(const CoroutineFramePool&) = delete;
  ~CoroutineFramePool();

  void* Allocate(std::size_t requested);
  void Release(void* frame) noexcept;

 private:
  struct Arena;
  Arena* arena_ = nullptr;
};

inline thread_local CoroutineFramePool g_coroutine_frame_pool;

// Under AddressSanitizer, bypass the pool so every frame is an individual
// allocation: use-after-free and double-destroy of coroutine frames then
// produce precise reports instead of silent pool-recycled corruption.
#if !defined(BYCORF_ASAN) && defined(__SANITIZE_ADDRESS__)
#define BYCORF_ASAN 1
#endif
#if !defined(BYCORF_ASAN) && defined(__has_feature)
#if __has_feature(address_sanitizer)
#define BYCORF_ASAN 1
#endif
#endif
#if defined(BYCORF_ASAN)
inline void* AllocateCoroutineFrame(std::size_t size) {
  return ::operator new(size);
}

inline void ReleaseCoroutineFrame(void* frame) noexcept {
  ::operator delete(frame);
}
#else
inline void* AllocateCoroutineFrame(std::size_t size) {
  return g_coroutine_frame_pool.Allocate(size);
}

inline void ReleaseCoroutineFrame(void* frame) noexcept {
  g_coroutine_frame_pool.Release(frame);
}
#endif

}  // namespace bycorf::detail

#endif  // BYCORF_RUNTIME_COROUTINE_FRAME_POOL_H_
