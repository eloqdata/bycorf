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

#ifndef BYCORF_FREEBSD_MACHINE_COUNTER_H_
#define BYCORF_FREEBSD_MACHINE_COUNTER_H_
#include <machine/atomic.h>
#include <sys/pcpu.h>

#define EARLY_COUNTER (&pcpu0.pc_early_dummy_counter)
#define counter_enter() \
  do {                  \
  } while (0)
#define counter_exit() \
  do {                 \
  } while (0)
#define counter_u64_add_protected(c, n) counter_u64_add(c, n)

static inline void counter_u64_add(counter_u64_t c, int64_t n) {
  atomic_add_64((uint64_t*)zpcpu_get(c), n);
}

#ifdef IN_SUBR_COUNTER_C
// Counter reset must not execute FreeBSD's SMP rendezvous/IPI path on Linux.
// Each cell is atomic, including when its owning worker updates it
// concurrently.
static inline uint64_t counter_u64_fetch_inline(counter_u64_t c) {
  uint64_t sum = 0;
  for (unsigned i = 0; i <= mp_maxid; ++i)
    sum += __atomic_load_n((uint64_t*)zpcpu_get_cpu(c, i), __ATOMIC_RELAXED);
  return sum;
}
static inline void counter_u64_zero_inline(counter_u64_t c) {
  for (unsigned i = 0; i <= mp_maxid; ++i)
    __atomic_store_n((uint64_t*)zpcpu_get_cpu(c, i), 0, __ATOMIC_RELAXED);
}
#endif

#endif
