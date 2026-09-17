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

#ifndef BYCORF_FREEBSD_INTERNAL_H_
#define BYCORF_FREEBSD_INTERNAL_H_

#include <sys/param.h>
#include <sys/limits.h>
#include <sys/systm.h>
#include <sys/proc.h>
#include <sys/malloc.h>
#include <sys/kernel.h>
#include <sys/lock.h>
#include "abi.h"

extern struct bycorf_bsd_host bycorf_bsd_host;
extern unsigned bycorf_bsd_worker_count;
void bycorf_bsd_clock_update(void);
void bycorf_bsd_callout_poll(void);
void bycorf_bsd_tasks_poll(void);
bool bycorf_bsd_tasks_pending(void);
void bycorf_bsd_epoch_poll(void);
void bycorf_bsd_lock(struct lock_object* lock);
void bycorf_bsd_unlock(struct lock_object* lock);

// Only a userspace scheduling hint: never enter a kernel idle/interrupt path
// while holding a shared registry or allocator lock.
static inline void bycorf_bsd_spinwait(void) {
#if defined(__aarch64__)
  __asm__ volatile("yield" ::: "memory");
#elif defined(__x86_64__)
  __asm__ volatile("pause" ::: "memory");
#else
#error "Unsupported FreeBSD host architecture"
#endif
}

#endif
