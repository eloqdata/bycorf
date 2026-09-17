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

#include "internal.h"
#include <sys/mutex.h>
#include <sys/rwlock.h>
#include <sys/sx.h>
#include <sys/rmlock.h>

// Retain FreeBSD's inline lock encoding. Global initialization/registries are
// actually shared, so even the prototype must not turn kernel locks into
// no-ops.
enum { BYCORF_MTX = 1, BYCORF_RW = 2, BYCORF_SX = 3 };

static void init_lock(struct lock_object* lo, const char* name, int kind,
                      int recurse) {
  memset(lo, 0, sizeof(*lo));
  lo->lo_name = name;
  lo->lo_flags =
      LO_INITIALIZED | (kind << LO_CLASSSHIFT) | (recurse ? LO_RECURSABLE : 0);
}
static uintptr_t owner(void) {
  if (!curthread) panic("FreeBSD lock used outside a worker context");
  return (uintptr_t)curthread;
}
static bool cas(volatile uintptr_t* p, uintptr_t old, uintptr_t value) {
  return __atomic_compare_exchange_n(p, &old, value, false, __ATOMIC_ACQUIRE,
                                     __ATOMIC_RELAXED);
}
static void spin(void) { bycorf_bsd_spinwait(); }

void _mtx_init(volatile uintptr_t* p, const char* name, const char* type,
               int opts) {
  (void)type;
  struct mtx* m = __containerof(p, struct mtx, mtx_lock);
  init_lock(&m->lock_object, name, BYCORF_MTX, opts & MTX_RECURSE);
  *p = MTX_UNOWNED;
}
void _mtx_destroy(volatile uintptr_t* p) {
  if (*p != MTX_UNOWNED) panic("destroying a locked mutex");
  __containerof(p, struct mtx, mtx_lock)->lock_object.lo_flags = 0;
}
void mtx_sysinit(const void* arg) {
  const struct mtx_args* a = arg;
  _mtx_init(&((struct mtx*)a->ma_mtx)->mtx_lock, a->ma_desc, NULL, a->ma_opts);
}
int _mtx_trylock_flags_(volatile uintptr_t* p, int opts, const char* file,
                        int line) {
  (void)opts;
  (void)file;
  (void)line;
  if (cas(p, MTX_UNOWNED, owner())) return 1;
  struct mtx* m = __containerof(p, struct mtx, mtx_lock);
  if ((*p & ~MTX_FLAGMASK) == owner() &&
      (m->lock_object.lo_flags & LO_RECURSABLE)) {
    ++m->mtx_recurse;
    *p |= MTX_RECURSED;
    return 1;
  }
  return 0;
}
void __mtx_lock_sleep(volatile uintptr_t* p, uintptr_t value) {
  (void)value;
  if ((*p & ~MTX_FLAGMASK) == owner() &&
      !(__containerof(p, struct mtx, mtx_lock)->lock_object.lo_flags &
        LO_RECURSABLE))
    panic("recursive acquisition of nonrecursive mutex");
  while (!_mtx_trylock_flags_(p, 0, NULL, 0)) spin();
}
void __mtx_unlock_sleep(volatile uintptr_t* p, uintptr_t value) {
  (void)value;
  struct mtx* m = __containerof(p, struct mtx, mtx_lock);
  if ((*p & ~MTX_FLAGMASK) != owner())
    panic("mutex released by another worker");
  if (m->mtx_recurse) {
    if (--m->mtx_recurse == 0) *p &= ~MTX_RECURSED;
  } else
    __atomic_store_n(p, MTX_UNOWNED, __ATOMIC_RELEASE);
}

static int try_writer(volatile uintptr_t* p, struct lock_object* lo) {
  if (cas(p, RW_UNLOCKED, owner())) return 1;
  if (RW_OWNER(*p) == owner() && !(*p & RW_LOCK_READ) &&
      (lo->lo_flags & LO_RECURSABLE)) {
    ++lo->lo_data;
    *p |= RW_LOCK_WRITER_RECURSED;
    return 1;
  }
  return 0;
}
static void release_writer(volatile uintptr_t* p, struct lock_object* lo) {
  if ((*p & RW_LOCK_READ) || RW_OWNER(*p) != owner())
    panic("write lock owner mismatch");
  if (lo->lo_data) {
    if (--lo->lo_data == 0) *p &= ~RW_LOCK_WRITER_RECURSED;
  } else
    __atomic_store_n(p, RW_UNLOCKED, __ATOMIC_RELEASE);
}
static int try_reader(volatile uintptr_t* p) {
  uintptr_t v = __atomic_load_n(p, __ATOMIC_RELAXED);
  while (v & RW_LOCK_READ) {
    if (__atomic_compare_exchange_n(p, &v, v + RW_ONE_READER, false,
                                    __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
      return 1;
  }
  return 0;
}
static void release_reader(volatile uintptr_t* p) {
  uintptr_t v = __atomic_load_n(p, __ATOMIC_RELAXED);
  if (!(v & RW_LOCK_READ) || RW_READERS(v) == 0)
    panic("unbalanced read lock %p value=%jx owner=%jx", p, (uintmax_t)v,
          (uintmax_t)owner());
  __atomic_fetch_sub(p, RW_ONE_READER, __ATOMIC_RELEASE);
}

void _rw_init_flags(volatile uintptr_t* p, const char* name, int opts) {
  init_lock(&__containerof(p, struct rwlock, rw_lock)->lock_object, name,
            BYCORF_RW, opts & RW_RECURSE);
  *p = RW_UNLOCKED;
}
void _rw_destroy(volatile uintptr_t* p) {
  if (*p != RW_UNLOCKED) panic("destroying a live rwlock");
}
void rw_sysinit(const void* arg) {
  const struct rw_args* a = arg;
  _rw_init_flags(&((struct rwlock*)a->ra_rw)->rw_lock, a->ra_desc, a->ra_flags);
}
int _rw_wowned(const volatile uintptr_t* p) {
  return !(*p & RW_LOCK_READ) && RW_OWNER(*p) == owner();
}
void __rw_rlock_int(struct rwlock* r) {
  while (!try_reader(&r->rw_lock)) spin();
}
void _rw_runlock_cookie_int(struct rwlock* r) { release_reader(&r->rw_lock); }
int __rw_try_rlock(volatile uintptr_t* p, const char* f, int l) {
  (void)f;
  (void)l;
  return try_reader(p);
}
int __rw_try_wlock(volatile uintptr_t* p, const char* f, int l) {
  (void)f;
  (void)l;
  return try_writer(p, &__containerof(p, struct rwlock, rw_lock)->lock_object);
}
void __rw_wlock_hard(volatile uintptr_t* p, uintptr_t v) {
  (void)v;
  while (!try_writer(p, &__containerof(p, struct rwlock, rw_lock)->lock_object))
    spin();
}
void __rw_wunlock_hard(volatile uintptr_t* p, uintptr_t v) {
  (void)v;
  release_writer(p, &__containerof(p, struct rwlock, rw_lock)->lock_object);
}
int __rw_try_upgrade_int(struct rwlock* r) {
  return cas(&r->rw_lock, RW_READERS_LOCK(1), owner());
}
void __rw_downgrade_int(struct rwlock* r) {
  if (!_rw_wowned(&r->rw_lock)) panic("invalid downgrade");
  __atomic_store_n(&r->rw_lock, RW_READERS_LOCK(1), __ATOMIC_RELEASE);
}

_Static_assert(SX_LOCK_UNLOCKED == RW_UNLOCKED &&
                   SX_LOCK_RECURSED == RW_LOCK_WRITER_RECURSED &&
                   SX_ONE_SHARER == RW_ONE_READER,
               "shared lock encoding changed");
void sx_init_flags(struct sx* s, const char* name, int opts) {
  init_lock(&s->lock_object, name, BYCORF_SX, opts & SX_RECURSE);
  s->sx_lock = SX_LOCK_UNLOCKED;
}
void sx_destroy(struct sx* s) {
  if (s->sx_lock != SX_LOCK_UNLOCKED) panic("destroying a live sx lock");
}
void sx_sysinit(const void* arg) {
  const struct sx_args* a = arg;
  sx_init_flags(a->sa_sx, a->sa_desc, a->sa_flags);
}
int _sx_slock_int(struct sx* s, int opts) {
  (void)opts;
  while (!try_reader(&s->sx_lock)) spin();
  return 0;
}
void _sx_sunlock_int(struct sx* s) { release_reader(&s->sx_lock); }
int _sx_xlock_hard(struct sx* s, uintptr_t v, int opts) {
  (void)v;
  (void)opts;
  while (!try_writer(&s->sx_lock, &s->lock_object)) spin();
  return 0;
}
void _sx_xunlock_hard(struct sx* s, uintptr_t v) {
  (void)v;
  release_writer(&s->sx_lock, &s->lock_object);
}
int sx_try_xlock_int(struct sx* s) {
  return try_writer(&s->sx_lock, &s->lock_object);
}

// Read-mostly locks use a real recursive mutex in this initial port. The
// shared registry is short-lived; queue and connection hot paths stay local.
void rm_init_flags(struct rmlock* r, const char* name, int opts) {
  (void)opts;
  mtx_init(&r->rm_lock_mtx, name, NULL, MTX_DEF | MTX_RECURSE);
}
void rm_destroy(struct rmlock* r) { mtx_destroy(&r->rm_lock_mtx); }
void _rm_wlock(struct rmlock* r) { mtx_lock(&r->rm_lock_mtx); }
void _rm_wunlock(struct rmlock* r) { mtx_unlock(&r->rm_lock_mtx); }
int _rm_rlock(struct rmlock* r, struct rm_priotracker* t, int attempt) {
  (void)t;
  if (attempt) return mtx_trylock(&r->rm_lock_mtx);
  mtx_lock(&r->rm_lock_mtx);
  return 1;
}
void _rm_runlock(struct rmlock* r, struct rm_priotracker* t) {
  (void)t;
  mtx_unlock(&r->rm_lock_mtx);
}

void bycorf_bsd_lock(struct lock_object* lo) {
  if (!lo) return;
  switch (LO_CLASSINDEX(lo)) {
    case BYCORF_MTX:
      mtx_lock((struct mtx*)lo);
      break;
    case BYCORF_RW:
      rw_wlock((struct rwlock*)lo);
      break;
    case BYCORF_SX:
      sx_xlock((struct sx*)lo);
      break;
    default:
      panic("unsupported FreeBSD callout lock class");
  }
}
void bycorf_bsd_unlock(struct lock_object* lo) {
  if (!lo) return;
  switch (LO_CLASSINDEX(lo)) {
    case BYCORF_MTX:
      mtx_unlock((struct mtx*)lo);
      break;
    case BYCORF_RW:
      rw_wunlock((struct rwlock*)lo);
      break;
    case BYCORF_SX:
      sx_xunlock((struct sx*)lo);
      break;
    default:
      panic("unsupported FreeBSD callout lock class");
  }
}
