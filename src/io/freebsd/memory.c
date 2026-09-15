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
#include <sys/domainset.h>
#include <sys/smr.h>
#include <vm/uma.h>

#undef malloc

struct allocation_header {
  size_t size;
  char reserved[64 - sizeof(size_t)];
};

MALLOC_DEFINE(M_TEMP, "celer temporary", "Host-backed kernel allocations");

// Kernel malloc has a different ABI from libc malloc. These symbols become
// local when the port is partially linked; host allocation crosses callbacks.
void* malloc(size_t size, struct malloc_type* type, int flags) {
  (void)type;
  if (size > SIZE_MAX - sizeof(struct allocation_header)) return NULL;
  struct allocation_header* header =
      celer_bsd_host.allocate(size + sizeof(*header), 64);
  void* p = header ? header + 1 : NULL;
  if (header) header->size = size;
  if (p && (flags & M_ZERO)) memset(p, 0, size);
  if (!p && (flags & M_WAITOK)) panic("FreeBSD M_WAITOK allocation exhausted");
  return p;
}

void free(void* p, struct malloc_type* type) {
  (void)type;
  if (p) celer_bsd_host.release((struct allocation_header*)p - 1);
}

void* realloc(void* p, size_t size, struct malloc_type* type, int flags) {
  void* next = malloc(size, type, flags);
  if (next && p) {
    size_t old = ((struct allocation_header*)p - 1)->size;
    memcpy(next, p, MIN(old, size));
    free(p, type);
  }
  return next;
}
char* strdup_flags(const char* text, struct malloc_type* type, int flags) {
  size_t size = strlen(text) + 1;
  char* copy = malloc(size, type, flags);
  if (copy) memcpy(copy, text, size);
  return copy;
}

void* mallocarray(size_t count, size_t size, struct malloc_type* type,
                  int flags) {
  if (size && count > SIZE_MAX / size) return NULL;
  return malloc(count * size, type, flags);
}

void* malloc_domainset(size_t size, struct malloc_type* type,
                       struct domainset* domain, int flags) {
  (void)domain;
  return malloc(size, type, flags);
}

// Type registration serves kernel accounting only. Host allocation does not
// rely on FreeBSD's malloc-type registry or VM bootstrap.
void malloc_init(void* arg) { (void)arg; }
void malloc_uninit(void* arg) { (void)arg; }

struct retired_item {
  struct retired_item* next;
  void* item;
  smr_seq_t sequence;
};

struct uma_zone {
  size_t size, alignment;
  uma_ctor ctor;
  uma_dtor dtor;
  uma_init init;
  uma_fini fini;
  unsigned flags;
  int maximum;
  unsigned current;
  smr_t smr;
  struct retired_item* retired;
  unsigned retire_lock;
};

static void zone_lock(struct uma_zone* z) {
  while (__atomic_exchange_n(&z->retire_lock, 1, __ATOMIC_ACQUIRE))
    celer_bsd_spinwait();
}
static void zone_unlock(struct uma_zone* z) {
  __atomic_store_n(&z->retire_lock, 0, __ATOMIC_RELEASE);
}

uma_zone_t uma_zcreate(const char* name, size_t size, uma_ctor ctor,
                       uma_dtor dtor, uma_init init, uma_fini fini, int align,
                       uint32_t flags) {
  struct uma_zone* z = malloc(sizeof(*z), M_TEMP, M_WAITOK | M_ZERO);
  z->size = size;
  z->alignment = align == UMA_ALIGN_CACHE ? 64 : (size_t)align + 1;
  if (z->alignment < sizeof(void*)) z->alignment = sizeof(void*);
  z->ctor = ctor;
  z->dtor = dtor;
  z->init = init;
  z->fini = fini;
  z->flags = flags;
  if (flags & UMA_ZONE_SMR) z->smr = smr_create(name, 1, 0);
  return z;
}

uma_zone_t uma_zsecond_create(const char* name, uma_ctor ctor, uma_dtor dtor,
                              uma_init init, uma_fini fini, uma_zone_t master) {
  // Separate backing allocations preserve secondary-zone constructor/finalizer
  // semantics. Sharing slabs is a subsequent optimization, not an aliasing
  // rule.
  return uma_zcreate(name, master->size, ctor, dtor, init, fini,
                     master->alignment - 1, master->flags & ~UMA_ZONE_SMR);
}

void* uma_zalloc_arg(uma_zone_t z, void* arg, int flags) {
  unsigned n = __atomic_add_fetch(&z->current, 1, __ATOMIC_RELAXED);
  if (z->maximum && n > (unsigned)z->maximum) {
    __atomic_sub_fetch(&z->current, 1, __ATOMIC_RELAXED);
    return NULL;
  }
  size_t size = (z->flags & UMA_ZONE_PCPU)
                    ? UMA_PCPU_ALLOC_SIZE * celer_bsd_worker_count
                    : z->size;
  void* p = celer_bsd_host.allocate(size, z->alignment);
  if (p && (flags & M_ZERO)) memset(p, 0, size);
  if (p && z->init && z->init(p, z->size, flags)) {
    celer_bsd_host.release(p);
    p = NULL;
  }
  if (p && z->ctor && z->ctor(p, z->size, arg, flags)) {
    if (z->fini) z->fini(p, z->size);
    celer_bsd_host.release(p);
    p = NULL;
  }
  if (!p) {
    __atomic_sub_fetch(&z->current, 1, __ATOMIC_RELAXED);
    if (flags & M_WAITOK) panic("FreeBSD zone allocation exhausted");
  }
  return p;
}

void uma_zfree_arg(uma_zone_t z, void* p, void* arg) {
  if (!p) return;
  if (z->dtor) z->dtor(p, z->size, arg);
  if (z->fini) z->fini(p, z->size);
  celer_bsd_host.release(p);
  __atomic_sub_fetch(&z->current, 1, __ATOMIC_RELAXED);
}

static void zone_reap(uma_zone_t z, bool wait) {
  zone_lock(z);
  struct retired_item** link = &z->retired;
  while (*link) {
    struct retired_item* r = *link;
    if (!smr_poll(z->smr, r->sequence, wait)) {
      link = &r->next;
      continue;
    }
    *link = r->next;
    uma_zfree_arg(z, r->item, NULL);
    free(r, M_TEMP);
  }
  zone_unlock(z);
}

void* uma_zalloc_smr(uma_zone_t z, int flags) {
  zone_reap(z, false);
  return uma_zalloc_arg(z, NULL, flags);
}

void uma_zfree_smr(uma_zone_t z, void* item) {
  // PCB lookup uses SMR: its destructor must wait until pre-existing readers
  // leave, even though its socket owner has already closed the connection.
  struct retired_item* r = malloc(sizeof(*r), M_TEMP, M_WAITOK);
  r->item = item;
  r->sequence = smr_advance(z->smr);
  zone_lock(z);
  r->next = z->retired;
  z->retired = r;
  zone_unlock(z);
}

void* uma_zalloc_pcpu_arg(uma_zone_t z, void* arg, int flags) {
  return uma_zalloc_arg(z, arg, flags);
}
void uma_zfree_pcpu_arg(uma_zone_t z, void* item, void* arg) {
  uma_zfree_arg(z, item, arg);
}
void uma_zdestroy(uma_zone_t z) {
  if (z->smr) {
    zone_reap(z, true);
    smr_destroy(z->smr);
  }
  if (z->current) panic("destroying a live FreeBSD zone");
  free(z, M_TEMP);
}
int uma_zone_set_max(uma_zone_t z, int n) {
  z->maximum = n;
  return n;
}
int uma_zone_get_max(uma_zone_t z) { return z->maximum; }
int uma_zone_get_cur(uma_zone_t z) {
  return __atomic_load_n(&z->current, __ATOMIC_RELAXED);
}
smr_t uma_zone_get_smr(uma_zone_t z) { return z->smr; }
unsigned int uma_get_cache_align_mask(void) { return 63; }
void uma_zone_set_warning(uma_zone_t z, const char* s) {
  (void)z;
  (void)s;
}
void uma_zone_set_maxaction(uma_zone_t z, uma_maxaction_t action) {
  (void)z;
  (void)action;
}
int uma_zone_exhausted(uma_zone_t z) {
  return z->maximum && z->current >= (unsigned)z->maximum;
}
void uma_zone_reclaim(uma_zone_t z, int req) {
  (void)req;
  if (z->smr) zone_reap(z, false);
}
