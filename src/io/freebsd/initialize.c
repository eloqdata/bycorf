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
#include <sys/jail.h>
#include <sys/module.h>
#include <sys/resourcevar.h>
#include <sys/mutex.h>
#include <sys/smp.h>
#include <sys/smr.h>
#include <sys/sx.h>
#include <sys/sysctl.h>
#include <net/vnet.h>
#include <vm/uma.h>

struct celer_bsd_host celer_bsd_host;
unsigned celer_bsd_worker_count;
_Thread_local struct pcpu* celer_bsd_pcpu;
_Thread_local struct thread* celer_bsd_curthread;
// Native lock words encode flags in the low five bits of the thread pointer.
// Kernel UMA normally supplies this alignment; host object layout must do so.
struct thread0_storage thread0_st __aligned(64);
struct proc proc0;
struct pcpu pcpu0;
struct prison prison0;
struct sx allprison_lock;
struct domainset domainset_prefer[MAXMEMDOM];
struct pcpu* cpuid_to_pcpu[MAXCPU];
uintptr_t dpcpu_off[MAXCPU];
cpuset_t all_cpus;
int mp_ncpus;
u_int mp_maxid;
int vm_ndomains = 1;
long physmem = (256L * 1024 * 1024) / PAGE_SIZE;
u_long vm_kmem_size = 256L * 1024 * 1024;
u_long maxphys = 1024 * 1024;
int maxfiles = 65536;
int bootverbose;
int cold = 1;
bool lse_supported;
uma_zone_t pcpu_zone_8;

SYSCTL_ROOT_NODE(CTL_KERN, kern, CTLFLAG_RW, 0, "Kernel");
SYSCTL_ROOT_NODE(CTL_NET, net, CTLFLAG_RW, 0, "Network");
SYSCTL_NODE(_kern, OID_AUTO, features, CTLFLAG_RD, 0, "Features");

static struct {
  struct pcpu pcpu;
  struct thread thread __aligned(64);
  struct proc proc;
  struct ucred cred;
  struct prison prison;
  struct plimit limits;
  struct mtx thread_lock;
}* contexts[MAXCPU];
static bool initialized;

static int initialize_context(unsigned worker) {
  if (contexts[worker]) return EALREADY;
  contexts[worker] =
      malloc(sizeof(*contexts[worker]), M_TEMP, M_WAITOK | M_ZERO);
  __typeof__(contexts[0]) c = contexts[worker];
  celer_bsd_pcpu = &c->pcpu;
  celer_bsd_curthread = worker ? &c->thread : &thread0;
  c->pcpu.pc_curthread = curthread;
  c->pcpu.pc_cpuid = worker;
  // The TLS pcpu overlay uses the generic UMA per-CPU accessor on both
  // architectures. Without this offset, every worker aliases worker 0's
  // counters and SMR reader state even though its VNET is independent.
  c->pcpu.pc_zpcpu_offset = zpcpu_offset_cpu(worker);
  cpuid_to_pcpu[worker] = &c->pcpu;
  size_t bytes = DPCPU_BYTES;
  void* image = malloc(bytes, M_TEMP, M_WAITOK);
  memcpy(image, (void*)DPCPU_START, bytes);
  c->pcpu.pc_dynamic = dpcpu_off[worker] = (uintptr_t)image - DPCPU_START;
  curthread->td_proc = &c->proc;
  curthread->td_ucred = &c->cred;
  curthread->td_tid = worker + 1;
  curthread->td_oncpu = worker;
  c->proc.p_ucred = &c->cred;
  c->proc.p_limit = curthread->td_limit = &c->limits;
  for (unsigned i = 0; i < RLIM_NLIMITS; ++i)
    c->limits.pl_rlimit[i] = (struct rlimit){RLIM_INFINITY, RLIM_INFINITY};
  c->cred.cr_ref = 1;
  c->cred.cr_prison = worker ? &c->prison : &prison0;
  mtx_init(&c->thread_lock, "celer thread", NULL, MTX_DEF | MTX_RECURSE);
  curthread->td_lock = &c->thread_lock;
  return 0;
}

int celer_bsd_initialize(const struct celer_bsd_host* host, unsigned workers) {
  if (initialized) return EALREADY;
  if (!host || workers == 0 || workers > MAXCPU) return EINVAL;
  celer_bsd_host = *host;
  celer_bsd_worker_count = workers;
  mp_ncpus = workers;
  mp_maxid = workers - 1;
  for (unsigned i = 0; i < workers; ++i) CPU_SET(i, &all_cpus);
  initialize_context(0);
  celer_bsd_clock_update();
  mtx_init(&prison0.pr_mtx, "root prison", NULL, MTX_DEF | MTX_RECURSE);
  sx_init(&allprison_lock, "prison list");
  prison0.pr_ref = 1;
  pcpu_zone_8 = uma_zcreate("celer counters", sizeof(uint64_t), NULL, NULL,
                            NULL, NULL, UMA_ALIGN_CACHE, UMA_ZONE_PCPU);
  smr_init();

  SET_DECLARE(sysinit_set, struct sysinit);
  const size_t count = SET_COUNT(sysinit_set);
  struct sysinit** ordered = malloc(count * sizeof(*ordered), M_TEMP, M_WAITOK);
  memcpy(ordered, SET_BEGIN(sysinit_set), count * sizeof(*ordered));
  // A stable order matches the kernel's subsystem/priority contract without
  // relying on host constructors or on link order for distinct priorities.
  for (size_t i = 1; i < count; ++i) {
    struct sysinit* s = ordered[i];
    size_t j = i;
    while (j && (ordered[j - 1]->subsystem > s->subsystem ||
                 (ordered[j - 1]->subsystem == s->subsystem &&
                  ordered[j - 1]->order > s->order))) {
      ordered[j] = ordered[j - 1];
      --j;
    }
    ordered[j] = s;
  }
  for (size_t i = 0; i < count; ++i) {
    struct sysinit* s = ordered[i];
    if (s->subsystem == SI_SUB_DUMMY) continue;
    s->func(s->udata);
  }
  free(ordered, M_TEMP);
  curthread->td_vnet = vnet0;
  cold = 0;
  initialized = true;
  return 0;
}

int celer_bsd_attach_worker(unsigned worker) {
  if (!initialized || worker == 0 || worker >= celer_bsd_worker_count)
    return EINVAL;
  int error = initialize_context(worker);
  if (error) return error;
  celer_bsd_clock_update();
  // Runtime serializes attachment. Constructors touch global registries while
  // allocating each worker's independent VNET and credentials.
  struct prison* pr = curthread->td_ucred->cr_prison;
  mtx_init(&pr->pr_mtx, "worker prison", NULL, MTX_DEF | MTX_RECURSE);
  pr->pr_ref = 1;
  pr->pr_flags = PR_VNET;
  curthread->td_vnet = pr->pr_vnet = vnet_alloc();
  return 0;
}

void module_register_init(const void* arg) {
  const moduledata_t* module = arg;
  // Some native modules only declare dependencies and have no event handler.
  if (!module->evhand) return;
  int error = module->evhand(NULL, MOD_LOAD, module->priv);
  if (error)
    panic("FreeBSD module %s initialization failed: %d", module->name, error);
}
void celer_bsd_poll(void) {
  celer_bsd_clock_update();
  celer_bsd_callout_poll();
  celer_bsd_tasks_poll();
  celer_bsd_epoch_poll();
}
