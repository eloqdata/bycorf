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

#ifndef CELER_FREEBSD_MACHINE_PCPU_H_
#define CELER_FREEBSD_MACHINE_PCPU_H_

// FreeBSD's native pcpu pointer lives in a kernel-reserved register. A Celer
// worker instead carries the context in TLS, including while a coroutine is
// suspended. Network state never follows a coroutine to another worker.
struct pcpu;
struct thread;
extern _Thread_local struct pcpu* celer_bsd_pcpu;
extern _Thread_local struct thread* celer_bsd_curthread;

#if defined(__x86_64__)
// Native PMAP headers parse an inline TLB workaround through PCPU_GET even
// though VM operations are unavailable in the host port. No kernel CPU
// management state is initialized or used by networking.
#define PCPU_MD_FIELDS bool pc_pcid_invlpg_workaround
#else
#define PCPU_MD_FIELDS char celer_unused
#endif
#define curthread celer_bsd_curthread
#define PCPU_GET(member) (celer_bsd_pcpu->pc_##member)
#define PCPU_PTR(member) (&celer_bsd_pcpu->pc_##member)
#define PCPU_SET(member, value) (celer_bsd_pcpu->pc_##member = (value))
#define PCPU_ADD(member, value) (celer_bsd_pcpu->pc_##member += (value))

static inline struct pcpu* get_pcpu(void) { return celer_bsd_pcpu; }

#endif
