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

#ifndef BYCORF_FREEBSD_MACHINE_PCPU_H_
#define BYCORF_FREEBSD_MACHINE_PCPU_H_

// FreeBSD's native pcpu pointer lives in a kernel-reserved register. A Bycorf
// worker instead carries the context in TLS, including while a coroutine is
// suspended. Network state never follows a coroutine to another worker.
struct pcpu;
struct thread;
extern _Thread_local struct pcpu* bycorf_bsd_pcpu;
extern _Thread_local struct thread* bycorf_bsd_curthread;

#if defined(__x86_64__)
// Native PMAP headers parse an inline TLB workaround through PCPU_GET even
// though VM operations are unavailable in the host port. No kernel CPU
// management state is initialized or used by networking.
#define PCPU_MD_FIELDS bool pc_pcid_invlpg_workaround
#else
#define PCPU_MD_FIELDS char bycorf_unused
#endif
#define curthread bycorf_bsd_curthread
#define PCPU_GET(member) (bycorf_bsd_pcpu->pc_##member)
#define PCPU_PTR(member) (&bycorf_bsd_pcpu->pc_##member)
#define PCPU_SET(member, value) (bycorf_bsd_pcpu->pc_##member = (value))
#define PCPU_ADD(member, value) (bycorf_bsd_pcpu->pc_##member += (value))

static inline struct pcpu* get_pcpu(void) { return bycorf_bsd_pcpu; }

#endif
