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

#ifndef CELER_FREEBSD_MACHINE_ATOMIC_H_
#define CELER_FREEBSD_MACHINE_ATOMIC_H_

#if defined(__x86_64__)
// The pinned amd64 header's kernel Store/Load fence accesses a private %gs
// cache line. Linux owns the segment registers, so select FreeBSD's userspace
// fence instead. Load the common types under _KERNEL first; the temporary
// selection must affect only the instruction definitions, not kernel types.
#include <sys/types.h>
#pragma push_macro("_KERNEL")
#undef _KERNEL
#include_next <machine/atomic.h>
#pragma pop_macro("_KERNEL")
#else
#include_next <machine/atomic.h>
#endif

#endif
