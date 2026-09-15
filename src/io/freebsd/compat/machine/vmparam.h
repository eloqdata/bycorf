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

#ifndef CELER_FREEBSD_MACHINE_VMPARAM_H_
#define CELER_FREEBSD_MACHINE_VMPARAM_H_

#include_next <machine/vmparam.h>
#include <sys/types.h>

// External-page mbufs are disabled, but native mbuf helpers still compile
// their direct-map branches. A Linux process has no FreeBSD physical map.
// Reject accidental entry instead of providing dummy kernel address globals
// or allowing physical addresses to become host pointers.
_Noreturn uintptr_t celer_bsd_no_direct_map(uintptr_t address);
#undef PHYS_TO_DMAP
#undef DMAP_TO_PHYS
#define PHYS_TO_DMAP(address) celer_bsd_no_direct_map((uintptr_t)(address))
#define DMAP_TO_PHYS(address) celer_bsd_no_direct_map((uintptr_t)(address))

#endif
