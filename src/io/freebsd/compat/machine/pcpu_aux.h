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

#ifndef BYCORF_FREEBSD_MACHINE_PCPU_AUX_H_
#define BYCORF_FREEBSD_MACHINE_PCPU_AUX_H_

// sys/pcpu.h includes this after defining struct pcpu. Native amd64 replaces
// curthread with a %gs load and assumes a page-sized kernel pcpu allocation.
// The host port's compact context and TLS accessors are defined in pcpu.h;
// neither kernel layout nor segment-register access applies here.
extern struct pcpu pcpu0;

#endif
