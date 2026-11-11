<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# RDMA registration and ownership

`BYCORF_ENABLE_RDMA` adds an independent I/O capability backed by libibverbs and
librdmacm. It defaults to off and does not select or require DPDK/SPDK. The
private `RdmaBackend<Worker>` facade reaches the worker's existing io_uring
through `KernelBackend()` for channel readiness and cancellation. It creates no
ring, polling thread, thread pool, or process-wide blocking lock. The worker
continues to own shared-ring submission and completion dispatch.

Construction does not enumerate or open devices. Explicit `OpenRdmaDevice`
selects a named verbs device; `AcquireRdmaDomain` accepts an owning reference to
the actual context selected by the caller, including a CM-selected context.
An aliasing shared pointer can keep the CM context owner alive. A borrowed raw
context with a no-op deleter is insufficient. Resource acquisition and cache
access occur on the owning worker. Each worker caches live PDs by actual
context identity; two contexts for one device and two workers using one context
have distinct PDs. The cache holds weak references, so resource ownership is
independent of worker shutdown. Partial initialization unwinds in reverse order.

`RdmaProtectionDomain` retains its PD and context. QP owners create QPs in that
PD and retain the domain until QP destruction. A registered MR can be used by
multiple QPs in the same PD; it belongs to neither a particular QP nor an IMM
(immediate-data) operation. Operation leases check both the QP's context and PD.
Connection management, QP posting and completion protocols are outside this
registration layer.

`RegisteredMemory` registers caller-owned backing memory with explicit access
permissions. Copies, registered slices, and operation leases retain the same
MR. The last reference deregisters it before releasing its PD and context.
An operation keeps its lease until terminal completion, including error and
cancellation; copying the SGE alone does not retain registration. The application
keeps backing memory alive through all local completions and remote quiescence,
then releases every registration reference before freeing the memory. Dropping
a registration handle or shutting down a worker does not stop remote access.
Teardown failures terminate rather than release dependencies still potentially
used by hardware.

Slices validate nonempty ranges without arithmetic overflow. Local reads need
no access flag; local DMA writes require local-write permission. Remote write
and atomic registration also require local-write permission. Unsupported access
bits are rejected. `RemoteMemoryRegion{address, length, rkey}` is a non-owning
peer descriptor with checked subranges, not a prescribed wire encoding. Export
requires explicit granted remote permissions. Its length is an application
contract: a slice does not narrow the hardware rkey's authority over the MR,
and exporting a subset of permissions does not reduce the MR's granted rights.

RDMA registration does not reuse storage `RegisterBuffers`, allocate backing
memory, grow arenas, or supply a business buffer pool. Memory used by both
storage and RDMA must independently satisfy both registrations' alignment,
access and lifetime requirements.

Sources: `include/bycorf/io/rdma_backend.h`,
`include/bycorf/io/rdma_memory.h`, `src/io/rdma_memory.cpp`,
`include/bycorf/runtime/worker.h`, `src/runtime/worker.cpp`.
