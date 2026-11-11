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

# RDMA connections, notifications and memory ownership

`BYCORF_ENABLE_RDMA` adds an independent I/O capability backed by libibverbs and
librdmacm. It defaults to off and does not select or require DPDK/SPDK. The
private `RdmaBackend<Worker>` facade reaches the worker's existing io_uring
through `KernelBackend()` for channel readiness and cancellation. It creates no
ring, polling thread, thread pool, or process-wide blocking lock. The worker
continues to own shared-ring submission, completion dispatch and sleeping.
`bycorf::rdma::RcListener` and `RcConnection` provide numeric-address RC
Listen/Accept/Connect/Close and CM private data independently of TCP streams.

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
RC connections expose their actual protection domain for registration. A caller
must use that domain rather than assume a separately opened device handle has
the same PD. Data-posting and application completion protocols are outside the
connection API.

## RC ownership and connection flow

Each worker lazily owns one nonblocking CM channel and its ID registry. Each
listener and every connection accepted by it stay on that worker; a connection
returned by Accept survives listener close. Connect and accepted handshakes
have one absolute monotonic deadline covering address resolution, route
resolution and establishment. Submission backpressure does not restart it.
Arguments are copied before returning a lazy task. Private data is limited to
56 bytes in both directions; peer data is copied before acknowledging the CM
event, with the length reported by CM (which can include provider padding).
Listen binds immediately; Accept, Connect and Close are awaitable operations.

Librdmacm shares its routing contexts process-wide. RC therefore opens a private
verbs context per worker on the device selected by CM, and uses CM's external-QP
mode. CM negotiates the path/QP number; the worker creates only RC QPs in its own
PD and applies CM-provided INIT/RTR/RTS attributes, then completes the active
handshake with `rdma_establish`. It does not replace CM's context or attach its
QP to the CM ID. QP, PD, MR, CQ and the verbs asynchronous event stream belong to
the same private context, with exactly one worker reading that event stream.
Other workers neither consume its events nor migrate its connections.

Each private context shares one completion channel and fixed-capacity CQ.
Admission reserves the provider's actual send plus receive WR capacities for
each QP, including capacities rounded up by the provider. All send WRs are
signalled. A QP remains unconnected and has no posted work until its actual
budget fits; excess capacity fails admission instead of growing the CQ.
Reservations cover error/flush completions as well as successful completions,
and are returned only after QP destruction and CQ draining.

## Notification and failure flow

The CM channel, completion channel and private context's async-event fd are
nonblocking, with persistent one-shot readiness polls in the shared io_uring.
A CQ notification is requested before checking the CQ, including initial
activation. Each delivered CQ event is acknowledged, notification is requested
again, and the CQ is drained before rearming the fd poll. An event may remain
after its CQE was already consumed; it still needs acknowledgement and rearming.
Notifications stay enabled during busy and idle periods. This makes pending
work either visible in the CQ recheck or capable of waking an idle worker;
there is no separate polling thread or second ring.

CM events are copied and acknowledged before dispatch can destroy an ID.
Failures retain the CM event name/raw status or the failing verbs operation.
QP errors terminate the affected connection; shared CQ errors, device fatal
errors and device removal terminate every connection on that device context.
Port errors terminate connections using the affected port. Event objects are
acknowledged before this fanout can destroy their referenced resources. A
failed CM channel stops all endpoints on its worker and rejects new admission.

## Close and worker shutdown

Public handles are move-only and used on their owner worker. Destruction
requests close; awaiting Close joins retirement. Closing stops admission,
disconnects the CM association and puts the external QP into ERR. The
connection retains its timer tag until the timer's terminal completion, even
when cancellation has already been accepted. It leaves the ID registry before
resource destruction and releases its CQ budget only after the QP is destroyed
and the CQ drained. The shared channel polls reference the worker's event
engine/device state, never a listener or connection object.

Listener close terminates queued and handshaking children and wakes Accept;
already returned connections remain independent. One Accept and one Close
waiter per handle are supported. Waiters detach when their coroutine frames
are reclaimed. Failed device state is released only after its connections and
both fd polls have retired. A cancellation request or the cancel request's own
CQE is not a retirement boundary for the original fd poll.

Worker shutdown first stops RDMA admission and requests cancellation while
retaining every poll tag and descriptor. It then quiesces the shared ring.
Only after ring shutdown confirms that no kernel operation can refer to those
tags does RDMA retire requests that did not receive callbacks, acknowledge
remaining events, destroy QPs/CM IDs, and release channels/CQs. Coroutine frames
are reclaimed afterward. Exported registration/PD handles retain their own
context independently. Unexpected destruction failures fail closed rather than
release resources that may still be referenced by the provider.

## Registered memory

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
`include/bycorf/rdma/rc.h`, `src/rdma/rc.cpp`, `src/rdma/rc_engine.cpp`,
`src/rdma/rc_internal.h`, `include/bycorf/runtime/worker.h`,
`src/runtime/worker.cpp`, `src/io/io_uring_backend.cpp`.
