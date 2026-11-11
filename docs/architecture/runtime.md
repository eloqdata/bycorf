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

# Runtime and ownership

`Runtime` creates the worker table and cross-core mailboxes before releasing
worker threads. Each worker normally pins to one CPU from the caller's
inherited affinity mask, cycling when workers outnumber CPUs. An explicit CPU
list can select the order and repeat IDs, but every entry must belong to the
inherited mask. Every thread completes affinity setup before any
worker main function runs. An affinity or thread-launch failure prevents the
worker main functions from entering partial service initialization.

A `Worker` owns its coroutine queues, connections, completion handling, and
[shared I/O backend and adapters](io.md). Each round polls completions and
runs bounded foreground and background coroutine work. The worker polls the
shared ring before native network work and polled storage. Before maintenance
and at the end of the round, it queues peer wakes, flushes native network
output, and submits the shared ring. Idle rechecks use the same ordering;
facades do not poll or submit the ring on the worker's behalf. Coroutines
cooperate: a non-suspending handler can
delay all work on its worker. Worker-local and cross-worker synchronization
have separate ownership contracts.

A worker resumes a ready coroutine directly. `Task` enters its child and
returns to its parent continuation by symmetric transfer, within the same
worker dispatch and task class. These transfers do not use a dispatcher or
ready queue; actual I/O or queue waits return control to the scheduler. Native
stack bounds rely on compiler tail transfers. The CMake target publishes
`-foptimize-sibling-calls` for GCC, including Debug builds, to every consumer
that compiles Task coroutine bodies. The bounded-stack regression exercises
the selected toolchain and build configuration. Awaited child frames remain
owned by the awaiting `Task` object, including when shutdown destroys a
suspended root and its child chain.

## Sleep and wakeups

The worker alone decides when its thread can block. It prepares source
notifications, rechecks I/O, mailbox and scheduler work, completes the mailbox
wake-sequence handshake, and submits operations and peer wakes. After a final
idle check it waits on the shared io_uring. Runnable work, a stop request,
unsubmitted operations, or storage requiring polling prevent sleep. Network
deadlines and enabled connection-idle checks bound the wait. Optional RDMA CM,
CQ-notification and device-async polls are persistent wake sources on this same
ring; RDMA connection deadlines are absolute ring timeouts. CQ notifications
are always armed and rechecked, so RDMA needs no busy polling or additional
sleep-preparation phase.

Cross-worker submissions enter destination mailboxes and resume coroutines on
their original workers. The mailbox handshake makes racing publication either
prevent sleep or leave a persistent wake: workers use MSG_RING, while foreign
ingress and process control use pre-created eventfds. Consuming a completion
can make work runnable or observe a stop request, so rechecking must account
for both before blocking.

Every wait attempt clears its mailbox parked state and ends source notification
preparation, including attempts abandoned before blocking. Source cleanup
releases per-attempt state; outstanding notification polls retain their tags
and descriptors through completion or ring shutdown.

`ForeignExecutor` is non-owning and valid while its runtime accepts work. Stop
closes ingress and wakes workers through descriptors that remain alive until
after their threads have joined.

## Shutdown and integration

`Server` starts each registered service on its selected global worker IDs;
an empty selection means every worker. Prepare sees the full runtime count,
while Run and FinalizeWorker execute only on selected workers. During shutdown,
workers first leave their loops, then quiesce I/O and reclaim coroutine frames
on their owning native threads. Service finalizers run after all worker frames
are reclaimed. RDMA uses a two-phase shutdown: stop admission before the ring
is quiesced, then release retained polls/events and connection resources after
kernel access has ended. This also covers accepted operations whose callbacks
are suppressed by ring shutdown. A worker that fails initialization contributes
to both teardown barriers so initialized peers can finish. Temporary I/O submission shortages
are retried by the worker loop. A permanent submission error, notification
preparation or cleanup failure, or shared-ring wait failure stops peer workers
before entering those barriers and yields a nonzero exit. Services outlive
the server.

With DPDK networking, runtime preparation configures the shared port before
launch. Worker 0 initializes the native FreeBSD kernel services; remaining
workers attach separate VNETs in serialized startup. The worker loop
runs packet input, TCP timers, and deferred BSD tasks. EAL may have control or
interrupt helper threads; it does not launch Bycorf network polling workers.
Worker-affine BSD sockets close and EAL thread registrations detach before
join; the runtime then releases the port and software packet queues.

Active registered streams are counted across all workers and runtimes in the
process. The atomic snapshot includes outgoing control connections and is
updated only at connection open/close, so monitoring can read it without
scheduling work on a control worker.

Sources: `src/runtime/runtime.cpp`, `src/runtime/worker.cpp`,
`src/runtime/worker_wait.h`, `include/bycorf/runtime/task.h`,
`include/bycorf/runtime/cross_core.h`, `src/runtime/foreign_executor.cpp`,
`src/net/server.cpp`.
