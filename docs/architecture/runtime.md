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
inherited affinity mask. Every thread completes affinity setup before any
worker main function runs. An affinity or thread-launch failure prevents the
worker main functions from entering partial service initialization.

A `Worker` owns its coroutine queues, connections, I/O backend, completion
handling, and optional storage poller. The scheduler bounds foreground and
background coroutine work and polls external completions between rounds.
Coroutines cooperate: a long non-suspending handler can delay all work on its
worker. Worker-local synchronization and cross-worker synchronization have
separate ownership contracts.

Cross-worker submissions enter destination mailboxes and resume waiting
coroutines on their original workers. Workers use io_uring MSG_RING to wake a
peer; foreign ingress and process control use the pre-created eventfds.
`ForeignExecutor` is non-owning and is valid only while its runtime accepts
work. Stop closes ingress and wakes workers through descriptors that remain
alive until after their threads have joined.

`Server` starts each registered service on every worker. During shutdown,
workers first leave their loops, then quiesce I/O and reclaim coroutine frames
on their owning native threads. Service finalizers run after all worker frames
are reclaimed. A worker that fails initialization contributes to both teardown
barriers so initialized peers can finish. Services outlive the server.

With DPDK networking, runtime preparation configures the shared port before
launch. Worker 0 initializes the native FreeBSD kernel services; remaining
workers attach separate VNETs in serialized startup. The existing worker loop
runs packet input, TCP timers, and deferred BSD tasks. EAL may have control or
interrupt helper threads; it does not launch Bycorf network polling workers.
Worker-affine BSD sockets close and EAL thread registrations detach before
join; the runtime then releases the port and software packet queues.

Sources: `src/runtime/runtime.cpp`, `src/runtime/worker.cpp`,
`include/bycorf/runtime/cross_core.h`, `src/runtime/foreign_executor.cpp`,
`src/net/server.cpp`.
