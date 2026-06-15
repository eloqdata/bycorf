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

# celer Roadmap

Durable backlog for the celer runtime (coroutines + io_uring, thread-per-core).

- **Pool-allocate coroutine frames.** Each `Task` heap-allocates its frame;
  pool/recycle per-worker to remove the per-request `malloc`/`free` on the hot
  path.
- **`IORING_RECVSEND_BUNDLE` for multishot recv.** Let one recv completion carry
  multiple buffer-ring buffers, cutting completions/syscalls under load.
- **Explicit `io_uring_prep_cancel` at shutdown.** Today we rely on `close(fd)`
  to terminate in-flight ops; issue explicit cancels per fd (dragonfly/helio
  style) for deterministic, prompt teardown.
- **Investigate non-pipeline tail latency.** memtier 1:1 (50c/4t) showed avg
  ~10ms while p50/p99 were ~0.9/1.8ms — a sub-0.1% long tail. Find the root cause
  (suspect: cross-core hop / park-wake timing).
- **(Optional) recv registered buffers.** A registered-buffer recv mode was
  removed during the IoBackend extraction; revisit if it beats the provided
  buffer ring.

## RPC (`celer::rpc`)

The first cut is a bare transport — framing + req-id multiplexing + verb dispatch
with synchronous handlers — for benchmarking against brpc. Production features,
deferred (seastar::rpc has all of these):

- **Async verb handlers (`Task<Bytes>`).** Let a handler `co_await SubmitTo(...)`
  to reach the data-owning core (keylane cross-node GET/SET forwards to the shard
  core). The bench version uses sync handlers (echo) processed inline.
- **Async client connect.** Replace the startup blocking `connect()` with io_uring
  `IORING_OP_CONNECT`.
- **Per-call timeout (deadline).** Must-have for production: a hung peer must not
  hang the caller. Give each pending a deadline; resume as `DeadlineExceeded`.
- **Reconnect.** Re-establish dropped peer connections (may live app-side, as in
  seastar where the app rebuilds the client).
- **Cancellation.** Cancel in-flight calls on timeout / shutdown.
- **Handshake / feature negotiation.** Protocol version + feature flags.
- **TCP keepalive.** setsockopt on rpc connections.
- **Compression (lz4)** for large / WAN payloads.
- **Streaming verbs.**
- **Shard-aware routing.** Client picks the source port so the connection lands
  on the data-owning core, eliminating the intra-node x→b→x hop.
