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

# Bycorf roadmap

Open work for the Bycorf runtime. Current behavior is documented in the
[architecture](docs/architecture/README.md); build and development commands
are in the [README](README.md).

- **`IORING_RECVSEND_BUNDLE` for multishot recv.** Let one recv completion carry
  multiple buffer-ring buffers, cutting completions/syscalls under load.
- **Explicit cancellation of remaining socket operations at shutdown.**
  Connection close already cancels its peer-disconnect observer, but still
  closes the descriptor to terminate other socket I/O. Evaluate explicit
  cancellation of those operations for prompt teardown.
- **Investigate non-pipeline tail latency.** Reproduce the reported long tail
  with current code, including cross-core scheduling and park/wake behavior.
  Keep measurements and hypotheses with the investigation.
- **(Optional) recv registered buffers.** A registered-buffer recv mode was
  removed during the IoBackend extraction; revisit if it beats the provided
  buffer ring.

## RPC (`bycorf::rpc`)

The transport provides framing, request-ID multiplexing, and synchronous or
asynchronous verb handlers. Remaining work:

- **Async client connect.** `bycorf::ConnectTcp` (net/tcp_stream.h) now provides
  `IORING_OP_CONNECT` with a deadline and loser-cancellation for outbound
  streams; rewiring `RpcClient::Connect`'s startup blocking `connect()` to it
  remains open.
- **Per-call timeout (deadline).** Must-have for production: a hung peer must not
  hang the caller. `bycorf::CancellableSleepFor` (io/storage.h) now provides the
  cancellable one-shot timer; giving each pending call a deadline and resuming
  as `DeadlineExceeded` remains open.
- **Reconnect.** Re-establish dropped peer connections (may live app-side, as in
  seastar where the app rebuilds the client).
- **Cancellation.** The backend now has cancel-by-user_data
  (`IoUringBackend::SubmitCancel`); cancelling in-flight rpc calls on timeout /
  shutdown remains open.
- **Handshake / feature negotiation.** Protocol version + feature flags.
- **TCP keepalive.** setsockopt on rpc connections.
- **Compression (lz4)** for large / WAN payloads.
- **Streaming verbs.**
- **Shard-aware routing.** Client picks the source port so the connection lands
  on the data-owning core, eliminating the intra-node x→b→x hop.
