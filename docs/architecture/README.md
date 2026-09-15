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

# Architecture

Celer is a Linux C++23 coroutine runtime. Applications provide services and
protocol handlers; the runtime owns worker threads, scheduling, connections,
and asynchronous I/O. Abseil supplies status types and spdlog supplies logging.

| Module | Responsibility | Document |
|---|---|---|
| Runtime | Worker lifecycle, coroutine scheduling, cross-worker and foreign ingress | [Runtime](runtime.md) |
| I/O | io_uring, optional SPDK storage, shared DPDK environment | [I/O backends](io.md) |
| Networking | Listeners, streams, services, TLS, optional DPDK/FreeBSD IPv4 TCP | [Networking](networking.md) |
| RPC | Framed request/reply protocol above streams | [RPC](rpc.md) |

Each connection and its coroutines have one worker owner. Key ownership and
application-level request routing may choose a different worker; that does not
move the connection. Cross-worker work returns to the awaiting coroutine's
worker before that coroutine uses its stream again.

`celer::core`, `celer::celer`, and `celer::io` alias the core library;
`celer::rpc` is a separate optional consumer of its public network primitives.
Build choices select the networking and storage implementations independently.
The [prototype runbook](../dpdk-prototype.md) describes the DPDK configuration.

Architecture documents describe the current core model and stable tradeoffs.
Keep local algorithms and tuning details near code, operational commands in
the runbook, and change-specific motivation and measurements in review context.
