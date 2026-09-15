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

# Celer overview

Celer is a Linux C++23 coroutine runtime with one worker per selected CPU,
worker-owned connections, io_uring I/O, optional SPDK storage, and optional
DPDK networking backed by a native FreeBSD IPv4/TCP port. Applications provide
services above `TcpStream`; RPC is a separate library target.

The current model and source map are maintained in the
[architecture index](docs/architecture/README.md). See the
[DPDK prototype runbook](docs/dpdk-prototype.md) for building and testing the
optional network backend.

A normal TCP echo server runs with:

```bash
./build/celer_echo 127.0.0.1 8080 4
```

The listener/stream contract permits one reader and one writer concurrently,
both on the connection's worker. Coroutines and deferred completions must drain
before the runtime reclaims connection state. Cross-worker application work
returns to the connection owner before using that stream again.
