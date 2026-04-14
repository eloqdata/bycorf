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

# Celer Project Overview

## Goal

Celer is a Linux-only experimental backend runtime/framework centered on:

- one worker thread per CPU core
- one `io_uring` instance per worker
- one TCP connection handled by one coroutine
- exception-free control flow
- status-based errors in a `Status` / `StatusOr` style

Current priority:

- keep the repository focused on the core framework itself
- develop `runtime/io/net`
- push protocol implementations into separate repositories on top of Celer

## Current State

The repository now contains:

- a multi-worker TCP echo server
- worker-owned connection lifecycle with delayed reclaim
- a compile-time receive mode split
- a thin internal logging layer
- a core-only build centered on `base/runtime/io/net`

Key files:

- `CMakeLists.txt`: core build definition
- `include/celer/base/status.h`: minimal internal `Status` / `StatusOr`
- `include/celer/base/log.h`: thin project-local logging facade
- `include/celer/io/io.h`: transport-agnostic I/O layer marker
- `include/celer/runtime/task.h`: coroutine `Task<T>`
- `include/celer/runtime/operation.h`: `io_uring` completion base type
- `include/celer/runtime/worker.h`: worker event loop, connection ownership, close/reclaim
- `include/celer/net/connection.h`: connection state and lifecycle fields
- `include/celer/net/tcp_listener.h`: listener abstraction
- `include/celer/net/tcp_stream.h`: public TCP stream handle
- `src/app/echo_server.cpp`: multi-worker TCP echo server

Current CMake targets:

- `celer`: core library with base/runtime/io/net
- aliases:
  - `celer::celer`
  - `celer::core`
  - `celer::io`

Current example binaries:

- `celer_echo`

## Layering

The intended layering is:

- `base`: status, logging, small utilities
- `runtime`: worker loop, tasks, operations
- `io`: transport-agnostic I/O concepts shared by network and future storage/file support
- `net`: TCP/listener/connection objects built on top of runtime + io

Protocol implementations, including HTTP, should live outside this repository and depend on Celer.

## Receive Modes

Receive mode is selected at compile time:

- default: `multishot`
- optional placeholder: `registered_buf`

Configure `registered_buf` mode with:

```bash
cmake -S . -B build -DCELER_USE_REGISTERED_BUF=ON
```

Notes:

- `multishot` is the active path
- `registered_buf` is intentionally left unimplemented
- future storage-oriented work should hang off `registered_buf`, not distort the multishot path

## Current Multishot Design

The multishot path is connection-level rather than one-read-one-recv:

- worker owns a provided buffer ring
- one multishot recv can stay armed on a connection
- CQEs enqueue buffer descriptors into `Connection.received_buffers`
- `TcpStream::ReadSome()` consumes those descriptors
- data is copied once into the caller buffer
- buf-ring entries are recycled only after the consumer finishes the chunk

## Logging

The project uses a thin local logging layer:

- `CELER_LOG_INFO`
- `CELER_LOG_WARN`
- `CELER_LOG_ERROR`

Current backend writes formatted lines to `stderr`.

## Important Constraints

These are currently intentional:

- a `TcpStream` is single-worker only
- at most one read may wait on a connection
- at most one write may be in flight on a connection
- one read and one write may coexist
- worker owns connections
- retired connections must not be freed until async state drains

## How To Run

TCP echo:

```bash
./build/celer_echo [bind_ip] [port] [threads] [idle_timeout_ms]
```

Example:

```bash
./build/celer_echo 127.0.0.1 8080 4
```

## Benchmarking Note

For raw TCP echo, use:

- `tcpkali`
- `sockperf`
- `iperf3` only for transport ceilings

## Immediate Next Steps

Recommended next implementation order:

1. continue developing the core framework: runtime/io/net
2. add the first storage/file-oriented building blocks under `io`
3. tighten multishot lifecycle/reclaim edge cases
4. keep `registered_buf` as the later storage-oriented path
5. keep protocol implementations, including HTTP, in separate repositories
