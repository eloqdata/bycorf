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

# Bycorf

A C++ server framework built on stackless coroutines, with io_uring and
kernel-bypass support.

Bycorf targets Linux and C++23, with SPDK storage and a native FreeBSD/DPDK TCP
backend available alongside io_uring. Bycorf owns worker threads, scheduling,
connections, and asynchronous I/O; applications provide services above
`TcpStream`. RPC is available through a separate library target.

- [Name](#name)
- [Build and test](#build-and-test)
- [Run the examples](#run-the-examples)
- [Use Bycorf in a CMake project](#use-bycorf-in-a-cmake-project)
- [Development](#development)
- [Documentation](docs/README.md) and [roadmap](ROADMAP.md)

## Name

**Bycorf** (pronounced "bye-korf") combines kernel-**BY**pass,
**COR**outine, and **F**ramework, inspired by the description
"A kernel-bypass, coroutine-based server framework."

## Build and test

The default build uses Linux TCP and io_uring. It needs a C++23 compiler,
CMake 3.20 or newer, Ninja, Make, and OpenSSL development files. CI uses
Clang 18 on Ubuntu 24.04, natively on AMD64 and ARM64.

On Ubuntu 24.04, install the build tools:

```bash
sudo apt-get update
sudo apt-get install --no-install-recommends \
  git build-essential clang-18 cmake ninja-build libssl-dev
```

For a new standalone checkout:

```bash
git clone https://github.com/thweetkomputer/celer.git bycorf
cd bycorf
```

Run the following commands from the Bycorf repository root, including when
Bycorf is checked out as another project's submodule. Only Abseil and liburing
need initialization for the default build:

```bash
git submodule update --init --depth 1 third_party/abseil third_party/liburing
cmake -S . -B build -G Ninja \
  -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  -DBYCORF_KERNEL_BYPASS=OFF
cmake --build build --parallel
ctest --test-dir build --output-on-failure --no-tests=error
```

Use `-DCMAKE_BUILD_TYPE=Release` in a separate build directory for performance
measurements. A Debug build is the default development and CI workflow above.

Runtime tests and examples need io_uring to be permitted by the host and
container policy, and enough locked-memory allowance for registered buffers.
If startup fails, check the host's `kernel.io_uring_disabled` setting and the
shell's `ulimit -l`; compilation alone does not verify these runtime conditions.

CTest registers three software regressions, each with a 90-second timeout:

| Test | Coverage |
|---|---|
| `bycorf_connect_timer_check` | Early shutdown, timers and cancellation, loopback TCP, refused connections, deadlines, address validation, and retirement of losing deadlines |
| `bycorf_connection_storage_check` | Connection storage lifetime while suspended coroutines still borrow it |
| `bycorf_backend_selection_check` | Unsupported backend requests, inactive optional backends, and immutable allocator/backend selection |

Network checks use one unpinned worker and ephemeral loopback ports. The
connection-timeout probe warns if the host rejects its unreachable destination
immediately, since that exercises connection failure instead of the deadline.
These software tests also run in a bypass-capable build with the default
kernel/io_uring selection, without activating EAL or accessing devices.

To build tests without the example applications:

```bash
cmake -S . -B build -DBYCORF_BUILD_TESTS=ON -DBYCORF_BUILD_EXAMPLES=OFF
cmake --build build --parallel
ctest --test-dir build --output-on-failure --no-tests=error
```

### Optional DPDK and SPDK backends

`BYCORF_KERNEL_BYPASS` defaults to `OFF`. Setting it to `ON` compiles DPDK
networking and SPDK storage together. Applications select them independently
with `ConfigureIoBackends` before runtime or storage initialization; kernel
networking and io_uring storage remain the startup defaults.

CMake exports the numeric `BYCORF_KERNEL_BYPASS` macro to linked consumers:
`1` when enabled and `0` otherwise. Use `#if BYCORF_KERNEL_BYPASS` for code
that requires the compiled DPDK/SPDK capabilities; use `DpdkNetworkEnabled()`
and `SpdkStorageEnabled()` to check the independent runtime selections.

Follow the [DPDK/SPDK build and test guide](docs/dpdk-prototype.md) for its
additional dependencies, virtual-device checks, physical-device configuration,
and experimental compatibility limits.

## Run the examples

The default build enables `BYCORF_BUILD_EXAMPLES`. If it was disabled, set
`-DBYCORF_BUILD_EXAMPLES=ON` and rebuild before running these commands.

### TCP echo

Start a single-worker server:

```bash
./build/bycorf_echo 127.0.0.1 8080 1
```

The first three arguments are the bind address, port, and worker count. The
server echoes bytes received from any TCP client. Stop it with Ctrl-C.
Workers pin to CPUs in the process's allowed affinity mask by default, so
choose a worker count no larger than the number of allowed CPUs.

### RPC echo and benchmark

Start the RPC server:

```bash
./build/rpc_echo_server 127.0.0.1 9000 1
```

In another terminal, run a five-second benchmark:

```bash
./build/rpc_bench 127.0.0.1 9000 1 1 1 64 5
```

The client arguments are server address, port, workers, connections per worker,
concurrent calls per connection, payload bytes, and duration in seconds.
It reports completed calls, throughput, and average latency; stop the server
with Ctrl-C afterwards. Use Release builds and separate CPU affinity sets
for server and client when measuring performance.

## Use Bycorf in a CMake project

Initialize Bycorf's Abseil and liburing submodules as above, then add its
checkout from the parent project's `CMakeLists.txt`:

```cmake
set(BYCORF_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BYCORF_BUILD_TESTS OFF CACHE BOOL "" FORCE)
add_subdirectory(bycorf)

# After defining the application target:
target_link_libraries(my_server PRIVATE bycorf::core)
```

Link `bycorf::rpc` instead when using RPC; it also links the core runtime.
`bycorf::bycorf` and `bycorf::io` are aliases of `bycorf::core`.

Standalone builds default `BYCORF_BUILD_TESTS` to `BUILD_TESTING`. As a
subdirectory it defaults to off, leaving test registration to the parent.
Parents that want Bycorf's regressions should enable CTest at their own root
and set `BYCORF_BUILD_TESTS=ON` before adding the subdirectory.

Each connection allows one pending reader and one pending writer on its owning
worker. Cross-worker work must return to that owner before using the stream.
See the [architecture index](docs/architecture/README.md) for ownership,
shutdown, and backend contracts.

## Development

Bycorf uses Google-style C++23 formatting and pins clang-format 23.1.1 through
`pre-commit`. Install and enable the hook from the Bycorf repository root:

```bash
sudo apt-get install pre-commit
pre-commit install
```

The hook formats staged first-party C and C++ files. If it changes a file, the
commit stops so the result can be reviewed and staged before retrying. Format
the complete maintained source tree with:

```bash
pre-commit run clang-format --all-files
```

The CMake `format` and `format-check` targets are enabled only when the detected
system clang-format reports exactly version 23.1.1. The pre-commit environment
downloads that pinned formatter independently, so it does not add a runtime
dependency to Bycorf. Build outputs and local editor/agent configuration are
ignored; imported sources retain their upstream formatting and notices.

## Continuous integration

The [CI workflow](.github/workflows/ci.yml) runs on pull requests, pushes to
`main`, and manual dispatch. One job checks all maintained sources using the
pinned clang-format hook. Two independent jobs build the core library, RPC
library, examples, and tests with Clang 18 in Debug, then run CTest natively
on AMD64 (`ubuntu-24.04`) and ARM64 (`ubuntu-24.04-arm`). The workflow enables
io_uring and raises the test shell's memlock limit on its disposable runners.

CTest must discover at least one test. Its JUnit report and detailed logs are
uploaded as per-architecture artifacts retained for seven days. Hosted CI
sets `BYCORF_KERNEL_BYPASS=OFF`; DPDK/SPDK hardware testing requires a separate host.
