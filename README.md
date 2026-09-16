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

# celer

Linux C++23 coroutine runtime with io_uring, optional SPDK storage, and an
optional native FreeBSD/DPDK TCP backend.

- [Architecture](docs/architecture/README.md)
- [DPDK prototype build and tests](docs/dpdk-prototype.md)

## Development

Celer uses Google-style C++23 formatting and pins clang-format 23.1.1 through
`pre-commit`. Install and enable the hook in a standalone Celer checkout:

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
dependency to Celer.

## Build and test

The default Linux build uses io_uring and needs a C++23 compiler, CMake, Ninja, Make,
and OpenSSL development files. Initialize the two runtime dependencies:

```bash
git submodule update --init --depth 1 third_party/abseil third_party/liburing
cmake -S . -B build-ci -G Ninja \
  -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build-ci --parallel
ctest --test-dir build-ci --output-on-failure --no-tests=error
```

`CELER_KERNEL_BYPASS` defaults to `OFF`. Set it to `ON` to compile DPDK
networking and SPDK storage together; see the [build prerequisites](docs/dpdk-prototype.md#build).
This does not activate either backend: applications select them independently
with `ConfigureIoBackends`, and kernel networking/io_uring storage remain the
startup defaults.

Standalone builds enable `CELER_BUILD_TESTS` by default when `BUILD_TESTING`
is enabled. It defaults to off when Celer is included with `add_subdirectory`,
so the parent project controls its own tests. Tests can be built independently
of examples with `CELER_BUILD_TESTS=ON` and `CELER_BUILD_EXAMPLES=OFF`.

The suite registers three software regressions. `celer_connect_timer_check` checks shutdown
requests before worker initialization and between initialization and Run, plus
eleven connection and timer scenarios: timer firing and cancellation, loopback TCP
echo, refused connections, connection deadlines, numeric-address validation,
and retirement of losing deadlines after successful or failed connections.
It exits nonzero on a failed check, and CTest bounds the complete run to
90 seconds. The timeout probe reports a warning when the host rejects its
unreachable destination immediately; that environment exercises connection
failure instead of the pending-connect deadline. `celer_connection_storage_check`
checks storage lifetime while closed connections still have suspended borrowers.
`celer_backend_selection_check` checks unsupported requests, inactive optional
backends, and the immutable allocator/backend choice after first use.

Runtime checks require io_uring to be allowed and sufficient locked-memory
allowance for registered buffers. They use one unpinned worker and ephemeral
loopback ports. These software checks also run in a bypass-capable build with
the default kernel/io_uring selection, without activating EAL or accessing devices.

## Continuous integration

The [CI workflow](.github/workflows/ci.yml) runs on pull requests, pushes to
`main`, and manual dispatch. One job checks all maintained sources using the
pinned clang-format hook. Two independent jobs build the core library, RPC
library, examples, and tests with Clang 18 in Debug, then run CTest natively
on AMD64 (`ubuntu-24.04`) and ARM64 (`ubuntu-24.04-arm`). The workflow enables
io_uring and raises the test shell's memlock limit on its disposable runners.

CTest must discover at least one test. Its JUnit report and detailed logs are
uploaded as per-architecture artifacts retained for seven days. Hosted CI
sets `CELER_KERNEL_BYPASS=OFF`; DPDK/SPDK hardware testing requires a separate host.
