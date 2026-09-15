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

# Native FreeBSD + DPDK prototype

This optional AArch64 and x86-64 backend serves ordinary IPv4 TCP clients
through DPDK and a native FreeBSD 15.0 source subset. It uses the existing worker threads,
stream interface, application parser, and storage APIs. It has no F-Stack
library or F-Stack-derived host adaptation.

## Build

Prerequisites include Clang 18, CMake, Ninja, Meson, Python 3/pyelftools,
pkg-config, GNU binutils/awk, OpenSSL development files, NUMA/UUID development
files, and DPDK/SPDK's normal build dependencies. Both AArch64 and x86-64
(Intel/AMD, called `amd64` by FreeBSD) use the same native build commands.
CMake's C++ compiler may be Clang or GCC, but the private kernel build requires
Clang. Headers and structure offsets follow the application's compiler target.
Full FreeBSD sources are not needed to build.

Initialize direct dependencies and SPDK's compression helpers explicitly:

```bash
git submodule update --init third_party/liburing third_party/abseil \
  third_party/spdk third_party/dpdk
git -C third_party/spdk submodule update --init isa-l isa-l-crypto
cmake -S . -B build-dpdk-net -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 \
  -DCELER_WITH_DPDK=ON -DCELER_BUILD_EXAMPLES=ON
cmake --build build-dpdk-net -j4
```

Celer builds and installs one static DPDK, then configures SPDK with
`--with-dpdk=<that-installation>`. It never builds SPDK's nested DPDK.
`CELER_DPDK_PREFIX` may point to an existing installation built from the same
pinned DPDK commit, with static libraries in `lib/` and headers in `include/`.
`CELER_DPDK_DRIVERS` defaults to `net/tap,net/ring,net/virtio`; select additional
PMDs at configuration time for other hardware. SPDK currently builds in its
submodule directory, so configure builds that use different DPDK prefixes
serially and keep each executable's dependency provenance.

`CELER_WITH_SPDK_STORAGE=ON` additionally enables NVMe storage. Networking and
SPDK share `EnsureDpdkEnvironment()` and one `spdk_env_init` call. EAL is DPDK's
environment layer for memory, devices, and thread/lcore registration; SPDK
initializes it on Celer's behalf.

## Virtual-device correctness tests

The test owns an isolated TAP named `celerdp0` and refuses to alter it if it
already exists. It uses CPUs 0 and 1 for the two server workers and CPUs 2 and 3
for ordinary Linux TCP clients. Both workers share exactly one RX/TX queue
pair. Root or equivalent network capabilities and `/dev/net/tun` are required.

```bash
sudo python3 tests/dpdk_smoke.py build-dpdk-net/celer_echo
sudo python3 tests/dpdk_smoke.py build-dpdk-net/celer_echo --loss
```

The second command adds 2% packet loss to the test's TAP with `tc netem`.
Both commands check stream contents, half-close, idle/resume, RX/TX forwarding,
and shutdown with live connections, in poll and adaptive modes. They retain
logs in a printed temporary directory. Change `--server-cpus` and
`--client-cpus` if the machine has a different allowed CPU set.

The startup regression check uses an in-memory ring PMD and needs no TAP:

```bash
CELER_EAL_ARGS='--no-huge --no-pci --vdev=net_ring0' \
  timeout 15s ./build-dpdk-net/celer_backend_startup_check 0
CELER_EAL_ARGS='--no-huge --no-pci --vdev=net_ring0' \
  timeout 15s ./build-dpdk-net/celer_backend_startup_check 1
```

Each invocation fails one worker before initialization and verifies that its
peer can leave startup and the runtime joins both threads. The two invocations
use separate processes because the stack has process lifetime.

### Standalone architecture checks

The private BSD bridge can also be tested without DPDK or io_uring:

```bash
sudo python3 tests/freebsd_port_smoke.py build-dpdk-net/celer_freebsd_port_check
sudo python3 tests/freebsd_port_smoke.py build-dpdk-net/celer_freebsd_port_check --loss
```

This fixture owns two temporary TAPs in the otherwise unused `198.19.0.0/24`
and `198.19.1.0/24` subnets. Two native threads own distinct VNETs and exchange
binary streams with ordinary Linux TCP clients, including odd lengths, mbuf
boundaries, half-close, idle/resume, retransmission, and live-socket shutdown.
It exercises the architecture adaptation; use `dpdk_smoke.py` above to check
production queue forwarding and adaptive wakeups.

On an AArch64 Ubuntu host, the BSD library and this fixture can be cross-built
and run as x86-64 code with `gcc-x86-64-linux-gnu`,
`g++-x86-64-linux-gnu`, and `qemu-user` installed:

```bash
python3 cmake/build_freebsd.py --source . --output build-freebsd-x86 \
  --cc clang-18 --target x86_64-linux-gnu
x86_64-linux-gnu-g++ -std=c++23 -O2 -g -pthread \
  tests/freebsd_port_check.cpp build-freebsd-x86/libceler_freebsd.a \
  -o build-freebsd-x86/celer_freebsd_port_check
sudo python3 tests/freebsd_port_smoke.py build-freebsd-x86/celer_freebsd_port_check \
  --runner 'qemu-x86_64 -L /usr/x86_64-linux-gnu' --loss
```

The helper selects matching binutils through Clang, with `--ld`, `--nm`,
`--objcopy`, and `--ar` overrides for other toolchain layouts. This standalone
cross-build does not configure cross builds of SPDK/DPDK or the full application.
QEMU user emulation validates the BSD instructions and TCP bridge; it does not
validate x86 NIC interrupts or establish physical-machine performance.

To run the echo server manually:

```bash
sudo env CELER_DPDK_MODE=adaptive CELER_DPDK_QUEUES=1 \
  taskset -c 0,1 ./build-dpdk-net/celer_echo 198.18.0.2 16390 2
```

After both `celer0: Ethernet address:` messages appear, configure the Linux
side of the newly created TAP from another terminal:

```bash
sudo ip link set celerdp0 address 02:00:00:00:00:01
sudo ip address add 198.18.0.1/24 dev celerdp0
```

The PMD configures the TAP during startup, so assigning its Linux-side MAC
before initialization finishes can lose that assignment. The client-side MAC
must differ from the stack's `02:00:00:00:00:02`. The TAP disappears when its
owning process closes. No physical NIC needs rebinding for these tests.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `CELER_DPDK_MODE` | `poll` | `poll` keeps polling; `adaptive` can arm RX notification and sleep on io_uring |
| `CELER_DPDK_QUEUES` | worker count, capped by hardware | RX/TX pair count, between 1 and worker count |
| `CELER_DPDK_IP` | `198.18.0.2` | Stack's IPv4 address |
| `CELER_DPDK_NETMASK` | `255.255.255.0` | IPv4 subnet mask |
| `CELER_DPDK_GATEWAY` | none | Optional default gateway |
| `CELER_DPDK_MEMORY_MB` | 512 for no-huge tests | EAL memory size in MiB |
| `CELER_EAL_ARGS` | virtual TAP configuration | Extra arguments passed through SPDK to EAL |

An unset/empty `CELER_EAL_ARGS` selects no hugepages, no PCI probing, and the
TAP test device. Explicit arguments replace this virtual default; configure
hugepages and the dedicated device allowlist for a physical run. Include the
NVMe controller in that allowlist when using SPDK storage too. The prototype
requires exactly one available Ethernet port and 1–15 workers. Changing the
backend requires rebuilding; changing these settings requires process restart.

A PMD must support the selected queue/MTU configuration. Adaptive hardware
sleep additionally needs per-queue interrupt control and an accessible RX
notification descriptor. Unsupported notification paths fall back to polling.
The TAP path tests descriptor readiness, not a physical MSI-X interrupt.

## Application and Keylane integration

The normal `TcpService` / `TcpStream` API is unchanged. The same incremental
parser consumes stream bytes, and the same storage implementation handles
commands. Raw BSD socket handles are worker-local and must not be passed to
Linux `send`, `poll`, `dup`, `close`, or descriptor-transfer APIs. Celer routes
its own close and peer-address operations to the right backend.

Keylane's integration worktree can select an external Celer checkout:

```bash
cmake -S /path/to/keylane -B /path/to/keylane/build-dpdk-net -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DKEYLANE_ENABLE_OPT=OFF \
  -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 \
  -DKEYLANE_CELER_SOURCE_DIR=/path/to/celer \
  -DCELER_WITH_DPDK=ON
cmake --build /path/to/keylane/build-dpdk-net --target keylane -j4
```

Use a fresh disposable data file for SET/GET runs, bind the server to the
configured stack address, and configure the TAP after initialization as above.
For two workers pinned to CPUs 0 and 1, pin memtier to other CPUs. Ordinary
files continue through io_uring; `KEYLANE_WITH_SPDK=ON` enables SPDK paths.
Keylane's replication handoff and any application path that operates directly
on Linux descriptors have not been ported by this prototype.

## Limits and interpretation

The implemented profile is one IPv4 address and one port, Ethernet MTU 1500,
ARP, ICMP control traffic, and native TCP. VLAN-tagged and IP-fragmented input
frames are dropped. IPv6 listeners, live socket migration, kernel-style sysctl,
IPsec, and BSD file-descriptor APIs are not provided. Outgoing TCP and Unix
sockets still use the kernel backend. TLS and replication are outside the
prototype's validation scope.

The port uses copied packets, per-allocation host memory, and software
checksums. Its kernel registries/VNETs have process lifetime, and only one
runtime can be created per process. `M_WAITOK` exhaustion aborts instead of
blocking a worker in a BSD VM allocator; unsupported kernel sleeping paths
also fail explicitly. This is not a production-hardened network stack.

TAP sends packets through Linux and introduces syscalls, scheduling, and
virtual-device overhead. The pinned TAP PMD also uses asynchronous realtime
signals as an RX trigger in both modes; Celer's poll/adaptive switch controls
worker parking and RX descriptor/interrupt arming, not that PMD implementation.
Its throughput is useful for finding prototype
bottlenecks, but cannot demonstrate the throughput or CPU savings of a
physical NIC with kernel bypass. Record server/client affinity, backend mode,
queue count, both repository revisions, workload, and device type with every
measurement. Physical queue and interrupt behavior needs separate NIC testing.

See [network architecture](architecture/networking.md) and
[FreeBSD provenance](../third_party/freebsd/README.md).
