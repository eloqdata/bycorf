<!-- Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 -->

# RDMA examples and real-device testing

The `bycorf_rdma_demo` example runs one client/server exchange through RC
SEND/RECV, READ, WRITE, SEND_WITH_IMM and WRITE_WITH_IMM. It verifies payloads,
completion kinds, lengths and immediate data, including zero. The server
exports a registered region; the client reads and writes it. Explicit receive
credits precede the immediate transfers. A write notification uses a separate
receive buffer and verifies that this buffer remains untouched.

This is a small example protocol with a versioned, big-endian descriptor,
not a Valkey protocol or a new Bycorf transport abstraction. Registrations use
the connection's actual PD. Backing memory survives local completion and peer
quiescence; every error path joins connection close before freeing it. The
client validates the final response and then disconnects. The server retains
that response's buffer until peer close, because a CM disconnect can precede
dispatch of its local SEND completion. Both process results must pass.

## Build

From the Bycorf repository root:

```bash
sudo apt-get install libibverbs-dev librdmacm-dev rdma-core ibverbs-providers iproute2
cmake -S . -B build-rdma -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBYCORF_ENABLE_RDMA=ON
cmake --build build-rdma --parallel
```

Standalone builds enable examples and tests by default. If an existing cache
disabled them, reconfigure with `BYCORF_BUILD_EXAMPLES=ON` and
`BYCORF_BUILD_TESTS=ON`. The demo is built only with RDMA and examples enabled;
the normal build remains usable without RDMA libraries. Python 3 and CTest
3.21 or newer are needed for the integration runner's JUnit output.

## Prepare a device

Use an existing device from `rdma link show`, or set up Soft-RoCE on a Linux
host that provides the `rdma_rxe` module. The following disposable, single-host
fixture uses a veth pair and an isolated benchmark subnet. First verify that
the interface/device names and `198.19.42.0/30` are unused on your host:

```bash
sudo modprobe rdma_rxe
sudo ip link add bcrdma0 type veth peer name bcrdma1
sudo ip address add 198.19.42.1/30 dev bcrdma0
sudo ip address add 198.19.42.2/30 dev bcrdma1
sudo ip link set bcrdma0 up
sudo ip link set bcrdma1 up
sudo rdma link add bycorf_rxe type rxe netdev bcrdma0
rdma link show
```

For two hosts, create an RXE device on each host's existing Ethernet interface
instead: `sudo rdma link add rxe0 type rxe netdev <interface>`. Use the IP
assigned to that interface. See the upstream
[RXE setup guide](https://github.com/linux-rdma/rdma-core/blob/master/Documentation/rxe.md).
Physical RDMA devices can also run the same examples and tests.

The test user needs access to `/dev/infiniband/rdma_cm` and the selected
device's uverbs node, sufficient locked-memory allowance (`ulimit -l`), and
permission to use io_uring. Device creation needs host network privileges;
module loading may need host administrator access. Containers need the host
RDMA device nodes and appropriate permissions. The test runner itself neither
invokes sudo nor changes networking.

## Run the client and server

In one terminal, start the single-connection server:

```bash
./build-rdma/bycorf_rdma_demo server 198.19.42.1
```

After it prints `RDMA_READY`, use another terminal:

```bash
./build-rdma/bycorf_rdma_demo client 198.19.42.1
```

For a second host, the client uses the server's RDMA IP. Both programs print
`RDMA_OK` and exit on success. Arguments are `server|client IP [PORT [BYTES
[TIMEOUT_SECONDS]]]`: defaults are port 7471, 65536 payload bytes and a
30-second whole-session deadline, including accepting a client. The server
accepts port zero and prints the assigned port. Both peers must specify the
same payload size, between 1 byte and 16 MiB. A deadline stops the worker;
ring/DMA shutdown completes before suspended frames and buffers are reclaimed.
Exit codes are 0 for success, 1 for runtime failure and 2 for invalid arguments.

## Run the integration gate

On one host, with the fixture above:

```bash
python3 tests/rdma_integration.py --build-dir build-rdma \
  --device bycorf_rxe --ip 198.19.42.1
```

The runner selects the real `RcLoop` and `RdmaRegistration` suites, supplies
their explicit device/IP opt-ins, and checks that no case was skipped. It then
starts independent server/client processes for 1-byte, 64-KiB and 1-MiB
payloads. A server readiness handshake avoids startup sleeps and port probing.
Each process has a deadline; failures clean up the child server and return a
nonzero exit. A missing device or executable, empty test selection, skipped
test, timeout or payload failure fails the gate. This command is deliberately
opt-in and is not run by ordinary CTest discovery.

Use the IP belonging to the selected RDMA device. Registration tests explicitly
open that device; RC routing uses the IP to let CM select its device. The runner
does not provision RXE or select DPDK/SPDK backends. Existing fake-provider
regressions remain useful for deterministic cancellation, notification-race
and queue-failure coverage; they do not replace this gate.

Clean up only the fixture created above, after all tests and examples exit:

```bash
sudo rdma link delete bycorf_rxe
sudo ip link delete bcrdma0
```

Soft-RoCE is a functional check. Throughput, tail latency and the benefit of CQ
polling need separate measurements on physical RDMA hardware; this example
does not report benchmark numbers.
