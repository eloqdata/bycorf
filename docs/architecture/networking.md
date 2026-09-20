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

# Networking

`Server` hosts services on the runtime. `TcpService` binds listeners, admits
connections, and starts session coroutines. `TcpStream` exposes asynchronous
byte-stream reads and writes; protocols such as an application's RESP parser
operate above this boundary. The HTTP service and OpenSSL TLS adaptation also
consume the stream abstraction.

A connection allows one pending reader and one pending writer. Closure stops
new operations and retires the connection only after buffers, completions, and
coroutine references drain. Socket operations must execute in the owning
worker. Sessions that can suspend on non-socket work explicitly borrow
connection storage before spawning and release it after their final users
unwind. These holds delay storage reclamation, not transport closure, and must
drain before the owning worker is destroyed. TCP service shutdown posts
listener-close work through each worker's foreign executor when the DPDK
backend is selected.

## Kernel TCP

Linux owns the TCP state and NIC receive processing. An accepted Linux socket
may be assigned to another worker before its session starts. The service uses
round-robin assignment within the service's selected worker set, then retains
that worker for the session. io_uring
completions make received byte ranges available to the stream.

## DPDK and native FreeBSD TCP

The backend selected at process startup configures one Ethernet port and a bounded number of
RX/TX queue pairs, capped by both device capabilities and the worker count.
The first queue-count workers each own one queue pair. All workers own
independent FreeBSD VNETs, listening on the configured shared IPv4 address.

Build-time worker capacity is shared by the backend and private FreeBSD state.
The DPDK environment provides registration slots for those workers plus its
initializer; startup checks both build capacity and remaining EAL slots before
configuring the port. This capacity is independent of physical queue count.
The [runbook](../dpdk-prototype.md#worker-capacity) describes builds and limits.

Port selection uses DPDK's application-available (unowned) port view. A parent
PMD such as netvsc can own an accelerated VF beneath that single visible port;
the parent controls child queues, datapath selection, fallback, and shutdown.
Bycorf does not independently configure or close that owned child port.

With default `hash` steering, a receive owner hashes the TCP tuple before
entering the stack. If another worker owns the flow, the receive owner publishes
the packet to that worker's bounded software ring. The connection owner runs
native Ethernet/ARP/IPv4/TCP processing, accepts the socket, parses application
requests, and produces replies. Its outgoing packets enter the software TX
ring of a queue owner.
Only that owner calls the device's TX burst API. Queue scarcity therefore does
not reduce the number of workers that can own TCP connections.

TCP service worker sets are registered by listening port before runtime
startup and remain immutable. Incoming connections are steered only to the
workers hosting that service. `ConnectTcp` also uses the selected DPDK backend:
native outgoing IPv4 sockets bind disjoint per-worker stripes of ports
32768–60999, skipping configured listeners. An atomic port-ownership table
routes replies to the initiating VNET, retains ownership through TIME_WAIT,
and prevents a listener from reusing a client port within the runtime lifetime.
Connect completions, refusal and cancellation use the same worker-owned
completion contract as kernel connects; IPv6 is rejected rather than sent
through a different network backend.

Optional `rss` steering keeps ordinary TCP packets on the receiving queue's
worker. Service subsets and outgoing-client replies still use explicit software
ownership to reach the correct VNET. It requires one queue pair per worker and
IPv4 TCP RSS support for multiple workers. The PMD's RSS mapping must remain
stable for the process lifetime so every segment reaches its existing VNET;
live RSS remapping and socket migration are unsupported. Control traffic still
fans out to every VNET, and outgoing packets retain the owner-local TX ring.

ARP and ICMP control traffic reach every VNET so their neighbor and connection
state can update. A single worker emits shared-address ARP and echo replies.
TCP state, callouts, and BSD deferred tasks stay with their socket's worker;
application key routing never changes this ownership.

The private build verifies pinned upstream source digests and applies narrow
host overlays. Both TCP ACK processing paths retire the initial-sequence guard
while sequence comparisons remain unambiguous, so long-lived streams cannot
revive that guard after crossing half of the 32-bit sequence space. Ordinary ACK
bounds checks remain active; `tests/freebsd_tcp_sequence_check.cpp` covers this
invariant through the real stack and an in-process Ethernet peer.

BSD sockets use private worker-encoded handles rather than Linux file
descriptors. Bycorf's close and peer-address helpers dispatch these handles to
the stack. Applications that directly pass stream handles to Linux syscalls or
transfer a live socket to another worker need explicit adaptation. Accepted
and outgoing IPv4 TCP use BSD; Unix sockets retain the kernel path. IPv6 DPDK
listeners and clients are unsupported.

The prototype copies MTU-sized frames between DPDK and BSD and copies received
bytes into per-connection buffers. It uses software checksums. Software queues
are bounded: overflow and partial device TX free unaccepted packets, leaving
native TCP responsible for retransmission. This is a functional integration
boundary, not a zero-copy or tuned packet allocator.

## Polling and sleep

Poll mode keeps workers running without arming RX interrupts. Adaptive mode
can wait on the same io_uring used for worker messages. A queue owner enables
its RX interrupt, registers the notification descriptor with io_uring, and
rechecks packet and software queues before sleeping. Packet publication either
prevents sleep or sends a MSG_RING wake to the parked destination. The next
BSD timer deadline bounds the wait; an arriving packet does not need to wait
for a timer tick. RX interrupts are disabled on returning to polling.

A TAP virtual device provides a readable packet descriptor instead of a
physical interrupt eventfd. The notification code preserves that distinction:
reading an eventfd acknowledgement from TAP would consume a packet. If the PMD
cannot provide the required notification capability, its queue owner keeps
polling. Virtual-device tests cover this wake protocol but do not validate
physical NIC interrupt behavior or predict kernel-bypass throughput. The TAP
PMD additionally uses Linux realtime signals for its internal RX trigger in
both modes; this is separate from Bycorf's worker sleep protocol.

Sources: `include/bycorf/net/connection.h`, `src/net/tcp_service.cpp`, `src/net/tcp_listener.cpp`,
`src/net/tcp_stream.cpp`, `src/net/socket_ops.cpp`, `src/io/dpdk_backend.cpp`,
`src/io/freebsd/abi.h`, `src/io/freebsd/`.
