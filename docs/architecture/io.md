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

# I/O backends

Every worker retains one io_uring instance for ordinary file I/O, kernel
sockets, completion delivery, timers, and cross-worker MSG_RING wakeups.
Registered storage buffers and network receive buffers have separate
lifecycles. The default network backend uses Linux TCP and provided-buffer
multishot receives, with a per-connection one-shot fallback.

`CELER_WITH_SPDK_STORAGE` enables direct NVMe I/O through SPDK. File paths
continue to use io_uring; SPDK paths select controllers and namespaces and
worker-owned queue pairs. Completion polling stays in the worker loop.
In-flight SPDK storage prevents that worker from sleeping before its storage
completion can be polled.

`CELER_WITH_DPDK` selects the native FreeBSD/DPDK network backend while retaining
io_uring for the other I/O roles. These options share one process-wide EAL,
initialized once through `spdk_env_init`. Saving and restoring the initializer's
CPU affinity prevents EAL's control-lcore pin from changing the application's
worker placement. Network workers register their existing native threads with
EAL; networking does not start a second worker pool.

SPDK and DPDK are direct, independently pinned Git submodules. SPDK is configured
against the single DPDK installation selected by Celer; its nested DPDK is not
part of the build. Static linking retains network and storage driver
constructors without introducing another EAL instance.

The native FreeBSD source subset is copied into `third_party/freebsd` with
upstream license notices, a revision, and file hashes. It is compiled with
private kernel headers and partially linked into an isolated symbol namespace.
Only the explicit `celer_bsd_*` bridge is exposed; kernel malloc, sockets, and
other ABI-incompatible symbols cannot interpose host libc. The build checks
import hashes and rejects unimplemented kernel dependencies. The header
overlay changes legacy kernel clock declarations to worker-local TLS snapshots.
The host port supplies allocation, locks, epochs, timers, nonblocking socket
bridges, and worker-driven task dispatch; it does not embed a BSD scheduler.
It targets AArch64 and x86-64 using private, target-specific headers. Per-CPU
access uses worker TLS and distinct UMA offsets, including SMR reader state;
native kernel register and physical-map assumptions do not cross the host ABI.

The prototype's kernel registries, VNETs, and EAL have process lifetime. It
supports one runtime per process. Network I/O and descriptors stop on worker
shutdown, but the complete BSD kernel subsystem teardown is not implemented.

Sources: `include/celer/io/net_backend.h`, `src/io/io_uring_backend.cpp`,
`src/io/storage.cpp`, `src/io/spdk_storage.cpp`, `src/io/dpdk_environment.cpp`,
`cmake/Dpdk.cmake`, `cmake/Freebsd.cmake`, `cmake/build_freebsd.py`.
