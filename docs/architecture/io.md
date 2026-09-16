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

`CELER_KERNEL_BYPASS` is the single build option for DPDK networking and SPDK
storage. It defaults to off; enabling it compiles and links both capabilities.
Applications select network and storage independently through
`ConfigureIoBackends` before workers or storage initialization. The defaults are
kernel networking and io_uring storage, even when both capabilities are linked.
Selection freezes at first use for the process lifetime; changing it after
allocation or runtime startup fails. This preserves DMA allocator identity,
registered-resource ownership and native TCP state. Unsupported capabilities
fail explicitly rather than falling back.

`NetBackend` owns the worker's single `IoUringBackend`. Its optional DPDK adapter
borrows this object; kernel sockets, storage, timers and MSG_RING use the same
submission/completion queues. Network-specific operations dispatch to the
selected backend. The worker drives completion polling and batching, and does
not create a second ring or worker pool for either accelerator.

SPDK storage uses DMA buffers, NVMe namespaces and worker-owned queue pairs.
The io_uring selection uses ordinary aligned buffers and kernel file/block I/O.
SPDK completion polling is inactive when storage uses io_uring; in-flight SPDK
storage prevents sleeping before its completions can be polled.

Either accelerator can initialize the one process-wide DPDK EAL, through the
shared `spdk_env_init` wrapper. Calling this wrapper alone does not activate
SPDK storage. The complete device allowlist and memory configuration must be
supplied before the first user initializes EAL. With both backends disabled,
EAL is not initialized. Saving and restoring the initializer's CPU affinity
prevents EAL's control-lcore pin from changing worker placement. DPDK network
workers register their existing native threads with EAL.

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

Sources: `include/celer/io/backend_options.h`, `src/io/backend_options.cpp`,
`include/celer/io/net_backend.h`, `src/io/io_uring_backend.cpp`,
`src/io/storage.cpp`, `src/io/spdk_storage.cpp`, `src/io/dpdk_environment.cpp`,
`cmake/Dpdk.cmake`, `cmake/Freebsd.cmake`, `cmake/build_freebsd.py`.
