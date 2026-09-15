# FreeBSD source subset

These files are copied from the official FreeBSD source repository. This
directory is not a Git submodule. `UPSTREAM.json` records the source revision
and SHA-256 digest of each imported file; `SOURCES` selects translation units.
Each source file retains its upstream copyright and license. `COPYRIGHT`
contains the upstream project's general notices.

Celer's Linux host adaptation is maintained separately in `src/io/freebsd/`.
Kernel headers are private to that adaptation and are never added to an
application's include path. The build verifies every imported file against its recorded digest. Keep host
adaptation outside this directory; an upstream refresh must update the complete
manifest and dependency closure together.

The subset comes from FreeBSD `releng/15.0`, revision
`af58d0db156a4036624d7f8a0bdd4b739a5418d2`. It targets AArch64 and x86-64 and
includes the IPv4/TCP, Ethernet/ARP, routing, socket, mbuf, and supporting kernel sources
listed in `SOURCES`. It has no dependency on a full FreeBSD checkout at build
time and no F-Stack source. `src/io/freebsd/printf.c` retains the upstream
license for its extracted kernel formatting routines.

Architecture headers and Concurrency Kit primitives are selected for the
compiler target (`arm64` or `amd64`/`x86`), while both targets share the native
portable Internet checksum implementation. Host overlays replace kernel pcpu
register access with TLS. The amd64 atomic overlay selects upstream userspace
fences, and direct-map conversions fail explicitly because Linux owns the VM.

`cmake/build_freebsd.py` generates option and interface headers, and changes
three clock declarations to TLS in a private generated header overlay. Its
generated TCP input source also retires the initial-sequence ACK guard before
header prediction can bypass slow ACK processing; the overlay checks its exact
upstream anchors and retains ordinary ACK validation. These explicit overlays
do not modify the imported files. Kernel symbols
are localized into one private object; undefined symbols are checked against
the host bridge's allowlist before archiving.

See the [prototype runbook](../../docs/dpdk-prototype.md) for configuration,
tests, and the limits of the host port.
