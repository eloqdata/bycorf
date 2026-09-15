#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Build the private FreeBSD kernel namespace without host libc headers."""

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def run(args, **kwargs):
    result = subprocess.run(args, capture_output=True, text=True, **kwargs)
    if result.returncode:
        raise RuntimeError(" ".join(map(str, args)) + "\n" + result.stdout + result.stderr)
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cc", default="clang")
    parser.add_argument("--target", help="Clang target triple (default: compiler's native target)")
    for tool in ("ld", "nm", "objcopy", "ar"):
        parser.add_argument(f"--{tool}", help=f"Override the compiler-selected {tool}")
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args()
    compiler = [args.cc] + ([f"--target={args.target}"] if args.target else [])
    macros = run(compiler + ["-dM", "-E", "-x", "c", "-"], input="")
    if "#define __linux__ 1" not in macros or "#define __LP64__ 1" not in macros:
        raise RuntimeError("The FreeBSD host port requires a 64-bit Linux target")
    if "#define __aarch64__ 1" in macros:
        architecture = "aarch64"
        directories = {"machine": "arm64/include", "arm": "arm/include"}
    elif "#define __x86_64__ 1" in macros:
        architecture = "x86_64"
        directories = {"machine": "amd64/include", "x86": "x86/include"}
    else:
        raise RuntimeError("The FreeBSD host port supports AArch64 and x86-64")
    # The build host may differ from the target. Use one compiler target for
    # headers, structure offsets, object files, and the binutils that read them.
    tools = {name: getattr(args, name) or run(
        compiler + [f"--print-prog-name={name}"]).strip()
        for name in ("ld", "nm", "objcopy", "ar")}
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    source = args.source.resolve()
    output = args.output.resolve()
    upstream = source / "third_party/freebsd"
    kernel = upstream / "sys"
    port = source / "src/io/freebsd"
    include = output / "include"
    include.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((upstream / "UPSTREAM.json").read_text())
    for name, digest in manifest["files"].items():
        if hashlib.sha256((upstream / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"FreeBSD source differs from pinned upstream: {name}")

    # Legacy kernel clock variables are snapshots for this worker. TLS avoids
    # C data races and prevents an idle RX owner from freezing another VNET's
    # TCP clock. Generate just these declaration changes in the private include
    # overlay; imported files and their upstream digests remain untouched.
    for name, variables in (("kernel.h", ("ticks",)),
                            ("time.h", ("time_second", "time_uptime"))):
        header = (kernel / "sys" / name).read_text()
        for variable in variables:
            header, count = re.subn(
                rf"extern volatile (int|time_t)(\s+{variable};)",
                r"extern _Thread_local volatile \1\2", header)
            if count != 1:
                raise RuntimeError(f"FreeBSD clock declaration changed: {variable}")
        (include / "sys").mkdir(exist_ok=True)
        (include / "sys" / name).write_text(header)
    for name in ("machine", "arm", "x86"):
        link = include / name
        target = kernel / directories[name] if name in directories else None
        if link.is_symlink():
            if target is not None and link.readlink() == target:
                continue
            link.unlink()
        if target is not None:
            link.symlink_to(target, target_is_directory=True)

    # Empty option headers disable unselected kernel facilities. The selected
    # global options are forced into every TU so structure layouts agree.
    inputs = list(kernel.rglob("*.h")) + list(kernel.rglob("*.c")) + list(port.rglob("*.c"))
    for path in inputs:
        for name in re.findall(r'#\s*include\s+["<](opt_\w+\.h)',
                               path.read_text(errors="surrogateescape")):
            (include / name).write_text("")
    (include / "opt_inet.h").write_text("#define INET 1\n")
    (include / "opt_global.h").write_text(
        "#define INET 1\n#define VIMAGE 1\n#define SMP 1\n"
        "#define MAXCPU 64\n#define CC_NEWRENO 1\n"
    )
    for name in ("bus", "device"):
        run(["awk", "-f", str(kernel / "tools/makeobjops.awk"),
             str(kernel / f"kern/{name}_if.m"), "-h"], cwd=include)
    for flag in ("-h", "-p", "-q"):
        run(["awk", "-f", str(kernel / "tools/vnode_if.awk"),
             str(kernel / "kern/vnode_if.src"), flag], cwd=include)

    flags = compiler + ["-c", "-std=gnu11", "-O2", "-g", "-ffreestanding", "-nostdinc",
             "-fno-builtin", "-fno-common", "-fno-stack-protector",
             "-ffunction-sections", "-fdata-sections", "-D_KERNEL", "-D__FreeBSD__=15",
             "-Wno-address-of-packed-member", "-Wno-pointer-sign", "-Wno-format",
             "-Wno-incompatible-pointer-types-discards-qualifiers",
             "-include", str(include / "opt_global.h"),
             "-I" + str(port / "compat"), "-I" + str(port), "-I" + str(include),
             "-I" + str(kernel), "-I" + str(kernel / "contrib/ck/include")]
    # genoffset encodes offsetof as common-symbol sizes; -fno-common would
    # encode ordinary BSS addresses instead and silently generate wrong offsets.
    run(flags + ["-fcommon", "-MD", "-MF", str(output / "genoffset.o.d"),
                 str(kernel / "kern/genoffset.c"), "-o", str(output / "genoffset.o")])
    run(["sh", str(kernel / "kern/genoffset.sh"), "-o", str(include / "offset.inc"),
         str(output / "genoffset.o")], env={**os.environ, "NM": tools["nm"]})

    sources = [upstream / line for line in (upstream / "SOURCES").read_text().splitlines() if line]
    sources += sorted(port.glob("*.c"))

    def compile_one(path):
        obj = output / (path.relative_to(source).as_posix().replace("/", "_") + ".o")
        run(flags + ["-MD", "-MF", str(obj) + ".d", str(path), "-o", str(obj)])
        return obj

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as executor:
        futures = [executor.submit(compile_one, path) for path in sources]
        objects, errors = [], []
        for future in futures:
            try:
                objects.append(future.result())
            except RuntimeError as error:
                errors.append(str(error))
    if errors:
        print("\n\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    combined = output / "kernel.o"
    run([tools["ld"], "-r", "-o", str(combined), *map(str, objects)])
    # A missing kernel shim must never silently resolve to an incompatible
    # libc ABI (notably malloc/free/realloc/socket). Only these host primitives,
    # compiler helpers, and linker-generated sets may escape the namespace.
    host_symbols = {"memcmp", "memcpy", "memmove", "memset", "strcasecmp",
                    "strcat", "strcmp", "strcpy", "strlcpy", "strlen",
                    "strncmp", "strncpy", "strnlen", "__udivti3"}
    for line in run([tools["nm"], "--undefined-only", str(combined)]).splitlines():
        name = line.split()[-1]
        linker_set = re.fullmatch(r"__(?:start|stop)_set_\w+", name)
        outlined_atomic = architecture == "aarch64" and re.fullmatch(r"__aarch64_\w+", name)
        if name not in host_symbols and not linker_set and not outlined_atomic:
            raise RuntimeError(f"Unimplemented FreeBSD kernel dependency: {name}")
    symbols = run([tools["nm"], "--defined-only", str(combined)])
    names = []
    for line in symbols.splitlines():
        fields = line.split()
        if len(fields) == 3 and fields[1].isupper() and not fields[2].startswith("celer_bsd_"):
            names.append(fields[2])
    # Keep kernel malloc/free/printf and socket symbols private. Only the
    # explicit C bridge remains visible to Celer and its other dependencies.
    localize = output / "local-symbols.txt"
    localize.write_text("\n".join(names) + "\n")
    run([tools["objcopy"], "--localize-symbols=" + str(localize), str(combined)])
    archive = output / "libceler_freebsd.a"
    archive.unlink(missing_ok=True)
    run([tools["ar"], "rcs", str(archive), str(combined)])
    print(f"Built {len(sources)} FreeBSD/port translation units for {architecture}: {archive}")


if __name__ == "__main__":
    main()
