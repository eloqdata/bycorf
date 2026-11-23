#!/usr/bin/env python3
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run real RDMA tests and two-process examples on an explicitly selected host.

Uses an existing Soft-RoCE or hardware device; never changes interfaces, loads
modules or invokes sudo. Explicit device selection makes skips an error.
"""

import argparse
import ipaddress
import os
from pathlib import Path
import selectors
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET


def require_passes(report):
    cases = ET.parse(report).getroot().findall(".//testcase")
    if not cases:
        raise RuntimeError("RDMA test report contains no test cases")
    for case in cases:
        if (case.find("skipped") is not None or case.find("failure") is not None
                or case.find("error") is not None or case.get("status") == "notrun"):
            raise RuntimeError(f"RDMA test did not pass: {case.get('name')}")
    return len(cases)


def wait_ready(process, timeout):
    deadline = time.monotonic() + timeout
    # Read raw bytes: TextIO buffering can hide a second already-read line
    # from select(), and readiness must not depend on log timing.
    data = b""
    with selectors.DefaultSelector() as poll:
        poll.register(process.stdout, selectors.EVENT_READ)
        while time.monotonic() < deadline:
            if not poll.select(max(0, deadline - time.monotonic())):
                break
            chunk = os.read(process.stdout.fileno(), 4096)
            if not chunk:
                raise RuntimeError("RDMA server exited before readiness")
            data += chunk
            while b"\n" in data:
                line, data = data.split(b"\n", 1)
                print(line.decode(errors="replace"), flush=True)
                if line.startswith(b"RDMA_READY "):
                    port = int(line.split()[1])
                    if not 0 < port <= 65535:
                        raise RuntimeError("invalid RDMA server port")
                    return port
    raise RuntimeError("RDMA server readiness timed out")


def run_example(binary, address, size, timeout):
    with tempfile.TemporaryFile(mode="w+") as errors:
        server = subprocess.Popen(
            [str(binary), "server", address, "0", str(size), str(timeout)],
            stdout=subprocess.PIPE, stderr=errors)
        try:
            port = wait_ready(server, timeout)
            client = subprocess.run(
                [str(binary), "client", address, str(port), str(size), str(timeout)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                timeout=timeout + 5, check=True)
            print(client.stdout, end="", flush=True)
            output, _ = server.communicate(timeout=timeout + 5)
            print(output.decode(errors="replace"), end="", flush=True)
            if server.returncode or "RDMA_OK client" not in client.stdout or b"RDMA_OK server" not in output:
                raise RuntimeError("RDMA example did not complete both peers")
        finally:
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
            server.stdout.close()
            errors.seek(0)
            print(errors.read(), end="", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--device", required=True, help="existing RDMA device, e.g. rxe0")
    parser.add_argument("--ip", required=True, help="local numeric IP on that device")
    parser.add_argument("--timeout", type=int, default=30,
                        help="example/readiness deadline in seconds (default: 30)")
    args = parser.parse_args()
    address = str(ipaddress.ip_address(args.ip))
    if not 1 <= args.timeout <= 3600:
        parser.error("--timeout must be between 1 and 3600")
    if Path(args.device).name != args.device or not Path("/sys/class/infiniband", args.device).is_dir():
        parser.error(f"RDMA device {args.device!r} is unavailable; see docs/rdma-testing.md")
    build = args.build_dir.resolve()
    for name in ("bycorf_rdma_demo", "bycorf_rdma_rc_tests", "bycorf_rdma_registration_tests"):
        if not (build / name).is_file():
            parser.error(f"missing {build / name}; build with RDMA, examples and tests enabled")
    env = os.environ.copy()
    env.update(BYCORF_RDMA_TEST_DEVICE=args.device, BYCORF_RDMA_TEST_IP=address)
    with tempfile.TemporaryDirectory(prefix="bycorf-rdma-") as directory:
        report = Path(directory) / "junit.xml"
        subprocess.run(
            ["ctest", "--test-dir", str(build), "-R", r"^bycorf_(RcLoop|RdmaRegistration)\.",
             "--parallel", "1", "--output-on-failure", "--no-tests=error",
             "--output-junit", str(report)],
            env=env, check=True, timeout=900)
        count = require_passes(report)
    for size in (1, 65536, 1048576):
        run_example(build / "bycorf_rdma_demo", address, size, args.timeout)
    print(f"PASS: {count} real-device tests and 3 two-process RDMA exchanges")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, ET.ParseError,
            subprocess.SubprocessError) as error:
        if isinstance(error, subprocess.CalledProcessError) and error.output:
            print(error.output, end="")
        raise SystemExit(f"FAIL: {error}") from error
