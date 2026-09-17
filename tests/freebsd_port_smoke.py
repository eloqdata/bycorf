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

"""Check the BSD port with real Linux TCP clients, natively or under QEMU.

Requires root/CAP_NET_ADMIN and free 198.19.0.0/24 and 198.19.1.0/24 subnets.
Owns two temporary TAPs; does not use DPDK, io_uring, or physical NICs.
"""
import argparse
import concurrent.futures
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import struct
import subprocess
import tempfile
import time


def command(*args):
    subprocess.run(args, check=True)


def receive(sock, size):
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        assert chunk, f"early EOF at {len(result)}/{size} bytes"
        result.extend(chunk)
    return result


def exchange(case):
    worker, index = case
    address = (f"198.19.{worker}.2", 16390)
    with socket.create_connection(address, 15) as sock:
        sock.settimeout(30)
        # Vary odd/even lengths across TCP segment and mbuf boundaries. Both
        # VNET owners run concurrently, including while their peer is idle.
        for size in (1, 31, 64, 1023, 4097, 65536, 131073 + index):
            payload = os.urandom(size)
            sock.sendall(payload)
            assert receive(sock, size) == payload, "TCP stream corruption"
        sock.shutdown(socket.SHUT_WR)
        assert sock.recv(1) == b"", "half-close did not drain the stream"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path, help="standalone freebsd_port_check")
    parser.add_argument("--runner", default="", help="e.g. 'qemu-x86_64 -L /usr/x86_64-linux-gnu'")
    parser.add_argument("--loss", action="store_true", help="drop 2%% of TAP packets")
    args = parser.parse_args()
    routes = json.loads(subprocess.check_output(["ip", "-j", "route", "show", "table", "all"]))
    for worker in range(2):
        subnet = ipaddress.ip_network(f"198.19.{worker}.0/24")
        for route in routes:
            destination = route.get("dst", "default")
            if destination == "default":
                continue
            existing = ipaddress.ip_network(destination, strict=False)
            if existing.version == 4 and subnet.overlaps(existing):
                raise RuntimeError(f"test subnet {subnet} overlaps route {existing}")
    directory = Path(tempfile.mkdtemp(prefix="bycorf-bsd-port-"))
    path = directory / "server.log"
    print(f"Logs: {directory}", flush=True)
    fds, live = [], []
    child = None
    try:
        for worker in range(2):
            name = f"cb{os.getpid()}{worker}"
            if Path(f"/sys/class/net/{name}").exists():
                raise RuntimeError(f"{name} already exists")
            fd = os.open("/dev/net/tun", os.O_RDWR | os.O_NONBLOCK | os.O_CLOEXEC)
            fds.append(fd)
            # TUNSETIFF, IFF_TAP | IFF_NO_PI. Closing the last descriptor
            # removes this nonpersistent interface and its routes/qdisc.
            fcntl.ioctl(fd, 0x400454CA, struct.pack("16sH", name.encode(), 0x1002))
            command("ip", "link", "set", name, "address", f"02:00:00:00:{worker:02x}:01")
            command("ip", "address", "add", f"198.19.{worker}.1/24", "dev", name)
            command("ip", "link", "set", name, "up")
            if args.loss:
                command("tc", "qdisc", "add", "dev", name, "root", "netem", "loss", "2%")
        with path.open("w") as log:
            child = subprocess.Popen(
                shlex.split(args.runner) + [str(args.binary.resolve()), *map(str, fds)],
                pass_fds=fds, stdout=log, stderr=subprocess.STDOUT)
            until = time.monotonic() + 60
            while not all(f"READY worker {worker}" in path.read_text() for worker in range(2)):
                if child.poll() is not None:
                    raise RuntimeError(f"stack startup exited: {child.returncode}")
                if time.monotonic() > until:
                    raise TimeoutError("stack startup")
                time.sleep(.05)
            with concurrent.futures.ThreadPoolExecutor(8) as clients:
                list(clients.map(exchange, [(worker, index)
                                           for index in range(20) for worker in range(2)]))
            for worker in range(2):
                live.append(socket.create_connection((f"198.19.{worker}.2", 16390), 15))
            for wave in range(8):
                time.sleep(.15)
                for worker, sock in enumerate(live):
                    sock.settimeout(15)
                    payload = f"worker {worker}, idle wave {wave}".encode()
                    sock.sendall(payload)
                    assert receive(sock, len(payload)) == payload
            child.send_signal(signal.SIGTERM)
            child.wait(timeout=15)
            assert child.returncode == 0, f"shutdown exit {child.returncode}"
        stats = re.findall(r"DONE worker (\d+): accepted=(\d+) echoed=(\d+)", path.read_text())
        assert {int(worker) for worker, _, _ in stats} == {0, 1}, stats
        assert all(int(accepted) == 21 and int(echoed) > 4_000_000
                   for _, accepted, echoed in stats), stats
        print("PASS: two worker VNETs, 40 binary streams, half-close, "
              "16 idle resumes, shutdown with live sockets", flush=True)
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for sock in live:
            sock.close()
        for fd in fds:
            os.close(fd)
        if path.exists():
            print(path.read_text(), flush=True)


if __name__ == "__main__":
    main()
