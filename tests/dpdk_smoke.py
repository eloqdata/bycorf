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

"""Exercise real Linux TCP clients against the optional DPDK/FreeBSD backend.

Owns a temporary TAP and child process; requires root (or equivalent network
capabilities). Does not bind PCI devices. No third-party Python modules needed.
"""
import argparse
import concurrent.futures
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time


ADDRESS = ("198.18.0.2", 16390)
TAP = "bycorfdp0"


def receive(sock, size):
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise AssertionError(f"early EOF after {len(result)}/{size} bytes")
        result.extend(chunk)
    return result


def exchange(index):
    payload = os.urandom(32768 + index)
    with socket.create_connection(ADDRESS, 8) as sock:
        sock.settimeout(15)
        sock.sendall(payload)
        assert receive(sock, len(payload)) == payload, "TCP stream corruption"
        sock.shutdown(socket.SHUT_WR)
        assert sock.recv(1) == b"", "half-close did not finish the session"


def run(args, mode, directory):
    if Path(f"/sys/class/net/{TAP}").exists():
        raise RuntimeError(f"{TAP} already exists; refusing to alter an existing interface")
    env = os.environ.copy()
    # The harness exclusively owns this TAP. Ignore a caller's physical-device
    # EAL settings so running a smoke test cannot accidentally select a NIC.
    env.pop("BYCORF_EAL_ARGS", None)
    env.pop("BYCORF_DPDK_GATEWAY", None)
    # The fixture owns one queue; multi-worker forwarding uses hash steering.
    env["BYCORF_DPDK_RX_STEERING"] = args.rx_steering
    env.update(BYCORF_DPDK_MODE=mode, BYCORF_DPDK_QUEUES="1",
               BYCORF_DPDK_IP=ADDRESS[0], BYCORF_DPDK_NETMASK="255.255.255.0")
    path = directory / f"{mode}.log"
    with path.open("w") as log:
        child = subprocess.Popen(
            ["taskset", "-c", args.server_cpus, str(args.binary.resolve()),
             ADDRESS[0], str(ADDRESS[1]), str(args.workers), "-1", "dpdk",
             *(["--no-pin-workers"] if args.no_pin_workers else [])], env=env,
            stdout=log, stderr=subprocess.STDOUT)
        live = []
        try:
            until = time.monotonic() + 20
            while True:
                text = path.read_text()
                # PMD configuration changes the TAP MAC. Wait until all VNET
                # interfaces exist before assigning the Linux-side address.
                if text.count("bycorf0: Ethernet address:") == args.workers:
                    break
                if child.poll() is not None:
                    raise RuntimeError(f"startup exited: {child.returncode}")
                if time.monotonic() > until:
                    raise TimeoutError("stack initialization")
                time.sleep(.05)
            subprocess.run(["ip", "link", "set", TAP, "address",
                            "02:00:00:00:00:01"], check=True)
            subprocess.run(["ip", "address", "add", "198.18.0.1/24",
                            "dev", TAP], check=True)
            if args.loss:
                subprocess.run(["tc", "qdisc", "add", "dev", TAP, "root",
                                "netem", "loss", "2%"], check=True)
            with concurrent.futures.ThreadPoolExecutor(8) as clients:
                list(clients.map(exchange, range(args.streams)))
            for _ in range(8):
                live.append(socket.create_connection(ADDRESS, 8))
            # Existing flows must resume after the owner and RX worker sleep.
            # Keep these sockets open through SIGTERM to exercise cancellation.
            for wave in range(10):
                time.sleep(.1)
                for index, sock in enumerate(live):
                    payload = f"idle wave {wave}, connection {index}".encode()
                    sock.sendall(payload)
                    assert receive(sock, len(payload)) == payload
            child.send_signal(signal.SIGTERM)
            child.wait(timeout=15)
            assert child.returncode == 0, f"shutdown exit {child.returncode}"
        finally:
            for sock in live:
                sock.close()
            if child.poll() is None:
                child.send_signal(signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            print(path.read_text(), flush=True)
    statistics = re.findall(r"DPDK worker (\d+): (.*)", path.read_text())
    stats = {int(worker): {name: int(value) for name, value in
                          re.findall(r"(\w+)=(\d+)", values)}
             for worker, values in statistics}
    assert set(stats) == set(range(args.workers)), stats
    if args.workers > 1:
        assert stats[0]["forward_rx"] > 0, stats
        assert sum(stats[i]["forward_tx"] for i in range(1, args.workers)) > 0, stats
    else:
        assert stats[0]["forward_rx"] == stats[0]["forward_tx"] == 0, stats
    assert all(s["rx"] > 0 for s in stats.values()), stats
    if mode == "adaptive":
        assert all(s["waits"] > 0 for s in stats.values()), stats
        assert stats[0]["arms"] > 0 and stats[0]["notifications"] > 0, stats
    else:
        assert all(s["waits"] == 0 for s in stats.values()), stats
    print(f"PASS {mode}: {args.workers} workers, {args.streams} streams, "
          "half-close, 80 idle wakeups, "
          "RX/TX forwarding, shutdown with live sockets", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path, help="DPDK-enabled bycorf_echo")
    parser.add_argument("--server-cpus", default="0,1")
    parser.add_argument("--client-cpus", default="2,3")
    parser.add_argument("--workers", type=int, default=2,
                        help="VNET owners sharing one RX/TX queue (default: 2)")
    parser.add_argument("--no-pin-workers", action="store_true",
                        help="allow oversubscribed correctness tests on a small host")
    parser.add_argument("--rx-steering", choices=("hash", "rss"), default="hash",
                        help="RSS with this single-queue TAP requires --workers=1")
    parser.add_argument("--streams", type=int, default=100,
                        help="number of stream integrity exchanges (default: 100)")
    parser.add_argument("--mode", choices=("poll", "adaptive", "both"), default="both")
    parser.add_argument("--loss", action="store_true", help="exercise TCP retransmissions with 2%% TAP packet loss")
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive; the binary checks its build capacity")
    if args.rx_steering == "rss" and args.workers != 1:
        parser.error("the single-queue TAP needs --workers=1 for RSS steering")
    if args.streams < args.workers:
        parser.error("--streams must be at least --workers")
    os.sched_setaffinity(0, {int(cpu) for cpu in args.client_cpus.split(",")})
    directory = Path(tempfile.mkdtemp(prefix="bycorf-dpdk-smoke-"))
    print(f"Logs: {directory}", flush=True)
    for mode in (("poll", "adaptive") if args.mode == "both" else (args.mode,)):
        run(args, mode, directory)


if __name__ == "__main__":
    main()
