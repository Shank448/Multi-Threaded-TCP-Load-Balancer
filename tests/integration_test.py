#!/usr/bin/env python3
"""End-to-end tests for the TCP load balancer (stdlib only).

Each test starts real processes (backends + load balancer) on localhost and
talks to them over real sockets. Set BIN_DIR to test a different build, e.g.
the ThreadSanitizer build:  make tsan
"""
import collections
import hashlib
import os
import signal
import socket
import subprocess
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.abspath(os.environ.get("BIN_DIR", os.path.join(ROOT, "build")))
LB_PORT = 18080
BACKEND_PORTS = [19101, 19102, 19103]


def wait_port(port, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} never came up")


def recv_line(sock):
    """Read until newline. A closed connection (FIN *or* RST) returns what we have so far."""
    buf = bytearray()
    while not buf.endswith(b"\n"):
        try:
            chunk = sock.recv(1 << 16)
        except ConnectionResetError:
            break
        if not chunk:
            break
        buf += chunk
    return bytes(buf)


def request(msg="hello", timeout=5.0):
    """One short-lived request through the LB; returns the reply line ('' on failure)."""
    with socket.create_connection(("127.0.0.1", LB_PORT), timeout=timeout) as s:
        s.sendall(msg.encode() + b"\n")
        return recv_line(s).decode()


def backend_of(reply):
    return reply[1:reply.index("]")] if reply.startswith("[") else None


class Stack(unittest.TestCase):
    def setUp(self):
        self.procs = {}
        self.logs = []

    def tearDown(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
            p.wait()
        # With the sanitizer builds, any report ends up in the LB log: fail loudly.
        for path in self.logs:
            with open(path, errors="replace") as f:
                text = f.read()
            if os.environ.get("KEEP_LOGS"):
                print(f"\n--- LB log ({self.id()}) ---\n{text}")
            os.unlink(path)
            self.assertNotIn("ThreadSanitizer", text, text)
            self.assertNotIn("AddressSanitizer", text, text)
            self.assertNotIn("runtime error", text, text)

    # -- helpers -----------------------------------------------------------
    def start_backend(self, port, mode="line"):
        self.procs[f"b{port}"] = subprocess.Popen(
            [f"{BIN}/echo_backend", "--port", str(port), "--name", f"b{port}", "--mode", mode],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_port(port)

    def start_lb(self, strategy="rr", ports=BACKEND_PORTS, extra=()):
        log = tempfile.NamedTemporaryFile("w", suffix=".log", delete=False)
        log.close()
        self.logs.append(log.name)
        errfile = open(log.name, "w")
        self.addCleanup(errfile.close)
        args = [f"{BIN}/lb", "--port", str(LB_PORT), "--strategy", strategy,
                "--health-interval", "300", "--connect-timeout", "300",
                "--stats-interval", "0", "--grace", "3", *extra]
        for p in ports:
            args += ["--backend", f"127.0.0.1:{p}"]
        self.procs["lb"] = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=errfile)
        wait_port(LB_PORT)

    def start_stack(self, strategy="rr", mode="line", extra=()):
        for p in BACKEND_PORTS:
            self.start_backend(p, mode)
        self.start_lb(strategy, extra=extra)

    def kill_backend(self, port):
        p = self.procs.pop(f"b{port}")
        p.kill()
        p.wait()

    # -- tests -------------------------------------------------------------
    def test_round_robin_is_perfectly_even(self):
        self.start_stack("rr")
        counts = collections.Counter(backend_of(request(f"r{i}")) for i in range(300))
        self.assertEqual(sorted(counts.values()), [100, 100, 100], counts)

    def test_least_connections_spreads_long_lived_connections(self):
        self.start_stack("lc")
        held, names = [], collections.Counter()
        for i in range(9):  # keep every connection open so `active` counts accumulate
            s = socket.create_connection(("127.0.0.1", LB_PORT), timeout=5)
            s.sendall(f"hold{i}\n".encode())
            names[backend_of(recv_line(s).decode())] += 1
            held.append(s)
        for s in held:
            s.close()
        self.assertEqual(sorted(names.values()), [3, 3, 3], names)

    def test_large_payload_integrity(self):
        self.start_stack("rr")
        payload = os.urandom(3 * 1024 * 1024).hex()  # 6 MB, one line
        with socket.create_connection(("127.0.0.1", LB_PORT), timeout=30) as s:
            s.sendall(payload.encode() + b"\n")
            reply = recv_line(s).decode()
        name, echoed = reply.split("] ", 1)
        self.assertEqual(echoed.rstrip("\n"), payload)

    def test_full_duplex_streaming_does_not_deadlock(self):
        """16 MB streamed both ways at once through a raw echo backend."""
        self.start_stack("rr", mode="raw")
        data = os.urandom(16 * 1024 * 1024)
        received = bytearray()
        with socket.create_connection(("127.0.0.1", LB_PORT), timeout=30) as s:
            def sender():
                s.sendall(data)
                s.shutdown(socket.SHUT_WR)  # half-close must be propagated end to end
            t = threading.Thread(target=sender)
            t.start()
            while True:
                chunk = s.recv(1 << 16)
                if not chunk:
                    break
                received += chunk
            t.join()
        self.assertEqual(len(received), len(data))
        self.assertEqual(hashlib.sha256(received).digest(), hashlib.sha256(data).digest())

    def test_many_concurrent_clients(self):
        self.start_stack("lc", extra=("--threads", "64"))
        ok, bad, lock = [0], [0], threading.Lock()

        def client(i):
            for j in range(5):
                try:
                    good = request(f"c{i}-{j}").startswith("[b")
                except OSError:
                    good = False
                with lock:
                    (ok if good else bad)[0] += 1

        threads = [threading.Thread(target=client, args=(i,)) for i in range(150)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual((ok[0], bad[0]), (750, 0))

    def test_failover_and_recovery(self):
        self.start_stack("rr")
        dead = BACKEND_PORTS[1]
        self.kill_backend(dead)
        # Immediately after the crash, before any health probe has noticed:
        # passive failover must keep every request succeeding.
        early = [backend_of(request(f"e{i}")) for i in range(30)]
        self.assertNotIn(None, early)
        self.assertNotIn(f"b{dead}", early)
        time.sleep(1.0)  # let the active health checker mark it DOWN
        later = collections.Counter(backend_of(request(f"l{i}")) for i in range(60))
        self.assertEqual(set(later), {f"b{BACKEND_PORTS[0]}", f"b{BACKEND_PORTS[2]}"})
        # Bring it back: the health checker must re-admit it automatically.
        self.start_backend(dead)
        time.sleep(1.2)
        after = collections.Counter(backend_of(request(f"a{i}")) for i in range(60))
        self.assertIn(f"b{dead}", after, after)

    def test_all_backends_down_closes_client_cleanly(self):
        self.start_stack("rr")
        for p in BACKEND_PORTS:
            self.kill_backend(p)
        time.sleep(0.8)
        self.assertEqual(request("anyone there?"), "")  # connection closed, no hang

    def test_overload_sheds_load_instead_of_hanging(self):
        """1 worker + queue of 1: further connections are rejected immediately."""
        self.start_backend(BACKEND_PORTS[0])
        self.start_lb("rr", ports=[BACKEND_PORTS[0]], extra=("--threads", "1", "--queue", "1"))
        # The readiness probe in start_lb() briefly occupies the lone worker; make sure it is
        # idle again so the burst below starts from a known state (matters on slow/1-core CI).
        deadline = time.time() + 10
        while not request("warmup").startswith("[b"):  # shed while the probe is still queued
            self.assertLess(time.time(), deadline, "worker never became idle")
            time.sleep(0.05)
        socks = []
        for i in range(6):
            s = socket.create_connection(("127.0.0.1", LB_PORT), timeout=3)
            s.sendall(f"x{i}\n".encode())
            socks.append(s)
        time.sleep(0.5)
        replies = []
        for s in socks:
            s.settimeout(0.5)
            try:
                replies.append(recv_line(s))
            except socket.timeout:
                replies.append(None)  # queued / waiting for a worker
        served = [r for r in replies if r]
        closed = [r for r in replies if r == b""]
        self.assertGreaterEqual(len(closed), 3, replies)  # rejected immediately
        self.assertGreaterEqual(len(served), 1, replies)
        for s in socks:
            s.close()

    def test_graceful_shutdown_exits_zero_and_quickly(self):
        self.start_stack("rr")
        s = socket.create_connection(("127.0.0.1", LB_PORT), timeout=5)
        s.sendall(b"still-open\n")
        self.assertTrue(recv_line(s).startswith(b"[b"))
        lb = self.procs["lb"]
        lb.send_signal(signal.SIGTERM)
        time.sleep(0.3)
        s.close()  # in-flight connection finishes => drain completes before the grace period
        t0 = time.time()
        self.assertEqual(lb.wait(timeout=8), 0)
        self.assertLess(time.time() - t0, 3.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
