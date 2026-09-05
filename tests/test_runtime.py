"""Regression checks. Privileged namespace checks require make integration."""
import ctypes
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import unittest

PROJECT = Path(__file__).resolve().parents[1]
ENGINE = str(PROJECT / "engine")


class Request(ctypes.Structure):
    _fields_ = [("kind", ctypes.c_int), ("container_id", ctypes.c_char * 64),
                ("rootfs", ctypes.c_char * 4096), ("command", ctypes.c_char * 4096),
                ("args", (ctypes.c_char * 4096) * 8), ("arg_count", ctypes.c_int),
                ("soft", ctypes.c_ulong), ("hard", ctypes.c_ulong), ("nice", ctypes.c_int)]


class Response(ctypes.Structure):
    _fields_ = [("status", ctypes.c_int), ("length", ctypes.c_uint),
                ("more", ctypes.c_int), ("message", ctypes.c_char * 8192)]


def receive(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise EOFError("truncated frame")
        data.extend(part)
    return bytes(data)


def frame(data=b"ok\n", status=0, more=0):
    result = Response(status=status, length=len(data), more=more)
    ctypes.memmove(ctypes.addressof(result) + Response.message.offset, data, len(data))
    return bytes(result)


class CliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="mcr-cli-")
        self.addCleanup(self.temp.cleanup)
        self.env = dict(os.environ, MINI_RUNTIME_DIR=self.temp.name)

    def cli(self, *args):
        return subprocess.run([ENGINE, *map(str, args)], env=self.env, capture_output=True, timeout=15)

    def exchange(self, args, output):
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(self.temp.name + "/control.sock")
        listener.listen(1)
        listener.settimeout(5)
        received, errors = [], []

        def server():
            try:
                with listener.accept()[0] as client:
                    client.settimeout(5)
                    received.append(Request.from_buffer_copy(receive(client, ctypes.sizeof(Request))))
                    # Split the response to exercise partial socket reads.
                    client.sendall(output[:7])
                    client.sendall(output[7:])
            except Exception as error:
                errors.append(error)

        worker = threading.Thread(target=server)
        worker.start()
        try:
            result = self.cli(*args)
        finally:
            worker.join(timeout=6)
            listener.close()
        self.assertFalse(worker.is_alive())
        self.assertFalse(errors, errors)
        return result, received[0]

    def test_help(self):
        self.assertEqual(self.cli("--help").returncode, 0)

    def test_invalid_arguments_rejected_before_connect(self):
        root = self.temp.name
        invalid = [["logs", "../escape"], ["stop", "x" * 64], ["ps", "extra"],
                   ["start", "id", root, "/fixture", *map(str, range(9))],
                   ["run", "id", "/", "/bin/sh"], ["run", "id", root, "relative"]]
        for option, value in [("--nice", ""), ("--nice", "20"), ("--nice", "-21"),
                              ("--soft-mib", "0"), ("--soft-mib", "-1"),
                              ("--hard-mib", "40"), ("--soft-mib", "999999999999999999999")]:
            invalid.append(["start", "id", root, "/fixture", option, value])
        invalid.append(["start", "id", root, "/fixture", "--nice"])
        for args in invalid:
            with self.subTest(args=args):
                result = self.cli(*args)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn(b"connect", result.stderr)

    def test_command_flags_and_literal_separator(self):
        result, request = self.exchange(
            ["run", "id", self.temp.name, "/bin/sh", "--nice", "-10", "-c", "echo hi", "--", "--soft-mib", "literal"],
            frame(b"running\n", more=1) + frame(b"done\n", status=37))
        self.assertEqual(result.returncode, 37)
        self.assertEqual(request.nice, -10)
        self.assertEqual(request.soft, 40 << 20)
        self.assertEqual([request.args[i].value for i in range(request.arg_count)],
                         [b"-c", b"echo hi", b"--soft-mib", b"literal"])

    def test_binary_streamed_response(self):
        result, _ = self.exchange(["logs", "id"], frame(b"a\x00b", more=1) + frame(b"end"))
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"a\x00bend")

    def test_truncated_response_is_failure(self):
        result, _ = self.exchange(["ps"], frame()[:20])
        self.assertNotEqual(result.returncode, 0)

    def test_run_disconnect_is_failure(self):
        result, _ = self.exchange(["run", "id", self.temp.name, "/fixture"], frame(more=1))
        self.assertNotEqual(result.returncode, 0)

    def test_invalid_frame_is_failure(self):
        response = Response(length=9000)
        result, _ = self.exchange(["ps"], bytes(response))
        self.assertNotEqual(result.returncode, 0)

    def test_workload_validation(self):
        for binary in ("cpu_hog", "memory_hog", "io_pulse"):
            for value in ("-1", "0", "abc", "18446744073709551616"):
                with self.subTest(binary=binary, value=value):
                    result = subprocess.run([str(PROJECT / binary), value], capture_output=True, timeout=5)
                    self.assertNotEqual(result.returncode, 0)

    def test_io_workload_output(self):
        output = Path(self.temp.name) / "pulse"
        result = subprocess.run([str(PROJECT / "io_pulse"), "3", "0", str(output)], capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(output.read_text(), "io_pulse iteration=1\nio_pulse iteration=2\nio_pulse iteration=3\n")

    def test_optimized_memory_workload_grows_rss(self):
        with subprocess.Popen([str(PROJECT / "memory_hog"), "8", "100"], stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
            try:
                for _ in range(4):
                    self.assertTrue(proc.stdout.readline().startswith(b"allocation="))
                status = Path(f"/proc/{proc.pid}/status").read_text()
                rss_kib = int(re.search(r"VmRSS:\s+(\d+)", status).group(1))
                self.assertGreater(rss_kib, 28 * 1024)
            finally:
                proc.terminate()
                proc.communicate(timeout=5)
            self.assertEqual(proc.returncode, 0)


@unittest.skipUnless(os.environ.get("MINI_RUNTIME_INTEGRATION") == "1", "requires make integration as root")
class SupervisorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.geteuid() != 0:
            raise RuntimeError("Integration tests require root and Linux namespace capabilities")
        probe = subprocess.run(["unshare", "--mount", "--pid", "--fork", "true"], capture_output=True)
        if probe.returncode:
            raise RuntimeError(probe.stderr.decode())

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="mcr-int-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.runtime = self.base / "runtime"
        self.env = dict(os.environ, MINI_RUNTIME_DIR=str(self.runtime))
        self.root = self.new_root("root")
        self.errors = tempfile.TemporaryFile()
        self.addCleanup(self.errors.close)
        self.supervisor = subprocess.Popen([ENGINE, "supervisor"], env=self.env, stdout=self.errors, stderr=self.errors)
        self.addCleanup(self.shutdown)
        self.wait_until(lambda: (self.runtime / "control.sock").exists())

    def new_root(self, name):
        root = self.base / name
        root.mkdir()
        (root / "proc").mkdir()
        shutil.copy2(PROJECT / "tests/fixture", root / "fixture")
        return root

    def tearDown(self):
        self.shutdown()
        self.errors.seek(0)
        diagnostics = self.errors.read()
        self.assertNotIn(b"ERROR: AddressSanitizer", diagnostics)
        self.assertNotIn(b"ERROR: LeakSanitizer", diagnostics)
        self.assertNotIn(b"runtime error:", diagnostics)

    def shutdown(self):
        if self.supervisor.poll() is None:
            self.supervisor.terminate()
            try:
                self.supervisor.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.supervisor.kill()
                self.supervisor.wait()
                self.fail("supervisor failed to shut down")

    def cli(self, *args, expected=0):
        result = subprocess.run([ENGINE, *map(str, args)], env=self.env, capture_output=True, timeout=15)
        if expected is not None:
            self.assertEqual(result.returncode, expected, (result.stdout, result.stderr))
        return result

    def wait_until(self, predicate, timeout=8):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if predicate():
                return
            time.sleep(0.03)
        self.fail("condition timed out")

    def start_hold(self, name="hold", root=None):
        result = self.cli("start", name, root or self.root, "/fixture", "hold")
        pid = int(re.search(rb"pid (\d+)", result.stdout).group(1))
        self.wait_until(lambda: b"ready" in self.cli("logs", name).stdout)
        return pid

    def test_namespace_and_descriptor_isolation(self):
        self.cli("run", "isolated", self.root, "/fixture", "inspect")
        self.assertEqual(self.cli("logs", "isolated").stdout,
                         b"pid=1 hostname=isolated stdin=eof descriptors=clean\n")

    def test_exit_code_and_exec_error(self):
        self.cli("run", "exit37", self.root, "/fixture", "exit", "37", expected=37)
        self.cli("run", "missing", self.root, "/missing", expected=127)
        self.assertIn(b"child: execv", self.cli("logs", "missing").stdout)

    def test_large_binary_logs_flushed_before_run_returns(self):
        self.cli("run", "large", self.root, "/fixture", "emit")
        self.assertEqual(self.cli("logs", "large").stdout, bytes(range(256)) * 4096)

    def test_rootfs_aliases_and_duplicate_id(self):
        self.start_hold()
        alias = self.base / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        for root in (alias, str(self.root) + "/../root"):
            result = self.cli("start", "collision", root, "/fixture", "hold", expected=1)
            self.assertIn(b"rootfs already in use", result.stdout)
        self.cli("start", "hold", self.new_root("second"), "/fixture", "hold", expected=1)

    def test_stop_does_not_block_ps_and_reaps_child(self):
        pid = self.start_hold()
        start = time.monotonic()
        self.cli("stop", "hold")
        self.cli("ps")
        self.assertLess(time.monotonic() - start, 1.5)
        self.wait_until(lambda: b"stopped" in self.cli("ps").stdout)
        self.assertFalse(Path(f"/proc/{pid}").exists())

    def test_second_supervisor_preserves_first_socket(self):
        result = self.cli("supervisor", expected=1)
        self.assertIn(b"another supervisor", result.stderr)
        self.assertIn(b"No containers", self.cli("ps").stdout)

    def test_malformed_and_partial_requests(self):
        for kind, count, identifier in [(1, 9, b"bad"), (1, -1, b"bad"), (4, 0, b"../escape"), (99, 0, b"bad")]:
            req = Request(kind=kind, arg_count=count, container_id=identifier)
            with socket.socket(socket.AF_UNIX) as sock:
                sock.settimeout(5)
                sock.connect(str(self.runtime / "control.sock"))
                sock.sendall(bytes(req))
                response = Response.from_buffer_copy(receive(sock, ctypes.sizeof(Response)))
                self.assertEqual(response.status, 1)
        with socket.socket(socket.AF_UNIX) as sock:
            sock.connect(str(self.runtime / "control.sock"))
            sock.sendall(b"x")
            self.cli("ps")  # Must recover after the 2-second request deadline.

    def test_disconnected_run_client_does_not_kill_supervisor(self):
        req = Request(kind=2, container_id=b"disconnected", rootfs=os.fsencode(self.root),
                      command=b"/fixture", arg_count=2, soft=40 << 20, hard=64 << 20)
        req.args[0].value, req.args[1].value = b"exit", b"0"
        with socket.socket(socket.AF_UNIX) as sock:
            sock.connect(str(self.runtime / "control.sock"))
            sock.sendall(bytes(req))
        self.wait_until(lambda: b"exited" in self.cli("ps").stdout)

    def test_failed_run_does_not_leak_descriptors(self):
        self.start_hold()
        fd_dir = Path(f"/proc/{self.supervisor.pid}/fd")
        before = len(list(fd_dir.iterdir()))
        for _ in range(12):
            self.cli("run", "hold", self.root, "/fixture", "exit", expected=1)
        self.assertLessEqual(len(list(fd_dir.iterdir())), before)

    def test_shutdown_kills_stubborn_children_and_removes_socket(self):
        pids = [self.start_hold("one"), self.start_hold("two", self.new_root("two"))]
        self.shutdown()
        self.assertEqual(self.supervisor.returncode, 0)
        self.assertFalse((self.runtime / "control.sock").exists())
        for pid in pids:
            self.assertFalse(Path(f"/proc/{pid}").exists())

    def test_log_symlink_and_hardlink_are_rejected_without_truncation(self):
        target = self.base / "untouched"
        target.write_text("keep this")
        log = self.runtime / "logs" / "unsafe.log"
        log.symlink_to(target)
        self.cli("start", "unsafe", self.root, "/fixture", "exit", expected=1)
        self.cli("logs", "unsafe", expected=1)
        log.unlink()
        os.link(target, log)
        self.cli("start", "unsafe", self.root, "/fixture", "exit", expected=1)
        self.assertEqual(target.read_text(), "keep this")

    def test_full_history_is_bounded_and_ps_is_not_truncated(self):
        identifiers = [f"record-{i:02d}-" + "x" * 53 for i in range(64)]
        for name in identifiers:
            self.cli("run", name, self.root, "/fixture", "exit", "0",
                     "--soft-mib", "1000000000000", "--hard-mib", "1000000000001")
        output = self.cli("ps").stdout
        self.assertGreater(len(output), 8192)
        for name in identifiers:
            self.assertIn(name.encode(), output)
        result = self.cli("run", "overflow", self.root, "/fixture", "exit", "0", expected=1)
        self.assertIn(b"history is full", result.stdout)


if __name__ == "__main__":
    unittest.main()
