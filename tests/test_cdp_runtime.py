"""Offline regressions for the deadline-bounded CDP client.

Run: python tests/test_cdp_runtime.py
Uses an in-memory fake websocket; no Chrome, no network.
"""
import importlib.util
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import websocket

spec = importlib.util.spec_from_file_location("cdp_runtime_target", ROOT / "cdp.py")
cdp = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = cdp
spec.loader.exec_module(cdp)


class FakeSocket:
    def __init__(self):
        self.sent = []
        self.recv_calls = 0
        self.recv_behavior = lambda: b'{"id": 0}'
        self.timeout = None
        self.aborted = False

    def settimeout(self, value):
        self.timeout = value

    def send(self, payload):
        self.sent.append(payload)

    def recv(self):
        self.recv_calls += 1
        return self.recv_behavior()

    def abort(self):
        self.aborted = True

    def shutdown(self):
        pass

    def close(self):
        pass


def make_client(fake):
    with patch.object(cdp.websocket, "create_connection", return_value=fake):
        return cdp.CDP("ws://fake", timeout=5)


def test_disconnected_socket_fails_fast():
    fake = FakeSocket()
    fake.recv_behavior = lambda: (_ for _ in ()).throw(ConnectionError("closed"))
    client = make_client(fake)
    started = time.monotonic()
    try:
        client.send("Runtime.evaluate", timeout=5)
        raise AssertionError("expected CDPError")
    except cdp.CDPError:
        pass
    elapsed = time.monotonic() - started
    assert elapsed < 0.5, f"disconnect spun for {elapsed:.2f}s"
    assert fake.recv_calls <= 2, f"busy loop detected: {fake.recv_calls} recv calls"
    assert fake.aborted, "client must abort the socket after disconnect"
    print("PASS: CDP disconnect fails fast instead of busy-looping")


def test_recv_timeout_aborts_within_deadline():
    fake = FakeSocket()

    def slow():
        time.sleep(0.3)
        raise websocket.WebSocketTimeoutException()

    fake.recv_behavior = slow
    client = make_client(fake)
    started = time.monotonic()
    try:
        client.send("Runtime.evaluate", timeout=0.2)
        raise AssertionError("expected TimeoutError")
    except TimeoutError:
        pass
    elapsed = time.monotonic() - started
    assert elapsed < 1.0, f"timeout took {elapsed:.2f}s"
    assert fake.aborted, "socket must be aborted after deadline"
    try:
        client.send("Runtime.evaluate", timeout=1)
        raise AssertionError("closed client must reject sends")
    except cdp.CDPError:
        pass
    print("PASS: CDP recv timeout aborts within the deadline")


def test_lock_wait_is_bounded():
    fake = FakeSocket()
    client = make_client(fake)
    client._lock.acquire()
    result = {}

    def attempt():
        started = time.monotonic()
        try:
            client.send("Runtime.evaluate", timeout=0.15)
        except TimeoutError as exc:
            result["elapsed"] = time.monotonic() - started
            result["message"] = str(exc)
        else:
            result["error"] = "no timeout"

    thread = threading.Thread(target=attempt)
    thread.start()
    thread.join(timeout=3)
    assert "elapsed" in result, result
    assert result["elapsed"] < 1.0, result
    assert "锁" in result["message"], result
    client._lock.release()
    print("PASS: CDP lock waiting is bounded by the deadline")


def test_matching_response_still_returns():
    fake = FakeSocket()
    client = make_client(fake)
    fake.recv_behavior = lambda: b'{"id": 1, "result": {"result": {"value": 42}}}'
    assert client.js("1+1") == 42
    assert not fake.aborted
    print("PASS: normal CDP round-trip still works")


if __name__ == "__main__":
    test_disconnected_socket_fails_fast()
    test_recv_timeout_aborts_within_deadline()
    test_lock_wait_is_bounded()
    test_matching_response_still_returns()
    print("ALL CDP RUNTIME TESTS PASSED")
