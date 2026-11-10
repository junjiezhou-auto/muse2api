"""Offline regressions for the bounded generation pool and streaming bridge.

Run: python tests/test_runtime_concurrency.py
No network, no real accounts, no Chromium: engine and store are patched.
"""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runtime import QueueFullError, StreamBridge, WorkerPool


def test_worker_pool_bounded_and_parallel():
    pool = WorkerPool(2, 1, name="unit")
    barrier = threading.Barrier(2, timeout=3)

    def job():
        barrier.wait()
        return "ok"

    f1 = pool.submit(job)
    f2 = pool.submit(job)
    assert f1.result(timeout=3) == "ok"
    assert f2.result(timeout=3) == "ok"

    # workers + 1 pending are admitted while busy...
    blocker = threading.Event()
    hold1 = pool.submit(blocker.wait, 3)
    hold2 = pool.submit(blocker.wait, 3)
    queued = pool.submit(lambda: "never-run-yet")
    deadline = time.monotonic() + 3
    snap = pool.snapshot()
    while snap["active"] < 2 and time.monotonic() < deadline:
        time.sleep(0.02)
        snap = pool.snapshot()
    assert snap["active"] == 2 and snap["queued"] == 1, snap

    # ...beyond capacity admission fails instead of spawning threads.
    try:
        pool.submit(lambda: None)
        raise AssertionError("expected QueueFullError")
    except QueueFullError:
        pass

    blocker.set()
    assert hold1.result(timeout=3) is True
    assert hold2.result(timeout=3) is True
    assert queued.result(timeout=3) == "never-run-yet"
    assert pool.snapshot()["active"] == 0 and pool.snapshot()["queued"] == 0
    pool.shutdown(timeout=2)
    print("PASS: worker pool is bounded, parallel and rejects overflow")


def test_stream_bridge_delivery_and_error():
    async def main():
        bridge = StreamBridge(max_items=8)

        def produce():
            for i in range(5):
                assert bridge.put(i) is True
            bridge.finish()

        threading.Thread(target=produce, daemon=True).start()
        got = [value async for value in bridge]
        assert got == [0, 1, 2, 3, 4], got

        failing = StreamBridge(max_items=8)
        threading.Thread(target=lambda: (failing.put("a"), failing.finish(ValueError("boom"))),
                         daemon=True).start()
        items = []
        try:
            async for value in failing:
                items.append(value)
            raise AssertionError("expected ValueError")
        except ValueError:
            pass
        assert items == ["a"], items
    asyncio.run(main())
    print("PASS: stream bridge delivers items and propagates errors")


def test_stream_bridge_cancel_unblocks_producer():
    async def main():
        bridge = StreamBridge(max_items=2)
        produced = []

        def produce():
            for i in range(50):
                if bridge.put(i) is False:
                    return
                produced.append(i)

        thread = threading.Thread(target=produce, daemon=True)
        thread.start()
        await asyncio.sleep(0.05)
        bridge.cancel()
        await asyncio.sleep(0.05)
        assert not thread.is_alive(), "producer must stop after cancel"
        assert len(produced) <= 2 + 1, produced
        async for _ in bridge:
            raise AssertionError("cancelled bridge must not yield")
    asyncio.run(main())
    print("PASS: stream bridge cancel unblocks the producer")


def _load_app_module():
    home = tempfile.mkdtemp(prefix="muse2api-concurrency-")
    os.environ.update(MUSE2API_HOME=home, MUSE2API_PROFILE_ROOT=home,
                      MUSE2API_KEY="test-only", MUSE2API_CONCURRENCY="2")
    spec = importlib.util.spec_from_file_location("runtime_concurrency_target", ROOT / "app.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.store.add_account({"hatch_sess": "a"}, "a")
    module.store.add_account({"hatch_sess": "b"}, "b")
    return module


class Client:
    def __init__(self, asgi_app, headers):
        self.asgi_app, self.headers = asgi_app, headers

    async def request(self, method, path, json_body=None):
        body = b"" if json_body is None else json.dumps(json_body).encode()
        headers = dict(self.headers)
        if json_body is not None:
            headers["content-type"] = "application/json"
        headers["content-length"] = str(len(body))
        scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
                 "method": method, "scheme": "http", "path": path,
                 "raw_path": path.encode(), "query_string": b"", "root_path": "",
                 "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
                 "server": ("test", 80), "client": ("test", 1)}
        messages = []
        sent = False

        async def receive():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.Event().wait()

        async def send(message):
            messages.append(message)

        await self.asgi_app(scope, receive, send)
        status = next(m["status"] for m in messages if m["type"] == "http.response.start")
        payload = b"".join(m.get("body", b"") for m in messages)
        return status, payload

    async def get(self, path):
        return await self.request("GET", path)

    async def post(self, path, json_body=None):
        return await self.request("POST", path, json_body=json_body)


def test_streaming_chat_keeps_event_loop_responsive():
    module = _load_app_module()

    def fake_chat_stream(cookies, prompt, expires, timeout, account_id=None, stop_event=None):
        yield "hello"
        for _ in range(20):
            if stop_event is not None and stop_event.is_set():
                return
            time.sleep(0.05)
        yield "world"

    with patch.object(module, "_renew_and_persist",
                      side_effect=lambda ident, **_: module.store.get_account(ident)), \
         patch.object(module, "_sync_cookies", lambda *_: None), \
         patch.object(module, "_bind_account_proxy", lambda *_: None), \
         patch.object(module, "_clear_account_proxy", lambda *_: None), \
         patch.object(module.engine, "start"), \
         patch.object(module.engine, "close_current_page"), \
         patch.object(module.engine, "chat_stream", fake_chat_stream):
        async def scenario():
            client = Client(module.app, {"Authorization": "Bearer test-only"})
            request = asyncio.create_task(client.post(
                "/v1/chat/completions",
                {"messages": [{"role": "user", "content": "hi"}], "stream": True}))
            await asyncio.sleep(0.3)  # let the worker submit and the stream start
            started = time.monotonic()
            status, payload = await client.get("/healthz")
            elapsed = time.monotonic() - started
            assert status == 200, payload
            assert elapsed < 0.4, f"healthz blocked for {elapsed:.2f}s while streaming"
            body = await request
            assert body[0] == 200, body[1][:400]
            assert b"[DONE]" in body[1]
            text = ""
            for line in body[1].decode().splitlines():
                if line.startswith("data: ") and line != "data: [DONE]":
                    event = json.loads(line[6:])
                    choices = event.get("choices") or [{}]
                    piece = choices[0].get("delta", {}).get("content")
                    if piece:
                        text += piece
            assert text == "helloworld", text
        asyncio.run(asyncio.wait_for(scenario(), timeout=15))

    assert not module.GEN_LOCK.locked(), "GEN_LOCK leaked after stream"
    assert not module.ACCOUNT_POOL.busy_accounts, "account lease leaked after stream"
    print("PASS: streaming chat keeps the event loop responsive and releases leases")


def test_queue_full_returns_429():
    module = _load_app_module()
    tiny = WorkerPool(1, 0, name="tiny")
    blocker = threading.Event()
    with patch.object(module, "GEN_POOL", tiny):
        held = tiny.submit(blocker.wait, 5)

        async def scenario():
            client = Client(module.app, {"Authorization": "Bearer test-only"})
            status, payload = await client.post(
                "/v1/chat/completions", {"messages": [{"role": "user", "content": "hi"}]})
            assert status == 429, (status, payload[:300])
            assert "队列已满" in payload.decode(), payload[:300]
            status, payload = await client.post("/v1/videos", {"prompt": "cat"})
            assert status == 429, (status, payload[:300])
            tasks = module.store.list_tasks(10)
            assert tasks and tasks[0]["status"] == "failed", tasks
        asyncio.run(asyncio.wait_for(scenario(), timeout=10))
        blocker.set()
        assert held.result(timeout=5) is True
    tiny.shutdown(timeout=2)
    print("PASS: saturated pool returns 429 and marks the video task failed")


def test_chat_core_cancel_releases_resources():
    module = _load_app_module()

    def slow_stream(cookies, prompt, expires, timeout, account_id=None, stop_event=None):
        while stop_event is None or not stop_event.is_set():
            yield "tick"
            time.sleep(0.02)

    stop = threading.Event()
    seen = []

    def emit(chunk):
        seen.append(chunk)
        return True

    with patch.object(module, "_renew_and_persist",
                      side_effect=lambda ident, **_: module.store.get_account(ident)), \
         patch.object(module, "_sync_cookies", lambda *_: None), \
         patch.object(module, "_bind_account_proxy", lambda *_: None), \
         patch.object(module, "_clear_account_proxy", lambda *_: None), \
         patch.object(module.engine, "start"), \
         patch.object(module.engine, "close_current_page"), \
         patch.object(module.engine, "chat_stream", slow_stream):
        worker = threading.Thread(
            target=module._chat_stream_core,
            args=("prompt", 5, None, emit, stop), daemon=True)
        worker.start()
        time.sleep(0.15)
        stop.set()
        worker.join(timeout=5)
        assert not worker.is_alive(), "core must exit promptly after stop"
    assert seen, "core should have streamed chunks"
    assert not module.GEN_LOCK.locked()
    assert not module.ACCOUNT_POOL.busy_accounts
    print("PASS: cancelling the chat core releases the browser slot and account")


if __name__ == "__main__":
    test_worker_pool_bounded_and_parallel()
    test_stream_bridge_delivery_and_error()
    test_stream_bridge_cancel_unblocks_producer()
    test_streaming_chat_keeps_event_loop_responsive()
    test_queue_full_returns_429()
    test_chat_core_cancel_releases_resources()
    print("ALL RUNTIME CONCURRENCY TESTS PASSED")
