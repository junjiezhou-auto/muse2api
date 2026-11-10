"""Offline fault injection: independent cleanup, retained ownership, safe retries.

Run: python tests/test_cleanup_exception_safety.py
Uses temporary directories and one loopback-only relay; no Chrome or upstream.
"""
import os
import shutil
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from engine import MuseEngine, MuseGenerationError
from proxy import HTTPProxyRelay


class Resource:
    def __init__(self, engine, failures=0):
        self.engine = engine
        self.failures = failures
        self.attempts = 0

    def close(self):
        assert_tracking_unlocked(self.engine)
        # Resource I/O must not borrow the context retry serialization lock.
        assert not self.engine._cleanup_lock.locked()
        self.attempts += 1
        if self.attempts <= self.failures:
            raise OSError("fixture cleanup failure")


class Browser:
    def __init__(self, engine, failures=0):
        self.engine = engine
        self.failures = failures
        self.disposed = []
        self.created = []

    def send(self, method, params=None, timeout=None):
        assert_tracking_unlocked(self.engine)
        if method == "Target.getBrowserContexts":
            raise OSError("inventory unavailable")
        if method == "Target.disposeBrowserContext":
            self.disposed.append(params["browserContextId"])
            if len(self.disposed) <= self.failures:
                raise OSError("context cleanup failure")
            return {"result": {}}
        if method == "Target.createBrowserContext":
            self.created.append(params)
            return {"result": {"browserContextId": "ctx-new"}}
        if method == "Target.createTarget":
            return {"result": {"targetId": "target-new"}}
        raise AssertionError(method)

    def close(self):
        assert_tracking_unlocked(self.engine)


def assert_tracking_unlocked(engine):
    assert engine._contexts_lock.acquire(blocking=False), "tracking lock held over I/O"
    engine._contexts_lock.release()


class CleanupSafetyTests(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.TemporaryDirectory(prefix="muse-cleanup-")
        self.addCleanup(self.home.cleanup)
        self.cfg = SimpleNamespace(profile_dir=self.home.name,
                                   download_dir=os.path.join(self.home.name, "downloads"),
                                   cdp_port=1)
        self.engine = MuseEngine(self.cfg)
        self.engine.browser = Browser(self.engine)

    def attach(self, page=None, relay=None, context="ctx", download=True):
        engine = self.engine
        engine.page = page
        engine.current_context_id = context
        engine.current_acc_id = "account"
        if context:
            engine._active_contexts.add(context)
        local = engine._ensure_local()
        local.proxy_config = {"server": "fixture"}
        local.proxy_relay = relay
        if relay is not None:
            engine._proxy_relays.add(relay)
        return engine._new_download_dir() if download else None

    def assert_detached(self):
        engine = self.engine
        self.assertIsNone(engine.page)
        self.assertIsNone(engine.current_context_id)
        self.assertIsNone(engine.current_acc_id)
        local = engine._ensure_local()
        self.assertIsNone(local.proxy_relay)
        self.assertIsNone(local.proxy_config)
        self.assertIsNone(local.download_dir)

    def test_all_failures_are_independent_and_next_close_retries(self):
        engine = self.engine
        engine.browser.failures = 1
        page, relay = Resource(engine, 1), Resource(engine, 1)
        directory = self.attach(page, relay)
        real_rmtree = shutil.rmtree
        attempts = []

        def remove(path):
            assert_tracking_unlocked(engine)
            self.assertFalse(engine._cleanup_lock.locked())
            self.assertEqual(path, directory)
            self.assertEqual(os.path.dirname(os.path.realpath(path)),
                             os.path.realpath(self.cfg.download_dir))
            attempts.append(path)
            if len(attempts) == 1:
                raise PermissionError("directory temporarily busy")
            return real_rmtree(path)

        with patch("engine.shutil.rmtree", side_effect=remove):
            engine.close_current_page()
            self.assert_detached()
            self.assertEqual((page.attempts, relay.attempts, len(attempts)), (1, 1, 1))
            self.assertFalse(engine._active_contexts)
            self.assertEqual(engine._pending_disposals, {"ctx"})
            self.assertIn(relay, engine._proxy_relays)
            self.assertIn(directory, engine._download_dirs)
            self.assertIn(directory, engine._download_roots)
            self.assertTrue(os.path.isdir(directory))
            self.assertEqual(len(engine._pending_resource_cleanup), 3)
            engine.close_current_page()  # No current context required to retry.

        self.assertEqual((page.attempts, relay.attempts, len(attempts)), (2, 2, 2))
        self.assertFalse(os.path.exists(directory))
        self.assertFalse(engine._pending_disposals)
        self.assertFalse(engine._pending_resource_cleanup)
        self.assertFalse(engine._resource_cleanup_inflight)
        self.assertFalse(engine._proxy_relays)
        self.assertFalse(engine._download_dirs)
        self.assertFalse(engine._download_roots)

    def test_failed_old_resources_do_not_contaminate_next_account(self):
        engine = self.engine
        old_page, old_relay = Resource(engine, 100), Resource(engine, 100)
        old_dir = self.attach(old_page, old_relay)
        real_rmtree = shutil.rmtree

        def remove(path):
            assert_tracking_unlocked(engine)
            if path == old_dir:
                raise OSError("old directory busy")
            real_rmtree(path)

        with patch("engine.shutil.rmtree", side_effect=remove):
            engine._close_page()
            page, relay = Resource(engine), Resource(engine)
            new_dir = self.attach(page, relay, context="ctx-next")
            engine._close_page()
        self.assert_detached()
        self.assertEqual((page.attempts, relay.attempts), (1, 1))
        self.assertEqual(engine._proxy_relays, {old_relay})
        self.assertEqual(engine._download_dirs, {old_dir})
        self.assertFalse(os.path.exists(new_dir))
        self.assertFalse(engine._active_contexts)
        self.assertEqual(engine.browser.disposed, ["ctx", "ctx-next"])

    def test_context_retry_exception_cannot_skip_other_resources(self):
        page, relay = Resource(self.engine), Resource(self.engine)
        directory = self.attach(page, relay)
        with patch.object(self.engine, "_retry_context_disposals", side_effect=OSError):
            self.engine._close_page()
        self.assert_detached()
        self.assertEqual((page.attempts, relay.attempts), (1, 1))
        self.assertFalse(os.path.exists(directory))
        self.assertEqual(self.engine._pending_disposals, {"ctx"})

    def test_retries_are_bounded_fair_and_skip_active_relays(self):
        engine = self.engine
        retired = [Resource(engine, 100) for _ in range(12)]
        active = Resource(engine)
        engine._proxy_relays.update(retired + [active])
        engine._retry_resource_cleanup([("relay", relay) for relay in retired])
        for _ in range(3):
            before = sum(relay.attempts for relay in retired)
            engine._retry_resource_cleanup()
            self.assertEqual(sum(relay.attempts for relay in retired) - before, 4)
        self.assertEqual([relay.attempts for relay in retired], [2] * 12)
        self.assertEqual(active.attempts, 0)
        self.assertEqual(len(engine._pending_resource_cleanup), 12)

    def test_resource_backlog_blocks_new_context_without_forgetting(self):
        engine = self.engine
        retired = [Resource(engine, 100) for _ in range(64)]
        engine._proxy_relays.update(retired)
        engine._retry_resource_cleanup([("relay", relay) for relay in retired])
        with self.assertRaises(MuseGenerationError):
            engine._open_page()
        self.assertFalse(engine.browser.created)
        self.assertEqual(len(engine._pending_resource_cleanup), 64)
        self.assertEqual(sum(relay.attempts for relay in retired), 68)

    def test_inflight_resource_is_not_double_closed_and_does_not_block_other_workers(self):
        engine = self.engine
        entered, release = threading.Event(), threading.Event()
        calls = []
        failures = []

        class BlockingRelay:
            def close(self):
                assert_tracking_unlocked(engine)
                calls.append(1)
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("test release timed out")

        relay = BlockingRelay()
        engine._proxy_relays.add(relay)

        def retire():
            try:
                engine._ensure_local().proxy_relay = relay
                engine._close_page()
            except BaseException as exc:
                failures.append(exc)

        worker = threading.Thread(target=retire)
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            self.assertIn(relay, engine._pending_resource_cleanup.values())
            engine._retry_resource_cleanup()
            page = Resource(engine)
            engine.page = page
            engine._close_page()
            self.assertEqual(page.attempts, 1)
            self.assertEqual(len(calls), 1)
        finally:
            release.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertFalse(failures)
        self.assertFalse(engine._resource_cleanup_inflight)
        self.assertFalse(engine._pending_resource_cleanup)

    def test_directory_cleanup_refuses_foreign_root_and_symlink(self):
        engine = self.engine
        outside = os.path.join(self.home.name, "outside")
        os.mkdir(outside)
        marker = Path(outside) / "keep.txt"
        marker.write_text("unrelated", encoding="utf-8")
        owned = engine._new_download_dir()
        link = os.path.join(self.cfg.download_dir, "context-link")
        os.symlink(outside, link)
        # Simulate corrupt/injected tracking: provenance + resolved path required.
        engine._download_dirs.update((outside, self.cfg.download_dir, link))
        engine._download_roots[link] = os.path.realpath(self.cfg.download_dir)
        targets = (outside, self.cfg.download_dir, link)
        with patch("engine.shutil.rmtree") as remove:
            engine._retry_resource_cleanup([("download", target) for target in targets])
            remove.assert_not_called()
        self.assertEqual(marker.read_text(encoding="utf-8"), "unrelated")
        self.assertTrue(os.path.isdir(owned))
        self.assertTrue(set(targets) <= engine._download_dirs)

    def test_missing_child_does_not_mean_root_directory_is_gone(self):
        engine = self.engine
        directory = self.attach(download=True)
        with patch("engine.shutil.rmtree", side_effect=FileNotFoundError("missing child")):
            engine._close_page()
        self.assertIn(directory, engine._download_dirs)
        self.assertTrue(os.path.isdir(directory))
        # Remove only this verified temporary generated directory; next cleanup
        # must recognize confirmed absence as successful, not leak its tracking.
        self.assertEqual(os.path.dirname(os.path.realpath(directory)),
                         os.path.realpath(self.cfg.download_dir))
        shutil.rmtree(directory)
        engine._close_page()
        self.assertNotIn(directory, engine._download_dirs)

    def test_stop_keeps_failed_resources_and_cleans_others(self):
        engine = self.engine
        bad, good = Resource(engine, 100), Resource(engine)
        engine._proxy_relays.update((bad, good))
        directory = engine._new_download_dir()
        engine._ensure_local().download_dir = None  # Another retired worker's dir.
        with patch("engine.shutil.rmtree", side_effect=OSError("busy")):
            engine.stop()
        self.assertEqual(good.attempts, 1)
        self.assertEqual(engine._proxy_relays, {bad})
        self.assertEqual(engine._download_dirs, {directory})
        bad.failures = 0
        engine.stop()
        self.assertFalse(engine._pending_resource_cleanup)
        self.assertFalse(engine._proxy_relays)
        self.assertFalse(engine._download_dirs)
        self.assertFalse(os.path.exists(directory))

    def test_partial_open_page_close_failure_remains_owned(self):
        engine = self.engine
        page = Resource(engine, 1)
        page.send = lambda *args, **kwargs: (_ for _ in ()).throw(OSError("init failed"))
        with patch("engine.CDP", return_value=page):
            with self.assertRaises(MuseGenerationError):
                engine._open_page()
        self.assertIsNone(engine.page)
        self.assertIn(page, engine._pending_resource_cleanup.values())
        self.assertFalse(engine._active_contexts)
        engine._close_page()
        self.assertEqual(page.attempts, 2)
        self.assertFalse(engine._pending_resource_cleanup)

    def test_real_relay_shutdown_failure_then_engine_retry_releases_listener(self):
        engine = self.engine
        relay = HTTPProxyRelay({"server": "http://127.0.0.1:1"})
        engine._proxy_relays.add(relay)
        engine._ensure_local().proxy_relay = relay
        original_shutdown = relay.server.shutdown
        calls = []

        def shutdown():
            assert_tracking_unlocked(engine)
            calls.append(1)
            if len(calls) == 1:
                raise OSError("one-time relay shutdown failure")
            original_shutdown()

        try:
            with patch.object(relay.server, "shutdown", side_effect=shutdown):
                engine._close_page()
                self.assert_detached()
                self.assertIn(relay, engine._proxy_relays)
                self.assertIn(relay, engine._pending_resource_cleanup.values())
                engine._close_page()
            self.assertEqual(len(calls), 2)
            self.assertFalse(relay.thread.is_alive())
            self.assertEqual(relay.server.socket.fileno(), -1)
            self.assertFalse(engine._proxy_relays)
            self.assertFalse(engine._pending_resource_cleanup)
        finally:
            original_shutdown()
            relay.server.server_close()
            relay.thread.join(5)


if __name__ == "__main__":
    unittest.main()
