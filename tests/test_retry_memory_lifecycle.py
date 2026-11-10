"""Offline regressions: failures retire pages and traceback payloads before lease waits."""
import gc
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
import weakref
from contextlib import ExitStack
from unittest.mock import patch, PropertyMock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
HOME = tempfile.TemporaryDirectory(prefix='muse-retry-lifecycle-')
os.environ.update(MUSE2API_HOME=HOME.name, MUSE2API_PROFILE_ROOT=HOME.name,
                  MUSE2API_KEY='offline-fixture', MUSE2API_CONCURRENCY='2')
import app

class Payload:
    pass

class RetryTests(unittest.TestCase):
    def test_error_metadata_detached_and_typed(self):
        for cls in (app.MuseAuthError, app.MuseProxyError, app.MuseModelRefusalError,
                    app.RefImageSafetyError, app.MuseGenerationError, RuntimeError):
            try:
                raise cls('fixture')
            except Exception as original:
                detached = app._retry_error(original)
                self.assertIsNot(detached, original)
                self.assertIsNone(detached.__traceback__)
                self.assertIsNone(detached.__context__)
                self.assertIsNone(detached.__cause__)
                expected = cls if cls is not RuntimeError else app.MuseGenerationError
                self.assertIsInstance(detached, expected)

    def check_retry(self, chat=False):
        entered, resume = threading.Event(), threading.Event()
        refs, events, errors = [], [], []
        alive = [False]
        calls = [0]
        acquisitions = [0]
        def acquire(**kw):
            acquisitions[0] += 1
            if acquisitions[0] == 2:
                events.append('retry-acquire')
                self.assertFalse(alive[0], 'old page still live while waiting for new lease')
                entered.set()
                if not resume.wait(5):
                    raise AssertionError('test timeout waiting for resume')
            return {'id': str(acquisitions[0]), 'cookies': {}}
        def release(aid):
            self.assertFalse(alive[0], 'account released before old page closed')
            events.append('release-' + aid)
        def close():
            alive[0] = False
            events.append('close')
        def generate(*args, **kw):
            calls[0] += 1
            alive[0] = True
            if calls[0] == 1:
                payload = Payload()
                payload.data = bytearray(4 * 1024 * 1024)
                refs.append(weakref.ref(payload))
                raise app.MuseGenerationError('fixture input missing')
            return {'filename': 'fixture'}
        def chat_stream(*args, **kw):
            generate(*args, **kw)
            yield 'done'
        def run():
            try:
                if chat:
                    app._chat_stream_core('fixture', 10, None, lambda chunk: True, threading.Event())
                else:
                    app._run_generation('fixture', 'video', 10)
            except BaseException as exc:
                errors.append(exc)
        with ExitStack() as stack:
            for name, value in [('acquire', acquire), ('release', release)]:
                stack.enter_context(patch.object(app.ACCOUNT_POOL, name, side_effect=value))
            for name, value in [('generate', generate), ('chat_stream', chat_stream), ('close_current_page', close)]:
                stack.enter_context(patch.object(app.engine, name, side_effect=value))
            stack.enter_context(patch.object(app.engine, 'start'))
            stack.enter_context(patch.object(app.engine, 'reset_thread', side_effect=AssertionError('must close, not reset on retry')))
            stack.enter_context(patch.object(app, '_renew_and_persist', return_value=None))
            stack.enter_context(patch.object(app, '_sync_cookies'))
            stack.enter_context(patch.object(app, '_bind_account_proxy'))
            stack.enter_context(patch.object(app, '_clear_account_proxy'))
            stack.enter_context(patch.object(app.store, 'get_account', return_value={}))
            stack.enter_context(patch.object(app.store, 'mark'))
            stack.enter_context(patch.object(app.store, 'latest_completed_account_id', return_value=None))
            stack.enter_context(patch.object(type(app.store), 'tasks', new_callable=PropertyMock,
                                            side_effect=AssertionError('full task snapshot retained')))
            t = threading.Thread(target=run)
            t.start()
            try:
                self.assertTrue(entered.wait(3), str(errors))
                gc.collect()
                self.assertEqual(len(refs), 1)
                self.assertIsNone(refs[0](), 'failed-attempt traceback retained payload during retry wait')
                self.assertFalse(alive[0])
            finally:
                resume.set()
                t.join(5)
            self.assertFalse(t.is_alive())
        self.assertFalse(errors, str(errors))
        self.assertEqual(calls[0], 2)
        self.assertLess(events.index('close'), events.index('release-1'))
        self.assertFalse(app.GEN_LOCK.locked())

    def test_media_retry_releases_before_wait(self):
        self.check_retry(chat=False)

    def test_chat_retry_releases_before_wait(self):
        self.check_retry(chat=True)

    def test_chat_close_failure_does_not_leak_slot(self):
        stop = threading.Event()
        stop.set()
        with patch.object(app.engine, 'close_current_page', side_effect=OSError('fixture')), \
             patch.object(app, '_clear_account_proxy') as clear:
            app._chat_stream_core('fixture', 1, None, lambda _: True, stop)
            clear.assert_called()
        self.assertFalse(app.GEN_LOCK.locked())

    def test_warmup_close_failure_does_not_leak_lease(self):
        account = {'id': 'fixture', 'cookies': {}}
        with ExitStack() as stack:
            stack.enter_context(patch.object(app.engine, 'start'))
            stack.enter_context(patch.object(app.engine, 'ensure_page'))
            stack.enter_context(patch.object(app.engine, 'close_current_page', side_effect=OSError('fixture')))
            stack.enter_context(patch.object(app, '_renew_and_persist', return_value=None))
            stack.enter_context(patch.object(app, '_bind_account_proxy'))
            clear = stack.enter_context(patch.object(app, '_clear_account_proxy'))
            stack.enter_context(patch.object(app.ACCOUNT_POOL, 'acquire', return_value=account))
            release = stack.enter_context(patch.object(app.ACCOUNT_POOL, 'release'))
            app._warmup_browser_sync()
            clear.assert_called()
            release.assert_called_once_with('fixture')
        self.assertFalse(app.GEN_LOCK.locked())

    def test_account_wait_no_snapshot_and_cancellable(self):
        class CheapStore:
            calls = threading.Event()
            @property
            def accounts(self):
                raise AssertionError('whole account pool copied')
            def pick_account(self, **kw):
                self.calls.set()
                return None
            def has_enabled_accounts_with_cookies(self):
                return True
        store = CheapStore()
        pool = app.AccountPoolManager(store)
        stop = threading.Event()
        failures = []
        def wait():
            try:
                pool.acquire(timeout=30, stop_event=stop)
            except app.MuseGenerationError as exc:
                failures.append(str(exc))
        t = threading.Thread(target=wait)
        t.start()
        self.assertTrue(store.calls.wait(2))
        stop.set()
        t.join(2)
        self.assertFalse(t.is_alive(), 'cancelled request kept waiting for full timeout')
        self.assertEqual(len(failures), 1)
        self.assertIn('取消', failures[0])
        self.assertFalse(pool.busy_accounts)

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        app.GEN_POOL.shutdown()
        app.store.close()
        HOME.cleanup()
