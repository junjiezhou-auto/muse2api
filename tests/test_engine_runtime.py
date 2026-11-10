"""Offline regressions for engine context disposal tracking and poll JS.

Run: python tests/test_engine_runtime.py
No Chrome, no network, no account data.
"""
import importlib.util
import json
import os
import sys
import tempfile
import threading
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

home = tempfile.mkdtemp(prefix="muse2api-engine-runtime-")
os.environ.update(MUSE2API_HOME=home, MUSE2API_PROFILE_ROOT=home, MUSE2API_KEY="test-only")

spec = importlib.util.spec_from_file_location("engine_runtime_target", ROOT / "engine.py")
engine_mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = engine_mod
spec.loader.exec_module(engine_mod)

import config as config_mod


class FakeBrowser:
    def __init__(self, fail_ids=()):
        self.fail_ids = set(fail_ids)
        self.disposed = []
        self.lock = threading.Lock()

    def send(self, method, params=None, timeout=None):
        if method != "Target.disposeBrowserContext":
            raise AssertionError(method)
        ctx = params["browserContextId"]
        if ctx in self.fail_ids:
            raise engine_mod.cdp.CDPError("dispose failed")
        self.disposed.append(ctx)
        return {"result": {}}


def make_engine():
    cfg = config_mod.Config(base_dir=home)
    cfg.ensure_dirs()
    return engine_mod.MuseEngine(cfg)


def test_failed_dispose_stays_tracked_and_retries():
    eng = make_engine()
    eng.browser = FakeBrowser(fail_ids={"ctx-bad"})
    with eng._contexts_lock:
        eng._active_contexts.update({"ctx-ok", "ctx-bad"})
    eng.current_context_id = "ctx-bad"
    eng.page = None
    eng._close_page()
    assert "ctx-bad" in eng._pending_disposals, "failed dispose must stay tracked"
    assert "ctx-bad" not in eng._active_contexts, "close must release the active slot"
    # A later worker retried disposals; the good one clears, bad stays owned.
    eng.current_context_id = "ctx-ok"
    eng._close_page()
    assert "ctx-ok" not in eng._pending_disposals
    assert "ctx-bad" in eng._pending_disposals
    assert not eng._active_contexts & {"ctx-ok", "ctx-bad"}
    assert eng.browser.disposed == ["ctx-ok"]
    print("PASS: failed context disposal stays tracked and is retried")


def test_poll_js_shape_and_no_full_dom_scan():
    js = engine_mod.MuseEngine._POLL_CHAT_JS
    assert "querySelectorAll('*')" not in js, "full-DOM scan must be gone"
    assert "__m2aScroll" in js and "__m2aScope" in js, "container cache missing"
    assert "getComputedStyle" in js
    assert "cnt:cnt" in js and "txt:txt" in js and "stop:stop" in js
    assert "JSON.stringify" in js
    # The script must stay a self-evaluating function returning JSON.
    assert js.startswith("(function(){") and js.endswith("})()")
    print("PASS: chat poll JS is cached and keeps its response contract")


def test_open_page_retries_pending_disposals():
    eng = make_engine()
    eng.browser = FakeBrowser()
    with eng._contexts_lock:
        eng._pending_disposals.add("ctx-stale")
    try:
        eng._open_page()
    except Exception:
        pass  # page creation fails here; disposal retry must still happen
    assert "ctx-stale" not in eng._pending_disposals
    assert "ctx-stale" in eng.browser.disposed
    print("PASS: opening a page first retries pending context disposals")


if __name__ == "__main__":
    test_failed_dispose_stays_tracked_and_retries()
    test_poll_js_shape_and_no_full_dom_scan()
    test_open_page_retries_pending_disposals()
    print("ALL ENGINE RUNTIME TESTS PASSED")
