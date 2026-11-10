"""Regression checks for maintenance activity and keepalive cadence."""
import asyncio
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("MUSE2API_KEY", "test-only")
sys.path.insert(0, str(ROOT))

from config import Config
from store import Store
import app


def test_maintenance_selection_does_not_count_as_use():
    with tempfile.TemporaryDirectory() as home:
        cfg = Config(base_dir=home)
        cfg.ensure_dirs()
        store = Store(cfg)
        acc = store.add_account({"hatch_sess": "fixture"}, "fixture")
        # Store snapshots are deep copies now; persist runtime stats explicitly.
        store.update_account_runtime(acc["id"], last_used=1234, use_count=7)
        selected = store.pick_account(record_use=False)
        assert selected["id"] == acc["id"]
        current = store.get_account(acc["id"])
        assert current["last_used"] == 1234
        assert current["use_count"] == 7
        store.pick_account()
        assert store.get_account(acc["id"])["use_count"] == 8


def test_keepalive_only_runs_when_due():
    now = 1_900_000_000
    original_store, original_cfg = app.store, app.CFG
    class FakeStore:
        def list_accounts(self):
            return [
                {"id": "fresh", "label": "fresh", "enabled": True,
                 "expires_at": now + 72 * 3600, "last_keepalive": now - 900, "ok": True},
                {"id": "due", "label": "due", "enabled": True,
                 "expires_at": now + 24 * 3600, "last_keepalive": now - 7 * 3600, "ok": True},
            ]
    app.store = FakeStore()
    app.CFG = SimpleNamespace(keepalive_disabled_accounts=False)
    called = []
    def fake_probe(aid, check_quota=False):
        called.append(aid)
        return {"id": aid, "ok": True}
    try:
        with patch.object(app, "_probe_account_sync", side_effect=fake_probe), \
             patch.object(app.asyncio, "sleep", return_value=None), \
             patch.object(app.time, "time", return_value=now):
            result = asyncio.run(app.run_keepalive_all())
        assert called == ["due"]
        assert result["refreshed_count"] == 1
        assert result["skipped_count"] == 1
    finally:
        app.store, app.CFG = original_store, original_cfg


if __name__ == "__main__":
    test_maintenance_selection_does_not_count_as_use()
    test_keepalive_only_runs_when_due()
    print("PASS: maintenance use tracking and keepalive cadence")
