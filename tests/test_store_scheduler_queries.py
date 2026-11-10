"""Offline scheduler-query regressions; no app imports or production configuration."""
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import store as store_module
from store import Store


class NonOverlappingLock:
    """Fail immediately if a query nests the account and task locks."""

    def __init__(self, held, name):
        self.held = held
        self.name = name
        self.lock = threading.Lock()

    def __enter__(self):
        if self.held:
            raise AssertionError(f"nested {self.name} lock while holding {self.held}")
        self.lock.acquire()
        self.held.add(self.name)
        return self

    def __exit__(self, *_):
        self.held.remove(self.name)
        self.lock.release()


class StoreSchedulerQueryTests(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.TemporaryDirectory(prefix="muse-store-query-test-")
        cfg = SimpleNamespace(
            accounts_file=str(Path(self.home.name) / "accounts.json"),
            task_ttl_seconds=60, task_max_entries=32,
            account_flush_interval=0.1)
        self.store = Store(cfg)
        self.clock = patch.object(store_module.time, "time", return_value=1000)
        self.clock.start()

    def tearDown(self):
        self.store.close()
        self.clock.stop()
        self.home.cleanup()

    def task(self, tid, account=None, **fields):
        task = {"id": tid, "kind": "video", "status": "completed",
                "created_at": 990, "updated_at": 1000, "account": account,
                "prompt": "fixture", "result": None}
        task.update(fields)
        with self.store._tasks_lock:
            self.store._tasks[tid] = task
        return task

    def test_empty_store(self):
        self.assertIs(self.store.has_enabled_accounts_with_cookies(), False)
        self.assertIsNone(self.store.latest_completed_account_id())

    def test_availability_matches_enabled_and_cookie_truthiness(self):
        fixtures = [
            ({"id": "default-enabled", "cookies": {"other": "cookie"}}, True),
            ({"id": "enabled", "enabled": True, "cookies": {"hatch_sess": ""}}, True),
            ({"id": "unhealthy", "ok": False, "cookies": {"hatch_sess": "value"}}, True),
            ({"id": "disabled", "enabled": False, "cookies": {"hatch_sess": "value"}}, False),
            ({"id": "disabled-null", "enabled": None, "cookies": {"hatch_sess": "value"}}, False),
            ({"id": "empty", "cookies": {}}, False),
            ({"id": "null", "cookies": None}, False),
            ({"id": "missing"}, False),
        ]
        for account, expected in fixtures:
            with self.subTest(account=account["id"]):
                self.store.accounts = [account]
                self.assertIs(self.store.has_enabled_accounts_with_cookies(), expected)
        self.store.accounts = [fixtures[3][0], fixtures[5][0], fixtures[0][0]]
        self.assertIs(self.store.has_enabled_accounts_with_cookies(), True)

    def test_latest_uses_creation_time_not_update_time_or_insertion_order(self):
        self.store.accounts = [{"id": "older"}, {"id": "newer"}]
        self.task("newer", "newer", created_at=995, updated_at=995)
        self.task("older", "older", created_at=980, updated_at=1000)
        self.assertEqual(self.store.latest_completed_account_id(), "newer")

    def test_equal_creation_times_keep_original_insertion_order(self):
        self.store.accounts = [{"id": "first"}, {"id": "second"}]
        self.task("first", "first")
        self.task("second", "second")
        self.assertEqual(self.store.latest_completed_account_id(), "first")

    def test_filters_kind_completion_and_missing_account(self):
        self.store.accounts = [{"id": "video"}, {"id": "image"}, {"id": "ignored"}]
        self.task("video", "video", created_at=980)
        self.task("image", "image", kind="image", created_at=990)
        for status in ("processing", "queued", "failed", "cancelled", "succeeded", "success", "done"):
            self.task(status, "ignored", status=status, created_at=999)
        self.task("missing-account", "deleted", created_at=999)
        self.task("null-account", None, created_at=999)
        self.task("empty-account", "", created_at=999)
        self.assertEqual(self.store.latest_completed_account_id(), "video")
        self.assertEqual(self.store.latest_completed_account_id("image"), "image")
        self.assertIsNone(self.store.latest_completed_account_id("audio"))

    def test_preference_requires_enabled_and_ok_not_false_but_not_cookies(self):
        self.store.accounts = [
            {"id": "healthy", "enabled": True, "ok": True},
            {"id": "default", "cookies": {}},
            {"id": "unknown", "ok": None},
            {"id": "zero", "ok": 0},
            {"id": "failed", "ok": False},
            {"id": "disabled", "enabled": False},
            {"id": "null-enabled", "enabled": None},
        ]
        for index, account in enumerate(self.store.accounts):
            self.task(account["id"], account["id"], created_at=980 + index)
        for expected in ("zero", "unknown", "default", "healthy"):
            with self.subTest(expected=expected):
                self.assertEqual(self.store.latest_completed_account_id(), expected)
                self.store.delete_task(expected)
        self.assertIsNone(self.store.latest_completed_account_id())

    def test_query_prunes_expired_terminal_tasks_but_retains_active_tasks(self):
        self.store.accounts = [{"id": "expired"}, {"id": "live"}]
        self.task("expired", "expired", created_at=999, updated_at=940)
        self.task("live", "live", created_at=980, updated_at=941)
        self.task("failed", "expired", status="failed", created_at=900, updated_at=900)
        self.task("active", "expired", status="processing", created_at=900, updated_at=900)
        self.assertEqual(self.store.latest_completed_account_id(), "live")
        self.assertEqual(set(self.store._tasks), {"live", "active"})
        with patch.object(store_module.time, "time", return_value=1001):
            self.assertIsNone(self.store.latest_completed_account_id())
        self.assertEqual(set(self.store._tasks), {"active"})

    def test_pruning_uses_created_at_when_updated_at_is_absent(self):
        self.store.accounts = [{"id": "expired"}, {"id": "live"}]
        expired = self.task("expired", "expired", created_at=940)
        live = self.task("live", "live", created_at=941)
        del expired["updated_at"]
        del live["updated_at"]
        self.assertEqual(self.store.latest_completed_account_id(), "live")
        self.assertEqual(set(self.store._tasks), {"live"})

    def test_query_preserves_capacity_pruning_order(self):
        self.store._task_max = 2
        self.store.accounts = [{"id": "evicted"}, {"id": "retained"}]
        self.task("evicted", "evicted", created_at=999, updated_at=990)
        self.task("retained", "retained", created_at=980, updated_at=991)
        self.task("active", status="processing", created_at=900, updated_at=900)
        self.assertEqual(self.store.latest_completed_account_id(), "retained")
        self.assertEqual(set(self.store._tasks), {"retained", "active"})

    def test_queries_do_not_copy_payloads_or_nest_locks(self):
        self.store.accounts = [{"id": "candidate", "cookies": {"hatch_sess": "value"}}]
        self.task("completed", "candidate", result={"large": ["payload"] * 10000})
        held = set()
        with patch.object(self.store, "_accounts_lock", NonOverlappingLock(held, "accounts")), \
             patch.object(self.store, "_tasks_lock", NonOverlappingLock(held, "tasks")), \
             patch.object(store_module.copy, "deepcopy", side_effect=AssertionError("payload copied")), \
             patch.object(self.store, "get_account", side_effect=AssertionError("account copied")):
            self.assertIs(self.store.has_enabled_accounts_with_cookies(), True)
            self.assertEqual(self.store.latest_completed_account_id(), "candidate")
        self.assertEqual(held, set())

    def test_query_observes_current_account_state_without_mutating_it(self):
        self.store.accounts = [{"id": "older", "use_count": 3}, {"id": "newer", "use_count": 5}]
        self.task("older", "older", created_at=980)
        self.task("newer", "newer", created_at=990)
        self.assertEqual(self.store.latest_completed_account_id(), "newer")
        with self.store._accounts_lock:
            self.store._accounts[1]["enabled"] = False
        self.assertEqual(self.store.latest_completed_account_id(), "older")
        with self.store._accounts_lock:
            self.store._accounts[0]["ok"] = False
        self.assertIsNone(self.store.latest_completed_account_id())
        self.assertEqual([a["use_count"] for a in self.store.accounts], [3, 5])
        self.assertEqual(self.store._revision, 0)
        self.assertIsNone(self.store._writer)


if __name__ == "__main__":
    unittest.main(verbosity=2)
