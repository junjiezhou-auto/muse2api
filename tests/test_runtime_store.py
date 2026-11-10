"""Offline runtime-store regression tests; never load production configuration."""
import concurrent.futures
import copy
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import store as store_module
from store import Store, TaskCapacityError


class RuntimeStoreTests(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.TemporaryDirectory(prefix="muse-store-test-")
        self.cfg = SimpleNamespace(
            accounts_file=str(Path(self.home.name) / "accounts.json"),
            tasks_file=str(Path(self.home.name) / "tasks.json"),
            task_ttl_seconds=60, task_max_entries=3,
            account_flush_interval=0.1)
        self.stores = []

    def tearDown(self):
        for instance in self.stores:
            instance.close()
        self.home.cleanup()

    def make_store(self):
        instance = Store(self.cfg)
        self.stores.append(instance)
        return instance

    def test_task_history_is_neither_loaded_nor_written(self):
        old = b'{"old":{"id":"old","status":"processing"}}'
        Path(self.cfg.tasks_file).write_bytes(old)
        instance = self.make_store()
        self.assertEqual(instance.tasks, {})
        with patch.object(store_module, "_write", side_effect=AssertionError("task wrote disk")):
            task = instance.create_task("video", "fixture")
            instance.update_task(task["id"], progress=15, status="processing")
            instance.update_task(task["id"], status="completed", result={"url": "/fixture"})
            self.assertEqual(instance.get_task(task["id"])["status"], "completed")
            instance.clear_tasks()
        self.assertEqual(Path(self.cfg.tasks_file).read_bytes(), old)

    def test_ttl_and_capacity_never_evict_active_tasks(self):
        instance = self.make_store()
        with patch.object(store_module.time, "time", return_value=1000):
            active = instance.create_task("video", "active")
            terminal = instance.create_task("video", "terminal")
            instance.update_task(terminal["id"], status="completed")
        with patch.object(store_module.time, "time", return_value=1061):
            self.assertIsNone(instance.get_task(terminal["id"]))
            self.assertIsNotNone(instance.get_task(active["id"]))
            instance.create_task("video", "active-2")
            instance.create_task("video", "active-3")
            with self.assertRaises(TaskCapacityError):
                instance.create_task("video", "over-capacity")
            self.assertEqual(instance.clear_tasks(), 0)
            self.assertEqual(len(instance.tasks), 3)

    def test_capacity_reclaims_oldest_terminal_first(self):
        instance = self.make_store()
        active = instance.create_task("video", "active")
        done = instance.create_task("video", "done")
        instance.update_task(done["id"], status="failed")
        instance.create_task("video", "third")
        instance.create_task("video", "fourth")
        self.assertIsNone(instance.get_task(done["id"]))
        self.assertIsNotNone(instance.get_task(active["id"]))
        self.assertEqual(len(instance.tasks), 3)

    def test_all_readers_return_nested_snapshots(self):
        instance = self.make_store()
        account = instance.add_account({"hatch_sess": "fixture"}, "fixture")
        account["cookies"]["hatch_sess"] = "changed-return"
        snapshot = instance.accounts
        snapshot[0]["cookies"]["hatch_sess"] = "changed-list"
        read = instance.get_account(account["id"])
        read["cookies"].clear()
        self.assertEqual(instance.get_account(account["id"])["cookies"], {"hatch_sess": "fixture"})
        task = instance.create_task("image", "fixture")
        instance.update_task(task["id"], result={"nested": [1]})
        tasks = instance.tasks
        tasks[task["id"]]["result"]["nested"].append(2)
        instance.get_task(task["id"])["result"]["nested"].append(3)
        self.assertEqual(instance.get_task(task["id"])["result"], {"nested": [1]})

    def test_runtime_updates_coalesce_and_flush_before_close(self):
        instance = self.make_store()
        account = instance.add_account({"hatch_sess": "fixture"}, "fixture")
        actual_write = store_module._write
        writes = []
        def tracking(path, snapshot):
            writes.append(copy.deepcopy(snapshot))
            actual_write(path, snapshot)
        with patch.object(store_module, "_write", side_effect=tracking):
            for _ in range(30):
                instance.pick_account()
                instance.mark(account["id"], True)
            instance.close()
        self.assertLess(len(writes), 10)
        saved = json.loads(Path(self.cfg.accounts_file).read_text())
        self.assertEqual(saved[0]["use_count"], 30)
        self.assertTrue(saved[0]["ok"])

    def test_background_flush_is_bounded_without_shutdown(self):
        instance = self.make_store()
        account = instance.add_account({"hatch_sess": "fixture"}, "fixture")
        persisted = threading.Event()
        actual_write = store_module._write
        def observe(path, snapshot):
            actual_write(path, snapshot)
            if snapshot[0].get("note") == "background":
                persisted.set()
        with patch.object(store_module, "_write", side_effect=observe):
            instance.update_account_runtime(account["id"], note="background")
            self.assertTrue(persisted.wait(2), "runtime metadata was never flushed")

    def test_management_and_new_credentials_are_immediately_durable(self):
        instance = self.make_store()
        account = instance.add_account({"hatch_sess": "fixture"}, "fixture")
        instance.update_account(account["id"], enabled=False, label="saved")
        saved = json.loads(Path(self.cfg.accounts_file).read_text())[0]
        self.assertFalse(saved["enabled"])
        self.assertEqual(saved["label"], "saved")
        instance.update_account_runtime(account["id"], cookies={"hatch_sess": "fresh-fixture"})
        saved = json.loads(Path(self.cfg.accounts_file).read_text())[0]
        self.assertEqual(saved["cookies"]["hatch_sess"], "fresh-fixture")
        instance.delete_account(account["id"])
        self.assertEqual(json.loads(Path(self.cfg.accounts_file).read_text()), [])

    def test_slow_account_io_does_not_hold_read_or_task_locks(self):
        instance = self.make_store()
        account = instance.add_account({"hatch_sess": "fixture"}, "fixture")
        entered, release = threading.Event(), threading.Event()
        actual_write = store_module._write
        def slow_write(path, snapshot):
            entered.set()
            if not release.wait(3):
                raise AssertionError("test writer was not released")
            actual_write(path, snapshot)
        with patch.object(store_module, "_write", side_effect=slow_write):
            instance.mark(account["id"], True)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                flush_job = executor.submit(instance.flush)
                self.assertTrue(entered.wait(2))
                task_job = executor.submit(instance.create_task, "video", "not-blocked")
                read_job = executor.submit(instance.get_account, account["id"])
                try:
                    self.assertEqual(task_job.result(timeout=0.5)["prompt"], "not-blocked")
                    self.assertTrue(read_job.result(timeout=0.5)["ok"])
                finally:
                    release.set()
                flush_job.result(timeout=2)

    def test_older_background_snapshot_cannot_overwrite_management(self):
        instance = self.make_store()
        account = instance.add_account({"hatch_sess": "fixture"}, "fixture")
        entered, release = threading.Event(), threading.Event()
        actual_write = store_module._write
        first = True
        def slow_first_write(path, snapshot):
            nonlocal first
            if first:
                first = False
                entered.set()
                if not release.wait(3):
                    raise AssertionError("test writer was not released")
            actual_write(path, snapshot)
        with patch.object(store_module, "_write", side_effect=slow_first_write):
            instance.mark(account["id"], True)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                old_write = executor.submit(instance.flush)
                self.assertTrue(entered.wait(2))
                managed = executor.submit(instance.update_account, account["id"], label="new-management-value")
                try:
                    deadline = time.monotonic() + 1
                    while instance.get_account(account["id"])["label"] != "new-management-value":
                        if time.monotonic() >= deadline:
                            self.fail("management update never reached memory")
                        threading.Event().wait(0.005)
                finally:
                    release.set()
                old_write.result(timeout=2)
                managed.result(timeout=2)
        self.assertEqual(json.loads(Path(self.cfg.accounts_file).read_text())[0]["label"], "new-management-value")

    def test_persistence_error_is_not_acknowledged_as_success(self):
        instance = self.make_store()
        account = instance.add_account({"hatch_sess": "fixture"}, "fixture")
        with patch.object(store_module, "_write", side_effect=OSError("fixture-disk-full")):
            with self.assertRaises(OSError):
                instance.update_account(account["id"], label="pending")
        instance.flush()
        self.assertEqual(json.loads(Path(self.cfg.accounts_file).read_text())[0]["label"], "pending")

    def test_parallel_task_updates_and_snapshots(self):
        instance = self.make_store()
        task = instance.create_task("video", "fixture")
        def work(index):
            for iteration in range(20):
                instance.update_task(task["id"], **{"worker_" + str(index): iteration})
                list(instance.tasks.values())
                instance.list_tasks()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(work, range(8)))
        result = instance.get_task(task["id"])
        self.assertTrue(all(result["worker_" + str(index)] == 19 for index in range(8)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
