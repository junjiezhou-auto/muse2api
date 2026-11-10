"""账号配置可靠落盘；生成任务只保留有界内存状态。"""
from __future__ import annotations

import copy
import json
import logging
import os
import tempfile
import threading
import time
import uuid

from proxy import public_proxy

log = logging.getLogger("muse2api.store")
ESSENTIAL_COOKIES = ("hatch_sess",)
VML_TTL = 2 * 86400


class TaskCapacityError(RuntimeError):
    """All retained task slots are active; reject admission instead of losing state."""


def _is_pos(v) -> bool:
    try:
        return int(v) > 0
    except (TypeError, ValueError):
        return False


def min_expiry(cookies_exp: dict | None) -> int | None:
    if not cookies_exp:
        return None
    vals = []
    for name in ESSENTIAL_COOKIES:
        value = cookies_exp.get(name)
        if _is_pos(value):
            vals.append(int(value))
    if not vals:
        vals = [int(value) for value in cookies_exp.values() if _is_pos(value)]
    return min(vals) if vals else None


def account_expiry(cookies_exp: dict | None, base_ts: int | None = None) -> int | None:
    """Prefer actual hatch_vml expiry; using an account never extends its TTL."""
    ce = cookies_exp or {}
    if _is_pos(ce.get("hatch_vml")):
        return int(ce["hatch_vml"])
    return int(base_ts) + VML_TTL if base_ts else min_expiry(ce)


def _read(path: str, default):
    if not os.path.isfile(path):
        return default
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return default


def _write(path: str, obj):
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class Store:
    _TERMINAL = frozenset(("completed", "failed", "cancelled", "canceled", "succeeded", "success", "done"))
    _CREDENTIAL_COOKIES = frozenset(("hatch_sess", "hatch_vml", "hatch_native_auth_device"))

    def __init__(self, cfg):
        self.cfg = cfg
        self._accounts_lock = threading.RLock()
        self._tasks_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._accounts = _read(cfg.accounts_file, [])
        for account in self._accounts:
            expiry = (account.get("cookies_exp") or {}).get("hatch_vml")
            if _is_pos(expiry):
                account["expires_at"] = int(expiry)
        # Deliberately leave any previous tasks.json untouched and unloaded.
        self._tasks: dict[str, dict] = {}
        self._task_ttl = max(1.0, float(getattr(cfg, "task_ttl_seconds", 3600)))
        self._task_max = max(1, int(getattr(cfg, "task_max_entries", 2048)))
        self._flush_interval = max(0.05, float(getattr(cfg, "account_flush_interval", 1)))
        self._revision = 0
        self._written_revision = 0
        self._closed = False
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._writer: threading.Thread | None = None

    @property
    def accounts(self) -> list[dict]:
        with self._accounts_lock:
            return copy.deepcopy(self._accounts)

    @accounts.setter
    def accounts(self, accounts):
        # Compatibility for callers replacing fixtures; normal mutations use methods.
        with self._accounts_lock:
            self._accounts = copy.deepcopy(accounts)

    def has_enabled_accounts_with_cookies(self) -> bool:
        """Check scheduler availability without copying account credentials or metadata."""
        with self._accounts_lock:
            return any(account.get("enabled", True) and account.get("cookies")
                       for account in self._accounts)

    @property
    def tasks(self) -> dict[str, dict]:
        with self._tasks_lock:
            self._prune_tasks_locked()
            return copy.deepcopy(self._tasks)

    def _changed_locked(self):
        if self._closed:
            raise RuntimeError("Account store is closed")
        self._revision += 1
        if self._writer is None:
            self._writer = threading.Thread(target=self._writer_loop,
                                            name="account-store-writer", daemon=True)
            self._writer.start()
        self._wake.set()

    def _writer_loop(self):
        while not self._stop.is_set():
            if not self._wake.wait(0.5):
                continue
            self._wake.clear()
            if self._stop.wait(self._flush_interval):
                break
            try:
                self.flush()
            except Exception:
                log.exception("Account persistence failed; retaining dirty revision for retry")
                self._wake.set()

    def flush(self, timeout: float = 10.0):
        """Persist a consistent account snapshot; failures are visible to the caller."""
        if not self._write_lock.acquire(timeout=max(0.0, timeout)):
            raise TimeoutError("Timed out waiting for account persistence")
        try:
            with self._accounts_lock:
                revision = self._revision
                if revision <= self._written_revision:
                    return
                snapshot = copy.deepcopy(self._accounts)
            # Every writer owns this lock before taking its snapshot, so an older
            # background snapshot cannot overwrite a completed management write.
            _write(self.cfg.accounts_file, snapshot)
            with self._accounts_lock:
                self._written_revision = revision
                if self._revision > revision:
                    self._wake.set()
        finally:
            self._write_lock.release()

    def close(self, timeout: float = 10.0):
        with self._accounts_lock:
            self._closed = True
        self._stop.set()
        self._wake.set()
        self.flush(timeout)
        if self._writer is not None and self._writer is not threading.current_thread():
            self._writer.join(timeout=max(0.0, timeout))
            if self._writer.is_alive():
                raise TimeoutError("Account persistence worker did not stop")

    def add_account(self, cookies: dict, label: str = "", cookies_exp: dict | None = None) -> dict:
        with self._accounts_lock:
            if self._closed:
                raise RuntimeError("Account store is closed")
            aid = uuid.uuid4().hex[:12]
            expiry = {k: int(v) for k, v in (cookies_exp or {}).items() if _is_pos(v)}
            now = int(time.time())
            account = {"id": aid, "label": label or aid, "cookies": copy.deepcopy(cookies),
                       "cookies_exp": expiry, "expires_at": account_expiry(expiry, now),
                       "expiry_anchor": None, "enabled": True, "created_at": now,
                       "last_used": None, "last_keepalive": None, "use_count": 0,
                       "ok": None, "note": "", "synced_at": None}
            self._accounts.append(account)
            self._changed_locked()
            result = copy.deepcopy(account)
        self.flush()
        return result

    @staticmethod
    def public_account(account: dict) -> dict:
        item = copy.deepcopy({k: v for k, v in account.items() if k not in ("cookies", "proxy")})
        item["proxy"] = public_proxy(account.get("proxy"))
        item["cookie_count"] = len(account.get("cookies", {}))
        return item

    def list_accounts(self) -> list[dict]:
        result = []
        for account in self.accounts:
            item = self.public_account(account)
            expiry = account.get("cookies_exp") or {}
            if _is_pos(expiry.get("hatch_vml")):
                item["expires_at"] = int(expiry["hatch_vml"])
            item["essential_ok"] = all(account.get("cookies", {}).get(n) for n in ESSENTIAL_COOKIES)
            item["expires_estimated"] = not _is_pos(expiry.get("hatch_vml"))
            result.append(item)
        return result

    def delete_account(self, aid: str) -> bool:
        with self._accounts_lock:
            if self._closed:
                raise RuntimeError("Account store is closed")
            original = len(self._accounts)
            self._accounts = [a for a in self._accounts if a["id"] != aid]
            changed = len(self._accounts) != original
            if changed:
                self._changed_locked()
        if changed:
            self.flush()
        return changed

    def pick_account(self, preferred_id: str | None = None, rotate: bool = True,
                     force_rotate: bool = False, exclude_id: str | None = None,
                     exclude_ids: set[str] | list[str] | None = None,
                     record_use: bool = True) -> dict | None:
        with self._accounts_lock:
            if self._closed:
                raise RuntimeError("Account store is closed")
            live = [a for a in self._accounts if a.get("enabled", True) and a.get("cookies")]
            if not live:
                return None
            blocked = set(exclude_ids or [])
            if exclude_id:
                blocked.add(exclude_id)
            if blocked:
                available = [a for a in live if a["id"] not in blocked]
                if available:
                    live = available
                elif exclude_ids is not None:
                    return None
                elif len(live) > 1:
                    live = [a for a in live if a["id"] != exclude_id] or live
            chosen = None
            if preferred_id and not force_rotate:
                for account in live:
                    if account["id"] == preferred_id and account.get("ok") is not False:
                        count = getattr(self, "_sticky_count", 0) if getattr(self, "_sticky_id", None) == preferred_id else 0
                        if not rotate or count < 15:
                            chosen = account
                            self._sticky_count = count + 1
                            break
            if chosen is None:
                candidates = [a for a in live if a.get("ok") is not False] or live
                chosen = min(candidates, key=lambda a: (a.get("last_used") or 0.0, a.get("use_count") or 0))
                self._sticky_count = 1
            self._sticky_id = chosen["id"]
            if record_use:
                chosen["last_used"] = time.time()
                chosen["use_count"] = chosen.get("use_count", 0) + 1
                self._changed_locked()
            return copy.deepcopy(chosen)

    def touch_keepalive(self, aid: str, ok: bool | None = True, note: str = ""):
        now = int(time.time())
        changes = {"checked_at": now}
        account = self.get_account(aid)
        if not account:
            return
        if ok is not None:
            changes["ok"] = ok
        if ok is True:
            changes.update(last_keepalive=now, expires_at=account_expiry(
                account.get("cookies_exp"), account.get("expiry_anchor") or account.get("created_at")))
        if note:
            changes["note"] = note[:300]
        self.update_account_runtime(aid, **changes)

    def mark(self, aid: str, ok: bool, note: str = ""):
        self.update_account_runtime(aid, ok=ok, note=note[:300], checked_at=int(time.time()))

    def get_account(self, aid: str) -> dict | None:
        with self._accounts_lock:
            return next((copy.deepcopy(a) for a in self._accounts if a["id"] == aid), None)

    def _update_account(self, aid: str, changes: dict, durable: bool) -> dict | None:
        with self._accounts_lock:
            if self._closed:
                raise RuntimeError("Account store is closed")
            account = next((a for a in self._accounts if a["id"] == aid), None)
            if account is None:
                return None
            previous = copy.deepcopy(account)
            for key, value in changes.items():
                if value is not None or key == "ok":
                    account[key] = copy.deepcopy(value)
            if changes.get("cookies_exp") is not None:
                if _is_pos(account["cookies_exp"].get("hatch_vml")):
                    account["expiry_anchor"] = None
                elif not account.get("expiry_anchor"):
                    account["expiry_anchor"] = int(time.time())
                account["expires_at"] = account_expiry(account["cookies_exp"],
                    account.get("expiry_anchor") or account.get("created_at"))
            # A newly issued authentication cookie must be durable before success
            # is reported, even when the caller requests runtime coalescing.
            old_cookies = previous.get("cookies") or {}
            new_cookies = account.get("cookies") or {}
            credential_changed = any(old_cookies.get(k) != new_cookies.get(k)
                                     for k in self._CREDENTIAL_COOKIES)
            if account != previous:
                self._changed_locked()
            result = copy.deepcopy(account)
        if durable or credential_changed:
            self.flush()
        return result

    def update_account(self, aid: str, **kw) -> dict | None:
        """Management changes are acknowledged only after reliable persistence."""
        return self._update_account(aid, kw, durable=True)

    def update_account_runtime(self, aid: str, **kw) -> dict | None:
        """Coalesce runtime metadata; changed authentication cookies flush immediately."""
        return self._update_account(aid, kw, durable=False)

    def stats(self) -> dict:
        accounts = self.accounts
        now = int(time.time())
        enabled = [a for a in accounts if a.get("enabled", True)]
        return {"total": len(accounts), "enabled": len(enabled),
                "disabled": len(accounts) - len(enabled),
                "healthy": sum(a.get("ok") is True for a in accounts),
                "error": sum(a.get("ok") is False for a in accounts),
                "expiring": sum(bool(a.get("expires_at")) and a["expires_at"] - now < 3 * 86400 for a in enabled),
                "expired": sum(bool(a.get("expires_at")) and a["expires_at"] <= now for a in enabled)}

    def _prune_tasks_locked(self, reserve: int = 0):
        now = time.time()
        terminal = [t for t in self._tasks.values() if t.get("status") in self._TERMINAL]
        for task in terminal:
            if now - task.get("updated_at", task.get("created_at", now)) >= self._task_ttl:
                self._tasks.pop(task["id"], None)
        excess = len(self._tasks) + reserve - self._task_max
        if excess > 0:
            terminal = sorted((t for t in terminal if t["id"] in self._tasks),
                              key=lambda t: (t.get("updated_at", 0), t.get("created_at", 0)))
            for task in terminal[:excess]:
                self._tasks.pop(task["id"], None)

    def create_task(self, kind: str, prompt: str) -> dict:
        with self._tasks_lock:
            self._prune_tasks_locked(reserve=1)
            if len(self._tasks) >= self._task_max:
                raise TaskCapacityError("Task state capacity is full; retry later")
            tid = "task_" + uuid.uuid4().hex[:20]
            task = {"id": tid, "object": "task", "kind": kind, "prompt": prompt,
                    "status": "queued", "created_at": int(time.time()),
                    "updated_at": int(time.time()), "result": None, "error": None}
            self._tasks[tid] = task
            return copy.deepcopy(task)

    def update_task(self, tid: str, **kw):
        with self._tasks_lock:
            task = self._tasks.get(tid)
            if task is None:
                return None
            if kw and any(task.get(key) != value for key, value in kw.items()):
                task.update(copy.deepcopy(kw))
                task["updated_at"] = int(time.time())
            return copy.deepcopy(task)

    def get_task(self, tid: str) -> dict | None:
        with self._tasks_lock:
            self._prune_tasks_locked()
            return copy.deepcopy(self._tasks.get(tid))

    def list_tasks(self, limit: int = 50) -> list[dict]:
        with self._tasks_lock:
            self._prune_tasks_locked()
            items = sorted(self._tasks.values(), key=lambda t: t.get("created_at") or 0, reverse=True)
            return copy.deepcopy(items[:max(0, limit)])

    def latest_completed_account_id(self, kind: str = "video") -> str | None:
        """Return the newest completed task's enabled, non-failed account, if any."""
        with self._tasks_lock:
            self._prune_tasks_locked()
            # Retention bounds this list by task count, not by result/prompt size.
            candidates = [(task.get("created_at", 0), task["account"])
                          for task in self._tasks.values()
                          if task.get("kind") == kind and task.get("status") == "completed"
                          and task.get("account")]
        if not candidates:
            return None
        # Stable ordering matches the tasks snapshot's insertion order on ties.
        candidates.sort(key=lambda candidate: candidate[0], reverse=True)
        candidate_ids = {account_id for _, account_id in candidates}
        with self._accounts_lock:
            eligible = {account["id"] for account in self._accounts
                        if account["id"] in candidate_ids and account.get("enabled", True)
                        and account.get("ok") is not False}
        # No task/account payload escapes either lock, and the locks never overlap.
        return next((account_id for _, account_id in candidates if account_id in eligible), None)

    def delete_task(self, tid: str) -> bool:
        with self._tasks_lock:
            if tid in self._tasks:
                del self._tasks[tid]
                return True
            return False

    def clear_tasks(self, keep: int = 0) -> int:
        with self._tasks_lock:
            items = sorted(self._tasks.values(), key=lambda t: t.get("created_at") or 0, reverse=True)
            keep_ids = {t["id"] for t in items[:max(0, keep)]}
            keep_ids.update(t["id"] for t in items if t.get("status") not in self._TERMINAL)
            removed = len(self._tasks) - len(keep_ids)
            self._tasks = {k: v for k, v in self._tasks.items() if k in keep_ids}
            return max(0, removed)
