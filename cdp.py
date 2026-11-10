"""Deadline-bounded Chrome DevTools Protocol client."""
from __future__ import annotations
import json
import threading
import time
import requests
import websocket


class CDPError(RuntimeError):
    pass


class CDP:
    def __init__(self, ws_url: str, timeout: float = 90.0, max_size: int = 256 << 20):
        # Trusted local Chrome; retain JSON validation without Python UTF-8 scans.
        self.ws = websocket.create_connection(ws_url, timeout=timeout, max_size=max_size,
                                               skip_utf8_validation=True)
        self.timeout = timeout
        self._id = 0
        self._lock = threading.Lock()
        self._closed = False

    def _abort(self):
        self._closed = True
        try:
            self.ws.abort()
        except Exception:
            pass

    @staticmethod
    def _remaining(deadline, method):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"CDP {method} 超时")
        return remaining

    def send(self, method: str, params: dict | None = None, timeout: float | None = None):
        deadline = time.monotonic() + max(0, self.timeout if timeout is None else timeout)
        if not self._lock.acquire(timeout=max(0, deadline - time.monotonic())):
            raise TimeoutError(f"CDP {method} 等待连接锁超时")
        try:
            if self._closed:
                raise CDPError("CDP 连接已关闭")
            self.ws.settimeout(self._remaining(deadline, method))
            self._id += 1
            mid = self._id
            self.ws.send(json.dumps({"id": mid, "method": method, "params": params or {}}))
            while True:
                self.ws.settimeout(self._remaining(deadline, method))
                raw = self.ws.recv()
                if not raw:
                    raise ConnectionError("Chrome closed the CDP connection")
                msg = json.loads(raw)
                if not isinstance(msg, dict):
                    raise ValueError("Invalid CDP response")
                if msg.get("id") == mid:
                    if "error" in msg:
                        raise CDPError(f"{method}: {msg['error']}")
                    return msg
        except websocket.WebSocketTimeoutException as exc:
            self._abort()
            raise TimeoutError(f"CDP {method} 超时") from exc
        except TimeoutError:
            self._abort()
            raise
        except CDPError:
            raise
        except Exception as exc:
            self._abort()
            raise CDPError(f"CDP {method} 连接或响应异常: {exc}") from exc
        finally:
            self._lock.release()

    def js(self, expr: str, await_promise: bool = False, timeout: float | None = None):
        r = self.send("Runtime.evaluate", {"expression": expr, "returnByValue": True,
                                           "awaitPromise": await_promise}, timeout)
        res = r.get("result", {}).get("result", {})
        return res["value"] if "value" in res else r.get("result")

    def pump(self, seconds: float, on_event=None, sock_timeout: float = 2.0):
        deadline = time.monotonic() + max(0, seconds)
        if not self._lock.acquire(timeout=max(0, seconds)):
            raise TimeoutError("CDP pump 等待连接锁超时")
        try:
            while time.monotonic() < deadline:
                if self._closed:
                    raise CDPError("CDP 连接已关闭")
                self.ws.settimeout(min(sock_timeout, self._remaining(deadline, "pump")))
                try:
                    raw = self.ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if not raw:
                    raise ConnectionError("Chrome closed the CDP connection")
                ev = json.loads(raw)
                if on_event and "method" in ev:
                    on_event(ev)
        except Exception as exc:
            self._abort()
            raise CDPError(f"CDP pump 连接异常: {exc}") from exc
        finally:
            self._lock.release()

    def close(self):
        # Interrupt recv without waiting for the command lock or close handshake.
        self._abort()
        try:
            self.ws.shutdown()
        except Exception:
            pass


def http_json(url: str, timeout: float = 5.0):
    return requests.get(url, timeout=timeout).json()
