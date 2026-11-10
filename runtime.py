"""Bounded browser workers and cancellable thread-to-async streaming.

Generation never occupies asyncio's default executor or AnyIO's HTTP pool.
Queued cancellation retains its slot until consumed, bounding queued bodies.
"""
from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import Future
import contextvars
import math
import queue
import threading
import time


class QueueFullError(RuntimeError):
    pass


class WorkerPool:
    def __init__(self, workers: int, pending: int, name: str = "generation"):
        self.workers = max(1, workers)
        self.capacity = self.workers + max(0, pending)
        self.name = name
        self._slots = threading.BoundedSemaphore(self.capacity)
        self._queue = queue.Queue(maxsize=self.capacity)
        self._lock = threading.Lock()
        self._threads = []
        self._closed = False
        self._active = 0
        self._admitted = 0

    def submit(self, fn, /, *args, **kwargs) -> Future:
        with self._lock:
            if self._closed:
                raise QueueFullError("Generation workers are shutting down")
            if not self._slots.acquire(blocking=False):
                raise QueueFullError("Generation queue is full; retry later")
            future = Future()
            self._admitted += 1
            self._queue.put_nowait((future, contextvars.copy_context(), fn, args, kwargs))
            if len(self._threads) < self.workers:
                thread = threading.Thread(target=self._worker,
                                          name=f"{self.name}-{len(self._threads) + 1}",
                                          daemon=True)
                self._threads.append(thread)
                thread.start()
            return future

    def _worker(self):
        while True:
            try:
                item = self._queue.get(timeout=0.2)
            except queue.Empty:
                with self._lock:
                    if self._closed:
                        return
                continue
            future, context, fn, args, kwargs = item
            running = False
            result = None
            try:
                if future.set_running_or_notify_cancel():
                    running = True
                    with self._lock:
                        self._active += 1
                    try:
                        result = context.run(fn, *args, **kwargs)
                    except BaseException as exc:
                        future.set_exception(exc)
                    else:
                        future.set_result(result)
            finally:
                with self._lock:
                    if running:
                        self._active -= 1
                    self._admitted -= 1
                self._slots.release()
                self._queue.task_done()
                del item, future, context, fn, args, kwargs, result

    async def run(self, fn, /, *args, **kwargs):
        return await asyncio.wrap_future(self.submit(fn, *args, **kwargs))

    def snapshot(self):
        with self._lock:
            return {"workers": self.workers, "active": self._active,
                    "queued": self._admitted - self._active,
                    "capacity": self.capacity, "closed": self._closed}

    def shutdown(self, timeout: float = 5.0):
        with self._lock:
            self._closed = True
        while True:
            try:
                future, context, fn, args, kwargs = self._queue.get_nowait()
            except queue.Empty:
                break
            future.cancel()
            with self._lock:
                self._admitted -= 1
            self._slots.release()
            self._queue.task_done()
        deadline = time.monotonic() + timeout
        for thread in list(self._threads):
            thread.join(max(0, deadline - time.monotonic()))


class StreamBridge:
    """An async consumer never parks an AnyIO/default-executor thread."""
    def __init__(self, max_items: int = 32, backpressure_timeout: float = 30.0):
        if not math.isfinite(backpressure_timeout) or backpressure_timeout <= 0:
            raise ValueError("backpressure_timeout must be finite and positive")
        self.loop = asyncio.get_running_loop()
        self.ready = asyncio.Event()
        self.stop_event = threading.Event()
        self._condition = threading.Condition()
        self._items = deque()
        self._max_items = max(1, max_items)
        self._backpressure_timeout = backpressure_timeout
        self._done = False
        self._error = None
        self.future = None

    def _wake(self):
        try:
            self.loop.call_soon_threadsafe(self.ready.set)
        except RuntimeError:
            self.cancel()

    def put(self, value):
        deadline = time.monotonic() + self._backpressure_timeout
        accepted = False
        with self._condition:
            while len(self._items) >= self._max_items and not self.stop_event.is_set() and not self._done:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._error = TimeoutError("Stream consumer stalled: backpressure timeout")
                    self._done = True
                    self.stop_event.set()
                    self._items.clear()
                    self._condition.notify_all()
                    break
                self._condition.wait(timeout=remaining)
            if not self.stop_event.is_set() and not self._done:
                self._items.append(value)
                accepted = True
        self._wake()
        return accepted

    def finish(self, error=None):
        with self._condition:
            if not self._done:
                self._error = error
            self._done = True
            self._condition.notify_all()
        self._wake()

    def cancel(self):
        self.stop_event.set()
        with self._condition:
            self._items.clear()
            self._error = None
            self._done = True  # Late producer finish(error) must not reattach traceback locals.
            self._condition.notify_all()
        if self.future is not None:
            self.future.cancel()
        # Do not recurse through _wake if the event loop has already closed.
        try:
            self.loop.call_soon_threadsafe(self.ready.set)
        except RuntimeError:
            pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            with self._condition:
                if self._items:
                    value = self._items.popleft()
                    self._condition.notify_all()
                    return value
                if self.stop_event.is_set():
                    if self._error is not None:
                        raise self._error
                    raise StopAsyncIteration
                if self._done:
                    if self._error is not None:
                        raise self._error
                    raise StopAsyncIteration
                self.ready.clear()
            await self.ready.wait()
