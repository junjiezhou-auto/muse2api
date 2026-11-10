"""Weakref regressions for idle-worker and late-finish retention (offline)."""
import asyncio
import gc
import sys
import threading
import time
import weakref
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime import WorkerPool, StreamBridge

class Payload:
    pass

def test_worker():
    pool = WorkerPool(1, 0)
    refs = []
    def produce():
        value = Payload()
        value.data = bytearray(1024 * 1024)
        refs.append(weakref.ref(value))
        return value
    def fail():
        raise RuntimeError('fixture')
    try:
        for _ in range(50):
            future = pool.submit(produce)
            future.result(timeout=2)
            del future
            deadline = time.monotonic() + 2
            while refs[-1]() is not None and time.monotonic() < deadline:
                gc.collect()
                time.sleep(.01)
            assert refs[-1]() is None, 'idle worker retained successful result'
            future = pool.submit(fail)
            try:
                future.result(timeout=2)
            except RuntimeError:
                pass
            del future
        gc.collect()
        assert all(ref() is None for ref in refs)
    finally:
        pool.shutdown()

async def test_late_finish():
    bridge = StreamBridge()
    bridge.cancel()
    def finish_error():
        value = Payload()
        ref = weakref.ref(value)
        try:
            raise RuntimeError('late producer failure')
        except RuntimeError as exc:
            bridge.finish(exc)
        return ref
    ref = finish_error()
    gc.collect()
    assert bridge._error is None
    assert ref() is None, 'late finish retained traceback after cancellation'
    try:
        await bridge.__anext__()
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError('cancel must remain terminal')

if __name__ == '__main__':
    test_worker()
    asyncio.run(test_late_finish())
    print('PASS: 50 success/failure cycles release idle results; late finish cannot restore traceback')
