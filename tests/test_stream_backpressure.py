"""Offline slow-consumer and queued-cancellation regressions."""
import asyncio
import sys
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime import StreamBridge, WorkerPool

async def scenario():
    pool = WorkerPool(1, 1)
    bridge = StreamBridge(max_items=1, backpressure_timeout=0.05)
    def produce():
        assert bridge.put('first')
        assert not bridge.put('blocked')
        bridge.finish()  # Must not overwrite timeout with success.
    bridge.future = pool.submit(produce)
    await asyncio.wrap_future(bridge.future)
    assert bridge.stop_event.is_set()
    try:
        await bridge.__anext__()
        raise AssertionError('timeout was hidden')
    except TimeoutError:
        pass
    assert not bridge.put('late')
    assert await asyncio.wrap_future(pool.submit(lambda: 'released')) == 'released'
    bridge.cancel()
    try:
        await bridge.__anext__()
        raise AssertionError('cancel must end iteration')
    except StopAsyncIteration:
        pass

    normal = StreamBridge(max_items=2)
    assert normal.put('a') and normal.put('b')
    normal.finish()
    assert [x async for x in normal] == ['a', 'b']
    waiting = StreamBridge()
    consumer = asyncio.create_task(waiting.__anext__())
    await asyncio.sleep(0)
    waiting.cancel()
    try:
        await asyncio.wait_for(consumer, 1)
        raise AssertionError('cancel must wake consumer')
    except StopAsyncIteration:
        pass
    pool.shutdown()

    # Test the actual app submit function in isolation, avoiding app startup.
    import ast
    source = Path(__file__).resolve().parents[1].joinpath('app.py').read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == '_submit_chat_stream')
    pool = WorkerPool(1, 1)
    hold = threading.Event()
    started = threading.Event()
    def blocker():
        started.set()
        hold.wait(2)
    first = pool.submit(blocker)
    assert started.wait(1)
    called = []
    from runtime import QueueFullError
    ns = dict(StreamBridge=StreamBridge, GEN_POOL=pool, QueueFullError=QueueFullError,
              _chat_stream_core=lambda *a: called.append(True))
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'app.py', 'exec'), ns)
    queued = ns['_submit_chat_stream']('test', 1)
    assert queued.future is not None
    queued.cancel()
    assert queued.future.cancelled()
    hold.set()
    await asyncio.wrap_future(first)
    deadline = time.monotonic() + 2
    while pool.snapshot()['queued'] and time.monotonic() < deadline:
        await asyncio.sleep(.01)
    assert not called
    assert pool.snapshot()['queued'] == 0
    pool.shutdown()

if __name__ == '__main__':
    asyncio.run(scenario())
    print('PASS: backpressure timeout, slot recovery, finish/error, cancellation wake and actual queued app cancellation')
