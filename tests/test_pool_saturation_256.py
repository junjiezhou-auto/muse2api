"""Saturation check for MUSE2API_CONCURRENCY=256 with a stubbed engine.

Run: python tests/test_pool_saturation_256.py
No network, no Chromium, no real accounts: engine calls are replaced with a
controllable barrier so all 400 submissions stay in flight simultaneously.
"""
import importlib.util
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
home = tempfile.mkdtemp(prefix="muse256-")
os.environ.update(MUSE2API_HOME=home, MUSE2API_PROFILE_ROOT=home,
                  MUSE2API_KEY="test-only", MUSE2API_CONCURRENCY="256",
                  MUSE2API_MAX_PENDING="64", MUSE2API_CHAT_TIMEOUT="30")
sys.path.insert(0, str(ROOT))

import requests
import uvicorn

spec = importlib.util.spec_from_file_location("muse256_app", ROOT / "app.py")
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)


def rss_mb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return 0.0


rss_before = rss_mb()

# 300 stub accounts so the account pool never becomes the bottleneck.
for i in range(300):
    app.store.add_account({"hatch_sess": f"stub-{i}"}, f"acc{i}")

HOLD = threading.Event()


def fake_chat_stream(cookies, prompt, expires, timeout, account_id=None, stop_event=None):
    HOLD.wait(15)  # keep every admitted worker in flight until released
    yield "hello "
    yield "world"


def fake_generate(prompt, kind, timeout, **kwargs):
    HOLD.wait(15)
    media = Path(app.CFG.media_dir)
    media.mkdir(parents=True, exist_ok=True)
    fixture = media / "fixture.bin"
    fixture.write_bytes(b"0" * 16)
    return {"filename": fixture.name, "size": 16, "kind": kind,
            "path": str(fixture)}, "fixture-account"


patches = [
    patch.object(app.engine, "chat_stream", fake_chat_stream),
    patch.object(app, "_run_generation", fake_generate),
    patch.object(app, "_renew_and_persist",
                 side_effect=lambda ident, **_: app.store.get_account(ident)),
    patch.object(app, "_sync_cookies", lambda *_: None),
    patch.object(app.engine, "start"),
    patch.object(app.engine, "close_current_page"),
]
for p in patches:
    p.start()

PORT = 8099
BASE = f"http://127.0.0.1:{PORT}"
server = uvicorn.Server(uvicorn.Config(app.app, host="127.0.0.1",
                                       port=PORT, log_level="warning"))
threading.Thread(target=server.run, daemon=True).start()
for _ in range(100):
    try:
        requests.get(BASE + "/healthz", timeout=1)
        break
    except Exception:
        time.sleep(0.1)
else:
    raise AssertionError("stub server did not start")

HEADERS = {"Authorization": "Bearer test-only"}
results = {"chat200": 0, "chat429": 0, "video200": 0, "video429": 0, "other": 0}
lock = threading.Lock()


def chat_job(_index):
    try:
        r = requests.post(BASE + "/v1/chat/completions", timeout=30, headers=HEADERS,
                          json={"messages": [{"role": "user", "content": "hi"}],
                                "stream": True, "timeout": 8})
    except Exception:
        with lock:
            results["other"] += 1
        return
    with lock:
        if r.status_code == 200:
            results["chat200"] += 1
        elif r.status_code == 429:
            results["chat429"] += 1
        else:
            results["other"] += 1
            print("unexpected chat status", r.status_code, r.text[:120])


def video_job(_index):
    try:
        r = requests.post(BASE + "/v1/videos", timeout=30, headers=HEADERS,
                          json={"prompt": "cat", "timeout": 8})
    except Exception:
        with lock:
            results["other"] += 1
        return
    with lock:
        if r.status_code in (200, 202):
            results["video200"] += 1
        elif r.status_code == 429:
            results["video429"] += 1
        else:
            results["other"] += 1
            print("unexpected video status", r.status_code, r.text[:120])


sampler_latencies = []
sampler_stop = threading.Event()


def sampler():
    while not sampler_stop.is_set():
        started = time.monotonic()
        try:
            requests.get(BASE + "/healthz", timeout=5)
            sampler_latencies.append(time.monotonic() - started)
        except Exception:
            sampler_latencies.append(9.9)
        time.sleep(0.05)


threading.Thread(target=sampler, daemon=True).start()

TOTAL = 400  # 300 streaming chats + 100 async videos; capacity is 256+64=320
with ThreadPoolExecutor(max_workers=TOTAL) as pool:
    futures = [pool.submit(chat_job if i % 4 else video_job, i) for i in range(TOTAL)]
    deadline = time.monotonic() + 15
    saturated = None
    while time.monotonic() < deadline:
        saturated = app.GEN_POOL.snapshot()
        if saturated["active"] + saturated["queued"] >= 320:
            break
        time.sleep(0.05)
    assert saturated and saturated["active"] + saturated["queued"] >= 320, saturated
    peak_threads = len(app.GEN_POOL._threads)
    time.sleep(2.0)  # saturated window measured by the sampler
    HOLD.set()       # release every in-flight worker
    for future in futures:
        future.result(timeout=60)

sampler_stop.set()
time.sleep(0.2)
for p in patches:
    p.stop()

final = app.GEN_POOL.snapshot()
assert results["other"] == 0, results
assert results["chat200"] + results["chat429"] == 300, results
assert results["video200"] + results["video429"] == 100, results
assert results["chat429"] + results["video429"] >= TOTAL - 320, results
assert final["active"] == 0 and final["queued"] == 0, final
assert not app.GEN_LOCK.locked(), "GEN_LOCK leaked after full drain"
assert not app.ACCOUNT_POOL.busy_accounts, "account leases leaked after full drain"

p50 = statistics.median(sampler_latencies)
p99 = sorted(sampler_latencies)[max(0, int(len(sampler_latencies) * 0.99) - 1)]
assert p99 < 0.5, f"healthz degraded under saturation: p99={p99:.3f}s"

print(json.dumps({
    "submitted": TOTAL,
    "admitted_capacity": 320,
    "results": results,
    "saturated_snapshot": saturated,
    "peak_worker_threads": peak_threads,
    "final_snapshot": final,
    "healthz_samples": len(sampler_latencies),
    "healthz_p50_ms": round(p50 * 1000, 1),
    "healthz_p99_ms": round(p99 * 1000, 1),
    "healthz_max_ms": round(max(sampler_latencies) * 1000, 1),
    "rss_delta_mb": round(rss_mb() - rss_before, 1),
}, ensure_ascii=False))
print("PASS: 256-concurrency saturation is bounded, responsive and fully drained")
