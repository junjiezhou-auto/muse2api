"""Fault injection for context ownership, retry fairness and admission backpressure."""
import sys, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from engine import MuseEngine, MuseGenerationError
from config import Config

class Browser:
    def __init__(self):
        self.attempts=[]; self.live=None; self.created=[]
    def send(self, method, params=None, timeout=None):
        if method == 'Target.getBrowserContexts':
            if self.live is None: raise RuntimeError('inventory unavailable')
            return {'result': {'browserContextIds': self.live}}
        if method == 'Target.disposeBrowserContext':
            self.attempts.append(params['browserContextId'])
            raise RuntimeError('persistent failure')
        if method == 'Target.createBrowserContext':
            self.created.append(params)
            raise RuntimeError('response lost')
        raise AssertionError(method)

with tempfile.TemporaryDirectory() as home:
    cfg=Config(base_dir=home);cfg.ensure_dirs()
    e=MuseEngine(cfg); e.browser=Browser()
    ids={'ctx-%02d'%i for i in range(12)}
    e._pending_disposals.update(ids)
    for _ in range(3): e._retry_context_disposals()
    assert set(e.browser.attempts)==ids, 'permanent failures starved other contexts'
    assert e._pending_disposals==ids, 'live or unknown IDs must never be evicted'
    e.browser.live=['ctx-11','foreign-context']
    e._last_disposal_inventory=0
    e._retry_context_disposals()
    assert e._pending_disposals=={'ctx-11'}
    assert 'foreign-context' not in e.browser.attempts
    e.browser.live=None
    e._pending_disposals.update('blocked-%d'%i for i in range(64))
    try: e._open_page()
    except MuseGenerationError: pass
    else: raise AssertionError('must reject new context under disposal backlog')
    assert not e.browser.created
    e._pending_disposals.clear()
    try: e._open_page()
    except Exception: pass
    assert e.browser.created[-1]=={'disposeOnDetach':True}
print('PASS: fair retries, verified-absent reconciliation, no foreign disposal, backlog gating, direct detach cleanup')
