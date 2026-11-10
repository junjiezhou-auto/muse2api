// Offline behavioral tests: node tests/test_api_only_web.mjs
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';

const html = readFileSync(new URL('../admin.html', import.meta.url), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
assert.doesNotMatch(script, /localStorage|sessionStorage|URLSearchParams|setInterval|\/admin\/update\/|api\.github\.com/);
assert.match(html, /<button class="active" data-t="play">/);
assert.match(html, /<section class="tab active" id="tab-play">/);
assert.doesNotMatch(html, /onclick="(?:upgradeNow|toggleRepoPanel)/);

const flush = () => new Promise(resolve => setImmediate(resolve));
const response = (data, status = 200) => ({status, ok:status >= 200 && status < 300, text:async () => JSON.stringify(data)});
const abortError = () => Object.assign(new Error('aborted'), {name:'AbortError'});

function harness(handler = () => response({})) {
  let now = 1000, timerId = 0;
  const timers = new Map(), elements = new Map(), requests = [], mediaLoads = [];
  class Element {
    constructor(tag = 'div', id = '') {
      this.tagName = tag.toUpperCase(); this.id = id; this.value = ''; this.textContent = '';
      this.disabled = false; this.children = []; this.dataset = {}; this.style = {}; this.listeners = {};
      this.classList = {add(){}, remove(){}, toggle(){}, contains(){return false;}};
    }
    set innerHTML(value) { this.html = value; this.children = []; }
    get innerHTML() { return this.html || ''; }
    set src(value) { mediaLoads.push(value); this.source = value; }
    appendChild(el) { this.children.push(el); return el; }
    addEventListener(type, fn) { this.listeners[type] = fn; }
    querySelectorAll() { return []; }
    remove() {}
  }
  const get = id => {
    if (!elements.has(id)) elements.set(id, new Element('div', id));
    return elements.get(id);
  };
  const tabs = ['pool','cookie','tasks','media','play','docs'].map(name => {
    const el = new Element('button'); el.dataset.t = name; return el;
  });
  get('imgPrompt').value = 'image prompt'; get('imgSize').value = '1:1';
  get('vidPrompt').value = 'video prompt'; get('vidSize').value = '16:9'; get('vidDur').value = '5';
  const document = {
    getElementById:get, querySelectorAll:selector => selector === 'nav.tabs button' ? tabs : [],
    createElement:tag => new Element(tag), body:new Element('body'), documentElement:new Element('html'), title:'muse2api',
  };
  const ctx = vm.createContext({
    document, navigator:{language:'zh-CN'}, location:{origin:'http://test.invalid', search:'?key=ignored'},
    window:{alert(){}, confirm(){}, prompt(){}}, MutationObserver:class {observe(){}},
    URL, AbortController, console,
    Date:class extends Date {static now(){return now;}},
    setTimeout(fn, ms) { const id = ++timerId; timers.set(id, {fn, at:now + ms}); return id; },
    clearTimeout(id) { timers.delete(id); },
    setInterval() { throw new Error('global polling is forbidden'); },
    fetch(path, opts) { requests.push({path, opts}); return handler(path, opts, requests.length); },
  });
  vm.runInContext(script, ctx, {filename:'admin.html'});
  return {
    ctx, get, requests, mediaLoads, timers, tabs,
    async advance() {
      await flush();
      const entry = [...timers].sort((a,b) => a[1].at-b[1].at)[0];
      if (!entry) return false;
      timers.delete(entry[0]); now = entry[1].at; entry[1].fn(); await flush(); return true;
    },
    async finish(promise) {
      let settled = false; promise.finally(() => {settled = true;});
      for (let i=0; i<100 && !settled; i++) { await flush(); if (!settled) await this.advance(); }
      assert.ok(settled, 'test operation did not settle'); await promise;
    },
  };
}

{
  const h = harness();
  assert.equal(h.requests.length, 0, 'startup must not send requests');
  assert.equal(h.timers.size, 0, 'startup must not schedule network work');
  assert.equal(h.get('key').value, '', 'URL key must not be consumed');
  h.get('key').value = 'manual-key'; h.ctx.saveKey();
  assert.equal(h.requests.length, 0, 'applying an in-memory key must not refresh management');
  assert.equal(vm.runInContext('KEY', h.ctx), 'manual-key');
  console.log('PASS startup and key application make zero requests');
}

for (const kind of ['image','video']) {
  const endpoint = kind === 'image' ? '/v1/images/tasks' : '/v1/videos';
  const task = 'task/id';
  const done = kind === 'image'
    ? {id:task, status:'completed', data:[{url:'/v1/media/result.png'}]}
    : {id:task, status:'completed', result:{url:'/v1/media/result.mp4'}};
  const h = harness((path, opts) => response(opts.method === 'POST' ? {id:task,status:'queued'} : done));
  const promise = h.ctx.runTest(kind);
  await h.ctx.runTest(kind); // second click while the first is in flight
  await h.finish(promise);
  assert.deepEqual(h.requests.map(r => [r.path,r.opts.method || 'GET']), [[endpoint,'POST'],[endpoint+'/task%2Fid','GET']]);
  assert.equal(h.mediaLoads.length, 0, 'result rendering must not load media');
  assert.doesNotMatch(h.get(kind === 'image' ? 'imgRes' : 'vidRes').innerHTML, /<(?:img|video)\b/);
  h.ctx.previewTest(kind);
  assert.deepEqual(h.mediaLoads, ['http://test.invalid/v1/media/result.' + (kind === 'image' ? 'png' : 'mp4')]);
  h.ctx.previewTest(kind);
  assert.equal(h.mediaLoads.length, 1, 'preview cannot duplicate media elements');
  assert.equal(h.timers.size, 0);
  console.log('PASS', kind, 'single submission, bounded polling, click-only preview');
}

{
  const h = harness((_path, opts) => new Promise((resolve,reject) => opts.signal.addEventListener('abort',() => reject(abortError()),{once:true})));
  const promise = h.ctx.runImage(); await flush(); h.ctx.cancelTest('image'); await promise;
  assert.equal(h.requests.length,1); assert.equal(h.requests[0].opts.method,'POST');
  assert.match(h.get('imgRes').innerHTML,/停止等待.*后端任务可能继续运行/);
  assert.equal(h.get('imgBtn').disabled,false); assert.equal(h.get('imgCancel').disabled,true);
  assert.equal(h.timers.size,0);
  console.log('PASS cancel during POST neither retries nor promises backend cancellation');
}

{
  const h = harness(() => response({id:'keep-this-id',status:'queued'}));
  const promise = h.ctx.runVideo(); await flush(); h.ctx.cancelTest('video'); await promise;
  assert.equal(h.requests.length,1); assert.match(h.get('vidRes').innerHTML,/keep-this-id/);
  assert.equal(h.timers.size,0);
  console.log('PASS cancel during backoff preserves task ID and stops polling');
}

{
  const h = harness(() => response({id:'deadline-task',status:'queued'}));
  const promise = h.ctx.runVideo(); await flush();
  vm.runInContext("activeTests.get('video').deadline = Date.now() + 1",h.ctx);
  await h.finish(promise);
  assert.equal(h.requests.length,1); assert.match(h.get('vidRes').innerHTML,/截止时间/);
  assert.equal(h.get('vidBtn').disabled,false); assert.equal(h.timers.size,0);
  console.log('PASS total deadline stops task polling and restores controls');
}

{
  const h = harness((_path,opts) => response(opts.method === 'POST' ? {id:'query-failure',status:'queued'} : {detail:'temporary'},opts.method === 'POST' ? 202 : 503));
  await h.finish(h.ctx.runImage());
  assert.equal(h.requests.filter(r=>r.opts.method==='POST').length,1);
  assert.equal(h.requests.length,7); assert.match(h.get('imgRes').innerHTML,/连续失败/);
  assert.equal(h.get('imgBtn').disabled,false); assert.equal(h.timers.size,0);
  console.log('PASS repeated query errors are bounded and do not re-submit generation');
}

{
  const h = harness((_path,opts) => response(opts.method === 'POST' ? {id:'gone',status:'queued'} : {detail:'expired'},opts.method === 'POST' ? 202 : 410));
  await h.finish(h.ctx.runVideo());
  assert.equal(h.requests.length,2); assert.match(h.get('vidRes').innerHTML,/expired/);
  console.log('PASS expired task is not polled repeatedly');
}

{
  const h = harness((_path,opts) => new Promise((resolve,reject) => opts.signal.addEventListener('abort',()=>reject(abortError()),{once:true})));
  const promise = h.ctx.api('/v1/videos',{method:'POST',timeoutMs:10});
  const rejected = assert.rejects(promise,error=>error.name==='TimeoutError' && /结果可能未知/.test(error.message));
  await h.advance(); await rejected;
  assert.equal(h.requests.length,1); assert.equal(h.timers.size,0);
  console.log('PASS API request timeout aborts transport without retry');
}

{
  const h = harness((_path,opts) => new Promise((resolve,reject) => opts.signal.addEventListener('abort',()=>reject(abortError()),{once:true})));
  const controller = new AbortController();
  const promise = h.ctx.api('/v1/videos/id',{signal:controller.signal});
  const rejected = assert.rejects(promise,error=>error.name==='AbortError');
  controller.abort(); await rejected;
  assert.equal(h.timers.size,0);
  console.log('PASS API external abort cleans its timeout');
}

{
  const h = harness();
  const state = {started:1000,httpStatus:200,result:{status:'completed',data:[{url:'javascript:alert(1)'}]}};
  h.ctx.renderTestResult('image',state); h.ctx.previewTest('image');
  assert.equal(h.get('imgRes').children.length,0); assert.equal(h.mediaLoads.length,0);
  console.log('PASS unsafe media scheme is not made clickable or previewed');
}
console.log('ALL PASS');
