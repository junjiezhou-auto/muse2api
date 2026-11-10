# 生产加固说明（相对上游 v1.6.9）

本仓库是 [`czg86389-hub/muse2api`](https://github.com/czg86389-hub/muse2api) 的 fork，用于单机多账号的长期无人值守部署。
上游功能（对话 / 生图 / 视频、OpenAI 兼容接口、多账号池、48h 会话续期、Chrome 导号扩展）保持不变，
下列改动全部针对「高并发 + 长时间运行」暴露出的稳定性问题。许可证沿用上游 MIT。

## 背景

单机 20 个 muse.ai 账号、并发上限 256 的部署在运行数周后出现过一次整机级故障：
容器没有内存上限，Chromium 进程累积到 460 个、占用约 29.6GB，内核在 6 小时内 OOM kill 37 次，
最终把 journald 与 containerd 一起拖垮，只能硬重启。随后的排查还发现：
高并发时 Python 单进程线程池被打满导致接口整体卡死、每次任务完成都全量重写 `tasks.json`、
账号会话失效后 CDP 调用会长时间挂住。以下改动逐一对应这些问题。

## 改动清单

### 1. 有界生成池与背压（`runtime.py`，新增）

- 生成任务不再直接使用 asyncio 默认 executor / AnyIO 线程池，改为独立的有界 `WorkerPool`：
  固定 worker 数 + 有界等待队列，队列满时立即返回 429（`QueueFullError`），不再无界创建线程。
- 每个排队任务保留自己的 contextvars 上下文；排队中的取消会保留槽位直到被消费，避免队列被取消请求无限撑大。
- 配置项：`MUSE2API_CONCURRENCY`（worker 数）、`MUSE2API_MAX_PENDING`（排队上限，默认 64）。

### 2. 任务内存化（`store.py`、`engine.py`）

- 任务状态改为内存表 + TTL 保留（`MUSE2API_TASK_TTL_SECONDS`，默认 3600 秒）与条数上限
  （`MUSE2API_TASK_MAX_ENTRIES`，默认 2048），不再每次任务变更都全量重写 `data/tasks.json`。
- 账号数据的落盘做了节流（`MUSE2API_ACCOUNT_FLUSH_INTERVAL`），避免高频写盘。

### 3. CDP 与浏览器上下文生命周期（`cdp.py`、`engine.py`）

- CDP 断连 fail-fast：浏览器进程/调试端口异常时立即失败，不再依赖超时把整个请求线程挂住。
- 上下文与页面关闭增加了强清理与重试：`Target.disposeBrowserContext`、下载目录清理、残留上下文回收，
  失败会重试有限次数，避免 Chromium 上下文/进程泄漏累积。

### 4. 按账号独立代理（`proxy.py`、`app.py`、`admin.html`）

- 支持为每个账号单独配置代理（含认证），并保存「已实测连通」状态；
  端口/协议校验、临时测试不污染已保存路线；切换节点、密码或 Cookie 后不继承旧的测试成功结果。
- 管理页对应提供了代理测试、批量导入与状态展示。

### 5. 媒体反代服务（`media-proxy/`、`docker-compose.media.yml`，新增）

- 独立 nginx 容器 `muse-media`（默认 18611），只反代 `/v1/media/`，其余路径一律 404，
  用于把内部媒体目录以只读、可缓存的方式暴露给下游。
- 上游需要管理 Key：配置以模板形式提供（`nginx.conf.template`），容器启动时由官方镜像的
  envsubst 机制用 `MUSE2API_KEY` 环境变量渲染。**仓库内不含任何密钥**。

### 6. 部署参数与运维配套（`Dockerfile`、`docker-compose.yml`）

- `docker-compose.yml` 的 `environment` 段显式引用环境变量（`${MUSE2API_XXX:-默认值}`），
  否则只改 `.env` 不会生效——这是踩过的坑，请勿退回成硬编码。
- 增加 `mem_limit` / `memswap_limit`（示例为 24g）防止 Chromium 堆积吃满整机；
  `shm_size: 2gb` 避免多标签渲染时 `/dev/shm` 耗尽。
- 建议配合每日低峰重启（例如 cron 04:00 `docker restart muse2api`）释放长期累积的浏览器上下文。

### 7. 测试

新增/更新了覆盖上述改动的测试：`tests/test_runtime_store.py`、`test_runtime_concurrency.py`、
`test_engine_runtime.py`、`test_cdp_runtime.py`、`test_stream_backpressure.py`、
`test_pool_saturation_256.py`、`test_worker_retention.py`、`test_context_lifecycle_cleanup.py`、
`test_cleanup_exception_safety.py`、`test_retry_memory_lifecycle.py`、`test_store_scheduler_queries.py`、
`test_account_proxy_routing.py`、`test_api_only_web.mjs` 等。

## 部署

```bash
git clone <this repo> muse2api
cd muse2api
cp .env.example .env        # 填入 MUSE2API_KEY 等
docker compose up -d --build
docker compose -f docker-compose.media.yml up -d --build   # 可选：媒体反代
```

关键环境变量：

| 变量 | 说明 | 示例 |
| --- | --- | --- |
| `MUSE2API_KEY` | 管理与 API 鉴权 Key（`m2a_` 开头），留空则首次启动自动生成并打印 | 必填 |
| `MUSE2API_CONCURRENCY` | 同时进行的生成任务数（每个任务一个浏览器上下文） | 4 ~ 16 |
| `MUSE2API_MAX_PENDING` | 生成队列排队上限，超出直接 429 | 64 |
| `MUSE2API_TASK_TTL_SECONDS` / `MUSE2API_TASK_MAX_ENTRIES` | 任务内存保留时长与条数上限 | 3600 / 2048 |
| `MUSE2API_IMAGE_TIMEOUT` / `MUSE2API_VIDEO_TIMEOUT` / `MUSE2API_CHAT_TIMEOUT` | 各类任务超时（秒） | 240 / 1800 / 300 |

并发建议：实际并发受账号数限制（同一账号互斥），把 `CONCURRENCY` 调到远超可用账号数只会在队列里空转，
并放大失败重试造成的浏览器上下文创建。

## 排障速记

- `GET /healthz`、`GET /readyz`（无可用账号时 `readyz` 返回 `no_account`）；`GET /admin/status`（Bearer 管理 Key）看账号池与队列。
- 返回体里的 `usage` 是固定的 0，**没有 token 统计**；要统计看 `data/tasks.json` 或 `GET /admin/tasks`（`kind/status/elapsed/account/error`）。
- 账号被上游登出后需要重新导入 cookie：管理页「更新 Cookie」（`POST /admin/accounts/{id}/cookies`）；
  浏览器扩展的导入接口是**新增**账号（`POST /admin/accounts`），不会覆盖旧账号。
- 容器内存接近上限、或出现新增 OOM kill 时优先怀疑浏览器上下文泄漏，重启容器是止血手段，根因看 `data/chromium.log`。
