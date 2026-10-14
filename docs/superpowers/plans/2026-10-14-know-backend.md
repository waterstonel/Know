# Know Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立能获取完整中英文文章、自动筛选、翻译并发布每日推荐的内容服务。

**Architecture:** FastAPI 提供版本化 API，独立 worker 执行采集与模型任务，PostgreSQL 保存正文、任务租约和原子发布批次。确定性规则控制准入与发布，模型只返回结构化判断。API 契约同时供 iOS 计划使用。

**Tech Stack:** Python 3.12、FastAPI、Pydantic 2、SQLAlchemy 2、Alembic、psycopg 3、httpx、feedparser、lxml、pytest、PostgreSQL 16、Docker Compose。安装使用 uv 并提交锁文件。

**Spec:** `docs/superpowers/specs/2026-10-14-know-design.md`（用户已确认）。客户端计划：`docs/superpowers/plans/2026-10-14-know-ios.md`。

## Global Constraints

- 使用现有 `/workspace/Know` checkout，不额外创建 Git worktree。
- Python 3.12；iOS 17+、SwiftUI、SwiftData；不引入 Redis。
- 七个领域、各最多 10 篇、首页最多 10 篇；首页尽量覆盖至少 5 个领域、同领域最多 3 篇。
- 时效类从原始发布时间保留 7 天；长期类从首次准入保留 365 天；重试不延长有效期。
- 到期只退出推荐池；正文、收藏和历史不因到期自动删除。
- 质量五项各 1–5 分，总分至少 18/25，可信度与可核实性分别至少 3。
- 共享推荐，不做行为个性化、账号系统、系统通知、管理网页或自主网络搜索。
- Asia/Shanghai；来源每 2 小时轮询，每日 06:00 开始编排。
- 凭据仅运行时注入；示例和测试结果不能作为真实采集、模型或部署成功的证据。

## Review Focus

1. 来源重定向到内网、超大或畸形 HTML：终止获取，不进入正文池（任务 2）。
2. 原文伪装指令、模型错误 JSON 或无依据评分：模型无工具权限，候选暂缓（任务 3）。
3. 过期、未来日期和重抓：不以重抓延长有效期，不推荐未来新闻（任务 4）。
4. worker 崩溃、双实例与不确定模型响应：租约恢复、唯一任务、防重复发布；计费不确定不能标记成功（任务 5）。
5. 分页时库存过期或新增：稳定顺序、不重放已有结果、不暴露未发布日批次（任务 6）。

## 文件边界与执行顺序

Python 包放在 `backend/src/know/`；测试放在 `backend/tests/`。`contracts.py` 定义 DTO；`db.py`/`models.py`/迁移保存数据；`sources/` 与 `documents.py` 获取解析；`agent/` 负责模型；`recommendations.py` 负责规则；`jobs.py`/`worker.py` 负责调度；`api.py` 只负责 HTTP。共享 API 文档及固定 JSON 样本放 `shared/`。

先完成任务 1 固定契约；任务 2–5 建立完整数据链；任务 6 完成服务联调。客户端可以在任务 1 契约完成后使用明确标为测试的样本开展开发。

### Task 1: 契约、数据库与可运行测试环境

**Files:** Create `backend/pyproject.toml`, `backend/uv.lock`, `backend/src/know/{__init__,settings,contracts,db,models}.py`, `backend/alembic.ini`, `backend/migrations/`, `backend/tests/{conftest,test_contracts,test_database}.py`, `compose.yaml`, `shared/api-v1.md`, `shared/fixtures/{edition,article}.json`。

**Interfaces:**
- `DomainID` 固定为 `literature_arts`, `history_civilization`, `economy_finance`, `society_world`, `science_health`, `technology_industry`, `philosophy_thought`。
- `ArticleCard`: `id: UUID`, `domain_id: DomainID`, `title: str`, `summary_zh: str`, `cover_url: str | None`, `source_name: str`, `source_url: str`, `author: str | None`, `published_at: datetime | None`, `reading_minutes: int`, `language: Literal["zh","en"]`, `recommendation_reason: str`。
- `DocumentBlock`: `id: str`, `kind` 为 paragraph/heading/list/quote/image/table/code，`text: str | None`, `level: int | None`, `items: list[str]`, `url: str | None`, `caption: str | None`, `rows: list[list[str]]`, `links: list[{text,url}]`。列表与表格默认空数组；块 ID 文档内唯一。
- `ArticleDetail`: `card: ArticleCard`, `version: int`, `original: list[DocumentBlock]`, `translation: list[DocumentBlock] | None`, `translation_status: pending/ready/failed/not_needed`, `body_available: bool`。
- `Edition`: `id: UUID`, `date: date`, `timezone: str`, `scope: str`, `items: list[ArticleCard]`, `coverage_limited: bool`；`ArticlePage`: `items: list[ArticleCard]`, `next_cursor: str | None`。
- `DomainSummary`: `id: DomainID`, `name: str`, `count: int`；domains 路由返回 `list[DomainSummary]`。
- `SourceConfig`: ID、名称、`kind: rss/json_api/web_index`、入口 URL、允许主机、语言、正文 CSS selector、作者/日期 selector、`full_text_allowed: bool`；json_api 配置候选字段映射，web_index 配置文章链接 selector。配置无凭据。
- 数据库实体与 spec 一致；增加文章 normalized URL 唯一键、版本正文 fingerprint、任务 idempotency key 唯一键、批次 `(date,scope)` 唯一键。

- [ ] **Step 1:** 创建数据库测试容器、依赖清单与测试夹具；写 `test_contract_samples_round_trip`、`test_duplicate_job_key_rejected`、`test_migration_upgrade`。断言 `len(set(DomainID)) == 7`、样本 `ArticleDetail.original` 非空且块 ID 唯一、重复任务键触发唯一约束。数据库测试使用 PostgreSQL，不用 SQLite 替代。
- [ ] **Step 2:** `cd backend && uv run pytest tests/test_contracts.py tests/test_database.py -v`；确认失败指向尚未实现的契约/迁移。
- [ ] **Step 3:** 实现上述契约及 `create_session_factory(database_url: str) -> async_sessionmaker`；保存 API 字段、错误格式 `{code,message}` 和 ISO 8601 UTC 时间格式。提供隔离测试数据库夹具，结束后只清理测试数据。
- [ ] **Step 4:** 执行 Step 2、`uv run alembic upgrade head`；确认测试通过、空数据库迁移成功。
- [ ] **Step 5:** 提交任务所列文件，消息 `feat: establish content contracts and database`。

### Task 2: 来源获取与完整正文解析

**Files:** Create `backend/src/know/sources/{__init__,http,rss,json_api,web_index,registry}.py`, `backend/src/know/documents.py`, `backend/tests/test_ingestion.py`, `backend/tests/fixtures/{zh_article,en_article,preview_only,structured_article}.html`。

**Interfaces:** Consumes `SourceConfig`, `DocumentBlock`；produces `fetch_candidates(source: SourceConfig) -> list[Candidate]`, `fetch_document(candidate: Candidate, source: SourceConfig) -> ParsedDocument`。`Candidate` 保存 URL、标题、来源 ID、原始日期；`ParsedDocument` 保存候选、blocks、作者、语言、normalized URL、fingerprint、`complete: bool`、失败原因。

- [ ] **Step 1:** 写 `test_zh_en_full_text`、`test_structure_and_order_preserved`、`test_preview_rejected`、`test_redirect_private_host_rejected`、`test_oversize_response_rejected`。断言块顺序及图片图注/表格/引用/代码/链接保留、导航广告不存在、预览 `complete is False`；通过模拟 HTTP 响应测试重定向内网及大小限制。
- [ ] **Step 2:** `uv run pytest tests/test_ingestion.py -v`；确认解析/连接器缺失导致失败。
- [ ] **Step 3:** 实现 RSS、JSON API 字段映射与网页索引候选连接器，以及按来源 selector 提取正文；每跳验证 HTTPS、允许主机和解析 IP，禁止内部/回环地址。限制最多 3 次重定向、20 秒超时、5 MiB 正文下载；拒绝非 HTML 正文，校验 TLS。完整性由来源规则和正文检查共同判定，不只按字数判定。
- [ ] **Step 4:** 执行 Step 2；测试三种入口产生同一 Candidate 结构、相对链接规范化、重复 URL、无图片的有效文章和畸形 HTML 不会中断整个来源任务。
- [ ] **Step 5:** 提交，消息 `feat: ingest and normalize complete articles`。

### Task 3: 模型评估、准入依据与翻译

**Files:** Create `backend/src/know/agent/{__init__,provider,evaluation,translation}.py`, `backend/tests/{test_evaluation,test_translation}.py`。

**Interfaces:** Consumes `ParsedDocument`, `DocumentBlock`；produces `ModelProvider.generate_json(task: str, payload: dict, request_key: str) -> dict`, `evaluate_document(document: ParsedDocument, provider: ModelProvider) -> EvaluationResult`, `translate_document(blocks: list[DocumentBlock], provider: ModelProvider, request_key: str) -> TranslationResult`。EvaluationResult 包含领域、tags、类型、时效标签、五项 scores、summary_zh、reason、引用原文块的 evidence、模型/策略版本及事件标签；TranslationResult 包含完整 blocks 与 ready/failed 状态。

- [ ] **Step 1:** 写 `test_quality_thresholds`、`test_grounded_evidence_required`、`test_invalid_model_response_quarantined`、`test_instruction_in_article_is_data`、`test_translation_structure_preserved`、`test_partial_translation_not_ready`。断言 17 分失败、18 分且两项均 3 时可过质量门槛、可信度 2 失败；不存在的依据块拒绝；译文块 ID/顺序/数字/URL 与原文一致。
- [ ] **Step 2:** `uv run pytest tests/test_evaluation.py tests/test_translation.py -v`；确认尚未实现行为失败。
- [ ] **Step 3:** 实现 OpenAI-compatible JSON 服务适配器与无工具评估；凭据仅 Settings 注入。schema 校验、依据块引用和摘录匹配、数字/链接校验失败不发布。模型失败记录可重试状态；原文始终保留。翻译将结构与文本拆分，完成全篇后才原子标记 ready。
- [ ] **Step 4:** 执行 Step 2；测试文学艺术与新闻使用不同可核实性提示，但共享评分边界。替身测试明确命名为 offline，不能当作真实模型验证。
- [ ] **Step 5:** 提交，消息 `feat: evaluate and translate grounded content`。

### Task 4: 有效期、去重及每日推荐编排

**Files:** Create `backend/src/know/recommendations.py`, `backend/tests/test_recommendations.py`。

**Interfaces:** Consumes `EvaluationResult` 与文章记录；produces `compute_expiry(kind: str, published_at: datetime | None, first_admitted_at: datetime) -> datetime`, `is_eligible(article: Article, evaluation: EvaluationResult, now: datetime) -> bool`, `build_editions(candidates: list[RankedArticle], now: datetime) -> EditionDraft`。RankedArticle 保存文章、评估、事件组与实质视角标签；EditionDraft 保存各领域 UUID 序列、首页 UUID 序列、覆盖不足标记。

- [ ] **Step 1:** 写 `test_expiry_boundary`、`test_refetch_preserves_first_admission`、`test_future_or_undated_news_rejected`、`test_domain_caps_and_home_diversity`、`test_no_quality_padding`、`test_same_event_dedup`。断言第 7 天及第 365 天的截止瞬间不可推荐；各领域 `<=10`、首页 `<=10`、条件充足时 `>=5` 领域且每领域 `<=3`。
- [ ] **Step 2:** `uv run pytest tests/test_recommendations.py -v`；确认失败。
- [ ] **Step 3:** 实现准入、有效期和排序：质量分降序，时效内容按发布日期、长期内容按首次准入日期，再按 UUID 稳定排序；先按领域与来源轮换，首页先分配不同领域再补位。同事件依据事件标签与实体/日期匹配归组，仅实质不同视角可共存。已有日批次入选过的内容不再作为下一日“新精选”，但仍可供库存阅读。
- [ ] **Step 4:** 执行 Step 2；测试提前失效、库存不足、只有两个领域有内容、不同视角保留和稀缺来源的可解释结果。
- [ ] **Step 5:** 提交，消息 `feat: compose diverse editions with expiry rules`。

### Task 5: 持久化 worker、调度与发布

**Files:** Create `backend/src/know/{jobs,worker,cli}.py`, `backend/tests/{test_jobs,test_publication}.py`, `backend/config/sources.example.yaml`。

**Interfaces:** Consumes任务 2–4；produces `claim_job(session: AsyncSession, worker_id: str, now: datetime) -> Job | None`, `run_job(job: Job, session: AsyncSession, provider: ModelProvider) -> None`, `publish_editions(session: AsyncSession, draft: EditionDraft, edition_date: date) -> None`；CLI `know sources import PATH`、`know ingest --once`、`know editions build --date YYYY-MM-DD`、`know worker`。

- [ ] **Step 1:** 写 `test_two_workers_one_claim`、`test_expired_lease_reclaimed`、`test_completed_step_reused`、`test_atomic_edition_publication`、`test_previous_edition_keeps_real_date`；断言并发只取得一份租约，事务中途失败不会暴露部分日批次，已完成模型结果不会再次调用 provider。
- [ ] **Step 2:** `uv run pytest tests/test_jobs.py tests/test_publication.py -v`；确认失败。
- [ ] **Step 3:** 实现 `FOR UPDATE SKIP LOCKED`、120 秒租约及 30 秒心跳；失败按 30/120/600 秒退避，最多 3 次，超限终止并记录原因。调度按配置时区和唯一时间槽键入队；批次一次事务发布。来源配置导入验证全文能力，示例配置明确禁用，不假装已接通来源。
- [ ] **Step 4:** 执行 Step 2 与两次同日 build；检查批次键及条目无重复。模型使用稳定 request_key；服务支持幂等键时透传。请求已送达但未落库的崩溃窗口标为 uncertain，不自动重复计费；可恢复结果后再继续，不能承诺外部服务天然 exactly-once。
- [ ] **Step 5:** 提交，消息 `feat: schedule durable curation and atomic publication`。

### Task 6: API、部署步骤及真实集成验证

**Files:** Create `backend/src/know/api.py`, `backend/tests/test_api.py`, `backend/tests/test_live_pipeline.py`, `backend/Dockerfile`, `.env.example`, `README.md`, `docs/operations.md`, `scripts/smoke_backend.py`；Modify `compose.yaml`, `shared/api-v1.md`。

**Interfaces:** 实现 spec 全部 HTTP 路由；`GET /v1/articles` 参数 `domain_id?`, `cursor?`, `exclude_id`（可重复）、`limit`（默认 20、最大 50）。游标保存库存快照时间及最后 `(first_admitted_at,id)`，只返回快照时已存在且现在有效的条目。排除 ID 上限 500，客户端进一步本地去重并顺次取下一页。

- [ ] **Step 1:** 写 `test_health_and_ready`、`test_expired_not_in_feed_but_detail_readable`、`test_pagination_during_insert_and_expiry`、`test_only_published_editions_visible`、`test_translation_retry_idempotent_and_limited`。断言过期正文详情仍 200、推荐过滤过期、连续页 UUID 无重复；重试接口 202、不内联等待模型，重复请求不重复排队，达到每 IP/文章每分钟 5 次后 429。
- [ ] **Step 2:** `uv run pytest tests/test_api.py -v`；确认路由或行为缺失失败。
- [ ] **Step 3:** 实现应用工厂、上述路由及参数校验；`/ready` 验证数据库。Compose 启动 API、worker、PostgreSQL，提供迁移、日志、停止及启动命令；生产运行使用 TLS 反向代理、持久数据库卷和模型预算设置。没有凭据时 API 仍可启动并报告 worker 外部依赖未配置。
- [ ] **Step 4:** `uv run pytest -m 'not live' -v`、`docker compose config --quiet`、`docker compose up -d --build`、`uv run python ../scripts/smoke_backend.py`。真实集成单独运行 `uv run pytest -m live tests/test_live_pipeline.py -v`，需一个已验证的中文源、英文源与模型配置；显式 skip 不算通过。真实测试检查获取正文、筛选、翻译与 HTTP 全文读取，而不是只检查连接。
- [ ] **Step 5:** 记录测试数量、失败/跳过与实际外部配置；检查 `git diff --check` 后提交 `feat: serve and validate the curation backend`。只在安装和服务启动实际验证后按 onboarding 工具保存安装脚本与启动指令；生产发布不属于配置草稿保存。

## 完成界限

离线测试、数据库集成、容器启动、真实采集和真实模型分别报告。没有模型凭据或来源配置可以完成独立开发，但不能宣称自动内容生产已完成；来源无法支持完整正文时更换为可支持的来源。至少一中一英真实验证只是管线验收，不代表七个领域已经具有足够生产供应；上线前需核对每个领域的来源与实际每日产量。

## 测试断言锚点

以下代码是对应任务测试的核心断言，夹具和调用由对应任务构建；不是已运行的测试。

```python
# Task 1 — test_contract_samples_round_trip
assert len(set(DomainID)) == 7
assert len({b.id for b in detail.original}) == len(detail.original) > 0
# Task 2 — test_preview_rejected / test_structure_and_order_preserved
assert preview.complete is False
assert [b.kind for b in structured.blocks] == ["heading", "paragraph", "list", "quote", "image", "table", "code"]
# Task 3 — test_quality_thresholds / test_partial_translation_not_ready
assert passes_quality(scores_17) is False
assert passes_quality(scores_18_with_required_3) is True
assert partial_translation.status == "failed"
# Task 4 — test_domain_caps_and_home_diversity
assert len(draft.home) <= 10
assert all(len(ids) <= 10 for ids in draft.domains.values())
assert len(home_domain_counts) >= 5  # 此夹具七个领域都有足量合格候选
assert max(home_domain_counts.values()) <= 3
# Task 5 — test_atomic_edition_publication
assert visible_editions_after_transaction_failure == previous_editions
assert provider_calls_after_completed_step_retry == provider_calls_before_retry
# Task 6 — test_expired_not_in_feed_but_detail_readable
assert expired_id not in {item.id for item in inventory.items}
assert expired_detail_response.status_code == 200
```

任务 3 的 `passes_quality(scores: QualityScores) -> bool` 在 `agent/evaluation.py` 定义；QualityScores 为 EvaluationResult 内五个 1–5 整数评分字段。测试夹具不得在准入函数内制造评分或直接绕过真实发布路径。
