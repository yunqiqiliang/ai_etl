# 设计提案：将 LLM Batch 推理封装为云器 Lakehouse AI Function in SQL

## 1. 背景

### 1.1 现状

云器 Lakehouse 已提供 `AI_COMPLETE` 和 `AI_EMBEDDING` 两个 SQL 标量函数，底层调用实时推理 API（同步请求-响应），适用于交互式分析和小批量场景。

```sql
SELECT AI_COMPLETE('endpoint:qwen3max', '中国的首都在哪里?');
```

### 1.2 问题

| 限制 | 实时 API | Batch API |
|------|---------|-----------|
| 成本 | 标准价 | **50% 折扣** |
| 规模 | 百级（QPM/TPM 限制） | **万级**（5 万/文件） |
| 容错 | 自行管理重试 | 服务端重试 + error 文件 |
| 多模态 | 支持 | 支持 |

### 1.3 核心矛盾

SQL 是同步的，Batch API 是异步的。Provider 承诺的 completion_window 通常是 **24 小时**。SQL 连接不能阻塞 24 小时。

### 1.4 已验证的基础

[ai_etl 项目](https://github.com/yunqiqiliang/ai_etl) 已实现完整的 Batch 推理 ETL 流水线并通过端到端验证（DashScope + ZhipuAI，Table + Volume 双数据源并行，增量处理，中断恢复）。

## 2. 首选方案：SQL 函数三件套

### 2.1 设计理念

将 Batch 任务的生命周期拆分为三个独立的 SQL 操作，每个操作都是秒级返回：

| 操作 | 函数 | 耗时 | 说明 |
|------|------|------|------|
| 提交 | `AI_BATCH_SUBMIT()` | 秒级 | 构建 JSONL → 上传 → 创建 batch → 返回 batch_id |
| 查状态 | `AI_BATCH_STATUS()` | 秒级 | 查询 Provider API → 返回进度 |
| 查结果 | 直接 `SELECT` 目标表 | 秒级 | 后台 worker 自动收割结果写入目标表 |

用户不需要等待 batch 完成。提交后可以关闭连接，随时回来查状态，结果自动出现在目标表中。

### 2.2 用户体验

#### 场景一：Table 模式（结构化文本）

```sql
-- Step 1: 提交 batch（秒级返回）
SELECT AI_BATCH_SUBMIT(
    model         => 'endpoint:qwen3max',
    source        => TABLE(SELECT product_id, product_name FROM products WHERE status = 'pending'),
    key_column    => 'product_id',
    text_column   => 'product_name',
    target_table  => 'product_ai_results',
    system_prompt => '为该产品写一段50字以内的营销描述',
    temperature   => 0.7,
    enable_thinking => false
) AS batch_id;
-- 返回: 'batch_abc123-xxxx-xxxx-xxxx-xxxxxxxxxxxx'

-- Step 2: 查状态（随时查，可选）
SELECT * FROM TABLE(AI_BATCH_STATUS('batch_abc123-xxxx-xxxx-xxxx-xxxxxxxxxxxx'));
-- | batch_id   | status      | completed | total | failed | elapsed |
-- |------------|-------------|-----------|-------|--------|---------|
-- | batch_abc. | in_progress | 8000      | 50000 | 0      | 35min   |

-- Step 3: 查结果（batch 完成后，结果已自动写入）
SELECT product_id, ai_result, model, total_tokens, processed_at
FROM product_ai_results
LIMIT 5;
```

#### 场景二：Volume 模式（图片/视频/音频）

```sql
SELECT AI_BATCH_SUBMIT(
    model         => 'endpoint:qwen3-vl-plus',
    source        => TABLE(
        SELECT relative_path,
               GET_PRESIGNED_URL(VOLUME my_images, relative_path, 86400) AS media_url
        FROM DIRECTORY(VOLUME my_images)
        WHERE relative_path LIKE '%.jpg'
    ),
    key_column    => 'relative_path',
    media_column  => 'media_url',
    target_table  => 'image_ai_results',
    system_prompt => '你是图片描述专家',
    user_prompt   => '用中文描述这张图片，50字以内'
) AS batch_id;
```

#### 场景三：任务管理

```sql
-- 列出当前 workspace 的所有 batch 任务
SELECT * FROM TABLE(AI_BATCH_LIST())
WHERE status = 'in_progress';

-- 取消任务
SELECT AI_BATCH_CANCEL('batch_abc123-xxxx-xxxx-xxxx-xxxxxxxxxxxx');

-- 手动触发结果收割（batch 已完成但自动写入失败时）
SELECT AI_BATCH_COLLECT('batch_abc123-xxxx-xxxx-xxxx-xxxxxxxxxxxx');
```

#### 场景四：增量处理

```sql
-- 第二次提交同一个 source → target 组合时，自动跳过已处理的 key
SELECT AI_BATCH_SUBMIT(
    model         => 'endpoint:qwen3max',
    source        => TABLE(SELECT product_id, product_name FROM products),
    key_column    => 'product_id',
    text_column   => 'product_name',
    target_table  => 'product_ai_results',   -- 已有 8000 行结果
    system_prompt => '写营销描述',
    incremental   => true                     -- 自动跳过 target 中已有的 key
) AS batch_id;
-- 只提交 target 中不存在的新行
```

#### 场景五：与 Dynamic Table 联动

```sql
-- batch 结果表作为 Dynamic Table 的上游
CREATE DYNAMIC TABLE product_insights
  TARGET_LAG = '1 hour'
AS
SELECT p.*, r.ai_result, r.model, r.total_tokens
FROM products p
JOIN product_ai_results r ON p.product_id = r.product_id;

-- batch 完成 → 结果写入 product_ai_results → Dynamic Table 自动增量刷新
```

### 2.3 函数接口定义

#### AI_BATCH_SUBMIT

```sql
AI_BATCH_SUBMIT(
    -- 必需参数
    model           STRING,              -- 'endpoint:<name>' 或 'connection:<name>'
    source          TABLE,               -- TABLE(SELECT ...) 子查询，包含 key 列和内容列
    key_column      STRING,              -- 主键列名（用于结果回溯和增量过滤）
    target_table    STRING,              -- 结果写入的目标表（不存在则自动创建）

    -- 内容参数（二选一）
    text_column     STRING DEFAULT NULL, -- 文本列名（Table 模式）
    media_column    STRING DEFAULT NULL, -- 预签名 URL 列名（Volume 模式）

    -- Prompt 参数
    system_prompt   STRING DEFAULT 'You are a helpful assistant.',
    user_prompt     STRING DEFAULT NULL, -- Volume 模式的用户指令

    -- 推理参数
    temperature     FLOAT DEFAULT NULL,
    max_tokens      INT DEFAULT NULL,
    top_p           FLOAT DEFAULT NULL,
    enable_thinking BOOLEAN DEFAULT NULL,

    -- 行为参数
    incremental     BOOLEAN DEFAULT true,  -- 是否自动跳过 target 中已有的 key
    result_column   STRING DEFAULT 'ai_result',
    write_mode      STRING DEFAULT 'append'  -- append | overwrite
)
RETURNS STRING  -- batch_id
```

#### AI_BATCH_STATUS

```sql
AI_BATCH_STATUS(batch_id STRING)
RETURNS TABLE(
    batch_id        STRING,
    status          STRING,     -- submitted | validating | in_progress | completed | failed | cancelled
    provider        STRING,
    model           STRING,
    total           INT,
    completed       INT,
    failed          INT,
    target_table    STRING,
    submitted_at    TIMESTAMP,
    completed_at    TIMESTAMP,
    elapsed_seconds INT,
    submitted_by    STRING
)
```

#### AI_BATCH_LIST

```sql
AI_BATCH_LIST()
RETURNS TABLE(
    -- 同 AI_BATCH_STATUS 的列
)
-- 返回当前 workspace 下所有 batch 任务
```

#### AI_BATCH_CANCEL

```sql
AI_BATCH_CANCEL(batch_id STRING)
RETURNS STRING  -- 新状态: 'cancelling' | 'cancelled'
```

#### AI_BATCH_COLLECT

```sql
AI_BATCH_COLLECT(batch_id STRING)
RETURNS TABLE(
    success_count   INT,
    error_count     INT,
    written_rows    INT,
    target_table    STRING
)
-- 手动触发结果收割（正常情况下后台 worker 自动执行）
```

#### AI_BATCH_PLAN

智能分析数据源，推荐 ETL 配置。采样少量数据，调用实时 API 分析内容，返回推荐的模型、prompt 和参数。

```sql
AI_BATCH_PLAN(
    -- 数据源（二选一）
    source_table    STRING DEFAULT NULL,   -- 表名（Table 模式）
    source_volume   STRING DEFAULT NULL,   -- Volume SQL 引用（Volume 模式，如 'USER VOLUME'）

    -- 可选参数
    subdirectory    STRING DEFAULT '',     -- Volume 子目录过滤
    hint            STRING DEFAULT '',     -- 用户意图提示（如 '提取情感倾向' '生成英文营销文案'）
    sample_size     INT DEFAULT 5          -- 采样行数/文件数
)
RETURNS TABLE(
    key_column      STRING,    -- 推荐的主键列（Table 模式）
    text_column     STRING,    -- 推荐的文本列（Table 模式）
    model           STRING,    -- 推荐的模型
    system_prompt   STRING,    -- 推荐的 system prompt
    user_prompt     STRING,    -- 推荐的 user prompt（含 {text} 占位符）
    file_types      STRING,    -- 推荐的文件类型过滤（Volume 模式）
    reasoning       STRING,    -- 推荐理由
    estimated_tokens INT,      -- 预估总 token 数
    estimated_cost  STRING,    -- 预估 batch 成本
    config_snippet  STRING     -- 可直接使用的 config.yaml 片段
)
```

**使用示例：**

```sql
-- Table 模式：AI 自动推荐配置
SELECT * FROM TABLE(AI_BATCH_PLAN(source_table => 'mcp_demo.demo_products'));

-- 带用户意图提示
SELECT * FROM TABLE(AI_BATCH_PLAN(
    source_table => 'mcp_demo.reviews',
    hint => '提取每条评论的情感倾向：正面/负面/中性'
));

-- Volume 模式
SELECT * FROM TABLE(AI_BATCH_PLAN(
    source_volume => 'USER VOLUME',
    subdirectory => 'product_images/',
    hint => '识别产品类别和主要颜色'
));

-- Plan → Submit 一步到位
WITH plan AS (
    SELECT * FROM TABLE(AI_BATCH_PLAN(
        source_table => 'mcp_demo.demo_products',
        hint => '生成英文营销文案'
    ))
)
SELECT AI_BATCH_SUBMIT(
    model         => plan.model,
    source        => TABLE(SELECT product_id, product_name FROM mcp_demo.demo_products),
    key_column    => plan.key_column,
    text_column   => plan.text_column,
    target_table  => 'mcp_demo.products_en_copy',
    system_prompt => plan.system_prompt,
    user_prompt   => plan.user_prompt
) FROM plan;
```

### 2.4 目标表 Schema

`AI_BATCH_SUBMIT` 自动创建目标表，Schema 根据模式自动适配：

**Table 模式：**

| 列 | 类型 | 来源 |
|----|------|------|
| `{key_column}` | 从源表推断 | 源表主键 |
| `{result_column}` | STRING | LLM 推理结果 |
| `model` | STRING | 模型名称 |
| `provider` | STRING | Provider 名称 |
| `prompt_tokens` | INT | 输入 token 数 |
| `completion_tokens` | INT | 输出 token 数 |
| `total_tokens` | INT | 总 token 数 |
| `batch_id` | STRING | Batch 任务 ID |
| `processed_at` | TIMESTAMP | 处理时间 |
| `status_code` | INT | HTTP 状态码 |
| `finish_reason` | STRING | 模型停止原因 |
| `response_id` | STRING | 服务端请求 ID |
| `source_text` | STRING | 原始输入文本 |
| `raw_response` | STRING | 完整响应 JSON |

**Volume 模式额外列：**

| 列 | 类型 | 来源 |
|----|------|------|
| `file_path` | STRING | 文件相对路径 |
| `volume_name` | STRING | Volume 标识 |
| `file_size` | BIGINT | 文件大小 |

### 2.5 系统架构

```
用户 SQL                          引擎                              Provider
───────                          ────                              ────────
                                 ┌─────────────────────┐
AI_BATCH_SUBMIT(...)  ────────>  │ 1. 解析 source 子查询 │
                                 │ 2. 增量过滤           │
                                 │ 3. 构建 JSONL         │
                                 │ 4. 上传文件           │──────>  Upload File
                                 │ 5. 创建 batch         │──────>  Create Batch
                                 │ 6. 写入 ai_batch_jobs │
                                 │ 7. 返回 batch_id      │
                                 └─────────────────────┘
                                           │
                                           ▼
                                 ┌─────────────────────┐
                                 │ 后台 Batch Worker     │
                                 │ (独立进程/线程)       │
                                 │                      │
AI_BATCH_STATUS(...)  ────────>  │ 轮询 Provider API    │──────>  Get Batch Status
                                 │                      │
                                 │ batch completed?     │
                                 │   ├─ yes: 下载结果   │──────>  Download Results
                                 │   │       解析 JSONL  │
                                 │   │       写入目标表   │
                                 │   │       更新状态     │
                                 │   └─ no:  继续轮询    │
                                 └─────────────────────┘
                                           │
                                           ▼
SELECT * FROM target  ────────>  ┌─────────────────────┐
                                 │ 目标表（已有结果）    │
                                 └─────────────────────┘
```

### 2.6 内部状态表

```sql
-- 引擎自动维护，用户可通过 AI_BATCH_LIST() 查询
-- 存储在 information_schema.ai_batch_jobs
CREATE TABLE information_schema.ai_batch_jobs (
    batch_id            STRING PRIMARY KEY,
    provider_batch_id   STRING,          -- Provider 返回的原始 batch_id
    provider            STRING,          -- dashscope | zhipuai
    model               STRING,
    status              STRING,          -- submitted | validating | in_progress | completed | failed | cancelled | collecting | collect_failed
    source_query        STRING,          -- 原始 source 子查询 SQL
    key_column          STRING,
    target_table        STRING,
    target_schema       STRING,
    total_requests      INT,
    completed_requests  INT,
    failed_requests     INT,
    written_rows        INT,
    system_prompt       STRING,
    temperature         FLOAT,
    max_tokens          INT,
    incremental         BOOLEAN,
    submitted_at        TIMESTAMP,
    completed_at        TIMESTAMP,
    collected_at        TIMESTAMP,       -- 结果写入完成时间
    submitted_by        STRING,          -- 提交用户
    workspace           STRING,
    error_message       STRING           -- 失败时的错误信息
);
```

### 2.7 后台 Batch Worker 设计

Worker 是引擎内部的后台服务，负责轮询和收割：

**职责：**
1. 定期扫描 `ai_batch_jobs` 中 status = `in_progress` 或 `validating` 的任务
2. 调用 Provider API 查询状态
3. 任务完成后：下载结果 JSONL → 解析 → 写入目标表 → 更新状态为 `completed`
4. 任务失败后：下载 error 文件 → 记录错误信息 → 更新状态为 `failed`

**轮询策略：**
- 默认间隔：60 秒
- 无任务时：休眠，有新提交时唤醒
- 并发：每个 workspace 独立轮询，避免互相影响

**容错：**
- Worker 重启后从 `ai_batch_jobs` 恢复所有未完成任务
- 写入目标表失败时状态设为 `collect_failed`，用户可通过 `AI_BATCH_COLLECT` 手动重试
- Provider API 调用失败时指数退避重试

**资源：**
- Worker 不占用用户的计算资源（VCluster）
- 使用系统级后台资源，类似 Dynamic Table 的刷新 worker

### 2.8 与现有 AI_COMPLETE 的关系

| 维度 | AI_COMPLETE | AI_BATCH_PLAN | AI_BATCH_SUBMIT |
|------|-------------|---------------|-----------------|
| 用途 | 实时推理 | 分析数据，推荐配置 | 批量推理 |
| 调用模式 | 同步（标量函数） | 同步（秒级返回） | 异步（提交-轮询-收割） |
| 适用规模 | 百级行 | 采样几行 | 万级行 |
| 成本 | 标准价 | 极低（仅采样） | 50% 折扣 |
| 结果返回 | 内联在 SELECT 结果中 | 推荐配置 + config 片段 | 写入独立目标表 |

两者共存，用户根据场景选择：
- 交互式分析、少量数据 → `AI_COMPLETE`
- 批量处理、ETL 流水线 → `AI_BATCH_SUBMIT`

### 2.9 实现计划

基于 [ai_etl 项目](https://github.com/yunqiqiliang/ai_etl) 已验证的代码，实现分为三个阶段：

**Week 1：核心函数**
- [ ] `AI_BATCH_SUBMIT`：解析参数 → 构建 JSONL → 调用 Provider → 写入状态表 → 返回 batch_id
- [ ] `AI_BATCH_STATUS`：查询状态表 + Provider API → 返回进度
- [ ] `AI_BATCH_PLAN`：采样数据 → 调用实时 API 分析 → 返回推荐配置（复用 ai_etl planner.py）
- [ ] `information_schema.ai_batch_jobs` 状态表
- [ ] 复用 ai_etl 的 JSONL 构建逻辑（`build_jsonl_for_batch` / `build_multimodal_jsonl`）
- [ ] 复用 ai_etl 的 Provider 层（`DashScopeProvider` / `ZhipuAIProvider`）

**Week 2：后台 Worker + 结果收割**
- [ ] Batch Worker 服务：轮询 + 下载 + 写入目标表
- [ ] 复用 ai_etl 的结果解析逻辑（`BatchProvider.parse_results`）
- [ ] 复用 ai_etl 的目标表写入逻辑（`write_results` / `write_volume_results`）
- [ ] `AI_BATCH_LIST` / `AI_BATCH_CANCEL` / `AI_BATCH_COLLECT`
- [ ] 增量过滤（查目标表已有 key）

**Week 3：集成测试 + 文档**
- [ ] 端到端测试：Table 模式 + Volume 模式
- [ ] 与 Dynamic Table 联动测试
- [ ] 并发提交测试（多个 batch 同时运行）
- [ ] 容错测试（Worker 重启、Provider 超时、写入失败）
- [ ] SQL 文档 + 用户指南

## 3. 备选方案

### 3.1 方案 B：Python Task 封装（过渡方案）

在方案 A 开发期间，可以先用 Python Task + 本项目代码作为过渡：

```sql
CREATE TASK product_ai_etl
  WAREHOUSE = default_ap
  SCHEDULE = 'USING CRON 0 2 * * *'
AS $$
from ai_etl.pipeline import AIETLPipeline
pipeline = AIETLPipeline()
try:
    pipeline.run()
finally:
    pipeline.close()
$$;
```

优点：零引擎改造，立即可用。缺点：不是纯 SQL 体验。

### 3.2 方案 C：引擎自动路由（远期演进）

方案 A 稳定后，可以进一步让 `AI_COMPLETE` 在大数据量时自动切换为 batch 模式：

```sql
-- 用户写法不变，引擎自动判断
INSERT INTO results
SELECT id, AI_COMPLETE('endpoint:qwen3max', text) FROM big_table;
-- 引擎检测到 50000 行 → 内部自动走 AI_BATCH_SUBMIT → 异步执行 → 结果物化
```

这需要引擎支持 SQL Job 的"挂起-唤醒"状态，是方案 A 的自然演进。

## 4. 设计决策记录

| 决策 | 选择 | 理由 |
|------|------|------|
| 首选方案 | A（SQL 函数套件） | 纯 SQL 体验，2-3 周可交付 |
| 函数数量 | 7 个 | PLAN + SUBMIT + STATUS + LIST + CANCEL + COLLECT + 状态表 |
| 异步模型 | 提交-轮询-收割 | 24 小时窗口下唯一可行的模型 |
| 智能配置 | AI_BATCH_PLAN | 采样分析 + 用户 hint → 推荐 model/prompt/参数 |
| 结果存储 | 写入用户指定的目标表 | 可被下游 SQL/BI/Dynamic Table 直接消费 |
| 增量处理 | 默认开启 | 避免重复处理，节省成本 |
| 元数据 | 12 个字段自动记录 | 审计、成本分析、问题排查 |
| Worker 资源 | 系统级后台资源 | 不占用用户 VCluster |
| Provider 抽象 | 复用 ai_etl Provider 层 | DashScope + ZhipuAI 已验证 |
