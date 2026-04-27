# 设计提案：将 LLM Batch 推理封装为云器 Lakehouse AI Function in SQL

## 背景

### 现状

云器 Lakehouse 已提供 `AI_COMPLETE` 和 `AI_EMBEDDING` 两个 SQL 标量函数，底层调用实时推理 API（同步请求-响应）。适用于交互式分析和小批量场景（百级行数）。

```sql
-- 现有能力：实时推理，同步返回
SELECT AI_COMPLETE('endpoint:qwen3max', '中国的首都在哪里?');
```

### 问题

当数据量达到万级以上时，实时 API 面临三个根本性限制：

| 限制 | 影响 |
|------|------|
| **成本** | 标准价格，无折扣 |
| **并发** | QPM/TPM 限制，大表扫描会触发限流 |
| **可靠性** | 单行失败不影响其他行，但引擎需要自行管理重试 |

LLM Provider（DashScope、ZhipuAI 等）提供的 **Batch API** 可以解决这些问题：50% 成本、服务端自动调度、内置重试。但 Batch API 是异步的 — 提交后需要等待 **数十分钟到 24 小时** 才能获取结果。

### 核心矛盾

**SQL 是同步的，Batch API 是异步的。**

SQL 函数必须在一次调用内返回结果。但 Batch 任务的生命周期跨越小时甚至天（Provider 承诺的 completion_window 通常是 24 小时）。这两个模型无法在一个函数调用里统一。

### 已验证的基础

本项目（[ai_etl](https://github.com/yunqiqiliang/ai_etl)）已实现完整的 Batch 推理 ETL 流水线，并通过端到端验证：

- 双数据源（Table + Volume）并行提交
- 统一轮询等待，先完成先写入
- 增量处理（Table 跳过已处理 key，Volume 跳过已处理文件）
- 结果 + 12 个元数据字段自动写回 Lakehouse
- 中断恢复（resume）

## 设计约束

1. **Batch 任务耗时不可控** — Provider 承诺 24 小时内完成，实测 10-60 分钟，极端情况可能更长
2. **SQL 连接不能阻塞 24 小时** — 连接超时、计算资源占用、用户体验均不可接受
3. **结果必须写回 Lakehouse 表** — 不能只返回文件，要能被下游 SQL/BI/Dynamic Table 消费
4. **需要支持多模态** — 不仅是文本，还有 Volume 中的图片/视频/音频
5. **需要增量处理** — 重跑时不重复处理已有结果

## 方案对比

### 方案 A：SQL 函数三件套

在 SQL 层新增三个函数，将 Batch 生命周期暴露给用户：

```sql
-- ① 提交（秒级返回 batch_id）
SELECT AI_BATCH_SUBMIT(
    'endpoint:qwen3max',                          -- 模型
    TABLE(SELECT id, text FROM products),          -- 源数据（表子查询）
    key_column    => 'id',
    text_column   => 'text',
    target_table  => 'product_results',
    system_prompt => '写一段50字营销描述',
    temperature   => 0.7
) AS batch_id;
-- 返回: 'batch_abc123-xxxx-xxxx'

-- ② 查状态（随时查，秒级返回）
SELECT * FROM TABLE(AI_BATCH_STATUS('batch_abc123-xxxx-xxxx'));
-- 返回:
-- | batch_id | status      | completed | total | failed | elapsed |
-- |----------|-------------|-----------|-------|--------|---------|
-- | batch_.. | in_progress | 8000      | 50000 | 12     | 35min   |

-- ③ 查结果（完成后自动写入 target_table，直接查）
SELECT id, ai_result, model, total_tokens
FROM product_results
LIMIT 10;
```

**Volume 多模态场景：**

```sql
SELECT AI_BATCH_SUBMIT(
    'endpoint:qwen3-vl-plus',
    TABLE(
        SELECT relative_path,
               GET_PRESIGNED_URL(VOLUME my_images, relative_path, 86400) AS url
        FROM DIRECTORY(VOLUME my_images)
        WHERE relative_path LIKE '%.jpg'
    ),
    key_column    => 'relative_path',
    media_column  => 'url',                        -- 预签名 URL 列
    target_table  => 'image_results',
    user_prompt   => '描述这张图片',
    system_prompt => '你是图片描述专家'
);
```

**任务管理：**

```sql
-- 列出所有 batch 任务
SELECT * FROM TABLE(AI_BATCH_LIST());

-- 取消任务
SELECT AI_BATCH_CANCEL('batch_abc123-xxxx-xxxx');

-- 恢复写入（batch 已完成但写入中断时）
SELECT AI_BATCH_COLLECT('batch_abc123-xxxx-xxxx', 'product_results');
```

**优点：**
- 纯 SQL 体验，不需要 Python
- 用户显式控制生命周期，心智模型清晰
- 可与现有 `AI_COMPLETE` 共存（小数据用 `AI_COMPLETE`，大数据用 `AI_BATCH_SUBMIT`）

**缺点：**
- 需要引擎支持新的函数类型（表函数 + 异步任务注册）
- 需要引擎维护 batch 任务状态表（类似 Job History）
- 用户需要学习新的异步模式

**引擎改造点：**
- 新增 `AI_BATCH_SUBMIT` 表函数：解析 TABLE() 子查询 → 构建 JSONL → 调用 Provider Batch API → 注册异步任务 → 返回 batch_id
- 新增 `AI_BATCH_STATUS` 表函数：查询 Provider API → 返回状态
- 新增内部任务表 `information_schema.ai_batch_jobs`：记录所有 batch 任务的状态、进度、目标表
- 新增后台 worker：轮询未完成的 batch 任务，完成后自动下载结果并写入目标表

### 方案 B：Python Task 封装（零引擎改造）

利用现有的 Python Task + 本项目代码，将 Batch ETL 封装为可调度的任务：

```sql
-- ① 一次性配置：创建 Python Task
CREATE TASK product_ai_etl
  WAREHOUSE = default_ap
  SCHEDULE = 'USING CRON 0 2 * * *'              -- 每天凌晨2点
  COMMENT = 'AI ETL: products → batch inference → product_results'
AS
$$
from ai_etl.pipeline import AIETLPipeline
pipeline = AIETLPipeline()
try:
    stats = pipeline.run()
    print(f"完成: {stats['success_count']} 成功, {stats['written_rows']} 写入")
finally:
    pipeline.close()
$$;

-- ② 手动触发
EXECUTE TASK product_ai_etl;

-- ③ 查看任务执行历史
SELECT * FROM TABLE(TASK_HISTORY())
WHERE name = 'PRODUCT_AI_ETL'
ORDER BY scheduled_time DESC;

-- ④ 查结果
SELECT * FROM product_results LIMIT 10;

-- ⑤ 下游 Dynamic Table 自动增量刷新
CREATE DYNAMIC TABLE product_insights
  TARGET_LAG = '1 hour'
AS
SELECT p.*, r.ai_result, r.model, r.total_tokens
FROM products p
JOIN product_results r ON p.product_id = r.product_id;
```

**优点：**
- **零引擎改造** — 完全基于现有产品能力（Python Task + Dynamic Table）
- **本项目代码直接复用** — pipeline.run() 已经处理了提交、轮询、写入、增量、恢复
- **可调度** — 支持 CRON 定时、手动触发、依赖编排
- **可观测** — Task History 记录每次执行的状态和耗时
- **增量天然支持** — 本项目已实现 Table/Volume 两种增量过滤

**缺点：**
- 不是纯 SQL 体验，Task 内部是 Python 代码
- 用户需要理解 Task + Dynamic Table 的编排模式
- config.yaml 需要部署到 Task 运行环境中

**落地路径：**
1. 将本项目打包为 pip 包，上传到 Lakehouse Python 环境
2. 创建 Python Task，内部调用 `AIETLPipeline().run()`
3. 配置文件通过 Task 参数或 Volume 文件传入
4. 下游用 Dynamic Table 做增量聚合

### 方案 C：Hybrid — 引擎自动路由（长期愿景）

扩展现有 `AI_COMPLETE`，引擎根据数据量自动选择实时或 batch：

```sql
-- 用户写法完全一样
INSERT INTO product_results
SELECT product_id, AI_COMPLETE('endpoint:qwen3max', product_name)
FROM products;

-- 引擎内部判断：
--   行数 < 阈值 → 走实时 API（同步返回）
--   行数 > 阈值 → 自动切换为 batch 模式：
--     1. 将所有行打包为 JSONL
--     2. 提交 Batch API
--     3. 将 SQL Job 状态设为 "等待外部任务"
--     4. 后台 worker 轮询 batch 状态
--     5. 完成后唤醒 SQL Job，写入结果
--     6. SQL Job 返回成功
```

**优点：**
- 用户零感知，SQL 写法不变
- 引擎自动选择最优路径

**缺点：**
- **24 小时级别的 SQL Job** — 引擎执行模型需要根本性改造
  - SQL Job 需要支持"挂起-唤醒"状态（类似操作系统的进程调度）
  - 计算资源不能被长时间占用
  - 连接管理、超时策略、失败恢复都需要重新设计
- 用户无法预知一条 SQL 会执行几秒还是 24 小时
- 实现周期最长

## 建议路径

```
现在                    3-6 个月                  12+ 个月
 │                        │                         │
 ▼                        ▼                         ▼
方案 B                  方案 A                    方案 C
Python Task 封装        SQL 函数三件套             引擎自动路由
(零改造，立即可用)      (引擎新增函数)            (引擎架构升级)
 │                        │                         │
 │  本项目代码直接复用     │  AI_BATCH_SUBMIT()      │  AI_COMPLETE 自动切换
 │  Task + Dynamic Table  │  AI_BATCH_STATUS()      │  SQL Job 挂起-唤醒
 │  CRON 调度             │  后台 worker 自动收割    │  对用户完全透明
```

**短期（现在）**：方案 B — 把本项目封装为 Python Task。零引擎改造，代码已验证，可以立即给客户用。

**中期（3-6 个月）**：方案 A — 引擎新增 `AI_BATCH_SUBMIT` / `AI_BATCH_STATUS` 函数。给用户纯 SQL 体验，同时保留方案 B 作为高级用法。

**长期（12+ 个月）**：方案 C — 引擎级自动路由。这是终极形态，但需要引擎团队深度参与执行模型改造。

## 附录：方案 A 的引擎接口设计草案

### AI_BATCH_SUBMIT

```
AI_BATCH_SUBMIT(
    model           STRING,          -- 'endpoint:xxx' 或 'connection:xxx'
    source_query    TABLE,           -- TABLE(SELECT ...) 子查询
    key_column      STRING,          -- 主键列名
    text_column     STRING,          -- 文本列名（Table 模式）
    media_column    STRING DEFAULT NULL,  -- 预签名 URL 列名（Volume 模式）
    target_table    STRING,          -- 结果写入的目标表
    system_prompt   STRING DEFAULT 'You are a helpful assistant.',
    user_prompt     STRING DEFAULT NULL,  -- Volume 模式的用户指令
    temperature     FLOAT DEFAULT NULL,
    max_tokens      INT DEFAULT NULL,
    enable_thinking BOOLEAN DEFAULT NULL
) RETURNS STRING                     -- 返回 batch_id
```

### AI_BATCH_STATUS

```
AI_BATCH_STATUS(
    batch_id STRING
) RETURNS TABLE(
    batch_id    STRING,
    status      STRING,    -- submitted / in_progress / completed / failed / cancelled
    total       INT,
    completed   INT,
    failed      INT,
    target_table STRING,
    submitted_at STRING,
    completed_at STRING,
    elapsed     STRING
)
```

### 内部任务表

```sql
-- 自动创建在 information_schema 中
CREATE TABLE information_schema.ai_batch_jobs (
    batch_id        STRING PRIMARY KEY,
    provider        STRING,
    model           STRING,
    status          STRING,
    target_table    STRING,
    source_query    STRING,
    total_requests  INT,
    completed       INT,
    failed          INT,
    submitted_at    TIMESTAMP,
    completed_at    TIMESTAMP,
    submitted_by    STRING,
    workspace       STRING
);
```
