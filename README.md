# AI ETL

**Lakehouse 读取 → LLM 批量推理 → 结果写回**

从 ClickZetta Lakehouse 源表读取文本数据，通过 LLM Batch API 进行批量推理，将结果按主键关联写回目标表，并附带模型、token 消耗、处理时间等元数据。

支持多 Provider：阿里云百炼（DashScope）、智谱 AI（ZhipuAI），通过配置文件一键切换。

## 为什么用批量推理而不是实时推理？

对数据仓库中的大量文本做 AI 处理（分类、摘要、翻译、特征提取等）时，有三种方式可选：

| 对比维度 | SQL AI Function | 实时推理 API | 批量推理 API (本项目) |
|---------|----------------|-------------|---------------------|
| **调用方式** | `SELECT ai_func(col) FROM table` | 逐条调用 `/chat/completions` | 提交 JSONL 文件，异步处理 |
| **成本** | 标准价格 | 标准价格 | **50% 折扣** |
| **适用规模** | 几十到几百条 | 几十到几百条 | **几千到几万条** (单文件最多 50,000) |
| **并发控制** | 受 SQL 引擎并发限制，大表容易超时 | 需自行管理并发和限流 (QPM/TPM) | 服务端自动调度，无需管理 |
| **容错** | 单行失败导致整条 SQL 报错 | 单条失败需自行重试 | 服务端自动重试，失败明细单独导出 |
| **编码复杂度** | 最简单（一条 SQL） | 需写异步/多线程代码 | 提交文件后等结果，代码简单 |
| **延迟** | 秒级（但大表可能分钟级） | 秒级 | 分钟到小时级（异步） |
| **元数据** | 无（只有结果值） | 需自行记录 | **自动记录** model/tokens/时间等 |
| **断点续跑** | 不支持，失败需全量重跑 | 需自行实现 | **内置支持**，中断后可恢复 |
| **Provider 切换** | 绑定数据库内置函数 | 需改代码 | **配置文件一键切换** |

**SQL AI Function 的局限**：在 SQL 中直接调用 `AI_COMPLETE()`、`AI_SENTIMENT()` 等函数虽然写法最简单，但当数据量超过几百条时，SQL 执行时间会很长，容易超时；单行调用失败会导致整条 SQL 回滚；无法记录 token 消耗等元数据；且只能使用数据库内置支持的模型，无法灵活切换 Provider。

**实时推理 API 的局限**：自行编写并发调用代码，需要处理限流、重试、进度跟踪、结果持久化等工程问题，代码复杂度高。

**批量推理 API 的优势**：成本减半，服务端自动调度并发和重试，代码极简（提交文件 → 等结果），天然支持大规模数据处理。本项目在此基础上进一步封装了 Lakehouse 读写、主键关联、元数据记录、断点续跑、多 Provider 切换等能力。

**适用场景**：商品描述生成、评论情感分析、文档摘要提取、数据标注、多语言翻译、知识抽取等——凡是"对一批数据做同样的 AI 处理，不要求实时返回"的场景。

## 架构

```
+--------------------------------------------------------------------------+
|                          AI ETL Pipeline                                 |
|                                                                          |
|  +-------------------+                          +---------------------+  |
|  | ClickZetta        |  Step 1: Read            | ClickZetta          |  |
|  | Lakehouse         |  SELECT key + text       | Lakehouse           |  |
|  |                   |  WHERE filter            |                     |  |
|  | Source Table      |----------+               | Target Table        |  |
|  | - primary key     |          |               | - primary key       |  |
|  | - text column     |          |               | - result column     |  |
|  | - filter          |          |               | - 12 metadata cols  |  |
|  +-------------------+          |               +---------------------+  |
|                                 v                        ^               |
|                        +------------------+              |               |
|                        | Step 2: Build    |              |               |
|                        | JSONL file       |              |               |
|                        | - encode keys    |              |               |
|                        | - skip empty     |              |               |
|                        | - validate size  |              |               |
|                        +--------+---------+              |               |
|                                 v                        |               |
|                   +--------------------------+           |               |
|                   | Step 3: LLM Batch API    |           |               |
|                   |                          |           |               |
|                   | +----------+ +----------+|           |               |
|                   | | DashScope| |  ZhipuAI ||           |               |
|                   | |  (Qwen)  | |   (GLM)  ||           |               |
|                   | +----------+ +----------+|           |               |
|                   |                          |           |               |
|                   | upload -> create ->      |           |               |
|                   | poll   -> download       |           |               |
|                   +------------+-------------+           |               |
|                                v                         |               |
|                        +------------------+              |               |
|                        | Step 4: Write    |              |               |
|                        | - match by key   +--------------+               |
|                        | - cast types     |                              |
|                        | - add metadata   |                              |
|                        +------------------+                              |
|                                                                          |
|  Config:  .env (credentials)  +  config.yaml (parameters)                |
+--------------------------------------------------------------------------+
```

**数据流**：
1. 从源表 SELECT 主键列 + 文本列（支持 WHERE 过滤）
2. 构建 JSONL 请求文件，主键值自动编码，确保推理结果与源数据一一对应
3. 上传文件 → 创建 Batch 任务 → 轮询等待完成 → 下载结果
4. 解析结果，按主键关联写入目标表（含 12 个元数据列）

## 快速开始

```bash
# 1. 安装
cd ai_etl
pip install -e .

# 2. 配置凭证
cp .env.example .env
# 编辑 .env，填入 API Key 和 Lakehouse 用户名密码

# 3. 配置业务参数
cp config.yaml.example config.yaml
# 编辑 config.yaml，配置 provider、模型、源表、目标表

# 4. 运行
python -m ai_etl run
```

## 安装

```bash
pip install -e .
```

核心依赖会自动安装：`openai`、`clickzetta-zettapark-python`。

如需使用智谱 Provider，额外安装：

```bash
pip install zhipuai
```

## 配置

项目使用两个配置文件，均已在 `.gitignore` 中，不会提交到 Git：

| 文件 | 内容 | 模板 |
|------|------|------|
| `.env` | API Key、数据库密码等敏感凭证 | `.env.example` |
| `config.yaml` | Provider、模型、源表、目标表等业务参数 | `config.yaml.example` |

### 凭证 — `.env`

```env
# LLM API Key（按使用的 provider 配置）
DASHSCOPE_API_KEY=sk-your-dashscope-key
ZHIPUAI_API_KEY=your-zhipuai-key

# ClickZetta Lakehouse 凭证
CLICKZETTA_USERNAME=your_username
CLICKZETTA_PASSWORD=your_password
```

### 业务参数 — `config.yaml`

```yaml
# 选择 Provider
provider: dashscope              # dashscope | zhipuai

# Provider 各自的模型和 prompt
dashscope:
  model: qwen3.5-flash
  system_prompt: "你是一个数据分析助手。"

zhipuai:
  model: glm-4-flash
  system_prompt: "你是一个数据分析助手。"

# Lakehouse 连接（用户名密码在 .env 中）
clickzetta:
  service: cn-shanghai-alicloud.api.clickzetta.com
  instance: your_instance
  workspace: your_workspace
  schema: public
  vcluster: default_ap

# ETL 源表和目标表
etl:
  source:
    table: "schema.source_table"
    key_columns: "id"            # 支持复合主键: "id,sub_id"
    text_column: "content"
    filter: "status = 'pending'" # 留空表示不过滤
    batch_size: 0                # 0 = 不限制
  target:
    table: "schema.target_table"
    result_column: "ai_result"
    write_mode: "append"         # append | overwrite
```

完整配置项说明见 [config.yaml.example](config.yaml.example)。

### 配置优先级

代码参数 > 环境变量(.env) > config.yaml > 内置默认值

## 使用方式

### 命令行

```bash
# 运行完整流水线（配置驱动）
python -m ai_etl run

# 指定 provider 和模型（覆盖 config.yaml）
python -m ai_etl run --provider zhipuai --model glm-4-plus

# 查询任务状态
python -m ai_etl status <batch_id>

# 恢复中断的任务（跳过读取和提交，直接下载结果并写入）
python -m ai_etl resume <batch_id>
```

### Python API

**完整流水线**：

```python
from ai_etl import AIETLPipeline

pipeline = AIETLPipeline()
stats = pipeline.run()
print(f"处理 {stats['source_rows']} 行, 写入 {stats['written_rows']} 行")
pipeline.close()
```

**参数覆盖**：

```python
from ai_etl import AIETLPipeline

pipeline = AIETLPipeline()
stats = pipeline.run(
    provider_name="zhipuai",
    model="glm-4-plus",
    source_table="my_schema.reviews",
    key_columns="review_id",
    text_column="review_text",
    filter_expr="lang = 'zh' AND processed = false",
    target_table="my_schema.review_sentiment",
    result_column="sentiment",
    system_prompt="分析情感倾向，只回答：正面、负面、中性",
)
pipeline.close()
```

**分步控制**：

```python
from ai_etl import Config, LakehouseClient, create_provider, BatchProvider
import os

cfg = Config()
lakehouse = LakehouseClient(config=cfg)
provider = create_provider(
    "dashscope",
    api_key=os.getenv("DASHSCOPE_API_KEY"),
    model="qwen3.5-flash",
)

# 1. 读取源数据
rows = lakehouse.read_source()

# 2. 构建 JSONL
jsonl = lakehouse.build_jsonl_for_batch(rows, endpoint=provider.build_jsonl_endpoint())

# 3. 提交推理
file_id = provider.upload_file(jsonl)
batch_id = provider.create_batch(file_id)
provider.wait_for_completion(batch_id)

# 4. 下载结果并写入目标表
result_text = provider.download_results(batch_id)
results = BatchProvider.parse_results(result_text)
lakehouse.write_results(
    results,
    provider_name="dashscope",
    batch_id=batch_id,
    source_rows=rows,       # 传入源数据，用于回填 source_text 元数据
)

lakehouse.close()
```

**恢复中断的任务**：

```python
from ai_etl import AIETLPipeline

# pipeline 中断时，batch_id 已保存在 output/last_batch.json
pipeline = AIETLPipeline()
stats = pipeline.resume("batch_xxx-xxx-xxx")
pipeline.close()
```

## 目标表元数据

写入目标表时，除了主键列和推理结果列，还会自动添加以下元数据列。目标表不存在时自动建表，已存在但缺列时自动 `ALTER TABLE ADD COLUMN`。

| 列名 | 类型 | 说明 |
|------|------|------|
| `model` | STRING | 使用的模型名称 |
| `provider` | STRING | Provider 名称 (dashscope / zhipuai) |
| `prompt_tokens` | INT | 输入 token 数 |
| `completion_tokens` | INT | 输出 token 数 |
| `total_tokens` | INT | 总 token 数 |
| `batch_id` | STRING | 批量任务 ID，用于溯源和排查 |
| `processed_at` | STRING | 处理时间 (UTC ISO 格式) |
| `status_code` | INT | 单条请求 HTTP 状态码 (200 = 成功) |
| `finish_reason` | STRING | 模型停止原因 (stop / length / content_filter) |
| `response_id` | STRING | 服务端请求级唯一 ID |
| `source_text` | STRING | 原始输入文本，方便对照检查结果质量 |
| `raw_response` | STRING | 完整 response body JSON，兜底保留所有信息 |

## 支持的 Provider

### DashScope（阿里云百炼）

- **Batch 模型**：qwen3-max, qwen3.5-plus, qwen3.5-flash, deepseek-r1, deepseek-v3 等
- **文件限制**：≤ 50,000 请求，≤ 500 MB
- **计费**：实时推理价格的 50%
- **文档**：https://help.aliyun.com/zh/model-studio/batch-inference

### ZhipuAI（智谱）

- **Batch 模型**：glm-4-flash, glm-4-plus, glm-4-air-250414 等
- **文件限制**：≤ 50,000 请求，≤ 100 MB
- **计费**：实时推理价格的 50%
- **文档**：https://docs.bigmodel.cn/cn/guide/tools/batch

## 项目结构

```
ai_etl/
├── .env.example              # 凭证模板
├── config.yaml.example       # 配置模板
├── pyproject.toml             # 项目依赖
├── ai_etl/
│   ├── __init__.py            # 公开 API
│   ├── __main__.py            # 命令行入口 (python -m ai_etl)
│   ├── config.py              # 配置加载 (.env + config.yaml)
│   ├── result_keys.py         # 统一字段常量 + 主键编解码
│   ├── lakehouse.py           # ClickZetta Lakehouse 读写
│   ├── pipeline.py            # 端到端 ETL 流水线 + 断点续跑
│   └── providers/
│       ├── base.py            # Provider 抽象基类 + 重试 + 结果解析
│       ├── registry.py        # Provider 注册表
│       ├── dashscope_provider.py   # 阿里云百炼
│       └── zhipuai_provider.py     # 智谱 AI
└── examples/
    ├── batch_demo.py          # 批量推理示例
    └── etl_demo.py            # 完整 ETL 示例
```

## 健壮性设计

| 场景 | 处理方式 |
|------|---------|
| 目标表不存在 | 自动建表，主键类型从源表推断 |
| 目标表缺少元数据列 | 自动 ALTER TABLE ADD COLUMN |
| 主键类型不匹配 (string → bigint) | 自动读取目标表 schema 做类型转换 |
| 源表文本字段为空 | 自动跳过，记录警告 |
| 主键值重复 | 自动去重，记录警告 |
| 单行请求 > 6 MB | 自动跳过，记录警告 |
| 总请求 > 50,000 行或文件 > 100 MB | 校验报错，提示通过 `batch_size` 分批 |
| 同一批次使用多个模型 | 校验报错，阻止提交 |
| API 限流 (429) 或服务端错误 (5xx) | 指数退避重试（最多 5 次） |
| 轮询中网络中断 | 捕获异常继续重试 |
| 任务过期 (expired) | 提示增大 completion_window |
| Pipeline 中断 | batch_id 持久化到 output/last_batch.json，支持 `resume` 恢复 |
| 不同 Provider 的协议差异 | 内部自动适配 |
