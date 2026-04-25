# Multimodal AI ETL

**Lakehouse → LLM Batch Inference → Lakehouse**

From ClickZetta Lakehouse, read structured text (tables) or unstructured files (images, video, audio from Volumes), run LLM batch inference at 50% cost, and write results back with full metadata.

Supports multiple providers (DashScope / ZhipuAI) and multiple modalities (text, image, video, audio), switchable via config.

## Why Batch Inference?

LLM providers like DashScope and ZhipuAI offer two ways to call their models:

- **Realtime API** — Synchronous, one request at a time. Good for interactive use (chatbots, copilots), but expensive at scale: you pay full price, manage your own concurrency (QPM/TPM limits), and handle retries yourself.

- **Batch API** — Asynchronous, bulk processing. You upload a JSONL file with thousands of requests, the provider processes them server-side, and you download the results when done. **50% cost savings**, automatic scheduling, built-in retry, and error files for failed requests. The tradeoff is latency: results take minutes to hours.

For large-scale AI Transform (tens of thousands of rows or files), **Batch mode is not just cheaper — it is fundamentally more reliable and complete**:

- **Reliability**: The provider handles scheduling, rate limiting, and transient failures internally. With Realtime API, a network hiccup at row 8,000 of 50,000 means you lose progress and must build your own checkpoint/retry logic. Batch API guarantees every request is attempted, and failed ones are reported in a separate error file — nothing is silently lost.
- **Completeness**: Batch results include a per-request status code, so you know exactly which rows succeeded and which failed. You get a complete audit trail (model, tokens, finish_reason) for every single request.
- **Resumability**: If your client process dies mid-wait, the batch job continues running server-side. You can resume later with just the batch_id — no data is lost, no work is repeated.

| Dimension | SQL AI Function | Realtime API | Batch API (this project) |
|-----------|----------------|-------------|--------------------------|
| **Cost** | Standard | Standard | **50% off** |
| **Scale** | Hundreds | Hundreds | **Tens of thousands** (50K/file) |
| **Concurrency** | SQL engine limited | Self-managed QPM/TPM | Server-side auto-scheduling |
| **Fault tolerance** | Single row fails entire SQL | Self-managed retry | Server-side retry, error file |
| **Modality** | Text only (built-in functions) | Text + multimodal | **Text + image + video + audio** |
| **Metadata** | None | Self-managed | **Auto-recorded** (model/tokens/time) |
| **Incremental** | Not supported | Self-managed | **Built-in** (skip processed files) |
| **Provider switch** | Locked to DB functions | Code changes | **Config file switch** |

## Architecture

```
+=============================================================================+
|                      Multimodal AI ETL Pipeline                             |
|                                                                             |
|  EXTRACT                                                                    |
|  ~~~~~~~                                                                    |
|  +----------------------------------+ +----------------------------------+  |
|  | Source A: Table                   | | Source B: Volume                |  |
|  |                                   | |                                 |  |
|  | SELECT key, text                  | | DIRECTORY(VOLUME)               |  |
|  |   FROM source_table               | |   -> file discovery             |  |
|  |   WHERE filter                    | |   -> extension filter           |  |
|  |   LIMIT batch_size                | |   -> incremental filter         |  |
|  +----------------+------------------+ |      (skip processed)           |  |
|                   |                    | GET_PRESIGNED_URL()             |  |
|                   |                    |   -> HTTP-accessible URLs       |  |
|                   |                    +----------------+-----------------+ |
|                   |                                     |                   |
|  TRANSFORM        |                                     |                   |
|  ~~~~~~~~~        v                                     v                   |
|  +----------------------------------+ +----------------------------------+  |
|  | Build Text JSONL                  | | Build Multimodal JSONL          |  |
|  |                                   | |                                 |  |
|  | custom_id: encode(key)            | | custom_id: encode(path)         |  |
|  | messages:                         | | messages:                       |  |
|  |   system: prompt                  | |   system: prompt                |  |
|  |   user:   text_value              | |   user:   [image_url /          |  |
|  |                                   | |     video_url / audio, text     |  |
|  +----------------+------------------+ +----------------+-----------------+ |
|                   |                                     |                   |
|                   +------------------+------------------+                   |
|                                      |                                      |
|                                      v                                      |
|               +----------------------------------------------+              |
|               | Upload JSONL + Create Batch                   |             |
|               | (both sources submitted in parallel)          |             |
|               +----------------------+-----------------------+              |
|                                      |                                      |
|                                      v                                      |
|               +----------------------------------------------+              |
|               | LLM Batch Inference (server-side)             |             |
|               |                                               |             |
|               | Provider:  DashScope / ZhipuAI                |             |
|               | Model:     qwen / glm / deepseek              |             |
|               | Cost:      50% of realtime                    |             |
|               | Scale:     up to 50K requests per file        |             |
|               +----------------------+-----------------------+              |
|                                      |                                      |
|               +----------------------------------------------+              |
|               | Unified Polling Loop                          |             |
|               | (parallel wait for all batch jobs)            |             |
|               +----------------------+-----------------------+              |
|                                      |                                      |
|  LOAD                                v                                      |
|  ~~~~             +------------------+------------------+                   |
|                   |                                     |                   |
|                   v                                     v                   |
|  +----------------------------------+ +----------------------------------+  |
|  | Table Target                      | | Volume Target                   |  |
|  |                                   | |                                 |  |
|  | key_columns  (from source)        | | file_path, volume_name          |  |
|  | ai_result                         | | file_size, ai_result            |  |
|  | + 12 metadata columns:            | | + 12 metadata columns           |  |
|  |   model, provider, tokens         | |   model, provider, tokens       |  |
|  |   batch_id, processed_at          | |   batch_id, processed_at        |  |
|  |   status, finish_reason           | |   status, finish_reason         |  |
|  |   source_text, raw_response       | |   source_text, raw_response     |  |
|  +----------------------------------+ +----------------------------------+  |
|                                                                             |
|  CONFIG                                                                     |
|  ~~~~~~                                                                     |
|  .env          API keys, Lakehouse password (secrets)                       |
|  config.yaml   Provider, sources, targets, prompts (parameters)             |
+=============================================================================+
```

### ClickZetta Lakehouse Features Used

- **[ZettaPark Python SDK](https://www.yunqi.tech/documents/ZettaparkQuickStart)** — DataFrame API for reading source tables and writing result tables via `session.sql()` and `create_dataframe().write.save_as_table()`
- **[Volume](https://www.yunqi.tech/documents/datalake_volume)** — Managed object storage for unstructured files (images, video, audio), supports External / User / Table Volume types
- **[DIRECTORY()](https://www.yunqi.tech/documents/unstructure_data_analysis)** — SQL function to list files in a Volume with relative_path and size metadata
- **[GET_PRESIGNED_URL()](https://www.yunqi.tech/documents/GET_PRESIGNED_URL)** — Generate time-limited HTTP-accessible URLs for Volume files, enabling LLM APIs to fetch media content directly
- **[Auto DDL](https://www.yunqi.tech/documents/create-table-ddl)** — Target tables are auto-created with `CREATE TABLE IF NOT EXISTS`, columns auto-added with `ALTER TABLE ADD COLUMN`
- **[information_schema](https://www.yunqi.tech/documents/worksapce-informaiton_schema-views)** — Table schema introspection for type-aware column mapping between source and target tables

## Prerequisites

- **Python 3.10+** (zettapark requires 3.10 or later)
- **ClickZetta Lakehouse account** — [sign up](https://www.yunqi.tech/documents/LoggingIn) and get service/instance/workspace info
- **LLM API Key** — at least one of:
  - [DashScope](https://help.aliyun.com/zh/model-studio/get-api-key) (recommended, supports all modalities)
  - [ZhipuAI](https://open.bigmodel.cn/) (install extra: `pip install ai-etl[zhipuai]`)
- **Source data ready in Lakehouse** — depending on which mode you use:
  - **Table mode**: a table containing at least two columns:
    - A **key column** (unique identifier per row, e.g. `id`, `product_id`). Supports composite keys via comma-separated names
    - A **text column** (the content to send to the LLM, e.g. `review_text`, `description`). Rows with empty text are skipped
  - **Volume mode**: files uploaded to a Lakehouse [Volume](https://www.yunqi.tech/documents/datalake_volume) (via `PUT` command, Zettapark SDK, or Lakehouse Studio). Supported file formats:

    | Type | Supported Extensions |
    |------|---------------------|
    | Image | `.jpg` `.jpeg` `.png` `.gif` `.bmp` `.webp` `.tiff` `.tif` |
    | Video | `.mp4` `.avi` `.mov` `.mkv` `.webm` |
    | Audio | `.mp3` `.wav` `.flac` `.ogg` `.m4a` (DashScope only) |

    Files with other extensions are ignored. Volume types supported: external Volume, User Volume, Table Volume
- **Network access** — the machine running this pipeline needs outbound HTTPS access to both the Lakehouse API and the LLM provider API

## Quick Start

### Step 1: Install

```bash
git clone https://github.com/yunqiqiliang/ai_etl.git
cd ai_etl
pip install -e .
# For ZhipuAI provider: pip install -e ".[zhipuai]"
```

### Step 2: Configure credentials (.env)

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials:

```dotenv
# At least one LLM provider API key is required
DASHSCOPE_API_KEY=sk-xxxxxxxxxxxxxxxxxxxx
ZHIPUAI_API_KEY=xxxxxxxxxxxxxxxxxxxx          # optional

# ClickZetta Lakehouse credentials
CLICKZETTA_USERNAME=your_username
CLICKZETTA_PASSWORD=your_password
```

### Step 3: Configure ETL parameters (config.yaml)

```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml`. Here is a minimal working example for **table mode** (structured text):

```yaml
provider: dashscope

dashscope:
  model: qwen3.5-flash
  endpoint: /v1/chat/completions
  completion_window: "24h"
  poll_interval: 30.0                  # seconds between status checks (use 300 for production)

clickzetta:
  service: cn-shanghai-alicloud.api.clickzetta.com
  instance: your_instance_id           # from Lakehouse console
  workspace: your_workspace            # from Lakehouse console
  schema: your_schema
  vcluster: default_ap

etl:
  sources:
    table:
      enabled: true
      table: "your_schema.your_table"  # source table with text data
      key_columns: "id"                # primary key column(s), comma-separated
      text_column: "content"           # column containing text to analyze
      filter: ""                       # optional WHERE clause
      batch_size: 100                  # rows per batch (0 = all rows)
      system_prompt: "You are a helpful assistant."
      target_table: "your_schema.your_results"  # auto-created if not exists
    volume:
      enabled: false
  target:
    result_column: "ai_result"
    write_mode: "append"
```

### Step 4: Run

```bash
python -m ai_etl run
```

Expected output:

```
[Table] 读取到 100 行
[Table] 构建 JSONL 并提交...
[Table] batch 已提交: batch_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
等待 1 个 batch 任务完成 (轮询间隔 30s)...
[table] batch_xxxxxxxx-xxxx: in_progress (0/100 done, 0%) [0s]
...
[table] batch_xxxxxxxx-xxxx: completed (100/100 done, 100%) [15min]
[table] 推理完成: 100 成功, 0 失败
[table] 写入 100 行到 your_schema.your_results

完成: 100 成功, 100 行写入
```

> **Batch inference runs server-side and typically takes 10–60 minutes.**
> You can safely Ctrl+C and later resume: `python -m ai_etl resume <batch_id>`

### Step 5: Verify results

After the pipeline completes, verify the results in your Lakehouse:

```sql
-- In Lakehouse Studio or any SQL client
SELECT COUNT(*) FROM your_schema.your_results;

SELECT key_column, ai_result, model, total_tokens, processed_at
FROM your_schema.your_results
LIMIT 5;
```

Expected result:

| key_column | ai_result | model | total_tokens | processed_at |
|---|---|---|---|---|
| 1001 | This product features... | qwen3.5-flash | 156 | 2026-04-25T12:30:00+08:00 |
| 1002 | A premium quality... | qwen3.5-flash | 142 | 2026-04-25T12:30:00+08:00 |

**Verify incremental processing** (Volume mode only) — run the pipeline again, it should skip already-processed files:

```bash
python -m ai_etl run
# Expected for Volume: "增量过滤: 3 → 0 个新文件" / "没有新文件需要处理"
# Note: Table mode re-processes all rows each run (use filter/batch_size to control scope)
```

### Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `未找到 dashscope 的 API Key` | `.env` not loaded | Check `.env` is in project root, key name matches exactly |
| `缺少 ClickZetta 连接参数` | Missing credentials | Check `CLICKZETTA_USERNAME` / `CLICKZETTA_PASSWORD` in `.env` |
| `未启用任何数据源` | No source enabled | Set `etl.sources.table.enabled: true` in `config.yaml` |
| `源表 xxx 不存在` | Wrong table name | Check `etl.sources.table.table` value, verify schema.table spelling |
| `batch 任务失败` | Invalid model or quota | Check model name in provider docs, verify API key has batch access |
| Stuck at `in_progress` for hours | Normal for large batches | DashScope batch can take up to 24h; check with `python -m ai_etl status <batch_id>` |

## Two Source Modes

Both sources can be enabled simultaneously — batches are submitted in parallel and polled together.

### Table Mode (structured text)

```yaml
etl:
  sources:
    table:
      enabled: true
      table: "schema.reviews"
      key_columns: "review_id"
      text_column: "review_text"
      filter: "status = 'pending'"
      system_prompt: "Classify sentiment: positive/negative/neutral"
      target_table: "schema.review_results"
```

### Volume Mode (images, video, audio)

```yaml
etl:
  sources:
    volume:
      enabled: true
      volume_type: "user"              # external | user | table
      volume_name: ""                  # external/table: required; user: leave empty
      file_types: [".jpg", ".png"]
      subdirectory: "products/2024/"
      url_expiration: 86400            # 24h presigned URL
      system_prompt: "You are an image analysis expert."
      user_prompt: "Describe this product image in 50 words."
      target_table: "schema.volume_results"
```

Volume mode features:
- **Incremental processing**: only new files not yet in target table
- **Presigned URLs**: `GET_PRESIGNED_URL()` generates HTTP-accessible URLs for LLM APIs
- **Multi-format**: images (.jpg/.png/.gif/.webp), video (.mp4/.mov), audio (.mp3/.wav)
- **Provider-aware**: auto-adapts content format per provider (image_url / video_url / input_audio)

## Usage

### CLI

```bash
python -m ai_etl run                                    # all enabled sources
python -m ai_etl run --source-type table                # table only
python -m ai_etl run --source-type volume               # volume only
python -m ai_etl run --provider zhipuai --model glm-4-flash  # override provider
python -m ai_etl status <batch_id>                      # check batch status
python -m ai_etl resume <batch_id>                      # resume interrupted job
```

### Python API

```python
from ai_etl.pipeline import AIETLPipeline

pipeline = AIETLPipeline()
stats = pipeline.run()                    # all enabled sources
stats = pipeline.run(source_type="table") # single source
pipeline.close()
```

## Target Table Metadata

Both modes auto-create target tables with these metadata columns:

| Column | Type | Description |
|--------|------|-------------|
| `model` | STRING | Model name |
| `provider` | STRING | Provider (dashscope/zhipuai) |
| `prompt_tokens` | INT | Input tokens |
| `completion_tokens` | INT | Output tokens |
| `total_tokens` | INT | Total tokens |
| `batch_id` | STRING | Batch job ID |
| `processed_at` | STRING | Processing timestamp (Beijing time, +08:00) |
| `status_code` | INT | HTTP status (200=success) |
| `finish_reason` | STRING | Model stop reason |
| `response_id` | STRING | Server request ID |
| `source_text` | STRING | Original input text / user prompt |
| `raw_response` | STRING | Full response body JSON |

Volume mode adds: `file_path` (STRING), `volume_name` (STRING), `file_size` (BIGINT).

## Robustness

| Scenario | Handling |
|----------|----------|
| Target table missing | Auto-create with correct schema |
| Missing metadata columns | Auto ALTER TABLE ADD COLUMN |
| Type mismatch (string→bigint) | Auto schema-aware type casting |
| Empty text / no new files | Skip with warning |
| Duplicate keys | Auto dedup with warning |
| JSONL line > 6 MB | Skip with warning |
| Total > 50K lines or > 100 MB | Validation error with batch_size guidance |
| Unsupported media type for provider | Skip with warning |
| API rate limit / 5xx | Exponential backoff retry (5x) |
| Network interruption during poll | Catch and continue |
| Pipeline interrupted | batch_id persisted, `resume` supported |
| Presigned URL expiry < batch window | Warning logged |
| Volume files already processed | Incremental filter (skip existing) |
| No source enabled | Clear error with config path hint |
| Dual source enabled | Parallel batch submission, unified polling |
| Table name without schema prefix | Auto-qualify with clickzetta.schema |

## Project Structure

```
ai_etl/
├── .env.example              # Credentials template
├── config.yaml.example       # Config template
├── ai_etl/
│   ├── __main__.py           # CLI (python -m ai_etl)
│   ├── config.py             # Config loader (.env + YAML)
│   ├── media_types.py        # Media type detection + content builders
│   ├── result_keys.py        # Field constants + key encoding
│   ├── lakehouse.py          # Lakehouse read/write + Volume ops
│   ├── pipeline.py           # ETL orchestration (table + volume)
│   └── providers/            # DashScope + ZhipuAI batch providers
└── tests/                    # 61 unit tests
```

## Appendix: Batch Model Support Details

> Survey date: 2026-04-25. Model availability changes frequently — check provider docs for latest.

### DashScope (阿里云百炼)

Docs: https://help.aliyun.com/zh/model-studio/batch-inference

**Text models** | **Multimodal models** | **Embedding models**

| Model | Batch | Notes |
|-------|:---:|-------|
| qwen3-max | ✅ | |
| qwen3.5-plus | ✅ | Thinking mode on by default |
| qwen3.5-flash | ✅ | Thinking mode on by default |
| qwen-max / plus / turbo / long | ✅ | |
| qwq-plus | ✅ | Reasoning model |
| deepseek-r1 / v3 | ✅ | Third-party |

| Model | Image | Video | Audio | Notes |
|-------|:---:|:---:|:---:|-------|
| qwen3-vl-plus | ✅ | ✅ | ❌ | Recommended |
| qwen3-vl-flash | ✅ | ✅ | ❌ | Lower cost |
| qwen-vl-max / plus | ✅ | ✅ | ❌ | |
| qwen-vl-ocr | ✅ | ❌ | ❌ | OCR extraction |
| qwen-omni-turbo | ✅ | ✅ | ✅ | Only audio model |
| text-embedding-v1/v2/v3/v4 | — | — | — | Embedding |

**Limits**: ≤ 50,000 requests/file, ≤ 500 MB/file, ≤ 6 MB/line. Cost = 50% of realtime.

### ZhipuAI (智谱)

Docs: https://docs.bigmodel.cn/cn/guide/tools/batch

| Model | Batch | Queue Limit | Notes |
|-------|:---:|---:|-------|
| glm-4-air / flashx (250414) | ✅ | 2M | High throughput |
| glm-4-plus | ✅ | 2M | |
| glm-4 / glm-4-0520 | ✅ | 50K–500K | |
| glm-4-long | ✅ | 200K | Long context |
| glm-4-flash | ✅ | — | Free tier |
| glm-4v-plus / glm-4v | ✅ | 10K | Image only |
| embedding-2 / embedding-3 | ✅ | 2M | Embedding |

**Limits**: ≤ 50,000 requests/file, ≤ 100 MB/file, custom_id ≥ 6 chars. Cost = 50% of realtime.
