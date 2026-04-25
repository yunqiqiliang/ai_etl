# AI ETL

**Multimodal AI ETL: Lakehouse → LLM Batch Inference → Lakehouse**

From ClickZetta Lakehouse, read structured text (tables) or unstructured files (images, video, audio from Volumes), run LLM batch inference at 50% cost, and write results back with full metadata.

Supports multiple providers (DashScope / ZhipuAI) and multiple modalities (text, image, video, audio), switchable via config.

## Why Batch Inference?

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
+------------------------------------------------------------------------+
|                    Multimodal AI ETL Pipeline                          |
|                                                                        |
|  +-----------------+                        +------------------------+ |
|  | Source: Table    |                       | Target Table           | |
|  | SELECT key+text  |                       | - key / file_path      | |
|  +-----------------+                        | - ai_result            | |
|  +-----------------+                        | - 12 metadata cols     | |
|  | Source: Volume   |                       +------------------------+ |
|  | DIRECTORY()      |--------+                         ^               |
|  | PRESIGNED_URL()  |        |                         |               |
|  +-----------------+        v                          |               |
|                      +----------------+                |               |
|                      | Build JSONL    |                |               |
|                      | text/multimodal|                |               |
|                      +-------+--------+                |               |
|                              v                         |               |
|                   +----------------------+             |               |
|                   | LLM Batch API        |             |               |
|                   | DashScope: Qwen/VL   |             |               |
|                   | ZhipuAI:   GLM/4V    |             |               |
|                   +----------+-----------+             |               |
|                              v                         |               |
|                      +----------------+                |               |
|                      | Write results  +----------------+               |
|                      | + metadata     |                                |
|                      +----------------+                                |
|                                                                        |
|  Config:  .env (credentials)  +  config.yaml (parameters)              |
+------------------------------------------------------------------------+
```

## Quick Start

```bash
# 1. Install
cd ai_etl && pip install -e .

# 2. Configure credentials
cp .env.example .env        # edit: API keys + Lakehouse password

# 3. Configure ETL parameters
cp config.yaml.example config.yaml   # edit: provider, source, target

# 4. Run
python -m ai_etl run                           # table mode (default)
python -m ai_etl run --source-type volume      # volume mode
```

## Two Source Modes

### Table Mode (structured text)

```yaml
etl:
  source:
    source_type: "table"
    table: "schema.reviews"
    key_columns: "review_id"
    text_column: "review_text"
    filter: "status = 'pending'"
    system_prompt: "Classify sentiment: positive/negative/neutral"
```

### Volume Mode (images, video, audio)

```yaml
etl:
  source:
    source_type: "volume"
    volume_name: "my_oss_volume"       # or "USER VOLUME"
    file_types: [".jpg", ".png"]
    subdirectory: "products/2024/"
    url_expiration: 86400              # 24h presigned URL
    system_prompt: "You are an image analysis expert."
    user_prompt: "Describe this product image in 50 words."
```

Volume mode features:
- **Incremental processing**: only new files not yet in target table
- **Presigned URLs**: `GET_PRESIGNED_URL()` generates HTTP-accessible URLs for LLM APIs
- **Multi-format**: images (.jpg/.png/.gif/.webp), video (.mp4/.mov), audio (.mp3/.wav)
- **Provider-aware**: auto-adapts content format per provider (image_url / video_url / input_audio)

## CLI

```bash
# Table ETL
python -m ai_etl run
python -m ai_etl run --provider zhipuai --model glm-4-flash

# Volume ETL
python -m ai_etl run --source-type volume --volume-name "USER VOLUME" --file-types .jpg,.png

# Query batch status
python -m ai_etl status <batch_id>

# Resume interrupted job
python -m ai_etl resume <batch_id>
```

## Python API

```python
from ai_etl import AIETLPipeline

# Table mode
pipeline = AIETLPipeline()
stats = pipeline.run()  # reads source_type from config.yaml
pipeline.close()

# Volume mode (override config)
pipeline = AIETLPipeline()
stats = pipeline.run(
    source_type="volume",
    volume_name="USER VOLUME",
    file_types=[".jpg", ".png"],
)
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
| `processed_at` | STRING | Processing time (UTC ISO) |
| `status_code` | INT | HTTP status (200=success) |
| `finish_reason` | STRING | Model stop reason |
| `response_id` | STRING | Server request ID |
| `source_text` | STRING | Original input text / user prompt |
| `raw_response` | STRING | Full response body JSON |

Volume mode adds: `file_path` (STRING), `volume_name` (STRING), `file_size` (BIGINT).

## Supported Providers & Models

| Provider | Text Models | Multimodal Models | Media Support |
|----------|------------|-------------------|---------------|
| **DashScope** | qwen3.5-flash, qwen3-max, deepseek-r1/v3 | qwen3-vl-plus/flash, qwen-vl-max, qwen-vl-ocr, qwen-omni-turbo | Image, Video, Audio |
| **ZhipuAI** | glm-4-flash, glm-4-plus | glm-4v, glm-4v-plus | Image, Video |

## Batch Model Support Details

> Survey date: 2026-04-25. Model availability changes frequently — check provider docs for latest.

### DashScope (阿里云百炼)

Docs: https://help.aliyun.com/zh/model-studio/batch-inference

**Text generation models (source_type: table)**

| Model | Batch Support | Notes |
|-------|:---:|-------|
| qwen3-max | ✅ | |
| qwen3.5-plus | ✅ | Thinking mode on by default, set `enable_thinking: false` to save cost |
| qwen3.5-flash | ✅ | Thinking mode on by default |
| qwen-max / qwen-max-latest | ✅ | |
| qwen-plus / qwen-plus-latest | ✅ | |
| qwen-turbo / qwen-turbo-latest | ✅ | |
| qwen-long / qwen-long-latest | ✅ | |
| qwq-plus | ✅ | Reasoning model |
| deepseek-r1 | ✅ | Third-party |
| deepseek-v3 / deepseek-v3.2 | ✅ | Third-party |

**Multimodal models (source_type: volume)**

| Model | Image | Video | Audio | OCR | Notes |
|-------|:---:|:---:|:---:|:---:|-------|
| qwen3-vl-plus | ✅ | ✅ | ❌ | ❌ | Recommended for image/video |
| qwen3-vl-flash | ✅ | ✅ | ❌ | ❌ | Faster, lower cost |
| qwen3.5-plus | ✅ | ✅ | ❌ | ❌ | Also supports multimodal input |
| qwen3.5-flash | ✅ | ✅ | ❌ | ❌ | Also supports multimodal input |
| qwen-vl-max / qwen-vl-max-latest | ✅ | ✅ | ❌ | ❌ | |
| qwen-vl-plus / qwen-vl-plus-latest | ✅ | ✅ | ❌ | ❌ | |
| qwen-vl-ocr / qwen-vl-ocr-latest | ✅ | ❌ | ❌ | ✅ | Text extraction from images |
| qwen-omni-turbo | ✅ | ✅ | ✅ | ❌ | Only model supporting audio |

**Embedding models**

| Model | Batch Support |
|-------|:---:|
| text-embedding-v1/v2/v3/v4 | ✅ |

**Limits**: ≤ 50,000 requests/file, ≤ 500 MB/file, ≤ 6 MB/line. Cost = 50% of realtime.

### ZhipuAI (智谱)

Docs: https://docs.bigmodel.cn/cn/guide/tools/batch

**Text generation models (source_type: table)**

| Model | Batch Support | Queue Limit | Notes |
|-------|:---:|---:|-------|
| glm-4-air-250414 | ✅ | 2M | High throughput |
| glm-4-flashx-250414 | ✅ | 2M | High throughput |
| glm-4-plus | ✅ | 2M | |
| glm-4-0520 | ✅ | 500K | |
| glm-4 | ✅ | 50K | |
| glm-4-long | ✅ | 200K | Long context |
| glm-4-flash | ✅ | — | Free tier available |

**Multimodal models (source_type: volume)**

| Model | Image | Video | Audio | Queue Limit | Notes |
|-------|:---:|:---:|:---:|---:|-------|
| glm-4v-plus | ✅ | ❌ | ❌ | 10K | Recommended for image |
| glm-4v-plus-0111 | ✅ | ❌ | ❌ | 10K | |
| glm-4v | ✅ | ❌ | ❌ | 10K | |

**Image generation models**

| Model | Queue Limit |
|-------|---:|
| cogview-4-250304 | 10K |
| cogview-3-plus | 10K |

**Embedding models**

| Model | Batch Support | Queue Limit |
|-------|:---:|---:|
| embedding-2 | ✅ | 2M |
| embedding-3 | ✅ | 2M |

**Limits**: ≤ 50,000 requests/file, ≤ 100 MB/file, custom_id ≥ 6 chars. Cost = 50% of realtime. `completion_window` deprecated (auto-scheduled, ~24h, 7-day timeout).

## Lakehouse AI Integration Roadmap

> Survey date: 2026-04-25. Source: [yunqi.tech product docs](https://yunqi.tech/llms-full.txt)

This project currently uses external Batch APIs (DashScope / ZhipuAI) for inference. ClickZetta Lakehouse offers several built-in AI capabilities that can complement or replace external batch processing in certain scenarios:

| Capability | What It Does | When to Use Instead of Batch API |
|-----------|-------------|----------------------------------|
| **AI Gateway** | Unified model routing, rate limiting, caching, cost attribution | Centralized model management across teams |
| **AI_COMPLETE()** | Call LLM directly in SQL | Small-scale text/image analysis (< 1K rows) |
| **AI_EMBEDDING()** | Generate vector embeddings in SQL | Building RAG indexes, semantic search |
| **Vector Index (HNSW)** | Similarity search with L2/cosine distance | Post-ETL retrieval and recommendation |
| **Dynamic Table** | Incremental refresh on upstream changes | Auto-aggregate ETL results |
| **Table Stream** | CDC on source tables | Event-driven ETL triggers |
| **External Function** | Custom UDF via cloud functions | Complex pre/post-processing |

### Batch API vs SQL AI Function

| Dimension | Batch API (this project) | SQL AI Function |
|-----------|-------------------------|-----------------|
| **Scale** | 10K–50K rows per job | Hundreds of rows |
| **Latency** | Minutes to hours | Seconds |
| **Cost** | 50% off standard pricing | Standard pricing |
| **Modality** | Text + image + video + audio | Text + image |
| **Infrastructure** | External API (DashScope/ZhipuAI) | Built-in Lakehouse SQL |
| **Best for** | Large-scale offline processing | Interactive analysis, small batches |

For details, see [AI Functions](https://www.yunqi.tech/documents/AI_function_in_SQL) and [Vector Search](https://www.yunqi.tech/documents/vector-search) in the Lakehouse docs.

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
└── tests/                    # 45 unit tests
```

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
