# Requirements: Multimodal AI ETL

## Introduction

将 AI ETL 项目升级为统一的多模态 AI ETL 框架。支持两种数据源：Lakehouse 表（结构化文本）和 Lakehouse Volume（非结构化文件：图片、音频、视频）。Volume 数据源通过 `GET_PRESIGNED_URL()` 生成预签名 URL，传递给 LLM Batch API 的多模态模型进行推理。支持增量处理——只处理目标表中尚未存在的新文件。

## Glossary

- **Volume**: ClickZetta Lakehouse 的存储抽象，挂载 OSS/COS/S3 对象存储，包含 External Volume、User Volume、Table Volume
- **DIRECTORY Function**: `DIRECTORY(VOLUME name)` SQL 函数，返回 Volume 中文件的元数据（relative_path, size 等）
- **GET_PRESIGNED_URL**: SQL 函数，为 Volume 文件生成有时效的外部可访问 HTTP URL
- **Presigned URL**: 由 GET_PRESIGNED_URL 生成的临时 URL，LLM API 通过此 URL 下载文件进行推理
- **Source Type**: 配置字段，指定数据源类型："table"（表文本）或 "volume"（Volume 文件）
- **Incremental Discovery**: 增量发现——对比 Volume 文件列表与目标表已处理记录，只提取未处理的新文件
- **Multimodal Model**: 接受非文本输入（图片/音频/视频）的 LLM 模型，如 qwen-vl-plus、GLM-4V-Plus

## Requirements

### Requirement 1: Unified Source Configuration

**User Story:** As a data engineer, I want a single configuration schema that supports both table and Volume data sources, so that I can switch between text and multimodal ETL by changing config only.

#### Acceptance Criteria

1. THE Config SHALL support a `source_type` field under `etl.source` with values "table" and "volume"
2. WHEN `source_type` is "table", THE Config SHALL require `table`, `key_columns`, and `text_column` fields (existing behavior)
3. WHEN `source_type` is "volume", THE Config SHALL require a `volume_name` field specifying the Volume name (e.g., "my_oss_volume" or "USER VOLUME")
4. WHEN `source_type` is "volume", THE Config SHALL support an optional `file_types` list of extensions to filter (e.g., [".jpg", ".png"])
5. WHEN `source_type` is "volume", THE Config SHALL support an optional `subdirectory` field to scope discovery to a path prefix
6. WHEN `source_type` is "volume", THE Config SHALL support a `url_expiration` field defaulting to 86400 seconds (24 hours)
7. WHEN `source_type` is "volume", THE Config SHALL support a `user_prompt` field for the text instruction accompanying each file (e.g., "Describe this image")

### Requirement 2: Volume File Discovery with Incremental Support

**User Story:** As a data engineer, I want the system to discover only new files in a Volume that haven't been processed yet, so that I can run the pipeline repeatedly without reprocessing.

#### Acceptance Criteria

1. THE LakehouseClient SHALL execute `ALTER VOLUME <volume_name> REFRESH` before querying the directory
2. THE LakehouseClient SHALL query `DIRECTORY(VOLUME <volume_name>)` to retrieve file metadata (relative_path, size)
3. WHEN `file_types` is configured, THE LakehouseClient SHALL filter to files matching the specified extensions (case-insensitive)
4. WHEN `subdirectory` is configured, THE LakehouseClient SHALL filter to files whose relative_path starts with the prefix
5. THE LakehouseClient SHALL query the target table for already-processed file paths: `SELECT DISTINCT file_path FROM <target_table>`
6. THE LakehouseClient SHALL exclude files whose relative_path already exists in the target table's file_path column
7. IF zero new files remain after incremental filtering, THE LakehouseClient SHALL log an info message "No new files to process" and return an empty list
8. WHEN `batch_size` is configured and > 0, THE LakehouseClient SHALL limit the number of new files to batch_size

### Requirement 3: Presigned URL Generation

**User Story:** As a data engineer, I want the system to generate presigned URLs for Volume files, so that LLM APIs can access files via HTTP.

#### Acceptance Criteria

1. THE LakehouseClient SHALL execute `SET cz.sql.function.get.presigned.url.force.external=true` before generating URLs
2. THE LakehouseClient SHALL generate URLs by executing `SELECT relative_path, GET_PRESIGNED_URL(VOLUME <volume_name>, relative_path, <expiration>) FROM ...` for discovered files
3. THE LakehouseClient SHALL use the configured `url_expiration` value
4. IF GET_PRESIGNED_URL returns null for a file, THE LakehouseClient SHALL log a warning and skip that file

### Requirement 4: Multimodal JSONL Construction

**User Story:** As a data engineer, I want the system to build JSONL with multimodal content parts, so that batch APIs can process images, videos, and audio.

#### Acceptance Criteria

1. FOR image files (.jpg, .jpeg, .png, .gif, .bmp, .webp, .tiff), THE JSONL builder SHALL include `{"type": "image_url", "image_url": {"url": "<presigned_url>"}}`
2. FOR video files (.mp4, .avi, .mov, .mkv, .webm), THE JSONL builder SHALL include `{"type": "video_url", "video_url": {"url": "<presigned_url>"}}` for DashScope, or `{"type": "video", "video": "<presigned_url>"}` for ZhipuAI
3. FOR audio files (.mp3, .wav, .flac, .ogg, .m4a), THE JSONL builder SHALL include the provider-appropriate audio content format
4. THE JSONL builder SHALL include a text content part with the configured `user_prompt`
5. THE JSONL builder SHALL encode the file's relative_path as the key in custom_id
6. THE JSONL builder SHALL skip files exceeding the 6 MB per-line limit and log a warning
7. THE JSONL builder SHALL validate total lines ≤ 50,000 and file size ≤ provider limit
8. IF the provider does not support the file's media type, THE JSONL builder SHALL skip the file and log a warning

### Requirement 5: Result Mapping and Target Table for Volume Sources

**User Story:** As a data engineer, I want results mapped back to source files with file metadata, so that I can trace results to input files.

#### Acceptance Criteria

1. THE target table for Volume sources SHALL include columns: `file_path` (STRING), `volume_name` (STRING), `file_size` (BIGINT), the result column (STRING), plus all metadata columns
2. THE LakehouseClient SHALL decode custom_id to recover the file's relative_path
3. THE LakehouseClient SHALL populate `file_path` with the relative_path, `volume_name` with the configured volume name, and `file_size` from the discovery data
4. WHEN the target table does not exist, THE LakehouseClient SHALL auto-create it with the correct schema
5. WHEN the target table exists but lacks Volume-specific columns, THE LakehouseClient SHALL ALTER TABLE ADD COLUMN

### Requirement 6: Pipeline Orchestration

**User Story:** As a data engineer, I want the pipeline to handle both table and Volume ETL flows seamlessly.

#### Acceptance Criteria

1. WHEN `source_type` is "volume", THE Pipeline SHALL execute: discover files → filter incremental → generate URLs → build JSONL → submit batch → wait → download → write results
2. WHEN `source_type` is "table", THE Pipeline SHALL execute the existing flow: read table → build JSONL → submit → wait → download → write results
3. THE Pipeline SHALL persist batch_id with source_type info to last_batch.json for resume
4. THE Pipeline SHALL report file counts in statistics (discovered, new, processed, failed, written)
5. WHEN `url_expiration` < `completion_window`, THE Pipeline SHALL log a warning about potential URL expiry

### Requirement 7: CLI Support

**User Story:** As a data engineer, I want CLI arguments for Volume ETL.

#### Acceptance Criteria

1. THE `run` subcommand SHALL accept `--source-type` (table/volume)
2. THE `run` subcommand SHALL accept `--volume-name` for Volume sources
3. THE `run` subcommand SHALL accept `--file-types` as comma-separated extensions
4. WHEN `--source-type volume` is used without volume_name (CLI or config), THE CLI SHALL exit with an error
