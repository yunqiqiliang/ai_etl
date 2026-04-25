# Implementation Plan: Multimodal AI ETL

## Overview

Extend the existing AI ETL framework to support multimodal data sources from ClickZetta Lakehouse Volumes. Implementation proceeds bottom-up: constants and pure-function modules first, then Lakehouse extensions, pipeline orchestration, CLI, configuration examples, and documentation. Each task builds on the previous, with tests validating each layer before moving to the next.

## Tasks

- [x] 1. Add Volume result key constants to `result_keys.py`
  - Add `FILE_PATH`, `VOLUME_NAME`, `FILE_SIZE` constants
  - Add `VOLUME_COLUMNS` list and `VOLUME_SQL_TYPES` dict
  - These constants are used by all downstream Volume modules
  - _Requirements: 5.1_

- [x] 2. Create `media_types.py` — MediaType enum and provider-specific content builders
  - [x] 2.1 Create `ai_etl/ai_etl/media_types.py` with `MediaType` enum, `EXTENSION_MAP`, `PROVIDER_MEDIA_SUPPORT`, `detect_media_type()`, `build_content_parts()`, and `is_provider_supported()`
    - `MediaType` enum: IMAGE, VIDEO, AUDIO, UNKNOWN
    - `EXTENSION_MAP`: map extensions (.jpg, .png, .mp4, etc.) to MediaType
    - `PROVIDER_MEDIA_SUPPORT`: DashScope supports image/video/audio; ZhipuAI supports image/video
    - `detect_media_type(file_path)`: case-insensitive extension lookup
    - `build_content_parts(presigned_url, media_type, user_prompt, provider_name)`: returns provider-specific content parts list or None if unsupported
    - `is_provider_supported(media_type, provider_name)`: boolean check
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.8_

  - [ ]* 2.2 Write property test: Config file_types normalization
    - **Property 1: Config file_types normalization**
    - For any `file_types` value (list, comma-separated string, empty, None), `Config.etl_source_file_types` returns a normalized Python list of stripped, non-empty extension strings
    - **Validates: Requirements 1.4**

  - [ ]* 2.3 Write property test: File extension filtering preserves only matching types
    - **Property 2: File extension filtering preserves only matching types**
    - For any list of file paths and any non-empty set of target extensions, filtering returns only files whose extension (case-insensitive) is in the target set
    - **Validates: Requirements 2.3**

  - [ ]* 2.4 Write property test: Media type mapping produces correct provider-specific content parts
    - **Property 6: Media type mapping produces correct provider-specific content parts**
    - For any file with a known extension and any supported provider, `build_content_parts()` returns correct format; for unsupported media types, returns None
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.8**

  - [ ]* 2.5 Write property test: Text prompt is always included in multimodal content
    - **Property 7: Text prompt is always included in multimodal content**
    - For any file and any non-empty user prompt, when `build_content_parts()` returns non-None, the last element is a text part containing the exact user prompt
    - **Validates: Requirements 4.4**

  - [ ]* 2.6 Write property test: Request ID round-trip for file paths
    - **Property 8: Request ID round-trip for file paths**
    - For any valid file relative_path string, `encode_custom_id([path])` then `decode_custom_id()` recovers the original path
    - **Validates: Requirements 4.5, 5.2**

- [x] 3. Extend `Config` class with Volume source properties
  - [x] 3.1 Add Volume source properties to `ai_etl/ai_etl/config.py`
    - Add `etl_source_type` property: returns "table" (default) or "volume" from `etl.source.source_type`
    - Add `etl_source_volume_name` property: from `etl.source.volume_name`
    - Add `etl_source_file_types` property: normalize list/string/None to list of extensions
    - Add `etl_source_subdirectory` property: from `etl.source.subdirectory`
    - Add `etl_source_url_expiration` property: from `etl.source.url_expiration`, default 86400
    - Add `etl_source_user_prompt` property: from `etl.source.user_prompt`, default "Describe this file"
    - Add `multimodal_model` property: reads `<provider>.multimodal_model` with fallback defaults
    - Add `resolve_model(source_type)` method: returns multimodal_model for "volume", model_name for "table"
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

  - [ ]* 3.2 Write unit tests for Config Volume properties
    - Test default values when no Volume config is present
    - Test `etl_source_file_types` with list, comma-separated string, and empty values
    - Test `resolve_model()` returns correct model for each source_type
    - Test `etl_source_type` defaults to "table"
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

- [x] 4. Checkpoint — Verify constants, media_types, and config
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement Volume file discovery in `lakehouse.py`
  - [x] 5.1 Add `discover_volume_files()` method to `LakehouseClient`
    - Execute `ALTER VOLUME <volume_name> REFRESH` before querying
    - Query `SELECT relative_path, size FROM DIRECTORY(VOLUME <volume_name>)`
    - Filter by `file_types` (case-insensitive extension match) if configured
    - Filter by `subdirectory` prefix if configured
    - Query target table for already-processed file paths: `SELECT DISTINCT file_path FROM <target_table>` (catch exception if table doesn't exist)
    - Exclude files already in target table (set difference)
    - Log info "No new files to process" and return empty list if zero new files
    - Apply `batch_size` limit if configured and > 0
    - Return list of `{relative_path, size}` dicts
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_

  - [ ]* 5.2 Write property test: Subdirectory prefix filtering
    - **Property 3: Subdirectory prefix filtering preserves only matching paths**
    - For any list of file paths and any subdirectory prefix, filtering returns only files whose relative_path starts with the prefix
    - **Validates: Requirements 2.4**

  - [ ]* 5.3 Write property test: Incremental discovery excludes all processed files
    - **Property 4: Incremental discovery excludes all processed files**
    - For any set of discovered file paths and any set of already-processed file paths, the result is exactly the set difference
    - **Validates: Requirements 2.6**

  - [ ]* 5.4 Write property test: Batch size limiting
    - **Property 5: Batch size limiting**
    - For any list of files and any positive batch_size, the result length ≤ batch_size; when input is longer, result length equals batch_size
    - **Validates: Requirements 2.8**

- [x] 6. Implement presigned URL generation in `lakehouse.py`
  - [x] 6.1 Add `generate_presigned_urls()` method to `LakehouseClient`
    - Execute `SET cz.sql.function.get.presigned.url.force.external=true`
    - For each file, execute `SELECT GET_PRESIGNED_URL(VOLUME <volume_name>, '<relative_path>', <expiration>)`
    - Use configured `url_expiration` value
    - Skip files where URL is null, log warning
    - Return enriched file list with `presigned_url` added to each dict
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

  - [ ]* 6.2 Write unit tests for `generate_presigned_urls()` with mocked session
    - Test that SET command is executed before URL generation
    - Test that null URLs are skipped with warning
    - Test that valid URLs are added to file dicts
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [x] 7. Implement multimodal JSONL builder in `lakehouse.py`
  - [x] 7.1 Add `build_multimodal_jsonl()` method to `LakehouseClient`
    - Accept files list (with presigned_url), model, user_prompt, provider_name, endpoint, output_path
    - For each file: detect media type, call `build_content_parts()`, encode relative_path as custom_id
    - Skip files with unsupported media types (log warning)
    - Skip files exceeding 6 MB per-line limit (log warning)
    - Validate total lines ≤ 50,000 and file size ≤ provider limit
    - Include text content part with configured user_prompt
    - Write JSONL file and return path
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_

  - [ ]* 7.2 Write unit tests for `build_multimodal_jsonl()`
    - Test JSONL output format for image, video, audio files
    - Test that unsupported media types are skipped
    - Test that oversized lines are skipped
    - Test that user_prompt appears as text content part
    - Test custom_id encodes relative_path
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_

- [x] 8. Checkpoint — Verify Lakehouse Volume methods
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement Volume result writing in `lakehouse.py`
  - [x] 9.1 Add `write_volume_results()` method to `LakehouseClient`
    - Target table includes columns: file_path (STRING), volume_name (STRING), file_size (BIGINT), result column (STRING), plus all metadata columns
    - Decode custom_id to recover file's relative_path
    - Populate file_path, volume_name, file_size from discovery data
    - Auto-create target table with correct schema if it doesn't exist
    - ALTER TABLE ADD COLUMN for missing Volume-specific columns if table exists
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [ ]* 9.2 Write property test: Volume result mapping preserves file metadata
    - **Property 9: Volume result mapping preserves file metadata**
    - For any inference result with a valid custom_id and file metadata dict, the output row contains correct file_path, volume_name, and file_size
    - **Validates: Requirements 5.3**

  - [ ]* 9.3 Write unit tests for `write_volume_results()` table creation
    - Test auto-create DDL includes Volume-specific columns
    - Test ALTER TABLE for missing columns on existing table
    - _Requirements: 5.4, 5.5_

- [x] 10. Extend pipeline orchestration in `pipeline.py`
  - [x] 10.1 Add `_run_volume()` method to `AIETLPipeline` and branch `run()` on source_type
    - Modify `run()` to check `source_type` (from config or parameter) and branch to `_run_volume()` or existing table flow
    - `_run_volume()` executes: discover files → filter incremental → generate URLs → build JSONL → submit batch → wait → download → write results
    - Use `resolve_model()` to select multimodal model for volume source
    - Persist batch_id with source_type info to last_batch.json for resume
    - Report file counts in statistics (discovered, new, processed, failed, written)
    - Log warning when `url_expiration` < `completion_window`
    - Validate volume_name is provided, raise RuntimeError if missing
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [ ]* 10.2 Write unit tests for pipeline source_type branching
    - Test that `run()` calls `_run_volume()` when source_type is "volume"
    - Test that `run()` calls existing table flow when source_type is "table"
    - Test that missing volume_name raises RuntimeError
    - Test URL expiration warning logic
    - _Requirements: 6.1, 6.2, 6.3, 6.5_

- [x] 11. Add CLI arguments for Volume ETL in `__main__.py`
  - [x] 11.1 Add `--source-type`, `--volume-name`, `--file-types` arguments to the `run` subcommand
    - `--source-type` with choices ["table", "volume"]
    - `--volume-name` for Volume source name
    - `--file-types` as comma-separated extensions string
    - Validate: when `--source-type volume` is used without volume_name (CLI or config), exit with error
    - Pass new arguments through to `pipeline.run()`
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [ ]* 11.2 Write unit tests for CLI argument parsing and validation
    - Test `--source-type volume` without volume_name exits with error
    - Test `--file-types` parsing into list
    - Test arguments are passed to pipeline correctly
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [x] 12. Checkpoint — Verify pipeline and CLI integration
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Update configuration example and documentation
  - [x] 13.1 Update `config.yaml.example` with Volume source configuration
    - Add `source_type` field under `etl.source`
    - Add Volume-specific fields: `volume_name`, `file_types`, `subdirectory`, `url_expiration`, `user_prompt`
    - Add `multimodal_model` field to dashscope and zhipuai sections
    - Include inline comments explaining each new field
    - _Requirements: 1.1, 1.3, 1.4, 1.5, 1.6, 1.7_

  - [x] 13.2 Update `README.md` with multimodal architecture, Volume usage, and new CLI args
    - Add Volume ETL flow to architecture diagram
    - Add Volume usage examples (CLI and Python API)
    - Document new CLI arguments (`--source-type`, `--volume-name`, `--file-types`)
    - Document Volume target table schema with file metadata columns
    - Update project structure to include `media_types.py`
    - _Requirements: 1.1, 6.1, 7.1, 7.2, 7.3_

- [x] 14. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation after each functional layer
- Property tests validate universal correctness properties from the design document (Properties 1–9)
- Unit tests validate specific examples and edge cases
- All tests should be created in `ai_etl/tests/` directory using pytest
- Property-based tests should use the `hypothesis` library with minimum 100 iterations per property
- The implementation language is Python, matching the existing codebase
