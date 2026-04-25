"""
ClickZetta Lakehouse 读写模块。

通过 ZettaPark Session 连接 ClickZetta Lakehouse，
提供从源表读取数据和向目标表写入推理结果的功能。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ai_etl.config import Config
from ai_etl.result_keys import (
    BATCH_ID, COMPLETION_TOKENS, CONTENT, CUSTOM_ID, FINISH_REASON,
    METADATA_COLUMNS, METADATA_SQL_TYPES, MODEL, PROCESSED_AT,
    PROMPT_TOKENS, PROVIDER, RAW_RESPONSE, RESPONSE_ID,
    SOURCE_TEXT, STATUS_CODE, TOTAL_TOKENS,
    decode_custom_id, encode_custom_id, normalize_custom_id,
)

logger = logging.getLogger(__name__)


class LakehouseError(Exception):
    """Lakehouse 操作错误。"""


class LakehouseClient:
    """ClickZetta Lakehouse 客户端。"""

    def __init__(
        self,
        config: Optional[Config] = None,
        session_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._config = config or Config()
        self._session_config = session_config or self._config.get_clickzetta_config()
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def _create_session(self):
        try:
            from clickzetta.zettapark.session import Session
        except ImportError:
            raise LakehouseError(
                "请安装 clickzetta-zettapark-python: "
                "pip install clickzetta-zettapark-python"
            )

        required = ["service", "instance", "workspace", "username", "password"]
        missing = [k for k in required if not self._session_config.get(k)]
        if missing:
            raise LakehouseError(
                f"缺少 ClickZetta 连接参数: {missing}。"
                f"请在 .env 中配置 CLICKZETTA_USERNAME/PASSWORD，"
                f"在 config.yaml 中配置 clickzetta.service/instance/workspace。"
            )

        logger.info(
            "连接 ClickZetta Lakehouse: %s/%s",
            self._session_config.get("service"),
            self._session_config.get("workspace"),
        )
        session = Session.builder.configs(self._session_config).create()
        logger.info("ClickZetta Session 创建成功")
        return session

    # ── 读取源表 ──────────────────────────────────────────────

    def read_source(
        self,
        table: Optional[str] = None,
        key_columns: Optional[str] = None,
        text_column: Optional[str] = None,
        filter_expr: Optional[str] = None,
        batch_size: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        """从源表读取数据，返回字典列表。"""
        cfg = self._config
        table = table or cfg.etl_source_table
        key_columns = key_columns or cfg.etl_source_key_columns
        text_column = text_column or cfg.etl_source_text_column
        filter_expr = filter_expr if filter_expr is not None else cfg.etl_source_filter
        batch_size = batch_size if batch_size is not None else cfg.etl_source_batch_size

        if not table:
            raise LakehouseError("未指定源表名。请在 config.yaml 的 etl.source.table 中配置。")

        key_cols = [c.strip() for c in key_columns.split(",")]
        select_cols = key_cols + [text_column]

        sql = f"SELECT {', '.join(select_cols)} FROM {table}"
        if filter_expr:
            sql += f" WHERE {filter_expr}"
        if batch_size and batch_size > 0:
            sql += f" LIMIT {batch_size}"

        logger.info("读取源表: %s", sql)
        rows = self.session.sql(sql).collect()

        result = []
        for row in rows:
            record = {}
            for col in select_cols:
                record[col] = str(row[col]) if row[col] is not None else ""
            result.append(record)

        logger.info("读取完成: %d 行", len(result))
        return result

    # ── Volume 文件发现 ───────────────────────────────────────

    def discover_volume_files(
        self,
        volume_sql_ref: Optional[str] = None,
        file_types: Optional[List[str]] = None,
        subdirectory: Optional[str] = None,
        target_table: Optional[str] = None,
        batch_size: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """发现 Volume 中的新文件（增量：排除目标表已处理的文件）。

        Args:
            volume_sql_ref: SQL 中的 Volume 引用，如 'VOLUME my_vol' / 'USER VOLUME' / 'TABLE VOLUME t'。
                           默认从 config 的 volume_type + volume_name 生成。
        """
        from ai_etl.media_types import filter_files_by_extensions, filter_files_by_subdirectory

        cfg = self._config
        if not volume_sql_ref:
            volume_sql_ref = cfg.get_volume_sql_ref()
        file_types = file_types if file_types is not None else cfg.etl_volume_file_types
        subdirectory = subdirectory if subdirectory is not None else cfg.etl_volume_subdirectory
        target_table = target_table or cfg.etl_target_table
        batch_size = batch_size if batch_size is not None else cfg.etl_volume_batch_size

        is_user_volume = volume_sql_ref.upper().strip() == "USER VOLUME"

        # 刷新 Volume 目录元数据（User Volume 不需要）
        if not is_user_volume:
            try:
                # External/Table Volume 需要 ALTER VOLUME REFRESH
                vol_name_only = volume_sql_ref.replace("VOLUME ", "").replace("TABLE ", "").strip()
                self.session.sql(f"ALTER VOLUME {vol_name_only} REFRESH").collect()
            except Exception as e:
                logger.warning("ALTER VOLUME REFRESH 失败: %s", e)

        # 查询文件列表（不同 Volume 类型语法不同）
        logger.info("查询 Volume 文件列表: %s", volume_sql_ref)
        if is_user_volume:
            dir_sql = "SELECT relative_path, size FROM (SHOW USER VOLUME DIRECTORY)"
        else:
            dir_sql = f"SELECT relative_path, size FROM DIRECTORY({volume_sql_ref})"
        rows = self.session.sql(dir_sql).collect()

        files = []
        for row in rows:
            rp = str(row["relative_path"]) if row["relative_path"] is not None else ""
            sz = int(row["size"]) if row["size"] is not None else 0
            if rp:
                files.append({"relative_path": rp, "size": sz})

        logger.info("Volume 中共 %d 个文件", len(files))

        # 按扩展名过滤
        if file_types:
            files = filter_files_by_extensions(files, file_types)
            logger.info("扩展名过滤后: %d 个文件", len(files))

        # 按子目录过滤
        if subdirectory:
            files = filter_files_by_subdirectory(files, subdirectory)
            logger.info("子目录过滤后: %d 个文件", len(files))

        # 增量过滤：排除目标表中已处理的文件
        processed_paths: set = set()
        if target_table:
            try:
                processed_rows = self.session.sql(
                    f"SELECT DISTINCT file_path FROM {target_table}"
                ).collect()
                processed_paths = {str(r["file_path"]) for r in processed_rows if r["file_path"]}
                logger.info("目标表已处理 %d 个文件", len(processed_paths))
            except Exception:
                logger.debug("目标表不存在或无 file_path 列，视为无已处理文件")

        if processed_paths:
            before = len(files)
            files = [f for f in files if f["relative_path"] not in processed_paths]
            logger.info("增量过滤: %d → %d 个新文件", before, len(files))

        if not files:
            logger.info("No new files to process")
            return []

        # 批次限制
        if batch_size and batch_size > 0 and len(files) > batch_size:
            files = files[:batch_size]
            logger.info("批次限制: 取前 %d 个文件", batch_size)

        logger.info("发现 %d 个待处理文件", len(files))
        return files

    # ── 预签名 URL 生成 ──────────────────────────────────────

    def generate_presigned_urls(
        self,
        files: List[Dict[str, Any]],
        volume_sql_ref: Optional[str] = None,
        expiration: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """为 Volume 文件生成预签名 URL。

        Returns:
            文件字典列表，每项增加 presigned_url 字段。
        """
        cfg = self._config
        if not volume_sql_ref:
            volume_sql_ref = cfg.get_volume_sql_ref()
        expiration = expiration if expiration is not None else cfg.etl_volume_url_expiration

        if not files:
            return []

        # 确保生成外部可访问的 URL
        self.session.sql("SET cz.sql.function.get.presigned.url.force.external=true").collect()

        result = []
        skipped = 0

        for f in files:
            rp = f["relative_path"]
            try:
                url_rows = self.session.sql(
                    f"SELECT GET_PRESIGNED_URL({volume_sql_ref}, '{rp}', {expiration}) AS url"
                ).collect()
                url = str(url_rows[0]["url"]) if url_rows and url_rows[0]["url"] else None
            except Exception as e:
                logger.warning("生成预签名 URL 失败 (%s): %s", rp, e)
                url = None

            if url:
                result.append({**f, "presigned_url": url})
            else:
                skipped += 1
                logger.warning("跳过无法生成 URL 的文件: %s", rp)

        if skipped:
            logger.warning("共 %d 个文件无法生成预签名 URL", skipped)

        logger.info("生成 %d 个预签名 URL (有效期 %ds = %.1fh)", len(result), expiration, expiration / 3600)
        return result

    # ── 多模态 JSONL 构建 ────────────────────────────────────

    def build_multimodal_jsonl(
        self,
        files: List[Dict[str, Any]],
        model: Optional[str] = None,
        user_prompt: Optional[str] = None,
        system_prompt: Optional[str] = None,
        provider_name: Optional[str] = None,
        endpoint: str = "/v1/chat/completions",
        output_path: Optional[Union[str, Path]] = None,
    ) -> Path:
        """从 Volume 文件构建多模态 Batch 推理的 JSONL 输入文件。"""
        import time
        from ai_etl.media_types import detect_media_type, build_content_parts

        cfg = self._config
        model = model or cfg.resolve_model("volume")
        user_prompt = user_prompt or cfg.etl_source_user_prompt
        system_prompt = system_prompt or cfg.etl_source_system_prompt
        provider_name = provider_name or cfg.provider_name

        if output_path is None:
            output_path = Path(cfg.output_dir) / f"batch_multimodal_{int(time.time())}.jsonl"
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        lines = []
        skipped_unsupported = 0
        skipped_oversized = 0

        for f in files:
            rp = f["relative_path"]
            url = f.get("presigned_url", "")
            if not url:
                continue

            media_type = detect_media_type(rp)
            content_parts = build_content_parts(url, media_type, user_prompt, provider_name)

            if content_parts is None:
                skipped_unsupported += 1
                logger.warning("跳过不支持的媒体类型: %s (type=%s, provider=%s)", rp, media_type.value, provider_name)
                continue

            custom_id = encode_custom_id([rp])

            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": content_parts})

            record = {
                "custom_id": custom_id,
                "method": "POST",
                "url": endpoint,
                "body": {"model": model, "messages": messages},
            }
            line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))

            if len(line.encode("utf-8")) > 6 * 1024 * 1024:
                skipped_oversized += 1
                logger.warning("跳过超过 6 MB 限制的行: %s", rp)
                continue

            lines.append(line)

        if skipped_unsupported:
            logger.warning("跳过 %d 个不支持的媒体类型文件", skipped_unsupported)
        if skipped_oversized:
            logger.warning("跳过 %d 个超过 6 MB 限制的行", skipped_oversized)

        if not lines:
            raise LakehouseError("没有有效文件可构建 JSONL（全部被跳过）")

        if len(lines) > 50_000:
            raise LakehouseError(
                f"请求数 {len(lines)} 超过 Batch API 单文件限制 (50,000)。"
                f"请设置 etl.source.batch_size <= 50000 分批处理。"
            )

        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        file_size = output_path.stat().st_size
        max_size = 100 * 1024 * 1024
        if file_size > max_size:
            output_path.unlink()
            raise LakehouseError(
                f"JSONL 文件大小 {file_size / 1024 / 1024:.1f} MB 超过限制 (100 MB)。"
                f"请设置 etl.source.batch_size 减小每批数据量。"
            )

        logger.info("多模态 JSONL 已生成: %s (%d 条请求, %.1f MB)", output_path, len(lines), file_size / 1024 / 1024)
        return output_path

    # ── Volume 结果写入 ──────────────────────────────────────

    def write_volume_results(
        self,
        results: List[Dict[str, Any]],
        volume_name: str,
        file_metadata: Dict[str, Dict[str, Any]],
        target_table: Optional[str] = None,
        result_column: Optional[str] = None,
        write_mode: Optional[str] = None,
        provider_name: str = "",
        batch_id: str = "",
        include_metadata: bool = True,
    ) -> int:
        """将 Volume 推理结果写入目标表（含文件元数据）。

        Args:
            file_metadata: {relative_path: {size: int, ...}} 文件元数据映射。
        """
        from ai_etl.result_keys import (
            FILE_PATH, VOLUME_NAME, FILE_SIZE, VOLUME_COLUMNS, VOLUME_SQL_TYPES,
        )

        cfg = self._config
        target_table = target_table or cfg.etl_target_table
        result_column = result_column or cfg.etl_target_result_column
        write_mode = write_mode or cfg.etl_target_write_mode

        if not target_table:
            raise LakehouseError("未指定目标表名。")
        if not results:
            logger.warning("没有结果需要写入")
            return 0

        # 确保目标表存在（含 Volume 特有列）
        self._ensure_volume_table_exists(target_table, result_column, include_metadata)

        # 读取目标表 schema
        target_type_map = self._get_target_type_map(target_table)

        # 构建写入列
        write_cols = [FILE_PATH, VOLUME_NAME, FILE_SIZE, result_column]
        if include_metadata:
            self._ensure_metadata_columns(target_table)
            write_cols += METADATA_COLUMNS

        # 只保留目标表中存在的列
        if target_type_map:
            write_cols = [c for c in write_cols if c.lower() in target_type_map]

        # 构建 schema
        from clickzetta.zettapark.types import StringType, StructField, StructType
        struct_fields = []
        for col in write_cols:
            col_type = target_type_map.get(col.lower(), StringType())
            struct_fields.append(StructField(col, col_type, nullable=True))
        write_schema = StructType(struct_fields)

        # 构建数据行
        from datetime import datetime, timezone, timedelta
        _tz_cst = timezone(timedelta(hours=8))
        now_str = datetime.now(_tz_cst).strftime("%Y-%m-%dT%H:%M:%S+08:00")

        typed_rows = []
        for r in results:
            key_values = decode_custom_id(r.get(CUSTOM_ID, ""))
            file_path = key_values[0] if key_values else ""
            fmeta = file_metadata.get(file_path, {})

            row_dict: Dict[str, Any] = {
                FILE_PATH: file_path,
                VOLUME_NAME: volume_name,
                FILE_SIZE: fmeta.get("size", 0),
                result_column: r.get(CONTENT, ""),
            }

            if include_metadata:
                row_dict[MODEL] = r.get(MODEL, "")
                row_dict[PROVIDER] = provider_name
                row_dict[PROMPT_TOKENS] = r.get(PROMPT_TOKENS, 0)
                row_dict[COMPLETION_TOKENS] = r.get(COMPLETION_TOKENS, 0)
                row_dict[TOTAL_TOKENS] = r.get(TOTAL_TOKENS, 0)
                row_dict[BATCH_ID] = batch_id
                row_dict[PROCESSED_AT] = now_str
                row_dict[STATUS_CODE] = r.get(STATUS_CODE, 0)
                row_dict[FINISH_REASON] = r.get(FINISH_REASON, "")
                row_dict[RESPONSE_ID] = r.get(RESPONSE_ID, "")
                row_dict[SOURCE_TEXT] = cfg.etl_source_user_prompt  # user_prompt as source context
                row_dict[RAW_RESPONSE] = r.get(RAW_RESPONSE, "")

            typed_row = []
            for col in write_cols:
                val = row_dict.get(col, "")
                typed_row.append(self._cast_value(val, col, target_type_map))
            typed_rows.append(typed_row)

        df = self.session.create_dataframe(typed_rows, schema=write_schema)

        logger.info("写入目标表: %s (%d 行, mode=%s, Volume 模式)", target_table, len(typed_rows), write_mode)
        df.write.save_as_table(target_table, mode=write_mode)
        logger.info("写入完成")
        return len(typed_rows)

    def _ensure_volume_table_exists(
        self, target_table: str, result_column: str, include_metadata: bool
    ) -> None:
        """确保 Volume 目标表存在，不存在则自动创建。"""
        from ai_etl.result_keys import VOLUME_SQL_TYPES

        try:
            self.session.table(target_table).schema
            return
        except Exception:
            pass

        col_defs = [
            "file_path STRING",
            "volume_name STRING",
            "file_size BIGINT",
            f"{result_column} STRING",
        ]
        if include_metadata:
            for col_name, col_type in METADATA_SQL_TYPES.items():
                col_defs.append(f"{col_name} {col_type}")

        ddl = f"CREATE TABLE IF NOT EXISTS {target_table} (\n  {', '.join(col_defs)}\n)"
        logger.info("Volume 目标表不存在，自动创建: %s", target_table)
        self.session.sql(ddl).collect()

    # ── 构建 JSONL ────────────────────────────────────────────

    def build_jsonl_for_batch(
        self,
        rows: List[Dict[str, str]],
        key_columns: Optional[str] = None,
        text_column: Optional[str] = None,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        endpoint: str = "/v1/chat/completions",
        output_path: Optional[Union[str, Path]] = None,
    ) -> Path:
        """从源表数据构建 Batch 推理的 JSONL 输入文件。"""
        import time

        cfg = self._config
        key_columns = key_columns or cfg.etl_source_key_columns
        text_column = text_column or cfg.etl_source_text_column
        model = model or cfg.model_name
        system_prompt = system_prompt or cfg.system_prompt

        key_cols = [c.strip() for c in key_columns.split(",")]

        if output_path is None:
            output_path = Path(cfg.output_dir) / f"batch_input_{int(time.time())}.jsonl"
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        lines = []
        skipped_empty = 0
        skipped_oversized = 0
        seen_ids: set = set()

        for row in rows:
            key_values = [str(row.get(k, "")) for k in key_cols]
            custom_id = encode_custom_id(key_values)
            text_value = row.get(text_column, "")

            if not text_value or not text_value.strip():
                skipped_empty += 1
                continue

            if custom_id in seen_ids:
                logger.warning("跳过重复 custom_id: %s", custom_id)
                continue
            seen_ids.add(custom_id)

            record = {
                "custom_id": custom_id,
                "method": "POST",
                "url": endpoint,
                "body": {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": text_value},
                    ],
                },
            }
            line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))

            if len(line.encode("utf-8")) > 6 * 1024 * 1024:
                skipped_oversized += 1
                logger.warning("跳过超过 6 MB 限制的行 (custom_id=%s)", custom_id)
                continue

            lines.append(line)

        if skipped_empty:
            logger.warning("跳过 %d 条空文本行", skipped_empty)
        if skipped_oversized:
            logger.warning("跳过 %d 条超过 6 MB 限制的行", skipped_oversized)
        if not lines:
            raise LakehouseError("没有有效数据可构建 JSONL 文件（全部被跳过）")

        # 文件级校验：超过 Batch API 限制时报错，提示用户通过 batch_size 分批
        if len(lines) > 50_000:
            raise LakehouseError(
                f"请求数 {len(lines)} 超过 Batch API 单文件限制 (50,000)。"
                f"请在 config.yaml 中设置 etl.source.batch_size <= 50000 分批处理。"
            )

        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        file_size = output_path.stat().st_size
        max_size = 100 * 1024 * 1024  # 智谱限制 100 MB，取较严的
        if file_size > max_size:
            output_path.unlink()
            raise LakehouseError(
                f"JSONL 文件大小 {file_size / 1024 / 1024:.1f} MB 超过限制 (100 MB)。"
                f"请在 config.yaml 中设置 etl.source.batch_size 减小每批数据量。"
            )

        logger.info("JSONL 文件已生成: %s (%d 条请求, %.1f MB)", output_path, len(lines), file_size / 1024 / 1024)
        return output_path

    # ── 写入目标表 ────────────────────────────────────────────

    def write_results(
        self,
        results: List[Dict[str, Any]],
        key_columns: Optional[str] = None,
        target_table: Optional[str] = None,
        result_column: Optional[str] = None,
        write_mode: Optional[str] = None,
        provider_name: str = "",
        batch_id: str = "",
        source_rows: Optional[List[Dict[str, str]]] = None,
        include_metadata: bool = True,
    ) -> int:
        """将推理结果写入目标表。

        Args:
            results: parse_results() 返回的结果列表。
            key_columns: 主键字段，逗号分隔。
            target_table: 目标表名。
            result_column: 推理结果列名。
            write_mode: 写入模式 (append/overwrite)。
            provider_name: provider 名称。
            batch_id: 批量任务 ID。
            source_rows: 源表原始数据（用于回填 source_text），来自 read_source()。
            include_metadata: 是否写入元数据列。

        Returns:
            写入的行数。
        """
        cfg = self._config
        key_columns = key_columns or cfg.etl_source_key_columns
        target_table = target_table or cfg.etl_target_table
        result_column = result_column or cfg.etl_target_result_column
        write_mode = write_mode or cfg.etl_target_write_mode
        text_column = cfg.etl_source_text_column

        if not target_table:
            raise LakehouseError("未指定目标表名。请在 config.yaml 的 etl.target.table 中配置。")
        if not results:
            logger.warning("没有结果需要写入")
            return 0

        key_cols = [c.strip() for c in key_columns.split(",")]

        # 确保目标表存在（不存在则自动创建）
        self._ensure_table_exists(target_table, key_cols, result_column, include_metadata)

        # 确定写入列
        write_cols = key_cols + [result_column]
        if include_metadata:
            self._ensure_metadata_columns(target_table)
            write_cols += METADATA_COLUMNS

        # 读取目标表 schema 做类型匹配
        target_type_map = self._get_target_type_map(target_table)

        # 只保留目标表中实际存在的列
        if target_type_map:
            write_cols = [c for c in write_cols if c.lower() in target_type_map]

        # 构建 schema
        from clickzetta.zettapark.types import StringType, StructField, StructType
        struct_fields = []
        for col in write_cols:
            col_type = target_type_map.get(col.lower(), StringType())
            struct_fields.append(StructField(col, col_type, nullable=True))
        write_schema = StructType(struct_fields)

        # 构建数据行
        from datetime import datetime, timezone, timedelta
        _tz_cst = timezone(timedelta(hours=8))
        now_str = datetime.now(_tz_cst).strftime("%Y-%m-%dT%H:%M:%S+08:00")

        # 构建 source_text 查找表（标准化 key → 原始文本）
        source_text_map: Dict[str, str] = {}
        if source_rows and text_column:
            for row in source_rows:
                key_values = [str(row.get(k, "")) for k in key_cols]
                # 用标准化的 key（不含前缀），确保无论结果中 custom_id 有无前缀都能匹配
                normalized_key = "|".join(key_values)
                source_text_map[normalized_key] = row.get(text_column, "")

        typed_rows = []
        for r in results:
            key_values = decode_custom_id(r.get(CUSTOM_ID, ""))
            # 标准化 key 用于 source_text 匹配
            normalized_key = "|".join(key_values)

            row_dict: Dict[str, Any] = {}
            for i, col in enumerate(key_cols):
                row_dict[col] = key_values[i] if i < len(key_values) else ""
            row_dict[result_column] = r.get(CONTENT, "")

            if include_metadata:
                row_dict[MODEL] = r.get(MODEL, "")
                row_dict[PROVIDER] = provider_name
                row_dict[PROMPT_TOKENS] = r.get(PROMPT_TOKENS, 0)
                row_dict[COMPLETION_TOKENS] = r.get(COMPLETION_TOKENS, 0)
                row_dict[TOTAL_TOKENS] = r.get(TOTAL_TOKENS, 0)
                row_dict[BATCH_ID] = batch_id
                row_dict[PROCESSED_AT] = now_str
                row_dict[STATUS_CODE] = r.get(STATUS_CODE, 0)
                row_dict[FINISH_REASON] = r.get(FINISH_REASON, "")
                row_dict[RESPONSE_ID] = r.get(RESPONSE_ID, "")
                row_dict[SOURCE_TEXT] = source_text_map.get(normalized_key, "")
                row_dict[RAW_RESPONSE] = r.get(RAW_RESPONSE, "")

            typed_row = []
            for col in write_cols:
                val = row_dict.get(col, "")
                typed_row.append(self._cast_value(val, col, target_type_map))
            typed_rows.append(typed_row)

        df = self.session.create_dataframe(typed_rows, schema=write_schema)

        meta_hint = " (含元数据)" if include_metadata else ""
        logger.info(
            "写入目标表: %s (%d 行, mode=%s%s)",
            target_table, len(typed_rows), write_mode, meta_hint,
        )
        df.write.save_as_table(target_table, mode=write_mode)
        logger.info("写入完成")
        return len(typed_rows)

    # ── 内部工具方法 ──────────────────────────────────────────

    def _get_target_type_map(self, target_table: str) -> Dict[str, Any]:
        """读取目标表 schema，返回 {列名小写: datatype} 映射。"""
        try:
            schema = self.session.table(target_table).schema
            return {f.name.strip("`").lower(): f.datatype for f in schema.fields}
        except Exception:
            return {}

    @staticmethod
    def _cast_value(val: Any, col_name: str, type_map: Dict[str, Any]) -> Any:
        """将值转换为目标表期望的 Python 类型。"""
        from clickzetta.zettapark.types import StringType

        col_type = type_map.get(col_name.lower())
        if col_type is None or isinstance(col_type, StringType):
            return str(val) if val is not None else ""

        type_name = type(col_type).__name__.lower()
        try:
            if "int" in type_name or "long" in type_name:
                return int(val) if val != "" and val is not None else 0
            if "float" in type_name or "double" in type_name or "decimal" in type_name:
                return float(val) if val != "" and val is not None else 0.0
        except (ValueError, TypeError):
            return str(val)
        return str(val)

    def _ensure_table_exists(
        self,
        target_table: str,
        key_cols: List[str],
        result_column: str,
        include_metadata: bool,
    ) -> None:
        """确保目标表存在，不存在则自动创建。"""
        try:
            self.session.table(target_table).schema
            return  # 表已存在
        except Exception:
            pass

        # 读取源表 schema 推断主键列类型
        source_table = self._config.etl_source_table
        source_type_map: Dict[str, str] = {}
        if source_table:
            try:
                src_schema = self.session.table(source_table).schema
                for f in src_schema.fields:
                    name = f.name.strip("`").lower()
                    type_name = type(f.datatype).__name__
                    # 映射 ZettaPark 类型名到 SQL 类型
                    if "long" in type_name.lower():
                        source_type_map[name] = "BIGINT"
                    elif "int" in type_name.lower():
                        source_type_map[name] = "INT"
                    elif "double" in type_name.lower() or "float" in type_name.lower():
                        source_type_map[name] = "DOUBLE"
                    elif "decimal" in type_name.lower():
                        source_type_map[name] = "DECIMAL(18,2)"
                    else:
                        source_type_map[name] = "STRING"
            except Exception:
                pass

        # 构建 CREATE TABLE
        col_defs = []
        for col in key_cols:
            sql_type = source_type_map.get(col.lower(), "STRING")
            col_defs.append(f"{col} {sql_type}")
        col_defs.append(f"{result_column} STRING")

        if include_metadata:
            for col_name, col_type in METADATA_SQL_TYPES.items():
                col_defs.append(f"{col_name} {col_type}")

        ddl = f"CREATE TABLE IF NOT EXISTS {target_table} (\n  {','.join(col_defs)}\n)"
        logger.info("目标表不存在，自动创建: %s", target_table)
        logger.debug("DDL: %s", ddl)
        self.session.sql(ddl).collect()

    def _ensure_metadata_columns(self, target_table: str) -> None:
        """确保目标表包含元数据列，不存在则自动添加。"""
        try:
            schema = self.session.table(target_table).schema
            existing = {f.name.strip("`").lower() for f in schema.fields}
        except Exception:
            return

        for col_name, col_type in METADATA_SQL_TYPES.items():
            if col_name.lower() not in existing:
                try:
                    self.session.sql(
                        f"ALTER TABLE {target_table} ADD COLUMN {col_name} {col_type}"
                    ).collect()
                    logger.info("已为目标表添加元数据列: %s %s", col_name, col_type)
                except Exception as e:
                    logger.debug("添加列 %s 失败（可能已存在）: %s", col_name, e)

    def close(self) -> None:
        if self._session is not None:
            try:
                self._session.close()
                logger.info("ClickZetta Session 已关闭")
            except Exception as e:
                logger.warning("关闭 Session 时出错: %s", e)
            self._session = None
