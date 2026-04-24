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
        from datetime import datetime, timezone
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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
