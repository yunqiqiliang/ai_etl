"""
AI ETL 流水线：Lakehouse 读取 → 批量推理 → 结果写回。

支持多 Provider（DashScope、ZhipuAI），通过 config.yaml 的 provider 字段切换。
目标表写入包含元数据：模型名称、token 消耗等。
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient
from ai_etl.providers import create_provider, BatchProvider

logger = logging.getLogger(__name__)

# Provider 名称 → 环境变量名
_API_KEY_ENV_MAP = {
    "dashscope": "DASHSCOPE_API_KEY",
    "zhipuai": "ZHIPUAI_API_KEY",
}


class AIETLPipeline:
    """AI ETL 流水线。

    用法::

        pipeline = AIETLPipeline()
        stats = pipeline.run()
        pipeline.close()
    """

    def __init__(self, config: Optional[Config] = None) -> None:
        self._config = config or Config()
        self._lakehouse = LakehouseClient(config=self._config)

    def _resolve_provider(self, provider_name: Optional[str] = None) -> BatchProvider:
        """根据 provider 名称创建实例，自动从对应 config section 读取参数。"""
        cfg = self._config
        name = provider_name or cfg.provider_name

        env_var = _API_KEY_ENV_MAP.get(name, f"{name.upper()}_API_KEY")
        api_key = os.getenv(env_var, "")
        if not api_key:
            raise RuntimeError(f"未找到 {name} 的 API Key，请在 .env 中配置 {env_var}")

        # 从对应 provider 的 config section 读取参数
        provider_cfg = cfg.get_provider_config(name)
        model = provider_cfg.get("model") or cfg.model_name
        poll_interval = float(provider_cfg.get("poll_interval") or cfg.poll_interval)
        completion_window = str(provider_cfg.get("completion_window") or cfg.completion_window)

        return create_provider(
            name=name,
            api_key=api_key,
            model=model,
            poll_interval=poll_interval,
            completion_window=completion_window,
        )

    def _resolve_model_and_prompt(
        self,
        provider_name: str,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
    ) -> tuple:
        """解析 model 和 system_prompt，优先级：参数 > provider section > 全局默认。"""
        cfg = self._config
        provider_cfg = cfg.get_provider_config(provider_name)

        resolved_model = model or provider_cfg.get("model") or cfg.model_name
        resolved_prompt = system_prompt or provider_cfg.get("system_prompt") or cfg.system_prompt

        return resolved_model, resolved_prompt

    def run(
        self,
        source_type: Optional[str] = None,
        source_table: Optional[str] = None,
        key_columns: Optional[str] = None,
        text_column: Optional[str] = None,
        filter_expr: Optional[str] = None,
        target_table: Optional[str] = None,
        result_column: Optional[str] = None,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        provider_name: Optional[str] = None,
        volume_name: Optional[str] = None,
        file_types: Optional[list] = None,
    ) -> Dict[str, Any]:
        """执行 AI ETL 流水线。

        支持三种模式：
        1. 显式指定 source_type → 只运行该类型
        2. config 中 etl.sources.table.enabled / etl.sources.volume.enabled → 运行启用的
        3. 两个都启用 → 依次运行 table 和 volume，合并统计
        """
        cfg = self._config

        # 如果显式指定了 source_type，只运行该类型
        if source_type:
            if source_type == "volume":
                return self._run_volume(
                    volume_name=volume_name, file_types=file_types,
                    target_table=target_table, result_column=result_column,
                    model=model, system_prompt=system_prompt, provider_name=provider_name,
                )
            else:
                return self._run_table(
                    source_table=source_table, key_columns=key_columns,
                    text_column=text_column, filter_expr=filter_expr,
                    target_table=target_table, result_column=result_column,
                    model=model, system_prompt=system_prompt, provider_name=provider_name,
                )

        # 检查 config 中启用了哪些 source
        table_enabled = cfg.etl_table_enabled
        volume_enabled = cfg.etl_volume_enabled

        # 如果都没启用，回退到旧的 source_type 字段
        if not table_enabled and not volume_enabled:
            old_type = cfg.etl_source_type
            if old_type == "volume":
                volume_enabled = True
            else:
                table_enabled = True

        all_stats: List[Dict[str, Any]] = []

        if table_enabled:
            logger.info("=" * 60)
            logger.info("运行 Table 数据源")
            logger.info("=" * 60)
            stats = self._run_table(
                source_table=source_table, key_columns=key_columns,
                text_column=text_column, filter_expr=filter_expr,
                target_table=target_table, result_column=result_column,
                model=model, system_prompt=system_prompt, provider_name=provider_name,
            )
            all_stats.append({"source": "table", **stats})

        if volume_enabled:
            logger.info("=" * 60)
            logger.info("运行 Volume 数据源")
            logger.info("=" * 60)
            stats = self._run_volume(
                volume_name=volume_name, file_types=file_types,
                target_table=target_table, result_column=result_column,
                model=model, system_prompt=system_prompt, provider_name=provider_name,
            )
            all_stats.append({"source": "volume", **stats})

        # 如果只运行了一个，直接返回
        if len(all_stats) == 1:
            return all_stats[0]

        # 合并统计
        total_source = sum(s.get("source_rows", 0) for s in all_stats)
        total_success = sum(s.get("success_count", 0) for s in all_stats)
        total_errors = sum(s.get("error_count", 0) for s in all_stats)
        total_written = sum(s.get("written_rows", 0) for s in all_stats)

        logger.info("=" * 60)
        logger.info("双数据源 ETL 完成: %d 源数据 → %d 成功 / %d 失败 → %d 写入",
                     total_source, total_success, total_errors, total_written)
        logger.info("=" * 60)

        return {
            "sources": all_stats,
            "source_rows": total_source,
            "success_count": total_success,
            "error_count": total_errors,
            "written_rows": total_written,
            "provider": all_stats[0].get("provider"),
            "model": None,
            "batch_id": None,
            "batch_status": "completed",
            "output_file": None,
        }

    def _run_table(
        self,
        source_table: Optional[str] = None,
        key_columns: Optional[str] = None,
        text_column: Optional[str] = None,
        filter_expr: Optional[str] = None,
        target_table: Optional[str] = None,
        result_column: Optional[str] = None,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        provider_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Table ETL 流程。"""
        cfg = self._config
        target_table = target_table or cfg.resolve_table_target()
        key_columns = key_columns or cfg.etl_table_key_columns
        text_column = text_column or cfg.etl_table_text_column
        provider_name = provider_name or cfg.provider_name

        provider = self._resolve_provider(provider_name)
        resolved_model = model or cfg.resolve_model("table")
        resolved_prompt = system_prompt or cfg.etl_table_system_prompt
        endpoint = provider.build_jsonl_endpoint()

        logger.info("=" * 60)
        logger.info("AI ETL 流水线启动 (provider=%s, model=%s)", provider.name, resolved_model)
        logger.info("=" * 60)

        # ── Step 1: 从 Lakehouse 读取源数据 ──────────────────
        logger.info("[Step 1/4] 从 Lakehouse 读取源数据...")
        rows = self._lakehouse.read_source(
            table=source_table or cfg.etl_table_name,
            key_columns=key_columns,
            text_column=text_column,
            filter_expr=filter_expr or cfg.etl_table_filter,
            batch_size=cfg.etl_table_batch_size,
        )
        logger.info("读取到 %d 行数据", len(rows))

        if not rows:
            logger.warning("源表无数据，流水线结束")
            return self._make_stats(0, provider.name, resolved_model)

        # ── Step 2: 构建 JSONL 并提交批量推理 ────────────────
        logger.info("[Step 2/4] 构建 JSONL 并提交批量推理...")
        jsonl_path = self._lakehouse.build_jsonl_for_batch(
            rows,
            key_columns=key_columns,
            text_column=text_column,
            model=resolved_model,
            system_prompt=resolved_prompt,
            endpoint=endpoint,
        )

        file_id = provider.upload_file(jsonl_path)
        batch_id = provider.create_batch(
            file_id,
            task_name=f"ai-etl-{source_table or cfg.etl_table_name}",
        )

        # ── Step 3: 等待完成并下载结果 ───────────────────────
        logger.info("[Step 3/4] 等待批量推理完成...")
        output_dir = Path(cfg.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / cfg.result_file
        error_file = output_dir / cfg.error_file

        # 持久化 batch_id，支持断点续查
        state_file = output_dir / "last_batch.json"
        state_file.write_text(json.dumps({
            "batch_id": batch_id,
            "provider": provider.name,
            "model": resolved_model,
            "source_rows": len(rows),
        }, ensure_ascii=False, indent=2))

        final_status = provider.wait_for_completion(batch_id)

        result_text = provider.download_results(batch_id)
        results = BatchProvider.parse_results(result_text) if result_text else []
        if result_text:
            output_file.write_text(result_text, encoding="utf-8")

        error_text = provider.download_errors(batch_id)
        errors = BatchProvider.parse_errors(error_text) if error_text else []
        if error_text:
            error_file.write_text(error_text, encoding="utf-8")

        success_count = len(results)
        error_count = len(errors)
        logger.info("推理完成: %d 成功, %d 失败", success_count, error_count)

        # 清理临时 JSONL
        if jsonl_path.exists():
            jsonl_path.unlink()

        # ── Step 4: 写入目标表 ───────────────────────────────
        logger.info("[Step 4/4] 将推理结果写入 Lakehouse 目标表...")
        if not results:
            logger.warning("没有成功的推理结果，跳过写入")
            written = 0
        else:
            written = self._lakehouse.write_results(
                results,
                key_columns=key_columns,
                target_table=target_table,
                result_column=result_column,
                provider_name=provider.name,
                batch_id=batch_id,
                source_rows=rows,
                include_metadata=True,
            )

        if error_count > 0:
            logger.warning("有 %d 条请求推理失败，详见: %s", error_count, error_file)

        logger.info("=" * 60)
        logger.info("AI ETL 流水线完成")
        logger.info(
            "  源表: %d 行 → 推理: %d 成功 / %d 失败 → 写入: %d 行",
            len(rows), success_count, error_count, written,
        )
        logger.info("=" * 60)

        return self._make_stats(
            len(rows), provider.name, resolved_model,
            batch_id=batch_id, batch_status=final_status,
            success_count=success_count, error_count=error_count,
            written_rows=written, output_file=str(output_file),
        )

    def _run_volume(
        self,
        volume_name: Optional[str] = None,
        file_types: Optional[list] = None,
        target_table: Optional[str] = None,
        result_column: Optional[str] = None,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        provider_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Volume ETL 流程：发现文件 → 生成 URL → 构建 JSONL → 提交 → 写回。"""
        cfg = self._config
        target_table = target_table or cfg.resolve_volume_target()
        provider_name = provider_name or cfg.provider_name

        # 生成 Volume SQL 引用
        try:
            volume_sql_ref = cfg.get_volume_sql_ref()
        except ValueError as e:
            raise RuntimeError(str(e))

        # 用于显示和元数据的 volume 标识
        volume_display = cfg.etl_volume_name or cfg.etl_volume_type

        if not volume_sql_ref:
            raise RuntimeError(
                "Volume 配置不完整。请在 config.yaml 的 etl.sources.volume 中配置 volume_type 和 volume_name。"
            )

        provider = self._resolve_provider(provider_name)
        resolved_model = model or cfg.resolve_model("volume")
        resolved_prompt = system_prompt or cfg.etl_volume_system_prompt
        endpoint = provider.build_jsonl_endpoint()

        logger.info("=" * 60)
        logger.info("AI ETL Volume 流水线启动 (provider=%s, model=%s, volume=%s)",
                     provider.name, resolved_model, volume_sql_ref)
        logger.info("=" * 60)

        # Step 1: 发现新文件
        logger.info("[Step 1/5] 发现 Volume 中的新文件...")
        files = self._lakehouse.discover_volume_files(
            volume_sql_ref=volume_sql_ref,
            file_types=file_types or cfg.etl_volume_file_types,
            subdirectory=cfg.etl_volume_subdirectory,
            target_table=target_table,
            batch_size=cfg.etl_volume_batch_size,
        )

        if not files:
            logger.info("没有新文件需要处理")
            return self._make_stats(0, provider.name, resolved_model)

        logger.info("发现 %d 个新文件", len(files))

        # Step 2: 生成预签名 URL
        logger.info("[Step 2/5] 生成预签名 URL...")
        url_expiration = cfg.etl_volume_url_expiration
        files_with_urls = self._lakehouse.generate_presigned_urls(
            files, volume_sql_ref=volume_sql_ref, expiration=url_expiration,
        )

        if not files_with_urls:
            logger.warning("所有文件的预签名 URL 生成失败")
            return self._make_stats(len(files), provider.name, resolved_model)

        # URL 过期警告
        try:
            from ai_etl.providers.base import _parse_completion_window_hours
            cw_hours = _parse_completion_window_hours(cfg.completion_window)
            if url_expiration < cw_hours * 3600:
                logger.warning(
                    "预签名 URL 有效期 (%ds = %.1fh) 短于 completion_window (%s)，"
                    "URL 可能在批量任务完成前过期",
                    url_expiration, url_expiration / 3600, cfg.completion_window,
                )
        except Exception:
            pass

        # Step 3: 构建多模态 JSONL
        logger.info("[Step 3/5] 构建多模态 JSONL...")
        jsonl_path = self._lakehouse.build_multimodal_jsonl(
            files_with_urls,
            model=resolved_model,
            user_prompt=cfg.etl_volume_user_prompt,
            system_prompt=resolved_prompt,
            provider_name=provider.name,
            endpoint=endpoint,
        )

        # Step 4: 提交并等待
        logger.info("[Step 4/5] 提交批量推理...")
        file_id = provider.upload_file(jsonl_path)
        batch_id = provider.create_batch(
            file_id, task_name=f"ai-etl-volume-{volume_display}",
        )

        # 持久化状态
        output_dir = Path(cfg.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        state_file = output_dir / "last_batch.json"
        state_file.write_text(json.dumps({
            "batch_id": batch_id,
            "provider": provider.name,
            "model": resolved_model,
            "source_type": "volume",
            "volume_ref": volume_sql_ref,
            "discovered_files": len(files),
            "files_with_urls": len(files_with_urls),
        }, ensure_ascii=False, indent=2))

        logger.info("等待批量推理完成...")
        final_status = provider.wait_for_completion(batch_id)

        result_text = provider.download_results(batch_id)
        results = BatchProvider.parse_results(result_text) if result_text else []

        output_file = output_dir / cfg.result_file
        if result_text:
            output_file.write_text(result_text, encoding="utf-8")

        error_text = provider.download_errors(batch_id)
        errors = BatchProvider.parse_errors(error_text) if error_text else []
        if error_text:
            (output_dir / cfg.error_file).write_text(error_text, encoding="utf-8")

        success_count = len(results)
        error_count = len(errors)
        logger.info("推理完成: %d 成功, %d 失败", success_count, error_count)

        if jsonl_path.exists():
            jsonl_path.unlink()

        # Step 5: 写入目标表
        logger.info("[Step 5/5] 写入目标表...")
        if not results:
            logger.warning("没有成功的推理结果，跳过写入")
            written = 0
        else:
            file_metadata = {f["relative_path"]: f for f in files_with_urls}
            written = self._lakehouse.write_volume_results(
                results,
                volume_name=volume_display,
                file_metadata=file_metadata,
                target_table=target_table,
                result_column=result_column,
                provider_name=provider.name,
                batch_id=batch_id,
            )

        logger.info("=" * 60)
        logger.info("AI ETL Volume 流水线完成")
        logger.info(
            "  文件: %d 发现 / %d URL / %d 成功 / %d 失败 → 写入: %d 行",
            len(files), len(files_with_urls), success_count, error_count, written,
        )
        logger.info("=" * 60)

        return self._make_stats(
            len(files), provider.name, resolved_model,
            batch_id=batch_id, batch_status=final_status,
            success_count=success_count, error_count=error_count,
            written_rows=written, output_file=str(output_file),
        )

    def resume(self, batch_id: str, provider_name: Optional[str] = None) -> Dict[str, Any]:
        """从 Step 3 恢复：查询已有 batch 的结果并写入目标表。

        用于 pipeline 中断后恢复，无需重新读取源表和提交推理。
        自动检测 last_batch.json 判断是 table 还是 volume 模式。
        """
        cfg = self._config
        provider_name = provider_name or cfg.provider_name
        provider = self._resolve_provider(provider_name)

        logger.info("恢复任务: batch_id=%s, provider=%s", batch_id, provider.name)

        info = provider.get_batch_status(batch_id)
        if info["status"] != "completed":
            logger.info("任务尚未完成 (status=%s)，等待中...", info["status"])
            provider.wait_for_completion(batch_id)

        result_text = provider.download_results(batch_id)
        results = BatchProvider.parse_results(result_text) if result_text else []

        if not results:
            logger.warning("没有成功的推理结果")
            return self._make_stats(0, provider.name, "")

        # 读取 last_batch.json 判断原始任务类型
        batch_state = self._load_batch_state(batch_id)
        source_type = batch_state.get("source_type", "table")

        if source_type == "volume":
            volume_display = batch_state.get("volume_ref", cfg.etl_volume_name or cfg.etl_volume_type)
            written = self._lakehouse.write_volume_results(
                results,
                volume_name=volume_display,
                file_metadata={},  # resume 时无文件元数据，file_size 会为 0
                provider_name=provider.name,
                batch_id=batch_id,
            )
        else:
            written = self._lakehouse.write_results(
                results,
                provider_name=provider.name,
                batch_id=batch_id,
                include_metadata=True,
            )

        return self._make_stats(
            0, provider.name, "",
            batch_id=batch_id, batch_status="completed",
            success_count=len(results), written_rows=written,
        )

    def _load_batch_state(self, batch_id: str) -> Dict[str, Any]:
        """从 output/last_batch.json 加载批次状态信息。"""
        state_file = Path(self._config.output_dir) / "last_batch.json"
        if not state_file.exists():
            return {}
        try:
            state = json.loads(state_file.read_text(encoding="utf-8"))
            # 只返回匹配当前 batch_id 的状态
            if state.get("batch_id") == batch_id:
                return state
        except Exception:
            pass
        return {}

    @staticmethod
    def _make_stats(
        source_rows: int = 0,
        provider: str = "",
        model: str = "",
        **kwargs,
    ) -> Dict[str, Any]:
        return {
            "source_rows": source_rows,
            "provider": provider or None,
            "model": model or None,
            "batch_id": kwargs.get("batch_id"),
            "batch_status": kwargs.get("batch_status", "skipped"),
            "success_count": kwargs.get("success_count", 0),
            "error_count": kwargs.get("error_count", 0),
            "written_rows": kwargs.get("written_rows", 0),
            "output_file": kwargs.get("output_file"),
        }

    def close(self) -> None:
        self._lakehouse.close()
