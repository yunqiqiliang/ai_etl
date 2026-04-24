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
from typing import Any, Dict, Optional

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
        """执行完整的 AI ETL 流水线。所有参数可选，未指定时从 config.yaml 读取。"""
        cfg = self._config
        key_columns = key_columns or cfg.etl_source_key_columns
        text_column = text_column or cfg.etl_source_text_column
        provider_name = provider_name or cfg.provider_name

        provider = self._resolve_provider(provider_name)
        resolved_model, resolved_prompt = self._resolve_model_and_prompt(
            provider.name, model, system_prompt
        )
        endpoint = provider.build_jsonl_endpoint()

        logger.info("=" * 60)
        logger.info("AI ETL 流水线启动 (provider=%s, model=%s)", provider.name, resolved_model)
        logger.info("=" * 60)

        # ── Step 1: 从 Lakehouse 读取源数据 ──────────────────
        logger.info("[Step 1/4] 从 Lakehouse 读取源数据...")
        rows = self._lakehouse.read_source(
            table=source_table,
            key_columns=key_columns,
            text_column=text_column,
            filter_expr=filter_expr,
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
            task_name=f"ai-etl-{source_table or cfg.etl_source_table}",
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

    def resume(self, batch_id: str, provider_name: Optional[str] = None) -> Dict[str, Any]:
        """从 Step 3 恢复：查询已有 batch 的结果并写入目标表。

        用于 pipeline 中断后恢复，无需重新读取源表和提交推理。
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

        written = self._lakehouse.write_results(
            results,
            provider_name=provider.name,
            include_metadata=True,
        )

        return self._make_stats(
            0, provider.name, "",
            batch_id=batch_id, batch_status="completed",
            success_count=len(results), written_rows=written,
        )

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
