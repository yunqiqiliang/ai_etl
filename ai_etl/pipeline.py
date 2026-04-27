"""
AI ETL 流水线：Lakehouse 读取 → 批量推理 → 结果写回。

支持多 Provider（DashScope、ZhipuAI），通过 config.yaml 的 provider 字段切换。
双数据源（table + volume）并行提交 batch，统一轮询等待。
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient
from ai_etl.providers import create_provider, BatchProvider

logger = logging.getLogger(__name__)

_API_KEY_ENV_MAP = {
    "dashscope": "DASHSCOPE_API_KEY",
    "zhipuai": "ZHIPUAI_API_KEY",
}

_TERMINAL_STATUSES = frozenset({"completed", "failed", "expired", "cancelled"})


@dataclass
class _BatchJob:
    """一个已提交的 batch 任务的上下文，用于并行等待。"""
    source_type: str               # "table" or "volume"
    batch_id: str
    provider: BatchProvider
    model: str
    target_table: str
    source_rows: int
    provider_name: str = ""
    # table 模式
    key_columns: str = ""
    result_column: str = ""
    rows: List[Dict[str, str]] = field(default_factory=list)
    # volume 模式
    volume_display: str = ""
    files_with_urls: List[Dict[str, Any]] = field(default_factory=list)


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

    # ── Provider 解析 ─────────────────────────────────────────

    def _resolve_provider(self, provider_name: Optional[str] = None) -> BatchProvider:
        cfg = self._config
        name = provider_name or cfg.provider_name
        env_var = _API_KEY_ENV_MAP.get(name, f"{name.upper()}_API_KEY")
        api_key = os.getenv(env_var, "")
        if not api_key:
            raise RuntimeError(f"未找到 {name} 的 API Key，请在 .env 中配置 {env_var}")
        provider_cfg = cfg.get_provider_config(name)
        return create_provider(
            name=name,
            api_key=api_key,
            model=provider_cfg.get("model") or cfg.model_name,
            poll_interval=float(provider_cfg.get("poll_interval") or cfg.poll_interval),
            completion_window=str(provider_cfg.get("completion_window") or cfg.completion_window),
        )

    # ── 主入口 ────────────────────────────────────────────────

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

        单源模式：显式指定 source_type 时只运行该类型。
        双源模式：两个都 enabled 时并行提交 batch，统一轮询等待。
        """
        cfg = self._config

        # 显式指定 source_type → 单源模式
        if source_type:
            if source_type == "volume":
                job = self._submit_volume(
                    volume_name=volume_name, file_types=file_types,
                    target_table=target_table, result_column=result_column,
                    model=model, system_prompt=system_prompt, provider_name=provider_name,
                )
                if job is None:
                    return self._make_stats(0, cfg.provider_name, "")
                self._wait_and_collect([job])
                return job.stats if job.stats else self._make_stats(0, cfg.provider_name, "")
            else:
                job = self._submit_table(
                    source_table=source_table, key_columns=key_columns,
                    text_column=text_column, filter_expr=filter_expr,
                    target_table=target_table, result_column=result_column,
                    model=model, system_prompt=system_prompt, provider_name=provider_name,
                )
                if job is None:
                    return self._make_stats(0, cfg.provider_name, "")
                self._wait_and_collect([job])
                return job.stats if job.stats else self._make_stats(0, cfg.provider_name, "")

        # 检查启用的 source
        table_enabled = cfg.etl_table_enabled
        volume_enabled = cfg.etl_volume_enabled
        if not table_enabled and not volume_enabled:
            raise RuntimeError(
                "未启用任何数据源。请在 config.yaml 中设置 "
                "etl.sources.table.enabled 或 etl.sources.volume.enabled 为 true。"
            )

        # ── 提交阶段：快速提交所有 batch ──────────────────────
        jobs: List[_BatchJob] = []

        if table_enabled:
            logger.info("=" * 60)
            logger.info("[提交] Table 数据源")
            logger.info("=" * 60)
            job = self._submit_table(
                source_table=source_table, key_columns=key_columns,
                text_column=text_column, filter_expr=filter_expr,
                target_table=target_table, result_column=result_column,
                model=model, system_prompt=system_prompt, provider_name=provider_name,
            )
            if job:
                jobs.append(job)

        if volume_enabled:
            logger.info("=" * 60)
            logger.info("[提交] Volume 数据源")
            logger.info("=" * 60)
            job = self._submit_volume(
                volume_name=volume_name, file_types=file_types,
                target_table=target_table, result_column=result_column,
                model=model, system_prompt=system_prompt, provider_name=provider_name,
            )
            if job:
                jobs.append(job)

        if not jobs:
            logger.info("所有数据源均无数据需要处理")
            return self._make_stats(0, cfg.provider_name, "")

        # ── 等待阶段：并行轮询所有 batch ──────────────────────
        self._wait_and_collect(jobs)

        # ── 汇总 ─────────────────────────────────────────────
        all_stats = [j.stats for j in jobs if j.stats]
        if len(all_stats) == 1:
            return all_stats[0]

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
        }

    # ── 提交阶段 ──────────────────────────────────────────────

    def _submit_table(
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
    ) -> Optional[_BatchJob]:
        """Table 提交阶段：读取 → 构建 JSONL → 上传 → 创建 batch。返回 job 或 None。"""
        cfg = self._config
        target_table = target_table or cfg.resolve_table_target()
        key_columns = key_columns or cfg.etl_table_key_columns
        text_column = text_column or cfg.etl_table_text_column
        result_column = result_column or cfg.etl_target_result_column
        provider_name = provider_name or cfg.provider_name

        provider = self._resolve_provider(provider_name)
        resolved_model = model or cfg.resolve_model("table")
        resolved_prompt = system_prompt or cfg.etl_table_system_prompt
        endpoint = provider.build_jsonl_endpoint()

        # Step 1: 读取源数据（增量：自动跳过目标表已有的 key）
        logger.info("[Table] 读取源数据...")
        rows = self._lakehouse.read_source(
            table=source_table or cfg.etl_table_name,
            key_columns=key_columns,
            text_column=text_column,
            filter_expr=filter_expr or cfg.etl_table_filter,
            batch_size=cfg.etl_table_batch_size,
            target_table=target_table,
        )
        logger.info("[Table] 读取到 %d 行", len(rows))

        if not rows:
            logger.warning("[Table] 源表无数据，跳过")
            return None

        # Step 2: 构建 JSONL → 上传 → 创建 batch
        logger.info("[Table] 构建 JSONL 并提交...")
        transform_params = cfg.get_transform_params("table")
        if transform_params:
            logger.info("[Table] 推理参数: %s", transform_params)
        jsonl_path = self._lakehouse.build_jsonl_for_batch(
            rows, key_columns=key_columns, text_column=text_column,
            model=resolved_model, system_prompt=resolved_prompt, endpoint=endpoint,
            transform_params=transform_params,
        )
        file_id = provider.upload_file(jsonl_path)
        if not file_id:
            raise RuntimeError("[Table] 文件上传失败，未获得 file_id。请检查 API Key 和网络连接。")
        batch_id = provider.create_batch(
            file_id, task_name=f"ai-etl-{source_table or cfg.etl_table_name}",
        )
        if not batch_id:
            raise RuntimeError("[Table] 创建 batch 失败，未获得 batch_id。请检查模型名称和 API 配额。")
        if jsonl_path.exists():
            jsonl_path.unlink()

        logger.info("[Table] batch 已提交: %s", batch_id)

        # 持久化状态
        self._save_batch_state({
            "batch_id": batch_id, "provider": provider.name,
            "model": resolved_model, "source_type": "table",
            "target_table": target_table, "source_rows": len(rows),
        })

        return _BatchJob(
            source_type="table", batch_id=batch_id, provider=provider,
            model=resolved_model, target_table=target_table,
            source_rows=len(rows), provider_name=provider.name,
            key_columns=key_columns, result_column=result_column, rows=rows,
        )

    def _submit_volume(
        self,
        volume_name: Optional[str] = None,
        file_types: Optional[list] = None,
        target_table: Optional[str] = None,
        result_column: Optional[str] = None,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        provider_name: Optional[str] = None,
    ) -> Optional[_BatchJob]:
        """Volume 提交阶段：发现文件 → URL → JSONL → 上传 → 创建 batch。返回 job 或 None。"""
        cfg = self._config
        target_table = target_table or cfg.resolve_volume_target()
        result_column = result_column or cfg.etl_target_result_column
        provider_name = provider_name or cfg.provider_name

        try:
            volume_sql_ref = cfg.get_volume_sql_ref()
        except ValueError as e:
            raise RuntimeError(str(e))

        volume_display = cfg.etl_volume_name or cfg.etl_volume_type

        provider = self._resolve_provider(provider_name)
        resolved_model = model or cfg.resolve_model("volume")
        resolved_prompt = system_prompt or cfg.etl_volume_system_prompt
        endpoint = provider.build_jsonl_endpoint()

        # Step 1: 发现新文件
        logger.info("[Volume] 发现新文件...")
        files = self._lakehouse.discover_volume_files(
            volume_sql_ref=volume_sql_ref,
            file_types=file_types or cfg.etl_volume_file_types,
            subdirectory=cfg.etl_volume_subdirectory,
            target_table=target_table,
            batch_size=cfg.etl_volume_batch_size,
        )
        if not files:
            logger.info("[Volume] 没有新文件需要处理")
            return None

        logger.info("[Volume] 发现 %d 个新文件", len(files))

        # Step 2: 生成预签名 URL
        logger.info("[Volume] 生成预签名 URL...")
        files_with_urls = self._lakehouse.generate_presigned_urls(
            files, volume_sql_ref=volume_sql_ref, expiration=cfg.etl_volume_url_expiration,
        )
        if not files_with_urls:
            logger.warning("[Volume] 所有文件的预签名 URL 生成失败")
            return None

        # Step 3: 构建 JSONL → 上传 → 创建 batch
        logger.info("[Volume] 构建多模态 JSONL 并提交...")
        transform_params = cfg.get_transform_params("volume")
        if transform_params:
            logger.info("[Volume] 推理参数: %s", transform_params)
        jsonl_path = self._lakehouse.build_multimodal_jsonl(
            files_with_urls, model=resolved_model,
            user_prompt=cfg.etl_volume_user_prompt,
            system_prompt=resolved_prompt,
            provider_name=provider.name, endpoint=endpoint,
            transform_params=transform_params,
        )
        file_id = provider.upload_file(jsonl_path)
        if not file_id:
            raise RuntimeError("[Volume] 文件上传失败，未获得 file_id。请检查 API Key 和网络连接。")
        batch_id = provider.create_batch(
            file_id, task_name=f"ai-etl-volume-{volume_display}",
        )
        if not batch_id:
            raise RuntimeError("[Volume] 创建 batch 失败，未获得 batch_id。请检查模型名称和 API 配额。")
        if jsonl_path.exists():
            jsonl_path.unlink()

        logger.info("[Volume] batch 已提交: %s", batch_id)

        # 持久化状态
        self._save_batch_state({
            "batch_id": batch_id, "provider": provider.name,
            "model": resolved_model, "source_type": "volume",
            "target_table": target_table, "volume_ref": volume_sql_ref,
            "discovered_files": len(files), "files_with_urls": len(files_with_urls),
        })

        return _BatchJob(
            source_type="volume", batch_id=batch_id, provider=provider,
            model=resolved_model, target_table=target_table,
            source_rows=len(files), provider_name=provider.name,
            result_column=result_column,
            volume_display=volume_display, files_with_urls=files_with_urls,
        )

    # ── 并行等待 + 收集结果 ───────────────────────────────────

    def _wait_and_collect(self, jobs: List[_BatchJob]) -> None:
        """并行轮询所有 batch 任务，完成后下载结果并写入目标表。"""
        cfg = self._config
        provider_cfg = cfg.get_provider_config(cfg.provider_name)
        poll_interval = float(provider_cfg.get("poll_interval") or cfg.poll_interval)

        pending = {j.batch_id: j for j in jobs}
        start_time = time.time()

        logger.info("等待 %d 个 batch 任务完成 (轮询间隔 %.0fs)...", len(pending), poll_interval)

        while pending:
            done_ids = []
            for batch_id, job in pending.items():
                try:
                    info = job.provider.get_batch_status(batch_id)
                except Exception as e:
                    logger.warning("[%s] 查询状态失败: %s", job.source_type, e)
                    continue

                status = info["status"]
                counts = info.get("request_counts") or {}
                total = counts.get("total", 0)
                completed = counts.get("completed", 0)
                failed = counts.get("failed", 0)

                elapsed = time.time() - start_time
                elapsed_str = f"{elapsed / 60:.0f}min" if elapsed > 60 else f"{elapsed:.0f}s"

                progress = ""
                if total > 0:
                    pct = (completed + failed) / total * 100
                    progress = f" ({completed}/{total} done, {failed} failed, {pct:.0f}%)"

                logger.info("[%s] %s: %s%s [%s]",
                            job.source_type, batch_id[:20], status, progress, elapsed_str)

                if status in _TERMINAL_STATUSES:
                    done_ids.append(batch_id)
                    if status == "completed":
                        self._collect_results(job)
                    elif status == "failed":
                        logger.error(
                            "[%s] batch 任务失败 (batch_id=%s)。"
                            "可能原因: 模型名称错误、API 配额不足、JSONL 格式问题。"
                            "用 'python -m ai_etl status %s' 查看详情。",
                            job.source_type, batch_id, batch_id,
                        )
                        job.stats = self._make_stats(
                            job.source_rows, job.provider_name, job.model,
                            batch_id=batch_id, batch_status="failed",
                        )
                    else:
                        logger.error(
                            "[%s] batch 任务终止 (status=%s, batch_id=%s)。",
                            job.source_type, status, batch_id,
                        )
                        job.stats = self._make_stats(
                            job.source_rows, job.provider_name, job.model,
                            batch_id=batch_id, batch_status=status,
                        )

            for bid in done_ids:
                del pending[bid]

            if pending:
                time.sleep(poll_interval)

    def _collect_results(self, job: _BatchJob) -> None:
        """下载 batch 结果并写入目标表。"""
        cfg = self._config
        output_dir = Path(cfg.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # 下载结果
        result_text = job.provider.download_results(job.batch_id)
        results = BatchProvider.parse_results(result_text) if result_text else []

        if result_text:
            suffix = f"_{job.source_type}" if job.source_type else ""
            (output_dir / f"result{suffix}.jsonl").write_text(result_text, encoding="utf-8")

        error_text = job.provider.download_errors(job.batch_id)
        errors = BatchProvider.parse_errors(error_text) if error_text else []
        if error_text:
            suffix = f"_{job.source_type}" if job.source_type else ""
            (output_dir / f"error{suffix}.jsonl").write_text(error_text, encoding="utf-8")

        success_count = len(results)
        error_count = len(errors)
        logger.info("[%s] 推理完成: %d 成功, %d 失败", job.source_type, success_count, error_count)

        if error_count > 0:
            logger.warning(
                "[%s] %d 条请求推理失败，详见 output/error_%s.jsonl",
                job.source_type, error_count, job.source_type,
            )

        # 写入目标表
        written = 0
        if not results:
            logger.warning(
                "[%s] batch 已完成但没有成功的推理结果。"
                "所有 %d 条请求均失败，请检查 error 文件。",
                job.source_type, error_count,
            )
        else:
            if job.source_type == "volume":
                file_metadata = {f["relative_path"]: f for f in job.files_with_urls}
                written = self._lakehouse.write_volume_results(
                    results,
                    volume_name=job.volume_display,
                    file_metadata=file_metadata,
                    target_table=job.target_table,
                    result_column=job.result_column,
                    provider_name=job.provider_name,
                    batch_id=job.batch_id,
                )
            else:
                written = self._lakehouse.write_results(
                    results,
                    key_columns=job.key_columns,
                    target_table=job.target_table,
                    result_column=job.result_column,
                    provider_name=job.provider_name,
                    batch_id=job.batch_id,
                    source_rows=job.rows,
                    include_metadata=True,
                )

        logger.info("[%s] 写入 %d 行到 %s", job.source_type, written, job.target_table)

        job.stats = self._make_stats(
            job.source_rows, job.provider_name, job.model,
            batch_id=job.batch_id, batch_status="completed",
            success_count=success_count, error_count=error_count,
            written_rows=written,
        )

    # ── Resume ────────────────────────────────────────────────

    def resume(self, batch_id: str, provider_name: Optional[str] = None) -> Dict[str, Any]:
        """恢复中断的任务：查询 batch 结果并写入目标表。"""
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

        batch_state = self._load_batch_state(batch_id)
        source_type = batch_state.get("source_type", "table")
        saved_target = batch_state.get("target_table", "")

        if source_type == "volume":
            target_table = saved_target or cfg.resolve_volume_target()
            volume_display = batch_state.get("volume_ref", cfg.etl_volume_name or cfg.etl_volume_type)
            written = self._lakehouse.write_volume_results(
                results, volume_name=volume_display, file_metadata={},
                target_table=target_table, provider_name=provider.name, batch_id=batch_id,
            )
        else:
            target_table = saved_target or cfg.resolve_table_target()
            written = self._lakehouse.write_results(
                results, target_table=target_table,
                provider_name=provider.name, batch_id=batch_id, include_metadata=True,
            )

        return self._make_stats(
            0, provider.name, "",
            batch_id=batch_id, batch_status="completed",
            success_count=len(results), written_rows=written,
        )

    # ── 工具方法 ──────────────────────────────────────────────

    def _save_batch_state(self, state: Dict[str, Any]) -> None:
        output_dir = Path(self._config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "last_batch.json").write_text(
            json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8",
        )

    def _load_batch_state(self, batch_id: str) -> Dict[str, Any]:
        state_file = Path(self._config.output_dir) / "last_batch.json"
        if not state_file.exists():
            return {}
        try:
            state = json.loads(state_file.read_text(encoding="utf-8"))
            if state.get("batch_id") == batch_id:
                return state
        except Exception:
            pass
        return {}

    @staticmethod
    def _make_stats(
        source_rows: int = 0, provider: str = "", model: str = "", **kwargs,
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
        }

    def close(self) -> None:
        self._lakehouse.close()
