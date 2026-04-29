"""Batch Provider 抽象基类。"""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = frozenset({"completed", "failed", "expired", "cancelled"})

MAX_API_RETRIES = 5
RETRY_BASE_DELAY = 2.0
RETRY_MAX_DELAY = 60.0


@dataclass
class ProviderConfig:
    """Provider 配置。"""
    name: str                    # provider 名称: dashscope, zhipuai
    api_key: str = ""
    base_url: str = ""
    default_model: str = ""
    endpoint: str = ""           # JSONL 中的 url 字段
    max_requests_per_file: int = 50_000
    max_file_size_mb: int = 500
    max_line_size_mb: int = 6
    completion_window: str = "24h"
    poll_interval: float = 300.0
    extra: Dict[str, Any] = field(default_factory=dict)


def retry_api_call(func, *args, max_retries: int = MAX_API_RETRIES, **kwargs):
    """带指数退避的 API 调用重试。

    4xx 客户端错误（认证失败、请求格式错误等）不重试，直接抛出。
    5xx 服务端错误、限流、网络错误才重试。
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            err_str = str(e).lower()

            # 检查 HTTP 状态码：4xx 客户端错误不重试
            status_code = getattr(e, "status_code", None) or getattr(e, "code", None)
            if status_code is not None:
                try:
                    code = int(status_code)
                    if 400 <= code < 500 and code != 429:
                        # 429 是限流，需要重试；其他 4xx 是客户端错误，不重试
                        raise
                except (ValueError, TypeError):
                    pass

            is_retryable = any(kw in err_str for kw in [
                "rate limit", "429", "500", "502", "503", "504",
                "timeout", "connection", "reset", "broken pipe",
            ])
            if not is_retryable or attempt == max_retries:
                raise
            delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), RETRY_MAX_DELAY)
            logger.warning(
                "API 调用失败 (第 %d/%d 次), %.1fs 后重试: %s",
                attempt, max_retries, delay, e,
            )
            time.sleep(delay)
    raise last_exc  # type: ignore


class BatchProvider(ABC):
    """Batch 推理 Provider 抽象基类。

    所有 provider（DashScope、ZhipuAI 等）实现此接口。
    """

    def __init__(self, config: ProviderConfig) -> None:
        self.config = config

    @property
    def name(self) -> str:
        return self.config.name

    @abstractmethod
    def upload_file(self, file_path: Path) -> str:
        """上传 JSONL 文件，返回 file_id。"""

    @abstractmethod
    def create_batch(
        self,
        input_file_id: str,
        task_name: Optional[str] = None,
        task_description: Optional[str] = None,
    ) -> str:
        """创建批量任务，返回 batch_id。"""

    @abstractmethod
    def get_batch_status(self, batch_id: str) -> Dict[str, Any]:
        """查询任务状态。"""

    @abstractmethod
    def download_results(self, batch_id: str) -> str:
        """下载成功结果，返回 JSONL 文本。"""

    @abstractmethod
    def download_errors(self, batch_id: str) -> Optional[str]:
        """下载错误详情，返回 JSONL 文本或 None。"""

    def cancel_batch(self, batch_id: str) -> str:
        """取消任务（可选实现）。"""
        raise NotImplementedError(f"{self.name} 不支持取消任务")

    def download_results_and_errors(self, batch_id: str):
        """一次调用同时获取结果和错误文件（默认实现：分两次调用，子类可覆盖优化）。

        Returns:
            (result_text, error_text): 结果 JSONL 文本和错误 JSONL 文本（无则为空字符串/None）。
        """
        result_text = self.download_results(batch_id)
        error_text = self.download_errors(batch_id)
        return result_text, error_text

    # ── 通用方法 ──────────────────────────────────────────────

    def wait_for_completion(
        self,
        batch_id: str,
        poll_interval: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> str:
        """轮询等待任务完成（通用实现）。"""
        poll_interval = poll_interval or self.config.poll_interval
        start = time.time()
        last_completed = 0
        last_progress_time = start

        while True:
            try:
                info = self.get_batch_status(batch_id)
            except Exception as e:
                logger.warning("查询状态失败，将继续重试: %s", e)
                time.sleep(poll_interval)
                continue

            status = info["status"]
            counts = info.get("request_counts") or {}
            elapsed = time.time() - start

            # 进度信息
            total = counts.get("total", 0)
            completed = counts.get("completed", 0)
            failed = counts.get("failed", 0)
            progress = ""
            if total > 0:
                done = completed + failed
                pct = done / total * 100
                progress = f" ({completed}/{total} 完成, {failed} 失败, {pct:.0f}%)"

                if done > last_completed:
                    rate = (done - last_completed) / max(time.time() - last_progress_time, 1)
                    remaining = total - done
                    if rate > 0:
                        eta = remaining / rate
                        progress += f" ETA ~{eta / 60:.0f}min" if eta > 60 else f" ETA ~{eta:.0f}s"
                    last_completed = done
                    last_progress_time = time.time()

            elapsed_str = f"{elapsed / 60:.0f}min" if elapsed > 60 else f"{elapsed:.0f}s"
            logger.info("[%s] 任务状态: %s%s [已等待 %s]", self.name, status, progress, elapsed_str)

            if status in _TERMINAL_STATUSES:
                if status == "completed":
                    return status
                if status == "failed":
                    raise RuntimeError(
                        f"[{self.name}] 批量任务失败 (batch_id={batch_id})。"
                        f"可能原因: 模型不支持 batch、API Key 无权限、请求格式错误。"
                        f"详情: {info}"
                    )
                if status == "expired":
                    raise RuntimeError(
                        f"[{self.name}] 批量任务已过期 (batch_id={batch_id})。"
                        f"任务未在 completion_window 内完成。"
                        f"建议: 减小 batch_size 或增大 completion_window 后重试。"
                    )
                raise RuntimeError(
                    f"[{self.name}] 批量任务终止 (batch_id={batch_id}, status={status})。"
                )

            if timeout is not None and elapsed > timeout:
                raise TimeoutError(
                    f"客户端等待超时 ({timeout}s), 当前状态: {status}。"
                    f"任务仍在服务端运行 (batch_id={batch_id})。"
                )

            time.sleep(poll_interval)

    def build_jsonl_endpoint(self) -> str:
        """返回此 provider 的 JSONL url 字段值。"""
        return self.config.endpoint

    @staticmethod
    def parse_results(text: str) -> List[Dict[str, Any]]:
        """解析 JSONL 结果文本为统一格式。

        从 response body 中尽可能多地提取元数据，不同模型返回的字段可能不同，
        缺失的字段用默认值填充。完整的 response body 保存在 raw_response 中兜底。
        """
        from ai_etl.result_keys import (
            COMPLETION_TOKENS, CONTENT, CUSTOM_ID, FINISH_REASON, MODEL,
            PROMPT_TOKENS, RAW, RAW_RESPONSE, RESPONSE_ID, STATUS_CODE,
            TOTAL_TOKENS, USAGE,
        )

        results = []
        for line_num, line in enumerate(text.strip().split("\n"), start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("结果文件第 %d 行 JSON 解析失败，已跳过", line_num)
                continue

            response = record.get("response", {})
            body = response.get("body", {})
            choices = body.get("choices", [])

            # 提取回复内容和 finish_reason
            content = ""
            finish_reason = ""
            if choices:
                first_choice = choices[0]
                message = first_choice.get("message", {})
                content = message.get("content", "")
                finish_reason = first_choice.get("finish_reason", "")

            # 提取 usage（不同模型字段可能不同）
            usage = body.get("usage") or {}
            prompt_tokens = usage.get("prompt_tokens", 0)
            completion_tokens = usage.get("completion_tokens", 0)
            total_tokens = usage.get("total_tokens", 0)

            # 服务端请求 ID（DashScope 在 response.request_id，智谱在 body.id 或 body.request_id）
            response_id = (
                response.get("request_id")
                or body.get("request_id")
                or body.get("id")
                or record.get("id")
                or ""
            )

            # 完整 response body 序列化为 JSON 字符串（兜底，不丢信息）
            raw_response = json.dumps(body, ensure_ascii=False, separators=(",", ":")) if body else ""

            results.append({
                CUSTOM_ID: record.get("custom_id"),
                CONTENT: content,
                MODEL: body.get("model", ""),
                USAGE: usage,
                PROMPT_TOKENS: prompt_tokens,
                COMPLETION_TOKENS: completion_tokens,
                TOTAL_TOKENS: total_tokens,
                STATUS_CODE: response.get("status_code", 0),
                FINISH_REASON: finish_reason,
                RESPONSE_ID: response_id,
                RAW_RESPONSE: raw_response,
                RAW: record,
            })
        return results

    @staticmethod
    def parse_errors(text: str) -> List[Dict[str, Any]]:
        """解析错误文件。"""
        from ai_etl.result_keys import CUSTOM_ID

        errors = []
        for line_num, line in enumerate(text.strip().split("\n"), start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("错误文件第 %d 行 JSON 解析失败，已跳过", line_num)
                continue
            error_info = record.get("error") or {}
            errors.append({
                CUSTOM_ID: record.get("custom_id"),
                "error_code": error_info.get("code"),
                "error_message": error_info.get("message"),
                "raw": record,
            })
        return errors
