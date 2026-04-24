"""DashScope (阿里云百炼) Batch Provider。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from openai import OpenAI

from ai_etl.providers.base import BatchProvider, ProviderConfig, retry_api_call

logger = logging.getLogger(__name__)


class DashScopeProvider(BatchProvider):
    """阿里云百炼 Batch 推理 Provider。"""

    def __init__(self, config: ProviderConfig) -> None:
        super().__init__(config)
        self._client = OpenAI(api_key=config.api_key, base_url=config.base_url)

    def upload_file(self, file_path: Path) -> str:
        logger.info("[dashscope] 上传文件: %s", file_path)
        obj = retry_api_call(self._client.files.create, file=file_path, purpose="batch")
        logger.info("[dashscope] file_id=%s", obj.id)
        return obj.id

    def create_batch(
        self, input_file_id: str,
        task_name: Optional[str] = None,
        task_description: Optional[str] = None,
    ) -> str:
        metadata: Dict[str, str] = {}
        if task_name:
            metadata["ds_name"] = task_name[:100]
        if task_description:
            metadata["ds_description"] = task_description[:200]

        kwargs: Dict[str, Any] = {
            "input_file_id": input_file_id,
            "endpoint": self.config.endpoint,
            "completion_window": self.config.completion_window,
        }
        if metadata:
            kwargs["metadata"] = metadata

        batch = retry_api_call(self._client.batches.create, **kwargs)
        logger.info("[dashscope] batch_id=%s, status=%s", batch.id, batch.status)
        return batch.id

    def get_batch_status(self, batch_id: str) -> Dict[str, Any]:
        batch = retry_api_call(self._client.batches.retrieve, batch_id)
        counts = None
        if batch.request_counts:
            counts = {
                "total": batch.request_counts.total,
                "completed": batch.request_counts.completed,
                "failed": batch.request_counts.failed,
            }
        return {
            "id": batch.id,
            "status": batch.status,
            "request_counts": counts,
            "output_file_id": batch.output_file_id,
            "error_file_id": batch.error_file_id,
        }

    def download_results(self, batch_id: str) -> str:
        batch = retry_api_call(self._client.batches.retrieve, batch_id)
        if not batch.output_file_id:
            return ""
        content = retry_api_call(self._client.files.content, batch.output_file_id)
        return content.text

    def download_errors(self, batch_id: str) -> Optional[str]:
        batch = retry_api_call(self._client.batches.retrieve, batch_id)
        if not batch.error_file_id:
            return None
        content = retry_api_call(self._client.files.content, batch.error_file_id)
        return content.text

    def cancel_batch(self, batch_id: str) -> str:
        batch = retry_api_call(self._client.batches.cancel, batch_id)
        return batch.status
