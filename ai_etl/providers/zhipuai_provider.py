"""ZhipuAI (智谱) Batch Provider。

使用智谱原生 SDK（zhipuai），因为文件上传接口不兼容 OpenAI SDK 的认证方式。

限制：
  - 文件大小: ≤ 100 MB
  - 请求数: ≤ 50,000
  - custom_id: ≥ 6 字符
  - completion_window: 已废弃（系统自动调度）
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from ai_etl.providers.base import BatchProvider, ProviderConfig, retry_api_call

logger = logging.getLogger(__name__)


class ZhipuAIProvider(BatchProvider):
    """智谱 AI Batch 推理 Provider（使用原生 zhipuai SDK）。"""

    def __init__(self, config: ProviderConfig) -> None:
        super().__init__(config)
        try:
            from zhipuai import ZhipuAI
        except ImportError:
            raise RuntimeError(
                "请安装智谱 SDK: pip install zhipuai"
            )
        self._client = ZhipuAI(api_key=config.api_key)

    def upload_file(self, file_path: Path) -> str:
        logger.info("[zhipuai] 上传文件: %s", file_path)
        obj = retry_api_call(
            self._client.files.create,
            file=open(file_path, "rb"),
            purpose="batch",
        )
        logger.info("[zhipuai] file_id=%s", obj.id)
        return obj.id

    def create_batch(
        self, input_file_id: str,
        task_name: Optional[str] = None,
        task_description: Optional[str] = None,
    ) -> str:
        metadata: Dict[str, str] = {}
        if task_name:
            metadata["description"] = task_name[:512]
        if task_description:
            metadata["project"] = task_description[:512]

        kwargs: Dict[str, Any] = {
            "input_file_id": input_file_id,
            "endpoint": self.config.endpoint,
            "completion_window": "24h",
            "auto_delete_input_file": False,
        }
        if metadata:
            kwargs["metadata"] = metadata

        batch = retry_api_call(self._client.batches.create, **kwargs)
        logger.info("[zhipuai] batch_id=%s, status=%s", batch.id, batch.status)
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
        # zhipuai SDK 返回 HttpxBinaryResponseContent，需要读取 text
        if hasattr(content, "text"):
            return content.text
        # 兼容：写到临时文件再读
        import tempfile
        tmp = Path(tempfile.mktemp(suffix=".jsonl"))
        content.write_to_file(str(tmp))
        text = tmp.read_text(encoding="utf-8")
        tmp.unlink()
        return text

    def download_errors(self, batch_id: str) -> Optional[str]:
        batch = retry_api_call(self._client.batches.retrieve, batch_id)
        if not batch.error_file_id:
            return None
        content = retry_api_call(self._client.files.content, batch.error_file_id)
        if hasattr(content, "text"):
            return content.text
        import tempfile
        tmp = Path(tempfile.mktemp(suffix=".jsonl"))
        content.write_to_file(str(tmp))
        text = tmp.read_text(encoding="utf-8")
        tmp.unlink()
        return text

    def cancel_batch(self, batch_id: str) -> str:
        batch = retry_api_call(self._client.batches.cancel, batch_id)
        return batch.status
