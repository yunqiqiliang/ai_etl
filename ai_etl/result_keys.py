"""
统一的结果字段常量和 custom_id 编解码工具。

所有模块通过这里的常量引用字段名，避免硬编码字符串散落各处。
"""

from __future__ import annotations

from typing import List

# ── 解析后的结果字典 key ──────────────────────────────────────

CUSTOM_ID = "custom_id"
CONTENT = "content"
MODEL = "model"
PROVIDER = "provider"
USAGE = "usage"
PROMPT_TOKENS = "prompt_tokens"
COMPLETION_TOKENS = "completion_tokens"
TOTAL_TOKENS = "total_tokens"
STATUS_CODE = "status_code"
RAW = "raw"

# ── 写入目标表时附加的元数据字段 ──────────────────────────────

BATCH_ID = "batch_id"
PROCESSED_AT = "processed_at"
SOURCE_TEXT = "source_text"
FINISH_REASON = "finish_reason"       # stop / length / content_filter 等
RESPONSE_ID = "response_id"           # 服务端返回的请求级唯一 ID
RAW_RESPONSE = "raw_response"         # 完整 response body JSON（兜底，不丢信息）

# 元数据列列表（写入目标表时按此顺序追加）
METADATA_COLUMNS = [
    MODEL, PROVIDER,
    PROMPT_TOKENS, COMPLETION_TOKENS, TOTAL_TOKENS,
    BATCH_ID, PROCESSED_AT, STATUS_CODE,
    FINISH_REASON, RESPONSE_ID,
    SOURCE_TEXT, RAW_RESPONSE,
]

# 元数据列对应的 SQL 类型
METADATA_SQL_TYPES = {
    MODEL: "STRING",
    PROVIDER: "STRING",
    PROMPT_TOKENS: "INT",
    COMPLETION_TOKENS: "INT",
    TOTAL_TOKENS: "INT",
    BATCH_ID: "STRING",
    PROCESSED_AT: "STRING",
    STATUS_CODE: "INT",
    FINISH_REASON: "STRING",
    RESPONSE_ID: "STRING",
    SOURCE_TEXT: "STRING",
    RAW_RESPONSE: "STRING",
}

# ── custom_id 编解码 ──────────────────────────────────────────

_CUSTOM_ID_PREFIX = "req-"


def encode_custom_id(key_values: List[str]) -> str:
    """将主键值列表编码为 custom_id。"""
    raw = "|".join(str(v) for v in key_values)
    return f"{_CUSTOM_ID_PREFIX}{raw}"


def decode_custom_id(custom_id: str) -> List[str]:
    """将 custom_id 解码为主键值列表。"""
    raw = custom_id
    if raw.startswith(_CUSTOM_ID_PREFIX):
        raw = raw[len(_CUSTOM_ID_PREFIX):]
    return raw.split("|")


def normalize_custom_id(custom_id: str) -> str:
    """将 custom_id 标准化为不含前缀的原始值，用于匹配。

    无论输入是 "req-1001" 还是 "1001"，都返回 "1001"。
    """
    if custom_id.startswith(_CUSTOM_ID_PREFIX):
        return custom_id[len(_CUSTOM_ID_PREFIX):]
    return custom_id
