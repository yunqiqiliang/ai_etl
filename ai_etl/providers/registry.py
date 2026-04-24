"""Provider 注册表。"""

from __future__ import annotations

from typing import Dict, Type

from ai_etl.providers.base import BatchProvider, ProviderConfig

_REGISTRY: Dict[str, Type[BatchProvider]] = {}

# Provider 默认配置
_DEFAULTS = {
    "dashscope": {
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "default_model": "qwen3.5-flash",
        "endpoint": "/v1/chat/completions",
        "max_file_size_mb": 500,
        "completion_window": "24h",
    },
    "zhipuai": {
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "default_model": "glm-4-flash",
        "endpoint": "/v4/chat/completions",
        "max_file_size_mb": 100,
        "completion_window": "24h",
    },
}


def _ensure_registered():
    """延迟注册内置 provider。"""
    if _REGISTRY:
        return
    from ai_etl.providers.dashscope_provider import DashScopeProvider
    from ai_etl.providers.zhipuai_provider import ZhipuAIProvider
    _REGISTRY["dashscope"] = DashScopeProvider
    _REGISTRY["zhipuai"] = ZhipuAIProvider


def create_provider(
    name: str,
    api_key: str,
    model: str = "",
    **overrides,
) -> BatchProvider:
    """根据名称创建 Provider 实例。

    Args:
        name: provider 名称 (dashscope, zhipuai)
        api_key: API Key
        model: 模型名称（覆盖默认值）
        **overrides: 覆盖 ProviderConfig 中的其他字段
    """
    _ensure_registered()
    name = name.lower()
    if name not in _REGISTRY:
        raise ValueError(f"未知 provider: {name}，可选: {list(_REGISTRY.keys())}")

    defaults = _DEFAULTS.get(name, {})
    config = ProviderConfig(
        name=name,
        api_key=api_key,
        base_url=overrides.get("base_url", defaults.get("base_url", "")),
        default_model=model or defaults.get("default_model", ""),
        endpoint=overrides.get("endpoint", defaults.get("endpoint", "")),
        max_requests_per_file=overrides.get("max_requests_per_file", 50_000),
        max_file_size_mb=overrides.get("max_file_size_mb", defaults.get("max_file_size_mb", 500)),
        max_line_size_mb=overrides.get("max_line_size_mb", 6),
        completion_window=overrides.get("completion_window", defaults.get("completion_window", "24h")),
        poll_interval=overrides.get("poll_interval", 300.0),
    )

    cls = _REGISTRY[name]
    return cls(config)


def list_providers() -> list:
    """列出所有可用的 provider 名称。"""
    _ensure_registered()
    return list(_REGISTRY.keys())
