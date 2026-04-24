"""
配置加载模块。

加载优先级（高 → 低）：
  1. 代码参数（直接传入函数）
  2. 环境变量（.env 文件或系统环境变量）
  3. config.yaml 配置文件
  4. 内置默认值
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional


def _find_project_root() -> Path:
    """从当前工作目录向上查找包含 config.yaml 的目录。"""
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        if (parent / "config.yaml").exists():
            return parent
    return cwd


def load_dotenv(env_path: Optional[Path] = None) -> None:
    """加载 .env 文件到环境变量（不覆盖已有值）。

    简单实现，不依赖 python-dotenv 库。
    支持格式：KEY=VALUE，# 注释行，空行。
    """
    if env_path is None:
        env_path = _find_project_root() / ".env"

    if not env_path.exists():
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # 去除引号
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            # 不覆盖已有环境变量
            if key not in os.environ:
                os.environ[key] = value


def load_yaml_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """加载 YAML 配置文件。

    使用简单的 YAML 解析（仅支持本项目用到的子集），
    避免引入 PyYAML 依赖。如果已安装 PyYAML 则优先使用。
    """
    if config_path is None:
        config_path = _find_project_root() / "config.yaml"

    if not config_path.exists():
        return {}

    text = config_path.read_text(encoding="utf-8")

    # 优先使用 PyYAML
    try:
        import yaml
        return yaml.safe_load(text) or {}
    except ImportError:
        pass

    # 简易 YAML 解析（支持两层嵌套 + 字符串/数字/null/布尔）
    return _simple_yaml_parse(text)


def _simple_yaml_parse(text: str) -> Dict[str, Any]:
    """极简 YAML 解析器，支持三层嵌套。"""
    result: Dict[str, Any] = {}
    section_stack: list = []  # [(indent_level, key), ...]

    for line in text.split("\n"):
        # 去掉注释
        if "#" in line:
            comment_pos = line.index("#")
            in_quote = False
            for ch in line[:comment_pos]:
                if ch in ('"', "'"):
                    in_quote = not in_quote
            if not in_quote:
                line = line[:comment_pos]

        stripped = line.rstrip()
        if not stripped or not stripped.strip():
            continue

        # 计算缩进级别
        indent = len(stripped) - len(stripped.lstrip())
        content = stripped.strip()

        if ":" not in content:
            continue

        key, _, value = content.partition(":")
        key = key.strip()
        value = value.strip()

        # 弹出缩进级别 >= 当前的栈项
        while section_stack and section_stack[-1][0] >= indent:
            section_stack.pop()

        if value:
            # 有值的 key
            parsed_val = _parse_value(value)
            _set_nested(result, [s[1] for s in section_stack] + [key], parsed_val)
        else:
            # 无值的 key（子节点容器）
            _set_nested(result, [s[1] for s in section_stack] + [key], {})
            section_stack.append((indent, key))

    return result


def _set_nested(d: Dict[str, Any], keys: list, value: Any) -> None:
    """在嵌套字典中设置值。"""
    for k in keys[:-1]:
        if k not in d or not isinstance(d[k], dict):
            d[k] = {}
        d = d[k]
    d[keys[-1]] = value


def _parse_value(value: str) -> Any:
    """解析 YAML 值。"""
    if not value:
        return None

    # 去除引号
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        return value[1:-1]

    # null
    if value.lower() in ("null", "~"):
        return None

    # 布尔
    if value.lower() in ("true", "yes"):
        return True
    if value.lower() in ("false", "no"):
        return False

    # 数字
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        pass

    return value


class Config:
    """统一配置对象。

    加载 .env + config.yaml，提供类型安全的访问方法。

    用法::

        cfg = Config()
        cfg.api_key        # → str (from .env)
        cfg.model_name     # → str (from config.yaml)
        cfg.poll_interval  # → float (from config.yaml)
    """

    def __init__(
        self,
        config_path: Optional[Path] = None,
        env_path: Optional[Path] = None,
    ) -> None:
        load_dotenv(env_path)
        self._yaml = load_yaml_config(config_path)

    def _get(self, section: str, key: str, default: Any = None) -> Any:
        """从 YAML 配置中获取值。支持两层嵌套（section.key）。"""
        sec = self._yaml.get(section, {})
        if isinstance(sec, dict):
            val = sec.get(key)
            if val is not None:
                return val
        return default

    def _get_nested(self, *keys: str, default: Any = None) -> Any:
        """从 YAML 配置中获取多层嵌套值。如 _get_nested("etl", "source", "table")。"""
        obj: Any = self._yaml
        for k in keys:
            if isinstance(obj, dict):
                obj = obj.get(k)
            else:
                return default
            if obj is None:
                return default
        return obj

    # ── Provider 选择 ────────────────────────────────────────

    @property
    def provider_name(self) -> str:
        """当前选择的 provider: dashscope, zhipuai。"""
        return str(self._yaml.get("provider", "dashscope"))

    # ── API 凭证 ──────────────────────────────────────────────

    @property
    def api_key(self) -> Optional[str]:
        """当前 provider 的 API Key。"""
        p = self.provider_name
        if p == "zhipuai":
            return os.getenv("ZHIPUAI_API_KEY")
        return os.getenv("DASHSCOPE_API_KEY")

    @property
    def dashscope_api_key(self) -> Optional[str]:
        return os.getenv("DASHSCOPE_API_KEY")

    @property
    def zhipuai_api_key(self) -> Optional[str]:
        return os.getenv("ZHIPUAI_API_KEY")

    # ── DashScope 服务 ────────────────────────────────────────

    @property
    def region(self) -> str:
        return self._get("dashscope", "region", "beijing")

    @property
    def base_url(self) -> Optional[str]:
        return self._get("dashscope", "base_url")

    # ── 模型（provider-aware） ────────────────────────────────

    @property
    def model_name(self) -> str:
        """当前 provider 的默认模型。"""
        p = self.provider_name
        val = self._get(p, "model")
        if val:
            return val
        return self._get("model", "name", "qwen3.5-flash")

    @property
    def system_prompt(self) -> str:
        p = self.provider_name
        val = self._get(p, "system_prompt")
        if val:
            return val
        return self._get("model", "system_prompt", "You are a helpful assistant.")

    @property
    def endpoint(self) -> str:
        p = self.provider_name
        val = self._get(p, "endpoint")
        if val:
            return val
        return self._get("model", "endpoint", "/v1/chat/completions")

    def get_provider_config(self, provider: str) -> Dict[str, Any]:
        """获取指定 provider 的完整配置段。"""
        return self._yaml.get(provider, {}) if isinstance(self._yaml.get(provider), dict) else {}

    # ── 批量任务 ──────────────────────────────────────────────

    @property
    def completion_window(self) -> str:
        return str(self._get("batch", "completion_window", "24h"))

    @property
    def poll_interval(self) -> float:
        return float(self._get("batch", "poll_interval", 300.0))

    @property
    def timeout(self) -> Optional[float]:
        val = self._get("batch", "timeout")
        return float(val) if val is not None else None

    # ── 输出 ──────────────────────────────────────────────────

    @property
    def output_dir(self) -> str:
        return self._get("output", "dir", "output")

    @property
    def result_file(self) -> str:
        return self._get("output", "result_file", "result.jsonl")

    @property
    def error_file(self) -> str:
        return self._get("output", "error_file", "error.jsonl")

    # ── ClickZetta Lakehouse ──────────────────────────────────

    @property
    def cz_service(self) -> str:
        return os.getenv("CLICKZETTA_SERVICE") or self._get("clickzetta", "service", "")

    @property
    def cz_instance(self) -> str:
        return os.getenv("CLICKZETTA_INSTANCE") or self._get("clickzetta", "instance", "")

    @property
    def cz_workspace(self) -> str:
        return os.getenv("CLICKZETTA_WORKSPACE") or self._get("clickzetta", "workspace", "")

    @property
    def cz_schema(self) -> str:
        return os.getenv("CLICKZETTA_SCHEMA") or self._get("clickzetta", "schema", "public")

    @property
    def cz_username(self) -> Optional[str]:
        return os.getenv("CLICKZETTA_USERNAME")

    @property
    def cz_password(self) -> Optional[str]:
        return os.getenv("CLICKZETTA_PASSWORD")

    @property
    def cz_vcluster(self) -> str:
        return os.getenv("CLICKZETTA_VCLUSTER") or self._get("clickzetta", "vcluster", "default_ap")

    @property
    def cz_sdk_job_timeout(self) -> int:
        return int(self._get("clickzetta", "sdk_job_timeout", 300))

    def get_clickzetta_config(self) -> Dict[str, Any]:
        """构建 ZettaPark Session 所需的连接参数字典。"""
        cfg: Dict[str, Any] = {}
        if self.cz_service:
            cfg["service"] = self.cz_service
        if self.cz_instance:
            cfg["instance"] = self.cz_instance
        if self.cz_workspace:
            cfg["workspace"] = self.cz_workspace
        if self.cz_username:
            cfg["username"] = self.cz_username
        if self.cz_password:
            cfg["password"] = self.cz_password
        cfg["schema"] = self.cz_schema
        cfg["vcluster"] = self.cz_vcluster
        cfg["sdk_job_timeout"] = self.cz_sdk_job_timeout
        return cfg

    # ── ETL 流水线 ────────────────────────────────────────────

    @property
    def etl_source_table(self) -> str:
        return self._get_nested("etl", "source", "table", default="")

    @property
    def etl_source_key_columns(self) -> str:
        return self._get_nested("etl", "source", "key_columns", default="id")

    @property
    def etl_source_text_column(self) -> str:
        return self._get_nested("etl", "source", "text_column", default="content")

    @property
    def etl_source_filter(self) -> Optional[str]:
        return self._get_nested("etl", "source", "filter", default=None)

    @property
    def etl_source_batch_size(self) -> int:
        return int(self._get_nested("etl", "source", "batch_size", default=0))

    @property
    def etl_target_table(self) -> str:
        return self._get_nested("etl", "target", "table", default="")

    @property
    def etl_target_result_column(self) -> str:
        return self._get_nested("etl", "target", "result_column", default="ai_result")

    @property
    def etl_target_write_mode(self) -> str:
        return self._get_nested("etl", "target", "write_mode", default="append")
