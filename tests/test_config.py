"""Tests for Config Volume source properties."""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_etl.config import Config


def _make_config(yaml_text: str) -> Config:
    """Create a Config from a YAML string."""
    tmp = Path(tempfile.mktemp(suffix=".yaml"))
    tmp.write_text(yaml_text, encoding="utf-8")
    env = Path(tempfile.mktemp(suffix=".env"))
    env.write_text("# empty\n", encoding="utf-8")
    try:
        return Config(config_path=tmp, env_path=env)
    finally:
        pass  # files cleaned up by OS


def test_source_type_default():
    cfg = _make_config("etl:\n  source:\n    table: t")
    assert cfg.etl_source_type == "table"


def test_source_type_volume():
    cfg = _make_config('etl:\n  source:\n    source_type: "volume"\n    volume_name: "my_vol"')
    assert cfg.etl_source_type == "volume"
    assert cfg.etl_source_volume_name == "my_vol"


def test_file_types_list():
    cfg = _make_config('etl:\n  sources:\n    volume:\n      file_types: [".jpg", ".png"]')
    assert cfg.etl_volume_file_types == [".jpg", ".png"]


def test_file_types_string():
    cfg = _make_config('etl:\n  sources:\n    volume:\n      file_types: ".jpg,.png"')
    assert cfg.etl_volume_file_types == [".jpg", ".png"]


def test_file_types_empty():
    cfg = _make_config("etl:\n  source:\n    table: t")
    assert cfg.etl_source_file_types == []


def test_url_expiration_default():
    cfg = _make_config("etl:\n  source:\n    table: t")
    assert cfg.etl_source_url_expiration == 86400


def test_url_expiration_custom():
    cfg = _make_config("etl:\n  sources:\n    volume:\n      url_expiration: 3600")
    assert cfg.etl_volume_url_expiration == 3600


def test_user_prompt_default():
    cfg = _make_config("etl:\n  source:\n    table: t")
    assert cfg.etl_source_user_prompt == "Describe this file"


def test_resolve_model_table():
    cfg = _make_config('provider: dashscope\ndashscope:\n  model: qwen3.5-flash\n  multimodal_model: qwen-vl-plus')
    assert cfg.resolve_model("table") == "qwen3.5-flash"


def test_resolve_model_volume():
    cfg = _make_config('provider: dashscope\ndashscope:\n  model: qwen3.5-flash\n  multimodal_model: qwen-vl-plus')
    assert cfg.resolve_model("volume") == "qwen-vl-plus"


def test_resolve_model_volume_default():
    cfg = _make_config('provider: dashscope\ndashscope:\n  model: qwen3.5-flash')
    assert cfg.resolve_model("volume") == "qwen-vl-plus"  # fallback default


def test_subdirectory_default():
    cfg = _make_config("etl:\n  source:\n    table: t")
    assert cfg.etl_source_subdirectory == ""


def test_system_prompt():
    cfg = _make_config('etl:\n  source:\n    system_prompt: "You are an expert."')
    assert cfg.etl_source_system_prompt == "You are an expert."


# ── Dual source enabled flags ────────────────────────────────

def test_table_enabled_default():
    cfg = _make_config("etl:\n  sources:\n    table:\n      table: t")
    assert cfg.etl_table_enabled is False  # default is false

def test_table_enabled_true():
    cfg = _make_config("etl:\n  sources:\n    table:\n      enabled: true")
    assert cfg.etl_table_enabled is True

def test_volume_enabled_true():
    cfg = _make_config("etl:\n  sources:\n    volume:\n      enabled: true\n      volume_name: v")
    assert cfg.etl_volume_enabled is True
    assert cfg.etl_volume_name == "v"

def test_both_enabled():
    cfg = _make_config("etl:\n  sources:\n    table:\n      enabled: true\n    volume:\n      enabled: true")
    assert cfg.etl_table_enabled is True
    assert cfg.etl_volume_enabled is True
