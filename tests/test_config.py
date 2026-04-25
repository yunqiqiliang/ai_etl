"""Tests for Config properties."""

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


# ── Table source properties ──────────────────────────────────

def test_table_name():
    cfg = _make_config('etl:\n  sources:\n    table:\n      table: "schema.my_table"')
    assert cfg.etl_table_name == "schema.my_table"


def test_table_key_columns_default():
    cfg = _make_config("etl:\n  sources:\n    table:\n      table: t")
    assert cfg.etl_table_key_columns == "id"


def test_table_text_column_default():
    cfg = _make_config("etl:\n  sources:\n    table:\n      table: t")
    assert cfg.etl_table_text_column == "content"


def test_table_system_prompt():
    cfg = _make_config('etl:\n  sources:\n    table:\n      system_prompt: "You are an expert."')
    assert cfg.etl_table_system_prompt == "You are an expert."


def test_table_system_prompt_default():
    cfg = _make_config("etl:\n  sources:\n    table:\n      table: t")
    assert cfg.etl_table_system_prompt == "You are a helpful assistant."


# ── Volume source properties ─────────────────────────────────

def test_file_types_list():
    cfg = _make_config('etl:\n  sources:\n    volume:\n      file_types: [".jpg", ".png"]')
    assert cfg.etl_volume_file_types == [".jpg", ".png"]


def test_file_types_string():
    cfg = _make_config('etl:\n  sources:\n    volume:\n      file_types: ".jpg,.png"')
    assert cfg.etl_volume_file_types == [".jpg", ".png"]


def test_file_types_empty():
    cfg = _make_config("etl:\n  sources:\n    volume:\n      volume_type: user")
    assert cfg.etl_volume_file_types == []


def test_url_expiration_default():
    cfg = _make_config("etl:\n  sources:\n    volume:\n      volume_type: user")
    assert cfg.etl_volume_url_expiration == 86400


def test_url_expiration_custom():
    cfg = _make_config("etl:\n  sources:\n    volume:\n      url_expiration: 3600")
    assert cfg.etl_volume_url_expiration == 3600


def test_user_prompt_default():
    cfg = _make_config("etl:\n  sources:\n    volume:\n      volume_type: user")
    assert cfg.etl_volume_user_prompt == "Describe this file"


def test_volume_system_prompt():
    cfg = _make_config('etl:\n  sources:\n    volume:\n      system_prompt: "You are an expert."')
    assert cfg.etl_volume_system_prompt == "You are an expert."


def test_subdirectory_default():
    cfg = _make_config("etl:\n  sources:\n    volume:\n      volume_type: user")
    assert cfg.etl_volume_subdirectory == ""


# ── Model resolution ─────────────────────────────────────────

def test_resolve_model_table():
    cfg = _make_config('provider: dashscope\ndashscope:\n  model: qwen3.5-flash\n  multimodal_model: qwen-vl-plus')
    assert cfg.resolve_model("table") == "qwen3.5-flash"


def test_resolve_model_volume():
    cfg = _make_config('provider: dashscope\ndashscope:\n  model: qwen3.5-flash\n  multimodal_model: qwen-vl-plus')
    assert cfg.resolve_model("volume") == "qwen-vl-plus"


def test_resolve_model_volume_default():
    cfg = _make_config('provider: dashscope\ndashscope:\n  model: qwen3.5-flash')
    assert cfg.resolve_model("volume") == "qwen-vl-plus"  # fallback default


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


# ── Per-source target table ──────────────────────────────────

def test_table_target_table():
    cfg = _make_config('etl:\n  sources:\n    table:\n      target_table: "s.t_results"')
    assert cfg.etl_table_target_table == "s.t_results"


def test_volume_target_table():
    cfg = _make_config('etl:\n  sources:\n    volume:\n      target_table: "s.v_results"')
    assert cfg.etl_volume_target_table == "s.v_results"


def test_resolve_table_target_from_source():
    cfg = _make_config('clickzetta:\n  schema: myschema\netl:\n  sources:\n    table:\n      target_table: "s.t_results"')
    assert cfg.resolve_table_target() == "s.t_results"


def test_resolve_table_target_fallback_global():
    cfg = _make_config('clickzetta:\n  schema: myschema\netl:\n  target:\n    table: "global_target"')
    assert cfg.resolve_table_target() == "myschema.global_target"


def test_resolve_volume_target_from_source():
    cfg = _make_config('clickzetta:\n  schema: myschema\netl:\n  sources:\n    volume:\n      target_table: "v_results"')
    assert cfg.resolve_volume_target() == "myschema.v_results"


def test_qualify_table_name_with_schema():
    cfg = _make_config('clickzetta:\n  schema: mcp_demo')
    assert cfg._qualify_table_name("my_table") == "mcp_demo.my_table"
    assert cfg._qualify_table_name("other.my_table") == "other.my_table"
    assert cfg._qualify_table_name("") == ""


# ── Volume type / SQL ref ────────────────────────────────────

def test_volume_type_user():
    cfg = _make_config('etl:\n  sources:\n    volume:\n      volume_type: user')
    assert cfg.get_volume_sql_ref() == "USER VOLUME"


def test_volume_type_external():
    cfg = _make_config('etl:\n  sources:\n    volume:\n      volume_type: external\n      volume_name: my_vol')
    assert cfg.get_volume_sql_ref() == "VOLUME my_vol"


def test_volume_type_table():
    cfg = _make_config('etl:\n  sources:\n    volume:\n      volume_type: table\n      volume_name: my_tbl')
    assert cfg.get_volume_sql_ref() == "TABLE VOLUME my_tbl"
