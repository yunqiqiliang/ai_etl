"""Tests for pipeline fixes: batch state, atomic write, error distribution, elapsed time."""

import json
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── Batch state: per-batch-id files, no mutual overwrite ─────

def test_save_batch_state_creates_per_batch_file(tmp_path):
    """Each batch_id gets its own state file."""
    from ai_etl.config import Config
    from ai_etl.pipeline import AIETLPipeline

    cfg = MagicMock(spec=Config)
    cfg.output_dir = str(tmp_path)

    pipeline = AIETLPipeline.__new__(AIETLPipeline)
    pipeline._config = cfg

    state_a = {"batch_id": "batch-aaa", "source_type": "table", "target_table": "t1"}
    state_b = {"batch_id": "batch-bbb", "source_type": "volume", "target_table": "t2"}

    pipeline._save_batch_state(state_a)
    pipeline._save_batch_state(state_b)

    # Both files must exist independently
    assert (tmp_path / "batch_batch-aaa.json").exists()
    assert (tmp_path / "batch_batch-bbb.json").exists()

    loaded_a = json.loads((tmp_path / "batch_batch-aaa.json").read_text())
    loaded_b = json.loads((tmp_path / "batch_batch-bbb.json").read_text())
    assert loaded_a["batch_id"] == "batch-aaa"
    assert loaded_b["batch_id"] == "batch-bbb"


def test_save_batch_state_no_mutual_overwrite(tmp_path):
    """Saving volume batch state must not overwrite table batch state."""
    from ai_etl.config import Config
    from ai_etl.pipeline import AIETLPipeline

    cfg = MagicMock(spec=Config)
    cfg.output_dir = str(tmp_path)

    pipeline = AIETLPipeline.__new__(AIETLPipeline)
    pipeline._config = cfg

    pipeline._save_batch_state({"batch_id": "table-001", "source_type": "table"})
    pipeline._save_batch_state({"batch_id": "vol-002", "source_type": "volume"})

    # table state must still be intact
    loaded = json.loads((tmp_path / "batch_table-001.json").read_text())
    assert loaded["source_type"] == "table"


def test_load_batch_state_by_batch_id(tmp_path):
    """_load_batch_state should find the correct file by batch_id."""
    from ai_etl.config import Config
    from ai_etl.pipeline import AIETLPipeline

    cfg = MagicMock(spec=Config)
    cfg.output_dir = str(tmp_path)

    pipeline = AIETLPipeline.__new__(AIETLPipeline)
    pipeline._config = cfg

    pipeline._save_batch_state({"batch_id": "abc-123", "target_table": "my_table"})
    pipeline._save_batch_state({"batch_id": "xyz-999", "target_table": "other_table"})

    state = pipeline._load_batch_state("abc-123")
    assert state["target_table"] == "my_table"

    state2 = pipeline._load_batch_state("xyz-999")
    assert state2["target_table"] == "other_table"


def test_load_batch_state_missing_returns_empty(tmp_path):
    """Loading a non-existent batch_id returns empty dict."""
    from ai_etl.config import Config
    from ai_etl.pipeline import AIETLPipeline

    cfg = MagicMock(spec=Config)
    cfg.output_dir = str(tmp_path)

    pipeline = AIETLPipeline.__new__(AIETLPipeline)
    pipeline._config = cfg

    state = pipeline._load_batch_state("nonexistent-id")
    assert state == {}


# ── Atomic write: tmp file then rename ───────────────────────

def test_save_batch_state_atomic_no_partial_file(tmp_path):
    """State file should not be left in partial state (tmp file cleaned up)."""
    from ai_etl.config import Config
    from ai_etl.pipeline import AIETLPipeline

    cfg = MagicMock(spec=Config)
    cfg.output_dir = str(tmp_path)

    pipeline = AIETLPipeline.__new__(AIETLPipeline)
    pipeline._config = cfg

    pipeline._save_batch_state({"batch_id": "test-atomic", "data": "x" * 1000})

    # No .tmp files should remain
    tmp_files = list(tmp_path.glob("*.tmp"))
    assert len(tmp_files) == 0

    # The actual file should be valid JSON
    content = (tmp_path / "batch_test-atomic.json").read_text()
    parsed = json.loads(content)
    assert parsed["batch_id"] == "test-atomic"


# ── Stats: elapsed_seconds field ─────────────────────────────

def test_make_stats_includes_elapsed():
    """_make_stats should include elapsed_seconds when provided."""
    from ai_etl.pipeline import AIETLPipeline

    stats = AIETLPipeline._make_stats(
        source_rows=100, provider="dashscope", model="qwen3.5-flash",
        batch_id="b-001", batch_status="completed",
        success_count=95, error_count=5, written_rows=95,
        elapsed_seconds=123.4,
    )
    assert stats["elapsed_seconds"] == 123.4
    assert stats["success_count"] == 95
    assert stats["error_count"] == 5


def test_make_stats_elapsed_none_by_default():
    """elapsed_seconds should be None when not provided."""
    from ai_etl.pipeline import AIETLPipeline

    stats = AIETLPipeline._make_stats(source_rows=0, provider="", model="")
    assert stats["elapsed_seconds"] is None


# ── Config: include_raw_response ─────────────────────────────

def test_include_raw_response_default():
    """include_raw_response defaults to True."""
    import tempfile
    from ai_etl.config import Config

    tmp = Path(tempfile.mktemp(suffix=".yaml"))
    tmp.write_text("etl:\n  target:\n    table: t\n", encoding="utf-8")
    env = Path(tempfile.mktemp(suffix=".env"))
    env.write_text("# empty\n", encoding="utf-8")
    cfg = Config(config_path=tmp, env_path=env)
    assert cfg.etl_target_include_raw_response is True


def test_include_raw_response_false():
    """include_raw_response can be disabled."""
    import tempfile
    from ai_etl.config import Config

    tmp = Path(tempfile.mktemp(suffix=".yaml"))
    tmp.write_text("etl:\n  target:\n    include_raw_response: false\n", encoding="utf-8")
    env = Path(tempfile.mktemp(suffix=".env"))
    env.write_text("# empty\n", encoding="utf-8")
    cfg = Config(config_path=tmp, env_path=env)
    assert cfg.etl_target_include_raw_response is False
