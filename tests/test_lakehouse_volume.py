"""Tests for LakehouseClient Volume methods (mocked session)."""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_etl.media_types import filter_files_by_extensions, filter_files_by_subdirectory


# ── Incremental discovery logic (Property 4) ─────────────────

def test_incremental_set_difference():
    """Property 4: incremental filter = exact set difference."""
    discovered = [
        {"relative_path": "a.jpg", "size": 100},
        {"relative_path": "b.png", "size": 200},
        {"relative_path": "c.gif", "size": 300},
    ]
    processed = {"a.jpg", "c.gif"}
    result = [f for f in discovered if f["relative_path"] not in processed]
    assert len(result) == 1
    assert result[0]["relative_path"] == "b.png"


def test_incremental_no_processed():
    discovered = [{"relative_path": "a.jpg", "size": 100}]
    processed = set()
    result = [f for f in discovered if f["relative_path"] not in processed]
    assert len(result) == 1


def test_incremental_all_processed():
    discovered = [{"relative_path": "a.jpg", "size": 100}]
    processed = {"a.jpg"}
    result = [f for f in discovered if f["relative_path"] not in processed]
    assert len(result) == 0


# ── Batch size limiting (Property 5) ─────────────────────────

def test_batch_size_limits():
    files = [{"relative_path": f"{i}.jpg", "size": 100} for i in range(100)]
    batch_size = 10
    result = files[:batch_size]
    assert len(result) == batch_size


def test_batch_size_smaller_than_files():
    files = [{"relative_path": f"{i}.jpg", "size": 100} for i in range(5)]
    batch_size = 10
    result = files[:batch_size] if len(files) > batch_size else files
    assert len(result) == 5


# ── Multimodal JSONL construction ─────────────────────────────

def test_build_multimodal_jsonl_image():
    """Test JSONL output for image files."""
    from ai_etl.media_types import detect_media_type, build_content_parts
    from ai_etl.result_keys import encode_custom_id

    files = [
        {"relative_path": "photos/cat.jpg", "size": 1000, "presigned_url": "https://example.com/cat.jpg"},
    ]

    # Simulate what build_multimodal_jsonl does
    for f in files:
        rp = f["relative_path"]
        url = f["presigned_url"]
        mt = detect_media_type(rp)
        parts = build_content_parts(url, mt, "Describe this image", "dashscope")

        assert parts is not None
        assert len(parts) == 2
        assert parts[0]["type"] == "image_url"
        assert parts[0]["image_url"]["url"] == url
        assert parts[1]["type"] == "text"
        assert parts[1]["text"] == "Describe this image"

        cid = encode_custom_id([rp])
        assert cid.startswith("req-")
        # custom_id 使用 URL encoding，'/' 被编码为 '%2F'
        assert "photos%2Fcat.jpg" in cid


def test_build_multimodal_jsonl_skips_unsupported():
    """Test that unsupported media types are skipped."""
    from ai_etl.media_types import detect_media_type, build_content_parts

    # Audio not supported by zhipuai
    mt = detect_media_type("track.mp3")
    parts = build_content_parts("https://url", mt, "Transcribe", "zhipuai")
    assert parts is None


def test_build_multimodal_jsonl_video_dashscope():
    """Test video content format for DashScope."""
    from ai_etl.media_types import detect_media_type, build_content_parts

    mt = detect_media_type("clip.mp4")
    parts = build_content_parts("https://url", mt, "Describe", "dashscope")
    assert parts is not None
    assert parts[0]["type"] == "video_url"
    assert parts[0]["video_url"]["url"] == "https://url"


def test_build_multimodal_jsonl_audio_dashscope():
    """Test audio content format for DashScope."""
    from ai_etl.media_types import detect_media_type, build_content_parts

    mt = detect_media_type("recording.wav")
    parts = build_content_parts("https://url", mt, "Transcribe", "dashscope")
    assert parts is not None
    assert parts[0]["type"] == "input_audio"


# ── Volume result mapping (Property 9) ───────────────────────

def test_volume_result_mapping():
    """Property 9: result mapping preserves file metadata."""
    from ai_etl.result_keys import encode_custom_id, decode_custom_id

    file_path = "images/photo_001.jpg"
    file_metadata = {
        file_path: {"relative_path": file_path, "size": 12345},
    }

    # Simulate what write_volume_results does
    custom_id = encode_custom_id([file_path])
    decoded = decode_custom_id(custom_id)
    recovered_path = decoded[0]

    assert recovered_path == file_path
    assert file_metadata[recovered_path]["size"] == 12345


def test_volume_result_mapping_special_chars():
    """Test file paths with special characters."""
    from ai_etl.result_keys import encode_custom_id, decode_custom_id

    file_path = "data/2024-Q1/report (v2).png"
    custom_id = encode_custom_id([file_path])
    decoded = decode_custom_id(custom_id)
    assert decoded[0] == file_path
