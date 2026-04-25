"""Tests for ai_etl.media_types module."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_etl.media_types import (
    MediaType, detect_media_type, build_content_parts,
    is_provider_supported, filter_files_by_extensions,
    filter_files_by_subdirectory,
)
from ai_etl.result_keys import encode_custom_id, decode_custom_id


# ── detect_media_type ─────────────────────────────────────────

def test_detect_image_types():
    for ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".tif"]:
        assert detect_media_type(f"photo{ext}") == MediaType.IMAGE
        assert detect_media_type(f"photo{ext.upper()}") == MediaType.IMAGE

def test_detect_video_types():
    for ext in [".mp4", ".avi", ".mov", ".mkv", ".webm"]:
        assert detect_media_type(f"clip{ext}") == MediaType.VIDEO

def test_detect_audio_types():
    for ext in [".mp3", ".wav", ".flac", ".ogg", ".m4a"]:
        assert detect_media_type(f"track{ext}") == MediaType.AUDIO

def test_detect_unknown():
    assert detect_media_type("data.csv") == MediaType.UNKNOWN
    assert detect_media_type("readme") == MediaType.UNKNOWN
    assert detect_media_type("") == MediaType.UNKNOWN


# ── is_provider_supported ────────────────────────────────────

def test_dashscope_supports_all():
    assert is_provider_supported(MediaType.IMAGE, "dashscope")
    assert is_provider_supported(MediaType.VIDEO, "dashscope")
    assert is_provider_supported(MediaType.AUDIO, "dashscope")

def test_zhipuai_no_audio():
    assert is_provider_supported(MediaType.IMAGE, "zhipuai")
    assert is_provider_supported(MediaType.VIDEO, "zhipuai")
    assert not is_provider_supported(MediaType.AUDIO, "zhipuai")

def test_unknown_provider():
    assert not is_provider_supported(MediaType.IMAGE, "unknown_provider")


# ── build_content_parts ──────────────────────────────────────

def test_image_content_dashscope():
    parts = build_content_parts("https://url", MediaType.IMAGE, "Describe", "dashscope")
    assert parts is not None
    assert len(parts) == 2
    assert parts[0]["type"] == "image_url"
    assert parts[0]["image_url"]["url"] == "https://url"
    assert parts[1] == {"type": "text", "text": "Describe"}

def test_video_content_dashscope():
    parts = build_content_parts("https://url", MediaType.VIDEO, "Describe", "dashscope")
    assert parts is not None
    assert parts[0]["type"] == "video_url"
    assert parts[0]["video_url"]["url"] == "https://url"

def test_audio_content_dashscope():
    parts = build_content_parts("https://url", MediaType.AUDIO, "Transcribe", "dashscope")
    assert parts is not None
    assert parts[0]["type"] == "input_audio"
    assert parts[0]["input_audio"]["url"] == "https://url"

def test_audio_unsupported_zhipuai():
    parts = build_content_parts("https://url", MediaType.AUDIO, "Transcribe", "zhipuai")
    assert parts is None

def test_image_content_zhipuai():
    parts = build_content_parts("https://url", MediaType.IMAGE, "Describe", "zhipuai")
    assert parts is not None
    assert parts[0]["type"] == "image_url"

def test_text_prompt_always_last():
    """Property 7: text prompt is always the last content part."""
    for mt in [MediaType.IMAGE, MediaType.VIDEO]:
        for prov in ["dashscope", "zhipuai"]:
            parts = build_content_parts("https://url", mt, "my prompt", prov)
            if parts is not None:
                assert parts[-1] == {"type": "text", "text": "my prompt"}

def test_unknown_media_type_returns_none():
    parts = build_content_parts("https://url", MediaType.UNKNOWN, "Describe", "dashscope")
    assert parts is None


# ── filter_files_by_extensions ───────────────────────────────

def test_filter_by_extensions():
    files = [
        {"relative_path": "a.jpg"},
        {"relative_path": "b.png"},
        {"relative_path": "c.csv"},
        {"relative_path": "d.MP4"},
    ]
    result = filter_files_by_extensions(files, [".jpg", ".png"])
    assert len(result) == 2
    assert all(f["relative_path"].lower().endswith((".jpg", ".png")) for f in result)

def test_filter_by_extensions_case_insensitive():
    files = [{"relative_path": "photo.JPG"}, {"relative_path": "photo.Png"}]
    result = filter_files_by_extensions(files, [".jpg", ".png"])
    assert len(result) == 2

def test_filter_by_extensions_empty_returns_all():
    files = [{"relative_path": "a.jpg"}, {"relative_path": "b.csv"}]
    result = filter_files_by_extensions(files, [])
    assert len(result) == 2


# ── filter_files_by_subdirectory ─────────────────────────────

def test_filter_by_subdirectory():
    files = [
        {"relative_path": "images/a.jpg"},
        {"relative_path": "images/sub/b.png"},
        {"relative_path": "docs/c.pdf"},
    ]
    result = filter_files_by_subdirectory(files, "images/")
    assert len(result) == 2

def test_filter_by_subdirectory_empty_returns_all():
    files = [{"relative_path": "a.jpg"}, {"relative_path": "b.csv"}]
    result = filter_files_by_subdirectory(files, "")
    assert len(result) == 2


# ── encode/decode round-trip (Property 8) ────────────────────

def test_custom_id_roundtrip_simple():
    path = "images/photo_001.jpg"
    encoded = encode_custom_id([path])
    decoded = decode_custom_id(encoded)
    assert decoded[0] == path

def test_custom_id_roundtrip_with_special_chars():
    path = "data/2024-01/report (final).png"
    encoded = encode_custom_id([path])
    decoded = decode_custom_id(encoded)
    assert decoded[0] == path
