"""
媒体类型检测和 Provider 特定的多模态 content part 构建。

纯函数模块，无外部依赖。
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Set


class MediaType(Enum):
    """文件媒体类型。"""
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    UNKNOWN = "unknown"


# ── 扩展名 → 媒体类型映射 ────────────────────────────────────

EXTENSION_MAP: Dict[str, MediaType] = {
    ".jpg": MediaType.IMAGE, ".jpeg": MediaType.IMAGE,
    ".png": MediaType.IMAGE, ".gif": MediaType.IMAGE,
    ".bmp": MediaType.IMAGE, ".webp": MediaType.IMAGE,
    ".tiff": MediaType.IMAGE, ".tif": MediaType.IMAGE,
    ".mp4": MediaType.VIDEO, ".avi": MediaType.VIDEO,
    ".mov": MediaType.VIDEO, ".mkv": MediaType.VIDEO,
    ".webm": MediaType.VIDEO,
    ".mp3": MediaType.AUDIO, ".wav": MediaType.AUDIO,
    ".flac": MediaType.AUDIO, ".ogg": MediaType.AUDIO,
    ".m4a": MediaType.AUDIO,
}

# ── Provider → 支持的媒体类型 ─────────────────────────────────

PROVIDER_MEDIA_SUPPORT: Dict[str, Set[MediaType]] = {
    "dashscope": {MediaType.IMAGE, MediaType.VIDEO, MediaType.AUDIO},
    "zhipuai": {MediaType.IMAGE, MediaType.VIDEO},
}


def detect_media_type(file_path: str) -> MediaType:
    """根据文件扩展名检测媒体类型（大小写不敏感）。

    >>> detect_media_type("photos/cat.JPG")
    <MediaType.IMAGE: 'image'>
    >>> detect_media_type("data.csv")
    <MediaType.UNKNOWN: 'unknown'>
    """
    if "." not in file_path:
        return MediaType.UNKNOWN
    ext = "." + file_path.rsplit(".", 1)[-1].lower()
    return EXTENSION_MAP.get(ext, MediaType.UNKNOWN)


def is_provider_supported(media_type: MediaType, provider_name: str) -> bool:
    """检查 provider 是否支持指定的媒体类型。

    >>> is_provider_supported(MediaType.AUDIO, "dashscope")
    True
    >>> is_provider_supported(MediaType.AUDIO, "zhipuai")
    False
    """
    return media_type in PROVIDER_MEDIA_SUPPORT.get(provider_name, set())


def build_content_parts(
    presigned_url: str,
    media_type: MediaType,
    user_prompt: str,
    provider_name: str,
) -> Optional[List[Dict[str, Any]]]:
    """构建 Provider 特定的多模态 content parts 列表。

    返回 None 表示该 provider 不支持此媒体类型。
    返回的列表最后一个元素始终是 text part（用户指令）。

    >>> parts = build_content_parts("https://url", MediaType.IMAGE, "Describe", "dashscope")
    >>> parts[-1]
    {'type': 'text', 'text': 'Describe'}
    """
    if not is_provider_supported(media_type, provider_name):
        return None

    parts: List[Dict[str, Any]] = []

    if media_type == MediaType.IMAGE:
        # DashScope 和 ZhipuAI 都使用 OpenAI 兼容的 image_url 格式
        parts.append({
            "type": "image_url",
            "image_url": {"url": presigned_url},
        })

    elif media_type == MediaType.VIDEO:
        # DashScope 和 ZhipuAI 都使用 video_url 格式
        parts.append({
            "type": "video_url",
            "video_url": {"url": presigned_url},
        })

    elif media_type == MediaType.AUDIO:
        if provider_name == "dashscope":
            # DashScope qwen-omni-turbo 使用 input_audio
            parts.append({
                "type": "input_audio",
                "input_audio": {"url": presigned_url},
            })
        else:
            return None  # 其他 provider 不支持音频

    # 用户指令始终作为最后一个 content part
    parts.append({"type": "text", "text": user_prompt})

    return parts


def filter_files_by_extensions(
    files: List[Dict[str, Any]],
    extensions: List[str],
    path_key: str = "relative_path",
) -> List[Dict[str, Any]]:
    """按扩展名过滤文件列表（大小写不敏感）。

    >>> files = [{"relative_path": "a.jpg"}, {"relative_path": "b.csv"}]
    >>> filter_files_by_extensions(files, [".jpg"])
    [{'relative_path': 'a.jpg'}]
    """
    if not extensions:
        return files
    ext_set = {e.lower() if e.startswith(".") else f".{e.lower()}" for e in extensions}
    result = []
    for f in files:
        path = f.get(path_key, "")
        if "." in path:
            file_ext = "." + path.rsplit(".", 1)[-1].lower()
            if file_ext in ext_set:
                result.append(f)
    return result


def filter_files_by_subdirectory(
    files: List[Dict[str, Any]],
    subdirectory: str,
    path_key: str = "relative_path",
) -> List[Dict[str, Any]]:
    """按子目录前缀过滤文件列表。

    >>> files = [{"relative_path": "img/a.jpg"}, {"relative_path": "doc/b.pdf"}]
    >>> filter_files_by_subdirectory(files, "img/")
    [{'relative_path': 'img/a.jpg'}]
    """
    if not subdirectory:
        return files
    return [f for f in files if f.get(path_key, "").startswith(subdirectory)]
