"""
Batch 推理直接调用示例（不经过 Lakehouse，纯 API 测试）。

用法：
    cd ai_etl
    pip install -e .
    python examples/batch_demo.py
"""

import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_etl.config import Config
from ai_etl.providers import create_provider, BatchProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def demo_text_batch():
    """示例：纯文本批量推理（不经过 Lakehouse）。"""
    print("\n" + "=" * 60)
    print("文本批量推理示例")
    print("=" * 60)

    cfg = Config()
    api_key = os.getenv("DASHSCOPE_API_KEY", "")
    provider = create_provider("dashscope", api_key=api_key, model="qwen3.5-flash")

    # 构建 JSONL
    prompts = ["用一句话介绍杭州西湖", "1+1等于几？", "Python 和 Java 的区别"]
    output_dir = Path("examples/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = output_dir / "test_batch.jsonl"

    lines = []
    for i, prompt in enumerate(prompts, 1):
        lines.append(json.dumps({
            "custom_id": f"req-{i:03d}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "qwen3.5-flash",
                "messages": [
                    {"role": "system", "content": "简洁回答，50字以内。"},
                    {"role": "user", "content": prompt},
                ],
            },
        }, ensure_ascii=False))
    jsonl_path.write_text("\n".join(lines) + "\n")
    print(f"JSONL: {jsonl_path} ({len(lines)} 条)")

    # 提交
    file_id = provider.upload_file(jsonl_path)
    batch_id = provider.create_batch(file_id, task_name="batch-demo")
    print(f"batch_id: {batch_id}")

    # 等待（这会很久，可以 Ctrl+C 后用 resume 恢复）
    print("等待完成（可能需要 30-60 分钟）...")
    provider.wait_for_completion(batch_id)

    # 下载结果
    result_text = provider.download_results(batch_id)
    results = BatchProvider.parse_results(result_text)
    for r in results:
        print(f"  [{r['custom_id']}] {r['content'][:80]}")


if __name__ == "__main__":
    demo_text_batch()
