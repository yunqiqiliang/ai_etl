"""
AI ETL 完整流水线示例。

前置条件：
    1. .env 中配置 API Key 和 Lakehouse 凭证
    2. config.yaml 中配置 sources 和 target

用法：
    cd ai_etl
    pip install -e .
    python examples/etl_demo.py
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_etl.pipeline import AIETLPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def demo_config_driven():
    """示例 1：配置驱动 — 根据 config.yaml 中的 enabled 开关运行。"""
    print("\n" + "=" * 60)
    print("示例 1：配置驱动（读取 config.yaml）")
    print("=" * 60)

    pipeline = AIETLPipeline()
    try:
        stats = pipeline.run()
        print(f"\n统计: {stats}")
    finally:
        pipeline.close()


def demo_table_mode():
    """示例 2：显式指定 Table 模式。"""
    print("\n" + "=" * 60)
    print("示例 2：Table 模式")
    print("=" * 60)

    pipeline = AIETLPipeline()
    try:
        stats = pipeline.run(source_type="table")
        print(f"\n处理 {stats.get('source_rows', 0)} 行, 写入 {stats.get('written_rows', 0)} 行")
    finally:
        pipeline.close()


def demo_volume_mode():
    """示例 3：显式指定 Volume 模式。"""
    print("\n" + "=" * 60)
    print("示例 3：Volume 模式")
    print("=" * 60)

    pipeline = AIETLPipeline()
    try:
        stats = pipeline.run(source_type="volume")
        print(f"\n处理 {stats.get('source_rows', 0)} 文件, 写入 {stats.get('written_rows', 0)} 行")
    finally:
        pipeline.close()


def demo_resume():
    """示例 4：恢复中断的任务。"""
    print("\n" + "=" * 60)
    print("示例 4：恢复中断的任务")
    print("=" * 60)

    import json
    state_file = Path("output/last_batch.json")
    if not state_file.exists():
        print("没有找到 output/last_batch.json，请先运行一次 pipeline")
        return

    state = json.loads(state_file.read_text())
    batch_id = state["batch_id"]
    provider = state.get("provider", "dashscope")
    print(f"恢复 batch_id={batch_id}, provider={provider}")

    pipeline = AIETLPipeline()
    try:
        stats = pipeline.resume(batch_id, provider_name=provider)
        print(f"\n恢复完成: {stats.get('written_rows', 0)} 行写入")
    finally:
        pipeline.close()


if __name__ == "__main__":
    # 默认运行配置驱动模式
    demo_config_driven()

    # 取消注释运行其他示例：
    # demo_table_mode()
    # demo_volume_mode()
    # demo_resume()
