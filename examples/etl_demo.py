"""
AI ETL 完整流水线示例。

流程：Lakehouse 源表 → 批量推理 → 结果写回 Lakehouse 目标表

前置条件：
    1. .env 中配置 DASHSCOPE_API_KEY 和 CLICKZETTA_* 连接参数
    2. config.yaml 中配置 etl.source 和 etl.target

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


def demo_full_pipeline():
    """示例 1：完整流水线 - 全部从配置文件读取。"""
    print("\n" + "=" * 60)
    print("AI ETL 完整流水线（配置驱动）")
    print("=" * 60)
    print("配置来源：")
    print("  凭证 → .env (DASHSCOPE_API_KEY, CLICKZETTA_*)")
    print("  表名/字段/模型 → config.yaml (etl.source, etl.target, model)")
    print()

    pipeline = AIETLPipeline()
    try:
        stats = pipeline.run()
        print(f"\n执行统计:")
        print(f"  源表读取: {stats['source_rows']} 行")
        print(f"  批量任务: {stats['batch_id']}")
        print(f"  推理成功: {stats['success_count']} 条")
        print(f"  推理失败: {stats['error_count']} 条")
        print(f"  写入目标: {stats['written_rows']} 行")
        print(f"  结果文件: {stats['output_file']}")
    finally:
        pipeline.close()


def demo_custom_params():
    """示例 2：通过参数覆盖配置。"""
    print("\n" + "=" * 60)
    print("AI ETL 流水线（参数覆盖）")
    print("=" * 60)

    pipeline = AIETLPipeline()
    try:
        stats = pipeline.run(
            # 覆盖 config.yaml 中的配置
            source_table="public.product_reviews",
            key_columns="review_id",
            text_column="review_text",
            filter_expr="language = 'zh' AND processed = false",
            target_table="public.review_analysis",
            result_column="sentiment",
            model="qwen3.5-flash",
            system_prompt="分析以下评论的情感倾向，只回答：正面、负面、中性",
        )
        print(f"\n处理完成: {stats['written_rows']} 行写入目标表")
    finally:
        pipeline.close()


def demo_step_by_step():
    """示例 3：分步执行 - 手动控制每个环节。"""
    print("\n" + "=" * 60)
    print("AI ETL 分步执行")
    print("=" * 60)

    from ai_etl.config import Config
    from ai_etl.dashscope_batch import DashScopeBatch
    from ai_etl.lakehouse import LakehouseClient

    cfg = Config()
    lakehouse = LakehouseClient(config=cfg)
    batch = DashScopeBatch(config=cfg)

    try:
        # Step 1: 读取源数据
        print("\n[Step 1] 读取源数据...")
        rows = lakehouse.read_source()
        print(f"  读取到 {len(rows)} 行")
        if rows:
            print(f"  首行预览: {rows[0]}")

        # Step 2: 构建 JSONL
        print("\n[Step 2] 构建 JSONL...")
        jsonl_path = lakehouse.build_jsonl_for_batch(rows)
        print(f"  文件: {jsonl_path}")

        # Step 3: 提交批量推理
        print("\n[Step 3] 提交批量推理...")
        file_id = batch.upload_file(jsonl_path)
        batch_id = batch.create_batch(file_id, task_name="etl-step-by-step")
        print(f"  batch_id: {batch_id}")

        # Step 4: 等待完成
        print("\n[Step 4] 等待完成...")
        batch.wait_for_completion(batch_id)

        # Step 5: 下载结果
        print("\n[Step 5] 下载结果...")
        results = batch.download_results(batch_id)
        print(f"  {len(results)} 条结果")
        if results:
            print(f"  首条: [{results[0]['custom_id']}] {results[0]['content'][:100]}")

        # Step 6: 写入目标表
        print("\n[Step 6] 写入目标表...")
        written = lakehouse.write_results(results)
        print(f"  写入 {written} 行")

    finally:
        lakehouse.close()


if __name__ == "__main__":
    # 默认运行完整流水线
    demo_full_pipeline()

    # 取消注释运行其他示例：
    # demo_custom_params()
    # demo_step_by_step()
