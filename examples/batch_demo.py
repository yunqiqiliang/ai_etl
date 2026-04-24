"""
DashScope Batch 推理调用示例。

前置条件：
    1. 在项目根目录的 .env 文件中配置 DASHSCOPE_API_KEY
    2. 在 config.yaml 中配置模型名称等参数（可选，有默认值）

用法：
    cd ai_etl
    pip install -e .
    python examples/batch_demo.py
"""

import logging
import sys
from pathlib import Path

# 将项目根目录加入 path，方便直接运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_etl.dashscope_batch import DashScopeBatch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def demo_simple_prompts():
    """示例 1：最简用法 - 从 prompt 列表批量推理。

    API Key 从 .env 读取，模型从 config.yaml 读取。
    """
    print("\n" + "=" * 60)
    print("示例 1：简单 prompt 列表批量推理")
    print("=" * 60)

    # 无需传入 api_key 和 model，自动从 .env 和 config.yaml 读取
    batch = DashScopeBatch()
    results = batch.run(
        prompts=[
            "用一句话介绍杭州西湖",
            "1+1等于几？请只回答数字",
            "Python 和 Java 的主要区别是什么？用三句话概括",
        ],
        system_prompt="你是一个简洁的助手，回答尽量简短。",
        task_name="demo-simple-prompts",
        output_file="examples/output/result_simple.jsonl",
    )

    print(f"\n共 {len(results)} 条结果：")
    for r in results:
        status = "✅" if r["status_code"] == 200 else "❌"
        print(f"  {status} [{r['custom_id']}] {r['content'][:120]}")
        if r["usage"]:
            u = r["usage"]
            print(
                f"     tokens: {u.get('prompt_tokens', 0)} in"
                f" / {u.get('completion_tokens', 0)} out"
            )

    # 导出 CSV
    csv_path = DashScopeBatch.results_to_csv(
        results, "examples/output/result_simple.csv"
    )
    print(f"\nCSV 已导出: {csv_path}")


def demo_multi_turn():
    """示例 2：多轮对话批量推理。"""
    print("\n" + "=" * 60)
    print("示例 2：多轮对话批量推理")
    print("=" * 60)

    batch = DashScopeBatch()
    results = batch.run(
        messages_list=[
            [
                {"role": "system", "content": "你是一个英语翻译助手，只输出翻译结果。"},
                {"role": "user", "content": "翻译：今天天气真好"},
            ],
            [
                {"role": "system", "content": "你是一个英语翻译助手，只输出翻译结果。"},
                {"role": "user", "content": "翻译：我喜欢编程"},
                {"role": "assistant", "content": "I love programming."},
                {"role": "user", "content": "翻译：人工智能正在改变世界"},
            ],
        ],
        task_name="demo-multi-turn",
    )

    print(f"\n共 {len(results)} 条结果：")
    for r in results:
        print(f"  [{r['custom_id']}] {r['content']}")


def demo_step_by_step():
    """示例 3：分步控制 - 手动管理每个环节。"""
    print("\n" + "=" * 60)
    print("示例 3：分步控制流程")
    print("=" * 60)

    batch = DashScopeBatch()

    # Step 1: 构建 JSONL（模型名称从 config.yaml 读取）
    print("\n[Step 1] 构建 JSONL 文件...")
    cfg = batch._config
    jsonl_path = DashScopeBatch.build_jsonl_from_prompts(
        prompts=["什么是机器学习？一句话回答", "什么是深度学习？一句话回答"],
        model=cfg.model_name,
        system_prompt="你是一个AI教授，回答简洁专业。",
        output_path="examples/output/step_input.jsonl",
    )
    print(f"  JSONL 文件: {jsonl_path}")
    print(f"  内容预览:")
    for line in jsonl_path.read_text().strip().split("\n"):
        print(f"    {line[:100]}...")

    # Step 2: 上传文件
    print("\n[Step 2] 上传文件...")
    file_id = batch.upload_file(jsonl_path)
    print(f"  file_id: {file_id}")

    # Step 3: 创建任务
    print("\n[Step 3] 创建批量任务...")
    batch_id = batch.create_batch(
        file_id,
        task_name="demo-step-by-step",
        task_description="分步控制演示",
    )
    print(f"  batch_id: {batch_id}")

    # Step 4: 查询状态
    print("\n[Step 4] 查询任务状态...")
    status_info = batch.get_batch_status(batch_id)
    print(f"  状态: {status_info['status']}")
    print(f"  请求计数: {status_info['request_counts']}")

    # Step 5: 等待完成
    print("\n[Step 5] 等待任务完成...")
    final_status = batch.wait_for_completion(batch_id, poll_interval=5.0)
    print(f"  最终状态: {final_status}")

    # Step 6: 下载结果
    print("\n[Step 6] 下载结果...")
    results = batch.download_results(
        batch_id, output_file="examples/output/step_result.jsonl"
    )
    for r in results:
        print(f"  [{r['custom_id']}] {r['content'][:150]}")

    # Step 7: 检查错误
    errors = batch.download_errors(batch_id)
    if errors:
        print(f"\n  ⚠️ {len(errors)} 条失败:")
        for e in errors:
            print(f"    [{e['custom_id']}] {e['error_code']}: {e['error_message']}")
    else:
        print("\n  ✅ 无失败请求")


def demo_override_model():
    """示例 4：通过参数覆盖 config.yaml 中的模型配置。"""
    print("\n" + "=" * 60)
    print("示例 4：参数覆盖配置（使用 batch-test-model 快速验证）")
    print("=" * 60)

    batch = DashScopeBatch()

    # 用 test model 快速验证，覆盖 config.yaml 中的默认模型
    results = batch.run(
        prompts=["Hello", "What is 2+2?"],
        model="batch-test-model",
        endpoint="/v1/chat/ds-test",
        task_name="demo-override",
        poll_interval=3.0,
    )

    print(f"\n共 {len(results)} 条结果：")
    for r in results:
        print(f"  [{r['custom_id']}] {r['content']}")


if __name__ == "__main__":
    # 创建输出目录
    Path("examples/output").mkdir(parents=True, exist_ok=True)

    # 先用 test model 快速验证链路
    demo_override_model()

    # 以下示例使用真实模型（config.yaml 中配置），耗时较长
    # 取消注释运行：
    # demo_simple_prompts()
    # demo_multi_turn()
    # demo_step_by_step()

    print("\n" + "=" * 60)
    print("示例执行完成！")
    print("=" * 60)
