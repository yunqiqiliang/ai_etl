"""
命令行入口。

用法：
    python -m ai_etl run                    # 运行完整流水线
    python -m ai_etl run --provider zhipuai # 指定 provider
    python -m ai_etl resume <batch_id>      # 恢复中断的任务
    python -m ai_etl status <batch_id>      # 查询任务状态
"""

from __future__ import annotations

import argparse
import logging
import sys


def main():
    parser = argparse.ArgumentParser(
        prog="ai_etl",
        description="AI ETL: Lakehouse → 批量推理 → 结果写回",
    )
    sub = parser.add_subparsers(dest="command")

    # run
    run_p = sub.add_parser("run", help="运行完整 ETL 流水线")
    run_p.add_argument("--provider", help="Provider 名称 (dashscope/zhipuai)")
    run_p.add_argument("--model", help="模型名称")
    run_p.add_argument("--source-table", help="源表名")
    run_p.add_argument("--target-table", help="目标表名")
    run_p.add_argument("--source-type", choices=["table", "volume"], help="数据源类型")
    run_p.add_argument("--volume-name", help="Volume 名称 (source-type=volume 时必需)")
    run_p.add_argument("--file-types", help="文件扩展名过滤，逗号分隔 (如 .jpg,.png)")

    # resume
    resume_p = sub.add_parser("resume", help="恢复中断的任务")
    resume_p.add_argument("batch_id", help="Batch 任务 ID")
    resume_p.add_argument("--provider", help="Provider 名称")

    # status
    status_p = sub.add_parser("status", help="查询任务状态")
    status_p.add_argument("batch_id", help="Batch 任务 ID")
    status_p.add_argument("--provider", help="Provider 名称")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.command == "run":
        from ai_etl.pipeline import AIETLPipeline
        pipeline = AIETLPipeline()
        try:
            # 解析 file_types
            file_types = None
            if hasattr(args, 'file_types') and args.file_types:
                file_types = [e.strip() for e in args.file_types.split(",") if e.strip()]

            stats = pipeline.run(
                source_type=args.source_type if hasattr(args, 'source_type') else None,
                provider_name=args.provider,
                model=args.model,
                source_table=args.source_table,
                target_table=args.target_table,
                volume_name=args.volume_name if hasattr(args, 'volume_name') else None,
                file_types=file_types,
            )
            print(f"\n完成: {stats['success_count']} 成功, {stats['written_rows']} 行写入")
        finally:
            pipeline.close()

    elif args.command == "resume":
        from ai_etl.pipeline import AIETLPipeline
        pipeline = AIETLPipeline()
        try:
            stats = pipeline.resume(args.batch_id, provider_name=args.provider)
            print(f"\n恢复完成: {stats['written_rows']} 行写入")
        finally:
            pipeline.close()

    elif args.command == "status":
        import os
        from ai_etl.config import Config
        from ai_etl.providers import create_provider

        cfg = Config()
        name = args.provider or cfg.provider_name
        key_map = {"dashscope": "DASHSCOPE_API_KEY", "zhipuai": "ZHIPUAI_API_KEY"}
        api_key = os.getenv(key_map.get(name, ""), "")
        provider = create_provider(name, api_key=api_key)
        info = provider.get_batch_status(args.batch_id)
        for k, v in info.items():
            print(f"  {k}: {v}")

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
