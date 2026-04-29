"""
真实性测试：验证 Table 和 Volume 两种模式的增量跳过是否有效。

前提：已完成过一次全量处理，目标表 etl_table_results / etl_volume_results 中有数据。
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_client():
    from ai_etl.config import Config
    from ai_etl.lakehouse import LakehouseClient
    cfg = Config()
    return cfg, LakehouseClient(cfg)


def test_table_incremental():
    """Table 模式：数据源未变化时，增量过滤后应返回 0 行。"""
    cfg, client = get_client()

    source_table = cfg.etl_table_name
    target_table = cfg.resolve_table_target()
    key_columns  = cfg.etl_table_key_columns
    text_column  = cfg.etl_table_text_column
    batch_size   = cfg.etl_table_batch_size

    # 源表总行数
    total = int(client.session.sql(f"SELECT COUNT(*) AS cnt FROM {source_table}").collect()[0]["cnt"])
    logger.info("[Table] 源表总行数: %d", total)

    # 目标表已处理行数
    processed = int(client.session.sql(f"SELECT COUNT(*) AS cnt FROM {target_table}").collect()[0]["cnt"])
    logger.info("[Table] 目标表已处理行数: %d", processed)

    # 增量读取（传入 target_table 触发 LEFT JOIN 过滤）
    new_rows = client.read_source(
        table=source_table,
        key_columns=key_columns,
        text_column=text_column,
        target_table=target_table,
        batch_size=batch_size,
    )
    logger.info("[Table] 增量过滤后新行数: %d", len(new_rows))

    assert len(new_rows) == 0, (
        f"增量过滤失败：源表 {total} 行，目标表已有 {processed} 行，"
        f"但增量读取仍返回 {len(new_rows)} 行新数据"
    )
    logger.info("[Table] ✅ 增量跳过有效：0 行新数据")
    client.close()


def test_volume_incremental():
    """Volume 模式：数据源未变化时，增量过滤后应返回 0 个新文件。"""
    cfg, client = get_client()

    target_table   = cfg.resolve_volume_target()
    volume_sql_ref = cfg.get_volume_sql_ref()
    file_types     = cfg.etl_volume_file_types
    subdirectory   = cfg.etl_volume_subdirectory
    batch_size     = cfg.etl_volume_batch_size

    # 先单独查 Volume 文件总数（不传 target_table，不做增量过滤）
    all_files = client.discover_volume_files(
        volume_sql_ref=volume_sql_ref,
        file_types=file_types,
        subdirectory=subdirectory,
        target_table=None,   # 不过滤，看原始文件数
        batch_size=0,
    )
    logger.info("[Volume] Volume 中符合条件的文件数: %d", len(all_files))

    # 目标表已处理文件数
    processed = int(client.session.sql(f"SELECT COUNT(*) AS cnt FROM {target_table}").collect()[0]["cnt"])
    logger.info("[Volume] 目标表已处理文件数: %d", processed)

    # 增量过滤（传入 target_table）
    new_files = client.discover_volume_files(
        volume_sql_ref=volume_sql_ref,
        file_types=file_types,
        subdirectory=subdirectory,
        target_table=target_table,
        batch_size=batch_size,
    )
    logger.info("[Volume] 增量过滤后新文件数: %d", len(new_files))

    assert len(new_files) == 0, (
        f"增量过滤失败：Volume 中 {len(all_files)} 个文件，目标表已有 {processed} 条记录，"
        f"但增量过滤仍返回 {len(new_files)} 个新文件"
    )
    logger.info("[Volume] ✅ 增量跳过有效：0 个新文件")
    client.close()


if __name__ == "__main__":
    print("=" * 60)
    print("测试 Table 模式增量跳过...")
    print("=" * 60)
    try:
        test_table_incremental()
    except AssertionError as e:
        print(f"\n❌ FAIL: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)

    print()
    print("=" * 60)
    print("测试 Volume 模式增量跳过...")
    print("=" * 60)
    try:
        test_volume_incremental()
    except AssertionError as e:
        print(f"\n❌ FAIL: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)

    print()
    print("✅ 两种模式增量跳过均有效")
