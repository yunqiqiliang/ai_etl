"""重建去重的竞品主数据表"""
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient

lh = LakehouseClient(config=Config())

# 1. 删旧表
lh.session.sql("DROP TABLE IF EXISTS cat_litter.competitor_product_master").collect()

# 2. 用 CTAS + ROW_NUMBER 去重：同平台同 source_key 只保留一条
sql = """
CREATE TABLE cat_litter.competitor_product_master
COMMENT '竞品产品主数据 - AI从5个渠道销售数据中提取生成，已去重'
AS
WITH all_data AS (
    SELECT '抖音' AS platform, `商品` AS source_key, `商品` AS source_name, ai_result
    FROM cat_litter.douyin_product_master
    UNION ALL
    SELECT '天猫', `商品编码`, `商品编码`, ai_result
    FROM cat_litter.tmall_product_master
    UNION ALL
    SELECT '天猫超市', `商品ID`, `商品ID`, ai_result
    FROM cat_litter.tmall_chaoshi_product_master
    UNION ALL
    SELECT '京东自营', `商品编号`, `商品编号`, ai_result
    FROM cat_litter.jd_self_product_master
    UNION ALL
    SELECT '京东POP', `商品编号`, `商品编号`, ai_result
    FROM cat_litter.jd_pop_product_master
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY platform, source_key ORDER BY ai_result) AS rn
    FROM all_data
)
SELECT
    platform,
    source_key,
    source_name,
    get_json_object(ai_result, '$.品牌')     AS brand,
    get_json_object(ai_result, '$.三级类目')  AS cat_level3,
    get_json_object(ai_result, '$.四级类目')  AS cat_level4,
    get_json_object(ai_result, '$.类目编码')  AS cat_code,
    get_json_object(ai_result, '$.产品名称')  AS product_name,
    get_json_object(ai_result, '$.配比')     AS formula,
    get_json_object(ai_result, '$.规格')     AS spec_kg,
    ai_result                                AS ai_raw
FROM deduped
WHERE rn = 1
"""

print("重建竞品主数据表（去重）...")
lh.session.sql(sql).collect()

# 3. 验证
cnt = lh.session.sql("SELECT COUNT(*) AS c FROM cat_litter.competitor_product_master").collect()[0]["c"]
print(f"\n总行数: {cnt}")

print("\n--- 各平台行数 ---")
for r in lh.session.sql("""
    SELECT platform, COUNT(*) AS cnt
    FROM cat_litter.competitor_product_master
    GROUP BY platform ORDER BY cnt DESC
""").collect():
    print(f"  {r['platform']}: {r['cnt']}")

print("\n--- 品牌 Top 10 ---")
for r in lh.session.sql("""
    SELECT brand, COUNT(*) AS cnt
    FROM cat_litter.competitor_product_master
    GROUP BY brand ORDER BY cnt DESC LIMIT 10
""").collect():
    print(f"  {r['brand']}: {r['cnt']}")

print("\n--- 类目分布 ---")
for r in lh.session.sql("""
    SELECT cat_level3, cat_code, COUNT(*) AS cnt
    FROM cat_litter.competitor_product_master
    GROUP BY cat_level3, cat_code ORDER BY cnt DESC LIMIT 10
""").collect():
    print(f"  {r['cat_level3']} ({r['cat_code']}): {r['cnt']}")

lh.close()
print("\n完成!")
