"""重建 competitor_product_master v2 - 与 product_master 结构对齐 + 溯源字段"""
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient

lh = LakehouseClient(config=Config())

lh.session.sql("DROP TABLE IF EXISTS cat_litter.competitor_product_master").collect()

sql = """
CREATE TABLE cat_litter.competitor_product_master
COMMENT '竞品产品主数据 - AI从5个渠道销售数据中提取，结构对齐product_master，已去重'
AS
WITH all_data AS (
    SELECT '抖音' AS platform, a.`商品` AS source_key, a.`商品` AS source_name, a.ai_result
    FROM cat_litter.douyin_product_master a
    UNION ALL
    SELECT '天猫', a.`商品编码`, b.`链接名称`, a.ai_result
    FROM cat_litter.tmall_product_master a
    LEFT JOIN cat_litter.sales_tmall b ON a.`商品编码` = b.`商品编码`
    UNION ALL
    SELECT '天猫超市', a.`商品ID`, b.`商品名称`, a.ai_result
    FROM cat_litter.tmall_chaoshi_product_master a
    LEFT JOIN cat_litter.sales_tmall_chaoshi b ON a.`商品ID` = b.`商品ID`
    UNION ALL
    SELECT '京东自营', a.`商品编号`, b.`商品名称`, a.ai_result
    FROM cat_litter.jd_self_product_master a
    LEFT JOIN cat_litter.sales_jd_self b ON a.`商品编号` = b.`商品编号`
    UNION ALL
    SELECT '京东POP', a.`商品编号`, b.`商品名称`, a.ai_result
    FROM cat_litter.jd_pop_product_master a
    LEFT JOIN cat_litter.sales_jd_pop b ON a.`商品编号` = b.`商品编号`
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY platform, source_key ORDER BY ai_result) AS rn
    FROM all_data
),
parsed AS (
    SELECT
        platform,
        source_key,
        source_name,
        get_json_object(ai_result, '$.品牌')     AS brand,
        get_json_object(ai_result, '$.三级类目')  AS cat3,
        get_json_object(ai_result, '$.四级类目')  AS cat4,
        get_json_object(ai_result, '$.类目编码')  AS cat_code,
        get_json_object(ai_result, '$.产品名称')  AS pname,
        get_json_object(ai_result, '$.配比')     AS formula,
        get_json_object(ai_result, '$.规格')     AS spec,
        ai_result
    FROM deduped WHERE rn = 1
),
with_seq AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY brand, cat_code
            ORDER BY platform, source_key
        ) AS seq
    FROM parsed
)
SELECT
    -- product_master 对齐的 9 列
    COALESCE(brand, '')                          AS `品牌`,
    '宠物用品'                                    AS `一级类目`,
    '猫砂'                                       AS `二级类目`,
    COALESCE(cat3, '')                           AS `三级类目`,
    COALESCE(cat4, '')                           AS `四级类目`,
    COALESCE(pname, '')                          AS `产品名称`,
    CONCAT(
        COALESCE(UPPER(SUBSTR(brand, 1, 2)), 'XX'), '-PS-CL-',
        COALESCE(cat_code, 'OL-OL'), '-',
        LPAD(CAST(seq AS STRING), 4, '0'), '-',
        CAST(CAST(COALESCE(NULLIF(spec, ''), '0') AS DOUBLE) * 1000 AS INT)
    )                                            AS `主数据`,
    COALESCE(spec, '')                           AS `规格`,
    COALESCE(formula, '')                        AS `配比`,
    -- 额外溯源字段
    platform                                     AS `数据来源平台`,
    source_key                                   AS `源数据主键`,
    source_name                                  AS `源数据商品名称`,
    cat_code                                     AS `类目编码`,
    ai_result                                    AS `ai_raw`
FROM with_seq
"""

print("重建 competitor_product_master v2...")
lh.session.sql(sql).collect()

# 添加列注释
comments = {
    "品牌": "AI识别的品牌名称",
    "一级类目": "一级类目(固定:宠物用品)",
    "二级类目": "二级类目(固定:猫砂)",
    "三级类目": "AI识别的三级类目",
    "四级类目": "AI识别的四级类目",
    "产品名称": "AI标准化的产品名称",
    "主数据": "自动生成的主数据编码(品牌缩写-PS-CL-类目编码-序号-规格克数)",
    "规格": "AI识别的规格(kg)",
    "配比": "AI识别的成分配比",
    "数据来源平台": "数据来源渠道(抖音/天猫/天猫超市/京东自营/京东POP)",
    "源数据主键": "源销售数据表中的主键",
    "源数据商品名称": "源销售数据中的原始商品名称",
    "类目编码": "三级-四级类目编码(对应category_mapping)",
    "ai_raw": "AI原始JSON响应(完整保留用于审计)",
}
for col, comment in comments.items():
    try:
        lh.session.sql(f"ALTER TABLE cat_litter.competitor_product_master ALTER COLUMN `{col}` COMMENT '{comment}'").collect()
    except Exception:
        pass

# 验证
cnt = lh.session.sql("SELECT COUNT(*) AS c FROM cat_litter.competitor_product_master").collect()[0]["c"]
print(f"\n总行数: {cnt}")

print("\n--- 结构对比 ---")
print("product_master 列:          品牌 | 一级类目 | 二级类目 | 三级类目 | 四级类目 | 产品名称 | 主数据 | 规格 | 配比")
s = lh.session.table("cat_litter.competitor_product_master").schema
cols = [f.name.strip("`") for f in s.fields]
print(f"competitor 列({len(cols)}): {' | '.join(cols)}")

print("\n--- 样本对比 ---")
print("\nproduct_master 样本:")
for r in lh.session.sql("SELECT * FROM cat_litter.product_master LIMIT 2").collect():
    print(f"  {r['品牌']} | {r['三级类目']}/{r['四级类目']} | {r['产品名称']} | {r['主数据']} | {r['规格']}kg | {r['配比']}")

print("\ncompetitor 样本:")
for r in lh.session.sql("SELECT * FROM cat_litter.competitor_product_master WHERE `品牌` != '福丸' LIMIT 5").collect():
    print(f"  {r['品牌']} | {r['三级类目']}/{r['四级类目']} | {r['产品名称']} | {r['主数据']} | {r['规格']}kg | {r['配比']}")
    print(f"    溯源: [{r['数据来源平台']}] {str(r['源数据主键'])[:30]}")

lh.close()
print("\n完成!")
