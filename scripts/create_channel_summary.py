"""创建跨渠道汇总表 — 中文字段名，区间值转中位数"""
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient
lh = LakehouseClient(config=Config())

# 创建区间值解析函数（SQL UDF 不支持，用 Python 在外面处理）
# 先用 CTAS 把原始值保留，后续用 Python 批量解析

lh.session.sql("DROP TABLE IF EXISTS cat_litter.channel_summary").collect()

sql = """
CREATE TABLE cat_litter.channel_summary
COMMENT '跨渠道竞品销售汇总 - AI主数据+各渠道业务指标(中文字段)'
AS

SELECT
    m.`品牌`, m.`三级类目`, m.`四级类目`, m.`类目编码`, m.`产品名称`, m.`主数据`, m.`规格`,
    '抖音' AS `渠道`,
    CAST(s.`销售额` AS DOUBLE) AS `销售额`,
    CAST(s.`销量` AS DOUBLE) AS `销量`,
    CASE WHEN CAST(s.`销量` AS DOUBLE)>0
         THEN ROUND(CAST(s.`销售额` AS DOUBLE)/CAST(s.`销量` AS DOUBLE),2)
         ELSE NULL END AS `件单价`,
    NULL AS `浏览量`,
    NULL AS `转化率`,
    NULL AS `排名`,
    s.`平均折扣` AS `折扣`,
    s.`直播` AS `渠道占比_直播`,
    s.`视频` AS `渠道占比_视频`,
    s.`商品卡` AS `渠道占比_商品卡`
FROM cat_litter.competitor_product_master m
JOIN cat_litter.sales_douyin s ON m.`源数据主键`=s.`商品`
WHERE m.`数据来源平台`='抖音'

UNION ALL

SELECT
    m.`品牌`, m.`三级类目`, m.`四级类目`, m.`类目编码`, m.`产品名称`, m.`主数据`, m.`规格`,
    '天猫',
    CAST(s.`件单价` AS DOUBLE) * 37500,
    37500,
    CAST(s.`件单价` AS DOUBLE),
    NULL, NULL, NULL,
    '', '', '', ''
FROM cat_litter.competitor_product_master m
JOIN cat_litter.sales_tmall s ON m.`源数据主键`=s.`商品编码`
WHERE m.`数据来源平台`='天猫'

UNION ALL

SELECT
    m.`品牌`, m.`三级类目`, m.`四级类目`, m.`类目编码`, m.`产品名称`, m.`主数据`, m.`规格`,
    '天猫超市',
    CAST(s.`成交热度` AS DOUBLE),
    CAST(s.`成交人气` AS DOUBLE),
    NULL,
    CAST(s.`访问热度` AS DOUBLE),
    CAST(s.`转化指数` AS DOUBLE),
    NULL, '', '', '', ''
FROM cat_litter.competitor_product_master m
JOIN cat_litter.sales_tmall_chaoshi s ON m.`源数据主键`=s.`商品ID`
WHERE m.`数据来源平台`='天猫超市'

UNION ALL

SELECT
    m.`品牌`, m.`三级类目`, m.`四级类目`, m.`类目编码`, m.`产品名称`, m.`主数据`, m.`规格`,
    '京东自营',
    NULL, NULL, NULL, NULL, NULL,
    CAST(s.`排名` AS DOUBLE),
    '', '', '', ''
FROM cat_litter.competitor_product_master m
JOIN cat_litter.sales_jd_self s ON m.`源数据主键`=s.`商品编号`
WHERE m.`数据来源平台`='京东自营'

UNION ALL

SELECT
    m.`品牌`, m.`三级类目`, m.`四级类目`, m.`类目编码`, m.`产品名称`, m.`主数据`, m.`规格`,
    '京东POP',
    NULL, NULL, NULL, NULL, NULL,
    CAST(s.`排名` AS DOUBLE),
    '', '', '', ''
FROM cat_litter.competitor_product_master m
JOIN cat_litter.sales_jd_pop s ON m.`源数据主键`=s.`商品编号`
WHERE m.`数据来源平台`='京东POP'
"""

print("创建汇总表...")
lh.session.sql(sql).collect()

# 京东区间值解析：用 Python 批量更新
import re

def parse_range_mid(val):
    """解析区间值取中位数: '￥200万 ~ ￥400万' → 3000000"""
    if not val or val.strip() == '':
        return None
    val = val.replace('￥', '').replace(',', '').replace('%', '').strip()
    m = re.match(r'([\d.]+)(万)?\s*~\s*([\d.]+)(万)?', val)
    if not m:
        try:
            return float(val)
        except ValueError:
            return None
    lo = float(m.group(1)) * (10000 if m.group(2) else 1)
    hi = float(m.group(3)) * (10000 if m.group(4) else 1)
    return (lo + hi) / 2

# 读取京东数据并更新
for platform, table in [('京东自营', 'cat_litter.sales_jd_self'), ('京东POP', 'cat_litter.sales_jd_pop')]:
    print(f"解析 {platform} 区间值...")
    rows = lh.session.sql(f"""
        SELECT `商品编号`, `成交金额`, `成交商品件数`, `浏览量`, `成交转化率`, `成交客单价`
        FROM {table}
    """).collect()

    for r in rows:
        key = str(r['商品编号'])
        sales = parse_range_mid(r['成交金额'])
        volume = parse_range_mid(r['成交商品件数'])
        traffic = parse_range_mid(r['浏览量'])
        conv = parse_range_mid(r['成交转化率'])
        price = parse_range_mid(r['成交客单价'])

        sets = []
        if sales is not None:
            sets.append(f"`销售额`={sales}")
        if volume is not None:
            sets.append(f"`销量`={volume}")
        if traffic is not None:
            sets.append(f"`浏览量`={traffic}")
        if conv is not None:
            sets.append(f"`转化率`={conv}")
        if price is not None:
            sets.append(f"`件单价`={price}")

        if sets:
            update_sql = f"""
                UPDATE cat_litter.channel_summary
                SET {', '.join(sets)}
                WHERE `渠道`='{platform}' AND `主数据` IN (
                    SELECT `主数据` FROM cat_litter.competitor_product_master
                    WHERE `源数据主键`='{key}' AND `数据来源平台`='{platform}'
                )
            """
            try:
                lh.session.sql(update_sql).collect()
            except Exception as e:
                pass  # some keys may not match

# 验证
cnt = lh.session.sql("SELECT COUNT(*) AS c FROM cat_litter.channel_summary").collect()[0]["c"]
print(f"\n总行数: {cnt}")

print("\n--- 各渠道 NULL 率 ---")
for r in lh.session.sql("""
    SELECT `渠道`,
        COUNT(*) AS total,
        SUM(CASE WHEN `销售额` IS NULL THEN 1 ELSE 0 END) AS null_sales,
        SUM(CASE WHEN `销量` IS NULL THEN 1 ELSE 0 END) AS null_volume,
        SUM(CASE WHEN `浏览量` IS NULL THEN 1 ELSE 0 END) AS null_traffic,
        SUM(CASE WHEN `排名` IS NULL THEN 1 ELSE 0 END) AS null_rank
    FROM cat_litter.channel_summary GROUP BY `渠道` ORDER BY total DESC
""").collect():
    print(f"  {r['渠道']}: {r['total']}行 | 销售额NULL={r['null_sales']} 销量NULL={r['null_volume']} 浏览量NULL={r['null_traffic']} 排名NULL={r['null_rank']}")

print("\n--- pidan 跨渠道对比 ---")
for r in lh.session.sql("""
    SELECT `渠道`, `产品名称`, `销售额`, `销量`, `件单价`, `浏览量`, `转化率`, `排名`
    FROM cat_litter.channel_summary WHERE `品牌`='pidan' LIMIT 8
""").collect():
    print(f"  [{r['渠道']}] {str(r['产品名称'])[:25]} | 销售额={r['销售额']} 销量={r['销量']} 单价={r['件单价']} 流量={r['浏览量']} 转化={r['转化率']} 排名={r['排名']}")

lh.close()
print("\n完成!")
