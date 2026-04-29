"""Create cross-channel summary table"""
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient
lh = LakehouseClient(config=Config())
lh.session.sql("DROP TABLE IF EXISTS cat_litter.channel_summary").collect()
sql = """CREATE TABLE cat_litter.channel_summary COMMENT 'Cross-channel competitive sales summary' AS
SELECT m.`品牌`,m.`三级类目`,m.`四级类目`,m.`类目编码`,m.`产品名称`,m.`主数据`,m.`规格`,
'抖音' AS platform, CAST(s.`销售额` AS DOUBLE) AS sales_amount, CAST(s.`销量` AS DOUBLE) AS sales_volume,
CASE WHEN CAST(s.`销量` AS DOUBLE)>0 THEN ROUND(CAST(s.`销售额` AS DOUBLE)/CAST(s.`销量` AS DOUBLE),2) ELSE NULL END AS unit_price,
NULL AS traffic, NULL AS conversion_pct, NULL AS rank_num,
s.`销售额` AS raw_sales, s.`销量` AS raw_volume, '' AS raw_traffic, '' AS raw_conversion
FROM cat_litter.competitor_product_master m JOIN cat_litter.sales_douyin s ON m.`源数据主键`=s.`商品` WHERE m.`数据来源平台`='抖音'
UNION ALL
SELECT m.`品牌`,m.`三级类目`,m.`四级类目`,m.`类目编码`,m.`产品名称`,m.`主数据`,m.`规格`,
'天猫', CAST(s.`件单价` AS DOUBLE)*37500, 37500, CAST(s.`件单价` AS DOUBLE),
NULL, NULL, NULL, '', s.`支付单量`, '', ''
FROM cat_litter.competitor_product_master m JOIN cat_litter.sales_tmall s ON m.`源数据主键`=s.`商品编码` WHERE m.`数据来源平台`='天猫'
UNION ALL
SELECT m.`品牌`,m.`三级类目`,m.`四级类目`,m.`类目编码`,m.`产品名称`,m.`主数据`,m.`规格`,
'天猫超市', CAST(s.`成交热度` AS DOUBLE), CAST(s.`成交人气` AS DOUBLE), NULL,
CAST(s.`访问热度` AS DOUBLE), CAST(s.`转化指数` AS DOUBLE), NULL,
CAST(s.`成交热度` AS STRING), CAST(s.`成交人气` AS STRING), CAST(s.`访问热度` AS STRING), CAST(s.`转化指数` AS STRING)
FROM cat_litter.competitor_product_master m JOIN cat_litter.sales_tmall_chaoshi s ON m.`源数据主键`=s.`商品ID` WHERE m.`数据来源平台`='天猫超市'
UNION ALL
SELECT m.`品牌`,m.`三级类目`,m.`四级类目`,m.`类目编码`,m.`产品名称`,m.`主数据`,m.`规格`,
'京东自营', NULL, NULL, NULL, NULL, NULL, CAST(s.`排名` AS INT),
s.`成交金额`, s.`成交商品件数`, s.`浏览量`, s.`成交转化率`
FROM cat_litter.competitor_product_master m JOIN cat_litter.sales_jd_self s ON m.`源数据主键`=s.`商品编号` WHERE m.`数据来源平台`='京东自营'
UNION ALL
SELECT m.`品牌`,m.`三级类目`,m.`四级类目`,m.`类目编码`,m.`产品名称`,m.`主数据`,m.`规格`,
'京东POP', NULL, NULL, NULL, NULL, NULL, CAST(s.`排名` AS INT),
s.`成交金额`, s.`成交商品件数`, s.`浏览量`, s.`成交转化率`
FROM cat_litter.competitor_product_master m JOIN cat_litter.sales_jd_pop s ON m.`源数据主键`=s.`商品编号` WHERE m.`数据来源平台`='京东POP'
"""
print("Creating channel_summary...")
lh.session.sql(sql).collect()
cnt = lh.session.sql("SELECT COUNT(*) AS c FROM cat_litter.channel_summary").collect()[0]["c"]
print(f"Total rows: {cnt}")
for r in lh.session.sql("SELECT platform, COUNT(*) AS cnt FROM cat_litter.channel_summary GROUP BY platform ORDER BY cnt DESC").collect():
    print(f"  {r['platform']}: {r['cnt']}")
print("\nBrand coverage:")
for r in lh.session.sql("SELECT `品牌`, COUNT(DISTINCT platform) AS ch, COUNT(*) AS t FROM cat_litter.channel_summary WHERE length(`品牌`)>0 GROUP BY `品牌` ORDER BY ch DESC, t DESC LIMIT 8").collect():
    print(f"  {r['品牌']}: {r['ch']} channels, {r['t']} items")
lh.close()
print("Done!")
