"""全面数据质量检查 — 检查 channel_summary 和源数据表"""
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient

lh = LakehouseClient(config=Config())


def q(sql):
    try:
        return lh.session.sql(sql).collect()
    except Exception as e:
        print(f"  SQL error: {e}")
        return []


issues = []

print("=" * 80)
print("Data Quality Check")
print("=" * 80)

# 0. Table existence
print("\n--- 0. Table Existence ---")
tables = [
    "cat_litter.channel_summary",
    "cat_litter.competitor_product_master",
    "cat_litter.sales_douyin",
    "cat_litter.sales_tmall",
    "cat_litter.sales_tmall_chaoshi",
    "cat_litter.sales_jd_self",
    "cat_litter.sales_jd_pop",
]
for t in tables:
    rows = q(f"SELECT COUNT(*) AS c FROM {t}")
    if rows:
        print(f"  {t}: {rows[0]['c']} rows")
    else:
        print(f"  {t}: MISSING")
        issues.append(f"Table missing: {t}")

# 1. Unit price anomalies
print("\n--- 1. Unit Price Anomalies (>1000) ---")
rows = q("""
    SELECT `渠道`, `品牌`, `产品名称`, `件单价`
    FROM cat_litter.channel_summary
    WHERE `件单价` > 1000
    ORDER BY `件单价` DESC LIMIT 10
""")
if rows:
    for r in rows:
        print(f"  [{r['渠道']}] {r['品牌']} | {str(r['产品名称'])[:30]} | price={r['件单价']:,.0f}")
        issues.append(f"Price anomaly: {r['渠道']}/{r['品牌']} = {r['件单价']:,.0f}")
else:
    print("  No anomalies found")

# 2. Conversion rate anomalies
print("\n--- 2. Conversion Rate Anomalies (>100%) ---")
rows = q("""
    SELECT `渠道`, `品牌`, `产品名称`, `转化率`
    FROM cat_litter.channel_summary
    WHERE `转化率` > 100
    ORDER BY `转化率` DESC LIMIT 10
""")
if rows:
    for r in rows:
        print(f"  [{r['渠道']}] {r['品牌']} | {str(r['产品名称'])[:30]} | conv={r['转化率']}%")
        issues.append(f"Conv>100%: {r['渠道']}/{r['品牌']} = {r['转化率']}%")
else:
    print("  No anomalies found")

# 3. Tmall sales volume distribution
print("\n--- 3. Tmall Sales Volume Distribution ---")
rows = q("""
    SELECT MIN(`销量`) AS min_v, MAX(`销量`) AS max_v,
        COUNT(DISTINCT `销量`) AS distinct_v, COUNT(*) AS total
    FROM cat_litter.channel_summary WHERE `渠道` = '天猫'
""")
if rows:
    r = rows[0]
    print(f"  min={r['min_v']} max={r['max_v']} distinct={r['distinct_v']} total={r['total']}")
    if r["distinct_v"] and r["distinct_v"] <= 1:
        issues.append("Tmall volume all same value (not fixed)")
        print("  NOT FIXED - all same value")
    else:
        print("  OK - multiple distinct values")

# 4. Empty brands
print("\n--- 4. Empty Brands ---")
rows = q("""
    SELECT `渠道`, COUNT(*) AS cnt
    FROM cat_litter.channel_summary
    WHERE `品牌` IS NULL OR `品牌` = '' OR `品牌` = 'null'
    GROUP BY `渠道`
""")
if rows:
    for r in rows:
        print(f"  {r['渠道']}: {r['cnt']} empty brands")
        issues.append(f"Empty brand: {r['渠道']} {r['cnt']}")
else:
    print("  No empty brands")

# 5. Negative values
print("\n--- 5. Negative Values ---")
has_neg = False
for col in ["销售额", "销量", "件单价", "浏览量"]:
    cnt = q(f"SELECT COUNT(*) AS c FROM cat_litter.channel_summary WHERE `{col}` < 0")
    if cnt and cnt[0]["c"] > 0:
        print(f"  {col}: {cnt[0]['c']} negative values")
        issues.append(f"Negative: {col} {cnt[0]['c']}")
        has_neg = True
if not has_neg:
    print("  No negative values")

# 6. NULL rate per channel
print("\n--- 6. NULL Rate Per Channel ---")
for r in q("""
    SELECT `渠道`, COUNT(*) AS total,
        SUM(CASE WHEN `销售额` IS NULL THEN 1 ELSE 0 END) AS n_sales,
        SUM(CASE WHEN `销量` IS NULL THEN 1 ELSE 0 END) AS n_vol,
        SUM(CASE WHEN `件单价` IS NULL THEN 1 ELSE 0 END) AS n_price,
        SUM(CASE WHEN `浏览量` IS NULL THEN 1 ELSE 0 END) AS n_traffic,
        SUM(CASE WHEN `转化率` IS NULL THEN 1 ELSE 0 END) AS n_conv,
        SUM(CASE WHEN `排名` IS NULL THEN 1 ELSE 0 END) AS n_rank
    FROM cat_litter.channel_summary GROUP BY `渠道` ORDER BY total DESC
"""):
    print(
        f"  {r['渠道']}: {r['total']}rows | "
        f"sales_null={r['n_sales']} vol_null={r['n_vol']} "
        f"price_null={r['n_price']} traffic_null={r['n_traffic']} "
        f"conv_null={r['n_conv']} rank_null={r['n_rank']}"
    )

# 7. Duplicate check
print("\n--- 7. Duplicates ---")
rows = q("""
    SELECT `渠道`, `主数据`, COUNT(*) AS cnt
    FROM cat_litter.channel_summary
    GROUP BY `渠道`, `主数据`
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC LIMIT 5
""")
if rows:
    for r in rows:
        name = str(r["主数据"])[:30]
        print(f"  [{r['渠道']}] '{name}' duplicated {r['cnt']}x")
        issues.append(f"Duplicate: {r['渠道']}/{name}")
else:
    print("  No duplicates")

# 8. JD raw vs parsed comparison
print("\n--- 8. JD Raw vs Parsed (sample) ---")
rows = q("""
    SELECT s.`商品名称`, s.`成交金额`, s.`成交商品件数`, s.`成交客单价`,
           c.`销售额`, c.`销量`, c.`件单价`
    FROM cat_litter.sales_jd_self s
    JOIN cat_litter.competitor_product_master m ON s.`商品编号` = m.`源数据主键`
    JOIN cat_litter.channel_summary c ON m.`主数据` = c.`主数据` AND c.`渠道` = '京东自营'
    WHERE m.`数据来源平台` = '京东自营'
    LIMIT 3
""")
for r in rows:
    print(f"  raw: amount={r['成交金额']} qty={r['成交商品件数']} price={r['成交客单价']}")
    print(f"  parsed: sales={r['销售额']} vol={r['销量']} price={r['件单价']}")
    print()

# 9. Price band distribution
print("\n--- 9. Price Band Distribution (JD Self) ---")
for r in q("""
    SELECT
        CASE
            WHEN `件单价` < 50 THEN '0-50'
            WHEN `件单价` < 100 THEN '50-100'
            WHEN `件单价` < 200 THEN '100-200'
            WHEN `件单价` < 500 THEN '200-500'
            WHEN `件单价` < 1000 THEN '500-1000'
            ELSE '1000+'
        END AS band,
        COUNT(*) AS cnt
    FROM cat_litter.channel_summary
    WHERE `渠道` = '京东自营' AND `件单价` IS NOT NULL
    GROUP BY 1 ORDER BY MIN(`件单价`)
"""):
    print(f"  {r['band']}: {r['cnt']} SKUs")

# 10. Competitor master data quality
print("\n--- 10. Competitor Master Data Quality ---")
rows = q("""
    SELECT `数据来源平台`, COUNT(*) AS total,
        SUM(CASE WHEN `品牌` IS NULL OR `品牌` = '' OR `品牌` = 'null' THEN 1 ELSE 0 END) AS no_brand,
        SUM(CASE WHEN `三级类目` IS NULL OR `三级类目` = '' THEN 1 ELSE 0 END) AS no_cat3,
        SUM(CASE WHEN `规格` IS NULL OR `规格` = '' THEN 1 ELSE 0 END) AS no_spec
    FROM cat_litter.competitor_product_master
    GROUP BY `数据来源平台`
""")
for r in rows:
    print(
        f"  {r['数据来源平台']}: {r['total']}rows | "
        f"no_brand={r['no_brand']} no_cat3={r['no_cat3']} no_spec={r['no_spec']}"
    )

# Summary
print("\n" + "=" * 80)
print(f"Found {len(issues)} issues")
print("=" * 80)
for i, issue in enumerate(issues, 1):
    print(f"  {i}. {issue}")

if not issues:
    print("  All checks passed!")

lh.close()
