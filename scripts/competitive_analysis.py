"""竞品运营分析 — 从 channel_summary 中提取运营洞察

前置条件: 先运行 fix_data_quality.py 修复数据质量问题
"""
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
    return lh.session.sql(sql).collect()


print("=" * 80)
print("1. Brand Market Share (JD Self, most reliable data)")
print("=" * 80)
for r in q("""
    SELECT `品牌`, COUNT(*) AS sku_count,
        SUM(`销售额`) AS total_sales,
        ROUND(AVG(`排名`), 1) AS avg_rank,
        ROUND(AVG(`转化率`), 1) AS avg_conv
    FROM cat_litter.channel_summary
    WHERE `渠道` = '京东自营' AND `品牌` IS NOT NULL AND `品牌` != '' AND `品牌` != 'null'
        AND `销售额` IS NOT NULL AND `销售额` > 0
    GROUP BY `品牌`
    ORDER BY total_sales DESC
    LIMIT 15
"""):
    conv_str = f"{r['avg_conv']}%" if r["avg_conv"] is not None else "N/A"
    rank_str = str(r["avg_rank"]) if r["avg_rank"] is not None else "N/A"
    print(
        f"  {str(r['品牌']):<10} SKU={r['sku_count']:<3} "
        f"sales={r['total_sales']:>12,.0f} rank={rank_str:<6} conv={conv_str}"
    )

print("\n" + "=" * 80)
print("2. Category Structure (which cat litter type sells best)")
print("=" * 80)
for r in q("""
    SELECT `三级类目`, `四级类目`, `类目编码`, COUNT(*) AS sku_count,
        SUM(CASE WHEN `渠道`='京东自营' THEN `销售额` ELSE 0 END) AS jd_sales,
        SUM(CASE WHEN `渠道`='抖音' THEN `销售额` ELSE 0 END) AS dy_sales
    FROM cat_litter.channel_summary
    WHERE `品牌` IS NOT NULL AND `品牌` != '' AND `品牌` != 'null'
    GROUP BY `三级类目`, `四级类目`, `类目编码`
    ORDER BY jd_sales DESC
    LIMIT 10
"""):
    cat3 = str(r["三级类目"] or "")
    cat4 = str(r["四级类目"] or "")
    code = str(r["类目编码"] or "")
    print(
        f"  {cat3}/{cat4} ({code}) SKU={r['sku_count']} "
        f"JD={r['jd_sales']:>10,.0f} Douyin={r['dy_sales']:>10,.0f}"
    )

print("\n" + "=" * 80)
print("3. Fuwan vs Top Competitors (JD Self)")
print("=" * 80)
brands = ["福丸", "pidan", "网易严选", "许翠花", "凯锐思", "洁客", "CATLINK", "小佩"]
for brand in brands:
    rows = q(f"""
        SELECT `品牌`, COUNT(*) AS skus,
            SUM(`销售额`) AS sales, ROUND(AVG(`排名`), 1) AS rank,
            ROUND(AVG(`件单价`), 0) AS price, ROUND(AVG(`转化率`), 1) AS conv
        FROM cat_litter.channel_summary
        WHERE `渠道` = '京东自营' AND `品牌` = '{brand}'
        GROUP BY `品牌`
    """)
    if rows:
        r = rows[0]
        sales = r["sales"] if r["sales"] else 0
        rank_str = str(r["rank"]) if r["rank"] is not None else "N/A"
        price_str = str(int(r["price"])) if r["price"] is not None else "N/A"
        conv_str = f"{r['conv']}%" if r["conv"] is not None else "N/A"
        print(
            f"  {str(r['品牌']):<8} SKU={r['skus']:<3} "
            f"sales={sales:>12,.0f} rank={rank_str:<6} "
            f"price={price_str:<6} conv={conv_str}"
        )
    else:
        print(f"  {brand:<8} (not found in JD Self)")

print("\n" + "=" * 80)
print("4. Fuwan Cross-Channel Performance")
print("=" * 80)
for r in q("""
    SELECT `渠道`, COUNT(*) AS skus,
        SUM(`销售额`) AS sales, ROUND(AVG(`销量`), 0) AS avg_vol,
        ROUND(AVG(`件单价`), 0) AS avg_price
    FROM cat_litter.channel_summary
    WHERE `品牌` = '福丸'
    GROUP BY `渠道`
    ORDER BY sales DESC
"""):
    sales = r["sales"] if r["sales"] else 0
    avg_vol = r["avg_vol"] if r["avg_vol"] else 0
    avg_price = r["avg_price"] if r["avg_price"] else "N/A"
    print(
        f"  {str(r['渠道']):<8} SKU={r['skus']:<3} "
        f"sales={sales:>12,.0f} avg_vol={avg_vol:>8,.0f} price={avg_price}"
    )

print("\n" + "=" * 80)
print("5. Price Band Analysis (JD Self)")
print("=" * 80)
for r in q("""
    SELECT
        CASE
            WHEN `件单价` < 50 THEN '0-50'
            WHEN `件单价` < 100 THEN '50-100'
            WHEN `件单价` < 200 THEN '100-200'
            WHEN `件单价` < 300 THEN '200-300'
            ELSE '300+'
        END AS price_band,
        COUNT(*) AS sku_count,
        SUM(`销售额`) AS total_sales,
        ROUND(AVG(`转化率`), 1) AS avg_conv
    FROM cat_litter.channel_summary
    WHERE `渠道` = '京东自营' AND `件单价` IS NOT NULL AND `件单价` > 0
    GROUP BY 1
    ORDER BY MIN(`件单价`)
"""):
    conv_str = f"{r['avg_conv']}%" if r["avg_conv"] is not None else "N/A"
    sales = r["total_sales"] if r["total_sales"] else 0
    print(
        f"  {str(r['price_band']):<12} SKU={r['sku_count']:<4} "
        f"sales={sales:>12,.0f} conv={conv_str}"
    )

print("\n" + "=" * 80)
print("6. Douyin Top Sellers")
print("=" * 80)
for r in q("""
    SELECT `品牌`, `产品名称`, `销售额`, `销量`
    FROM cat_litter.channel_summary
    WHERE `渠道` = '抖音' AND `销售额` IS NOT NULL AND `销售额` > 100000
    ORDER BY `销售额` DESC
    LIMIT 10
"""):
    print(
        f"  {str(r['品牌']):<6} {str(r['产品名称'])[:30]:<31} "
        f"sales={r['销售额']:>10,.0f} vol={r['销量']:>8,.0f}"
    )

print("\n" + "=" * 80)
print("7. High Conversion Products (JD, conv>15%)")
print("=" * 80)
for r in q("""
    SELECT `品牌`, `产品名称`, `渠道`, `转化率`, `排名`, `销售额`
    FROM cat_litter.channel_summary
    WHERE `转化率` > 15 AND `渠道` IN ('京东自营', '京东POP')
    ORDER BY `转化率` DESC
    LIMIT 10
"""):
    sales = r["销售额"] if r["销售额"] else 0
    rank_str = str(r["排名"]) if r["排名"] is not None else "N/A"
    print(
        f"  [{r['渠道']}] {str(r['品牌']):<8} {str(r['产品名称'])[:25]:<26} "
        f"conv={r['转化率']}% rank={rank_str} sales={sales:>10,.0f}"
    )

print("\n" + "=" * 80)
print("8. Cross-Channel Brand Comparison (Top 10 brands)")
print("=" * 80)
top_brands = q("""
    SELECT `品牌`, SUM(`销售额`) AS total
    FROM cat_litter.channel_summary
    WHERE `品牌` IS NOT NULL AND `品牌` != '' AND `品牌` != 'null'
        AND `销售额` IS NOT NULL
    GROUP BY `品牌` ORDER BY total DESC LIMIT 10
""")
for brand_row in top_brands:
    brand = brand_row["品牌"]
    channels = q(f"""
        SELECT `渠道`, SUM(`销售额`) AS sales, COUNT(*) AS skus,
            ROUND(AVG(`件单价`), 0) AS avg_price
        FROM cat_litter.channel_summary
        WHERE `品牌` = '{brand}' AND `销售额` IS NOT NULL
        GROUP BY `渠道` ORDER BY sales DESC
    """)
    ch_parts = []
    for ch in channels:
        s = ch["sales"] if ch["sales"] else 0
        p = int(ch["avg_price"]) if ch["avg_price"] else 0
        ch_parts.append(f"{ch['渠道']}={s:,.0f}(avg{p})")
    print(f"  {str(brand):<10} {' | '.join(ch_parts)}")

lh.close()
print("\nAnalysis complete!")
