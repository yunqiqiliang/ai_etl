"""竞品运营分析 — 从 channel_summary 中提取运营洞察"""
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient
lh = LakehouseClient(config=Config())

def q(sql):
    return lh.session.sql(sql).collect()

print("=" * 80)
print("1. 品牌市场份额（按京东自营销售额排名，最可靠的数据源）")
print("=" * 80)
for r in q("""
    SELECT `品牌`, COUNT(*) AS sku_count,
        SUM(`销售额`) AS total_sales,
        ROUND(AVG(`排名`), 1) AS avg_rank,
        ROUND(AVG(`转化率`), 1) AS avg_conv
    FROM cat_litter.channel_summary
    WHERE `渠道` = '京东自营' AND `品牌` != '' AND `销售额` > 0
    GROUP BY `品牌`
    ORDER BY total_sales DESC
    LIMIT 15
"""):
    print(f"  {r['品牌']:<10} SKU={r['sku_count']:<3} 销售额={r['total_sales']:>12,.0f} 平均排名={r['avg_rank']:<6} 转化率={r['avg_conv']}%")

print("\n" + "=" * 80)
print("2. 品类结构分析（哪种猫砂卖得最好）")
print("=" * 80)
for r in q("""
    SELECT `三级类目`, `四级类目`, `类目编码`, COUNT(*) AS sku_count,
        SUM(CASE WHEN `渠道`='京东自营' THEN `销售额` ELSE 0 END) AS jd_sales,
        SUM(CASE WHEN `渠道`='抖音' THEN `销售额` ELSE 0 END) AS dy_sales
    FROM cat_litter.channel_summary
    WHERE `品牌` != ''
    GROUP BY `三级类目`, `四级类目`, `类目编码`
    ORDER BY jd_sales DESC
    LIMIT 10
"""):
    print(f"  {r['三级类目']}/{r['四级类目']} ({r['类目编码']}) SKU={r['sku_count']} 京东={r['jd_sales']:>10,.0f} 抖音={r['dy_sales']:>10,.0f}")

print("\n" + "=" * 80)
print("3. 福丸 vs 竞品头部品牌对比（京东自营）")
print("=" * 80)
brands = ['福丸', 'pidan', '网易严选', '许翠花', '凯锐思', '洁客']
for brand in brands:
    rows = q(f"""
        SELECT `品牌`, COUNT(*) AS skus,
            SUM(`销售额`) AS sales, ROUND(AVG(`排名`),1) AS rank,
            ROUND(AVG(`件单价`),0) AS price, ROUND(AVG(`转化率`),1) AS conv
        FROM cat_litter.channel_summary
        WHERE `渠道`='京东自营' AND `品牌`='{brand}'
        GROUP BY `品牌`
    """)
    if rows:
        r = rows[0]
        print(f"  {r['品牌']:<8} SKU={r['skus']:<3} 销售额={r['sales']:>12,.0f} 排名={r['rank']:<6} 均价={r['price']:<6} 转化={r['conv']}%")

print("\n" + "=" * 80)
print("4. 福丸各渠道表现")
print("=" * 80)
for r in q("""
    SELECT `渠道`, COUNT(*) AS skus,
        SUM(`销售额`) AS sales, ROUND(AVG(`销量`),0) AS avg_vol,
        ROUND(AVG(`件单价`),0) AS avg_price
    FROM cat_litter.channel_summary
    WHERE `品牌` = '福丸'
    GROUP BY `渠道`
    ORDER BY sales DESC
"""):
    print(f"  {r['渠道']:<8} SKU={r['skus']:<3} 销售额={r['sales']:>12,.0f} 平均销量={r['avg_vol']:>8,.0f} 均价={r['avg_price']}")

print("\n" + "=" * 80)
print("5. 价格带分析（京东自营，按件单价分段）")
print("=" * 80)
for r in q("""
    SELECT
        CASE
            WHEN `件单价` < 50 THEN '0-50元'
            WHEN `件单价` < 100 THEN '50-100元'
            WHEN `件单价` < 200 THEN '100-200元'
            WHEN `件单价` < 300 THEN '200-300元'
            ELSE '300元+'
        END AS price_band,
        COUNT(*) AS sku_count,
        SUM(`销售额`) AS total_sales,
        ROUND(AVG(`转化率`), 1) AS avg_conv
    FROM cat_litter.channel_summary
    WHERE `渠道` = '京东自营' AND `件单价` IS NOT NULL AND `件单价` > 0
    GROUP BY 1
    ORDER BY MIN(`件单价`)
"""):
    print(f"  {r['price_band']:<12} SKU={r['sku_count']:<4} 销售额={r['total_sales']:>12,.0f} 转化率={r['avg_conv']}%")

print("\n" + "=" * 80)
print("6. 抖音渠道结构分析（直播 vs 视频 vs 商品卡）")
print("=" * 80)
for r in q("""
    SELECT `品牌`, `产品名称`,
        `销售额`, `销量`,
        `渠道占比_直播` AS live, `渠道占比_视频` AS video, `渠道占比_商品卡` AS card
    FROM cat_litter.channel_summary
    WHERE `渠道` = '抖音' AND `销售额` > 100000
    ORDER BY `销售额` DESC
    LIMIT 10
"""):
    print(f"  {r['品牌']:<6} {str(r['产品名称'])[:25]:<26} 销售额={r['销售额']:>10,.0f} 直播={r['live']} 视频={r['video']} 商品卡={r['card']}")

print("\n" + "=" * 80)
print("7. 高转化率商品（京东，转化率>25%）")
print("=" * 80)
for r in q("""
    SELECT `品牌`, `产品名称`, `渠道`, `转化率`, `排名`, `销售额`
    FROM cat_litter.channel_summary
    WHERE `转化率` > 25 AND `渠道` IN ('京东自营', '京东POP')
    ORDER BY `转化率` DESC
    LIMIT 10
"""):
    print(f"  [{r['渠道']}] {r['品牌']:<8} {str(r['产品名称'])[:25]:<26} 转化={r['转化率']}% 排名={r['排名']} 销售额={r['销售额']:>10,.0f}")

lh.close()
print("\n分析完成!")
