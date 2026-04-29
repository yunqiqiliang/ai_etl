"""修复数据质量问题并重建 channel_summary

修复的问题：
1. 京东件单价异常 — 成交客单价字段有 ￥10万~￥20万 的值，cap at 5000
2. 天猫超市转化率>100% — 转化指数是指数不是百分比，不放入转化率列
3. 天猫销量全部固定37500 — 每行用各自的支付单量区间中位数
4. 空品牌名
5. VOID 类型问题 — 用 CREATE TABLE 显式定义 schema，避免 NULL 推断为 VOID
"""
import re
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient

lh = LakehouseClient(config=Config())


def parse_range_mid(val):
    """解析区间值取中位数: '￥200万 ~ ￥400万' → 3000000"""
    if not val or str(val).strip() == "":
        return None
    val = str(val).replace(",", "").strip()
    val_clean = val.replace("￥", "").replace("%", "").strip()
    m = re.match(r"([\d.]+)(万)?\s*~\s*([\d.]+)(万)?", val_clean)
    if not m:
        try:
            return float(val_clean)
        except ValueError:
            return None
    lo = float(m.group(1)) * (10000 if m.group(2) else 1)
    hi = float(m.group(3)) * (10000 if m.group(4) else 1)
    return (lo + hi) / 2


def parse_tmall_volume(val):
    """天猫支付单量: '2.5万 ~ 5万' → 37500"""
    if not val or str(val).strip() == "":
        return None
    return parse_range_mid(val)


# ── Step 1: 用显式 schema 创建表，避免 VOID 类型 ──────────────
lh.session.sql("DROP TABLE IF EXISTS cat_litter.channel_summary").collect()

print("创建 channel_summary 表（显式 schema）...")
lh.session.sql("""
CREATE TABLE cat_litter.channel_summary (
    `品牌`       STRING,
    `三级类目`   STRING,
    `四级类目`   STRING,
    `类目编码`   STRING,
    `产品名称`   STRING,
    `主数据`     STRING,
    `规格`       STRING,
    `渠道`       STRING,
    `销售额`     DOUBLE,
    `销量`       DOUBLE,
    `件单价`     DOUBLE,
    `浏览量`     DOUBLE,
    `转化率`     DOUBLE,
    `排名`       DOUBLE,
    `数据精度`   STRING
)
COMMENT '跨渠道竞品销售汇总(已修复数据质量)'
""").collect()
print("  表结构创建 ✅")

# ── Step 2: 抖音 — 数据质量好，直接 INSERT ──────────────────
lh.session.sql("""
INSERT INTO cat_litter.channel_summary
SELECT
    m.`品牌`, m.`三级类目`, m.`四级类目`, m.`类目编码`, m.`产品名称`, m.`主数据`, m.`规格`,
    '抖音' AS `渠道`,
    CAST(s.`销售额` AS DOUBLE) AS `销售额`,
    CAST(s.`销量` AS DOUBLE) AS `销量`,
    CASE WHEN CAST(s.`销量` AS DOUBLE) > 0
         THEN ROUND(CAST(s.`销售额` AS DOUBLE) / CAST(s.`销量` AS DOUBLE), 2)
         ELSE NULL END AS `件单价`,
    CAST(NULL AS DOUBLE) AS `浏览量`,
    CAST(NULL AS DOUBLE) AS `转化率`,
    CAST(NULL AS DOUBLE) AS `排名`,
    '精确值' AS `数据精度`
FROM cat_litter.competitor_product_master m
JOIN cat_litter.sales_douyin s ON m.`源数据主键` = s.`商品`
WHERE m.`数据来源平台` = '抖音'
""").collect()
dy_cnt = lh.session.sql(
    "SELECT COUNT(*) AS c FROM cat_litter.channel_summary WHERE `渠道`='抖音'"
).collect()[0]["c"]
print(f"  抖音 ✅ ({dy_cnt} 行)")

# ── Step 3: 天猫超市 — 转化指数不是百分比，不放入转化率列 ────
lh.session.sql("""
INSERT INTO cat_litter.channel_summary
SELECT
    m.`品牌`, m.`三级类目`, m.`四级类目`, m.`类目编码`, m.`产品名称`, m.`主数据`, m.`规格`,
    '天猫超市',
    CAST(s.`成交热度` AS DOUBLE),
    CAST(s.`成交人气` AS DOUBLE),
    CAST(NULL AS DOUBLE),
    CAST(s.`访问热度` AS DOUBLE),
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    '指数值(非百分比)'
FROM cat_litter.competitor_product_master m
JOIN cat_litter.sales_tmall_chaoshi s ON m.`源数据主键` = s.`商品ID`
WHERE m.`数据来源平台` = '天猫超市'
""").collect()
tc_cnt = lh.session.sql(
    "SELECT COUNT(*) AS c FROM cat_litter.channel_summary WHERE `渠道`='天猫超市'"
).collect()[0]["c"]
print(f"  天猫超市 ✅ ({tc_cnt} 行, 转化指数不再放入转化率列)")

# ── Step 4: 天猫 — 每行用各自的支付单量区间中位数 ────────────
from clickzetta.zettapark.types import StringType, DoubleType, StructField, StructType

schema = StructType([
    StructField("品牌", StringType()),
    StructField("三级类目", StringType()),
    StructField("四级类目", StringType()),
    StructField("类目编码", StringType()),
    StructField("产品名称", StringType()),
    StructField("主数据", StringType()),
    StructField("规格", StringType()),
    StructField("渠道", StringType()),
    StructField("销售额", DoubleType()),
    StructField("销量", DoubleType()),
    StructField("件单价", DoubleType()),
    StructField("浏览量", DoubleType()),
    StructField("转化率", DoubleType()),
    StructField("排名", DoubleType()),
    StructField("数据精度", StringType()),
])

tmall_rows = lh.session.sql("""
    SELECT m.`品牌`, m.`三级类目`, m.`四级类目`, m.`类目编码`, m.`产品名称`, m.`主数据`, m.`规格`,
           s.`件单价`, s.`支付单量`
    FROM cat_litter.competitor_product_master m
    JOIN cat_litter.sales_tmall s ON m.`源数据主键` = s.`商品编码`
    WHERE m.`数据来源平台` = '天猫'
""").collect()

tmall_data = []
for r in tmall_rows:
    price = float(r["件单价"]) if r["件单价"] else None
    vol = parse_tmall_volume(r["支付单量"])
    sales = price * vol if price and vol else None
    tmall_data.append([
        r["品牌"], r["三级类目"], r["四级类目"], r["类目编码"],
        r["产品名称"], r["主数据"], r["规格"],
        "天猫", sales, vol, price, None, None, None, "区间中位数估算",
    ])

if tmall_data:
    df = lh.session.create_dataframe(tmall_data, schema=schema)
    df.write.save_as_table("cat_litter.channel_summary", mode="append")
print(f"  天猫 ✅ ({len(tmall_data)} 行, 每行用各自的支付单量)")

# ── Step 5: 京东自营/POP — 件单价 cap at 5000, 转化率 cap at 100% ──
for platform, table in [
    ("京东自营", "cat_litter.sales_jd_self"),
    ("京东POP", "cat_litter.sales_jd_pop"),
]:
    rows = lh.session.sql(f"""
        SELECT m.`品牌`, m.`三级类目`, m.`四级类目`, m.`类目编码`, m.`产品名称`, m.`主数据`, m.`规格`,
               s.`成交金额`, s.`成交商品件数`, s.`成交客单价`, s.`成交转化率`, s.`浏览量`, s.`排名`
        FROM cat_litter.competitor_product_master m
        JOIN {table} s ON m.`源数据主键` = s.`商品编号`
        WHERE m.`数据来源平台` = '{platform}'
    """).collect()

    jd_data = []
    for r in rows:
        sales = parse_range_mid(r["成交金额"])
        volume = parse_range_mid(r["成交商品件数"])
        raw_price = parse_range_mid(r["成交客单价"])
        conv = parse_range_mid(r["成交转化率"])
        traffic = parse_range_mid(r["浏览量"])
        rank_val = int(r["排名"]) if r["排名"] else None

        # 修复：客单价>5000 的是异常值，置为 NULL
        price = raw_price if raw_price and raw_price < 5000 else None
        # 修复：转化率>100% 的是异常值，置为 NULL
        if conv and conv > 100:
            conv = None

        jd_data.append([
            r["品牌"], r["三级类目"], r["四级类目"], r["类目编码"],
            r["产品名称"], r["主数据"], r["规格"],
            platform, sales, volume, price, traffic, conv,
            float(rank_val) if rank_val else None, "区间中位数",
        ])

    if jd_data:
        df = lh.session.create_dataframe(jd_data, schema=schema)
        df.write.save_as_table("cat_litter.channel_summary", mode="append")
    print(
        f"  {platform} ✅ ({len(jd_data)} 行, 客单价>5000置NULL, 转化率>100%置NULL)"
    )

# ── Step 6: 验证 ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("修复后验证")
print("=" * 70)

cnt = lh.session.sql(
    "SELECT COUNT(*) AS c FROM cat_litter.channel_summary"
).collect()[0]["c"]
print(f"\n总行数: {cnt}")

print("\n各渠道行数:")
for r in lh.session.sql("""
    SELECT `渠道`, COUNT(*) AS cnt FROM cat_litter.channel_summary
    GROUP BY `渠道` ORDER BY cnt DESC
""").collect():
    print(f"  {r['渠道']}: {r['cnt']}")

print("\n件单价分布:")
for r in lh.session.sql("""
    SELECT `渠道`,
        MIN(`件单价`) AS min_p, MAX(`件单价`) AS max_p, ROUND(AVG(`件单价`), 0) AS avg_p,
        COUNT(*) AS total,
        SUM(CASE WHEN `件单价` IS NULL THEN 1 ELSE 0 END) AS null_cnt
    FROM cat_litter.channel_summary
    GROUP BY `渠道`
""").collect():
    print(
        f"  {r['渠道']}: min={r['min_p']} max={r['max_p']} avg={r['avg_p']} "
        f"(null={r['null_cnt']}/{r['total']})"
    )

print("\n转化率分布:")
for r in lh.session.sql("""
    SELECT `渠道`,
        MIN(`转化率`) AS min_c, MAX(`转化率`) AS max_c, ROUND(AVG(`转化率`), 1) AS avg_c,
        SUM(CASE WHEN `转化率` IS NOT NULL THEN 1 ELSE 0 END) AS has_cnt
    FROM cat_litter.channel_summary
    GROUP BY `渠道`
""").collect():
    print(f"  {r['渠道']}: min={r['min_c']}% max={r['max_c']}% avg={r['avg_c']}% (有值={r['has_cnt']})")

print("\n天猫销量分布 (应该不再全部是37500):")
for r in lh.session.sql("""
    SELECT MIN(`销量`) AS min_v, MAX(`销量`) AS max_v,
        COUNT(DISTINCT `销量`) AS distinct_v, COUNT(*) AS total
    FROM cat_litter.channel_summary WHERE `渠道` = '天猫'
""").collect():
    print(f"  min={r['min_v']} max={r['max_v']} distinct={r['distinct_v']} total={r['total']}")

print("\n空品牌统计:")
for r in lh.session.sql("""
    SELECT `渠道`, COUNT(*) AS cnt
    FROM cat_litter.channel_summary
    WHERE `品牌` IS NULL OR `品牌` = '' OR `品牌` = 'null'
    GROUP BY `渠道`
""").collect():
    print(f"  {r['渠道']}: {r['cnt']} 条空品牌")

lh.close()
print("\n修复完成! ✅")
