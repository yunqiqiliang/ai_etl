"""对比 product_master 和 competitor_product_master"""
from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient
lh = LakehouseClient(config=Config())

print("=" * 80)
print("product_master (客户期望的结构)")
print("=" * 80)
s1 = lh.session.table("cat_litter.product_master").schema
for f in s1.fields:
    print(f"  {f.name.strip('`'):<12} {type(f.datatype).__name__}")

print("\n样本:")
rows = lh.session.sql("SELECT * FROM cat_litter.product_master LIMIT 5").collect()
for r in rows:
    print(f"  品牌={r['品牌']} | 一级={r['一级类目']} | 二级={r['二级类目']} | 三级={r['三级类目']} | 四级={r['四级类目']}")
    print(f"    产品名称={r['产品名称']} | 主数据={r['主数据']} | 规格={r['规格']} | 配比={r['配比']}")

print("\n" + "=" * 80)
print("competitor_product_master (AI 生成的)")
print("=" * 80)
s2 = lh.session.table("cat_litter.competitor_product_master").schema
for f in s2.fields:
    print(f"  {f.name.strip('`'):<14} {type(f.datatype).__name__}")

print("\n样本:")
rows = lh.session.sql("""
    SELECT * FROM cat_litter.competitor_product_master
    WHERE brand != '福丸' LIMIT 5
""").collect()
for r in rows:
    print(f"  platform={r['platform']} | brand={r['brand']} | cat3={r['cat_level3']} | cat4={r['cat_level4']} | code={r['cat_code']}")
    print(f"    product_name={r['product_name']} | formula={r['formula']} | spec={r['spec_kg']}")
    print(f"    source_key={str(r['source_key'])[:40]} | source_name={str(r['source_name'])[:40]}")

print("\n" + "=" * 80)
print("差异分析")
print("=" * 80)
print("\nproduct_master 列:       品牌, 一级类目, 二级类目, 三级类目, 四级类目, 产品名称, 主数据, 规格, 配比")
print("competitor 列:           platform, source_key, source_name, brand, cat_level3, cat_level4, cat_code, product_name, formula, spec_kg, ai_raw")
print()
print("差异:")
print("  1. competitor 缺少: 一级类目, 二级类目 (固定值: 宠物用品, 猫砂)")
print("  2. competitor 缺少: 主数据编码 (需要按规则生成)")
print("  3. competitor 多了: platform, source_key, source_name, cat_code, ai_raw (溯源字段)")
print("  4. 列名不同: brand vs 品牌, cat_level3 vs 三级类目, etc.")
print("  5. 规格格式: product_master 是纯数字(2.5), competitor 有的带'kg'后缀")

# 检查规格格式
print("\n--- competitor 规格格式检查 ---")
rows = lh.session.sql("""
    SELECT spec_kg, COUNT(*) AS cnt
    FROM cat_litter.competitor_product_master
    WHERE spec_kg IS NOT NULL AND spec_kg != ''
    GROUP BY spec_kg ORDER BY cnt DESC LIMIT 15
""").collect()
for r in rows:
    print(f"  '{r['spec_kg']}': {r['cnt']}")

# 检查空值
print("\n--- competitor 空值检查 ---")
for col in ['brand', 'cat_level3', 'cat_code', 'product_name', 'formula', 'spec_kg']:
    cnt = lh.session.sql(f"""
        SELECT COUNT(*) AS c FROM cat_litter.competitor_product_master
        WHERE {col} IS NULL OR {col} = '' OR {col} = 'null'
    """).collect()[0]["c"]
    if cnt > 0:
        print(f"  {col}: {cnt} 个空值")

lh.close()
