"""Check JD data formats"""
from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient
lh = LakehouseClient(config=Config())

print("--- JD self: 成交金额 ---")
for r in lh.session.sql("SELECT DISTINCT `成交金额` FROM cat_litter.sales_jd_self WHERE `成交金额` IS NOT NULL AND `成交金额` != '' LIMIT 8").collect():
    print(f"  {repr(r['成交金额'])}")

print("\n--- JD self: 成交商品件数 ---")
for r in lh.session.sql("SELECT DISTINCT `成交商品件数` FROM cat_litter.sales_jd_self WHERE `成交商品件数` IS NOT NULL AND `成交商品件数` != '' LIMIT 8").collect():
    print(f"  {repr(r['成交商品件数'])}")

print("\n--- JD self: 浏览量 ---")
for r in lh.session.sql("SELECT DISTINCT `浏览量` FROM cat_litter.sales_jd_self WHERE `浏览量` IS NOT NULL AND `浏览量` != '' LIMIT 8").collect():
    print(f"  {repr(r['浏览量'])}")

print("\n--- JD self: 成交转化率 ---")
for r in lh.session.sql("SELECT DISTINCT `成交转化率` FROM cat_litter.sales_jd_self WHERE `成交转化率` IS NOT NULL AND `成交转化率` != '' LIMIT 8").collect():
    print(f"  {repr(r['成交转化率'])}")

print("\n--- JD self: 成交客单价 ---")
for r in lh.session.sql("SELECT DISTINCT `成交客单价` FROM cat_litter.sales_jd_self WHERE `成交客单价` IS NOT NULL AND `成交客单价` != '' LIMIT 8").collect():
    print(f"  {repr(r['成交客单价'])}")

lh.close()
