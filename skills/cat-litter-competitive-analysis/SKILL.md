---
name: cat-litter-competitive-analysis
description: |
  猫砂竞品全链路分析 Skill：从多平台销售数据出发，通过 AI ETL Batch 推理自动生成
  结构化竞品主数据（品牌、类目、规格、配比），构建跨渠道汇总表，修复数据质量问题，
  最终输出品牌份额、品类结构、价格带、跨渠道对比等运营洞察。
  覆盖 5 个电商平台（抖音/天猫/天猫超市/京东自营/京东POP）、862 条竞品商品。
  当用户说"猫砂竞品分析"、"竞品主数据生成"、"跨渠道分析"、"AI ETL 案例"、
  "cat litter analysis"、"competitive analysis"、"品牌份额分析"、"价格带分析"、
  "AI 主数据"、"Batch 推理案例"时触发。
---

# 猫砂竞品全链路分析

本 Skill 指导 AI Agent 复现完整的猫砂竞品分析流程：从数据导入到运营洞察。

## 前置条件

- **AI ETL 项目已安装**: `git clone https://github.com/yunqiqiliang/ai_etl.git && cd ai_etl && pip install -e .`
- **ClickZetta Lakehouse 账号**: 需要 service/instance/workspace 信息
- **DashScope API Key**: 用于 LLM Batch 推理（`DASHSCOPE_API_KEY` 环境变量）
- **ClickZetta 凭证**: `CLICKZETTA_USERNAME` / `CLICKZETTA_PASSWORD` 环境变量
- **Python 3.10+**

## 项目结构

```
ai_etl/
├── ai_etl/              # 核心代码
│   ├── config.py        # 配置加载（.env + YAML）
│   ├── lakehouse.py     # Lakehouse 读写 + Volume 操作
│   ├── pipeline.py      # ETL 编排（table + volume 并行）
│   ├── planner.py       # AI plan + test 命令
│   └── providers/       # DashScope / ZhipuAI batch providers
├── configs/             # 猫砂各渠道的 ETL 配置
│   ├── cat_litter_douyin.yaml
│   ├── cat_litter_tmall.yaml
│   ├── cat_litter_tmall_chaoshi.yaml
│   ├── cat_litter_jd_self.yaml
│   └── cat_litter_jd_pop.yaml
├── scripts/             # 数据处理脚本
│   ├── rebuild_competitor_master_v2.py   # 合并去重生成主数据表
│   ├── fix_data_quality.py              # 数据质量修复 + channel_summary 构建
│   ├── data_quality_check.py            # 10 项数据质量检查
│   └── competitive_analysis.py          # 竞品运营分析
├── config.yaml          # 默认 ETL 配置
├── .env                 # API 凭证（不提交到 git）
└── docs/
    └── case-study-cat-litter.html       # 完整案例文档
```

---

## Phase 1: 数据导入

### 目标
将 Excel 销售数据导入 ClickZetta Lakehouse 的 `cat_litter` schema。

### 数据源
原始数据是一个 Excel 文件，包含 9 个 sheet：

| Sheet | 表名 | 行数 | 说明 |
|---|---|---|---|
| 福丸竞品主数据 | `product_master` | 46 | 福丸品牌人工标注的 SKU 主数据（标杆） |
| 猫砂抖音 | `sales_douyin` | 44 | 抖音销售数据（精确值） |
| 猫砂天猫 | `sales_tmall` | 50 | 天猫销售数据（件单价+区间销量） |
| 猫砂天猫超市 | `sales_tmall_chaoshi` | 368 | 天猫超市销售数据（指数值） |
| 猫砂京东自营 | `sales_jd_self` | 200 | 京东自营销售排行（区间值） |
| 猫砂京东pop | `sales_jd_pop` | 200 | 京东POP销售排行（区间值） |
| 类目表 | `category_mapping` | 114 | 四级类目编码映射 |

### 操作步骤

```python
from ai_etl.lakehouse import LakehouseClient
from ai_etl.config import Config

lh = LakehouseClient(config=Config())

# 1. 创建 schema
lh.session.sql("CREATE SCHEMA IF NOT EXISTS cat_litter COMMENT '猫砂竞品分析'").collect()

# 2. 用 openpyxl 读取 Excel，每个 sheet 写入一张表
# （使用 ZettaPark SDK 的 create_dataframe + save_as_table）

# 3. 为表和字段添加中文注释
lh.session.sql("ALTER TABLE cat_litter.sales_jd_self SET COMMENT '京东自营猫砂销售排行'").collect()
```

### 关键注意事项
- 各平台数据格式差异极大：抖音是精确值，天猫是区间值，天猫超市是指数值，京东是区间值
- 所有列统一用 STRING 类型导入，后续在汇总时做类型转换
- `product_master` 的 46 条数据是标杆数据，用于 few-shot 示例，不是要处理的数据

---

## Phase 2: AI 主数据生成

### 目标
用 LLM Batch 推理从电商营销标题中提取结构化主数据（品牌、类目、规格、配比）。

### Prompt 设计要点

1. **类目体系封闭**: 将 16 个类目编码作为"选择题"嵌入 system_prompt
2. **Few-shot 示例**: 从 `product_master` 选 3 个不同品牌/类目的示例
3. **JSON 结构化输出**: 要求直接返回 JSON，不要 markdown 代码块
4. **低 temperature**: 0.1，确保输出稳定

### 类目编码体系（16 个）

| 编码 | 三级类目 | 四级类目 |
|---|---|---|
| HB-TFCL | 混合猫砂 | 豆腐膨润土猫砂 |
| HB-TTCL | 混合猫砂 | 木薯豆腐膨润土猫砂 |
| HB-TPCL | 混合猫砂 | 木薯膨润土猫砂 |
| HB-TPTF | 混合猫砂 | 木薯豆腐猫砂 |
| HB-TFMN | 混合猫砂 | 豆腐矿石猫砂 |
| HB-TPMN | 混合猫砂 | 木薯混矿猫砂 |
| HB-TCO | 混合猫砂 | 豆腐混膨润土、矿 |
| HB-OCL | 混合猫砂 | 其他 |
| PB-TF | 植物猫砂 | 豆腐猫砂 |
| PB-TP | 植物猫砂 | 木薯猫砂 |
| CL-CL | 膨润土猫砂 | 膨润土猫砂 |
| MN-MN | 矿砂 | 矿砂 |
| LS-LS | 猫砂组合 | 猫砂组合 |
| OCG-OCG | 除臭颗粒 | 除臭颗粒 |
| OL-OL | 其他 | 其他 |

### 推理参数

| 参数 | 值 | 理由 |
|---|---|---|
| model | `qwen3-max` | 复杂语义理解需要强模型 |
| temperature | `0.1` | 低随机性，确保 JSON 稳定 |
| enable_thinking | `false` | 不需要思考链，节省 token |

### 执行步骤

```bash
# 方式一：逐渠道提交（推荐，可控性强）
for config in configs/cat_litter_*.yaml; do
    PYTHONPATH=. python -c "
from pathlib import Path
from ai_etl.config import Config
from ai_etl.pipeline import AIETLPipeline
cfg = Config(config_path=Path('$config'))
p = AIETLPipeline(config=cfg)
p.run()
p.close()
"
done

# 方式二：用 test 命令先验证 prompt 效果
python -m ai_etl test --config configs/cat_litter_jd_self.yaml --count 2
```

### 预期结果
- 5 个渠道各生成一张 `*_product_master` 表
- 862/862 全部成功，0 失败
- 每行包含 `ai_result` 列（JSON 格式的结构化主数据）

---

## Phase 3: 合并去重 → 统一主数据表

### 目标
将 5 个渠道的 AI 结果合并为一张 `competitor_product_master` 表，结构与 `product_master` 对齐。

### 执行

```bash
PYTHONPATH=. python scripts/rebuild_competitor_master_v2.py
```

### 核心逻辑
1. **UNION ALL**: 5 个渠道的 AI 结果表 + JOIN 回源表取原始商品名称
2. **去重**: `ROW_NUMBER() OVER (PARTITION BY platform, source_key)`
3. **JSON 解析**: `get_json_object(ai_result, '$.品牌')` 等
4. **主数据编码生成**: `品牌缩写-PS-CL-类目编码-序号-规格克数`

### 输出表结构（14 列）
- 与 `product_master` 对齐的 9 列：品牌、一级类目、二级类目、三级类目、四级类目、产品名称、主数据、规格、配比
- 溯源字段 5 列：数据来源平台、源数据主键、源数据商品名称、类目编码、ai_raw

---

## Phase 4: 跨渠道汇总 + 数据质量修复

### 目标
将主数据与销售数据 JOIN，构建 `channel_summary` 表，并修复数据质量问题。

### 执行

```bash
PYTHONPATH=. python scripts/fix_data_quality.py
```

### 必须修复的 5 个数据质量问题

| # | 问题 | 原因 | 修复 |
|---|---|---|---|
| 1 | VOID 类型错误 | CTAS 用抖音数据建表，NULL 列推断为 VOID | 用 `CREATE TABLE` 显式定义 schema |
| 2 | 京东件单价异常 | `成交客单价` 有 `￥10万~￥20万` 的值 | 件单价 > 5000 置 NULL |
| 3 | 天猫超市转化率 > 100% | `转化指数` 是指数不是百分比 | 不放入转化率列 |
| 4 | 天猫销量全部固定 37500 | 所有行用同一个中位数 | 每行独立解析区间中位数 |
| 5 | 京东转化率 > 100% | 区间值解析后部分超 100% | 转化率 > 100% 置 NULL |

### channel_summary 表结构

```sql
CREATE TABLE cat_litter.channel_summary (
    `品牌`     STRING,   -- AI 主数据
    `三级类目` STRING,
    `四级类目` STRING,
    `类目编码` STRING,
    `产品名称` STRING,
    `主数据`   STRING,
    `规格`     STRING,
    `渠道`     STRING,   -- 抖音/天猫/天猫超市/京东自营/京东POP
    `销售额`   DOUBLE,   -- 统一为数值（区间取中位数）
    `销量`     DOUBLE,
    `件单价`   DOUBLE,
    `浏览量`   DOUBLE,
    `转化率`   DOUBLE,
    `排名`     DOUBLE,
    `数据精度` STRING    -- 精确值/区间中位数/指数值
)
```

### 各渠道数据处理逻辑

| 渠道 | 销售额 | 销量 | 件单价 | 转化率 | 特殊处理 |
|---|---|---|---|---|---|
| 抖音 | 精确值 | 精确值 | 销售额/销量 | 无 | 直接用 |
| 天猫 | 件单价×销量 | 区间中位数 | 精确值 | 无 | 每行独立解析支付单量 |
| 天猫超市 | 成交热度(指数) | 成交人气(指数) | 无 | 无 | 转化指数不放入转化率列 |
| 京东自营 | 区间中位数 | 区间中位数 | 区间中位数(cap 5000) | 区间中位数(cap 100%) | 区间值解析函数 |
| 京东POP | 同京东自营 | 同上 | 同上 | 同上 | 同上 |

### 区间值解析函数

```python
def parse_range_mid(val):
    """解析区间值取中位数: '￥200万 ~ ￥400万' → 3000000"""
    val = str(val).replace('￥', '').replace(',', '').replace('%', '').strip()
    m = re.match(r'([\d.]+)(万)?\s*~\s*([\d.]+)(万)?', val)
    if not m:
        return float(val)  # 非区间值直接转数字
    lo = float(m.group(1)) * (10000 if m.group(2) else 1)
    hi = float(m.group(3)) * (10000 if m.group(4) else 1)
    return (lo + hi) / 2
```

### 验证

```bash
PYTHONPATH=. python scripts/data_quality_check.py
```

预期：862 行，件单价最高 ≤ 5000，转化率最高 ≤ 100%，天猫销量有多个不同值。

---

## Phase 5: 竞品运营分析

### 目标
从 `channel_summary` 中提取运营洞察。

### 执行

```bash
PYTHONPATH=. python scripts/competitive_analysis.py
```

### 分析维度

#### 5.1 品牌市场份额（京东自营，数据最完整）

```sql
SELECT `品牌`, COUNT(*) AS sku_count,
    SUM(`销售额`) AS total_sales,
    ROUND(AVG(`排名`), 1) AS avg_rank,
    ROUND(AVG(`转化率`), 1) AS avg_conv
FROM cat_litter.channel_summary
WHERE `渠道` = '京东自营' AND `品牌` IS NOT NULL AND `品牌` != ''
    AND `销售额` > 0
GROUP BY `品牌`
ORDER BY total_sales DESC
LIMIT 15
```

预期结果（Top 5）：
1. 许翠花 ¥12,750,000（14 SKU，排名 43.3）
2. pidan ¥9,625,000（16 SKU，排名 72.1）
3. 网易严选 ¥4,340,000（13 SKU，转化率 30.2%）
4. 凯锐思 ¥2,815,000
5. 京东京造 ¥2,775,000
6. **福丸 ¥2,745,000**（5 SKU，排名 59.4，转化率 26.5%）

#### 5.2 品类结构

```sql
SELECT `三级类目`, `四级类目`, `类目编码`, COUNT(*) AS sku_count,
    SUM(CASE WHEN `渠道`='京东自营' THEN `销售额` ELSE 0 END) AS jd_sales,
    SUM(CASE WHEN `渠道`='抖音' THEN `销售额` ELSE 0 END) AS dy_sales
FROM cat_litter.channel_summary
GROUP BY `三级类目`, `四级类目`, `类目编码`
ORDER BY jd_sales DESC
```

#### 5.3 价格带分析（京东自营）

```sql
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
```

预期：100-200 元是销售额最高的价格带（¥25,195,000），50-100 元 SKU 最多（68 个）。

#### 5.4 福丸跨渠道表现

```sql
SELECT `渠道`, COUNT(*) AS skus,
    SUM(`销售额`) AS sales,
    ROUND(AVG(`件单价`), 0) AS avg_price
FROM cat_litter.channel_summary
WHERE `品牌` = '福丸'
GROUP BY `渠道`
ORDER BY sales DESC
```

预期：天猫 ¥10,322,688 > 京东自营 ¥2,745,000 > 抖音 ¥2,649,151

#### 5.5 跨渠道品牌对比

```sql
-- Top 10 品牌在各渠道的销售额
SELECT `品牌`, `渠道`, SUM(`销售额`) AS sales, COUNT(*) AS skus
FROM cat_litter.channel_summary
WHERE `品牌` IN (SELECT `品牌` FROM cat_litter.channel_summary
    WHERE `销售额` IS NOT NULL GROUP BY `品牌` ORDER BY SUM(`销售额`) DESC LIMIT 10)
GROUP BY `品牌`, `渠道`
ORDER BY `品牌`, sales DESC
```

---

## 关键经验总结

### Prompt 设计
- **类目体系封闭**: 16 个编码作为选择题，确保 100% 匹配率
- **Few-shot 示例**: 3 个不同品牌/类目的示例比 10 个同质示例效果好
- **低 temperature (0.1)**: 确保 JSON 输出稳定

### 数据质量
- **数据正确是分析前提**: 不修复就分析 = 误导决策
- **显式 schema**: 用 `CREATE TABLE` 而非 CTAS，避免 NULL 列推断为 VOID
- **区间值解析**: 京东/天猫的区间值需要逐行解析，不能用统一中位数
- **指数 ≠ 百分比**: 天猫超市的转化指数不是转化率

### Batch API
- **并行提交**: 5 个渠道同时提交，总等待 = 最慢的那个
- **100% 成功率**: 服务端重试机制保证完整性
- **50% 成本**: 相比实时 API

### 运营洞察
- **京东数据最完整**: 有销售额、件单价、转化率、排名，是分析首选
- **SKU 数量是关键杠杆**: 福丸 5 SKU vs pidan 16 SKU，扩充 SKU 是最直接的增长手段
- **渠道差异化**: 不同渠道品类偏好不同，需要差异化选品

---

## 参考文件

- [完整案例文档 (HTML)](../docs/case-study-cat-litter.html)
- [AI ETL README](../README.md)
- [Batch AI Function 设计](../docs/batch-ai-function-design.md)
- [数据质量修复脚本](../scripts/fix_data_quality.py)
- [竞品分析脚本](../scripts/competitive_analysis.py)
- [京东POP 配置示例](../configs/cat_litter_jd_pop.yaml)
