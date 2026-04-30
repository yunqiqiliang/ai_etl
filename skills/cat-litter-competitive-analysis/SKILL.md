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

阅读 [references/source-table-schemas.md](references/source-table-schemas.md) 了解各源表字段和渠道差异。
阅读 [references/prompt-template.md](references/prompt-template.md) 了解完整的 Prompt 模板。

## 前置条件

- **AI ETL 项目已安装**: `git clone https://github.com/yunqiqiliang/ai_etl.git && cd ai_etl && pip install -e .`
- **ClickZetta Lakehouse 账号**: 需要 service/instance/workspace 信息
- **DashScope API Key**: 用于 LLM Batch 推理（`DASHSCOPE_API_KEY` 环境变量）
- **ClickZetta 凭证**: `CLICKZETTA_USERNAME` / `CLICKZETTA_PASSWORD` 环境变量
- **Python 3.10+**
- **openpyxl**: `pip install openpyxl`（Phase 1 Excel 导入需要）
- **原始 Excel 文件**: 包含 9 个 sheet 的猫砂销售数据

## 项目结构

```
ai_etl/
├── ai_etl/              # 核心代码
│   ├── config.py        # 配置加载（.env + YAML）
│   ├── lakehouse.py     # Lakehouse 读写 + Volume 操作
│   ├── pipeline.py      # ETL 编排（table + volume 并行）
│   ├── planner.py       # AI plan + test 命令
│   └── providers/       # DashScope / ZhipuAI batch providers
├── configs/             # 猫砂各渠道的 ETL 配置（5 个 YAML）
├── scripts/
│   ├── import_excel.py              # Phase 1: Excel 导入
│   ├── rebuild_competitor_master_v2.py  # Phase 3: 合并去重
│   ├── fix_data_quality.py          # Phase 4: 数据质量修复
│   ├── data_quality_check.py        # Phase 4: 质量检查
│   └── competitive_analysis.py      # Phase 5: 竞品分析
├── config.yaml          # 默认 ETL 配置
├── .env                 # API 凭证（不提交到 git）
└── docs/
    └── case-study-cat-litter.html   # 完整案例文档
```

---

## Phase 1: 数据导入

### 目标
将 Excel 销售数据导入 ClickZetta Lakehouse 的 `cat_litter` schema。

### 执行

```bash
PYTHONPATH=. python scripts/import_excel.py /path/to/猫砂数据.xlsx
```

脚本自动完成：创建 schema → 读取 9 个 sheet → 写入 7 张表 → 添加中文注释。

### 数据源（9 sheets → 7 张表）

| Sheet | 表名 | 行数 | 数据格式 |
|---|---|---|---|
| 福丸竞品主数据 | `product_master` | 46 | 标杆主数据（few-shot 示例来源） |
| 猫砂抖音 | `sales_douyin` | 44 | **精确值** |
| 猫砂天猫 | `sales_tmall` | 50 | 件单价精确 + **销量区间值** |
| 猫砂天猫超市 | `sales_tmall_chaoshi` | 368 | **指数值**（非真实金额） |
| 猫砂京东自营 | `sales_jd_self` | 200 | **区间值**（如 ￥50万~￥75万） |
| 猫砂京东pop | `sales_jd_pop` | 200 | **区间值** |
| 类目表 | `category_mapping` | 114 | 16 个类目编码映射 |

### 关键注意事项
- 各平台数据格式差异极大，这些差异在 Phase 4 汇总时需要逐一处理
- 所有列统一用 STRING 类型导入，后续在汇总时做类型转换
- `product_master` 的 46 条数据是标杆数据，用于 few-shot 示例，不是要处理的数据
- 详细字段说明见 [references/source-table-schemas.md](references/source-table-schemas.md)

### 验证

```sql
SELECT COUNT(*) FROM cat_litter.sales_jd_self;   -- 预期 200
SELECT COUNT(*) FROM cat_litter.product_master;   -- 预期 46
```

---

## Phase 2: AI 主数据生成（Batch 推理）

### 目标
用 LLM Batch 推理从电商营销标题中提取结构化主数据（品牌、类目、规格、配比）。

### Prompt 设计要点

1. **类目体系封闭**: 将 16 个类目编码作为"选择题"嵌入 system_prompt
2. **Few-shot 示例**: 从 `product_master` 选 3 个不同品牌/类目的示例
3. **JSON 结构化输出**: 要求直接返回 JSON，不要 markdown 代码块
4. **低 temperature (0.1)**: 确保输出稳定

完整 Prompt 见 [references/prompt-template.md](references/prompt-template.md)。

### 各渠道配置差异

每个渠道的 key_columns 和 text_column 不同，已预配置在 `configs/` 目录：

| 渠道 | 配置文件 | key_columns | text_column |
|---|---|---|---|
| 抖音 | `cat_litter_douyin.yaml` | `商品` | `商品` |
| 天猫 | `cat_litter_tmall.yaml` | `商品编码` | `链接名称` |
| 天猫超市 | `cat_litter_tmall_chaoshi.yaml` | `商品ID` | `商品名称` |
| 京东自营 | `cat_litter_jd_self.yaml` | `商品编号` | `商品名称` |
| 京东POP | `cat_litter_jd_pop.yaml` | `商品编号` | `商品名称` |

注意：抖音的 key_columns 和 text_column 都是 `商品`（同一列），因为抖音数据没有独立的商品编号。

### 执行步骤

```bash
# 先用 test 命令验证 prompt 效果（秒级返回）
python -m ai_etl test --config configs/cat_litter_jd_self.yaml --count 2

# 逐渠道提交 Batch 推理
for name in douyin tmall tmall_chaoshi jd_self jd_pop; do
    echo "=== 提交 $name ==="
    PYTHONPATH=. python -c "
from pathlib import Path
from ai_etl.config import Config
from ai_etl.pipeline import AIETLPipeline
cfg = Config(config_path=Path('configs/cat_litter_${name}.yaml'))
p = AIETLPipeline(config=cfg)
p.run()
p.close()
"
done
```

> Batch 推理在 DashScope 服务端运行，通常需要 10-60 分钟。5 个渠道并行提交后总等待约 40 分钟。

### 预期结果
- 5 个渠道各生成一张 `*_product_master` 表（如 `cat_litter.jd_self_product_master`）
- 862/862 全部成功，0 失败
- 每行包含 `ai_result` 列（JSON 格式的结构化主数据）

### 验证

```sql
SELECT COUNT(*) FROM cat_litter.jd_self_product_master;  -- 预期 200
SELECT ai_result FROM cat_litter.jd_self_product_master LIMIT 1;
-- 预期: {"品牌":"pidan","三级类目":"混合猫砂",...}
```

---

## Phase 3: 合并去重 → 统一主数据表

### 目标
将 5 个渠道的 AI 结果合并为一张 `competitor_product_master` 表（862 行），结构与 `product_master` 对齐。

### 执行

```bash
PYTHONPATH=. python scripts/rebuild_competitor_master_v2.py
```

### 核心逻辑
1. **UNION ALL**: 5 个渠道的 AI 结果表 + JOIN 回源表取原始商品名称
2. **去重**: `ROW_NUMBER() OVER (PARTITION BY platform, source_key)`
3. **JSON 解析**: `get_json_object(ai_result, '$.品牌')` 等
4. **主数据编码生成**: `品牌缩写-PS-CL-类目编码-序号-规格克数`（如 `PD-PS-CL-HB-TFCL-0001-3600`）

### 输出表结构（14 列）
- 与 `product_master` 对齐的 9 列：品牌、一级类目、二级类目、三级类目、四级类目、产品名称、主数据、规格、配比
- 溯源字段 5 列：数据来源平台、源数据主键、源数据商品名称、类目编码、ai_raw

### 验证

```sql
SELECT COUNT(*) FROM cat_litter.competitor_product_master;  -- 预期 862
-- 对比结构
SELECT * FROM cat_litter.product_master LIMIT 1;
SELECT * FROM cat_litter.competitor_product_master LIMIT 1;
```

---

## Phase 4: 跨渠道汇总 + 数据质量修复

### 目标
将主数据与销售数据 JOIN，构建 `channel_summary` 表，并修复数据质量问题。

**这是整个流程中最关键的一步。** 数据正确是分析的基本前提，不修复就分析 = 误导决策。

### 执行

```bash
# 修复数据质量并构建 channel_summary
PYTHONPATH=. python scripts/fix_data_quality.py

# 运行 10 项数据质量检查
PYTHONPATH=. python scripts/data_quality_check.py
```

### 必须修复的 5 个数据质量问题

| # | 问题 | 原因 | 修复 |
|---|---|---|---|
| 1 | **VOID 类型错误** | CTAS 用抖音数据建表，NULL 列推断为 VOID | 用 `CREATE TABLE` 显式定义 schema（所有数值列为 DOUBLE） |
| 2 | **京东件单价异常** | `成交客单价` 有 `￥10万~￥20万` 的值，解析为 150,000 | 件单价 > 5,000 置 NULL |
| 3 | **天猫超市转化率 > 100%** | `转化指数` 是指数不是百分比 | 不放入转化率列，标记为"指数值" |
| 4 | **天猫销量全部固定 37,500** | 所有行用同一个中位数 | 每行独立解析各自的 `支付单量` 区间中位数 |
| 5 | **京东转化率 > 100%** | 区间值解析后部分超 100% | 转化率 > 100% 置 NULL |

### 各渠道数据处理逻辑

| 渠道 | 销售额 | 销量 | 件单价 | 转化率 | 特殊处理 |
|---|---|---|---|---|---|
| 抖音 | 精确值 | 精确值 | 销售额/销量 | 无 | 直接 INSERT |
| 天猫 | 件单价×销量 | 区间中位数 | 精确值 | 无 | Python 逐行解析支付单量 |
| 天猫超市 | 成交热度(指数) | 成交人气(指数) | 无 | 无 | 转化指数不放入转化率列 |
| 京东自营 | 区间中位数 | 区间中位数 | 区间中位数(cap 5000) | 区间中位数(cap 100%) | Python 逐行解析 |
| 京东POP | 同京东自营 | 同上 | 同上 | 同上 | 同上 |

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
    `转化率`   DOUBLE,   -- 百分比，仅京东有
    `排名`     DOUBLE,   -- 仅京东有
    `数据精度` STRING    -- 精确值/区间中位数/区间中位数估算/指数值(非百分比)
)
```

### 区间值解析函数

```python
import re

def parse_range_mid(val):
    """解析区间值取中位数: '￥200万 ~ ￥400万' → 3000000"""
    if not val or str(val).strip() == '':
        return None
    val = str(val).replace('￥', '').replace(',', '').replace('%', '').strip()
    m = re.match(r'([\d.]+)(万)?\s*~\s*([\d.]+)(万)?', val)
    if not m:
        try:
            return float(val)
        except ValueError:
            return None
    lo = float(m.group(1)) * (10000 if m.group(2) else 1)
    hi = float(m.group(3)) * (10000 if m.group(4) else 1)
    return (lo + hi) / 2
```

### 验证

```bash
PYTHONPATH=. python scripts/data_quality_check.py
```

预期：
- 862 行，5 个渠道全部写入
- 件单价最高 ≤ 5,000（京东自营 max=350）
- 转化率最高 ≤ 100%（京东POP max=77.5%）
- 天猫销量有 10 个不同值（不再全部 37,500）
- 无负值

---

## Phase 5: 竞品运营分析

### 目标
从 `channel_summary` 中提取运营洞察。

### 执行

```bash
PYTHONPATH=. python scripts/competitive_analysis.py
```

### 分析维度（8 个）

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
ORDER BY total_sales DESC LIMIT 15
```

预期 Top 6：许翠花 ¥12.75M → pidan ¥9.63M → 网易严选 ¥4.34M → 凯锐思 ¥2.82M → 京东京造 ¥2.78M → **福丸 ¥2.75M**

#### 5.2 品类结构（哪种猫砂卖得最好）

```sql
SELECT `三级类目`, `四级类目`, `类目编码`, COUNT(*) AS sku_count,
    SUM(CASE WHEN `渠道`='京东自营' THEN `销售额` ELSE 0 END) AS jd_sales,
    SUM(CASE WHEN `渠道`='抖音' THEN `销售额` ELSE 0 END) AS dy_sales
FROM cat_litter.channel_summary
WHERE `品牌` IS NOT NULL AND `品牌` != ''
GROUP BY `三级类目`, `四级类目`, `类目编码`
ORDER BY jd_sales DESC LIMIT 10
```

#### 5.3 福丸 vs 竞品头部品牌

逐品牌查询京东自营的 SKU 数、销售额、排名、均价、转化率。

#### 5.4 福丸跨渠道表现

```sql
SELECT `渠道`, COUNT(*) AS skus, SUM(`销售额`) AS sales,
    ROUND(AVG(`件单价`), 0) AS avg_price
FROM cat_litter.channel_summary WHERE `品牌` = '福丸'
GROUP BY `渠道` ORDER BY sales DESC
```

#### 5.5 价格带分析（京东自营）

按 0-50 / 50-100 / 100-200 / 200-300 / 300+ 分段，统计 SKU 数、销售额、转化率。

#### 5.6 抖音 Top 商品

按销售额排序，展示抖音渠道的畅销商品。

#### 5.7 高转化商品发现

京东渠道转化率 > 15% 的商品，按转化率排序。

#### 5.8 跨渠道品牌对比

Top 10 品牌在各渠道的销售额对比。

### 核心发现

- **福丸在京东自营排名第 6**，SKU 仅 5 个（远少于许翠花 14、pidan 16）
- **天猫是福丸最大渠道**（¥10.3M），但均价最低（¥68）
- **100-200 元是销售额最高的价格带**（¥25.2M），福丸均价 ¥105 定位合理
- **不同渠道品类偏好不同**：木薯猫砂在京东领先，豆腐膨润土混合在抖音领先

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
- **异常值过滤**: 件单价 > 5000、转化率 > 100% 需要置 NULL

### Batch API
- **并行提交**: 5 个渠道同时提交，总等待 = 最慢的那个
- **100% 成功率**: 服务端重试机制保证完整性
- **50% 成本**: 相比实时 API
- **先 test 再 run**: 用 `ai_etl test` 实时验证 prompt 效果，秒级返回

### 运营洞察
- **京东数据最完整**: 有销售额、件单价、转化率、排名，是分析首选
- **SKU 数量是关键杠杆**: 福丸 5 SKU vs pidan 16 SKU，扩充 SKU 是最直接的增长手段
- **渠道差异化**: 不同渠道品类偏好不同，需要差异化选品

---

## 参考文件

| 文件 | 说明 |
|---|---|
| [references/source-table-schemas.md](references/source-table-schemas.md) | 各源表字段说明、数据格式、渠道差异 |
| [references/prompt-template.md](references/prompt-template.md) | 完整的 system_prompt 和 user_prompt |
| [../docs/case-study-cat-litter.html](../docs/case-study-cat-litter.html) | 完整案例文档（HTML，含 SVG 流程图和分析图表） |
| [../configs/cat_litter_jd_pop.yaml](../configs/cat_litter_jd_pop.yaml) | 京东POP 配置示例（其他渠道类似） |
| [../scripts/import_excel.py](../scripts/import_excel.py) | Phase 1: Excel 导入脚本 |
| [../scripts/rebuild_competitor_master_v2.py](../scripts/rebuild_competitor_master_v2.py) | Phase 3: 合并去重脚本 |
| [../scripts/fix_data_quality.py](../scripts/fix_data_quality.py) | Phase 4: 数据质量修复脚本 |
| [../scripts/data_quality_check.py](../scripts/data_quality_check.py) | Phase 4: 10 项质量检查脚本 |
| [../scripts/competitive_analysis.py](../scripts/competitive_analysis.py) | Phase 5: 竞品分析脚本 |
| [../README.md](../README.md) | AI ETL 项目 README |
