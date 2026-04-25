# AI ETL TODO

## 等 batch 测试完成后

### P0: 端到端验证
- [ ] Volume batch (`batch_dfd05341`) 完成后验证结果写入目标表
- [ ] 验证增量处理：重跑 pipeline 应该 0 新文件
- [ ] Table 模式回归测试：确认双数据源改造没有破坏原有流程

### P1: 配置重构 — ETL 三段式
当前问题：provider 段（model/prompt/poll_interval）和 etl 段有大量重叠，Transform 和 Load 配置是黑盒。

目标结构：
```yaml
etl:
  sources:           # Extract
    table:
      enabled: true
      table: "schema.source"
      key_columns: "id"
      text_column: "content"
      filter: ""
      batch_size: 0
    volume:
      enabled: false
      volume_type: "user"
      volume_name: ""
      file_types: [".jpg"]
      subdirectory: ""
      url_expiration: 86400
      batch_size: 0

  transform:         # Transform（集中所有推理相关配置）
    provider: dashscope
    model: ""                    # 空 = 按 source 类型自动选择
    multimodal_model: ""
    system_prompt: "You are a helpful assistant."
    user_prompt: "Describe this file"
    temperature: null            # null = 模型默认值
    max_tokens: null
    enable_thinking: null        # 控制思考模式（qwen3.5 系列默认开启）
    completion_window: "24h"
    poll_interval: 300.0

  target:            # Load
    table: "schema.target"
    result_column: "ai_result"
    write_mode: "append"
    include_metadata: true       # 是否写入 12 个元数据列
    include_source_text: true    # 是否写入原始输入文本
    include_raw_response: true   # 是否写入完整 response JSON
```

变更范围：
- config.py: 所有 ETL 属性路径改为三段式
- pipeline.py: _run_table/_run_volume 读取 transform 段
- lakehouse.py: build_jsonl 支持 temperature/max_tokens 等参数
- config.yaml.example: 更新模板
- tests/: 更新测试

顶层 provider 段只保留连接层配置（endpoint/base_url），不放模型和 prompt。

### P2: 提交到 GitHub
- [ ] git add + commit + push 多模态升级代码

### P3: Lakehouse AI 特性集成（基于 yunqi.tech 产品文档分析）

> 调研日期: 2026-04-25。来源: https://yunqi.tech/llms-full.txt

#### 已发现的优化机会

**1. AI Gateway 集成（替代直连 Provider）**
- Lakehouse 内置 AI Gateway，支持统一接入、路由分发、负载均衡、限流熔断、缓存和监控
- 可通过 AI Gateway 管理 DashScope/ZhipuAI 等多模型供应商的 API 调用
- 优势：统一管理、成本归因、权限隔离、动态限流
- 文档: https://www.yunqi.tech/documents/AIGateway
- **建议**: 增加 `transform.gateway: lakehouse` 配置选项，通过 AI Gateway 路由推理请求

**2. SQL AI 函数（AI_COMPLETE / AI_EMBEDDING）**
- `AI_COMPLETE(model, prompt)`: 在 SQL 中直接调用 LLM，支持文本和图像输入
- `AI_EMBEDDING(model, text)`: 在 SQL 中生成向量嵌入
- 支持通过 AI Gateway Endpoint 或自定义 API Connection 调用
- 文档: https://www.yunqi.tech/documents/AI_COMPLETE, https://www.yunqi.tech/documents/AI_EMBEDDING
- **建议**: 增加 `transform.mode: sql_ai_function` 选项，对小批量数据直接用 SQL AI 函数处理，无需走 Batch API

**3. 向量嵌入生成 + 向量检索**
- VECTOR 数据类型，HNSW 索引，L2/cosine/dot_product 距离函数
- 可在同一张表中同时建立倒排索引和向量索引，实现混合检索
- 文档: https://www.yunqi.tech/documents/vector-search, https://www.yunqi.tech/documents/vector-type
- **建议**: 增加 `transform.task: embedding` 模式，批量生成文本/图像嵌入向量，写入 VECTOR 列 + 建 HNSW 索引

**4. Dynamic Table 增量处理**
- 动态表通过 SQL 查询定义，支持增量刷新（分钟级）
- 可用于构建 ETL 后的增量聚合/分析管道
- 文档: https://www.yunqi.tech/documents/dynamic-table
- **建议**: ETL 结果表可作为 Dynamic Table 的上游，自动触发下游增量分析

**5. Table Stream 变化数据捕获**
- 捕获表的 INSERT/UPDATE/DELETE 变更
- 可用于监控源表变化，自动触发 ETL
- 文档: https://www.yunqi.tech/documents/table_stream
- **建议**: 用 Table Stream 监控源表变更，实现事件驱动的 ETL 触发

**6. 非结构化数据 SQL 分析**
- 内置 `image_to_text`、`llm` 等 AI 函数，可在 SQL 中直接解析图像、PDF
- 文档: https://www.yunqi.tech/documents/datalake_unstructure_data
- **建议**: 对于简单的图像描述任务，可直接用 SQL AI 函数替代 Batch API

**7. External Function（远程函数）**
- 通过 Python/Java 创建 UDF，远程调用阿里云 FC 或腾讯云 SCF
- 可用于自定义 AI 处理逻辑
- 文档: https://www.yunqi.tech/documents/RemoteFunctionintro
- **建议**: 对于需要自定义预处理/后处理的场景，可封装为 External Function

**8. COPY INTO 导出到 Volume**
- 可将查询结果导出为 CSV/Parquet/JSON 到 Volume
- 文档: https://www.yunqi.tech/documents/COPY_INTO_Location
- **建议**: ETL 结果可同时写入表和导出到 Volume，方便下游消费

#### 优先级排序

| 优先级 | 特性 | 价值 | 复杂度 |
|--------|------|------|--------|
| P0 | AI Gateway 集成 | 统一管理、降低成本 | 中 |
| P0 | SQL AI 函数模式 | 小批量场景秒级响应 | 低 |
| P1 | 向量嵌入生成 | 支持 RAG/语义搜索 | 中 |
| P1 | Dynamic Table 联动 | 自动增量分析 | 低 |
| P2 | Table Stream 触发 | 事件驱动 ETL | 中 |
| P2 | External Function | 自定义 AI 处理 | 高 |
| P3 | COPY INTO 导出 | 多格式输出 | 低 |
