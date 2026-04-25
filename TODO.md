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
- [x] git add + commit + push 多模态升级代码
