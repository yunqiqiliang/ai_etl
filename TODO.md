# AI ETL — TODO

## 已完成 ✅

- [x] Table 数据源：结构化文本 → LLM 批量推理 → 结果写回
- [x] Volume 数据源：图片/视频/音频 → 预签名 URL → 多模态推理 → 结果写回
- [x] 双数据源并行提交，统一轮询等待
- [x] Per-source 独立目标表（table/volume 各自写入不同表）
- [x] 目标表自动创建 + 列自动添加 + 类型自动匹配
- [x] 12 个元数据列（model, tokens, batch_id, processed_at 等）
- [x] 增量处理（Volume 模式跳过已处理文件）
- [x] Resume 恢复中断的任务
- [x] 多 Provider 支持（DashScope + ZhipuAI）
- [x] 北京时间 processed_at
- [x] 表名自动补 schema 前缀
- [x] 友好的错误信息（数据源/模型/配置问题）
- [x] 空数据防御（空表/空文件/空结果）
- [x] 端到端验证通过（双数据源并行，13/13 成功）
- [x] README 完整文档
- [x] 解决方案文档（docs/solution.html）
- [x] 61 个单元测试

## 待优化

### P1: Transform 参数扩展
- [ ] 支持 `temperature`, `max_tokens`, `enable_thinking` 等推理参数
- [ ] 这些参数目前在 provider 段配置，应集中到 source 级别或独立 transform 段

### P2: Table 模式增量处理
- [ ] 当前 table 模式每次重跑会重新处理所有行
- [ ] 可通过记录已处理的 key 到目标表实现增量（类似 volume 模式的 file_path 过滤）

### P3: 多 Batch 状态持久化
- [ ] 当前 `last_batch.json` 只保存最后一个 batch 的状态
- [ ] 双数据源模式下，volume 的状态会覆盖 table 的
- [ ] 改为按 batch_id 索引的状态文件，支持同时 resume 多个任务
