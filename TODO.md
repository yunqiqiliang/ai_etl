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
- [ ] JSONL body 中支持 `temperature`, `max_tokens`, `top_p` 等推理参数
- [ ] 支持 `enable_thinking: false` 关闭 qwen3.5 系列的思考模式（可节省 token）
- [ ] 这些参数在 source 级别配置，不同 source 可以用不同的推理参数

### P2: Table 模式增量处理
- [ ] 当前 table 模式每次重跑会重新处理所有匹配行
- [ ] 用户可通过 `filter` 字段手动控制范围（如 `created_at > '2026-04-25'`）
- [ ] 框架级增量：查询目标表已有 key，自动跳过（类似 volume 模式的 file_path 过滤）
