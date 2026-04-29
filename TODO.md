# AI ETL — TODO

## 已完成 ✅

- [x] Table 数据源：结构化文本 → LLM 批量推理 → 结果写回
- [x] Volume 数据源：图片/视频/音频 → 预签名 URL → 多模态推理 → 结果写回
- [x] 双数据源并行提交，统一轮询等待
- [x] Per-source 独立目标表（table/volume 各自写入不同表）
- [x] 目标表自动创建 + 列自动添加 + 类型自动匹配
- [x] 12 个元数据列（model, tokens, batch_id, processed_at 等）
- [x] 增量处理（Volume 跳过已处理文件，Table 跳过已处理 key）
- [x] Resume 恢复中断的任务
- [x] 多 Provider 支持（DashScope + ZhipuAI）
- [x] 推理参数可配（temperature/max_tokens/top_p/enable_thinking，per-source 级别）
- [x] 北京时间 processed_at
- [x] 表名自动补 schema 前缀
- [x] 友好的错误信息（数据源/模型/配置问题）
- [x] 空数据防御（空表/空文件/空结果）
- [x] 端到端验证通过（双数据源并行，13/13 成功）
- [x] README 完整文档
- [x] 解决方案文档（docs/multimodal-ai-etl-solution.html）
- [x] 61 个单元测试
- [x] AI Plan 命令（采样分析 → 推荐配置）
- [x] AI Test 命令（实时 API 验证 prompt 效果）
- [x] Cat Litter Case Study（5 渠道 862 条竞品主数据生成）
- [x] 数据质量修复（VOID 类型、件单价异常、转化率异常、天猫销量固定值）
- [x] 跨渠道竞品分析（channel_summary 表 + 运营分析脚本）

## 待优化

（暂无）
