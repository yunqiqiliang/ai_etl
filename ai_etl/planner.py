"""
AI ETL Planner — 自动分析数据源，推荐 ETL 配置。

用实时 API 分析少量样本，生成 batch 推理的 config 片段。
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from ai_etl.config import Config
from ai_etl.lakehouse import LakehouseClient

logger = logging.getLogger(__name__)

# 采样行数 / 文件数
_TABLE_SAMPLE_SIZE = 5
_VOLUME_SAMPLE_SIZE = 2

# 分析用的 meta-prompt
_TABLE_ANALYSIS_PROMPT = """你是一个 AI ETL 配置专家。用户有一张数据库表，想用 LLM 批量处理其中的文本数据。

表信息：
- 表名: {table_name}
- 列: {columns}
- 行数: {row_count}

以下是 {sample_size} 行样本数据：
{sample_data}
{hint_section}
请分析这些数据，推荐最佳的 ETL 配置。返回严格的 JSON 格式（不要 markdown 代码块）：
{{
  "key_column": "推荐的主键列名",
  "text_column": "推荐的文本列名（用于 LLM 输入）",
  "model": "推荐的模型名（qwen3.5-flash 适合短文本，qwen3-max 适合复杂任务）",
  "system_prompt": "推荐的 system prompt",
  "user_prompt": "推荐的 user prompt 模板，用 {{text}} 作为占位符",
  "reasoning": "推荐理由（一句话）"
}}"""

_VOLUME_ANALYSIS_PROMPT = """你是一个 AI ETL 配置专家。用户有一批存储在云端的文件，想用多模态 LLM 批量处理。

Volume 信息：
- 文件总数: {file_count}
- 文件类型分布: {type_distribution}
- 样本文件名: {sample_files}

{image_analysis}
{hint_section}
请分析这些文件，推荐最佳的 ETL 配置。返回严格的 JSON 格式（不要 markdown 代码块）：
{{
  "model": "推荐的多模态模型名（qwen3-vl-plus 适合图片/视频，qwen-omni-turbo 支持音频）",
  "system_prompt": "推荐的 system prompt",
  "user_prompt": "推荐的 user prompt",
  "file_types": ["推荐过滤的扩展名列表"],
  "reasoning": "推荐理由（一句话）"
}}"""


def _call_llm(prompt: str, api_key: str, model: str = "qwen3.5-flash") -> str:
    """调用实时 API 获取分析结果。"""
    from openai import OpenAI
    client = OpenAI(
        api_key=api_key,
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        extra_body={"enable_thinking": False},
    )
    return resp.choices[0].message.content.strip()


def _parse_json_response(text: str) -> Dict[str, Any]:
    """从 LLM 响应中提取 JSON（处理 markdown 代码块等情况）。"""
    # 去掉 markdown 代码块
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]
    return json.loads(text.strip())


def plan_table(
    table: str,
    config: Optional[Config] = None,
    sample_size: int = _TABLE_SAMPLE_SIZE,
    hint: str = "",
) -> Dict[str, Any]:
    """分析表数据，推荐 ETL 配置。"""
    cfg = config or Config()
    lh = LakehouseClient(config=cfg)
    api_key = os.getenv("DASHSCOPE_API_KEY", "")
    if not api_key:
        raise RuntimeError("需要 DASHSCOPE_API_KEY 来分析数据")

    try:
        # 1. 获取表 schema
        logger.info("分析源表: %s", table)
        schema = lh.session.table(table).schema
        columns = [{"name": f.name.strip("`"), "type": type(f.datatype).__name__} for f in schema.fields]
        col_desc = ", ".join(f"{c['name']}({c['type']})" for c in columns)

        # 2. 获取行数
        count_row = lh.session.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()
        row_count = int(count_row[0]["cnt"]) if count_row else 0

        # 3. 采样数据
        sample_rows = lh.session.sql(f"SELECT * FROM {table} LIMIT {sample_size}").collect()
        sample_data = ""
        for i, row in enumerate(sample_rows, 1):
            row_dict = {c["name"]: str(row[c["name"]]) if row[c["name"]] is not None else "NULL" for c in columns}
            sample_data += f"  行{i}: {json.dumps(row_dict, ensure_ascii=False)}\n"

        # 4. 调用 LLM 分析
        hint_section = f"用户的需求提示：{hint}\n请在推荐配置时充分考虑用户的需求。\n" if hint else ""
        prompt = _TABLE_ANALYSIS_PROMPT.format(
            table_name=table, columns=col_desc, row_count=row_count,
            sample_size=len(sample_rows), sample_data=sample_data,
            hint_section=hint_section,
        )
        logger.info("调用 LLM 分析数据样本...")
        raw = _call_llm(prompt, api_key)
        result = _parse_json_response(raw)

        # 5. 估算成本
        avg_tokens = 200  # 粗估每行 200 tokens
        total_tokens = row_count * avg_tokens
        cost_realtime = total_tokens / 1_000_000 * 2.0  # 约 ¥2/M tokens
        cost_batch = cost_realtime * 0.5

        result["table"] = table
        result["row_count"] = row_count
        result["columns"] = columns
        result["estimated_tokens"] = total_tokens
        result["estimated_cost_batch"] = f"¥{cost_batch:.2f}"

        return result
    finally:
        lh.close()


def plan_volume(
    volume_type: str = "user",
    volume_name: str = "",
    subdirectory: str = "",
    config: Optional[Config] = None,
    sample_size: int = _VOLUME_SAMPLE_SIZE,
    hint: str = "",
) -> Dict[str, Any]:
    """分析 Volume 文件，推荐 ETL 配置。"""
    cfg = config or Config()
    lh = LakehouseClient(config=cfg)
    api_key = os.getenv("DASHSCOPE_API_KEY", "")
    if not api_key:
        raise RuntimeError("需要 DASHSCOPE_API_KEY 来分析数据")

    try:
        # 1. 构建 Volume SQL 引用
        if volume_type == "user":
            vol_ref = "USER VOLUME"
        elif volume_type == "table":
            vol_ref = f"TABLE VOLUME {volume_name}"
        else:
            vol_ref = f"VOLUME {volume_name}"

        # 2. 列出文件
        logger.info("分析 Volume: %s", vol_ref)
        if vol_ref == "USER VOLUME":
            rows = lh.session.sql("SELECT relative_path, size FROM (SHOW USER VOLUME DIRECTORY)").collect()
        else:
            rows = lh.session.sql(f"SELECT relative_path, size FROM DIRECTORY({vol_ref})").collect()

        files = [{"path": str(r["relative_path"]), "size": int(r["size"] or 0)} for r in rows if r["relative_path"]]

        # 按子目录过滤
        if subdirectory:
            files = [f for f in files if f["path"].startswith(subdirectory)]

        if not files:
            return {"error": "Volume 中没有文件", "volume": vol_ref, "subdirectory": subdirectory}

        # 3. 统计文件类型分布
        from collections import Counter
        ext_counter = Counter()
        for f in files:
            ext = "." + f["path"].rsplit(".", 1)[-1].lower() if "." in f["path"] else "(no ext)"
            ext_counter[ext] += 1
        type_dist = ", ".join(f"{ext}: {cnt}" for ext, cnt in ext_counter.most_common())

        # 4. 采样分析图片内容（用实时 API）
        from ai_etl.media_types import detect_media_type, MediaType
        image_analysis = ""
        image_files = [f for f in files if detect_media_type(f["path"]) == MediaType.IMAGE]

        if image_files:
            sample_files = image_files[:sample_size]
            lh.session.sql("SET cz.sql.function.get.presigned.url.force.external=true").collect()

            descriptions = []
            for sf in sample_files:
                try:
                    url_rows = lh.session.sql(
                        f"SELECT GET_PRESIGNED_URL({vol_ref}, '{sf['path']}', 3600) AS url"
                    ).collect()
                    url = str(url_rows[0]["url"]) if url_rows else None
                    if url:
                        # 用实时 API 快速分析图片内容
                        from openai import OpenAI
                        client = OpenAI(api_key=api_key, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")
                        resp = client.chat.completions.create(
                            model="qwen3-vl-plus",
                            messages=[{"role": "user", "content": [
                                {"type": "image_url", "image_url": {"url": url}},
                                {"type": "text", "text": "用一句话描述这张图片的内容和类型（如产品图、风景照、文档扫描等）"},
                            ]}],
                            extra_body={"enable_thinking": False},
                        )
                        desc = resp.choices[0].message.content.strip()
                        descriptions.append(f"  - {sf['path']}: {desc}")
                        logger.info("采样分析: %s → %s", sf["path"], desc[:60])
                except Exception as e:
                    logger.warning("采样分析失败 (%s): %s", sf["path"], e)

            if descriptions:
                image_analysis = "AI 对采样图片的分析：\n" + "\n".join(descriptions)

        sample_names = [f["path"] for f in files[:10]]

        # 5. 调用 LLM 推荐配置
        hint_section = f"用户的需求提示：{hint}\n请在推荐配置时充分考虑用户的需求。\n" if hint else ""
        prompt = _VOLUME_ANALYSIS_PROMPT.format(
            file_count=len(files), type_distribution=type_dist,
            sample_files=", ".join(sample_names),
            image_analysis=image_analysis or "（未采样分析图片内容）",
            hint_section=hint_section,
        )
        logger.info("调用 LLM 推荐配置...")
        raw = _call_llm(prompt, api_key)
        result = _parse_json_response(raw)

        # 6. 估算成本
        avg_tokens = 300
        total_tokens = len(files) * avg_tokens
        cost_batch = total_tokens / 1_000_000 * 2.0 * 0.5

        result["volume"] = vol_ref
        result["subdirectory"] = subdirectory
        result["file_count"] = len(files)
        result["type_distribution"] = dict(ext_counter)
        result["estimated_tokens"] = total_tokens
        result["estimated_cost_batch"] = f"¥{cost_batch:.2f}"

        return result
    finally:
        lh.close()


def format_plan_result(result: Dict[str, Any], source_type: str) -> str:
    """格式化 plan 结果为可读文本。"""
    lines = []

    if "error" in result:
        return f"❌ {result['error']}"

    if source_type == "table":
        lines.append(f"📊 源表: {result.get('table', '?')} ({result.get('row_count', '?')} 行)")
        lines.append(f"   列: {', '.join(c['name'] for c in result.get('columns', []))}")
        lines.append("")
        lines.append("推荐配置:")
        lines.append(f"  key_column:    {result.get('key_column', '?')}")
        lines.append(f"  text_column:   {result.get('text_column', '?')}")
        lines.append(f"  model:         {result.get('model', '?')}")
        lines.append(f"  system_prompt: \"{result.get('system_prompt', '?')}\"")
        lines.append(f"  user_prompt:   \"{result.get('user_prompt', '?')}\"")
    else:
        lines.append(f"🖼️  Volume: {result.get('volume', '?')} ({result.get('file_count', '?')} 文件)")
        dist = result.get("type_distribution", {})
        lines.append(f"   类型: {', '.join(f'{k}:{v}' for k, v in dist.items())}")
        lines.append("")
        lines.append("推荐配置:")
        lines.append(f"  model:         {result.get('model', '?')}")
        lines.append(f"  file_types:    {result.get('file_types', [])}")
        lines.append(f"  system_prompt: \"{result.get('system_prompt', '?')}\"")
        lines.append(f"  user_prompt:   \"{result.get('user_prompt', '?')}\"")

    lines.append("")
    lines.append(f"  预估 tokens:   {result.get('estimated_tokens', '?'):,}")
    lines.append(f"  预估成本:      {result.get('estimated_cost_batch', '?')} (batch 价)")
    lines.append("")
    lines.append(f"💡 {result.get('reasoning', '')}")

    return "\n".join(lines)


def generate_config_snippet(result: Dict[str, Any], source_type: str) -> str:
    """从 plan 结果生成 config.yaml 片段。"""
    if source_type == "table":
        return f"""etl:
  sources:
    table:
      enabled: true
      table: "{result.get('table', 'schema.table')}"
      key_columns: "{result.get('key_column', 'id')}"
      text_column: "{result.get('text_column', 'content')}"
      system_prompt: "{result.get('system_prompt', '')}"
      user_prompt: "{result.get('user_prompt', '')}"
      target_table: "{result.get('table', 'schema.table').rsplit('.', 1)[0]}.{result.get('table', 'table').rsplit('.', 1)[-1]}_ai_results"
"""
    else:
        ft = result.get("file_types", [".jpg"])
        return f"""etl:
  sources:
    volume:
      enabled: true
      volume_type: "{result.get('volume', 'USER VOLUME').split()[0].lower()}"
      file_types: {json.dumps(ft)}
      system_prompt: "{result.get('system_prompt', '')}"
      user_prompt: "{result.get('user_prompt', '')}"
      target_table: "schema.volume_ai_results"
"""


def test_with_realtime(
    config: Optional[Config] = None,
    sample_count: int = 1,
) -> List[Dict[str, Any]]:
    """用实时 API 测试当前 config 的 prompt 效果。

    从源表随机取 sample_count 条数据，用实时 API 秒级返回结果。
    用于 plan 后快速验证 prompt 质量，无需等待 batch。
    """
    cfg = config or Config()
    api_key = os.getenv("DASHSCOPE_API_KEY", "")
    if not api_key:
        raise RuntimeError("需要 DASHSCOPE_API_KEY")

    lh = LakehouseClient(config=cfg)
    results = []

    try:
        # 读取源表采样
        table = cfg.etl_table_name
        key_col = cfg.etl_table_key_columns
        text_col = cfg.etl_table_text_column

        if not table:
            raise RuntimeError("config 中未配置 etl.sources.table.table")

        logger.info("从 %s 采样 %d 条数据...", table, sample_count)
        rows = lh.session.sql(
            f"SELECT `{key_col}`, `{text_col}` FROM {table} LIMIT {sample_count}"
        ).collect()

        if not rows:
            raise RuntimeError(f"源表 {table} 无数据")

        # 构建 prompt
        system_prompt = cfg.etl_table_system_prompt
        user_prompt_tpl = cfg.etl_table_user_prompt
        model = cfg.resolve_model("table")
        transform_params = cfg.get_transform_params("table")

        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")

        for row in rows:
            key_val = str(row[key_col])
            text_val = str(row[text_col])

            # 构建 user message（和 batch 逻辑一致）
            if user_prompt_tpl:
                user_content = user_prompt_tpl.replace("{text}", text_val)
            else:
                user_content = text_val

            logger.info("测试: %s = %s", key_col, key_val)

            # 实时调用
            kwargs = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
            }
            # 推理参数
            extra_body = {}
            for k, v in (transform_params or {}).items():
                if k == "enable_thinking":
                    extra_body["enable_thinking"] = v
                else:
                    kwargs[k] = v
            if extra_body:
                kwargs["extra_body"] = extra_body

            resp = client.chat.completions.create(**kwargs)
            ai_result = resp.choices[0].message.content.strip()
            tokens = resp.usage.total_tokens if resp.usage else 0

            results.append({
                "key": key_val,
                "input": text_val,
                "output": ai_result,
                "tokens": tokens,
                "model": model,
            })

        return results
    finally:
        lh.close()


def format_test_results(results: List[Dict[str, Any]]) -> str:
    """格式化测试结果。"""
    lines = []
    for i, r in enumerate(results, 1):
        lines.append(f"━━━ 测试 {i}/{len(results)} ━━━")
        lines.append(f"  输入: {r['input'][:80]}")
        lines.append(f"  输出: {r['output'][:200]}")
        lines.append(f"  模型: {r['model']}  tokens: {r['tokens']}")
        lines.append("")
    return "\n".join(lines)
