"""
多 LLM 提供商统一接口
====================
支持 OpenAI / Claude / DeepSeek + 自定义 API。
"""

import json
import logging
import requests
from datetime import datetime

# 配置日志
logger = logging.getLogger("llm_client")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    fh = logging.FileHandler("network.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
    logger.addHandler(ch)
    from log_buffer import BufferHandler
    bh = BufferHandler(category="llm")
    bh.setLevel(logging.INFO)
    bh.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(bh)
from models import load_config, save_config


def get_active_provider():
    """获取当前激活的 LLM 提供商配置"""
    config = load_config()
    llm_cfg = config.get("llm", {})
    active = llm_cfg.get("active_provider", "openai")
    providers = llm_cfg.get("providers", {})
    provider = providers.get(active, {})
    return active, provider


def call_llm(messages: list, provider_key: str = None) -> str:
    """
    统一 LLM 调用接口。
    messages: [{"role": "system"/"user", "content": "..."}]
    返回 LLM 响应文本。
    """
    config = load_config()
    llm_cfg = config.get("llm", {})

    if provider_key is None:
        provider_key = llm_cfg.get("active_provider", "openai")

    providers = llm_cfg.get("providers", {})
    provider = providers.get(provider_key, {})

    api_url = provider.get("api_url", "")
    api_key = provider.get("api_key", "")
    model = provider.get("model", "")
    max_tokens = provider.get("max_tokens", 4096)

    if not api_url or not api_key:
        raise ValueError(f"LLM 提供商 '{provider_key}' 未配置 API URL 或 API Key")

    # Claude 使用 Anthropic 原生格式
    if provider_key == "claude":
        return _call_claude(api_url, api_key, model, max_tokens, messages)
    else:
        # OpenAI 兼容格式（OpenAI / DeepSeek / 自定义）
        return _call_openai_compatible(api_url, api_key, model, max_tokens, messages)


def _call_openai_compatible(api_url, api_key, model, max_tokens, messages) -> str:
    """调用 OpenAI 兼容 API"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3,
    }

    logger.info(f"[REQUEST] POST {api_url} | model={model} | messages={len(messages)}条")
    logger.debug(f"[REQUEST BODY] {json.dumps(payload, ensure_ascii=False)[:2000]}")
    # 写入详细日志
    from log_buffer import add_log
    req_detail = "\n".join(f"[{m['role']}] {m['content']}" for m in messages)
    add_log("llm", "info", f"[发送请求] POST {api_url} | model={model}", req_detail)

    resp = requests.post(api_url, headers=headers, json=payload, timeout=120)
    logger.info(f"[RESPONSE] {resp.status_code} | {len(resp.text)} chars")
    logger.debug(f"[RESPONSE BODY] {resp.text[:3000]}")

    resp.raise_for_status()
    data = resp.json()

    # 提取响应文本
    choices = data.get("choices", [])
    result = choices[0].get("message", {}).get("content", "") if choices else ""
    logger.info(f"[RESULT] 返回 {len(result)} 字符")
    add_log("llm", "info", f"[收到响应] {resp.status_code} | {len(result)} 字符", result)
    return result


def _call_claude(api_url, api_key, model, max_tokens, messages) -> str:
    """调用 Claude API (Anthropic 原生格式)"""
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }

    # 转换消息格式：提取 system message
    system_msg = ""
    claude_messages = []
    for msg in messages:
        if msg["role"] == "system":
            system_msg = msg["content"]
        else:
            claude_messages.append({"role": msg["role"], "content": msg["content"]})

    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": claude_messages,
        "temperature": 0.3,
    }
    if system_msg:
        payload["system"] = system_msg

    logger.info(f"[REQUEST] POST {api_url} | model={model} | messages={len(claude_messages)}条")
    logger.debug(f"[REQUEST BODY] {json.dumps(payload, ensure_ascii=False)[:2000]}")
    from log_buffer import add_log
    req_detail = ""
    if system_msg:
        req_detail += f"[system] {system_msg}\n"
    req_detail += "\n".join(f"[{m['role']}] {m['content']}" for m in claude_messages)
    add_log("llm", "info", f"[发送请求] POST {api_url} | model={model}", req_detail)

    resp = requests.post(api_url, headers=headers, json=payload, timeout=120)
    logger.info(f"[RESPONSE] {resp.status_code} | {len(resp.text)} chars")
    logger.debug(f"[RESPONSE BODY] {resp.text[:3000]}")

    resp.raise_for_status()
    data = resp.json()

    # Claude 响应格式
    content = data.get("content", [])
    result = content[0].get("text", "") if content else ""
    logger.info(f"[RESULT] 返回 {len(result)} 字符")
    add_log("llm", "info", f"[收到响应] {resp.status_code} | {len(result)} 字符", result)
    return result


def is_english_title(title: str) -> bool:
    """判断标题是否为英文（ASCII字符占比>50%）"""
    if not title:
        return False
    ascii_count = sum(1 for c in title if ord(c) < 128)
    return ascii_count / len(title) > 0.5


def batch_translate_titles(titles: list, batch_size: int = 30) -> dict:
    """
    批量翻译英文标题为中文。
    返回 {原标题: 中文翻译} 字典。
    """
    result = {}
    # 分批处理
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i+batch_size]
        titles_text = "\n".join(f"{j+1}. {t}" for j, t in enumerate(batch))

        messages = [
            {"role": "system", "content": (
                "你是一名专业翻译。请将以下英文新闻标题翻译为简体中文。"
                "要求：\n"
                "1. 翻译准确、专业，符合中文新闻标题的表达习惯\n"
                "2. 专有名词用通用中文译法（如 Express Entry→快捷通道，eVisa→电子签证）\n"
                "3. 国家/地区名用标准中文译名\n"
                "4. 以JSON对象格式返回，key为序号，value为中文翻译\n"
                '例如：{"1": "中文标题1", "2": "中文标题2"}\n'
                "只返回JSON，不要添加其他文字。"
            )},
            {"role": "user", "content": f"请翻译以下{len(batch)}条英文标题：\n\n{titles_text}"},
        ]

        try:
            response = call_llm(messages)
            json_str = _extract_json(response)
            if json_str:
                translations = json.loads(json_str)
                for j, title in enumerate(batch):
                    cn = translations.get(str(j+1), "")
                    if cn:
                        result[title] = cn
            logger.info(f"[翻译] 批次 {i//batch_size+1}: 成功翻译 {len([k for k in result if k in batch])}/{len(batch)} 条")
        except Exception as e:
            logger.error(f"[翻译错误] 批次 {i//batch_size+1}: {e}")

    return result


def filter_articles(titles: list, user_prompt: str = None) -> list:
    """
    发送文章标题列表到 LLM 进行筛选。
    返回 LLM 认为相关的标题列表。
    """
    config = load_config()
    prompt = user_prompt or config.get("prompts", {}).get("filter", "")

    titles_text = "\n".join(f"{i+1}. {t}" for i, t in enumerate(titles))

    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"以下是{len(titles)}条新闻标题：\n\n{titles_text}"},
    ]

    response = call_llm(messages)

    # 解析 JSON 数组响应
    try:
        # 尝试提取 JSON
        json_match = _extract_json(response)
        if json_match:
            result = json.loads(json_match)
            if isinstance(result, list):
                return result
    except (json.JSONDecodeError, ValueError):
        pass

    # 降级：逐行匹配
    selected = []
    for line in response.strip().split("\n"):
        line = line.strip().strip('"').strip("'").strip(",").strip()
        if line and any(t in line or line in t for t in titles):
            selected.append(line)
    return selected


def _build_articles_text(articles: list) -> str:
    """将文章列表构建为发送给 LLM 的文本"""
    text = ""
    for i, a in enumerate(articles):
        text += f"\n--- 第{i+1}篇 ---\n"
        text += f"标题：{a.get('title', '')}\n"
        text += f"来源：{a.get('source', '')}\n"
        text += f"日期：{a.get('date', '')}\n"
        if a.get("country"):
            text += f"国家/地区：{a['country']}\n"
        text += f"正文：{a.get('content', '（无正文）')}\n"
    return text


def _generate_single_batch(articles: list, prompt: str) -> dict:
    """对单批文章调用 LLM 生成子报告"""
    articles_text = _build_articles_text(articles)

    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"以下是{len(articles)}篇新闻：\n{articles_text}"},
    ]

    response = call_llm(messages)

    try:
        json_str = _extract_json(response)
        if json_str:
            return json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        pass

    return {"raw_response": response}


def _merge_reports(reports: list) -> dict:
    """将多个子报告合并为一份完整报告"""
    if len(reports) == 1:
        return reports[0]

    # 以第一份报告为基础
    final = {
        "title": reports[0].get("title", "皇岗边检站国际移民资讯"),
        "dateStart": reports[0].get("dateStart", ""),
        "dateEnd": reports[-1].get("dateEnd", reports[0].get("dateEnd", "")),
        "summaries": [],
        "sections": []
    }

    # 用有序字典按 section title 分组聚合 newsItems
    section_map = {}
    section_order = []

    for rp in reports:
        if "raw_response" in rp:
            continue  # 跳过解析失败的批次

        # 合并 summaries
        final["summaries"].extend(rp.get("summaries", []))

        # 合并 sections
        for sec in rp.get("sections", []):
            sec_title = sec.get("title", "其他")
            if sec_title not in section_map:
                section_map[sec_title] = []
                section_order.append(sec_title)
            section_map[sec_title].extend(sec.get("newsItems", []))

    # 按原始顺序组装 sections
    for title in section_order:
        final["sections"].append({
            "title": title,
            "newsItems": section_map[title]
        })

    logger.info(f"[报告合并] {len(reports)} 个子报告 → "
                f"summaries={len(final['summaries'])} 条, "
                f"sections={len(final['sections'])} 个, "
                f"newsItems={sum(len(s['newsItems']) for s in final['sections'])} 条")

    return final


def generate_report(articles: list, user_prompt: str = None,
                    progress_callback=None) -> dict:
    """
    发送文章标题+正文到 LLM 生成结构化报告。
    支持分批生成+合并。当文章数超过 batch_size 时自动分批。
    progress_callback(current_batch, total_batches) 用于通知进度。
    """
    config = load_config()
    prompt = user_prompt or config.get("prompts", {}).get("report", "")
    batch_size = config.get("report_generation", {}).get("batch_size", 15)

    # 如果文章数在 batch_size 以内，直接单次生成
    if len(articles) <= batch_size:
        logger.info(f"[报告生成] 共 {len(articles)} 篇，单批生成")
        if progress_callback:
            progress_callback(1, 1)
        return _generate_single_batch(articles, prompt)

    # 分批生成
    total_batches = (len(articles) + batch_size - 1) // batch_size
    logger.info(f"[报告生成] 共 {len(articles)} 篇，分 {total_batches} 批（每批 {batch_size} 篇）")

    sub_reports = []
    for batch_idx in range(total_batches):
        start = batch_idx * batch_size
        end = min(start + batch_size, len(articles))
        batch = articles[start:end]

        logger.info(f"[报告生成] 正在生成第 {batch_idx+1}/{total_batches} 批（{len(batch)} 篇）...")
        if progress_callback:
            progress_callback(batch_idx + 1, total_batches)

        sub_report = _generate_single_batch(batch, prompt)
        sub_reports.append(sub_report)

    # 合并所有子报告
    return _merge_reports(sub_reports)


def _extract_json(text: str) -> str:
    """从文本中提取 JSON 字符串"""
    # 尝试找到 JSON 块
    # 1. 代码块中的 JSON
    import re
    code_match = re.search(r'```(?:json)?\s*\n?([\s\S]*?)\n?```', text)
    if code_match:
        return code_match.group(1).strip()

    # 2. 直接 JSON
    # 找到第一个 [ 或 { 和最后一个 ] 或 }
    start_arr = text.find('[')
    start_obj = text.find('{')
    if start_arr == -1 and start_obj == -1:
        return None

    if start_arr != -1 and (start_obj == -1 or start_arr < start_obj):
        end = text.rfind(']')
        if end > start_arr:
            return text[start_arr:end+1]
    elif start_obj != -1:
        end = text.rfind('}')
        if end > start_obj:
            return text[start_obj:end+1]

    return None


def test_connection(provider_key: str = None) -> dict:
    """测试 LLM 连接是否正常"""
    try:
        messages = [
            {"role": "user", "content": "请回复'连接成功'四个字。"}
        ]
        response = call_llm(messages, provider_key)
        return {"status": "success", "response": response[:100]}
    except Exception as e:
        return {"status": "error", "error": str(e)}
