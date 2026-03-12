"""
国际移民新闻聚合 Web 应用
========================
Flask 主程序，提供 REST API 和定时爬取任务。

启动方式: python app.py
访问地址: http://localhost:5000
"""

import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file, render_template, Response
from apscheduler.schedulers.background import BackgroundScheduler

from models import (
    load_config, save_config, init_db, upsert_article, bulk_upsert_articles,
    get_articles_by_time_range, get_article_by_url, update_article_content,
    update_article_title_cn,
    get_article_count, get_sources_stats, log_scrape_run, get_latest_scrape_logs,
    search_articles,
    save_report_history, get_report_history_list, get_report_history_detail
)
from scrapers import run_all_scrapers, scrape_article_content, ALL_SCRAPERS
from llm_client import (
    filter_articles, generate_report, test_connection, call_llm,
    is_english_title, batch_translate_titles
)
from report_generator import generate_word_report, generate_filename


app = Flask(__name__)

# 爬虫运行状态
scrape_status = {
    "is_running": False,
    "last_run": None,
    "last_results": {},
}


# ===================================================================
# 定时爬取任务
# ===================================================================
def scheduled_scrape():
    """定时爬取所有新闻源（逐条入库，连续重复即停）"""
    if scrape_status["is_running"]:
        return
    scrape_status["is_running"] = True
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 开始定时爬取...")

    config = load_config()
    delay = config.get("scraper", {}).get("delay_between_requests", 1.5)
    max_dup = 5  # 连续重复阈值

    total_new = 0
    source_results = {}

    for name, cls in ALL_SCRAPERS.items():
        try:
            scraper = cls(delay=delay)
            articles = scraper.scrape()
        except Exception as e:
            log_scrape_run(name, 0, 0, "error", str(e))
            print(f"  [!] {name}: {e}")
            source_results[name] = 0
            continue

        new_count = 0
        consecutive_dup = 0
        processed = 0

        for article in articles:
            processed += 1
            is_new = upsert_article(article)
            if is_new:
                new_count += 1
                consecutive_dup = 0  # 重置计数
            else:
                consecutive_dup += 1
                if consecutive_dup >= max_dup:
                    print(f"  [→] {name}: 连续 {max_dup} 条重复，跳过剩余 {len(articles)-processed} 条")
                    break

        total_new += new_count
        source_results[name] = len(articles)
        log_scrape_run(name, new_count, processed)
        print(f"  [✓] {name}: 获取 {len(articles)} 条，处理 {processed} 条，新增 {new_count} 条")

    # 翻译新增英文标题
    try:
        from models import get_db
        db = get_db()
        en_articles = list(db["articles"].find(
            {"title_cn": {"$exists": False}},
            {"_id": 0, "url": 1, "title": 1}
        ))
        en_titles = [(a["url"], a["title"]) for a in en_articles if is_english_title(a.get("title", ""))]
        if en_titles:
            print(f"  [翻译] 发现 {len(en_titles)} 条英文标题，正在翻译...")
            titles_only = [t for _, t in en_titles]
            translations = batch_translate_titles(titles_only)
            for url, title in en_titles:
                cn = translations.get(title)
                if cn:
                    update_article_title_cn(url, cn)
            print(f"  [✓] 翻译完成：{len(translations)}/{len(en_titles)} 条")
    except Exception as e:
        print(f"  [!] 翻译出错: {e}")

    scrape_status["is_running"] = False
    scrape_status["last_run"] = datetime.now().isoformat()
    scrape_status["last_results"] = source_results
    print(f"  [✓] 爬取完成，共新增 {total_new} 条\n")


# ===================================================================
# API 路由
# ===================================================================
@app.route("/")
def index():
    """前端管理面板（暗色版）"""
    return render_template("index.html")


@app.route("/new")
def index_new():
    """前端管理面板（亮色 Dify 风格）"""
    return render_template("index_new.html")


@app.route("/api/articles")
def api_articles():
    """查询文章列表"""
    hours = request.args.get("hours", 24, type=int)
    source = request.args.get("source", None)
    keyword = request.args.get("keyword",  None)
    limit = request.args.get("limit", 5000, type=int)

    # 自定义时间范围
    start_str = request.args.get("start")
    end_str = request.args.get("end")

    if start_str:
        start = datetime.fromisoformat(start_str)
    else:
        start = datetime.now() - timedelta(hours=hours)

    if end_str:
        end = datetime.fromisoformat(end_str)
    else:
        end = datetime.now()

    if keyword:
        articles = search_articles(keyword, limit)
        # 按时间和来源进一步过滤
        articles = [a for a in articles if a.get("scraped_at") and a["scraped_at"] >= start and a["scraped_at"] <= end]
        if source:
            articles = [a for a in articles if a.get("source") == source]
    else:
        articles = get_articles_by_time_range(start, end, source, limit)

    # 序列化 datetime
    for a in articles:
        for k, v in a.items():
            if isinstance(v, datetime):
                a[k] = v.isoformat()

    return jsonify({
        "count": len(articles),
        "articles": articles
    })


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    """手动触发爬取"""
    if scrape_status["is_running"]:
        return jsonify({"status": "already_running", "message": "爬虫正在运行中"})

    # 在后台线程中运行
    thread = threading.Thread(target=scheduled_scrape, daemon=True)
    thread.start()

    return jsonify({"status": "started", "message": "爬虫已启动"})


@app.route("/api/scrape/status")
def api_scrape_status():
    """查询爬虫状态"""
    stats = get_sources_stats()
    logs = get_latest_scrape_logs(10)

    # 序列化
    for s in stats:
        if isinstance(s.get("latest"), datetime):
            s["latest"] = s["latest"].isoformat()
    for l in logs:
        for k, v in l.items():
            if isinstance(v, datetime):
                l[k] = v.isoformat()

    return jsonify({
        "is_running": scrape_status["is_running"],
        "last_run": scrape_status["last_run"],
        "last_results": scrape_status["last_results"],
        "sources_stats": stats,
        "recent_logs": logs,
        "total_articles": get_article_count()
    })


@app.route("/api/llm/filter", methods=["POST"])
def api_llm_filter():
    """LLM 文章筛选"""
    data = request.json
    titles = data.get("titles", [])
    prompt = data.get("prompt", None)

    if not titles:
        return jsonify({"error": "标题列表为空"}), 400

    try:
        selected = filter_articles(titles, prompt)
        return jsonify({
            "total": len(titles),
            "selected_count": len(selected),
            "selected": selected
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/llm/report", methods=["POST"])
def api_llm_report():
    """LLM 生成报告"""
    data = request.json
    articles = data.get("articles", [])
    prompt = data.get("prompt", None)

    if not articles:
        return jsonify({"error": "文章列表为空"}), 400

    # 对没有正文的文章，先并发爬取正文（并发数 3）
    need_content = [(i, a) for i, a in enumerate(articles) if not a.get("content") and a.get("url")]

    def _fetch_content(item):
        idx, a = item
        url = a.get("url", "")
        source = a.get("source", "")
        content = scrape_article_content(url, source)
        return idx, url, content

    if need_content:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(_fetch_content, item) for item in need_content]
            for future in as_completed(futures):
                try:
                    idx, url, content = future.result()
                    if content:
                        articles[idx]["content"] = content
                        update_article_content(url, content)
                except Exception as e:
                    print(f"[爬取正文] 并发任务异常: {e}")

    try:
        report = generate_report(articles, prompt)
        # 自动保存到历史记录
        try:
            filter_titles = [a.get("title_cn") or a.get("title", "") for a in articles]
            save_report_history(filter_titles, report, articles)
        except Exception as he:
            print(f"[历史记录] 保存失败: {he}")
        return jsonify({"report": report})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/llm/report/stream", methods=["POST"])
def api_llm_report_stream():
    """SSE 流式报告生成（含进度推送）"""
    data = request.json
    articles = data.get("articles", [])
    prompt = data.get("prompt", None)

    if not articles:
        return jsonify({"error": "文章列表为空"}), 400

    def generate_events():
        import queue
        progress_q = queue.Queue()

        # -- Phase 1: 爬取正文 --
        need_content = [(i, a) for i, a in enumerate(articles)
                        if not a.get("content") and a.get("url")]
        total_crawl = len(need_content)

        if total_crawl > 0:
            crawl_done = [0]  # mutable counter

            def _fetch_one(item):
                idx, a = item
                url = a.get("url", "")
                source = a.get("source", "")
                title = a.get("title_cn") or a.get("title", "未知标题")
                content = scrape_article_content(url, source)
                return idx, url, title, content

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(_fetch_one, item): item for item in need_content}
                for future in as_completed(futures):
                    try:
                        idx, url, title, content = future.result()
                        crawl_done[0] += 1
                        preview = ""
                        if content:
                            articles[idx]["content"] = content
                            update_article_content(url, content)
                            preview = content[:150].replace("\n", " ") + ("…" if len(content) > 150 else "")
                        evt = json.dumps({
                            "phase": "crawl",
                            "current": crawl_done[0],
                            "total": total_crawl,
                            "title": title,
                            "content_preview": preview,
                            "success": bool(content)
                        }, ensure_ascii=False)
                        yield f"data: {evt}\n\n"
                    except Exception as e:
                        crawl_done[0] += 1
                        evt = json.dumps({
                            "phase": "crawl",
                            "current": crawl_done[0],
                            "total": total_crawl,
                            "title": "未知",
                            "content_preview": f"错误: {e}",
                            "success": False
                        }, ensure_ascii=False)
                        yield f"data: {evt}\n\n"
        else:
            evt = json.dumps({"phase": "crawl", "current": 0, "total": 0,
                              "title": "", "content_preview": "所有文章已有正文",
                              "success": True}, ensure_ascii=False)
            yield f"data: {evt}\n\n"

        # -- Phase 2: LLM 生成 --
        evt = json.dumps({"phase": "llm", "current": 0, "total": 1,
                          "message": "正在发送到 LLM 生成报告…"}, ensure_ascii=False)
        yield f"data: {evt}\n\n"

        try:
            def _llm_progress(current_batch, total_batches):
                progress_q.put((current_batch, total_batches))

            # 在子线程中运行 LLM 以便能 yield 进度
            result_holder = [None]
            error_holder = [None]

            def _run_llm():
                try:
                    result_holder[0] = generate_report(articles, prompt,
                                                       progress_callback=_llm_progress)
                except Exception as e:
                    error_holder[0] = e

            llm_thread = threading.Thread(target=_run_llm)
            llm_thread.start()

            # 等待 LLM 完成，同时发送批次进度
            while llm_thread.is_alive():
                llm_thread.join(timeout=0.5)
                while not progress_q.empty():
                    cur, tot = progress_q.get_nowait()
                    evt = json.dumps({"phase": "llm", "current": cur, "total": tot,
                                      "message": f"正在生成第 {cur}/{tot} 批报告…"},
                                     ensure_ascii=False)
                    yield f"data: {evt}\n\n"

            # drain remaining progress
            while not progress_q.empty():
                cur, tot = progress_q.get_nowait()
                evt = json.dumps({"phase": "llm", "current": cur, "total": tot,
                                  "message": f"正在生成第 {cur}/{tot} 批报告…"},
                                 ensure_ascii=False)
                yield f"data: {evt}\n\n"

            if error_holder[0]:
                raise error_holder[0]

            report = result_holder[0]

            # 自动保存到历史记录
            try:
                filter_titles = [a.get("title_cn") or a.get("title", "") for a in articles]
                save_report_history(filter_titles, report, articles)
            except Exception as he:
                print(f"[历史记录] 保存失败: {he}")

            # -- Phase 3: 完成 --
            evt = json.dumps({"phase": "done", "report": report}, ensure_ascii=False)
            yield f"data: {evt}\n\n"

        except Exception as e:
            evt = json.dumps({"phase": "error", "message": str(e)}, ensure_ascii=False)
            yield f"data: {evt}\n\n"

    return Response(generate_events(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/report/generate", methods=["POST"])
def api_report_generate():
    """生成 Word 文档"""
    report_data = request.json

    if not report_data:
        return jsonify({"error": "报告数据为空"}), 400

    try:
        buffer = generate_word_report(report_data)
        filename = generate_filename(report_data)
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/article/content", methods=["POST"])
def api_article_content():
    """爬取单篇文章正文"""
    data = request.json
    url = data.get("url", "")
    source = data.get("source", "")

    if not url:
        return jsonify({"error": "URL 为空"}), 400

    # 先检查数据库
    existing = get_article_by_url(url)
    if existing and existing.get("content"):
        return jsonify({"content": existing["content"], "cached": True})

    # 爬取
    content = scrape_article_content(url, source)
    if content:
        update_article_content(url, content)
        return jsonify({"content": content, "cached": False})

    return jsonify({"content": "", "error": "无法获取正文"}), 404


@app.route("/api/config", methods=["GET", "PUT"])
def api_config():
    """查看/修改配置"""
    if request.method == "GET":
        config = load_config()
        # 隐藏 API Key（只显示后 4 位）
        for key, provider in config.get("llm", {}).get("providers", {}).items():
            api_key = provider.get("api_key", "")
            if api_key and len(api_key) > 4:
                provider["api_key_masked"] = "***" + api_key[-4:]
            else:
                provider["api_key_masked"] = ""
        return jsonify(config)

    elif request.method == "PUT":
        new_config = request.json
        if not new_config:
            return jsonify({"error": "配置数据为空"}), 400

        # 合并更新（保留未修改的字段）
        config = load_config()

        # 更新 LLM 配置
        if "llm" in new_config:
            llm = new_config["llm"]
            if "active_provider" in llm:
                config["llm"]["active_provider"] = llm["active_provider"]
            if "providers" in llm:
                for key, val in llm["providers"].items():
                    if key in config["llm"]["providers"]:
                        for field in ["api_url", "api_key", "model", "max_tokens", "name"]:
                            if field in val and val[field]:
                                config["llm"]["providers"][key][field] = val[field]
                    else:
                        config["llm"]["providers"][key] = val

        # 更新提示词
        if "prompts" in new_config:
            config["prompts"].update(new_config["prompts"])

        # 更新爬虫配置
        if "scraper" in new_config:
            config["scraper"].update(new_config["scraper"])

        save_config(config)
        return jsonify({"status": "success", "message": "配置已保存"})


@app.route("/api/llm/test", methods=["POST"])
def api_llm_test():
    """测试 LLM 连接"""
    data = request.json or {}
    provider = data.get("provider", None)
    try:
        result = test_connection(provider)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})


# ===================================================================
# 启动
# ===================================================================
@app.route("/api/logs")
def api_logs():
    """获取实时日志"""
    from log_buffer import get_logs, get_all_logs
    since_id = request.args.get("since_id", 0, type=int)
    category = request.args.get("category", "")

    if since_id > 0:
        logs = get_logs(since_id)
    else:
        logs = get_all_logs()

    if category:
        logs = [l for l in logs if l["category"] == category]

    return jsonify({"logs": logs})


@app.route("/api/history")
def api_history_list():
    """获取报告历史列表"""
    limit = request.args.get("limit", 50, type=int)
    history = get_report_history_list(limit)
    # 转换 datetime 为字符串
    for h in history:
        if "created_at" in h:
            h["created_at"] = h["created_at"].strftime("%Y-%m-%d %H:%M:%S")
    return jsonify({"history": history})


@app.route("/api/history/<history_id>")
def api_history_detail(history_id):
    """获取报告历史详情"""
    try:
        detail = get_report_history_detail(history_id)
        if not detail:
            return jsonify({"error": "记录不存在"}), 404
        if "created_at" in detail:
            detail["created_at"] = detail["created_at"].strftime("%Y-%m-%d %H:%M:%S")
        return jsonify(detail)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    print("=" * 60)
    print("  国际移民新闻聚合系统")
    print("=" * 60)

    # 初始化数据库
    init_db()

    # 启动定时任务
    config = load_config()
    interval = config.get("scraper", {}).get("interval_hours", 24)

    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_scrape, 'interval', hours=interval, id='scrape_job')
    scheduler.start()
    print(f"  [✓] 定时爬取已启动（每 {interval} 小时）")

    print(f"  [✓] 访问 http://localhost:5100")
    print("=" * 60)

    app.run(host="0.0.0.0", port=5100, debug=False)
