"""
MongoDB 数据库操作模块
====================
管理新闻文章的存储、去重和查询。
"""

import json
import os
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError


# 配置文件路径
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")


def load_config():
    """加载配置文件"""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(config):
    """保存配置文件"""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def get_db():
    """获取 MongoDB 数据库连接"""
    config = load_config()
    mongo_cfg = config.get("mongodb", {})
    uri = mongo_cfg.get("uri", "mongodb://localhost:27017")
    db_name = mongo_cfg.get("db_name", "news_aggregator")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client[db_name]


def init_db():
    """初始化数据库，创建索引"""
    db = get_db()

    # articles 集合索引
    articles = db["articles"]
    articles.create_index("url", unique=True)
    articles.create_index("scraped_at")
    articles.create_index("source")
    articles.create_index("date")
    articles.create_index([("scraped_at", DESCENDING)])

    # scrape_logs 集合索引
    logs = db["scrape_logs"]
    logs.create_index("started_at")

    # report_history 集合索引
    history = db["report_history"]
    history.create_index([("created_at", DESCENDING)])

    print("[DB] 数据库索引初始化完成")
    return db


def upsert_article(article: dict) -> bool:
    """
    插入或更新文章（以 url 去重）。
    返回 True 表示新增，False 表示已存在。
    """
    db = get_db()
    collection = db["articles"]

    now = datetime.now()
    article.setdefault("created_at", now)
    article["scraped_at"] = now

    # 确保必要字段
    if not article.get("url"):
        return False

    try:
        result = collection.update_one(
            {"url": article["url"]},
            {"$setOnInsert": article},
            upsert=True
        )
        return result.upserted_id is not None  # True = 新插入
    except DuplicateKeyError:
        return False


def bulk_upsert_articles(articles: list) -> int:
    """
    批量插入文章，返回新增数量。
    """
    new_count = 0
    for article in articles:
        if upsert_article(article):
            new_count += 1
    return new_count


def get_articles_by_time_range(start: datetime = None, end: datetime = None,
                                source: str = None, limit: int = 5000) -> list:
    """
    按时间范围查询文章。
    默认查询最近 24 小时。
    """
    db = get_db()
    collection = db["articles"]

    if end is None:
        end = datetime.now()
    if start is None:
        start = end - timedelta(hours=24)

    query = {"scraped_at": {"$gte": start, "$lte": end}}
    if source:
        query["source"] = source

    cursor = collection.find(
        query,
        {"_id": 0}  # 排除 _id
    ).sort("scraped_at", DESCENDING).limit(limit)

    return list(cursor)


def get_article_by_url(url: str) -> dict:
    """根据 URL 获取单篇文章"""
    db = get_db()
    return db["articles"].find_one({"url": url}, {"_id": 0})


def update_article_content(url: str, content: str):
    """更新文章正文内容"""
    db = get_db()
    db["articles"].update_one(
        {"url": url},
        {"$set": {"content": content, "content_scraped_at": datetime.now()}}
    )


def update_article_title_cn(url: str, title_cn: str):
    """更新文章的中文翻译标题"""
    db = get_db()
    db["articles"].update_one(
        {"url": url},
        {"$set": {"title_cn": title_cn}}
    )


def get_article_count(source: str = None) -> int:
    """获取文章总数"""
    db = get_db()
    query = {"source": source} if source else {}
    return db["articles"].count_documents(query)


def get_sources_stats() -> list:
    """获取各来源的文章统计"""
    db = get_db()
    pipeline = [
        {"$group": {
            "_id": "$source",
            "count": {"$sum": 1},
            "latest": {"$max": "$scraped_at"}
        }},
        {"$sort": {"count": -1}}
    ]
    return list(db["articles"].aggregate(pipeline))


def log_scrape_run(source: str, new_count: int, total_count: int,
                    status: str = "success", error: str = None):
    """记录爬取运行日志"""
    db = get_db()
    db["scrape_logs"].insert_one({
        "source": source,
        "new_count": new_count,
        "total_count": total_count,
        "status": status,
        "error": error,
        "started_at": datetime.now()
    })


def get_latest_scrape_logs(limit: int = 20) -> list:
    """获取最近的爬取日志"""
    db = get_db()
    cursor = db["scrape_logs"].find(
        {}, {"_id": 0}
    ).sort("started_at", DESCENDING).limit(limit)
    return list(cursor)


def search_articles(keyword: str, limit: int = 100) -> list:
    """按关键词搜索文章标题"""
    db = get_db()
    cursor = db["articles"].find(
        {"title": {"$regex": keyword, "$options": "i"}},
        {"_id": 0}
    ).sort("scraped_at", DESCENDING).limit(limit)
    return list(cursor)


def save_report_history(filter_results: list, report: dict, articles: list) -> str:
    """
    保存报告生成记录。
    filter_results: 筛选结果标题列表
    report: LLM 生成的报告 JSON
    articles: 使用的文章列表（含 url, title, source, date）
    返回插入的文档 ID。
    """
    db = get_db()
    # 只保存文章的关键字段，不存正文以节省空间
    articles_slim = [{
        "title": a.get("title", ""),
        "title_cn": a.get("title_cn", ""),
        "url": a.get("url", ""),
        "source": a.get("source", ""),
        "date": a.get("date", ""),
    } for a in articles]

    doc = {
        "created_at": datetime.now(),
        "filter_results": filter_results,
        "report": report,
        "articles": articles_slim,
        "summary_count": len(report.get("summaries", [])),
        "title": report.get("title", "报告"),
        "date_range": f"{report.get('dateStart', '')}-{report.get('dateEnd', '')}",
    }
    result = db["report_history"].insert_one(doc)
    return str(result.inserted_id)


def get_report_history_list(limit: int = 50) -> list:
    """获取报告历史列表（摘要，不含完整报告内容）"""
    db = get_db()
    cursor = db["report_history"].find(
        {},
        {"report": 0, "articles": 0}  # 不返回大字段
    ).sort("created_at", DESCENDING).limit(limit)
    results = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


def get_report_history_detail(history_id: str) -> dict:
    """获取单条报告历史详情"""
    from bson import ObjectId
    db = get_db()
    doc = db["report_history"].find_one({"_id": ObjectId(history_id)})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc
