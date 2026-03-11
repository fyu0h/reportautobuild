"""
Fragomen 国际移民新闻爬虫
================================
爬取 https://www.fragomen.com/insights/ 网站的所有新闻文章。
支持分页遍历、内容提取和结果导出（JSON/CSV）。

使用方法:
    pip install requests beautifulsoup4 lxml
    python fragomen_scraper.py

可选参数:
    --type news         内容类型 (news/blog_post/event/podcast,video)
    --max-pages 50      最大爬取页数
    --output news.json  输出文件名
    --csv               同时导出CSV
    --detail            爬取每篇文章的详细内容
    --delay 2           请求间隔秒数
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import argparse
import os
import sys
import re
from datetime import datetime
from urllib.parse import urljoin, urlencode


# ===== 配置 =====
BASE_URL = "https://www.fragomen.com"
INSIGHTS_URL = f"{BASE_URL}/insights/index.html"
PAGE_SIZE = 20  # 每页20条

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


def create_session():
    """创建带有持久化 Cookie 的 Session"""
    session = requests.Session()
    session.headers.update(HEADERS)
    # 先访问主页获取 Cookie
    print("[*] 初始化会话，获取 Cookie...")
    try:
        resp = session.get(f"{BASE_URL}/", timeout=15)
        print(f"    主页状态码: {resp.status_code}")
    except Exception as e:
        print(f"    [!] 警告: 无法访问主页 - {e}")
    return session


def fetch_page(session, content_type="news", offset=0, retries=3):
    """获取指定偏移量的列表页"""
    params = {"type": content_type}
    if offset > 0:
        params["f"] = str(offset)

    url = f"{INSIGHTS_URL}?{urlencode(params)}"

    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code == 403:
                print(f"    [!] 403 Forbidden (尝试 {attempt+1}/{retries})，等待后重试...")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"    [!] 状态码 {resp.status_code}")
                return None
        except requests.RequestException as e:
            print(f"    [!] 请求异常: {e}")
            time.sleep(3)

    return None


def parse_list_page(html):
    """解析列表页，提取所有新闻条目"""
    soup = BeautifulSoup(html, "lxml")
    articles = []

    # 查找所有新闻卡片 - 使用多种选择器兼容
    cards = soup.select('div[class*="styles__card"]')
    if not cards:
        # 备用选择器
        cards = soup.select('div[class*="result"]')

    for card in cards:
        article = {}

        # 标题和链接
        title_elem = card.select_one('a[class*="type__h6"]')
        if not title_elem:
            title_elem = card.select_one('a[class*="galleryView"]')
        if not title_elem:
            title_elem = card.select_one("h6 a, h5 a, h4 a")

        if title_elem:
            article["title"] = title_elem.get_text(strip=True)
            href = title_elem.get("href", "")
            if href:
                article["url"] = urljoin(BASE_URL, href)
        else:
            continue  # 没有标题的跳过

        # 元数据 (类型 | 日期 | 国家/地区)
        meta_elem = card.select_one('div[class*="type__level7"]')
        if not meta_elem:
            meta_elem = card.select_one('div[class*="level7"]')

        if meta_elem:
            meta_text = meta_elem.get_text(strip=True)
            article["meta_raw"] = meta_text
            # 解析 "Immigration alert | March 10, 2026 | Armenia"
            parts = [p.strip() for p in meta_text.split("|")]
            if len(parts) >= 1:
                article["category"] = parts[0]
            if len(parts) >= 2:
                article["date"] = parts[1]
            if len(parts) >= 3:
                article["region"] = " | ".join(parts[2:])

        # 摘要/描述
        desc_elem = card.select_one('div[class*="description"], p[class*="description"]')
        if desc_elem:
            article["description"] = desc_elem.get_text(strip=True)

        if article.get("title"):
            articles.append(article)

    # 检查是否有下一页
    next_page = soup.select_one('a[aria-label="Next page"]')
    has_next = next_page is not None

    return articles, has_next


def fetch_article_detail(session, url, delay=1):
    """获取单篇文章的详细内容"""
    time.sleep(delay)
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        detail = {}

        # 文章正文
        content_area = soup.select_one(
            'div[class*="richText"], div[class*="article-body"], '
            'div[class*="content-area"], article'
        )
        if content_area:
            # 提取纯文本
            detail["content"] = content_area.get_text(separator="\n", strip=True)
            # 提取所有段落
            paragraphs = content_area.find_all("p")
            detail["paragraphs"] = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]

        # 发布日期（详细页可能有更精确的日期）
        date_elem = soup.select_one('time, span[class*="date"], div[class*="date"]')
        if date_elem:
            detail["publish_date"] = date_elem.get_text(strip=True)
            if date_elem.get("datetime"):
                detail["datetime"] = date_elem["datetime"]

        # 作者
        author_elem = soup.select_one('span[class*="author"], div[class*="author"], a[class*="author"]')
        if author_elem:
            detail["author"] = author_elem.get_text(strip=True)

        # 标签/关键词
        tags = soup.select('a[class*="tag"], span[class*="tag"], a[class*="topic"]')
        if tags:
            detail["tags"] = [t.get_text(strip=True) for t in tags]

        return detail

    except Exception as e:
        print(f"    [!] 获取详情失败: {e}")
        return None


def save_json(data, filename):
    """保存为 JSON 文件"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[✓] 已保存 JSON: {filename} ({len(data)} 条)")


def save_csv(data, filename):
    """保存为 CSV 文件"""
    if not data:
        return

    # 收集所有可能的字段
    fieldnames = []
    for item in data:
        for key in item.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with open(filename, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    print(f"[✓] 已保存 CSV: {filename} ({len(data)} 条)")


def main():
    parser = argparse.ArgumentParser(description="Fragomen 国际移民新闻爬虫")
    parser.add_argument("--type", default="news", help="内容类型: news/blog_post/event (默认: news)")
    parser.add_argument("--max-pages", type=int, default=100, help="最大爬取页数 (默认: 100)")
    parser.add_argument("--output", default=None, help="输出JSON文件名")
    parser.add_argument("--csv", action="store_true", help="同时导出CSV")
    parser.add_argument("--detail", action="store_true", help="爬取每篇文章的详细内容")
    parser.add_argument("--delay", type=float, default=2.0, help="请求间隔秒数 (默认: 2.0)")
    args = parser.parse_args()

    # 默认输出文件名
    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"fragomen_{args.type}_{timestamp}.json"

    print("=" * 60)
    print("  Fragomen 国际移民新闻爬虫")
    print("=" * 60)
    print(f"  内容类型: {args.type}")
    print(f"  最大页数: {args.max_pages}")
    print(f"  请求间隔: {args.delay}s")
    print(f"  爬取详情: {'是' if args.detail else '否'}")
    print(f"  输出文件: {args.output}")
    print("=" * 60)

    session = create_session()
    all_articles = []
    seen_urls = set()
    page = 0

    while page < args.max_pages:
        offset = page * PAGE_SIZE
        print(f"\n[*] 第 {page + 1} 页 (offset={offset})...")

        html = fetch_page(session, args.type, offset)
        if not html:
            print("    [!] 获取页面失败，停止爬取")
            break

        articles, has_next = parse_list_page(html)

        if not articles:
            print("    [i] 该页无新闻条目，爬取完成")
            break

        # 去重
        new_count = 0
        for article in articles:
            url = article.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_articles.append(article)
                new_count += 1

        print(f"    [✓] 获取 {len(articles)} 条，新增 {new_count} 条 (累计 {len(all_articles)} 条)")

        if not has_next:
            print("    [i] 没有下一页，爬取完成")
            break

        if new_count == 0:
            print("    [i] 无新增条目，爬取完成")
            break

        page += 1
        time.sleep(args.delay)

    # 爬取详情
    if args.detail and all_articles:
        print(f"\n[*] 开始爬取 {len(all_articles)} 篇文章详情...")
        for i, article in enumerate(all_articles):
            url = article.get("url", "")
            if not url:
                continue
            print(f"    [{i+1}/{len(all_articles)}] {article.get('title', '')[:50]}...")
            detail = fetch_article_detail(session, url, delay=args.delay)
            if detail:
                article.update(detail)

    # 保存结果
    print(f"\n{'=' * 60}")
    print(f"  爬取完成！共 {len(all_articles)} 篇文章")
    print(f"{'=' * 60}")

    save_json(all_articles, args.output)

    if args.csv:
        csv_file = args.output.rsplit(".", 1)[0] + ".csv"
        save_csv(all_articles, csv_file)

    # 打印摘要
    if all_articles:
        print(f"\n--- 最新 5 篇 ---")
        for a in all_articles[:5]:
            print(f"  [{a.get('date', 'N/A')}] {a.get('title', 'N/A')[:60]}")
            print(f"    {a.get('url', '')}")

    return all_articles


if __name__ == "__main__":
    main()
