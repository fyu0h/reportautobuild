"""
VisaHQ 全球流动性新闻爬虫
================================
爬取 https://www.visahq.com/news/ 网站的新闻文章。
按日期倒序遍历，支持语言选择、日期范围过滤和 JSON/CSV 导出。

使用方法:
    pip install requests beautifulsoup4 lxml
    python visahq_scraper.py

可选参数:
    --lang zh           语言 (zh/en/fr/es/ar，默认: zh)
    --days 30           爬取最近N天的新闻 (默认: 30)
    --start 2026-03-01  起始日期
    --end 2026-03-11    结束日期
    --output news.json  输出文件名
    --csv               同时导出CSV
    --detail            爬取文章详细内容
    --delay 1.5         请求间隔秒数
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import argparse
import os
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin


# ===== 配置 =====
BASE_URL = "https://www.visahq.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 国家代码映射（常见的）
COUNTRY_CODES = {
    "us": "美国", "cn": "中国", "hk": "中国香港", "tw": "中国台湾",
    "jp": "日本", "kr": "韩国", "gb": "英国", "uk": "英国",
    "de": "德国", "fr": "法国", "au": "澳大利亚", "ca": "加拿大",
    "br": "巴西", "in": "印度", "ru": "俄罗斯", "sg": "新加坡",
    "th": "泰国", "my": "马来西亚", "id": "印度尼西亚", "vn": "越南",
    "ph": "菲律宾", "ae": "阿联酋", "sa": "沙特阿拉伯",
    "es": "西班牙", "it": "意大利", "nl": "荷兰", "be": "比利时",
    "ch": "瑞士", "at": "奥地利", "pl": "波兰", "pt": "葡萄牙",
    "se": "瑞典", "no": "挪威", "dk": "丹麦", "fi": "芬兰",
    "ie": "爱尔兰", "nz": "新西兰", "za": "南非", "eg": "埃及",
    "tr": "土耳其", "il": "以色列", "mx": "墨西哥", "ar": "阿根廷",
    "co": "哥伦比亚", "cl": "智利", "pe": "秘鲁", "ng": "尼日利亚",
    "ke": "肯尼亚", "gh": "加纳", "et": "埃塞俄比亚",
    "cy": "塞浦路斯", "kp": "朝鲜",
}


def create_session():
    """创建 Session"""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def build_news_url(lang, date_str=None):
    """构建新闻列表 URL
    - 首页: /zh/news/ 或 /news/
    - 某日: /news/2026-03-10/ (英文版按日期)
    - 中文首页: /zh/news/
    """
    if lang == "en":
        if date_str:
            return f"{BASE_URL}/news/{date_str}/"
        return f"{BASE_URL}/news/"
    else:
        if date_str:
            # 带日期的URL不带语言前缀也可以访问
            return f"{BASE_URL}/news/{date_str}/"
        return f"{BASE_URL}/{lang}/news/"


def fetch_main_page(session, lang="zh"):
    """获取新闻首页，提取所有文章链接"""
    url = build_news_url(lang)
    print(f"  请求: {url}")
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code == 200:
            return resp.text
        else:
            print(f"  [!] 状态码: {resp.status_code}")
            return None
    except requests.RequestException as e:
        print(f"  [!] 请求异常: {e}")
        return None


def fetch_date_page(session, date_str):
    """获取某一天的新闻"""
    url = f"{BASE_URL}/news/{date_str}/"
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code == 200:
            return resp.text
        elif resp.status_code == 404:
            return None  # 该日没有新闻
        else:
            print(f"  [!] {date_str} 状态码: {resp.status_code}")
            return None
    except requests.RequestException as e:
        print(f"  [!] 请求异常: {e}")
        return None


def parse_news_page(html, lang="zh"):
    """解析新闻页面，提取所有新闻条目"""
    soup = BeautifulSoup(html, "lxml")
    articles = []

    # 查找所有新闻链接 - VisaHQ 的文章 URL 模式: /news/YYYY-MM-DD/CC/slug/
    article_links = soup.find_all("a", href=re.compile(r"/news/\d{4}-\d{2}-\d{2}/\w{2}/"))

    seen_urls = set()
    for link in article_links:
        href = link.get("href", "")
        full_url = urljoin(BASE_URL, href)

        # 去重
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        article = {"url": full_url}

        # 从 URL 解析日期和国家
        match = re.search(r"/news/(\d{4}-\d{2}-\d{2})/(\w{2})/([^/]+)/", full_url)
        if match:
            article["date"] = match.group(1)
            country_code = match.group(2).lower()
            article["country_code"] = country_code
            article["country"] = COUNTRY_CODES.get(country_code, country_code.upper())
            article["slug"] = match.group(3)

        # 标题 - 链接文本
        title = link.get_text(strip=True)
        if title and len(title) > 5:
            # 清理标题（可能包含日期后缀）
            # 移除末尾的日期文本如 "3月 11, 2026"
            title = re.sub(r'\d{1,2}月\s*\d{1,2},?\s*\d{4}\s*$', '', title).strip()
            title = re.sub(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},?\s*\d{4}\s*$', '', title).strip()
            article["title"] = title

        # 查找摘要 - 通常在同一容器或相邻元素中
        parent = link.parent
        if parent:
            # 往上找到卡片容器
            card = parent
            for _ in range(5):
                if card.parent and len(card.parent.get_text(strip=True)) < 2000:
                    card = card.parent
                else:
                    break

            # 查找描述文本
            paragraphs = card.find_all("p")
            for p in paragraphs:
                p_text = p.get_text(strip=True)
                if len(p_text) > 30 and p_text != article.get("title", ""):
                    article["summary"] = p_text
                    break

            # 如果没找到 <p>，尝试从卡片的完整文本中提取
            if "summary" not in article:
                card_text = card.get_text(separator="\n", strip=True)
                lines = [l.strip() for l in card_text.split("\n") if len(l.strip()) > 30]
                for line in lines:
                    if line != article.get("title", "") and not line.startswith("http"):
                        article["summary"] = line
                        break

        if article.get("title"):
            articles.append(article)

    return articles


def fetch_article_detail(session, url, delay=1):
    """获取单篇文章的详细内容"""
    time.sleep(delay)
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        detail = {}

        # 文章正文 - 查找主要内容区域
        content_area = soup.find("article") or soup.find("div", class_=re.compile(r"content|article|body|text"))
        if content_area:
            paragraphs = content_area.find_all("p")
            detail["paragraphs"] = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
            detail["content"] = "\n".join(detail["paragraphs"])

        # 如果没找到 article 标签，尝试提取页面主体文本
        if "content" not in detail:
            main = soup.find("main") or soup.find("div", class_=re.compile(r"main|wrapper"))
            if main:
                paragraphs = main.find_all("p")
                texts = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20]
                if texts:
                    detail["paragraphs"] = texts
                    detail["content"] = "\n".join(texts)

        # 标签/关键词
        tags = soup.find_all("a", class_=re.compile(r"tag|topic|category"))
        if tags:
            detail["tags"] = list(set(t.get_text(strip=True) for t in tags if t.get_text(strip=True)))

        return detail if detail else None

    except Exception as e:
        print(f"    [!] 获取详情失败: {e}")
        return None


def save_json(data, filename):
    """保存为 JSON"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[✓] JSON 已保存: {filename} ({len(data)} 条)")


def save_csv(data, filename):
    """保存为 CSV"""
    if not data:
        return
    fieldnames = []
    for item in data:
        for key in item.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with open(filename, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    print(f"[✓] CSV 已保存: {filename} ({len(data)} 条)")


def main():
    parser = argparse.ArgumentParser(description="VisaHQ 全球流动性新闻爬虫")
    parser.add_argument("--lang", default="zh", help="语言: zh/en/fr/es/ar (默认: zh)")
    parser.add_argument("--days", type=int, default=30, help="爬取最近N天 (默认: 30)")
    parser.add_argument("--start", default=None, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--output", default=None, help="输出JSON文件名")
    parser.add_argument("--csv", action="store_true", help="同时导出CSV")
    parser.add_argument("--detail", action="store_true", help="爬取文章详细内容")
    parser.add_argument("--delay", type=float, default=1.5, help="请求间隔秒数 (默认: 1.5)")
    args = parser.parse_args()

    # 计算日期范围
    if args.end:
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    else:
        end_date = datetime.now()

    if args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
    else:
        start_date = end_date - timedelta(days=args.days)

    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"visahq_news_{timestamp}.json"

    print("=" * 60)
    print("  VisaHQ 全球流动性新闻爬虫")
    print("=" * 60)
    print(f"  语言: {args.lang}")
    print(f"  日期范围: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    print(f"  请求间隔: {args.delay}s")
    print(f"  爬取详情: {'是' if args.detail else '否'}")
    print(f"  输出文件: {args.output}")
    print("=" * 60)

    session = create_session()
    all_articles = []
    seen_urls = set()

    # 策略1: 先爬首页获取最新新闻
    print(f"\n[*] 获取新闻首页...")
    html = fetch_main_page(session, args.lang)
    if html:
        articles = parse_news_page(html, args.lang)
        for a in articles:
            url = a.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_articles.append(a)
        print(f"  [✓] 首页获取 {len(articles)} 条新闻，新增 {len(all_articles)} 条")

    # 策略2: 按日期遍历
    current_date = end_date
    empty_count = 0
    max_empty = 5  # 连续5天无新闻则停止

    while current_date >= start_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"\n[*] {date_str}...")

        html = fetch_date_page(session, date_str)
        if html:
            articles = parse_news_page(html, args.lang)
            new_count = 0
            for a in articles:
                url = a.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(a)
                    new_count += 1

            if new_count > 0:
                print(f"  [✓] {len(articles)} 条新闻，新增 {new_count} 条 (累计 {len(all_articles)} 条)")
                empty_count = 0
            else:
                if len(articles) > 0:
                    print(f"  [i] {len(articles)} 条新闻（均已存在）")
                else:
                    print(f"  [i] 当日无新闻")
                    empty_count += 1
        else:
            print(f"  [i] 无数据")
            empty_count += 1

        if empty_count >= max_empty:
            print(f"\n[i] 连续 {max_empty} 天无新闻，停止爬取")
            break

        current_date -= timedelta(days=1)
        time.sleep(args.delay)

    # 爬取详情
    if args.detail and all_articles:
        print(f"\n[*] 开始爬取 {len(all_articles)} 篇文章详情...")
        for i, article in enumerate(all_articles):
            url = article.get("url", "")
            if not url:
                continue
            title_preview = article.get("title", "")[:40]
            print(f"  [{i+1}/{len(all_articles)}] {title_preview}...")
            detail = fetch_article_detail(session, url, delay=args.delay)
            if detail:
                article.update(detail)

    # 按日期排序（最新的在前）
    all_articles.sort(key=lambda x: x.get("date", ""), reverse=True)

    # 保存结果
    print(f"\n{'=' * 60}")
    print(f"  爬取完成！共 {len(all_articles)} 篇文章")
    print(f"{'=' * 60}")

    save_json(all_articles, args.output)

    if args.csv:
        csv_file = args.output.rsplit(".", 1)[0] + ".csv"
        save_csv(all_articles, csv_file)

    # 统计摘要
    if all_articles:
        countries = {}
        for a in all_articles:
            c = a.get("country", "未知")
            countries[c] = countries.get(c, 0) + 1

        print(f"\n--- 按国家/地区统计 ---")
        for c, count in sorted(countries.items(), key=lambda x: -x[1])[:15]:
            print(f"  {c}: {count} 篇")

        print(f"\n--- 最新 5 篇 ---")
        for a in all_articles[:5]:
            print(f"  [{a.get('date', 'N/A')}] [{a.get('country', 'N/A')}] {a.get('title', 'N/A')[:55]}")

    return all_articles


if __name__ == "__main__":
    main()
