"""
大公文匯網新闻爬虫
================================
爬取 https://www.tkww.hk/ 及电子报 https://epaper.tkww.hk/ 的新闻文章。
支持频道筛选、分页遍历和 JSON/CSV 导出。

使用方法:
    pip install requests beautifulsoup4 lxml
    python tkww_scraper.py

可选参数:
    --source main        数据源: main(主站)/epaper(电子报)/both (默认: main)
    --max-pages 20       主站最大翻页数 (默认: 20)
    --output news.json   输出文件名
    --csv                同时导出CSV
    --detail             爬取文章详细内容
    --delay 1.5          请求间隔秒数
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import argparse
import re
from datetime import datetime
from urllib.parse import urljoin


# ===== 配置 =====
BASE_URL = "https://www.tkww.hk"
EPAPER_URL = "https://epaper.tkww.hk"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 电子报栏目 ID -> 名称
EPAPER_SECTIONS = {
    "dgeconomic":       "經濟",
    "dgProperty":       "地產",
    "dginternational":  "國際",
    "dgphysicaleducation": "體育",
    "dgCulture":        "文化",
    "dgsupplement":     "副刊",
    "dgteach":          "教育",
    "sprincipality":    "小公園",
    "bprincipality":    "大公園",
}

# 主站频道
MAIN_CHANNELS = {
    "home":          "/",
    "time":          "/time",
    "HK_Macao":      "/info/HK_Macao",
    "gedidongtai":   "/info/gedidongtai",
    "dwhealth":      "/info/dwhealth",
}

# 文章 URL 正则: /a/YYYYMM/DD/APxxxxxxxxx.html
ARTICLE_URL_PATTERN = re.compile(r'/a/(\d{6})/(\d{2})/AP([0-9a-f]+)\.html')


def create_session():
    """创建 Session"""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def fetch_page(session, url, retries=3):
    """带重试的页面获取"""
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code == 403:
                print(f"    [!] 403 (尝试 {attempt+1}/{retries})")
                time.sleep(3 * (attempt + 1))
            else:
                print(f"    [!] 状态码 {resp.status_code}")
                return None
        except requests.RequestException as e:
            print(f"    [!] 请求异常: {e}")
            time.sleep(2)
    return None


def parse_article_url(url):
    """从文章 URL 中提取日期和 ID"""
    match = ARTICLE_URL_PATTERN.search(url)
    if match:
        ym = match.group(1)  # e.g. "202603"
        day = match.group(2)  # e.g. "11"
        article_id = match.group(3)
        date_str = f"{ym[:4]}-{ym[4:]}-{day}"
        return date_str, article_id
    return None, None


def scrape_main_site(session, max_pages=20, delay=1.5):
    """爬取主站 (tkww.hk) 首页和时间链"""
    all_articles = []
    seen_urls = set()

    # 首页
    print("\n[*] 爬取主站首页...")
    html = fetch_page(session, BASE_URL)
    if html:
        articles = parse_article_links(html, BASE_URL)
        for a in articles:
            if a["url"] not in seen_urls:
                seen_urls.add(a["url"])
                a["source"] = "主站首页"
                all_articles.append(a)
        print(f"  [✓] 首页获取 {len(articles)} 条，新增 {len(all_articles)} 条")
    time.sleep(delay)

    # 时间链 (最新新闻流)
    print("\n[*] 爬取时间链...")
    html = fetch_page(session, f"{BASE_URL}/time")
    if html:
        articles = parse_article_links(html, BASE_URL)
        new_count = 0
        for a in articles:
            if a["url"] not in seen_urls:
                seen_urls.add(a["url"])
                a["source"] = "時間鏈"
                all_articles.append(a)
                new_count += 1
        print(f"  [✓] 时间链获取 {len(articles)} 条，新增 {new_count} 条")
    time.sleep(delay)

    # 各频道
    for ch_name, ch_path in MAIN_CHANNELS.items():
        if ch_name in ("home", "time"):
            continue
        print(f"\n[*] 爬取频道: {ch_name}...")
        html = fetch_page(session, f"{BASE_URL}{ch_path}")
        if html:
            articles = parse_article_links(html, BASE_URL)
            new_count = 0
            for a in articles:
                if a["url"] not in seen_urls:
                    seen_urls.add(a["url"])
                    a["source"] = ch_name
                    all_articles.append(a)
                    new_count += 1
            print(f"  [✓] 获取 {len(articles)} 条，新增 {new_count} 条")
        time.sleep(delay)

    return all_articles


def scrape_epaper(session, delay=1.5):
    """爬取电子报 (epaper.tkww.hk / takungpao.com)"""
    all_articles = []
    seen_urls = set()

    # 电子报首页
    print("\n[*] 爬取电子报首页...")
    html = fetch_page(session, "https://www.takungpao.com/")
    if html:
        articles = parse_article_links(html, EPAPER_URL)
        for a in articles:
            if a["url"] not in seen_urls:
                seen_urls.add(a["url"])
                a["source"] = "電子報"
                all_articles.append(a)
        print(f"  [✓] 首页获取 {len(articles)} 条，新增 {len(all_articles)} 条")
    time.sleep(delay)

    # 各栏目
    for sec_id, sec_name in EPAPER_SECTIONS.items():
        print(f"\n[*] 爬取栏目: {sec_name} ({sec_id})...")
        url = f"{EPAPER_URL}/{sec_id}"
        html = fetch_page(session, url)
        if html:
            articles = parse_article_links(html, EPAPER_URL)
            new_count = 0
            for a in articles:
                if a["url"] not in seen_urls:
                    seen_urls.add(a["url"])
                    a["source"] = f"電子報-{sec_name}"
                    a["category"] = sec_name
                    all_articles.append(a)
                    new_count += 1
            print(f"  [✓] 获取 {len(articles)} 条，新增 {new_count} 条")
        time.sleep(delay)

    return all_articles


def parse_article_links(html, base_url):
    """解析页面中的所有文章链接"""
    soup = BeautifulSoup(html, "lxml")
    articles = []
    seen = set()

    # 查找所有符合文章 URL 模式的链接
    for link in soup.find_all("a", href=ARTICLE_URL_PATTERN):
        href = link.get("href", "")
        full_url = urljoin(base_url, href)

        # 规范化 URL
        full_url = full_url.split("?")[0].split("#")[0]
        if full_url in seen:
            continue
        seen.add(full_url)

        # 标题
        title = link.get_text(strip=True)
        if not title or len(title) < 3:
            # 尝试从父元素或子元素获取标题
            parent = link.parent
            if parent:
                title = parent.get_text(strip=True)
            if not title or len(title) < 3:
                continue

        # 清理标题（去掉可能混入的其他文本）
        title = title.strip()
        if len(title) > 200:
            title = title[:200]

        article = {"title": title, "url": full_url}

        # 从 URL 提取日期
        date_str, article_id = parse_article_url(full_url)
        if date_str:
            article["date"] = date_str
            article["article_id"] = article_id

        articles.append(article)

    return articles


def fetch_article_detail(session, url, delay=1):
    """获取单篇文章详细内容"""
    time.sleep(delay)
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        detail = {}

        # 标题 (更精确)
        h1 = soup.find("h1")
        if h1:
            detail["full_title"] = h1.get_text(strip=True)

        # 发布时间
        time_elem = soup.find("time") or soup.find("span", class_=re.compile(r"time|date|publish"))
        if time_elem:
            detail["publish_time"] = time_elem.get_text(strip=True)
            if time_elem.get("datetime"):
                detail["datetime"] = time_elem["datetime"]

        # 来源/作者
        source_elem = soup.find("span", class_=re.compile(r"source|author|from"))
        if source_elem:
            detail["author"] = source_elem.get_text(strip=True)

        # 正文内容
        # 大公文匯網的文章正文通常在 article 或特定的 div 中
        content_area = (
            soup.find("article") or
            soup.find("div", class_=re.compile(r"article|content|body|text|detail", re.I)) or
            soup.find("div", id=re.compile(r"article|content|body", re.I))
        )

        if content_area:
            # 移除脚本和样式
            for tag in content_area.find_all(["script", "style", "nav", "header", "footer"]):
                tag.decompose()

            paragraphs = content_area.find_all("p")
            detail["paragraphs"] = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
            detail["content"] = "\n".join(detail["paragraphs"])

        # 图片
        if content_area:
            images = content_area.find_all("img", src=True)
            img_urls = []
            for img in images:
                src = img.get("src", "")
                if src and not src.endswith((".svg", ".gif")) and "icon" not in src.lower():
                    img_urls.append(urljoin(url, src))
            if img_urls:
                detail["images"] = img_urls

        # 标签
        tags = soup.find_all("a", class_=re.compile(r"tag|keyword|topic"))
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
            if key not in fieldnames and key not in ("paragraphs", "images"):
                fieldnames.append(key)
    with open(filename, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    print(f"[✓] CSV 已保存: {filename} ({len(data)} 条)")


def main():
    parser = argparse.ArgumentParser(description="大公文匯網新闻爬虫")
    parser.add_argument("--source", default="both",
                        choices=["main", "epaper", "both"],
                        help="数据源: main/epaper/both (默认: both)")
    parser.add_argument("--max-pages", type=int, default=20, help="最大翻页数 (默认: 20)")
    parser.add_argument("--output", default=None, help="输出JSON文件名")
    parser.add_argument("--csv", action="store_true", help="同时导出CSV")
    parser.add_argument("--detail", action="store_true", help="爬取文章详细内容")
    parser.add_argument("--delay", type=float, default=1.5, help="请求间隔秒数 (默认: 1.5)")
    args = parser.parse_args()

    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"tkww_news_{timestamp}.json"

    print("=" * 60)
    print("  大公文匯網新闻爬虫")
    print("=" * 60)
    print(f"  数据源: {args.source}")
    print(f"  请求间隔: {args.delay}s")
    print(f"  爬取详情: {'是' if args.detail else '否'}")
    print(f"  输出文件: {args.output}")
    print("=" * 60)

    session = create_session()
    all_articles = []
    seen_urls = set()

    # 爬取主站
    if args.source in ("main", "both"):
        main_articles = scrape_main_site(session, args.max_pages, args.delay)
        for a in main_articles:
            if a["url"] not in seen_urls:
                seen_urls.add(a["url"])
                all_articles.append(a)

    # 爬取电子报
    if args.source in ("epaper", "both"):
        epaper_articles = scrape_epaper(session, args.delay)
        for a in epaper_articles:
            if a["url"] not in seen_urls:
                seen_urls.add(a["url"])
                all_articles.append(a)

    # 爬取详情
    if args.detail and all_articles:
        print(f"\n[*] 开始爬取 {len(all_articles)} 篇文章详情...")
        for i, article in enumerate(all_articles):
            url = article.get("url", "")
            if not url:
                continue
            title_preview = article.get("title", "")[:35]
            print(f"  [{i+1}/{len(all_articles)}] {title_preview}...")
            detail = fetch_article_detail(session, url, delay=args.delay)
            if detail:
                article.update(detail)

    # 按日期排序
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
        # 按来源统计
        sources = {}
        for a in all_articles:
            s = a.get("source", "未知")
            sources[s] = sources.get(s, 0) + 1
        print(f"\n--- 按来源统计 ---")
        for s, count in sorted(sources.items(), key=lambda x: -x[1]):
            print(f"  {s}: {count} 篇")

        # 按日期统计
        dates = {}
        for a in all_articles:
            d = a.get("date", "未知")
            dates[d] = dates.get(d, 0) + 1
        print(f"\n--- 按日期统计 ---")
        for d, count in sorted(dates.items(), reverse=True)[:7]:
            print(f"  {d}: {count} 篇")

        print(f"\n--- 最新 5 篇 ---")
        for a in all_articles[:5]:
            print(f"  [{a.get('date', 'N/A')}] {a.get('title', 'N/A')[:50]}")

    return all_articles


if __name__ == "__main__":
    main()
