"""
香港新聞網 (hkcna.hk) 新闻爬虫
================================
爬取 https://www.hkcna.hk/ 香港中國通訊社的新闻文章。
支持多频道（港澳、大灣區、台灣、內地、國際等）爬取和 JSON/CSV 导出。

使用方法:
    pip install requests beautifulsoup4 lxml
    python hkcna_scraper.py

可选参数:
    --channel all       频道: all/gangao/taiwan/neidi/guoji/dawanqu (默认: all)
    --max-pages 10      每个频道最大翻页数 (默认: 10)
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
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs


# ===== 配置 =====
BASE_URL = "https://www.hkcna.hk"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,zh-CN;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.hkcna.hk/",
}

# 频道配置 (ID -> 名称, 列表页类型)
CHANNELS = {
    "gangao":   {"name": "港澳",   "id": "2804", "list": "index_col",      "sub": [4371, 4372, 2812, 2813]},
    "dawanqu":  {"name": "大灣區", "id": "5773", "list": "index_col_else", "sub": [5773]},
    "taiwan":   {"name": "台灣",   "id": "2805", "list": "index_col_else", "sub": [2805]},
    "neidi":    {"name": "內地",   "id": "2808", "list": "index_col_else", "sub": [2808]},
    "guoji":    {"name": "國際",   "id": "2810", "list": "index_col_else", "sub": [2810]},
}

# 文章 URL 正则
DOC_DETAIL_PATTERN = re.compile(r'docDetail\.jsp\?id=(\d+)(?:&(?:amp;)?channel=(\d+))?')


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
            elif resp.status_code in (403, 429):
                print(f"    [!] {resp.status_code} (尝试 {attempt+1}/{retries})")
                time.sleep(3 * (attempt + 1))
            else:
                print(f"    [!] 状态码 {resp.status_code}")
                return None
        except requests.RequestException as e:
            print(f"    [!] 请求异常: {e}")
            time.sleep(2)
    return None


def parse_doc_url(url):
    """从 docDetail URL 中提取 ID 和频道"""
    match = DOC_DETAIL_PATTERN.search(url)
    if match:
        return match.group(1), match.group(2)
    return None, None


def parse_article_links(html, base_url=BASE_URL):
    """解析页面中的所有文章链接"""
    soup = BeautifulSoup(html, "lxml")
    articles = []
    seen_ids = set()

    for link in soup.find_all("a", href=DOC_DETAIL_PATTERN):
        href = link.get("href", "")
        full_url = urljoin(base_url, href).replace("&amp;", "&")

        doc_id, channel_id = parse_doc_url(full_url)
        if not doc_id or doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)

        title = link.get_text(strip=True)
        if not title or len(title) < 3:
            continue

        # 清理标题（去除序号前缀如 "1." "2."）
        title = re.sub(r'^\d+\.\s*', '', title).strip()
        if not title:
            continue

        article = {
            "title": title,
            "url": full_url,
            "doc_id": doc_id,
            "channel_id": channel_id,
        }

        # 尝试从上下文获取日期 (格式: MM-DD)
        parent = link.parent
        if parent:
            parent_text = parent.get_text(strip=True)
            date_match = re.search(r'(\d{2})-(\d{2})', parent_text)
            if date_match:
                month = date_match.group(1)
                day = date_match.group(2)
                year = datetime.now().strftime("%Y")
                article["date"] = f"{year}-{month}-{day}"

        # 尝试获取摘要
        next_sib = link.find_next_sibling(string=True)
        if next_sib and len(next_sib.strip()) > 10:
            article["summary"] = next_sib.strip()[:200]
        else:
            # 检查父元素的下一个文本节点
            if parent:
                all_text = parent.get_text(separator="\n", strip=True)
                lines = [l.strip() for l in all_text.split("\n") if len(l.strip()) > 15]
                for line in lines:
                    if line != title and not line.startswith("更") and not re.match(r'^\d{2}-\d{2}', line):
                        article["summary"] = line[:200]
                        break

        articles.append(article)

    return articles


def scrape_homepage(session):
    """爬取首页（通过文字新闻列表页）"""
    print("\n[*] 爬取最新新闻列表...")
    # 首页根URL返回405，使用文字新闻列表页代替
    html = fetch_page(session, f"{BASE_URL}/channel_txt.jsp")
    if not html:
        return []

    articles = parse_article_links(html)
    print(f"  [✓] 最新新闻获取 {len(articles)} 条")
    return articles


def scrape_channel(session, channel_key, max_pages=10, delay=1.5):
    """爬取指定频道"""
    ch = CHANNELS.get(channel_key)
    if not ch:
        print(f"  [!] 未知频道: {channel_key}")
        return []

    print(f"\n[*] 爬取频道: {ch['name']} (channel={ch['id']})...")

    all_articles = []
    seen_ids = set()

    # 频道列表页
    list_type = ch["list"]
    url = f"{BASE_URL}/{list_type}.jsp?channel={ch['id']}"
    html = fetch_page(session, url)
    if html:
        articles = parse_article_links(html)
        for a in articles:
            if a["doc_id"] not in seen_ids:
                seen_ids.add(a["doc_id"])
                a["channel_name"] = ch["name"]
                all_articles.append(a)
        print(f"  [✓] 列表页获取 {len(articles)} 条，新增 {len(all_articles)} 条")
    time.sleep(delay)

    # 尝试分页 (page=2, page=3, ...)
    for page in range(2, max_pages + 1):
        paged_url = f"{url}&page={page}"
        html = fetch_page(session, paged_url)
        if not html:
            break

        articles = parse_article_links(html)
        new_count = 0
        for a in articles:
            if a["doc_id"] not in seen_ids:
                seen_ids.add(a["doc_id"])
                a["channel_name"] = ch["name"]
                all_articles.append(a)
                new_count += 1

        if new_count == 0:
            break

        print(f"  [✓] 第{page}页: {len(articles)} 条，新增 {new_count} 条")
        time.sleep(delay)

    # 子频道
    for sub_ch in ch.get("sub", []):
        if str(sub_ch) == ch["id"]:
            continue
        sub_url = f"{BASE_URL}/channel_txt.jsp?channel={sub_ch}"
        html = fetch_page(session, sub_url)
        if html:
            articles = parse_article_links(html)
            new_count = 0
            for a in articles:
                if a["doc_id"] not in seen_ids:
                    seen_ids.add(a["doc_id"])
                    a["channel_name"] = ch["name"]
                    all_articles.append(a)
                    new_count += 1
            if new_count > 0:
                print(f"  [✓] 子频道 {sub_ch}: 新增 {new_count} 条")
        time.sleep(delay)

    return all_articles


def fetch_article_detail(session, url, delay=1):
    """获取单篇文章详细内容"""
    time.sleep(delay)
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        detail = {}

        # 标题
        h1 = soup.find("h1")
        if h1:
            detail["full_title"] = h1.get_text(strip=True)

        # 时间和来源 — 通常在 newsCon 区域
        news_con = soup.find("div", class_="newsCon") or soup.find("div", class_="news")
        if news_con:
            time_elem = news_con.find("span", class_=re.compile(r"time|date")) or news_con.find("time")
            if time_elem:
                detail["publish_time"] = time_elem.get_text(strip=True)

            source_elem = news_con.find("span", class_=re.compile(r"source|from|author"))
            if source_elem:
                detail["source"] = source_elem.get_text(strip=True)

        # 尝试在 meta 中查找时间
        if "publish_time" not in detail:
            meta_time = soup.find("meta", attrs={"name": re.compile(r"pubdate|publishdate", re.I)})
            if meta_time:
                detail["publish_time"] = meta_time.get("content", "")

        # 正文
        content_area = (
            soup.find("div", class_="newsCon") or
            soup.find("div", class_="news") or
            soup.find("article") or
            soup.find("div", class_=re.compile(r"content|detail|body|text", re.I))
        )

        if content_area:
            for tag in content_area.find_all(["script", "style", "nav", "iframe"]):
                tag.decompose()

            paragraphs = content_area.find_all("p")
            if paragraphs:
                detail["paragraphs"] = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
                detail["content"] = "\n".join(detail["paragraphs"])

        # 图片
        if content_area:
            images = content_area.find_all("img", src=True)
            img_urls = []
            for img in images:
                src = img.get("src", "")
                if src and "logo" not in src.lower() and "icon" not in src.lower():
                    img_urls.append(urljoin(url, src))
            if img_urls:
                detail["images"] = img_urls

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
    parser = argparse.ArgumentParser(description="香港新聞網 (hkcna.hk) 新闻爬虫")
    parser.add_argument("--channel", default="all",
                        help="频道: all/gangao/taiwan/neidi/guoji/dawanqu (默认: all)")
    parser.add_argument("--max-pages", type=int, default=10, help="每频道最大翻页数 (默认: 10)")
    parser.add_argument("--output", default=None, help="输出JSON文件名")
    parser.add_argument("--csv", action="store_true", help="同时导出CSV")
    parser.add_argument("--detail", action="store_true", help="爬取文章详细内容")
    parser.add_argument("--delay", type=float, default=1.5, help="请求间隔秒数 (默认: 1.5)")
    args = parser.parse_args()

    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"hkcna_news_{timestamp}.json"

    channels_to_scrape = list(CHANNELS.keys()) if args.channel == "all" else [args.channel]

    print("=" * 60)
    print("  香港新聞網 (hkcna.hk) 新闻爬虫")
    print("=" * 60)
    print(f"  频道: {', '.join(CHANNELS[c]['name'] for c in channels_to_scrape if c in CHANNELS)}")
    print(f"  每频道最大页数: {args.max_pages}")
    print(f"  请求间隔: {args.delay}s")
    print(f"  爬取详情: {'是' if args.detail else '否'}")
    print(f"  输出文件: {args.output}")
    print("=" * 60)

    session = create_session()
    all_articles = []
    seen_ids = set()

    # 首页
    homepage_articles = scrape_homepage(session)
    for a in homepage_articles:
        if a["doc_id"] not in seen_ids:
            seen_ids.add(a["doc_id"])
            all_articles.append(a)
    time.sleep(args.delay)

    # 各频道
    for ch_key in channels_to_scrape:
        if ch_key not in CHANNELS:
            print(f"\n[!] 跳过未知频道: {ch_key}")
            continue
        ch_articles = scrape_channel(session, ch_key, args.max_pages, args.delay)
        new_count = 0
        for a in ch_articles:
            if a["doc_id"] not in seen_ids:
                seen_ids.add(a["doc_id"])
                all_articles.append(a)
                new_count += 1
        if new_count > 0:
            print(f"  [i] 频道 {CHANNELS[ch_key]['name']} 最终新增 {new_count} 条")

    # 爬取详情
    if args.detail and all_articles:
        print(f"\n[*] 开始爬取 {len(all_articles)} 篇文章详情...")
        for i, article in enumerate(all_articles):
            url = article.get("url", "")
            if not url:
                continue
            title_preview = article.get("title", "")[:30]
            print(f"  [{i+1}/{len(all_articles)}] {title_preview}...")
            detail = fetch_article_detail(session, url, delay=args.delay)
            if detail:
                article.update(detail)

    # 按 doc_id 降序排（最新的 id 最大）
    all_articles.sort(key=lambda x: int(x.get("doc_id", "0")), reverse=True)

    # 保存
    print(f"\n{'=' * 60}")
    print(f"  爬取完成！共 {len(all_articles)} 篇文章")
    print(f"{'=' * 60}")

    save_json(all_articles, args.output)

    if args.csv:
        csv_file = args.output.rsplit(".", 1)[0] + ".csv"
        save_csv(all_articles, csv_file)

    # 统计
    if all_articles:
        channels = {}
        for a in all_articles:
            c = a.get("channel_name", "首页")
            channels[c] = channels.get(c, 0) + 1
        print(f"\n--- 按频道统计 ---")
        for c, count in sorted(channels.items(), key=lambda x: -x[1]):
            print(f"  {c}: {count} 篇")

        print(f"\n--- 最新 5 篇 ---")
        for a in all_articles[:5]:
            date = a.get("date", "N/A")
            ch = a.get("channel_name", "")
            title = a.get("title", "N/A")[:50]
            print(f"  [{date}] [{ch}] {title}")

    return all_articles


if __name__ == "__main__":
    main()
