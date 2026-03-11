"""
统一爬虫模块
===========
将 4 个新闻源（Fragomen、VisaHQ、大公文匯、香港新聞網）封装为统一接口。
"""

import logging
import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlencode
from abc import ABC, abstractmethod

# 配置日志
logger = logging.getLogger("爬虫")
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
    bh = BufferHandler(category="scraper")
    bh.setLevel(logging.INFO)
    bh.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(bh)


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


class BaseScraper(ABC):
    """爬虫基类"""

    source_name = "unknown"

    def __init__(self, delay=1.5):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def fetch(self, url, retries=3):
        """带重试的页面获取"""
        for attempt in range(retries):
            try:
                logger.info(f"[请求] GET {url} (第{attempt+1}/{retries}次)")
                resp = self.session.get(url, timeout=20)
                logger.info(f"[响应] {resp.status_code} | {len(resp.text)} 字符 | {url}")
                from log_buffer import add_log
                add_log("scraper", "info", f"[响应] {resp.status_code} | {len(resp.text)} 字符 | {url}", resp.text[:3000])
                if resp.status_code == 200:
                    return resp.text
                elif resp.status_code in (403, 429):
                    logger.warning(f"[被拦截] {resp.status_code} {url}，等待重试...")
                    time.sleep(3 * (attempt + 1))
                else:
                    logger.warning(f"[错误] 状态码 {resp.status_code} {url}")
                    return None
            except requests.RequestException as e:
                logger.error(f"[异常] {url} - {e}")
                time.sleep(2)
        logger.error(f"[失败] {url} 全部 {retries} 次尝试均失败")
        return None

    @abstractmethod
    def scrape(self) -> list:
        """爬取文章列表，返回标准化字典列表"""
        pass

    def scrape_detail(self, url) -> dict:
        """爬取文章详情，返回 {content, paragraphs, ...}"""
        time.sleep(self.delay)
        try:
            logger.info(f"[详情请求] GET {url}")
            resp = self.session.get(url, timeout=20)
            logger.info(f"[详情响应] {resp.status_code} | {len(resp.text)} 字符")
            if resp.status_code != 200:
                return {}
            soup = BeautifulSoup(resp.text, "lxml")
            content_area = (
                soup.find("article") or
                soup.find("div", class_=re.compile(r"content|article|body|text|detail|richText", re.I))
            )
            if content_area:
                for tag in content_area.find_all(["script", "style", "nav", "iframe"]):
                    tag.decompose()
                paragraphs = [p.get_text(strip=True) for p in content_area.find_all("p") if p.get_text(strip=True)]
                logger.info(f"[详情解析] 提取 {len(paragraphs)} 段落 | {url}")
                from log_buffer import add_log
                add_log("scraper", "info", f"[详情解析] 提取 {len(paragraphs)} 段落 | {url}", "\n\n".join(paragraphs))
                return {"content": "\n".join(paragraphs), "paragraphs": paragraphs}
            logger.warning(f"[详情为空] 未找到正文区域 | {url}")
            return {}
        except Exception as e:
            logger.error(f"[详情错误] {url} - {e}")
            return {}

    def _normalize(self, article: dict) -> dict:
        """标准化文章字典"""
        article["source"] = self.source_name
        # 标准化日期为 YYYY-MM-DD
        date = article.get("date", "")
        if date and not re.match(r"\d{4}-\d{2}-\d{2}", date):
            try:
                for fmt in ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%Y/%m/%d"]:
                    try:
                        dt = datetime.strptime(date.strip(), fmt)
                        article["date"] = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
            except Exception:
                pass
        # 如果仍然没有日期，设为今天
        if not article.get("date"):
            article["date"] = datetime.now().strftime("%Y-%m-%d")
        # 清理标题
        title = article.get("title", "").strip()
        title = re.sub(r'\s+', ' ', title)
        article["title"] = title
        # 如果有 description 且没有 content，用 description 作为 content
        if article.get("description") and not article.get("content"):
            article["content"] = article["description"]
        return article


# ===================================================================
# Fragomen 爬虫
# ===================================================================
class FragomenScraper(BaseScraper):
    source_name = "Fragomen"

    def scrape(self) -> list:
        articles = []
        seen = set()
        page = 0
        max_pages = 10

        while page < max_pages:
            offset = page * 20
            params = {"nt": "109097", "type": "news"}
            if offset > 0:
                params["f"] = str(offset)
            url = f"https://www.fragomen.com/insights/index.html?{urlencode(params)}"
            html = self.fetch(url)
            if not html:
                break

            soup = BeautifulSoup(html, "lxml")

            # 以标题链接为锚点遍历，不依赖卡片容器 class
            title_links = soup.select('a[class*="type__h6"]')
            if not title_links:
                title_links = soup.select('a[class*="galleryView"]')
            if not title_links:
                break

            new_count = 0
            for title_elem in title_links:
                # 标题：优先从 span.rte-title-mode 提取
                rte_span = title_elem.select_one('span.rte-title-mode')
                title = rte_span.get_text(strip=True) if rte_span else title_elem.get_text(strip=True)
                if not title or len(title) < 3:
                    continue

                href = title_elem.get("href", "")
                full_url = urljoin("https://www.fragomen.com", href) if href else ""
                if not full_url or full_url in seen:
                    continue
                seen.add(full_url)

                article = {"title": title, "url": full_url}

                # 向上查找父容器，从中提取元数据
                container = title_elem.find_parent("div")
                if container:
                    container = container.find_parent("div") or container

                if container:
                    # 元数据：匹配 p 或 div 中含 type__level7 的元素
                    meta = container.select_one('[class*="type__level7"]')
                    if meta:
                        parts = [p.strip() for p in meta.get_text(strip=True).split("|")]
                        if len(parts) >= 1:
                            article["category"] = parts[0]
                        if len(parts) >= 2:
                            article["date"] = parts[1]
                        if len(parts) >= 3:
                            article["country"] = " | ".join(parts[2:])

                articles.append(self._normalize(article))
                new_count += 1

            next_page = soup.select_one('a[aria-label="Next page"]')
            if not next_page or new_count == 0:
                break
            page += 1
            time.sleep(self.delay)

        return articles


# ===================================================================
# VisaHQ 爬虫
# ===================================================================
class VisaHQScraper(BaseScraper):
    source_name = "VisaHQ"

    COUNTRY_CODES = {
        "us": "美国", "cn": "中国", "hk": "中国香港", "jp": "日本",
        "kr": "韩国", "gb": "英国", "de": "德国", "fr": "法国",
        "au": "澳大利亚", "ca": "加拿大", "br": "巴西", "in": "印度",
        "sg": "新加坡", "th": "泰国", "ae": "阿联酋", "es": "西班牙",
        "it": "意大利", "ch": "瑞士", "at": "奥地利", "pl": "波兰",
        "pt": "葡萄牙", "se": "瑞典", "fi": "芬兰", "nz": "新西兰",
        "tr": "土耳其", "mx": "墨西哥", "cy": "塞浦路斯", "kp": "朝鲜",
    }

    def scrape(self, days=7) -> list:
        articles = []
        seen = set()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        current = end_date
        empty_count = 0

        # 首页
        html = self.fetch("https://www.visahq.com/zh/news/")
        if html:
            arts = self._parse_page(html)
            for a in arts:
                if a["url"] not in seen:
                    seen.add(a["url"])
                    articles.append(a)
        time.sleep(self.delay)

        # 按日期遍历
        while current >= start_date and empty_count < 5:
            date_str = current.strftime("%Y-%m-%d")
            html = self.fetch(f"https://www.visahq.com/news/{date_str}/")
            if html:
                arts = self._parse_page(html)
                new = [a for a in arts if a["url"] not in seen]
                for a in new:
                    seen.add(a["url"])
                    articles.append(a)
                empty_count = 0 if new else empty_count + 1
            else:
                empty_count += 1
            current -= timedelta(days=1)
            time.sleep(self.delay)

        return articles

    def _parse_page(self, html) -> list:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        for link in soup.find_all("a", href=re.compile(r"/news/\d{4}-\d{2}-\d{2}/\w{2}/")):
            href = link.get("href", "")
            full_url = urljoin("https://www.visahq.com", href)

            # 优先从 <h3> 提取标题
            h3 = link.find("h3")
            if h3:
                title = h3.get_text(strip=True)
            else:
                title = link.get_text(strip=True)
            if not title or len(title) < 5:
                continue

            # 清理标题末尾日期
            title = re.sub(r'\d{1,2}月\s*\d{1,2},?\s*\d{4}\s*$', '', title).strip()
            title = re.sub(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},?\s*\d{4}\s*$', '', title).strip()
            # 中文日期格式  "2026年3月11日 星期三"
            title = re.sub(r'\d{4}年\d{1,2}月\d{1,2}日\s*(?:星期[一二三四五六日])?\s*$', '', title).strip()

            article = {"title": title, "url": full_url}

            # 提取摘要 (<p> 标签)
            p_tag = link.find("p")
            if p_tag:
                article["description"] = p_tag.get_text(strip=True)

            # 提取日期 (meta 区域)
            meta_span = link.select_one("div.meta span, span.date")
            if meta_span:
                raw_date = meta_span.get_text(strip=True)
                article["date_raw"] = raw_date

            # 从 URL 提取日期和国家代码
            match = re.search(r"/news/(\d{4}-\d{2}-\d{2})/(\w{2})/", full_url)
            if match:
                article["date"] = match.group(1)
                cc = match.group(2).lower()
                article["country_code"] = cc
                article["country"] = self.COUNTRY_CODES.get(cc, cc.upper())

            articles.append(self._normalize(article))
        return articles

    def scrape_detail(self, url) -> dict:
        """VisaHQ 详情页正文在 <div itemprop='articleBody'> 中"""
        time.sleep(self.delay)
        try:
            logger.info(f"[详情请求] GET {url}")
            resp = self.session.get(url, timeout=20)
            logger.info(f"[详情响应] {resp.status_code} | {len(resp.text)} 字符")
            if resp.status_code != 200:
                return {}
            soup = BeautifulSoup(resp.text, "lxml")
            body = soup.find("div", attrs={"itemprop": "articleBody"})
            if not body:
                body = soup.find("div", class_="news-article-list-body")
            if not body:
                logger.warning(f"[详情为空] 未找到正文区域 | {url}")
                return {}
            # 移除图片和脚本
            for tag in body.find_all(["script", "style", "img", "div"]):
                tag.decompose()
            # 正文用 <br> 分段
            text = body.get_text(separator="\n", strip=True)
            paragraphs = [p.strip() for p in text.split("\n") if p.strip() and len(p.strip()) > 10]
            logger.info(f"[详情解析] 提取 {len(paragraphs)} 段落 | {url}")
            from log_buffer import add_log
            add_log("scraper", "info", f"[详情解析] 提取 {len(paragraphs)} 段落 | {url}", "\n\n".join(paragraphs))
            return {"content": "\n".join(paragraphs), "paragraphs": paragraphs}
        except Exception as e:
            logger.error(f"[详情错误] {url} - {e}")
            return {}


# ===================================================================
# 大公文匯網 爬虫
# ===================================================================
TKWW_ARTICLE_RE = re.compile(r'/a/(\d{6})/(\d{2})/AP([0-9a-f]+)\.html')


class TKWWScraper(BaseScraper):
    source_name = "大公文匯網"

    EPAPER_SECTIONS = {
        "dgeconomic": "經濟", "dgProperty": "地產", "dginternational": "國際",
        "dgphysicaleducation": "體育", "dgCulture": "文化", "dgsupplement": "副刊",
    }

    def scrape(self) -> list:
        articles = []
        seen = set()

        # 主站首页
        html = self.fetch("https://www.tkww.hk/")
        if html:
            for a in self._parse_links(html, "https://www.tkww.hk"):
                if a["url"] not in seen:
                    seen.add(a["url"])
                    a["category"] = "主站"
                    articles.append(a)
        time.sleep(self.delay)

        # 电子报
        html = self.fetch("https://www.takungpao.com/")
        if html:
            for a in self._parse_links(html, "https://epaper.tkww.hk"):
                if a["url"] not in seen:
                    seen.add(a["url"])
                    a["category"] = "電子報"
                    articles.append(a)
        time.sleep(self.delay)

        # 电子报栏目
        for sec_id, sec_name in self.EPAPER_SECTIONS.items():
            html = self.fetch(f"https://epaper.tkww.hk/{sec_id}")
            if html:
                for a in self._parse_links(html, "https://epaper.tkww.hk"):
                    if a["url"] not in seen:
                        seen.add(a["url"])
                        a["category"] = sec_name
                        articles.append(a)
            time.sleep(self.delay)

        return articles

    def _parse_links(self, html, base_url) -> list:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        for link in soup.find_all("a", href=TKWW_ARTICLE_RE):
            href = link.get("href", "")
            full_url = urljoin(base_url, href).split("?")[0]
            title = link.get_text(strip=True)
            if not title or len(title) < 3 or len(title) > 200:
                continue

            article = {"title": title, "url": full_url}
            match = TKWW_ARTICLE_RE.search(full_url)
            if match:
                ym = match.group(1)
                day = match.group(2)
                article["date"] = f"{ym[:4]}-{ym[4:]}-{day}"
                article["article_id"] = match.group(3)

            articles.append(self._normalize(article))
        return articles


# ===================================================================
# 香港新聞網 爬虫
# ===================================================================
HKCNA_DOC_RE = re.compile(r'docDetail\.jsp\?id=(\d+)(?:&(?:amp;)?channel=(\d+))?')


class HKCNAScraper(BaseScraper):
    source_name = "香港新聞網"

    CHANNELS = {
        "gangao":  {"id": "2804", "list": "index_col", "sub": [4371, 4372, 2812, 2813]},
        "dawanqu": {"id": "5773", "list": "index_col_else", "sub": [5773]},
        "taiwan":  {"id": "2805", "list": "index_col_else", "sub": [2805]},
        "neidi":   {"id": "2808", "list": "index_col_else", "sub": [2808]},
        "guoji":   {"id": "2810", "list": "index_col_else", "sub": [2810]},
    }

    def scrape(self) -> list:
        articles = []
        seen = set()
        base = "https://www.hkcna.hk"

        # 最新新闻列表
        html = self.fetch(f"{base}/channel_txt.jsp")
        if html:
            for a in self._parse_links(html, base):
                if a.get("doc_id") and a["doc_id"] not in seen:
                    seen.add(a["doc_id"])
                    articles.append(a)
        time.sleep(self.delay)

        # 各频道
        for ch_key, ch_info in self.CHANNELS.items():
            list_type = ch_info["list"]
            url = f"{base}/{list_type}.jsp?channel={ch_info['id']}"
            html = self.fetch(url)
            if html:
                for a in self._parse_links(html, base):
                    if a.get("doc_id") and a["doc_id"] not in seen:
                        seen.add(a["doc_id"])
                        a["channel"] = ch_key
                        articles.append(a)
            time.sleep(self.delay)

            # 子频道
            for sub in ch_info.get("sub", []):
                if str(sub) == ch_info["id"]:
                    continue
                html = self.fetch(f"{base}/channel_txt.jsp?channel={sub}")
                if html:
                    for a in self._parse_links(html, base):
                        if a.get("doc_id") and a["doc_id"] not in seen:
                            seen.add(a["doc_id"])
                            a["channel"] = ch_key
                            articles.append(a)
                time.sleep(self.delay)

        return articles

    def _parse_links(self, html, base_url) -> list:
        soup = BeautifulSoup(html, "lxml")
        articles = []
        year = datetime.now().strftime("%Y")
        for link in soup.find_all("a", href=HKCNA_DOC_RE):
            href = link.get("href", "")
            full_url = urljoin(base_url, href).replace("&amp;", "&")
            title = link.get_text(strip=True)
            if not title or len(title) < 3:
                continue
            title = re.sub(r'^\d+\.\s*', '', title).strip()

            doc_id, channel_id = None, None
            match = HKCNA_DOC_RE.search(full_url)
            if match:
                doc_id = match.group(1)
                channel_id = match.group(2)

            article = {
                "title": title, "url": full_url,
                "doc_id": doc_id, "channel_id": channel_id,
            }

            # 从上下文提取日期（格式 MM-DD）
            parent = link.parent
            if parent:
                parent_text = parent.get_text(strip=True)
                date_match = re.search(r'(\d{2})-(\d{2})', parent_text)
                if date_match:
                    article["date"] = f"{year}-{date_match.group(1)}-{date_match.group(2)}"

            articles.append(self._normalize(article))
        return articles

    def scrape_detail(self, url) -> dict:
        """香港新聞網的文章详情"""
        time.sleep(self.delay)
        try:
            logger.info(f"[详情请求] GET {url}")
            resp = self.session.get(url, timeout=20)
            logger.info(f"[详情响应] {resp.status_code} | {len(resp.text)} 字符")
            if resp.status_code != 200:
                return {}
            soup = BeautifulSoup(resp.text, "lxml")
            detail = {}
            content_area = (
                soup.find("div", class_="xlCon") or
                soup.find("div", class_="newsCon") or
                soup.find("div", class_="news") or
                soup.find("article")
            )
            if content_area:
                for tag in content_area.find_all(["script", "style", "iframe"]):
                    tag.decompose()
                paragraphs = [p.get_text(strip=True) for p in content_area.find_all("p") if p.get_text(strip=True)]
                if not paragraphs:
                    # fallback: get all text split by newlines
                    text = content_area.get_text(separator="\n", strip=True)
                    paragraphs = [p.strip() for p in text.split("\n") if p.strip() and len(p.strip()) > 10]
                detail["content"] = "\n".join(paragraphs)
                detail["paragraphs"] = paragraphs
                logger.info(f"[详情解析] 提取 {len(paragraphs)} 段落 | {url}")
                from log_buffer import add_log
                add_log("scraper", "info", f"[详情解析] 提取 {len(paragraphs)} 段落 | {url}", "\n\n".join(paragraphs))
            return detail
        except Exception:
            return {}


# ===================================================================
# 统一调度
# ===================================================================
ALL_SCRAPERS = {
    "fragomen": FragomenScraper,
    "visahq": VisaHQScraper,
    "tkww": TKWWScraper,
    "hkcna": HKCNAScraper,
}


def run_all_scrapers(delay=1.5) -> dict:
    """运行所有爬虫，返回 {source: [articles]}"""
    results = {}
    for name, cls in ALL_SCRAPERS.items():
        try:
            scraper = cls(delay=delay)
            articles = scraper.scrape()
            results[name] = articles
        except Exception as e:
            results[name] = {"error": str(e)}
    return results


def scrape_article_content(url: str, source: str = None) -> str:
    """根据来源选择合适的爬虫获取文章正文"""
    scraper_map = {
        "Fragomen": FragomenScraper,
        "VisaHQ": VisaHQScraper,
        "大公文匯網": TKWWScraper,
        "香港新聞網": HKCNAScraper,
    }
    cls = scraper_map.get(source, BaseScraper)
    if cls == BaseScraper:
        # 通过 URL 猜测来源
        if "fragomen.com" in url:
            cls = FragomenScraper
        elif "visahq.com" in url:
            cls = VisaHQScraper
        elif "tkww.hk" in url or "takungpao.com" in url:
            cls = TKWWScraper
        elif "hkcna.hk" in url:
            cls = HKCNAScraper
        else:
            cls = FragomenScraper  # fallback

    scraper = cls(delay=0.5)
    detail = scraper.scrape_detail(url)
    return detail.get("content", "")
