# 国际移民管理咨询工作流

自动化新闻采集、LLM 智能筛选与报告生成系统，面向出入境管理部门的移民资讯情报工作流。

## 功能特性

- **多源新闻爬取** — Fragomen、VisaHQ、大公文匯網、香港新聞網，定时自动采集
- **增量爬取** — 连续 5 篇重复文章自动停止，避免冗余抓取
- **英文标题翻译** — 自动批量翻译英文标题为中文，双语存储
- **LLM 智能筛选** — 一键发送标题列表，AI 筛选出与移民政策相关的文章
- **分批报告生成** — 文章过多时自动分批调用 LLM，智能合并子报告
- **Word 导出** — 生成符合公文规范的 Word 文档（仿宋字体、固定行距 28 磅）
- **历史记录** — 自动保存每次报告，可回溯查看筛选结果和完整报告
- **实时日志** — 终端风格日志页面，查看爬虫请求和 LLM 请求/响应原文

## 技术栈

| 组件 | 技术 |
|------|------|
| 后端 | Python / Flask |
| 数据库 | MongoDB |
| 前端 | 单页应用（原生 HTML/CSS/JS） |
| LLM | 支持 OpenAI / Claude / DeepSeek 等兼容 API |
| 部署 | Gunicorn + Systemd |

## 快速开始

### 1. 安装依赖

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置

编辑 `config.json`，设置 LLM API 和 MongoDB 连接：

```json
{
  "llm": {
    "active_provider": "openai",
    "providers": {
      "openai": {
        "api_url": "https://api.openai.com/v1/chat/completions",
        "api_key": "sk-xxx",
        "model": "gpt-4o"
      }
    }
  },
  "mongodb": {
    "uri": "mongodb://localhost:27017",
    "database": "news_aggregator"
  }
}
```

### 3. 启动

```bash
# 开发模式
python app.py

# 生产模式
pip install gunicorn
bash start.sh
```

访问 `http://localhost:5100/new`

### 4. 生产部署（Systemd）

```bash
cp reportautobuild.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable reportautobuild
systemctl start reportautobuild
```

更新：`git pull && systemctl restart reportautobuild`

## 项目结构

```
├── app.py                  # Flask 主应用 + API 路由
├── scrapers.py             # 统一爬虫模块（4 个数据源）
├── llm_client.py           # LLM 统一接口（筛选/报告/翻译）
├── models.py               # MongoDB 数据模型
├── report_generator.py     # Word 文档生成器
├── log_buffer.py           # 实时日志内存缓冲
├── config.json             # 全局配置
├── templates/
│   └── index_new.html      # 前端单页应用
├── static/
│   └── logo.png            # 移民局 LOGO
├── start.sh                # 生产启动脚本
└── reportautobuild.service # Systemd 服务配置
```

## 工作流程

```
爬取新闻 → 入库去重 → 翻译英文标题 → 选择文章 → LLM 筛选
→ 勾选确认 → LLM 生成报告（自动分批） → 预览 → 导出 Word
```

## 配置说明

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `scraper.interval_hours` | 自动爬取间隔（小时） | 24 |
| `scraper.delay_between_requests` | 请求间隔（秒） | 1.5 |
| `report_generation.batch_size` | 每批发送给 LLM 的文章数 | 15 |
| `prompts.filter` | 筛选提示词 | 内置默认 |
| `prompts.report` | 报告生成提示词 | 内置默认 |
