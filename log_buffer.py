"""
实时日志缓冲模块
===============
提供内存日志缓冲和自定义 logging.Handler，
支持爬虫、LLM 等模块的日志实时推送到前端。
"""

import logging
import time
from collections import deque
from threading import Lock


# 全局日志缓冲区（最多保留 500 条）
_log_buffer = deque(maxlen=500)
_log_lock = Lock()
_log_counter = 0


def add_log(category: str, level: str, message: str, detail: str = None):
    """添加一条日志到缓冲区"""
    global _log_counter
    with _log_lock:
        _log_counter += 1
        entry = {
            "id": _log_counter,
            "ts": time.time(),
            "time": time.strftime("%H:%M:%S"),
            "category": category,    # scraper / llm / system
            "level": level,          # info / warn / error / debug
            "message": message,
            "detail": detail,        # 长文本（可选）
        }
        _log_buffer.append(entry)
    return entry


def get_logs(since_id: int = 0) -> list:
    """获取 since_id 之后的所有日志"""
    with _log_lock:
        return [e for e in _log_buffer if e["id"] > since_id]


def get_all_logs() -> list:
    """获取所有日志"""
    with _log_lock:
        return list(_log_buffer)


class BufferHandler(logging.Handler):
    """将 Python logging 输出同时写入内存缓冲区的 Handler"""

    def __init__(self, category: str = "system"):
        super().__init__()
        self.category = category

    def emit(self, record):
        try:
            msg = self.format(record)
            level = record.levelname.lower()
            if level == "warning":
                level = "warn"
            add_log(self.category, level, msg)
        except Exception:
            self.handleError(record)
