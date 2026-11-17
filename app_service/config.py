"""采集端配置（可通过环境变量覆盖）。"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    # 默认放在工作目录下；Docker 中由 MUSEUM_DB_PATH=/data/museum.db 覆盖
    db_path: str = _env("MUSEUM_DB_PATH", "data/museum.db")
    # 网页表单中目录接口的预填地址
    default_source_url: str = _env("MUSEUM_DEFAULT_SOURCE_URL", "http://127.0.0.1:8001/catalog")
    # 源管理控制台地址（仅用于网页跳转提示）
    source_admin_url: str = _env("MUSEUM_SOURCE_ADMIN_URL", "http://127.0.0.1:8001/")
    request_timeout: float = float(_env("MUSEUM_REQUEST_TIMEOUT", "15"))
    page_pause: float = float(_env("MUSEUM_PAGE_PAUSE", "0.15"))


settings = Settings()
