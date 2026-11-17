"""SQLite 持久化层。

设计要点
--------
- 单连接 + 进程内 RLock：采集线程与 API 线程共享，所有写入串行化。
- 批次页原始 JSON 落 ``batch_pages``：失败/重启后续采直接复用已保存页，
  绝不重复请求源。
- 发布在单个事务内完成：整批采齐 → 替换 items / 重写 published_items →
  更新 published_snapshot；任一步失败则回滚，旧发布数据保持不变。
- 发布带"新旧快照保护"：较早启动的批次后完成时，若已有更晚启动批次
  发布的数据，则拒绝覆盖（批次标记为 superseded）。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url      TEXT NOT NULL,
    snapshot_token  TEXT NOT NULL,
    source_version  TEXT,
    total_pages     INTEGER,
    status          TEXT NOT NULL,              -- running|failed|invalid|completed|published|superseded
    error_code      TEXT,
    error_message   TEXT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT
);

CREATE TABLE IF NOT EXISTS batch_pages (
    batch_id    INTEGER NOT NULL,
    page_no     INTEGER NOT NULL,
    fetched_at  TEXT NOT NULL,
    payload     TEXT NOT NULL,                  -- 源返回的整页 JSON
    PRIMARY KEY (batch_id, page_no),
    FOREIGN KEY (batch_id) REFERENCES batches(id)
);

CREATE TABLE IF NOT EXISTS items (
    item_id     TEXT PRIMARY KEY,               -- 藏品编号
    name        TEXT NOT NULL,
    era         TEXT,
    material    TEXT,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS published_items (
    item_id     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    era         TEXT,
    material    TEXT
);

CREATE TABLE IF NOT EXISTS published_snapshot (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    batch_id        INTEGER NOT NULL,
    snapshot_token  TEXT NOT NULL,
    source_version  TEXT,
    published_at    TEXT NOT NULL,
    item_count      INTEGER NOT NULL
);
"""

# 批次终态集合
TERMINAL_STATUSES = {"failed", "invalid", "published", "superseded"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Database:
    def __init__(self, path: str | None = None) -> None:
        self.path = path or settings.db_path
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ---------------- 批次 ----------------

    def create_batch(self, source_url: str, snapshot_token: str,
                     source_version: str | None, total_pages: int | None) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO batches (source_url, snapshot_token, source_version, "
                "total_pages, status, started_at) VALUES (?, ?, ?, ?, 'running', ?)",
                (source_url, snapshot_token, source_version, total_pages, utc_now()),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def get_batch(self, batch_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches ORDER BY id DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_batch(self, batch_id: int, status: str,
                   error_code: str | None = None,
                   error_message: str | None = None,
                   finished: bool = False) -> None:
        with self._lock:
            if finished:
                self._conn.execute(
                    "UPDATE batches SET status = ?, error_code = ?, error_message = ?, "
                    "finished_at = ? WHERE id = ?",
                    (status, error_code, error_message, utc_now(), batch_id),
                )
            else:
                self._conn.execute(
                    "UPDATE batches SET status = ?, error_code = ?, error_message = ? "
                    "WHERE id = ?",
                    (status, error_code, error_message, batch_id),
                )
            self._conn.commit()

    def reset_to_running(self, batch_id: int) -> None:
        """续采：把失败批次重新置为 running 并清掉旧错误。"""
        with self._lock:
            self._conn.execute(
                "UPDATE batches SET status = 'running', error_code = NULL, "
                "error_message = NULL, finished_at = NULL WHERE id = ?",
                (batch_id,),
            )
            self._conn.commit()

    def update_batch_meta(self, batch_id: int, total_pages: int,
                          source_version: str | None) -> None:
        """回填首页探测得到的总页数与源版本。"""
        with self._lock:
            self._conn.execute(
                "UPDATE batches SET total_pages = ?, source_version = ? WHERE id = ?",
                (total_pages, source_version, batch_id),
            )
            self._conn.commit()

    def recover_on_startup(self) -> list[int]:
        """应用重启：上次未结束的 running 批次没有存活采集线程，转为 failed。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM batches WHERE status = 'running'"
            ).fetchall()
            ids = [int(r["id"]) for r in rows]
            for bid in ids:
                self._conn.execute(
                    "UPDATE batches SET status = 'failed', error_code = 'interrupted', "
                    "error_message = '应用重启，批次中断，可续采复用已保存页', "
                    "finished_at = ? WHERE id = ?",
                    (utc_now(), bid),
                )
            self._conn.commit()
            return ids

    # ---------------- 页 ----------------

    def save_page(self, batch_id: int, page_no: int, payload: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO batch_pages (batch_id, page_no, fetched_at, payload) "
                "VALUES (?, ?, ?, ?)",
                (batch_id, page_no, utc_now(), json.dumps(payload, ensure_ascii=False)),
            )
            self._conn.commit()

    def get_saved_pages(self, batch_id: int) -> dict[int, dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT page_no, payload FROM batch_pages WHERE batch_id = ?",
                (batch_id,),
            ).fetchall()
            return {int(r["page_no"]): json.loads(r["payload"]) for r in rows}

    # ---------------- 发布 ----------------

    def get_published(self) -> dict[str, Any] | None:
        with self._lock:
            snap = self._conn.execute(
                "SELECT * FROM published_snapshot WHERE id = 1"
            ).fetchone()
            if not snap:
                return None
            items = self._conn.execute(
                "SELECT item_id, name, era, material FROM published_items ORDER BY item_id"
            ).fetchall()
            return {
                "snapshot": dict(snap),
                "items": [dict(r) for r in items],
            }

    def publish_batch(self, batch: dict[str, Any], pages: list[dict[str, Any]]) -> str:
        """整批发布。返回 'published' 或 'superseded'。

        新旧快照保护：若已发布数据来自更晚启动（id 更大）的批次，
        本批次不得覆盖，标记 superseded 并原样保留当前发布。
        """
        with self._lock:
            current = self._conn.execute(
                "SELECT batch_id FROM published_snapshot WHERE id = 1"
            ).fetchone()
            if current is not None and int(current["batch_id"]) > int(batch["id"]):
                self._conn.execute(
                    "UPDATE batches SET status = 'superseded', finished_at = ? WHERE id = ?",
                    (utc_now(), batch["id"]),
                )
                self._conn.commit()
                return "superseded"

            # 汇总整批藏品（以编号去重，后页覆盖前页）
            merged: dict[str, dict[str, Any]] = {}
            for page in pages:
                for item in page.get("items", []):
                    item_id = str(item["id"])
                    merged[item_id] = {
                        "item_id": item_id,
                        "name": str(item.get("name", "")),
                        "era": item.get("era"),
                        "material": item.get("material"),
                    }
            ordered = [merged[k] for k in sorted(merged)]
            now = utc_now()

            try:
                # 1) 全量重写 items（源中已不存在的藏品自然被移除）
                self._conn.execute("DELETE FROM items")
                self._conn.executemany(
                    "INSERT INTO items (item_id, name, era, material, updated_at) "
                    "VALUES (:item_id, :name, :era, :material, :updated_at)",
                    [{**it, "updated_at": now} for it in ordered],
                )
                # 2) 重写已发布视图
                self._conn.execute("DELETE FROM published_items")
                self._conn.executemany(
                    "INSERT INTO published_items (item_id, name, era, material) "
                    "VALUES (:item_id, :name, :era, :material)",
                    ordered,
                )
                # 3) 记录发布快照
                self._conn.execute(
                    "INSERT INTO published_snapshot (id, batch_id, snapshot_token, "
                    "source_version, published_at, item_count) "
                    "VALUES (1, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET batch_id = excluded.batch_id, "
                    "snapshot_token = excluded.snapshot_token, "
                    "source_version = excluded.source_version, "
                    "published_at = excluded.published_at, "
                    "item_count = excluded.item_count",
                    (batch["id"], batch["snapshot_token"], batch.get("source_version"),
                     now, len(ordered)),
                )
                # 4) 批次落终态
                self._conn.execute(
                    "UPDATE batches SET status = 'published', finished_at = ? WHERE id = ?",
                    (now, batch["id"]),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            return "published"

    def close(self) -> None:
        with self._lock:
            self._conn.close()
