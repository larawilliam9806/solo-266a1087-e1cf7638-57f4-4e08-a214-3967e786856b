"""SQLite 持久化层。

设计要点
--------
- 单连接 + 进程内 RLock：采集线程与 API 线程共享，所有写入串行化。
- 批次页原始 JSON 落 ``batch_pages``：失败/重启后续采直接复用已保存页，
  绝不重复请求源。
- 发布在单个事务内完成：整批采齐 → 替换 items / 重写 published_items →
  更新 published_snapshot；任一步失败则回滚，旧发布数据保持不变。
- 发布带"新旧快照保护"：启动顺序由建批请求到达服务端时分配的 ``start_seq``
  决定（不是首页返回先后，也不是批次 id）。较早启动的批次后完成时，若已有
  更晚启动批次发布的数据，则拒绝覆盖（批次标记为 superseded）。
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
    -- 启动序：建批请求开始被服务端处理时分配，与首页返回先后、批次 id 无关。
    -- 旧库没有该列，打开时自动迁移并以 id 回填，保持既有语义。
    start_seq       INTEGER,
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

-- 启动序分配器（单行）。用数据库事务保证跨连接/跨进程唯一递增；
-- 多 worker 部署时不会有两个批次拿到同一启动序。
CREATE TABLE IF NOT EXISTS batch_seq (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    value   INTEGER NOT NULL
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
        # autocommit 模式：事务边界由本类显式控制（BEGIN IMMEDIATE / COMMIT），
        # 使“启动序分配”和“检查已发布 + 写入发布”的读改写跨连接原子化。
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        # 多 worker/多连接竞争启动序分配时稍等写锁，而不是立刻报错
        self._conn.execute("PRAGMA busy_timeout=5000")
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._migrate_schema()
            # 启动序分配器：旧库升级时以已有批次的最大 start_seq 为下限
            self._conn.execute(
                "INSERT OR IGNORE INTO batch_seq (id, value) VALUES (1, 0)"
            )
            self._conn.execute(
                "UPDATE batch_seq SET value = MAX(value, "
                "(SELECT COALESCE(MAX(start_seq), 0) FROM batches)) WHERE id = 1"
            )
            self._conn.commit()

    def _migrate_schema(self) -> None:
        """旧库兼容：补 ``start_seq`` 列并按 id 回填。

        - 新库由 SCHEMA 直接带列；
        - 旧库 ``CREATE TABLE IF NOT EXISTS`` 不会改已有表，这里显式 ALTER；
        - 迁移前已存在的批次以其 id 作为启动序，保持旧版“id 越大启动越晚”
          的发布保护语义，重启/升级后不会被误判。
        """
        cols = {row["name"] for row in self._conn.execute(
            "PRAGMA table_info(batches)"
        ).fetchall()}
        if "start_seq" not in cols:
            try:
                self._conn.execute("ALTER TABLE batches ADD COLUMN start_seq INTEGER")
            except sqlite3.OperationalError as exc:
                # 另一个进程/连接刚完成同一迁移：忽略重复列错误，继续回填
                if "duplicate column" not in str(exc).lower():
                    raise
        self._conn.execute(
            "UPDATE batches SET start_seq = id WHERE start_seq IS NULL"
        )

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def _begin_immediate(self) -> None:
        """开启写事务并立即取写锁：并发发布时先到者先检查、先写入。"""
        self._conn.execute("BEGIN IMMEDIATE")

    # ---------------- 批次 ----------------

    def allocate_start_seq(self) -> int:
        """在服务端开始处理建批请求时分配启动序（越早到达序号越小）。

        必须由调用方在探测源首页之前调用：首页响应再慢也不能改变启动先后。
        自增与读取在同一写事务内完成，跨连接/跨进程唯一；只分配不落库，
        若建批失败留下空号无副作用。
        """
        with self._lock:
            try:
                self._begin_immediate()
                self._conn.execute("UPDATE batch_seq SET value = value + 1 WHERE id = 1")
                row = self._conn.execute(
                    "SELECT value FROM batch_seq WHERE id = 1"
                ).fetchone()
                self._conn.execute("COMMIT")
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise
            return int(row["value"])

    def _bump_start_seq_locked(self, start_seq: int) -> None:
        """显式指定启动序时同步分配器，避免后续分配重号。"""
        self._conn.execute(
            "UPDATE batch_seq SET value = MAX(value, ?) WHERE id = 1",
            (int(start_seq),),
        )

    def create_batch(self, source_url: str, snapshot_token: str,
                     source_version: str | None, total_pages: int | None,
                     start_seq: int | None = None) -> int:
        with self._lock:
            try:
                # 分配（若未显式给定）与插入同一写事务，跨连接不乱序/不重号
                self._begin_immediate()
                if start_seq is None:
                    self._conn.execute(
                        "UPDATE batch_seq SET value = value + 1 WHERE id = 1")
                    row = self._conn.execute(
                        "SELECT value FROM batch_seq WHERE id = 1"
                    ).fetchone()
                    start_seq = int(row["value"])
                else:
                    self._bump_start_seq_locked(start_seq)
                cur = self._conn.execute(
                    "INSERT INTO batches (source_url, snapshot_token, source_version, "
                    "total_pages, status, started_at, start_seq) "
                    "VALUES (?, ?, ?, ?, 'running', ?, ?)",
                    (source_url, snapshot_token, source_version, total_pages,
                     utc_now(), start_seq),
                )
                self._conn.execute("COMMIT")
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise
            return int(cur.lastrowid)

    def get_batch(self, batch_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            # 按启动序展示（而不是落库 id）：慢首页的较早批次 id 可能更大，
            # 用 id 排序会把它误显示为“更新”。
            rows = self._conn.execute(
                "SELECT * FROM batches ORDER BY start_seq DESC, id DESC"
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
            try:
                self._begin_immediate()
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
                self._conn.execute("COMMIT")
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise
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
                "SELECT ps.*, "
                "COALESCE((SELECT b.start_seq FROM batches b "
                "          WHERE b.id = ps.batch_id), ps.batch_id) AS start_seq "
                "FROM published_snapshot ps WHERE ps.id = 1"
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

        启动序保护：若当前已发布数据来自更晚启动（``start_seq`` 更大）的批次，
        本批次不得覆盖，标记 superseded 并原样保留当前发布。

        比较依据是建批请求到达服务端时分配的 ``start_seq``，而不是首页返回
        先后或自增 id；较早批次的首页再慢、后采齐、续采或重启恢复后完成，
        都不会覆盖已完整发布的较晚批次。较晚批次未发布（失败/失效/尚未采齐）
        时当前发布仍是更早批次，本批次可正常发布。
        """
        with self._lock:
            try:
                # 检查与写入必须在同一写事务内：并发发布的另一连接不能在这两步
                # 之间插入自己的发布，否则会出现“都通过检查、后写覆盖先写”。
                self._begin_immediate()
                current = self._conn.execute(
                    "SELECT ps.batch_id AS batch_id, "
                    "COALESCE((SELECT b.start_seq FROM batches b "
                    "          WHERE b.id = ps.batch_id), ps.batch_id) AS start_seq "
                    "FROM published_snapshot ps WHERE ps.id = 1"
                ).fetchone()
                batch_seq = batch.get("start_seq")
                if batch_seq is None:  # 防御：旧数据缺列时退回 id 语义
                    batch_seq = batch["id"]
                if current is not None and int(current["start_seq"]) > int(batch_seq):
                    self._conn.execute(
                        "UPDATE batches SET status = 'superseded', finished_at = ? "
                        "WHERE id = ?",
                        (utc_now(), batch["id"]),
                    )
                    self._conn.execute("COMMIT")
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
                self._conn.execute("COMMIT")
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise
            return "published"

    def close(self) -> None:
        with self._lock:
            self._conn.close()
