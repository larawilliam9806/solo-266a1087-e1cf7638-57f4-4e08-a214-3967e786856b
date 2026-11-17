"""批次采集器。

并发模型
--------
每个批次运行在独立守护线程中，多个批次可并行；线程之间通过 Database 的
锁串行化写入，网络请求在线程内阻塞进行（httpx 同步客户端）。

续采规则
--------
- 已落库的页直接复用，不再请求源（失败后/重启后均如此）。
- 令牌失效（HTTP 410）→ 批次置 invalid 终态，不可续采，必须新建批次，
  从源获取新快照令牌，新旧快照绝不混用。
- 其他请求错误 → 批次置 failed，可续采。
- 只有整批页全部到位才触发发布；发布失败或批次失败都不触碰旧发布数据。
"""

from __future__ import annotations

import threading
import time
from typing import Any

import httpx

from .config import Settings, settings as default_settings
from .db import Database, TERMINAL_STATUSES


class Collector:
    def __init__(self, db: Database, config: Settings | None = None) -> None:
        self.db = db
        self.config = config or default_settings
        # batch_id -> 存活中的采集线程
        self._workers: dict[int, threading.Thread] = {}
        self._reg_lock = threading.Lock()

    def is_active(self, batch_id: int) -> bool:
        with self._reg_lock:
            t = self._workers.get(batch_id)
            return t is not None and t.is_alive()

    def active_ids(self) -> list[int]:
        with self._reg_lock:
            return [bid for bid, t in self._workers.items() if t.is_alive()]

    def start_worker(self, batch_id: int) -> None:
        with self._reg_lock:
            existing = self._workers.get(batch_id)
            if existing is not None and existing.is_alive():
                return
            t = threading.Thread(
                target=self._run_safely, args=(batch_id,),
                name=f"collector-batch-{batch_id}", daemon=True,
            )
            self._workers[batch_id] = t
            t.start()

    def _run_safely(self, batch_id: int) -> None:
        try:
            self.run_batch(batch_id)
        except Exception as exc:  # 兜底：线程异常绝不能静默吞掉
            self.db.mark_batch(
                batch_id, "failed",
                error_code="worker_crash",
                error_message=f"采集线程异常: {exc!r}",
                finished=True,
            )
        finally:
            with self._reg_lock:
                self._workers.pop(batch_id, None)

    # ------------------------------------------------------------------

    def run_batch(self, batch_id: int) -> None:
        batch = self.db.get_batch(batch_id)
        if batch is None:
            return
        if batch["status"] in TERMINAL_STATUSES:
            return

        saved = self.db.get_saved_pages(batch_id)
        token = batch["snapshot_token"]
        total_pages = batch["total_pages"]
        # 建批时已探测首页并回填 total_pages；异常数据按失败处理，可续采
        if not isinstance(total_pages, int) or total_pages < 1:
            self.db.mark_batch(
                batch_id, "failed",
                error_code="bad_total_pages",
                error_message="批次缺少合法 total_pages",
                finished=True,
            )
            return

        with httpx.Client(timeout=self.config.request_timeout) as client:
            for page_no in range(1, total_pages + 1):
                if page_no in saved:
                    # 续采核心：已保存的页直接复用，不再访问源
                    continue
                outcome = self._fetch_and_store(client, batch, page_no, token)
                if outcome != "ok":
                    return
                saved = self.db.get_saved_pages(batch_id)
                if page_no < total_pages and self.config.page_pause > 0:
                    time.sleep(self.config.page_pause)

        # 整批采齐 → 按页序汇总并发布
        saved = self.db.get_saved_pages(batch_id)
        if set(saved) != set(range(1, total_pages + 1)):
            self.db.mark_batch(
                batch_id, "failed",
                error_code="incomplete",
                error_message="采集结束但页面不完整，可续采补齐",
                finished=True,
            )
            return

        pages = [saved[n] for n in sorted(saved)]
        result = self.db.publish_batch(self.db.get_batch(batch_id), pages)
        if result == "superseded":
            # 较早启动的批次后完成：数据保留不发布，网页上可看到 superseded
            return

    def _fetch_and_store(
        self,
        client: httpx.Client,
        batch: dict[str, Any],
        page_no: int,
        token: str,
    ) -> str:
        """请求单页并落库。返回 ok / failed / invalid。"""
        batch_id = int(batch["id"])
        params: dict[str, int | str] = {"page": page_no}
        # 批次自始至终只使用建批时保存的快照令牌，不接受外部替换
        params["token"] = token
        try:
            # 合并而非替换源地址原有查询参数，保留目录接口上的其它参数
            url = httpx.URL(batch["source_url"]).copy_merge_params(params)
            resp = client.get(url)
        except httpx.HTTPError as exc:
            self.db.mark_batch(
                batch_id, "failed",
                error_code="request_error",
                error_message=f"第 {page_no} 页请求失败: {exc!r}",
                finished=True,
            )
            return "failed"

        if resp.status_code == 410:
            # 快照令牌失效：终态，不可续采；必须新建批次获取新快照
            self.db.mark_batch(
                batch_id, "invalid",
                error_code="snapshot_expired",
                error_message=f"第 {page_no} 页返回 410：快照令牌已失效，请新建批次",
                finished=True,
            )
            return "invalid"

        if resp.status_code != 200:
            self.db.mark_batch(
                batch_id, "failed",
                error_code=f"http_{resp.status_code}",
                error_message=f"第 {page_no} 页返回 HTTP {resp.status_code}: "
                              f"{resp.text[:200]}",
                finished=True,
            )
            return "failed"

        try:
            payload = resp.json()
        except ValueError as exc:
            self.db.mark_batch(
                batch_id, "failed",
                error_code="bad_json",
                error_message=f"第 {page_no} 页响应不是合法 JSON: {exc!r}",
                finished=True,
            )
            return "failed"

        # 防护：源必须回传同一快照令牌，否则视为快照不一致
        if payload.get("snapshot_token") != token:
            self.db.mark_batch(
                batch_id, "invalid",
                error_code="snapshot_mismatch",
                error_message="源返回的快照令牌与批次绑定令牌不一致，拒绝混用快照",
                finished=True,
            )
            return "invalid"

        if page_no == 1 and batch["total_pages"] is None:
            total = payload.get("total_pages")
            if not isinstance(total, int) or total < 1:
                self.db.mark_batch(
                    batch_id, "failed",
                    error_code="bad_total_pages",
                    error_message="首页缺少合法 total_pages",
                    finished=True,
                )
                return "failed"
            # 回填批次元数据（版本、总页数）
            self.db.update_batch_meta(batch_id, total, payload.get("version"))

        self.db.save_page(batch_id, page_no, payload)
        return "ok"
