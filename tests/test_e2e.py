"""端到端自测：真实 uvicorn 服务器 + HTTP 调用，覆盖关键业务流程。

运行：.venv/bin/pytest -q
每个用例使用独立临时目录的 SQLite 数据库，并各自启动源服务与采集端。
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

from app_service.config import Settings
from app_service.main import create_app
from source_service.app import app as source_app


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class Server:
    """uvicorn 运行在独立守护线程的事件循环中，跨测试用例稳定存活。"""

    def __init__(self, app, prefix: str):
        self.app = app
        self.prefix = prefix
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()

    def start(self) -> None:
        def _serve() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            config = uvicorn.Config(
                self.app, host="127.0.0.1", port=self.port,
                log_level="warning", lifespan="on",
            )
            self._server = uvicorn.Server(config)

            async def _watch_ready() -> None:
                while not self._server.started:
                    await asyncio.sleep(0.02)
                self._ready.set()

            loop.create_task(_watch_ready())
            try:
                loop.run_until_complete(self._server.serve())
            finally:
                loop.close()

        self._thread = threading.Thread(target=_serve, name=f"uvicorn-{self.prefix}", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=15):
            raise RuntimeError(f"{self.prefix} 服务器启动超时")

    def stop(self) -> None:
        assert self._server is not None and self._thread is not None
        self._server.should_exit = True
        self._thread.join(timeout=15)
        assert not self._thread.is_alive(), f"{self.prefix} 服务器停止超时"


class Harness:
    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.db_path = str(tmp_path / "museum.db")
        self.source = Server(source_app, "source")

    def __enter__(self) -> "Harness":
        self.source.start()
        self.client = httpx.Client(base_url=self.source.base_url, timeout=10)
        return self

    def build_app(self) -> Server:
        cfg = Settings(
            db_path=self.db_path,
            default_source_url=f"{self.source.base_url}/catalog",
            source_admin_url=f"{self.source.base_url}/",
            request_timeout=10,
            page_pause=0,
        )
        return Server(create_app(settings=cfg), "app")

    def start_app(self) -> Server:
        srv = self.build_app()
        srv.start()
        return srv

    def open_db(self):
        """以独立连接打开同一数据库文件（用于模拟崩溃现场）。"""
        from app_service.db import Database

        return Database(self.db_path)

    def __exit__(self, *exc) -> None:
        self.client.close()
        self.source.stop()


@pytest.fixture()
def harness(tmp_path):
    h = Harness(tmp_path)
    h.__enter__()
    yield h
    h.__exit__()


def _wait_status(client: httpx.Client, batch_id: int,
                 expected: set[str], timeout: float = 15) -> dict:
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        r = client.get(f"/api/batches/{batch_id}")
        assert r.status_code == 200
        last = r.json()
        if last["status"] in expected:
            return last
        time.sleep(0.1)
    raise AssertionError(f"批次 {batch_id} 未在超时内到达 {expected}，最后状态: {last}")


CATALOG = None  # 运行时由各用例以 source.base_url 拼接


def test_full_happy_path_and_download(harness):
    """整批采齐自动发布，下载到的 JSON 与源 v1 完整一致。"""
    app_srv = harness.start_app()
    try:
        with httpx.Client(base_url=app_srv.base_url, timeout=10) as c:
            r = c.post("/api/batches",
                       json={"source_url": f"{harness.source.base_url}/catalog"})
            assert r.status_code == 201, r.text
            batch = r.json()
            assert batch["status"] == "running"
            assert batch["total_pages"] == 3
            assert batch["snapshot_token"].startswith("snap-v1-")

            done = _wait_status(c, batch["id"], {"published"})
            assert done["saved_pages"] == [1, 2, 3]

            pub = c.get("/api/published").json()
            ids = [it["id"] for it in pub["items"]]
            assert ids == ["A001", "A002", "A003", "A004", "A005", "A006"]
            assert pub["snapshot"]["item_count"] == 6

            dl = c.get("/api/published/download")
            assert dl.status_code == 200
            assert "attachment" in dl.headers["content-disposition"]
            data = dl.json()
            assert data["item_count"] == 6 and len(data["items"]) == 6
            assert data["snapshot_token"] == batch["snapshot_token"]
            assert {"id", "name", "era", "material"} <= set(data["items"][0])
    finally:
        app_srv.stop()


def test_failure_then_resume_reuses_saved_pages(harness):
    """单页失败一次：批次 failed；续采后成功，且已保存页不重复请求。"""
    r = harness.client.post("/admin/fail-page-once",
                            params={"version": "v1", "page": 2})
    assert r.status_code == 200

    app_srv = harness.start_app()
    try:
        with httpx.Client(base_url=app_srv.base_url, timeout=10) as c:
            batch = c.post("/api/batches",
                           json={"source_url": f"{harness.source.base_url}/catalog"}).json()
            bid = batch["id"]

            failed = _wait_status(c, bid, {"failed"})
            assert failed["error_code"] == "http_500"
            assert failed["saved_pages"] == [1]  # 第 1 页已落库

            r = c.post(f"/api/batches/{bid}/resume")
            assert r.status_code == 200
            done = _wait_status(c, bid, {"published"})
            assert done["saved_pages"] == [1, 2, 3]

            # 已发布批次不允许再续采
            r = c.post(f"/api/batches/{bid}/resume")
            assert r.status_code == 409

            pub = c.get("/api/published").json()
            assert pub["snapshot"]["batch_id"] == bid
    finally:
        app_srv.stop()


def test_token_invalidation_requires_new_batch(harness):
    """令牌失效：续采触发 410 后批次转 invalid 且再次续采被拒；新建批次拿新快照成功。"""
    # 建批探测会先成功取第 1 页，对第 3 页装填故障使批次先落为 failed
    assert harness.client.post("/admin/fail-page-once",
                               params={"version": "v1", "page": 3}).status_code == 200
    app_srv = harness.start_app()
    try:
        with httpx.Client(base_url=app_srv.base_url, timeout=10) as c:
            b1 = c.post("/api/batches",
                        json={"source_url": f"{harness.source.base_url}/catalog"}).json()
            _wait_status(c, b1["id"], {"failed"})
            old_token = b1["snapshot_token"]

            # 令当前所有令牌失效
            assert harness.client.post("/admin/invalidate-token").status_code == 200

            # 旧令牌直接访问源 → 410
            r = harness.client.get("/catalog",
                                   params={"token": old_token, "page": 2})
            assert r.status_code == 410

            # 第一次续采：worker 请求源拿到 410，批次落 invalid 终态
            r = c.post(f"/api/batches/{b1['id']}/resume")
            assert r.status_code == 200
            invalid = _wait_status(c, b1["id"], {"invalid"})
            assert invalid["error_code"] == "snapshot_expired"
            # 第 1、2 页此前已保存，续采只请求了缺失的第 3 页便收到 410
            assert invalid["saved_pages"] == [1, 2]

            # 再次续采被明确拒绝：必须新建批次
            r = c.post(f"/api/batches/{b1['id']}/resume")
            assert r.status_code == 409
            assert "新建批次" in r.json()["detail"]

            # 新建批次：源首页签发新令牌，整批成功，新旧快照未混用
            b2 = c.post("/api/batches",
                        json={"source_url": f"{harness.source.base_url}/catalog"}).json()
            assert b2["snapshot_token"] != old_token
            _wait_status(c, b2["id"], {"published"})

            pub = c.get("/api/published").json()
            assert pub["snapshot"]["batch_id"] == b2["id"]
            assert pub["snapshot"]["snapshot_token"] == b2["snapshot_token"]
    finally:
        app_srv.stop()


def test_restart_resume_with_saved_pages(harness):
    """应用重启：停机时仍为 running 的批次转 failed；新进程续采复用已保存页并发布。"""
    assert harness.client.post("/admin/fail-page-once",
                               params={"version": "v1", "page": 2}).status_code == 200
    srv1 = harness.start_app()
    try:
        with httpx.Client(base_url=srv1.base_url, timeout=10) as c:
            b1 = c.post("/api/batches",
                        json={"source_url": f"{harness.source.base_url}/catalog"}).json()
            _wait_status(c, b1["id"], {"failed"})
            # 模拟"采集中进程被杀死"：直接在库里把批次改回 running，
            # 此时没有任何 worker 线程，与真实崩溃后的磁盘状态一致
            db = harness.open_db()
            try:
                db.mark_batch(b1["id"], "running")
            finally:
                db.close()
            assert c.get(f"/api/batches/{b1['id']}").json()["status"] == "running"
    finally:
        srv1.stop()  # 模拟应用重启

    srv2 = harness.start_app()  # 同一 db_path
    try:
        with httpx.Client(base_url=srv2.base_url, timeout=10) as c:
            health = c.get("/health").json()
            assert b1["id"] in health["recovered_batches"]
            after = c.get(f"/api/batches/{b1['id']}").json()
            assert after["status"] == "failed" and after["saved_pages"] == [1]

            r = c.post(f"/api/batches/{b1['id']}/resume")
            assert r.status_code == 200
            done = _wait_status(c, b1["id"], {"published"})
            assert done["saved_pages"] == [1, 2, 3]
    finally:
        srv2.stop()


def test_parallel_batches_and_staleness_guard(harness):
    """两个批次并行；较早启动的批次后完成不得覆盖较晚批次的发布。"""
    # 批次1 将卡在第 2 页（一次性故障，先不续采）
    assert harness.client.post("/admin/fail-page-once",
                               params={"version": "v1", "page": 2}).status_code == 200
    srv = harness.start_app()
    try:
        with httpx.Client(base_url=srv.base_url, timeout=10) as c:
            url = f"{harness.source.base_url}/catalog"
            b1 = c.post("/api/batches", json={"source_url": url}).json()
            _wait_status(c, b1["id"], {"failed"})

            # 批次2 在批次1 未完成期间启动并完整发布
            b2 = c.post("/api/batches", json={"source_url": url}).json()
            _wait_status(c, b2["id"], {"published"})

            # 再续采批次1（较早启动）→ 采齐但发布被保护拦截
            r = c.post(f"/api/batches/{b1['id']}/resume")
            assert r.status_code == 200
            done = _wait_status(c, b1["id"], {"superseded"})
            assert done["saved_pages"] == [1, 2, 3]  # 数据采齐了，只是不允许覆盖

            pub = c.get("/api/published").json()
            assert pub["snapshot"]["batch_id"] == b2["id"]  # 仍是较晚批次的数据
    finally:
        srv.stop()


def test_publish_removes_items_vanished_from_source(harness):
    """新版本删除的藏品在发布后从数据集中移除。"""
    srv = harness.start_app()
    try:
        with httpx.Client(base_url=srv.base_url, timeout=10) as c:
            url = f"{harness.source.base_url}/catalog"
            b1 = c.post("/api/batches", json={"source_url": url}).json()
            _wait_status(c, b1["id"], {"published"})
            ids_v1 = {it["id"] for it in c.get("/api/published").json()["items"]}
            assert "A004" in ids_v1

            # 源切换到 v2（A004 消失，新增 B001），旧令牌失效
            sw = harness.client.post("/admin/switch-version", params={"version": "v2"})
            assert sw.status_code == 200

            b2 = c.post("/api/batches", json={"source_url": url}).json()
            assert b2["source_version"] == "v2"
            _wait_status(c, b2["id"], {"published"})

            pub = c.get("/api/published").json()
            ids_v2 = {it["id"] for it in pub["items"]}
            assert "A004" not in ids_v2          # 源中已不存在 → 移除
            assert "B001" in ids_v2              # 新增藏品
            assert "A001" in ids_v2              # 保留藏品
            a001 = next(it for it in pub["items"] if it["id"] == "A001")
            assert a001["name"] == "青铜兽面纹鼎"  # 名称修订同步
            assert pub["snapshot"]["batch_id"] == b2["id"]

            # 继续切 v3，验证再次全量替换
            assert harness.client.post(
                "/admin/switch-version", params={"version": "v3"}).status_code == 200
            b3 = c.post("/api/batches", json={"source_url": url}).json()
            _wait_status(c, b3["id"], {"published"})
            ids_v3 = {it["id"] for it in c.get("/api/published").json()["items"]}
            assert ids_v3 == {"A002", "B001", "C001"}

            dl = c.get("/api/published/download").json()
            assert {it["id"] for it in dl["items"]} == ids_v3
    finally:
        srv.stop()


def test_concurrent_batches_run_in_parallel(harness):
    """多个批次同时启动：两个 worker 并行，各自独立发布/竞争。"""
    srv = harness.start_app()
    try:
        with httpx.Client(base_url=srv.base_url, timeout=10) as c:
            url = f"{harness.source.base_url}/catalog"
            r1 = c.post("/api/batches", json={"source_url": url})
            r2 = c.post("/api/batches", json={"source_url": url})
            assert r1.status_code == r2.status_code == 201
            id1, id2 = r1.json()["id"], r2.json()["id"]
            assert id1 != id2

            for bid in (id1, id2):
                _wait_status(c, bid, {"published", "superseded"})
            listing = c.get("/api/batches").json()["batches"]
            statuses = {b["id"]: b["status"] for b in listing}
            assert "failed" not in statuses.values()
            assert "invalid" not in statuses.values()
            assert sorted(statuses.values()) in (["published", "published"],
                                                  ["published", "superseded"])
    finally:
        srv.stop()


def test_no_published_data_returns_404_and_bad_url_rejected(harness):
    srv = harness.start_app()
    try:
        with httpx.Client(base_url=srv.base_url, timeout=10) as c:
            assert c.get("/api/published").status_code == 404
            assert c.get("/api/published/download").status_code == 404

            r = c.post("/api/batches", json={"source_url": "not-a-url"})
            assert r.status_code == 400

            r = c.post("/api/batches",
                       json={"source_url": "http://127.0.0.1:9/catalog"})
            assert r.status_code == 502

            assert c.get("/").status_code == 200
    finally:
        srv.stop()
