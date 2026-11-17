"""博物馆藏品开放目录采集端（FastAPI 应用工厂）。

API 一览
--------
POST   /api/batches                新建批次（探测源首页、绑定快照令牌并立即开跑）
POST   /api/batches/{id}/resume    续采失败批次（复用已保存页；invalid 拒绝）
GET    /api/batches                批次列表
GET    /api/batches/{id}           批次详情（含已保存页号）
GET    /api/published              当前已发布数据集元数据 + 藏品
GET    /api/published/download     下载完整 JSON（附件）
GET    /api/config                 前端预填配置
GET    /health                     健康检查
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from .collector import Collector
from .config import Settings, settings as default_settings
from .db import Database

BASE_DIR = Path(__file__).resolve().parent


class CreateBatchBody(BaseModel):
    source_url: str = Field(..., min_length=1, description="目录接口地址（指向 /catalog）")


def _validate_url(url: str) -> str:
    url = url.strip()
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(status_code=400, detail="目录接口地址必须是合法的 http(s) URL")
    return url


def _public_item(row: dict) -> dict:
    """研究人员对外字段：编号用 id（与源协议一致），而非库内列名 item_id。"""
    return {
        "id": row["item_id"],
        "name": row["name"],
        "era": row.get("era"),
        "material": row.get("material"),
    }


def create_app(settings: Settings | None = None,
               db: Database | None = None) -> FastAPI:
    cfg = settings or default_settings
    database = db or Database(cfg.db_path)
    collector = Collector(database, cfg)

    # 重启恢复：上次 running 的批次已没有存活线程，转为可续采的 failed
    recovered = database.recover_on_startup()

    app = FastAPI(title="博物馆藏品开放目录采集端", version="1.0.0")
    app.state.db = database
    app.state.collector = collector
    app.state.settings = cfg
    app.state.recovered_on_startup = recovered

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "recovered_batches": recovered}

    @app.get("/api/config")
    def get_config() -> dict:
        return {
            "default_source_url": cfg.default_source_url,
            "source_admin_url": cfg.source_admin_url,
        }

    # ---------------- 批次 ----------------

    @app.post("/api/batches", status_code=201)
    def create_batch(body: CreateBatchBody) -> dict:
        url = _validate_url(body.source_url)

        # 建批前先探测首页：拿到快照令牌、版本与总页数，确保批次绑定确定快照
        try:
            with httpx.Client(timeout=cfg.request_timeout) as client:
                resp = client.get(url, params={"page": 1})
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"无法连接目录接口: {exc!r}",
            ) from exc

        if resp.status_code == 410:
            raise HTTPException(
                status_code=409,
                detail="源快照令牌已失效（410）：请不带令牌请求源首页获取新快照，或在源控制台重置后重试",
            )
        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"目录接口返回 HTTP {resp.status_code}: {resp.text[:200]}",
            )
        try:
            first_page = resp.json()
        except ValueError as exc:
            raise HTTPException(status_code=502, detail=f"首页响应不是合法 JSON: {exc!r}") from exc

        token = first_page.get("snapshot_token")
        total_pages = first_page.get("total_pages")
        if not token or not isinstance(total_pages, int) or total_pages < 1:
            raise HTTPException(status_code=502, detail="首页缺少 snapshot_token / total_pages，协议不兼容")

        batch_id = database.create_batch(
            source_url=url,
            snapshot_token=str(token),
            source_version=first_page.get("version"),
            total_pages=total_pages,
        )
        # 首页已成功取得，直接落库；采集线程从第 2 页开始，绝不重复请求第 1 页
        database.save_page(batch_id, 1, first_page)
        collector.start_worker(batch_id)

        return database.get_batch(batch_id)

    @app.post("/api/batches/{batch_id}/resume")
    def resume_batch(batch_id: int) -> dict:
        batch = database.get_batch(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="批次不存在")
        if collector.is_active(batch_id):
            raise HTTPException(status_code=409, detail="该批次正在采集中，无需续采")
        if batch["status"] == "invalid":
            raise HTTPException(
                status_code=409,
                detail="该批次快照令牌已失效，无法续采；请新建批次获取新快照，新旧快照不可混用",
            )
        if batch["status"] in ("published", "superseded"):
            raise HTTPException(status_code=409, detail=f"批次已处于终态 {batch['status']}，无需续采")
        if batch["status"] == "running":
            raise HTTPException(status_code=409, detail="批次仍在运行中")

        database.reset_to_running(batch_id)
        collector.start_worker(batch_id)
        return database.get_batch(batch_id)

    @app.get("/api/batches")
    def list_batches() -> dict:
        batches = database.list_batches()
        active = set(collector.active_ids())
        for b in batches:
            b["active"] = b["id"] in active
            b["saved_pages"] = sorted(database.get_saved_pages(b["id"]).keys())
        return {"batches": batches}

    @app.get("/api/batches/{batch_id}")
    def get_batch(batch_id: int) -> dict:
        batch = database.get_batch(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="批次不存在")
        batch["active"] = collector.is_active(batch_id)
        batch["saved_pages"] = sorted(database.get_saved_pages(batch_id).keys())
        return batch

    # ---------------- 已发布数据 ----------------

    @app.get("/api/published")
    def get_published() -> JSONResponse:
        published = database.get_published()
        if published is None:
            return JSONResponse(
                status_code=404,
                content={"detail": "尚无已发布数据集，需有一个批次整批采齐后自动发布"},
            )
        published["items"] = [_public_item(it) for it in published["items"]]
        return JSONResponse(content=published)

    @app.get("/api/published/download")
    def download_published() -> JSONResponse:
        published = database.get_published()
        if published is None:
            raise HTTPException(status_code=404, detail="尚无已发布数据集可下载")
        snap = published["snapshot"]
        body = {
            "dataset": "museum-open-catalog",
            "published_at": snap["published_at"],
            "source_version": snap.get("source_version"),
            "snapshot_token": snap["snapshot_token"],
            "batch_id": snap["batch_id"],
            "item_count": len(published["items"]),
            "items": [_public_item(it) for it in published["items"]],
        }
        filename = f"museum-catalog-batch-{snap['batch_id']}.json"
        return JSONResponse(
            content=body,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Type": "application/json; charset=utf-8",
            },
        )

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse((BASE_DIR / "index.html").read_text(encoding="utf-8"))

    return app


app = create_app()
