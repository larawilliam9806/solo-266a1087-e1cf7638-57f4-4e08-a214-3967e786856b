"""本地示例藏品目录源（独立 FastAPI 应用）。

分页协议
--------
GET /catalog                       取第一页，响应中下发新的 snapshot_token
GET /catalog?token=<t>&page=<n>    凭快照令牌翻页（page 从 1 开始）

每个版本对应不可变快照。同一版本允许多个有效令牌并存（对应多个并行
批次）；切换版本或手动令令牌失效后，旧令牌一律返回 410 Gone；不带
令牌请求第一页会签发新令牌。

故障注入（管理接口，见 / 控制台）
----------------------------------
POST /admin/switch-version        切换版本，旧版本全部令牌失效
POST /admin/fail-page-once        令某版本的某一页下一次请求失败（500，仅一次）
POST /admin/invalidate-token      立即令所有存活令牌失效
GET  /admin/state                 查询源当前状态

首页慢响应演示
--------------
GET /catalog?home_delay=<秒>      仅让“无令牌取首页”的响应延迟（0~60 秒）。
延迟在锁外 sleep，多个请求互不阻塞；翻页请求不受影响。配合两个不同
home_delay 的目录地址，可稳定复现“先启动的请求后返回首页”的乱序场景。
"""

from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, HTMLResponse

from .catalog_data import ALL_VERSIONS, paginate

BASE_DIR = Path(__file__).resolve().parent


class SourceState:
    """内存态：当前版本、快照令牌、一次性故障标记。线程安全。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.version = ALL_VERSIONS[0]
        # 存活令牌 -> 所属版本（同版本可多令牌并存，支撑并行批次）
        self.live_tokens: dict[str, str] = {}
        self.current_token: str | None = None
        # 已失效令牌集合：切换版本/手动失效后加入，永不复用
        self.dead_tokens: set[str] = set()
        # 一次性故障：{(version, page_no): count}，>0 时下一次访问该页报错
        self.fail_once: dict[tuple[str, int], int] = {}
        # 首页请求到达记录（含被 home_delay 延迟的请求），供演示/自测确认
        # “先到的启动请求可能后拿到首页”
        self._home_requests = 0
        self.home_arrivals: list[dict] = []
        self._mint_locked()

    def note_home_arrival(self, delay: float) -> int:
        """记录一次无令牌首页请求的到达（在 sleep 之前调用）。"""
        with self._lock:
            self._home_requests += 1
            seq = self._home_requests
            now = time.time()
            self.home_arrivals.append({
                "seq": seq,
                "delay": delay,
                "arrived_at": datetime.fromtimestamp(now, timezone.utc).isoformat(
                    timespec="milliseconds"),
            })
            # 只保留最近 50 条，避免内存无限增长
            if len(self.home_arrivals) > 50:
                del self.home_arrivals[:-50]
            return seq

    def _mint_locked(self) -> str:
        token = f"snap-{self.version}-{uuid.uuid4().hex[:12]}"
        self.live_tokens[token] = self.version
        self.current_token = token
        return token

    def reset(self) -> None:
        """把源恢复到初始状态（v1、仅一个新令牌、无故障）。供演示/自测隔离。"""
        with self._lock:
            self.version = ALL_VERSIONS[0]
            self.live_tokens.clear()
            self.dead_tokens.clear()
            self.fail_once.clear()
            self.current_token = None
            self._home_requests = 0
            self.home_arrivals = []
            self._mint_locked()

    def _kill_all_live_locked(self) -> None:
        self.dead_tokens.update(self.live_tokens)
        self.live_tokens.clear()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "version": self.version,
                "current_token": self.current_token,
                "live_tokens": sorted(self.live_tokens),
                "dead_tokens": sorted(self.dead_tokens),
                "fail_once": [
                    {"version": v, "page": p, "remaining": n}
                    for (v, p), n in sorted(self.fail_once.items())
                ],
                "versions": ALL_VERSIONS,
                "total_pages": len(paginate(self.version)),
                "home_requests": self._home_requests,
                "home_arrivals": list(self.home_arrivals),
            }

    def switch_version(self, version: str) -> dict:
        with self._lock:
            if version not in ALL_VERSIONS:
                raise KeyError(version)
            if version == self.version and self.live_tokens:
                return {"ok": True, "version": version, "token": self.current_token}
            # 旧版本的所有快照令牌一律失效，不可与新版本混用
            self._kill_all_live_locked()
            self.version = version
            self.fail_once.clear()
            token = self._mint_locked()
            return {"ok": True, "version": version, "token": token}

    def invalidate(self) -> dict:
        with self._lock:
            self._kill_all_live_locked()
            self.current_token = None
            return {"ok": True}

    def arm_fail_once(self, version: str, page: int) -> dict:
        with self._lock:
            if version not in ALL_VERSIONS:
                raise KeyError(version)
            total = len(paginate(version))
            if not 1 <= page <= total:
                raise ValueError(f"page 必须在 1..{total} 之间")
            self.fail_once[(version, page)] = self.fail_once.get((version, page), 0) + 1
            return {"ok": True, "version": version, "page": page}

    def serve_catalog(self, token: str | None, page: int) -> JSONResponse:
        with self._lock:
            version = self.version
            pages = paginate(version)
            total_pages = len(pages)

            if page < 1 or page > total_pages:
                return JSONResponse(
                    status_code=400,
                    content={"error": "bad_page", "message": f"page 超出 1..{total_pages}"},
                )

            # 第一页：无令牌 → 签发新快照令牌；持已失效令牌 → 显式 410
            if page == 1 and token is None:
                active_token = self._mint_locked()
            elif page == 1 and token in self.dead_tokens:
                return JSONResponse(
                    status_code=410,
                    content={
                        "error": "snapshot_expired",
                        "message": "快照令牌已失效，请新建批次获取新令牌",
                    },
                )
            else:
                # 翻页必须携带令牌：失效或非存活令牌一律 410
                if not token:
                    return JSONResponse(
                        status_code=400,
                        content={"error": "token_required",
                                 "message": "翻页必须携带 snapshot_token"},
                    )
                if token in self.dead_tokens or token not in self.live_tokens:
                    return JSONResponse(
                        status_code=410,
                        content={
                            "error": "snapshot_expired",
                            "message": "快照令牌已失效，该批次无法继续，请新建批次",
                        },
                    )
                # 双保险：令牌必须属于当前版本
                if self.live_tokens.get(token) != version:
                    return JSONResponse(
                        status_code=410,
                        content={
                            "error": "snapshot_expired",
                            "message": "快照属于旧版本，已失效，请新建批次",
                        },
                    )
                active_token = token

            # 一次性故障：命中则消耗一次并返回 500（页面未被成功消费）
            key = (version, page)
            if self.fail_once.get(key, 0) > 0:
                self.fail_once[key] -= 1
                if self.fail_once[key] == 0:
                    del self.fail_once[key]
                return JSONResponse(
                    status_code=500,
                    content={"error": "injected_failure",
                             "message": f"第 {page} 页注入失败（仅一次）"},
                )

            body = {
                "version": version,
                "snapshot_token": active_token,
                "page": page,
                "total_pages": total_pages,
                "items": pages[page - 1],
                "next_page": page + 1 if page < total_pages else None,
            }
            return JSONResponse(status_code=200, content=body)


state = SourceState()
app = FastAPI(title="博物馆藏品示例目录源", version="1.0.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/catalog")
def catalog(
    token: str | None = Query(default=None, description="快照令牌"),
    page: int = Query(default=1, ge=1, description="页码，从 1 开始"),
    home_delay: float = Query(
        default=0, ge=0, le=60,
        description="仅对无令牌首页响应生效的模拟延迟（秒），用于演示响应乱序",
    ),
) -> JSONResponse:
    # 延迟必须在状态锁之外：其他并发请求（含较晚批次）不被慢首页阻塞。
    # 只影响“无令牌取首页”的探测请求，翻页采集仍按原速进行。
    if page == 1 and token is None:
        state.note_home_arrival(home_delay)
        if home_delay > 0:
            time.sleep(home_delay)
    return state.serve_catalog(token, page)


@app.post("/admin/switch-version")
def admin_switch_version(version: str = Query(...)) -> JSONResponse:
    try:
        result = state.switch_version(version)
    except KeyError:
        return JSONResponse(
            status_code=404,
            content={"error": "unknown_version", "message": f"未知版本 {version}"},
        )
    return JSONResponse(status_code=200, content=result)


@app.post("/admin/fail-page-once")
def admin_fail_page_once(version: str = Query(...), page: int = Query(...)) -> JSONResponse:
    try:
        result = state.arm_fail_once(version, page)
    except KeyError:
        return JSONResponse(
            status_code=404,
            content={"error": "unknown_version", "message": f"未知版本 {version}"},
        )
    except ValueError as exc:
        return JSONResponse(status_code=400,
                            content={"error": "bad_page", "message": str(exc)})
    return JSONResponse(status_code=200, content=result)


@app.post("/admin/invalidate-token")
def admin_invalidate() -> dict:
    return state.invalidate()


@app.get("/admin/state")
def admin_state() -> dict:
    return state.snapshot()


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse((BASE_DIR / "admin.html").read_text(encoding="utf-8"))
