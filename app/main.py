import os
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import BadSignature, URLSafeSerializer
from pydantic import BaseModel

from app.config import Config
from app.db import (
    add_source_rows,
    get_db,
    get_pipeline,
    init_db,
    list_pipelines,
    list_source_rows,
    now_str,
    parse_moment,
    pin_spec,
    run_extract,
    run_load,
    set_clock,
    set_extract_delay,
    spec_payload,
)

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
serializer = URLSafeSerializer(Config.SECRET, salt="cc-etl-session")

app = FastAPI(title="cc-etl-lab")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


def _read_session(request: Request) -> dict:
    raw = request.cookies.get("etl_session")
    if not raw:
        return {}
    try:
        data = serializer.loads(raw)
        return data if isinstance(data, dict) else {}
    except BadSignature:
        return {}


def require_user(request: Request) -> str:
    user = _read_session(request).get("user")
    if user != Config.ADMIN_USER:
        raise HTTPException(status_code=401, detail="未登录")
    return user


def _require_dev() -> None:
    if not Config.DEV_HOOKS:
        raise HTTPException(status_code=404, detail="Not found")


async def _json_body(request: Request) -> dict:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def _fail(payload: dict, status: int) -> JSONResponse:
    return JSONResponse({"ok": False, "error": payload["error"]}, status_code=status)


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/")
def root(request: Request):
    if _read_session(request).get("user") == Config.ADMIN_USER:
        return RedirectResponse("/app/", status_code=302)
    return RedirectResponse("/login", status_code=302)


@app.get("/login")
def login_page():
    return FileResponse(STATIC / "login.html")


@app.get("/app/")
def spa_page(request: Request):
    if _read_session(request).get("user") != Config.ADMIN_USER:
        return RedirectResponse("/login", status_code=302)
    return FileResponse(STATIC / "index.html")


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    user = str(body.get("username", "")).strip()
    pwd = str(body.get("password", ""))
    if user != Config.ADMIN_USER or pwd != Config.ADMIN_PASSWORD:
        return JSONResponse({"ok": False, "error": "账号或密码错误"}, status_code=400)
    resp = JSONResponse({"ok": True, "user": user})
    resp.set_cookie(
        "etl_session",
        serializer.dumps({"user": user}),
        httponly=True,
        samesite="lax",
    )
    return resp


@app.post("/api/logout")
def api_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("etl_session")
    return resp


@app.get("/api/me")
def api_me(user: str = Depends(require_user)):
    return {"user": user, "lab": Config.LAB_NAME, "domain": "数据工程"}


# ---------------------------------------------------------------- 口径设置


@app.get("/api/spec")
def api_spec_get(user: str = Depends(require_user)):
    with get_db() as conn:
        return spec_payload(conn)


@app.put("/api/spec")
async def api_spec_put(request: Request, user: str = Depends(require_user)):
    body = await _json_body(request)
    with get_db() as conn:
        payload, status = pin_spec(
            conn,
            method=str(body.get("method") or ""),
            note=str(body.get("note") or ""),
        )
    if status != 200:
        return _fail(payload, status)
    return {"ok": True, "spec": payload}


# ---------------------------------------------------------------- 管道与水位


@app.get("/api/pipelines")
def api_pipelines(user: str = Depends(require_user)):
    with get_db() as conn:
        return {"items": list_pipelines(conn)}


@app.get("/api/pipelines/{pipeline_id}")
def api_pipeline_detail(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = get_pipeline(conn, pipeline_id)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


class ExtractIn(BaseModel):
    mode: str = ""
    method: str = ""
    reason: str = ""


@app.post("/api/pipelines/{pipeline_id}/extract")
def api_extract(
    pipeline_id: int,
    payload: ExtractIn | None = None,
    user: str = Depends(require_user),
):
    # 同步端点：抽取在工作线程里跑，并发争抢才真实发生
    body = payload or ExtractIn()
    with get_db() as conn:
        payload, status = run_extract(
            conn,
            pipeline_id,
            mode=body.mode,
            method=body.method,
            reason=body.reason,
            actor=user,
        )
    if payload is None:
        raise HTTPException(status_code=404, detail="管道不存在")
    if status != 200:
        return _fail(payload, status)
    return payload


@app.post("/api/pipelines/{pipeline_id}/load")
def api_load(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        payload, status = run_load(conn, pipeline_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="管道不存在")
    return payload


# ---------------------------------------------------------------- mock 源行


@app.get("/api/pipelines/{pipeline_id}/source-rows")
def api_source_rows(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        items = list_source_rows(conn, pipeline_id)
    if items is None:
        raise HTTPException(status_code=404, detail="管道不存在")
    return {"items": items}


@app.post("/api/pipelines/{pipeline_id}/source-rows")
async def api_source_rows_add(
    pipeline_id: int, request: Request, user: str = Depends(require_user)
):
    body = await _json_body(request)
    rows = body.get("rows")
    if not isinstance(rows, list) or not rows:
        return JSONResponse({"ok": False, "error": "rows 不能为空"}, status_code=400)
    with get_db() as conn:
        payload, status = add_source_rows(conn, pipeline_id, rows)
    if payload is None:
        raise HTTPException(status_code=404, detail="管道不存在")
    if status != 200:
        return _fail(payload, status)
    return payload


# ---------------------------------------------------------------- 测试钩子


@app.post("/api/dev/clock")
async def api_dev_clock_set(request: Request, user: str = Depends(require_user)):
    _require_dev()
    body = await _json_body(request)
    try:
        moment = parse_moment(str(body.get("now") or ""))
    except ValueError:
        return JSONResponse(
            {"ok": False, "error": "时刻格式无法识别"}, status_code=400
        )
    set_clock(moment)
    return {"ok": True, "now": now_str()}


@app.delete("/api/dev/clock")
def api_dev_clock_clear(user: str = Depends(require_user)):
    _require_dev()
    set_clock(None)
    return {"ok": True, "now": now_str()}


@app.post("/api/dev/extract-delay")
async def api_dev_extract_delay(request: Request, user: str = Depends(require_user)):
    _require_dev()
    body = await _json_body(request)
    try:
        seconds = float(body.get("seconds") or 0)
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "秒数无法识别"}, status_code=400)
    set_extract_delay(seconds)
    return {"ok": True, "seconds": seconds}


def create_app() -> FastAPI:
    return app
