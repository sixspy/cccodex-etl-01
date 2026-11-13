"""存储与领域逻辑：管道、增量水位、口径设置、抽取争用。"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from app.config import Config

# ---------------------------------------------------------------- 推进法与文案

METHOD_SOURCE_MAX_TS = "source_max_ts"
METHOD_LAST_LOADED_COUNT = "last_loaded_count"
METHODS = {
    METHOD_SOURCE_MAX_TS: "源端最大变更时刻",
    METHOD_LAST_LOADED_COUNT: "上次成功落库条数",
}

MODE_INCREMENTAL = "incremental"
MODE_FULL = "full"
MODE_LABELS = {MODE_INCREMENTAL: "增量", MODE_FULL: "全量"}

RUN_SUCCESS = "success"
RUN_CONTENTION_FAILED = "contention_failed"
RUN_STATUS_LABELS = {
    RUN_SUCCESS: "成功",
    RUN_CONTENTION_FAILED: "抽取争用失败",
}

PHASE_LABELS = {"extract": "抽取推进", "load": "落库推进"}

ERR_SPEC_MISSING = "今日推进法未钉死，请先在口径设置钉死"
ERR_METHOD_MISMATCH = "水位推进写歪了"
ERR_FULL_REASON = "全量抽取必须写明原因"
ERR_CONTENTION = "抽取争用失败"
ERR_SPEC_LOCKED = "抽取进行中，禁止改推进说明"
ERR_SPEC_PINNED = "本日推进法已钉死，当日不得改弦"
ERR_SPEC_NOTE = "推进说明必填"
ERR_SPEC_METHOD = "推进法未知，只能二选一"
ERR_MODE = "抽取模式未知"

SEED_PIPELINES = [
    ("订单明细日抽", "orders_daily", "待抽取", "mock://orders/day"),
    ("用户画像宽表", "user_profile", "待抽取", "mock://users/profile"),
    ("库存快照落地", "inventory_snap", "待抽取", "mock://stock/snap"),
    ("支付流水清洗", "pay_clean", "待抽取", "mock://pay/flow"),
]

SEED_SOURCE_ROWS = {
    "orders_daily": [
        ("ORD-1001", "2026-11-10 08:00:00"),
        ("ORD-1002", "2026-11-11 09:30:00"),
        ("", "2026-11-12 10:00:00"),
        ("ORD-1003", "2026-11-12 18:45:00"),
    ],
    "user_profile": [
        ("USR-201", "2026-11-10 07:15:00"),
        ("USR-202", "2026-11-11 12:00:00"),
        ("USR-203", "2026-11-12 21:20:00"),
    ],
    "inventory_snap": [
        ("SKU-88", "2026-11-11 06:00:00"),
        ("", "2026-11-12 06:00:00"),
        ("SKU-89", "2026-11-12 23:00:00"),
    ],
    "pay_clean": [
        ("PAY-1", "2026-11-10 00:30:00"),
        ("PAY-2", "2026-11-11 11:11:11"),
        ("PAY-3", "2026-11-12 12:12:12"),
        ("PAY-4", "2026-11-12 13:13:13"),
    ],
}

# ---------------------------------------------------------------- 时钟（测试可钉死）

_clock_guard = threading.Lock()
_clock_pin: datetime | None = None


def set_clock(moment: datetime | None) -> None:
    """钉死后台时刻；传 None 恢复真实时钟。"""
    global _clock_pin
    with _clock_guard:
        _clock_pin = moment


def now() -> datetime:
    with _clock_guard:
        if _clock_pin is not None:
            return _clock_pin
    return datetime.now(timezone.utc).replace(tzinfo=None)


def now_str() -> str:
    return now().strftime("%Y-%m-%d %H:%M:%S")


def today() -> str:
    return now().strftime("%Y-%m-%d")


def parse_moment(text: str) -> datetime:
    text = (text or "").strip()
    if not text:
        raise ValueError("empty moment")
    return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)


# ---------------------------------------------------------------- 抽取争用锁与延迟钩子

_locks_guard = threading.Lock()
_extract_locks: dict[int, threading.Lock] = {}
_delay_guard = threading.Lock()
_extract_delay = 0.0


def _lock_for(pipeline_id: int) -> threading.Lock:
    with _locks_guard:
        return _extract_locks.setdefault(pipeline_id, threading.Lock())


def is_extracting(pipeline_id: int) -> bool:
    return _lock_for(pipeline_id).locked()


def any_extracting() -> bool:
    with _locks_guard:
        locks = list(_extract_locks.values())
    return any(lk.locked() for lk in locks)


def set_extract_delay(seconds: float) -> None:
    """拉长抽取窗口的测试钩子，便于复现争抢。"""
    global _extract_delay
    with _delay_guard:
        _extract_delay = max(0.0, float(seconds))


def extract_delay() -> float:
    with _delay_guard:
        return _extract_delay


# ---------------------------------------------------------------- 连接与建库


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


@contextmanager
def get_db(db_path: str | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


PIPELINE_EXTRA_COLUMNS = {
    "watermark": "TEXT NOT NULL DEFAULT ''",
    "extracted_today": "INTEGER NOT NULL DEFAULT 0",
    "extract_date": "TEXT NOT NULL DEFAULT ''",
    "staged": "INTEGER NOT NULL DEFAULT 0",
    "staged_nonempty": "INTEGER NOT NULL DEFAULT 0",
    "staged_mode": "TEXT NOT NULL DEFAULT ''",
}


def init_db(db_path: str | None = None, seed_source: bool | None = None) -> None:
    if seed_source is None:
        seed_source = os.environ.get("APP_SEED_SOURCE", "1") != "0"
    with get_db(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pipelines (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              code TEXT NOT NULL UNIQUE,
              status TEXT NOT NULL,
              source_uri TEXT NOT NULL,
              extracted INTEGER NOT NULL DEFAULT 0,
              loaded INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS day_specs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              day TEXT NOT NULL UNIQUE,
              method TEXT NOT NULL,
              note TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS source_rows (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
              code TEXT NOT NULL DEFAULT '',
              change_ts TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS watermark_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
              old_watermark TEXT NOT NULL,
              new_watermark TEXT NOT NULL,
              method TEXT NOT NULL,
              phase TEXT NOT NULL,
              run_id INTEGER,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS extract_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
              actor TEXT NOT NULL,
              mode TEXT NOT NULL,
              method TEXT NOT NULL,
              status TEXT NOT NULL,
              error TEXT NOT NULL DEFAULT '',
              rows INTEGER NOT NULL DEFAULT 0,
              reason TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL
            );
            """
        )
        have = {r["name"] for r in conn.execute("PRAGMA table_info(pipelines)")}
        for col, ddl in PIPELINE_EXTRA_COLUMNS.items():
            if col not in have:
                conn.execute(f"ALTER TABLE pipelines ADD COLUMN {col} {ddl}")

        n = conn.execute("SELECT COUNT(*) AS c FROM pipelines").fetchone()["c"]
        if n == 0:
            conn.executemany(
                "INSERT INTO pipelines (title, code, status, source_uri) VALUES (?,?,?,?)",
                SEED_PIPELINES,
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('lab_name', ?)",
                (Config.LAB_NAME,),
            )
        if seed_source:
            c = conn.execute("SELECT COUNT(*) AS c FROM source_rows").fetchone()["c"]
            if c == 0:
                id_by_code = {
                    r["code"]: r["id"]
                    for r in conn.execute("SELECT id, code FROM pipelines").fetchall()
                }
                for code, rows in SEED_SOURCE_ROWS.items():
                    pid = id_by_code.get(code)
                    if not pid:
                        continue
                    conn.executemany(
                        "INSERT INTO source_rows (pipeline_id, code, change_ts, created_at) "
                        "VALUES (?,?,?,?)",
                        [(pid, rc, ts, now_str()) for rc, ts in rows],
                    )


# ---------------------------------------------------------------- 口径设置（今日推进法）


def get_spec(conn: sqlite3.Connection, day: str | None = None) -> dict | None:
    row = conn.execute(
        "SELECT id, day, method, note, created_at FROM day_specs WHERE day = ?",
        (day or today(),),
    ).fetchone()
    if not row:
        return None
    d = dict(row)
    d["method_name"] = METHODS.get(d["method"], d["method"])
    return d


def spec_payload(conn: sqlite3.Connection) -> dict:
    return {
        "today": today(),
        "spec": get_spec(conn),
        "methods": [{"key": k, "name": v} for k, v in METHODS.items()],
        "extracting": any_extracting(),
    }


def pin_spec(conn: sqlite3.Connection, *, method: str, note: str) -> tuple[dict, int]:
    """钉死本日推进法。写死后当日不得改弦；抽取进行中禁止改。"""
    if any_extracting():
        return {"error": ERR_SPEC_LOCKED}, 409
    if method not in METHODS:
        return {"error": ERR_SPEC_METHOD}, 400
    note = (note or "").strip()
    if not note:
        return {"error": ERR_SPEC_NOTE}, 400
    if get_spec(conn):
        return {"error": ERR_SPEC_PINNED}, 409
    conn.execute(
        "INSERT INTO day_specs (day, method, note, created_at) VALUES (?,?,?,?)",
        (today(), method, note, now_str()),
    )
    return get_spec(conn), 200


# ---------------------------------------------------------------- 视图


def _view(conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
    spec = get_spec(conn)
    d = dict(row)
    watermark = d["watermark"]
    if spec and spec["method"] == METHOD_LAST_LOADED_COUNT and not watermark:
        watermark = "0"
    d["watermark"] = watermark
    d["watermark_display"] = watermark if watermark else "（起点）"
    d["extracted_today"] = d["extracted_today"] if d["extract_date"] == today() else 0
    d["method"] = spec["method"] if spec else None
    d["method_name"] = spec["method_name"] if spec else "未钉死"
    d["spec_set"] = spec is not None
    d["extracting"] = is_extracting(d["id"])
    return d


def list_pipelines(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM pipelines ORDER BY id").fetchall()
    return [_view(conn, r) for r in rows]


def get_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if not row:
        return None
    d = _view(conn, row)
    d["watermark_events"] = [
        {
            **dict(e),
            "method_name": METHODS.get(e["method"], e["method"]),
            "phase_label": PHASE_LABELS.get(e["phase"], e["phase"]),
        }
        for e in conn.execute(
            "SELECT * FROM watermark_events WHERE pipeline_id = ? ORDER BY id",
            (pipeline_id,),
        ).fetchall()
    ]
    d["extract_runs"] = [
        {
            **dict(r),
            "method_name": METHODS.get(r["method"], r["method"]),
            "mode_label": MODE_LABELS.get(r["mode"], r["mode"]),
            "status_label": RUN_STATUS_LABELS.get(r["status"], r["status"]),
        }
        for r in conn.execute(
            "SELECT * FROM extract_runs WHERE pipeline_id = ? ORDER BY id DESC LIMIT 50",
            (pipeline_id,),
        ).fetchall()
    ]
    return d


# ---------------------------------------------------------------- mock 源行


def list_source_rows(conn: sqlite3.Connection, pipeline_id: int) -> list[dict] | None:
    exists = conn.execute(
        "SELECT 1 FROM pipelines WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if not exists:
        return None
    return [
        dict(r)
        for r in conn.execute(
            "SELECT id, pipeline_id, code, change_ts, created_at FROM source_rows "
            "WHERE pipeline_id = ? ORDER BY change_ts, id",
            (pipeline_id,),
        ).fetchall()
    ]


def add_source_rows(
    conn: sqlite3.Connection, pipeline_id: int, rows: list[dict]
) -> tuple[dict, int]:
    exists = conn.execute(
        "SELECT 1 FROM pipelines WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if not exists:
        return None, 404
    inserted = 0
    for raw in rows:
        code = str(raw.get("code") or "")
        try:
            ts = parse_moment(str(raw.get("change_ts") or "")).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            return {"error": f"变更时刻格式无法识别：{raw.get('change_ts')}"}, 400
        conn.execute(
            "INSERT INTO source_rows (pipeline_id, code, change_ts, created_at) "
            "VALUES (?,?,?,?)",
            (pipeline_id, code, ts, now_str()),
        )
        inserted += 1
    return {"ok": True, "inserted": inserted}, 200


# ---------------------------------------------------------------- 抽取与落库


def _record_run(
    conn: sqlite3.Connection,
    pipeline_id: int,
    *,
    actor: str,
    mode: str,
    method: str,
    status: str,
    rows: int = 0,
    reason: str = "",
    error: str = "",
) -> int:
    cur = conn.execute(
        "INSERT INTO extract_runs "
        "(pipeline_id, actor, mode, method, status, error, rows, reason, created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (pipeline_id, actor, mode, method, status, error, rows, reason, now_str()),
    )
    return cur.lastrowid


def _record_event(
    conn: sqlite3.Connection,
    pipeline_id: int,
    *,
    old: str,
    new: str,
    method: str,
    phase: str,
    run_id: int | None,
) -> None:
    conn.execute(
        "INSERT INTO watermark_events "
        "(pipeline_id, old_watermark, new_watermark, method, phase, run_id, created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (pipeline_id, old, new, method, phase, run_id, now_str()),
    )


def run_extract(
    conn: sqlite3.Connection,
    pipeline_id: int,
    *,
    mode: str,
    method: str,
    reason: str,
    actor: str,
) -> tuple[dict | None, int]:
    """执行抽取。水位怎么推进由服务端按今日钉死的推进法裁定。"""
    row = conn.execute(
        "SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if not row:
        return None, 404
    spec = get_spec(conn)
    if not spec:
        return {"error": ERR_SPEC_MISSING}, 400
    if method != spec["method"]:
        return {"error": ERR_METHOD_MISMATCH}, 400
    if mode not in MODE_LABELS:
        return {"error": ERR_MODE}, 400
    reason = (reason or "").strip()
    if mode == MODE_FULL and not reason:
        return {"error": ERR_FULL_REASON}, 400

    lock = _lock_for(pipeline_id)
    if not lock.acquire(blocking=False):
        _record_run(
            conn,
            pipeline_id,
            actor=actor,
            mode=mode,
            method=method,
            status=RUN_CONTENTION_FAILED,
            reason=reason,
            error=ERR_CONTENTION,
        )
        return {"error": ERR_CONTENTION}, 409
    try:
        delay = extract_delay()
        if delay > 0:
            time.sleep(delay)

        old_wm = row["watermark"]
        if mode == MODE_FULL:
            rows = conn.execute(
                "SELECT * FROM source_rows WHERE pipeline_id = ? ORDER BY change_ts, id",
                (pipeline_id,),
            ).fetchall()
        elif spec["method"] == METHOD_SOURCE_MAX_TS:
            rows = conn.execute(
                "SELECT * FROM source_rows WHERE pipeline_id = ? AND change_ts > ? "
                "ORDER BY change_ts, id",
                (pipeline_id, old_wm),
            ).fetchall()
        else:
            try:
                offset = int(old_wm or "0")
            except ValueError:
                offset = 0
            rows = conn.execute(
                "SELECT * FROM source_rows WHERE pipeline_id = ? "
                "ORDER BY change_ts, id LIMIT -1 OFFSET ?",
                (pipeline_id, offset),
            ).fetchall()

        nonempty = sum(1 for r in rows if r["code"].strip())
        run_id = _record_run(
            conn,
            pipeline_id,
            actor=actor,
            mode=mode,
            method=method,
            status=RUN_SUCCESS,
            rows=len(rows),
            reason=reason,
        )

        new_wm = old_wm
        if (
            mode == MODE_INCREMENTAL
            and spec["method"] == METHOD_SOURCE_MAX_TS
            and nonempty
        ):
            # 跟着源端最大变更时刻往前走；编码空的行不推动水位
            mx = max(r["change_ts"] for r in rows if r["code"].strip())
            if mx > old_wm:
                new_wm = mx
                _record_event(
                    conn,
                    pipeline_id,
                    old=old_wm,
                    new=new_wm,
                    method=spec["method"],
                    phase="extract",
                    run_id=run_id,
                )

        day = today()
        extracted_today = (
            row["extracted_today"] + len(rows) if row["extract_date"] == day else len(rows)
        )
        conn.execute(
            "UPDATE pipelines SET status = '已抽取待加载', "
            "extracted = extracted + ?, extracted_today = ?, extract_date = ?, "
            "staged = ?, staged_nonempty = ?, staged_mode = ?, watermark = ? "
            "WHERE id = ?",
            (
                len(rows),
                extracted_today,
                day,
                len(rows),
                nonempty,
                mode,
                new_wm,
                pipeline_id,
            ),
        )
        return get_pipeline(conn, pipeline_id), 200
    finally:
        lock.release()


def run_load(
    conn: sqlite3.Connection, pipeline_id: int
) -> tuple[dict | None, int]:
    """执行落库。推进法为“上次成功落库条数”时，水位在此跟着落库条数走。"""
    row = conn.execute(
        "SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if not row:
        return None, 404
    if row["status"] != "已抽取待加载":
        return get_pipeline(conn, pipeline_id), 200
    spec = get_spec(conn)
    new_wm = row["watermark"]
    if (
        spec
        and spec["method"] == METHOD_LAST_LOADED_COUNT
        and row["staged_mode"] == MODE_INCREMENTAL
        and row["staged_nonempty"] > 0
    ):
        try:
            base = int(row["watermark"] or "0")
        except ValueError:
            base = 0
        new_wm = str(base + row["staged_nonempty"])
        _record_event(
            conn,
            pipeline_id,
            old=row["watermark"],
            new=new_wm,
            method=spec["method"],
            phase="load",
            run_id=None,
        )
    conn.execute(
        "UPDATE pipelines SET status = '已落库', loaded = loaded + staged, "
        "staged = 0, staged_nonempty = 0, staged_mode = '', watermark = ? WHERE id = ?",
        (new_wm, pipeline_id),
    )
    return get_pipeline(conn, pipeline_id), 200
