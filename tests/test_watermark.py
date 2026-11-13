"""增量水位分项验收：水位推进、增量边界、争抢。"""

import time
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.main import app

TS = {"method": "source_max_ts", "note": "跟着源端最大变更时刻往前走"}
CNT = {"method": "last_loaded_count", "note": "跟着本管道上次成功落库条数往前走"}


def pin_spec(client, spec=TS):
    r = client.put("/api/spec", json=spec)
    assert r.status_code == 200, r.text
    return r


def seed_rows(client, pid, rows):
    r = client.post(f"/api/pipelines/{pid}/source-rows", json={"rows": rows})
    assert r.status_code == 200, r.text
    return r


def extract(client, pid, method="source_max_ts", mode="incremental", reason=""):
    return client.post(
        f"/api/pipelines/{pid}/extract",
        json={"mode": mode, "method": method, "reason": reason},
    )


def first_pid(client):
    return client.get("/api/pipelines").json()["items"][0]["id"]


def get_item(client, pid):
    items = client.get("/api/pipelines").json()["items"]
    return next(i for i in items if i["id"] == pid)


def second_client():
    c = TestClient(app)
    r = c.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200
    return c


# ---------------------------------------------------------------- 口径设置


def test_spec_pin_once_per_day(authed):
    pin_spec(authed, TS)
    again = authed.put("/api/spec", json=CNT)
    assert again.status_code == 409
    assert "不得改弦" in again.json()["error"]
    spec = authed.get("/api/spec").json()
    assert spec["spec"]["method"] == "source_max_ts"
    assert spec["spec"]["method_name"] == "源端最大变更时刻"
    assert spec["spec"]["note"] == TS["note"]


def test_spec_requires_note_and_known_method(authed):
    r1 = authed.put("/api/spec", json={"method": "source_max_ts", "note": ""})
    assert r1.status_code == 400
    assert "说明" in r1.json()["error"]
    r2 = authed.put("/api/spec", json={"method": "nonsense", "note": "x"})
    assert r2.status_code == 400


def test_extract_without_spec_blocked(authed):
    pid = first_pid(authed)
    r = extract(authed, pid)
    assert r.status_code == 400
    assert "钉死" in r.json()["error"]


# ---------------------------------------------------------------- 推进法写歪了要拦


def test_wrong_method_blocked(authed):
    pin_spec(authed, TS)
    pid = first_pid(authed)
    seed_rows(authed, pid, [{"code": "W-1", "change_ts": "2026-11-13 08:00:00"}])
    r = extract(authed, pid, method="last_loaded_count")
    assert r.status_code == 400
    assert r.json()["error"] == "水位推进写歪了"
    item = get_item(authed, pid)
    assert item["extracted_today"] == 0
    assert item["watermark"] == ""


# ---------------------------------------------------------------- 水位推进与增量边界


def test_incremental_advance_and_boundary(authed):
    pin_spec(authed, TS)
    pid = first_pid(authed)
    seed_rows(
        authed,
        pid,
        [
            {"code": "A-1", "change_ts": "2026-11-13 08:00:00"},
            {"code": "A-2", "change_ts": "2026-11-13 09:00:00"},
            {"code": "", "change_ts": "2026-11-13 10:00:00"},  # 空编码不推水位
        ],
    )
    r = extract(authed, pid)
    assert r.status_code == 200
    assert r.json()["watermark"] == "2026-11-13 09:00:00"
    assert r.json()["extracted_today"] == 3

    # 增量边界：水位之前（含等于水位）的变更不再收
    seed_rows(
        authed,
        pid,
        [
            {"code": "A-3", "change_ts": "2026-11-13 08:30:00"},  # 水位前 → 不收
            {"code": "A-4", "change_ts": "2026-11-13 09:00:00"},  # 等于水位 → 不收
            {"code": "A-5", "change_ts": "2026-11-13 09:30:00"},  # 水位后 → 收
        ],
    )
    r2 = extract(authed, pid)
    assert r2.status_code == 200
    # 收 A-5；10:00 的空编码行因没推动水位被重新收到，但仍不推水位
    assert r2.json()["staged"] == 2
    assert r2.json()["extracted_today"] == 5
    assert r2.json()["watermark"] == "2026-11-13 09:30:00"

    detail = authed.get(f"/api/pipelines/{pid}").json()
    events = detail["watermark_events"]
    assert [e["new_watermark"] for e in events] == [
        "2026-11-13 09:00:00",
        "2026-11-13 09:30:00",
    ]
    assert events[0]["old_watermark"] == ""
    assert all(e["method"] == "source_max_ts" for e in events)


def test_empty_code_does_not_advance(authed):
    pin_spec(authed, TS)
    pid = first_pid(authed)
    seed_rows(
        authed,
        pid,
        [
            {"code": "", "change_ts": "2026-11-13 08:00:00"},
            {"code": "   ", "change_ts": "2026-11-13 09:00:00"},
        ],
    )
    r = extract(authed, pid)
    assert r.status_code == 200
    assert r.json()["extracted_today"] == 2
    assert r.json()["watermark"] == ""
    detail = authed.get(f"/api/pipelines/{pid}").json()
    assert detail["watermark_events"] == []


# ---------------------------------------------------------------- 全量须填原因


def test_full_requires_reason_and_crosses_watermark(authed):
    pin_spec(authed, TS)
    pid = first_pid(authed)
    seed_rows(
        authed,
        pid,
        [
            {"code": "F-1", "change_ts": "2026-11-13 08:00:00"},
            {"code": "F-2", "change_ts": "2026-11-13 09:00:00"},
        ],
    )
    extract(authed, pid)  # 增量先把水位推到 09:00

    no_reason = extract(authed, pid, mode="full", reason="")
    assert no_reason.status_code == 400
    assert "原因" in no_reason.json()["error"]

    ok = extract(authed, pid, mode="full", reason="月底重跑全量")
    assert ok.status_code == 200
    assert ok.json()["staged"] == 2  # 越过水位，全量都收
    assert ok.json()["watermark"] == "2026-11-13 09:00:00"  # 全量不推水位

    detail = authed.get(f"/api/pipelines/{pid}").json()
    latest = detail["extract_runs"][0]
    assert latest["mode"] == "full"
    assert latest["reason"] == "月底重跑全量"


# ---------------------------------------------------------------- 落库条数推进法


def test_count_method_advances_on_load(authed):
    pin_spec(authed, CNT)
    pid = first_pid(authed)
    seed_rows(
        authed,
        pid,
        [
            {"code": "C-1", "change_ts": "2026-11-13 08:00:00"},
            {"code": "", "change_ts": "2026-11-13 08:05:00"},  # 空编码不推水位
            {"code": "C-2", "change_ts": "2026-11-13 08:10:00"},
            {"code": "C-3", "change_ts": "2026-11-13 08:15:00"},
        ],
    )
    r = extract(authed, pid, method="last_loaded_count")
    assert r.status_code == 200
    assert r.json()["staged"] == 4
    assert r.json()["watermark"] == "0"  # 抽取不推，落库才推

    ld = authed.post(f"/api/pipelines/{pid}/load")
    assert ld.status_code == 200
    assert ld.json()["loaded"] == 4
    assert ld.json()["watermark"] == "3"  # 4 行里 3 行有编码

    detail = authed.get(f"/api/pipelines/{pid}").json()
    event = detail["watermark_events"][-1]
    assert event["phase"] == "load"
    assert event["new_watermark"] == "3"

    # 全量 + 落库不推水位
    extract(authed, pid, method="last_loaded_count", mode="full", reason="核对全量")
    authed.post(f"/api/pipelines/{pid}/load")
    assert authed.get(f"/api/pipelines/{pid}").json()["watermark"] == "3"


# ---------------------------------------------------------------- 争抢只留一笔


def test_concurrent_extract_only_one_wins(authed):
    pin_spec(authed, TS)
    pid = first_pid(authed)
    seed_rows(authed, pid, [{"code": "X-1", "change_ts": "2026-11-13 08:00:00"}])
    authed.post("/api/dev/extract-delay", json={"seconds": 0.6})
    other = second_client()
    body = {"mode": "incremental", "method": "source_max_ts", "reason": ""}
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(authed.post, f"/api/pipelines/{pid}/extract", json=body)
            f2 = pool.submit(other.post, f"/api/pipelines/{pid}/extract", json=body)
            results = [f1.result(timeout=15), f2.result(timeout=15)]
    finally:
        authed.post("/api/dev/extract-delay", json={"seconds": 0})
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 409]
    loser = next(r for r in results if r.status_code == 409)
    assert loser.json()["error"] == "抽取争用失败"

    detail = authed.get(f"/api/pipelines/{pid}").json()
    runs = detail["extract_runs"]
    assert sum(1 for r in runs if r["status"] == "success") == 1
    assert sum(1 for r in runs if r["status"] == "contention_failed") == 1
    assert any("抽取争用失败" in r["error"] for r in runs)


def test_spec_change_blocked_while_extracting(authed):
    pin_spec(authed, TS)
    pid = first_pid(authed)
    seed_rows(authed, pid, [{"code": "Y-1", "change_ts": "2026-11-13 08:00:00"}])
    authed.post("/api/dev/extract-delay", json={"seconds": 0.8})
    other = second_client()
    body = {"mode": "incremental", "method": "source_max_ts", "reason": ""}
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(other.post, f"/api/pipelines/{pid}/extract", json=body)
            time.sleep(0.2)  # 等抽取进入进行中
            r = authed.put("/api/spec", json=CNT)
            assert r.status_code == 409
            assert "抽取进行中" in r.json()["error"]
            assert fut.result(timeout=15).status_code == 200
    finally:
        authed.post("/api/dev/extract-delay", json={"seconds": 0})


# ---------------------------------------------------------------- 时钟钉死与跨日


def test_clock_pin_and_day_rollover(authed):
    authed.post("/api/dev/clock", json={"now": "2026-11-13 08:00:00"})
    pin_spec(authed, TS)
    pid = first_pid(authed)
    seed_rows(authed, pid, [{"code": "D-1", "change_ts": "2026-11-13 07:00:00"}])
    extract(authed, pid)
    assert get_item(authed, pid)["extracted_today"] == 1

    # 钉到次日：今日已抽归零，可重新钉推进法（换法也允许）
    authed.post("/api/dev/clock", json={"now": "2026-11-14 08:00:00"})
    item = get_item(authed, pid)
    assert item["extracted_today"] == 0
    r = authed.put("/api/spec", json=CNT)
    assert r.status_code == 200
    spec = authed.get("/api/spec").json()
    assert spec["spec"]["day"] == "2026-11-14"
    assert spec["spec"]["method"] == "last_loaded_count"


# ---------------------------------------------------------------- 看板读数与页面


def test_list_exposes_watermark_fields(authed):
    pin_spec(authed, TS)
    item = get_item(authed, first_pid(authed))
    for key in (
        "watermark",
        "watermark_display",
        "extracted_today",
        "loaded",
        "method",
        "method_name",
    ):
        assert key in item
    assert item["method_name"] == "源端最大变更时刻"
    detail = authed.get(f"/api/pipelines/{item['id']}").json()
    assert "watermark_events" in detail
    assert "extract_runs" in detail


def test_page_distinguishes_watermark_and_method(authed):
    r = authed.get("/app/")
    assert r.status_code == 200
    assert "水位值" in r.text
    assert "推进法" in r.text
    assert "口径设置" in r.text
