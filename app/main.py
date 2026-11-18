"""FastAPI 入口：迁移控制、进度查询、父表查询、采集写入演示、一致性校验、站号接续。"""

from __future__ import annotations

import os
import random
import threading
from pathlib import Path

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

import migrator
import succession
from db import connect, ensure_meta_tables, log, logs_since
from migrator import (
    AlreadyDone,
    MigrationBusy,
    current_worker_month,
    next_month,
    parse_month,
    start_migration,
)
from succession import SuccessionConflict

app = FastAPI(title="观测数据月分区在线迁移")
STATIC = Path(__file__).parent / "static"


@app.on_event("startup")
def _startup():
    ensure_meta_tables()
    log("应用启动，元数据表就绪")


# --------------------------------------------------------------------- 页面
@app.get("/", response_class=FileResponse)
def index():
    return STATIC / "index.html"


# --------------------------------------------------------------------- 状态
def _partition_months(conn) -> dict[str, str]:
    """已挂载的月分区：{ 'YYYY-MM': 'observations_YYYY_MM' }（默认分区除外）。"""
    rows = conn.execute(
        """
        SELECT c.relname FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 'observations'::regclass
          AND c.relname <> 'observations_default'
        """
    ).fetchall()
    out = {}
    for (name,) in rows:
        # observations_2026_01 -> 2026-01
        try:
            suffix = name.rsplit("observations_", 1)[1]
            y, m = suffix.split("_")
            out[f"{y}-{m}"] = name
        except Exception:  # noqa: BLE001
            out[name] = name
    return out


@app.get("/api/state")
def api_state():
    with connect() as conn:
        default_rows = {
            r[0].isoformat()[:7]: r[1]
            for r in conn.execute(
                "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date AS m,"
                "       count(*) FROM observations_default GROUP BY 1"
            )
        }
        parts = _partition_months(conn)
        part_counts = {}
        for ym, tbl in parts.items():
            part_counts[ym] = conn.execute(
                f'SELECT count(*) FROM "{tbl}"'
            ).fetchone()[0]
        runs = {
            r[0].isoformat()[:7]: {
                "status": r[1],
                "total_estimate": r[2],
                "copied_rows": r[3],
                "applied_delta": r[4],
                "error": r[5],
                "updated_at": r[6].isoformat() if r[6] else None,
            }
            for r in conn.execute(
                "SELECT month, status, total_estimate, copied_rows,"
                "       applied_delta, error, updated_at FROM migration_runs"
            )
        }
        backlog = {
            r[0].isoformat()[:7]: r[1]
            for r in conn.execute(
                "SELECT month, count(*) FROM migration_delta GROUP BY 1"
            )
        }
    months = sorted(set(default_rows) | set(parts) | set(runs), reverse=True)
    return {
        "months": [
            {
                "month": ym,
                "default_rows": default_rows.get(ym, 0),
                "partition": parts.get(ym),
                "partition_rows": part_counts.get(ym, 0),
                "run": runs.get(ym),
                "delta_backlog": backlog.get(ym, 0),
            }
            for ym in months
        ],
        "worker_month": current_worker_month(),
        "auto_writer": auto_writer_state(),
    }


@app.get("/api/logs")
def api_logs(since: int = 0):
    return {"logs": logs_since(since)}


# --------------------------------------------------------------------- 迁移
class MigrateReq(BaseModel):
    month: str
    batch_size: int = Field(default=500, ge=1, le=10000)
    sleep_ms: int = Field(default=0, ge=0, le=10000)
    fail_at: str | None = None  # 故障注入（演示/自测用）


@app.post("/api/migrate")
def api_migrate(req: MigrateReq):
    try:
        return start_migration(req.month, req.batch_size, req.sleep_ms, req.fail_at)
    except AlreadyDone as e:
        raise HTTPException(status_code=409, detail=str(e))
    except MigrationBusy as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ------------------------------------------------------------------ 父表查询
@app.get("/api/query")
def api_query(month: str, limit: int = 20):
    m = parse_month(month)
    lo, hi = m.isoformat(), next_month(m).isoformat()
    with connect() as conn:
        cnt, distinct_cnt, vsum = conn.execute(
            "SELECT count(*), count(DISTINCT (station_id, ts)),"
            "       coalesce(sum(value), 0)"
            " FROM observations WHERE ts >= %s::date AND ts < %s::date",
            (lo, hi),
        ).fetchone()
        rows = conn.execute(
            "SELECT station_id, ts, value FROM observations"
            " WHERE ts >= %s::date AND ts < %s::date"
            " ORDER BY ts DESC, station_id LIMIT %s",
            (lo, hi, min(limit, 200)),
        ).fetchall()
    return {
        "month": month,
        "parent_count": cnt,
        "distinct_keys": distinct_cnt,
        "duplicates": cnt - distinct_cnt,
        "value_sum": vsum,
        "sample": [
            {"station_id": r[0], "ts": r[1].isoformat(), "value": r[2]}
            for r in rows
        ],
    }


# --------------------------------------------------------------- 采集写入演示
STATIONS = [f"ST{i:03d}" for i in range(1, 9)]


def _random_ts_in_month(conn, lo: str, hi: str) -> str:
    return conn.execute(
        "SELECT (%s::timestamptz + random() * (%s::timestamptz - %s::timestamptz))"
        "::timestamptz",
        (lo, hi, lo),
    ).fetchone()[0].isoformat()


def _targeted_write(req: "WriteReq") -> dict:
    """定向写入：显式指定站号/时刻（可再指定新站号/新时刻做改号、跨界改时刻）。

    与随机写入一样走独立连接直接 SQL；接续边界由数据库触发器强制，
    违规时抛出的异常由上层统一转成 409。
    """
    station = (req.station_id or "").strip()
    if not station or not req.ts:
        raise HTTPException(status_code=400,
                            detail="定向写入需要 station_id 与 ts")
    ts = succession.parse_ts(req.ts)
    with connect() as conn:  # 独立连接，自动提交
        if req.op == "insert":
            value = req.value if req.value is not None else random.randint(0, 100)
            row = conn.execute(
                "INSERT INTO observations (station_id, ts, value)"
                " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                " RETURNING station_id, ts, value",
                (station, ts, value),
            ).fetchone()
            if not row:
                return {"op": "insert", "skipped": "主键冲突，未写入"}
            return {"op": "insert", "station_id": row[0],
                    "ts": row[1].isoformat(), "value": row[2]}
        if req.op == "update":
            new_station = (req.new_station_id or "").strip() or station
            new_ts = succession.parse_ts(req.new_ts) if req.new_ts else ts
            value = req.value if req.value is not None else random.randint(0, 100)
            row = conn.execute(
                "UPDATE observations SET station_id = %s, ts = %s, value = %s"
                " WHERE station_id = %s AND ts = %s"
                " RETURNING station_id, ts, value",
                (new_station, new_ts, value, station, ts),
            ).fetchone()
            if not row:
                return {"op": "update", "skipped": "目标行不存在"}
            return {"op": "update", "station_id": row[0],
                    "ts": row[1].isoformat(), "value": row[2]}
        if req.op == "delete":
            row = conn.execute(
                "DELETE FROM observations WHERE station_id = %s AND ts = %s"
                " RETURNING station_id, ts, value",
                (station, ts),
            ).fetchone()
            if not row:
                return {"op": "delete", "skipped": "目标行不存在"}
            return {"op": "delete", "station_id": row[0],
                    "ts": row[1].isoformat(), "old_value": row[2]}
    raise HTTPException(status_code=400, detail=f"未知操作: {req.op}")


def demo_write(op: str, month: str) -> dict:
    """模拟独立采集连接：每次新建连接，对父表直接 INSERT/UPDATE/DELETE。"""
    m = parse_month(month)
    lo, hi = m.isoformat(), next_month(m).isoformat()
    with connect() as conn:  # 独立连接，自动提交
        if op == "insert":
            for _ in range(10):
                station = random.choice(STATIONS)
                ts = _random_ts_in_month(conn, lo, hi)
                value = random.randint(0, 100)
                row = conn.execute(
                    "INSERT INTO observations (station_id, ts, value)"
                    " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                    " RETURNING station_id, ts, value",
                    (station, ts, value),
                ).fetchone()
                if row:
                    return {"op": "insert", "station_id": row[0],
                            "ts": row[1].isoformat(), "value": row[2]}
            return {"op": "insert", "skipped": "随机主键冲突，未写入"}
        # update / delete 先随机取该月一行
        row = conn.execute(
            "SELECT station_id, ts, value FROM observations"
            " WHERE ts >= %s::date AND ts < %s::date"
            " ORDER BY random() LIMIT 1",
            (lo, hi),
        ).fetchone()
        if not row:
            return {"op": op, "skipped": "该月没有可操作的数据"}
        if op == "update":
            new_value = random.randint(0, 100)
            conn.execute(
                "UPDATE observations SET value = %s"
                " WHERE station_id = %s AND ts = %s",
                (new_value, row[0], row[1]),
            )
            return {"op": "update", "station_id": row[0], "ts": row[1].isoformat(),
                    "old_value": row[2], "value": new_value}
        if op == "delete":
            conn.execute(
                "DELETE FROM observations WHERE station_id = %s AND ts = %s",
                (row[0], row[1]),
            )
            return {"op": "delete", "station_id": row[0], "ts": row[1].isoformat(),
                    "old_value": row[2]}
    raise HTTPException(status_code=400, detail=f"未知操作: {op}")


class WriteReq(BaseModel):
    op: str
    month: str | None = None
    # 定向写入（可选）：显式站号/时刻/测值，update 可再改站号与时刻
    station_id: str | None = None
    ts: str | None = None
    value: int | None = None
    new_station_id: str | None = None
    new_ts: str | None = None


@app.post("/api/demo/write")
def api_demo_write(req: WriteReq):
    try:
        if req.station_id or req.ts:
            result = _targeted_write(req)
        else:
            if not req.month:
                raise HTTPException(status_code=400, detail="随机写入需要 month")
            result = demo_write(req.op, req.month)
    except psycopg.Error as e:
        # 接续边界等数据库侧拒绝：写入已整体回滚，接续关系不受影响
        log(f"[采集写入] 被拒绝: {e.diag.message_primary if e.diag else e}")
        raise HTTPException(
            status_code=409,
            detail=e.diag.message_primary if e.diag else str(e),
        ) from None
    log(f"[采集写入] {result}")
    return result


# 自动采集写入（演示复制期间持续不断的独立写入）
class AutoReq(BaseModel):
    enabled: bool
    month: str
    interval_ms: int = Field(default=300, ge=50, le=10000)


_auto_stop = threading.Event()
_auto_thread: threading.Thread | None = None
_auto_lock = threading.Lock()


def _auto_loop(month: str, interval_ms: int):
    ops = ["insert"] * 5 + ["update"] * 3 + ["delete"] * 2
    while not _auto_stop.is_set():
        try:
            result = demo_write(random.choice(ops), month)
            log(f"[自动采集] {result}")
        except Exception as exc:  # noqa: BLE001
            log(f"[自动采集] 写入失败: {exc!r}")
        _auto_stop.wait(interval_ms / 1000)


def auto_writer_state() -> dict:
    t = _auto_thread
    return {"running": bool(t and t.is_alive())}


@app.post("/api/demo/auto")
def api_demo_auto(req: AutoReq):
    global _auto_thread
    with _auto_lock:
        if _auto_thread and _auto_thread.is_alive():
            _auto_stop.set()
            _auto_thread.join(timeout=3)
        _auto_thread = None
        if req.enabled:
            parse_month(req.month)
            _auto_stop.clear()
            _auto_thread = threading.Thread(
                target=_auto_loop, args=(req.month, req.interval_ms),
                daemon=True, name="auto-writer",
            )
            _auto_thread.start()
            log(f"[自动采集] 已启动，目标月份 {req.month}，间隔 {req.interval_ms}ms")
        else:
            log("[自动采集] 已停止")
    return auto_writer_state()


# ------------------------------------------------------------- 自测用崩溃注入
@app.post("/api/debug/crash")
def api_debug_crash():
    """模拟进程宕机：0.2s 后 os._exit，不做任何清理（自测断点续迁用）。"""
    log("[debug] 收到崩溃指令，进程即将硬退出（模拟宕机）")
    threading.Timer(0.2, lambda: os._exit(1)).start()
    return {"crashing": True}


# ----------------------------------------------------------------- 站号接续
class SuccessionReq(BaseModel):
    old_station: str
    new_station: str
    effective_at: str  # ISO 8601，缺省时区按 UTC


@app.get("/api/successions")
def api_successions():
    return {"successions": succession.list_successions()}


@app.post("/api/successions", status_code=201)
def api_register_succession(req: SuccessionReq):
    try:
        rec = succession.register_succession(
            req.old_station, req.new_station,
            succession.parse_ts(req.effective_at),
        )
    except SuccessionConflict as e:
        log(f"[站号接续] 登记被拒绝: {e}")
        raise HTTPException(status_code=409, detail=str(e)) from None
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    log(f"[站号接续] 已登记: {rec['old_station']} → {rec['new_station']}"
        f" @ {rec['effective_at']}")
    return rec


@app.get("/api/succession/query")
def api_succession_query(station: str, month: str, limit: int = 1000):
    m = parse_month(month)
    try:
        result = succession.spliced_query(
            station, m.isoformat(), next_month(m).isoformat(), limit
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    result["month"] = month
    return result


# --------------------------------------------------------------------- 校验
@app.get("/api/verify")
def api_verify(month: str):
    m = parse_month(month)
    lo, hi = m.isoformat(), next_month(m).isoformat()
    with connect() as conn:
        parent_cnt, parent_distinct, parent_sum = conn.execute(
            "SELECT count(*), count(DISTINCT (station_id, ts)),"
            "       coalesce(sum(value),0) FROM observations"
            " WHERE ts >= %s::date AND ts < %s::date",
            (lo, hi),
        ).fetchone()
        default_cnt = conn.execute(
            "SELECT count(*) FROM observations_default"
            " WHERE ts >= %s::date AND ts < %s::date",
            (lo, hi),
        ).fetchone()[0]
        parts = _partition_months(conn)
        part_cnt = None
        if month in parts:
            part_cnt = conn.execute(
                f'SELECT count(*) FROM "{parts[month]}"'
            ).fetchone()[0]
        run = conn.execute(
            "SELECT status FROM migration_runs WHERE month = %s", (m,)
        ).fetchone()
    return {
        "month": month,
        "status": run[0] if run else None,
        "parent_count": parent_cnt,
        "parent_duplicates": parent_cnt - parent_distinct,
        "parent_value_sum": parent_sum,
        "default_remaining": default_cnt,
        "partition": parts.get(month),
        "partition_count": part_cnt,
        "consistent": (
            parent_cnt == parent_distinct
            and (part_cnt is None or part_cnt == parent_cnt)
            and (part_cnt is None or default_cnt == 0)
        ),
    }
