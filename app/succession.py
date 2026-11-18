"""站号接续：登记（旧号/新号/UTC 生效时刻）与按新号的拼接查询。

并发正确性：
  * 登记事务先取排他咨询锁 pg_advisory_xact_lock(SUCC_LOCK_KEY)，
    观测写入触发器取同一键的共享锁（写入之间互不阻塞）；
    因此登记的数据校验与采集写入严格互斥：
      - 写入先提交 → 登记校验看到违规行 → 整单拒绝；
      - 登记先提交 → 后续违规写入被触发器拒绝；
    不会两边都放行。
  * "每个站号只能参与一次接续"由 station_participation 主键硬保证
    （含跨 old/new 两列的情形），登记事务内再做一次友好校验。
"""

from __future__ import annotations

from datetime import datetime, timezone

import psycopg

from db import connect

# 与 db/init.sql、app/db.py 中触发器使用的咨询锁键一致
SUCC_LOCK_KEY = 730901


class SuccessionConflict(Exception):
    """登记与已有接续关系或已有观测数据冲突（整单拒绝）。"""


def parse_ts(raw: str) -> datetime:
    """解析 ISO 时刻；缺省时区按 UTC 处理。"""
    try:
        ts = datetime.fromisoformat((raw or "").strip().replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"时刻格式应为 ISO 8601（UTC），收到: {raw!r}") from None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def register_succession(old: str, new: str, effective_at: datetime) -> dict:
    """登记一条站号接续。任何校验失败都整体回滚（整单拒绝）。"""
    old = (old or "").strip()
    new = (new or "").strip()
    if not old or not new:
        raise ValueError("旧站号与新站号均不能为空")
    if old == new:
        raise ValueError("旧站号与新站号不能相同")
    with connect() as conn, conn.transaction():
        # 与观测写入触发器互斥（写方持共享锁），并串行化并发登记
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (SUCC_LOCK_KEY,))
        # 再挡住绕过本应用的直接 SQL 登记（触发器之外的第二道保险）
        conn.execute("LOCK TABLE station_succession IN SHARE ROW EXCLUSIVE MODE")
        clash = conn.execute(
            "SELECT station FROM station_participation"
            " WHERE station IN (%s, %s) LIMIT 1",
            (old, new),
        ).fetchone()
        if clash:
            raise SuccessionConflict(
                f"站号 {clash[0]} 已参与过接续，每个站号只能参与一次接续"
            )
        old_after, new_before = conn.execute(
            """
            SELECT
              (SELECT count(*) FROM observations
                WHERE station_id = %s AND ts >= %s),
              (SELECT count(*) FROM observations
                WHERE station_id = %s AND ts <  %s)
            """,
            (old, effective_at, new, effective_at),
        ).fetchone()
        if old_after or new_before:
            raise SuccessionConflict(
                "已有观测违反接续边界，整单拒绝："
                f"旧站号 {old} 在生效时刻后有 {old_after} 条，"
                f"新站号 {new} 在生效时刻前有 {new_before} 条"
            )
        try:
            row = conn.execute(
                "INSERT INTO station_succession"
                "       (old_station, new_station, effective_at)"
                " VALUES (%s, %s, %s)"
                " RETURNING id, effective_at, created_at",
                (old, new, effective_at),
            ).fetchone()
        except psycopg.errors.UniqueViolation:
            # 并发直接 SQL 登记越过校验时被参与约束拦下
            raise SuccessionConflict(
                "站号参与冲突：每个站号只能参与一次接续"
            ) from None
    return {
        "id": row[0],
        "old_station": old,
        "new_station": new,
        "effective_at": row[1].isoformat(),
        "created_at": row[2].isoformat(),
    }


def list_successions() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, old_station, new_station, effective_at, created_at"
            " FROM station_succession ORDER BY id"
        ).fetchall()
    return [
        {
            "id": r[0],
            "old_station": r[1],
            "new_station": r[2],
            "effective_at": r[3].isoformat(),
            "created_at": r[4].isoformat(),
        }
        for r in rows
    ]


def spliced_query(station: str, lo: str, hi: str, limit: int = 1000) -> dict:
    """按站号 + UTC 月份范围查询。

    若该站参与接续（作为新号或旧号），返回拼接后的连续序列：
    生效前只取旧号、生效后只取新号；父表原始站号原样返回。
    """
    station = (station or "").strip()
    if not station:
        raise ValueError("站号不能为空")
    with connect() as conn:
        succ = conn.execute(
            "SELECT id, old_station, new_station, effective_at"
            " FROM station_succession"
            " WHERE new_station = %s OR old_station = %s",
            (station, station),
        ).fetchone()
        if succ:
            _id, old, new, eff = succ
            where = (
                "ts >= %s::date AND ts < %s::date AND "
                "((station_id = %s AND ts < %s) OR (station_id = %s AND ts >= %s))"
            )
            params = (lo, hi, old, eff, new, eff)
        else:
            old = new = None
            where = "ts >= %s::date AND ts < %s::date AND station_id = %s"
            params = (lo, hi, station)
        cnt, vsum = conn.execute(
            f"SELECT count(*), coalesce(sum(value), 0)"
            f" FROM observations WHERE {where}",
            params,
        ).fetchone()
        rows = conn.execute(
            f"SELECT station_id, ts, value FROM observations"
            f" WHERE {where} ORDER BY ts, station_id LIMIT %s",
            (*params, min(limit, 5000)),
        ).fetchall()
    return {
        "station": station,
        "succession": (
            {
                "id": succ[0],
                "old_station": succ[1],
                "new_station": succ[2],
                "effective_at": succ[3].isoformat(),
            }
            if succ
            else None
        ),
        "count": cnt,
        "value_sum": vsum,
        "rows": [
            {
                "station_id": r[0],
                "ts": r[1].isoformat(),
                "value": r[2],
                "leg": (
                    ("old" if r[0] == old else "new") if succ else "plain"
                ),
            }
            for r in rows
        ],
    }
