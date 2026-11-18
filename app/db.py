"""数据库连接与进程内日志缓冲。"""

from __future__ import annotations

import os
import threading
from collections import deque
from datetime import datetime, timezone

import psycopg

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://envmon:envmon@localhost:5432/envmon"
)

_lock = threading.Lock()
_logs: deque[dict] = deque(maxlen=500)
_seq = 0


def connect() -> psycopg.Connection:
    """新建独立连接（采集写入演示也用它，模拟独立采集连接）。"""
    return psycopg.connect(
        DATABASE_URL, autocommit=True, options="-c timezone=UTC"
    )


def log(msg: str):
    global _seq
    with _lock:
        _seq += 1
        _logs.append(
            {
                "seq": _seq,
                "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "msg": msg,
            }
        )
    print(f"[log] {msg}", flush=True)


def logs_since(seq: int) -> list[dict]:
    with _lock:
        return [e for e in _logs if e["seq"] > seq]


def ensure_meta_tables():
    """元数据表兜底创建（init.sql 已建则跳过；也覆盖旧版本已初始化的库）。"""
    with connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_runs (
                month           date        PRIMARY KEY,
                status          text        NOT NULL DEFAULT 'preparing',
                total_estimate  bigint      NOT NULL DEFAULT 0,
                copied_rows     bigint      NOT NULL DEFAULT 0,
                applied_delta   bigint      NOT NULL DEFAULT 0,
                last_station    text,
                last_ts         timestamptz,
                error           text,
                updated_at      timestamptz NOT NULL DEFAULT now(),
                created_at      timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_delta (
                seq         bigserial PRIMARY KEY,
                month       date        NOT NULL,
                op          text        NOT NULL,
                station_id  text        NOT NULL,
                ts          timestamptz NOT NULL,
                value       integer
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS migration_delta_month_seq "
            "ON migration_delta (month, seq)"
        )
        ensure_succession_objects(conn)


def ensure_succession_objects(conn):
    """站号接续：登记表、参与约束、写入边界触发器（与 db/init.sql 保持一致）。

    旧数据卷不会重跑 init.sql，因此应用启动时在此兜底创建。
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS station_succession (
            id           bigserial    PRIMARY KEY,
            old_station  text         NOT NULL,
            new_station  text         NOT NULL,
            effective_at timestamptz  NOT NULL,
            created_at   timestamptz  NOT NULL DEFAULT now(),
            CHECK (old_station <> new_station)
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS station_succession_old_uniq "
        "ON station_succession (old_station)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS station_succession_new_uniq "
        "ON station_succession (new_station)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS station_participation (
            station        text    PRIMARY KEY,
            succession_id  bigint  NOT NULL
                REFERENCES station_succession(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION succession_participation_sync()
        RETURNS trigger AS $fn$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                DELETE FROM station_participation WHERE succession_id = OLD.id;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                DELETE FROM station_participation WHERE succession_id = OLD.id;
            END IF;
            INSERT INTO station_participation (station, succession_id)
            VALUES (NEW.old_station, NEW.id), (NEW.new_station, NEW.id);
            RETURN NEW;
        END
        $fn$ LANGUAGE plpgsql
        """
    )
    row = conn.execute(
        "SELECT 1 FROM pg_trigger WHERE tgname = 'station_succession_participation'"
    ).fetchone()
    if not row:
        conn.execute(
            "CREATE TRIGGER station_succession_participation "
            "AFTER INSERT OR UPDATE OR DELETE ON station_succession "
            "FOR EACH ROW EXECUTE FUNCTION succession_participation_sync()"
        )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION observations_succession_boundary()
        RETURNS trigger AS $fn$
        DECLARE
            s RECORD;
        BEGIN
            PERFORM pg_advisory_xact_lock_shared(730901);
            FOR s IN
                SELECT old_station, new_station, effective_at
                FROM station_succession
                WHERE old_station = NEW.station_id
                   OR new_station = NEW.station_id
            LOOP
                IF NEW.station_id = s.old_station
                   AND NEW.ts >= s.effective_at THEN
                    RAISE EXCEPTION
                        '站号接续边界违例：旧站号 % 在生效时刻(UTC) % 及之后不得再写观测（本次 ts=%）',
                        s.old_station, s.effective_at, NEW.ts;
                END IF;
                IF NEW.station_id = s.new_station
                   AND NEW.ts < s.effective_at THEN
                    RAISE EXCEPTION
                        '站号接续边界违例：新站号 % 在生效时刻(UTC) % 之前不得写观测（本次 ts=%）',
                        s.new_station, s.effective_at, NEW.ts;
                END IF;
            END LOOP;
            RETURN NEW;
        END
        $fn$ LANGUAGE plpgsql
        """
    )
    row = conn.execute(
        "SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "WHERE t.tgname = 'observations_succession_boundary' "
        "AND c.relname = 'observations'"
    ).fetchone()
    if not row:
        conn.execute(
            "CREATE TRIGGER observations_succession_boundary "
            "BEFORE INSERT OR UPDATE ON observations "
            "FOR EACH ROW EXECUTE FUNCTION observations_succession_boundary()"
        )
