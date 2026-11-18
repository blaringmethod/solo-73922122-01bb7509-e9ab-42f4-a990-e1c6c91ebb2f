-- ============================================================================
-- 环境监测数仓 · 观测表分区迁移演示 — 数据库初始化
-- ----------------------------------------------------------------------------
--  * observations  : 父表，按 UTC 时刻 RANGE 分区，主键 (station_id, ts)
--  * observations_default : 唯一的默认分区，历史观测最初全部落在这里
--  * migration_runs  : 每个月份迁移的状态机记录（幂等/断点续迁的依据）
--  * migration_delta : 迁移期间由触发器捕获的增量变更（新增/更正/删除）
-- ============================================================================

CREATE TABLE IF NOT EXISTS observations (
    station_id  text          NOT NULL,
    ts          timestamptz   NOT NULL,
    value       integer       NOT NULL,
    PRIMARY KEY (station_id, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE IF NOT EXISTS observations_default
    PARTITION OF observations DEFAULT;

-- 迁移状态机：idle → preparing → copying → applying → finalizing → done
-- failed 状态仅作展示；重试时从断点继续（影子表/增量表均保留）。
CREATE TABLE IF NOT EXISTS migration_runs (
    month           date        PRIMARY KEY,   -- 月份首日（UTC）
    status          text        NOT NULL DEFAULT 'preparing',
    total_estimate  bigint      NOT NULL DEFAULT 0,
    copied_rows     bigint      NOT NULL DEFAULT 0,
    applied_delta   bigint      NOT NULL DEFAULT 0,
    last_station    text,
    last_ts         timestamptz,
    error           text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- 增量变更日志：迁移期间默认分区上该月数据的 INSERT/UPDATE/DELETE
CREATE TABLE IF NOT EXISTS migration_delta (
    seq         bigserial PRIMARY KEY,
    month       date        NOT NULL,
    op          text        NOT NULL CHECK (op IN ('insert','update','delete')),
    station_id  text        NOT NULL,
    ts          timestamptz NOT NULL,
    value       integer
);
CREATE INDEX IF NOT EXISTS migration_delta_month_seq
    ON migration_delta (month, seq);

-- ============================================================================
-- 站号接续：监测站换号后，分析员按新站号连续读取换号前后的观测。
-- 父表始终保留各自原始站号；接续关系只影响"查询拼接"与"写入边界"。
-- ============================================================================

-- 接续登记：旧号、新号、UTC 生效时刻。old/new 各自唯一（同一角色不重复）。
CREATE TABLE IF NOT EXISTS station_succession (
    id           bigserial    PRIMARY KEY,
    old_station  text         NOT NULL,
    new_station  text         NOT NULL,
    effective_at timestamptz  NOT NULL,
    created_at   timestamptz  NOT NULL DEFAULT now(),
    CHECK (old_station <> new_station)
);
CREATE UNIQUE INDEX IF NOT EXISTS station_succession_old_uniq
    ON station_succession (old_station);
CREATE UNIQUE INDEX IF NOT EXISTS station_succession_new_uniq
    ON station_succession (new_station);

-- 参与约束：每个站号（无论作为旧号还是新号）只能参与一次接续。
-- 由触发器同步维护；主键是跨列唯一性的硬保证（并发登记也不会越过）。
CREATE TABLE IF NOT EXISTS station_participation (
    station        text    PRIMARY KEY,
    succession_id  bigint  NOT NULL REFERENCES station_succession(id) ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION succession_participation_sync() RETURNS trigger AS $fn$
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
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS station_succession_participation ON station_succession;
CREATE TRIGGER station_succession_participation
    AFTER INSERT OR UPDATE OR DELETE ON station_succession
    FOR EACH ROW EXECUTE FUNCTION succession_participation_sync();

-- 写入边界强制：登记后，任何经父表（或直接写分区）的独立 SQL 新增/更正
-- （含改站号、跨界改时刻）都不得违反接续边界：
--   旧站号不得有 ts >= 生效时刻 的观测；新站号不得有 ts < 生效时刻 的观测。
-- 共享咨询锁与登记事务的排他咨询锁互斥：登记与采集并发时，
-- 要么登记先提交（违规写入随后被拒），要么写入先提交（登记看到违规数据整单拒绝），
-- 不会放行违规行。写入与写入之间都是共享锁，互不阻塞。
CREATE OR REPLACE FUNCTION observations_succession_boundary() RETURNS trigger AS $fn$
DECLARE
    s RECORD;
BEGIN
    PERFORM pg_advisory_xact_lock_shared(730901);
    FOR s IN
        SELECT old_station, new_station, effective_at
        FROM station_succession
        WHERE old_station = NEW.station_id OR new_station = NEW.station_id
    LOOP
        IF NEW.station_id = s.old_station AND NEW.ts >= s.effective_at THEN
            RAISE EXCEPTION '站号接续边界违例：旧站号 % 在生效时刻(UTC) % 及之后不得再写观测（本次 ts=%）',
                s.old_station, s.effective_at, NEW.ts;
        END IF;
        IF NEW.station_id = s.new_station AND NEW.ts < s.effective_at THEN
            RAISE EXCEPTION '站号接续边界违例：新站号 % 在生效时刻(UTC) % 之前不得写观测（本次 ts=%）',
                s.new_station, s.effective_at, NEW.ts;
        END IF;
    END LOOP;
    RETURN NEW;
END
$fn$ LANGUAGE plpgsql;

-- 挂在父表上：自动克隆到现有及未来 ATTACH 的每个分区（含默认分区），
-- 直接写分区与经父表写入同样受约束。删除观测不产生归属问题，不限制 DELETE。
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        WHERE t.tgname = 'observations_succession_boundary'
          AND c.relname = 'observations'
    ) THEN
        CREATE TRIGGER observations_succession_boundary
            BEFORE INSERT OR UPDATE ON observations
            FOR EACH ROW EXECUTE FUNCTION observations_succession_boundary();
    END IF;
END
$$;

-- ============================================================================
-- 演示种子数据：最近 6 个自然月（UTC），8 个站点，每小时一条整数测值。
-- 全部写入默认分区 —— 即"历史观测 initially 都在默认分区"。
-- ============================================================================
INSERT INTO observations (station_id, ts, value)
SELECT 'ST' || lpad(s::text, 3, '0'),
       date_trunc('hour', t),
       (20 + 60 * random())::int
FROM generate_series(1, 8) AS s
CROSS JOIN generate_series(
        date_trunc('month', now()) - interval '5 months',
        now() - interval '1 hour',
        interval '1 hour'
    ) AS t
ON CONFLICT (station_id, ts) DO NOTHING;
