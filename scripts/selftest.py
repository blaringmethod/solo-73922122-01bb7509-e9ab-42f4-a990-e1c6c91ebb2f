#!/usr/bin/env python3
"""
端到端自测（真实 PostgreSQL + 真实 HTTP API + 真实进程崩溃）：

  1. 慢速复制 + 并发直接 SQL 增删改 → 切换后父表内容与期望逐行一致
  2. 一次只迁一个月（并发启动第二个 → 409）
  3. 已完成月份再次迁移 → 409 且数据不变
  4. copying / finalizing 阶段故障注入 → 父表完整可查 → 重试完成
  5. SIGKILL 应用进程（复制进行中）→ 父表仍完整可查可写 → 重启后重试完成
  6. 收尾检查：默认分区无残留、无重复主键、触发器/增量清理干净
  7. 站号接续：登记校验（重复参与/已有数据越界 → 整单拒绝）；
     登记后独立 SQL 增/改（含改站号、跨界改时刻）遵守边界，违规被拒、
     他站可写、已登记关系不受拒写影响；按新号拼接查询 == 直接 SQL 期望
  8. 登记与采集并发交错：违规行与登记不同时放行；登记期间有效写入不中断
  9. 接续关系下的月迁移（含 copying 故障续迁）：复制/切换不把旧号读数
     误归新站，迁移中边界仍强制，挂载分区上触发器克隆继续强制
 10. 应用重启后接续查询与边界仍正确

运行：DATABASE_URL=... python3 scripts/selftest.py
（脚本自己拉起/杀掉 uvicorn 进程；要求 DATABASE_URL 指向已初始化的库）
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8010")
DSN = os.environ.get(
    "DATABASE_URL", "postgresql://envmon@127.0.0.1:5544/envmon"
)
APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
APP_PORT = int(os.environ.get("APP_PORT", "8010"))
STATIONS = [f"ST{i:03d}" for i in range(1, 9)]

_failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""), flush=True)
    if not cond:
        _failures.append(f"{name}: {detail}")


def db():
    return psycopg.connect(DSN, autocommit=True, options="-c timezone=UTC")


def month_range(month: str):
    lo = datetime.fromisoformat(month + "-01").replace(tzinfo=timezone.utc)
    hi = (lo.replace(day=28) + timedelta(days=8)).replace(day=1)
    return lo, hi


def snapshot(month: str) -> dict:
    """父表该月全部内容（ground truth，直接 SQL）。"""
    lo, hi = month_range(month)
    with db() as c:
        rows = c.execute(
            "SELECT station_id, ts, value FROM observations"
            " WHERE ts >= %s AND ts < %s",
            (lo, hi),
        ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def default_count(month: str) -> int:
    lo, hi = month_range(month)
    with db() as c:
        return c.execute(
            "SELECT count(*) FROM observations_default WHERE ts >= %s AND ts < %s",
            (lo, hi),
        ).fetchone()[0]


def run_row(month: str):
    with db() as c:
        return c.execute(
            "SELECT status, copied_rows, applied_delta, error"
            " FROM migration_runs WHERE month = %s::date", (month + "-01",)
        ).fetchone()


def partition_name(month: str) -> str:
    return "observations_" + month.replace("-", "_")


def partition_exists(month: str) -> bool:
    with db() as c:
        return bool(
            c.execute(
                "SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid"
                " WHERE i.inhparent = 'observations'::regclass AND c.relname = %s",
                (partition_name(month),),
            ).fetchone()
        )


def partition_count(month: str) -> int:
    with db() as c:
        return c.execute(
            f"SELECT count(*) FROM {partition_name(month)}"
        ).fetchone()[0]


class ChaosWriter(threading.Thread):
    """独立采集连接：直接 SQL 随机 新增/更正/删除，并维护期望结果。"""

    def __init__(self, month: str, expected: dict):
        super().__init__(daemon=True)
        self.month = month
        self.expected = expected
        self.stop_event = threading.Event()
        self.ops = 0
        self.errors: list[str] = []
        self.lo, self.hi = month_range(month)

    def run(self):
        try:
            conn = psycopg.connect(DSN, autocommit=True,
                                   options="-c timezone=UTC")
        except Exception as e:  # noqa: BLE001
            self.errors.append(f"connect: {e!r}")
            return
        with conn:
            while not self.stop_event.is_set():
                op = random.choices(["insert", "update", "delete"],
                                    weights=[5, 3, 2])[0]
                try:
                    self._do(conn, op)
                    self.ops += 1
                except Exception as e:  # noqa: BLE001
                    self.errors.append(f"{op}: {e!r}")
                time.sleep(random.uniform(0.01, 0.06))

    def _do(self, conn, op: str):
        if op == "insert":
            station = random.choice(STATIONS)
            span = int((self.hi - self.lo).total_seconds())
            ts = self.lo + timedelta(seconds=random.randrange(span))
            value = random.randint(0, 100)
            row = conn.execute(
                "INSERT INTO observations (station_id, ts, value)"
                " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
                " RETURNING station_id, ts, value",
                (station, ts, value),
            ).fetchone()
            if row:
                self.expected[(row[0], row[1])] = row[2]
        elif op == "update":
            row = conn.execute(
                "SELECT station_id, ts FROM observations"
                " WHERE ts >= %s AND ts < %s ORDER BY random() LIMIT 1",
                (self.lo, self.hi),
            ).fetchone()
            if row:
                value = random.randint(0, 100)
                conn.execute(
                    "UPDATE observations SET value = %s"
                    " WHERE station_id = %s AND ts = %s",
                    (value, row[0], row[1]),
                )
                self.expected[(row[0], row[1])] = value
        else:
            row = conn.execute(
                "SELECT station_id, ts FROM observations"
                " WHERE ts >= %s AND ts < %s ORDER BY random() LIMIT 1",
                (self.lo, self.hi),
            ).fetchone()
            if row:
                conn.execute(
                    "DELETE FROM observations WHERE station_id = %s AND ts = %s",
                    (row[0], row[1]),
                )
                self.expected.pop((row[0], row[1]), None)


# ------------------------------------------------------------------ app 进程
_app_proc: subprocess.Popen | None = None


def start_app() -> subprocess.Popen:
    global _app_proc
    logf = open("/tmp/pg-runtime/app.log", "ab")
    _app_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app",
         "--host", "127.0.0.1", "--port", str(APP_PORT)],
        cwd=APP_DIR, stdout=logf, stderr=subprocess.STDOUT,
        env={**os.environ, "DATABASE_URL": DSN},
    )
    wait_healthy()
    return _app_proc


def crash_app():
    """通过调试端点让应用硬退出（os._exit，等价于宕机/SIGKILL 的效果）。"""
    global _app_proc
    try:
        requests.post(f"{BASE}/api/debug/crash", timeout=3)
    except requests.RequestException:
        pass
    if _app_proc:
        try:
            _app_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    _app_proc = None
    # 等端口彻底拒绝连接
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            requests.get(f"{BASE}/api/state", timeout=1)
            time.sleep(0.3)
        except requests.RequestException:
            return
    raise RuntimeError("应用进程未能退出")


def wait_healthy(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{BASE}/api/state", timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise RuntimeError("应用未在预期时间内就绪")


def api_migrate(month: str, **kw) -> requests.Response:
    body = {"month": month, "batch_size": 200, "sleep_ms": 80}
    body.update(kw)
    return requests.post(f"{BASE}/api/migrate", json=body, timeout=10)


def wait_status(month: str, want: set[str], timeout=180) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = run_row(month)
        if row and row[0] in want:
            return row[0]
        time.sleep(0.4)
    raise RuntimeError(f"{month} 等待状态 {want} 超时，当前 {run_row(month)}")


def wait_leave_status(month: str, leave: set[str], timeout=60) -> str:
    """等待状态离开指定集合（用于重试启动后避开上一次残留的 failed）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = run_row(month)
        if row and row[0] not in leave:
            return row[0]
        time.sleep(0.2)
    raise RuntimeError(f"{month} 等待离开状态 {leave} 超时，当前 {run_row(month)}")


def verify_month(month: str, expected: dict, label: str):
    """迁移完成后的一致性校验（全部用直接 SQL 独立验证）。"""
    got = snapshot(month)
    check(f"{label}: 父表内容与期望逐行一致", got == expected,
          f"父表 {len(got)} 行 vs 期望 {len(expected)} 行"
          + ("" if got == expected else
             f"，差异样例 {list((got.items() ^ expected.items()))[:3]}"))
    check(f"{label}: 默认分区无残留", default_count(month) == 0,
          f"剩余 {default_count(month)}")
    check(f"{label}: 月分区已挂载", partition_exists(month))
    if partition_exists(month):
        check(f"{label}: 分区行数 == 父表行数",
              partition_count(month) == len(got),
              f"{partition_count(month)} vs {len(got)}")
    with db() as c:
        dup = c.execute(
            "SELECT count(*) - count(DISTINCT (station_id, ts))"
            " FROM observations WHERE ts >= %s AND ts < %s",
            month_range(month),
        ).fetchone()[0]
        trg = c.execute(
            "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'obs_delta%'"
        ).fetchone()[0]
        delta = c.execute(
            "SELECT count(*) FROM migration_delta WHERE month = %s::date",
            (month + "-01",),
        ).fetchone()[0]
    check(f"{label}: 父表主键无重复", dup == 0, f"重复 {dup}")
    check(f"{label}: 增量触发器已清理", trg == 0, f"剩余 {trg}")
    check(f"{label}: 增量日志已清理", delta == 0, f"剩余 {delta}")
    r = requests.get(f"{BASE}/api/verify", params={"month": month}, timeout=10)
    check(f"{label}: /api/verify 判定一致", r.ok and r.json()["consistent"],
          r.text[:200])


# ---------------------------------------------------------------------- 用例
# ============================================================ 站号接续辅助
def api_succ_register(old: str, new: str, eff) -> requests.Response:
    return requests.post(
        f"{BASE}/api/successions",
        json={"old_station": old, "new_station": new,
              "effective_at": eff if isinstance(eff, str) else eff.isoformat()},
        timeout=15,
    )


def succ_list() -> list[dict]:
    r = requests.get(f"{BASE}/api/successions", timeout=10)
    r.raise_for_status()
    return r.json()["successions"]


def splice_api_map(station: str, month: str):
    r = requests.get(f"{BASE}/api/succession/query",
                     params={"station": station, "month": month, "limit": 5000},
                     timeout=15)
    r.raise_for_status()
    j = r.json()
    return j, {(x["station_id"], datetime.fromisoformat(x["ts"])): x["value"]
               for x in j["rows"]}


def expected_splice_map(old: str, new: str, eff: datetime, month: str) -> dict:
    """直接 SQL 计算拼接查询期望：生效前只取旧号、生效后只取新号。"""
    lo, hi = month_range(month)
    with db() as c:
        rows = c.execute(
            "SELECT station_id, ts, value FROM observations"
            " WHERE ts >= %s AND ts < %s"
            "   AND ((station_id = %s AND ts < %s)"
            "        OR (station_id = %s AND ts >= %s))",
            (lo, hi, old, eff, new, eff),
        ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def check_splice(station: str, month: str, old: str, new: str,
                 eff: datetime, label: str):
    j, got = splice_api_map(station, month)
    want = expected_splice_map(old, new, eff, month)
    check(f"{label}: 拼接查询与直接SQL逐行一致", got == want and j["count"] == len(want),
          f"api {len(got)}/{j['count']} 行 vs sql {len(want)} 行")
    legs_ok = all((ts < eff and s == old) or (ts >= eff and s == new)
                  for (s, ts) in got)
    check(f"{label}: 生效前只取旧号、生效后只取新号", legs_ok)


def try_sql(sql: str, args=()) -> bool:
    """独立连接直接 SQL；返回是否提交成功（被拒则 False）。"""
    try:
        with db() as c:
            c.execute(sql, args)
        return True
    except Exception:  # noqa: BLE001
        return False


def try_ins(station: str, ts: datetime, value: int = 1) -> bool:
    return try_sql(
        "INSERT INTO observations (station_id, ts, value) VALUES (%s, %s, %s)",
        (station, ts, value),
    )


def row_exists(station: str, ts: datetime) -> bool:
    with db() as c:
        return bool(c.execute(
            "SELECT 1 FROM observations WHERE station_id = %s AND ts = %s",
            (station, ts),
        ).fetchone())


# ---------------------------------------------------------------------- 用例
def test_happy_path(month: str):
    print(f"\n== 用例1：慢速复制 + 并发直接SQL增删改（{month}）==", flush=True)
    expected = snapshot(month)
    base_rows = len(expected)
    r = api_migrate(month, batch_size=200, sleep_ms=120)
    check("迁移启动成功", r.ok, r.text[:200])
    # 一次只迁一个月
    r2 = api_migrate(NEXT_MONTHS[1])
    check("迁移进行中再启动另一个月被拒绝(409)", r2.status_code == 409,
          f"rc={r2.status_code}")
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("迁移完成", status == "done", f"status={status} err={run_row(month)}")
    check("并发写入无错误", not writer.errors,
          "; ".join(writer.errors[:3]))
    print(f"  并发写入 {writer.ops} 次；迁移前 {base_rows} 行 → 期望 {len(expected)} 行")
    verify_month(month, expected, "用例1")
    # 幂等：已完成月份再次操作不再搬迁
    before = snapshot(month)
    r3 = api_migrate(month)
    check("已完成月份再次迁移被拒绝(409)", r3.status_code == 409, f"rc={r3.status_code}")
    check("重复操作后数据不变", snapshot(month) == before)


def test_fail_and_retry(month: str, fail_at: str):
    print(f"\n== 用例：{fail_at} 阶段故障注入后重试（{month}）==", flush=True)
    expected = snapshot(month)
    r = api_migrate(month, fail_at=fail_at, sleep_ms=40)
    check("迁移启动成功", r.ok, r.text[:200])
    status = wait_status(month, {"failed", "done"}, timeout=120)
    check("故障注入后状态为 failed", status == "failed", f"status={status}")
    # 父表在失败后仍完整可查
    got = snapshot(month)
    check("失败后父表内容完整不变", got == expected,
          f"{len(got)} vs {len(expected)}")
    check("失败后默认分区仍持有全部数据", default_count(month) == len(expected))
    # 重试（无故障注入）+ 并发写入
    r2 = api_migrate(month, sleep_ms=60)
    check("重试启动成功", r2.ok, r2.text[:200])
    wait_leave_status(month, {"failed"})  # 等 worker 真正接管，避开残留的 failed
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("重试后迁移完成", status == "done", f"status={status}")
    check("重试期间并发写入无错误", not writer.errors,
          "; ".join(writer.errors[:3]))
    verify_month(month, expected, f"重试({fail_at})")


def test_process_kill(month: str):
    print(f"\n== 用例：复制进行中应用进程硬退出（{month}）==", flush=True)
    expected = snapshot(month)
    r = api_migrate(month, batch_size=150, sleep_ms=150)
    check("迁移启动成功", r.ok, r.text[:200])
    # 等复制真正开始
    deadline = time.time() + 60
    while time.time() < deadline:
        row = run_row(month)
        if row and row[1] and row[1] > 0:
            break
        time.sleep(0.2)
    row = run_row(month)
    check("复制已开始（copied_rows>0）", bool(row and row[1] and row[1] > 0),
          f"run={row}")
    crash_app()
    time.sleep(1)
    # 应用已死：父表仍完整可查，且采集写入（直接 SQL）仍可提交
    got = snapshot(month)
    check("进程被杀后父表内容完整", got == expected,
          f"{len(got)} vs {len(expected)}")
    probe = ChaosWriter(month, expected)
    probe.start()
    time.sleep(2)
    probe.stop_event.set()
    probe.join(timeout=10)
    check("应用停机期间直接 SQL 写入正常", probe.ops > 0 and not probe.errors,
          f"ops={probe.ops} err={probe.errors[:2]}")
    # 重启应用，重试迁移
    start_app()
    r2 = api_migrate(month, sleep_ms=60)
    check("重启后重试启动成功", r2.ok, r2.text[:200])
    writer = ChaosWriter(month, expected)
    writer.start()
    status = wait_status(month, {"done", "failed"})
    writer.stop_event.set()
    writer.join(timeout=10)
    check("崩溃恢复后迁移完成", status == "done", f"status={status}")
    verify_month(month, expected, "崩溃恢复")


def test_succession_registration(months: list[str]) -> datetime:
    """登记校验：格式、已有数据越界整单拒绝、每个站号只参与一次。"""
    print("\n== 用例7a：站号接续登记校验 ==", flush=True)
    n0 = len(succ_list())
    r = api_succ_register("ST001", "ST001", "2026-01-01 00:00:00")
    check("旧新站号相同被拒绝(400)", r.status_code == 400, f"rc={r.status_code}")
    r = api_succ_register("ST001", "", "2026-01-01 00:00:00")
    check("空站号被拒绝(400/422)", r.status_code in (400, 422), f"rc={r.status_code}")
    m1 = months[0]
    r = api_succ_register("ST003", "ST090", f"{m1}-15 00:00:00")
    check("旧号在生效后已有观测 → 整单拒绝(409)", r.status_code == 409,
          r.text[:150])
    r = api_succ_register("ST090", "ST004", f"{m1}-15 00:00:00")
    check("新号在生效前已有观测 → 整单拒绝(409)", r.status_code == 409,
          r.text[:150])
    eff = datetime.now(timezone.utc) + timedelta(hours=1)
    r = api_succ_register("ST100", "ST200", eff)
    check("无冲突登记成功(201)", r.status_code == 201, r.text[:200])
    for old, new in [("ST200", "ST300"), ("ST300", "ST100"),
                     ("ST100", "ST300"), ("ST300", "ST200")]:
        r = api_succ_register(old, new, eff)
        check(f"重复参与 {old}→{new} 被拒绝(409)", r.status_code == 409,
              f"rc={r.status_code} {r.text[:80]}")
    lst = succ_list()
    check("被拒绝的登记不留痕迹（整单拒绝，原子）",
          len(lst) == n0 + 1 and lst[-1]["old_station"] == "ST100"
          and lst[-1]["new_station"] == "ST200",
          f"list={lst}")
    return eff


def test_succession_boundary(eff: datetime):
    """登记后：独立 SQL 增/改（含改站号、跨界改时刻）遵守边界。"""
    print("\n== 用例7b：登记后独立SQL写入边界 ==", flush=True)
    h = timedelta(hours=1)
    check("旧号生效前写入 允许", try_ins("ST100", eff - h, 11))
    check("旧号生效后写入 拒绝", not try_ins("ST100", eff + h, 12))
    check("新号生效前写入 拒绝", not try_ins("ST200", eff - h, 13))
    check("新号生效后写入 允许", try_ins("ST200", eff + h, 14))
    check("跨界改时刻(旧号→生效后) 拒绝", not try_sql(
        "UPDATE observations SET ts = %s WHERE station_id = 'ST100' AND ts = %s",
        (eff + 2 * h, eff - h)))
    check("改站号(旧→新, 生效前) 拒绝", not try_sql(
        "UPDATE observations SET station_id = 'ST200'"
        " WHERE station_id = 'ST100' AND ts = %s", (eff - h,)))
    check("新号改时刻到生效前 拒绝", not try_sql(
        "UPDATE observations SET ts = %s WHERE station_id = 'ST200' AND ts = %s",
        (eff - 2 * h, eff + h)))
    check("仅更正测值 允许", try_sql(
        "UPDATE observations SET value = 77"
        " WHERE station_id = 'ST100' AND ts = %s", (eff - h,)))
    check("其他站采集仍可提交", try_ins("ST005", eff + h, 15))
    check("删除观测 允许", try_sql(
        "DELETE FROM observations WHERE station_id = 'ST005' AND ts = %s",
        (eff + h,)))
    # 被拒的行确实没有落库；有效行都在
    with db() as c:
        old_after = c.execute(
            "SELECT count(*) FROM observations"
            " WHERE station_id = 'ST100' AND ts >= %s", (eff,)).fetchone()[0]
        new_before = c.execute(
            "SELECT count(*) FROM observations"
            " WHERE station_id = 'ST200' AND ts < %s", (eff,)).fetchone()[0]
    check("被拒写入未落库（旧号生效后0行、新号生效前0行）",
          old_after == 0 and new_before == 0,
          f"old_after={old_after} new_before={new_before}")
    check("有效行保留", row_exists("ST100", eff - h)
          and row_exists("ST200", eff + h))
    lst = succ_list()
    check("拒绝写入不破坏已登记关系",
          any(s["old_station"] == "ST100" and s["new_station"] == "ST200"
              for s in lst), f"list={lst}")
    # API 定向写入：违规返回 409，合法返回 200
    r = requests.post(f"{BASE}/api/demo/write",
                      json={"op": "insert", "station_id": "ST100",
                            "ts": (eff + 3 * h).isoformat(), "value": 1},
                      timeout=10)
    check("API 定向违规写入返回409", r.status_code == 409,
          f"rc={r.status_code} {r.text[:100]}")
    r = requests.post(f"{BASE}/api/demo/write",
                      json={"op": "insert", "station_id": "ST200",
                            "ts": (eff + 3 * h).isoformat(), "value": 16},
                      timeout=10)
    check("API 定向合法写入返回200", r.ok and r.json().get("value") == 16,
          r.text[:150])
    month = eff.isoformat()[:7]
    check_splice("ST200", month, "ST100", "ST200", eff, "边界用例")


def test_succession_splice_seed_station():
    """种子站换号：整月历史(旧号) + 生效后(新号) 的拼接读取。"""
    print("\n== 用例7c：种子站接续的拼接查询 ==", flush=True)
    eff = datetime.now(timezone.utc) + timedelta(hours=1)
    r = api_succ_register("ST001", "ST009", eff)
    check("种子站登记成功(201)（历史数据均在生效前）", r.status_code == 201,
          r.text[:200])
    check("新号生效后写入 允许", try_ins("ST009", eff + timedelta(minutes=5), 21)
          and try_ins("ST009", eff + timedelta(minutes=10), 22))
    month = eff.isoformat()[:7]
    check_splice("ST009", month, "ST001", "ST009", eff, "按新号查询")
    check_splice("ST001", month, "ST001", "ST009", eff, "按旧号查询(同一连续序列)")
    j, got = splice_api_map("ST009", month)
    check("拼接结果含旧号历史行（换号前观测可按新号读到）",
          any(s == "ST001" for (s, _ts) in got),
          f"rows={len(got)}")


def test_registration_write_interleaving():
    """登记与采集并发：违规行与登记不同时放行；有效写入在登记期间不中断。"""
    print("\n== 用例8：登记与采集并发交错 ==", flush=True)
    # 8a: 登记事务与违规写入并发 —— 最终态必须二选一
    # 偶数轮同时释放；奇数轮登记先行 30ms（覆盖"登记先提交→触发器拒写"分支）
    rounds = 12
    outcomes = {"reg": 0, "ins": 0}
    for i in range(rounds):
        old, new = f"R{i:03d}A", f"R{i:03d}B"
        eff = datetime.now(timezone.utc) + timedelta(hours=1)
        viol_ts = eff + timedelta(hours=1)
        barrier = threading.Barrier(2)
        outcome: dict = {}
        reg_first = (i % 2 == 1)

        def do_reg(o=old, n=new, e=eff, head=reg_first):
            barrier.wait()
            if not head:
                time.sleep(random.uniform(0, 0.01))
            outcome["reg"] = api_succ_register(o, n, e).status_code

        def do_ins(o=old, t=viol_ts, head=reg_first):
            barrier.wait()
            time.sleep(0.08 if head else random.uniform(0, 0.01))
            outcome["ins"] = try_ins(o, t, 1)

        threads = [threading.Thread(target=f) for f in (do_reg, do_ins)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        reg_ok = outcome["reg"] == 201
        ins_ok = outcome["ins"]
        with db() as c:
            succ_n = c.execute(
                "SELECT count(*) FROM station_succession WHERE old_station = %s",
                (old,)).fetchone()[0]
        row_there = row_exists(old, viol_ts)
        # 核心不变式：违规行与接续关系不得同时存在
        check(f"交错{i}: 违规行与登记不同时放行",
              not (row_there and succ_n == 1),
              f"reg={outcome['reg']} ins={ins_ok} row={row_there} succ={succ_n}")
        check(f"交错{i}: 终态自洽（登记成功⇔违规行不存在）",
              (succ_n == 1) == (not row_there),
              f"succ={succ_n} row={row_there}")
        outcomes["reg" if reg_ok else "ins"] += 1
    print(f"  交错结果分布：登记先 {outcomes['reg']} 次 / 写入先 {outcomes['ins']} 次")

    # 8b: 登记期间有效采集写入不中断
    old, new = "CV0OLD", "CV0NEW"
    eff = datetime.now(timezone.utc) + timedelta(hours=1)
    stop = threading.Event()
    errs: list[str] = []
    ops = [0]

    def valid_writer():
        while not stop.is_set():
            ts = eff - timedelta(seconds=random.randint(120, 3600),
                                 milliseconds=random.randint(0, 999))
            try:
                with db() as c:
                    c.execute(
                        "INSERT INTO observations (station_id, ts, value)"
                        " VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (old, ts, random.randint(0, 100)))
                ops[0] += 1
            except Exception as e:  # noqa: BLE001
                errs.append(repr(e))
            time.sleep(0.005)

    t = threading.Thread(target=valid_writer)
    t.start()
    time.sleep(0.3)
    r = api_succ_register(old, new, eff)
    stop.set()
    t.join(timeout=10)
    check("登记期间有效采集写入无错误", not errs, "; ".join(errs[:3]))
    check("登记期间有效采集写入持续提交", ops[0] > 5, f"ops={ops[0]}")
    check("并发有效写入下登记成功(201)", r.status_code == 201,
          f"rc={r.status_code} {r.text[:120]}")


def test_succession_migration(eff: datetime):
    """接续关系下的月迁移：复制/故障续迁/切换不误改站号归属，边界全程强制。"""
    month = eff.isoformat()[:7]
    print(f"\n== 用例9：接续边界下的月迁移（{month}，含失败续迁）==", flush=True)

    def attr_snapshot() -> dict:
        with db() as c:
            return {(r[0], r[1]): r[2] for r in c.execute(
                "SELECT station_id, ts, value FROM observations"
                " WHERE station_id IN ('ST100', 'ST200')")}

    before_attr = attr_snapshot()
    # 1) copying 故障注入 → failed：拼接查询与归属不受影响
    r = api_migrate(month, fail_at="copying", sleep_ms=60)
    check("迁移启动成功", r.ok, r.text[:200])
    status = wait_status(month, {"failed", "done"}, timeout=120)
    check("故障注入后状态 failed", status == "failed", f"status={status}")
    check("失败中断后归属快照不变", attr_snapshot() == before_attr)
    check_splice("ST200", month, "ST100", "ST200", eff, "失败中断后")
    # 2) 无故障续迁（慢速）：迁移进行中边界仍强制、有效写入可提交
    r = api_migrate(month, sleep_ms=150)
    check("续迁启动成功", r.ok, r.text[:200])
    deadline = time.time() + 60
    while time.time() < deadline:
        row = run_row(month)
        if row and row[0] in ("copying", "applying"):
            break
        time.sleep(0.2)
    check("迁移进行中违规写入被拒", not try_ins("ST200", eff - timedelta(hours=1)))
    extra_ts = eff - timedelta(minutes=17, seconds=13)
    extra_ok = try_ins("ST100", extra_ts, 777)
    check("迁移进行中有效写入可提交", extra_ok)
    status = wait_status(month, {"done", "failed"})
    check("续迁完成", status == "done", f"status={status}")
    # 3) 归属与拼接：旧号读数不得误归新站
    want_attr = dict(before_attr)
    if extra_ok:
        want_attr[("ST100", extra_ts)] = 777
    after_attr = attr_snapshot()
    check("复制/切换/续迁后站号归属逐行不变", after_attr == want_attr,
          f"{len(after_attr)} vs {len(want_attr)}"
          + ("" if after_attr == want_attr else
             f" 差异 {list(after_attr.items() ^ want_attr.items())[:3]}"))
    check("默认分区该月无残留", default_count(month) == 0)
    check("月分区已挂载", partition_exists(month))
    check_splice("ST200", month, "ST100", "ST200", eff, "迁移完成后")
    # 4) 挂载分区上的触发器克隆继续强制边界
    check("挂载分区上边界仍强制（新号生效前写入被拒）",
          not try_ins("ST200", eff - timedelta(minutes=1)))
    check("挂载分区上合法写入可提交",
          try_ins("ST200", eff + 2 * timedelta(hours=1), 18))
    check_splice("ST200", month, "ST100", "ST200", eff, "分区写入后")


def test_succession_restart(eff: datetime):
    """应用重启后：接续关系、拼接查询、写入边界保持正确。"""
    month = eff.isoformat()[:7]
    print("\n== 用例10：应用重启后接续仍正确 ==", flush=True)
    crash_app()
    start_app()
    lst = succ_list()
    check("重启后接续关系仍在",
          any(s["old_station"] == "ST100" and s["new_station"] == "ST200"
              for s in lst), f"list={lst}")
    check_splice("ST200", month, "ST100", "ST200", eff, "重启后")
    check("重启后边界仍强制（旧号生效后写入被拒）",
          not try_ins("ST100", eff + 5 * timedelta(hours=1)))
    check("重启后合法写入可提交", try_ins("ST200", eff + 5 * timedelta(hours=1), 19))
    check_splice("ST200", month, "ST100", "ST200", eff, "重启写入后")


def main():
    global NEXT_MONTHS
    random.seed(20261116)
    with db() as c:
        months = [r[0].isoformat()[:7] for r in c.execute(
            "SELECT (date_trunc('month', ts AT TIME ZONE 'UTC'))::date AS m,"
            "       count(*) FROM observations_default"
            " GROUP BY 1 ORDER BY 1"
        )]
    check("种子数据至少 4 个月份", len(months) >= 4, f"months={months}")
    NEXT_MONTHS = months
    m1, m2, m3, m4 = months[:4]

    start_app()
    try:
        r = requests.get(f"{BASE}/api/state", timeout=10)
        check("GET /api/state 可用", r.ok)
        test_happy_path(m1)
        test_fail_and_retry(m2, "copying")
        test_fail_and_retry(m3, "finalizing")
        test_process_kill(m4)
        # 站号接续（在迁移用例之后：ST001 等种子站此后才登记接续）
        eff = test_succession_registration(months)
        test_succession_boundary(eff)
        test_succession_splice_seed_station()
        test_registration_write_interleaving()
        test_succession_migration(eff)
        test_succession_restart(eff)
    finally:
        crash_app()

    print("\n" + "=" * 60)
    if _failures:
        print(f"自测失败 {len(_failures)} 项：")
        for f in _failures:
            print("  -", f)
        sys.exit(1)
    print("全部自测通过 ✓")


if __name__ == "__main__":
    main()
