# 环境监测数仓 · 默认分区 → 月分区在线迁移演示 + 站号接续

把 `observations` 父表**默认分区**中的历史观测，按自然月（UTC）在线搬迁到新建的月分区。
复制期间采集链路（独立连接）对该月的**新增 / 测值更正 / 删除**不中断；
仅最终切换瞬间短暂锁表；失败或进程崩溃后可断点续迁；已完成月份重复操作不再搬迁。

监测站**换号**时可在网页登记**站号接续**（旧号 → 新号 + UTC 生效时刻）：
父表始终保留各自原始站号，分析员按**新站号 + UTC 月份**即可拼接连贯读取
（生效前只取旧号、生效后只取新号）；登记后任何独立 SQL 写入同样受边界约束。

## 数据模型

```sql
observations (station_id text, ts timestamptz, value integer,
              PRIMARY KEY (station_id, ts)) PARTITION BY RANGE (ts);
observations_default  -- 初始唯一分区（DEFAULT），历史数据都在这

station_succession (id, old_station, new_station, effective_at, created_at)
  -- 站号接续登记：旧号/新号/UTC 生效时刻；old、new 各自唯一
station_participation (station PK, succession_id)
  -- 由触发器维护：每个站号（无论新旧角色）只能参与一次接续的硬约束
```

元数据表：`migration_runs`（每月迁移状态机 + 进度断点）、
`migration_delta`（迁移期间触发器捕获的增量变更）。

## 站号接续规则

- **登记**：`POST /api/successions {old_station, new_station, effective_at}`。
  - 每个站号只能参与一次接续（`station_participation` 主键硬保证，含跨新旧两列）；
  - 若已有**旧号在生效时刻后**或**新号在生效时刻前**的观测，整单拒绝（409，原子回滚）。
- **写入边界**：登记后，父表上的 BEFORE 行级触发器（自动克隆到现有及未来
  ATTACH 的每个分区）强制：旧号不得写入 `ts >= 生效时刻`、新号不得写入
  `ts < 生效时刻` 的观测——独立 SQL 的新增/更正（含改站号、跨界改时刻）同样被拒；
  被拒写入只回滚自身，不影响已登记关系；其他站点的采集写入不受影响。
- **并发**：登记事务持排他咨询锁（`pg_advisory_xact_lock(730901)`），
  观测写入触发器持同一键的共享锁（写入之间互不阻塞）。因此登记与采集并发时：
  写入先提交则登记看到违规数据整单拒绝；登记先提交则违规写入被触发器拒绝——
  不会两边同时放行。
- **拼接查询**：`GET /api/succession/query?station=<新号>&month=YYYY-MM`，
  生效前取旧号、生效后取新号，父表原始站号原样返回（查询侧拼接，不改数据）。
- **迁移兼容**：月迁移只按原样复制行（station_id 从不改写），复制/切换/失败续迁
  不会把旧号读数误归新站；ATTACH 时边界触发器自动克隆到新分区继续强制；
  接续关系存于数据库，应用重启后查询与边界仍正确。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`db` 服务首次启动会执行 `db/init.sql`：建父表/默认分区/元数据表/接续关系表，
并写入最近 6 个自然月 × 8 站点 × 每小时一条的整数测值（约 3 万行，全部在默认分区）。

### 本地（无 docker）

需要本机有 PostgreSQL 16 服务端二进制与 Python 3.11+，然后：

```bash
pip install -r app/requirements.txt
PGHOME=/path/to/pgsql PY=python3 bash scripts/dev_up.sh   # 初始化DB+种子+启动应用
```

## 网页操作

1. **月份总览**：各月在默认分区的行数、已挂载月分区、迁移状态。
2. **迁移控制**：选月份 → 设批大小/批间休眠（慢复制）→ 开始迁移；进度条实时刷新。
   一次只迁一个月（进行中再发起会收到 409）。
3. **采集写入演示**：对选定月份手动或自动（可调间隔）做直接 SQL 增/改/删——
   每次都用**独立连接**写父表，模拟真实采集链路；也可用"定向"行显式指定
   站号/时刻/测值（update 可再改站号与时刻），验证接续边界拒绝。
4. **父表查询**：迁移任意时刻查询父表，行数、主键重复数（恒为 0）、样本行。
5. **一致性校验**：父表行数 / 默认分区剩余（完成后为 0）/ 月分区行数三方核对。
6. **站号接续**：登记旧号/新号/UTC 生效时刻（重复参与或已有数据越界会被
   整单拒绝）；按新号（或旧号）+ UTC 月份查询拼接结果，逐行标注原始站号与接续段。

迁移流程（`app/migrator.py`）：

| 阶段 | 做什么 | 是否锁写 |
|---|---|---|
| preparing | 建带 CHECK 的影子表、默认分区上挂增量触发器、登记运行记录 | 仅 DDL 瞬间 |
| waiting | 等触发器创建前开启的老事务结束（轮询 pg_stat_activity） | 否 |
| copying | 键集分页分批 `INSERT..ON CONFLICT` 复制，进度落库，可 sleep 放慢 | 否 |
| applying | 按 seq 顺序回放 `migration_delta`（insert/update→upsert，delete→删） | 否 |
| finalizing | **单事务**：SHARE 锁分区树 → 回放剩余增量 → 摘触发器 → 搬漏网行 → `ATTACH PARTITION` → 清理 → 置 done | **仅此短暂** |

正确性要点：

- 影子表在 `ATTACH` 前不属于父表，父表查询任何时刻都只见到一份数据——不重复、不缺失；
- 复制期间的所有变更由触发器落 `migration_delta`，回放幂等，可重复执行；
- 最终切换是**一个事务**，要么全部生效要么整体回滚，崩溃不会留下半吊子状态；
- 任意时刻进程死亡：影子表/增量/进度都在库里，重试从断点继续（影子表不清空、复制按
  最后主键续传、增量继续累积）；
- `migration_runs.status='done'` 的月份再次迁移直接拒绝（409），数据不变。

## 自测

```bash
# 需要已初始化的库；脚本自己拉起/崩溃/重启应用进程
DATABASE_URL=postgresql://envmon@127.0.0.1:5544/envmon python3 scripts/selftest.py
```

覆盖：慢复制+并发直接 SQL 增删改后父表逐行一致；一次只迁一个月；已完成月份幂等；
copying/finalizing 故障注入后重试成功；复制进行中进程硬退出（`os._exit`）后
父表仍完整可查可写、重启后续迁完成；触发器/增量/默认分区残留清理检查；
站号接续的登记校验（重复参与/已有数据越界整单拒绝）、登记后独立 SQL 边界
（含改站号、跨界改时刻）、拒写不破坏已登记关系、他站写入不受影响、
拼接查询与直接 SQL 逐行一致、登记与采集并发交错不同时放行、
接续关系下的月迁移（含失败续迁）归属不变、应用重启后接续仍正确。

## API 摘要

- `GET  /api/state` 月份总览与迁移进度
- `POST /api/migrate` `{month, batch_size, sleep_ms, fail_at}` 启动迁移（409=进行中/已完成）
- `GET  /api/query?month=` 父表查询（行数/重复数/样本）
- `POST /api/demo/write` `{op, month}` 或 `{op, station_id, ts, value, new_station_id, new_ts}`
  独立连接直接 SQL 增/改/删（随机或定向；违反接续边界返回 409）
- `POST /api/demo/auto` `{enabled, month, interval_ms}` 自动采集写入开关
- `GET  /api/verify?month=` 一致性校验
- `GET  /api/successions` 接续关系列表
- `POST /api/successions` `{old_station, new_station, effective_at}` 登记接续
  （201=成功；400=参数非法；409=重复参与或已有数据越界，整单拒绝）
- `GET  /api/succession/query?station=&month=` 按站号拼接查询
  （参与接续则生效前取旧号、生效后取新号，否则普通查询）
- `POST /api/debug/crash` 进程硬退出（仅自测用）

## 已知边界（如实说明）

- 单应用进程内用互斥锁保证"一次只迁一个月"；多副本部署需改用数据库咨询锁。
- `ATTACH PARTITION` 需对父表短暂 `ACCESS EXCLUSIVE`，并会顺序扫描默认分区
  校验无越界行（演示数据量下为毫秒级）；默认分区极大时该扫描会变长，
  生产上可先把数据搬空再挂载以缩短。
- `CREATE TRIGGER` 与 `ATTACH` 等 DDL 本身需要瞬间锁，无法完全避免；
  除此之外复制/回放阶段完全不锁写。
- 种子数据与月份边界均按 UTC；父表查询按 `ts` 的 UTC 自然月切分。
- 接续登记的"已有数据越界"校验要顺序扫描父表中相关站号的行（按
  `(station_id, ts)` 主键索引走索引范围扫描）；超大表上该校验耗时随数据量增长。
- 每条观测写入会在触发器内取一次共享咨询锁并查一次接续表（索引查找）；
  写入之间不互斥，但与登记事务互斥（登记通常毫秒级）。
- 接续关系一经登记不提供删除接口（演示范围）；删除需直接 SQL 操作
  `station_succession`（参与约束由触发器同步清理）。
