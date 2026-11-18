# 水质数仓单实例 PITR 恢复服务

面向"值班员误删样品后，要取回删前核对时点数据、同时保留生产库后续写入"的场景。
基于 **FastAPI + PostgreSQL 16 + Docker Compose**：

- 生产源库可写样品库（样品号 / 站号 / pH），开启 **WAL 持续归档**；
- API 自动制作一份 **物理基准备份**（`pg_basebackup`，源库不停写）；
- 可随时登记 **命名核对点**（`pg_create_restore_point`）；
- 按核对点 **PITR 恢复到独立只读实例** 并查询样品；
- 恢复结果只含核对点前已提交的数据，**跨点后才提交的事务不会出现**；
- 查询只打恢复实例，**绝不用当前源库冒充恢复结果**；
- WAL 未归档齐备时任务显示等待、不开放目标查询；
- 失败或 API 重启后可按原核对点重试；一次只恢复一项；全程不改写生产库。

另提供 **站号归并**：设备更名导致同一监测站出现多个站号时，登记"旧站号 → 现行站号"
映射（存于源库 `station_aliases` 表），查询样品同时返回**原站号与规范站号**，
样品本身绝不改写；连续更名沿链解析到现行号，同一旧号不可改指他站，
环路（含并发登记相连映射）一律拒绝。

## 快速开始

```bash
docker compose up -d --build        # 启动源库 + API（首次自动做基准备份）
python3 scripts/demo.py             # 端到端演示：更正 / 误删 / 跨点事务 + 恢复校验
python3 scripts/merge_demo.py       # 站号归并验证：并发归并 / 同一快照 / 旧核对点
```

Swagger 文档：<http://localhost:8000/docs>

演示脚本依次验证：

| 场景 | 操作 | 恢复校验 |
| --- | --- | --- |
| 更正 | `UPDATE` 修改 S-2026-0002 的 pH | cp1 中仍是旧值 6.95，cp3 中是新值 7.55 |
| 误删 | `DELETE` 删除 S-2026-0003 | cp1 中查得到，cp3 中已删除 |
| 跨点事务 | 事务在 cp3 前 `INSERT`、cp3 后才 `COMMIT` | cp3 恢复结果中**不含**该行 |
| 源库不受打扰 | 恢复进行期间向源库写入新样品 | 写入成功；恢复实例中也看不到该行（证明没拿源库冒充） |

## 架构

```
                 ┌──────────────────────────── compose ───────────────────────────┐
                 │                                                                  │
  写入/变更 ───▶ │  db (PostgreSQL 16, 可写)                                        │
                 │    archive_mode=on ──WAL 段(原子改名)──▶ [wal_archive 卷]         │
                 │                                          │ (只读挂载)             │
                 │  api (FastAPI + PG16 工具)               ▼                       │
                 │    ① pg_basebackup ──▶ [/backups/base]   核对 WAL 段齐备          │
                 │    ② pg_create_restore_point(核对点) ──▶ 源库（仅 WAL 元数据）    │
                 │    ③ 拷贝备份 + recovery.signal ──▶ [/recoveries/job-N]           │
                 │       restore_command 从归档重放 ─▶ pause@核对点 ─▶ 校验 ─▶ 提升  │
                 │    ④ 只读查询 ◀── 恢复实例 127.0.0.1:55433（独立数据目录）        │
                 │                                                                  │
                 └──────────────────────────────────────────────────────────────────┘
```

## 关键机制与需求对照

| 需求 | 实现 |
| --- | --- |
| 物理基准备份 + 持续归档 | `pg_basebackup -Fp -X stream`（自洽含备份期 WAL）；源库 `archive_command` 先写临时文件再原子 `mv`，API 只会看到完整 WAL 段 |
| 登记命名核对点 | `POST /api/checkpoints` → `pg_create_restore_point` + `pg_switch_wal`（促使该段立即归档） |
| 只含核对点前已提交数据 | `recovery_target_name` + `recovery_target_action=pause`，重放精确停在核对点；跨点后提交的事务不重放 |
| 不用源库冒充恢复 | 查询连接的是 `/recoveries/job-N` 下独立实例（端口 55433），响应中标注实例与 as-of LSN |
| WAL 未齐备显示等待 | 核对点 `waiting_wal` 状态；任务逐段核对"备份起点→核对点"全部 WAL 段，缺段时停留 `waiting_wal` 并列出缺失段 |
| 不把半库报成功 | pause 后校验 `pg_last_wal_replay_lsn() >= 核对点 LSN` 才 promote；再查 `samples`  sanity + `default_transaction_read_only=on` 才置 `ready`；任何异常 → `failed` + 日志 |
| 失败/重启可重试 | 任务状态落 SQLite；`POST /api/recoveries/{id}/retry` 按原点重试；API 重启时进行中任务自动重新排队、ready 实例自动拉起 |
| 一次只恢复一项 | 有进行中任务时 `POST /api/recoveries` 返回 409；新任务就绪前会把旧 ready 实例停掉并标记 `superseded` |
| 源库可写、不被改写 | 基准备份/恢复点均为在线只读操作；API 对源库的唯一写入是 `station_aliases`（站号归并登记），样品表绝不改写；归档卷对 API 只读挂载 |
| 站号归并登记 | `POST /api/station-merges`：旧号 → 现行号；同目标重复登记幂等（200），改指他站 409，自环 400，成环 409；登记经 `pg_advisory_xact_lock` 串行化，**并发登记相连映射也不会成环** |
| 连续更名追到现行号 | 映射成链（A→B、B→C），查询与列表均用递归 CTE 解析到链尾现行号 |
| 不改写样品 | 查询返回 `station_no`（原站号，如库存储）与 `canonical_station_no`（规范站号）两个字段；`station_no` 过滤同时命中规范站号与原站号，按站取样不被更名拆散 |
| 当前查询同一快照 | `GET /api/samples` 用**单条 SQL** 读取样品与映射：单语句即同一已提交快照，样品站号变更与归并登记交错也不会读到拼接状态 |
| 按核对点供数 | 恢复实例的样品与映射表都来自该核对点的 WAL 重放，**只用该核对点的样品和映射**；旧核对点没有映射表时自动回退按原站号供数（响应标注 `mapping_table=false`） |
| 实例切换不错标时点 | 恢复目标名烧在实例配置里，查询前核对 `SHOW recovery_target_name` 与任务的核对点一致，不一致返回 409 而非错标数据；响应附 `as_of_lsn`（恢复停住的 WAL 位点，即核对点 LSN） |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查（源库连通性、已归档 WAL 数） |
| POST | `/api/basebackup?force=` | 制作物理基准备份（默认启动时自动做一次） |
| GET | `/api/basebackup` | 备份状态（起始 LSN、大小、历史） |
| POST | `/api/checkpoints` | 登记命名核对点 `{"name": "cp1"}` |
| GET | `/api/checkpoints` `/api/checkpoints/{name}` | 核对点状态（`waiting_wal`/`ready`/`expired`） |
| POST | `/api/recoveries` | 按核对点发起恢复 `{"checkpoint": "cp1"}`（一次一项） |
| GET | `/api/recoveries` `/api/recoveries/{id}` | 任务状态查询 |
| POST | `/api/recoveries/{id}/retry` | 失败/取消/被取代后按原点重试 |
| DELETE | `/api/recoveries/{id}` | 取消进行中任务 / 停止已就绪实例 |
| GET | `/api/recoveries/{id}/samples` | 查询恢复实例样品（`station_no`/`sample_no` 过滤，仅 `ready` 后开放；含规范站号） |
| POST | `/api/recoveries/{id}/query` | 只读 SQL（演示用，限单条 SELECT，连接强制只读兜底） |
| POST | `/api/station-merges` | 登记站号归并 `{"old_station_no":"ST-07","new_station_no":"ST-07A"}`（201；幂等 200；改指/成环 409；自环 400） |
| GET | `/api/station-merges` | 归并映射列表（含沿链解析到的现行号 `canonical_station_no`） |
| GET | `/api/samples` | 查询当前库样品（原站号 + 规范站号，单语句同一快照；`station_no` 命中规范或原站号） |

### 恢复任务状态机

```
queued → waiting_wal → preparing → recovering → verifying → ready
           (等待WAL归档齐备)                            ↘ failed（可 retry）
ready --(新任务就绪前)--> superseded        ready --(DELETE)--> stopped
进行中 --(DELETE)--> cancelled             API 重启：进行中→重新排队；ready→自动拉起
```

## 手动操作示例（curl）

```bash
# 1. 等基准备份完成
curl -s localhost:8000/api/basebackup | jq .latest_done

# 2. 登记核对点（等 status 变 ready，通常几秒）
curl -s -X POST localhost:8000/api/checkpoints -H 'content-type: application/json' -d '{"name":"cp1"}'
curl -s localhost:8000/api/checkpoints/cp1

# 3. 源库继续变更（更正、误删……）
docker compose exec db psql -U postgres -d water -c "DELETE FROM samples WHERE sample_no='S-2026-0003'"

# 4. 发起恢复并等 ready
curl -s -X POST localhost:8000/api/recoveries -H 'content-type: application/json' -d '{"checkpoint":"cp1"}'
curl -s localhost:8000/api/recoveries/1

# 5. 查询删前时点（S-2026-0003 回来了）
curl -s 'localhost:8000/api/recoveries/1/samples?station_no=ST-02'

# 6. 站号归并：设备更名 ST-07 → ST-07A（后又更名 ST-07B）
curl -s -X POST localhost:8000/api/station-merges -H 'content-type: application/json' \
     -d '{"old_station_no":"ST-07","new_station_no":"ST-07A"}'
curl -s -X POST localhost:8000/api/station-merges -H 'content-type: application/json' \
     -d '{"old_station_no":"ST-07A","new_station_no":"ST-07B"}'
curl -s localhost:8000/api/station-merges          # ST-07 沿链解析到 ST-07B
curl -s 'localhost:8000/api/samples?station_no=ST-07B'   # 按现行号取全站样品
```

## 限制与说明

- 演示环境口令为 `postgres/postgres`（scram-sha-256，复制连接已在 `db/init/02_hba.sh` 放行），生产使用请更换口令并收敛 pg_hba；
- 归档卷只增不减，生产需自行配置 WAL 保留/清理策略（如 `pg_archivecleanup` + 备份周期）；
- 重做基准备份（`force=true`）会使早于新备份起点的核对点过期（`expired`）；
- 恢复实例监听容器内 55433，仅 API 经本机 socket 访问，不对外暴露；
- `/query` 为演示便利，真正兜底是连接级 `default_transaction_read_only=on`，任何写操作都会被数据库拒绝。

## 已验证项

**站号归并（本次新增，PostgreSQL 16.14 同拓扑实测，`scripts/demo.py` + `scripts/merge_demo.py` 全量 PASS）：**

- 归并登记幂等（重复同目标 200）、改指他站 409、自环 400、互指成环 409、链式续登解析到现行号；
- 并发归并：4 对互指边并发每对恰好一成一拒、三角边并发恰好两条成功、同一映射并发恰好一个 201——均不成环；
- 样品站号变更与归并登记在同一事务里交错时，当前查询提交前只见旧快照、提交后两者同一快照生效；
- 模拟升级前（无映射表）的核对点恢复后按原站号供数（`mapping_table=false`），新核对点按映射规范站号供数；
- 实例切换（含带外把端口换成别的恢复实例）时旧任务查询被拒 409，不错标时点；
- API 重启后 `ready` 实例自动拉起、`as_of_lsn` 保留、归并查询不受影响；
- 原 `scripts/demo.py` 回归全部通过（更正/误删/跨点事务/恢复期间源库可写/不拿源库冒充）。

**PITR 基础能力（PostgreSQL 16.10 实测）：**

- 冷启动 → 自动基准备份 → `scripts/demo.py` 全部断言 PASS；
- 归档滞后时核对点与任务停留 `waiting_wal`、目标查询返回 409，归档恢复后任务自动续跑到 `ready`；
- 损坏 WAL 段 → 任务如实 `failed` 并附日志，修复后 `retry` 按原点恢复成功；
- API 优雅重启后 `ready` 实例自动拉起可查；API 被 SIGKILL 于恢复中途（遗留孤儿恢复进程）→ 重启后自动清理并按原点重试至 `ready`；
- 并发第二个恢复请求返回 409；新任务就绪时旧实例被 `superseded`；
- 恢复实例直连写入被数据库拒绝（read-only transaction），`/query` 非 SELECT 与多语句被 400 拦截；
- 强制重做基准备份后旧核对点自动 `expired`，恢复过期核对点返回 409。
