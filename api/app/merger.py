"""站号归并：设备更名后，把旧站号登记为指向现行站号。

本模块是唯一会向生产源库写入的模块，且只写 public.station_aliases 一张表，
样品表绝不改写。映射存于源库，随基准备份与 WAL 归档一起进入 PITR，
因此恢复实例里看到的映射天然就是核对点时点的映射。

一致性保证：
- 登记经 pg_advisory_xact_lock 串行化：并发登记相连映射也绝不会成环；
- 同一旧号一旦登记即不可改指他站（主键兜底 + 冲突检查）；
  重复登记同一目标是幂等操作；
- 连续更名沿链解析到现行号；自环与成环登记一律拒绝；
- 当前查询用单条 SQL（递归 CTE 展开映射 + 读样品），单语句即同一
  已提交快照：样品站号变更与归并登记交错，也不会读到拼接状态。
"""
import psycopg
from psycopg.rows import dict_row

from . import config

# 归并登记的串行化锁（pg_advisory_xact_lock 固定 key，ASCII "STAL"）
_LOCK_KEY = 0x5354414C

STATION_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"


class MergeConflict(Exception):
    """同一旧号已指向其他站，不能改指。"""


class MergeCycle(Exception):
    """登记会形成环路，已拒绝。"""


_DDL = """
CREATE TABLE IF NOT EXISTS public.station_aliases (
    old_station_no text PRIMARY KEY,
    new_station_no text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
)
"""

# 沿链解析：station → 现行号（取链尾；无映射时返回自身）
_RESOLVE_SQL = """
WITH RECURSIVE walk(node, depth) AS (
    SELECT a.new_station_no, 1
    FROM public.station_aliases a
    WHERE a.old_station_no = %s
    UNION
    SELECT a.new_station_no, w.depth + 1
    FROM public.station_aliases a
    JOIN walk w ON a.old_station_no = w.node
    WHERE w.depth < 100
)
SELECT node FROM walk ORDER BY depth DESC LIMIT 1
"""

# 环路检查：登记 old→new 前，从 new 沿既有映射走，若能回到 old 则成环
_CYCLE_SQL = """
WITH RECURSIVE walk(node) AS (
    SELECT a.new_station_no
    FROM public.station_aliases a
    WHERE a.old_station_no = %s
    UNION
    SELECT a.new_station_no
    FROM public.station_aliases a
    JOIN walk w ON a.old_station_no = w.node
)
SELECT 1 FROM walk WHERE node = %s LIMIT 1
"""

# 全量映射 + 各自沿链解析到的现行号
_LIST_SQL = """
WITH RECURSIVE walk(orig, node, depth) AS (
    SELECT a.old_station_no, a.new_station_no, 1
    FROM public.station_aliases a
    UNION
    SELECT w.orig, a.new_station_no, w.depth + 1
    FROM walk w
    JOIN public.station_aliases a ON a.old_station_no = w.node
    WHERE w.depth < 100
),
term AS (
    SELECT DISTINCT ON (orig) orig, node AS canonical_station_no
    FROM walk
    ORDER BY orig, depth DESC
)
SELECT a.old_station_no, a.new_station_no, t.canonical_station_no, a.created_at
FROM public.station_aliases a
JOIN term t ON t.orig = a.old_station_no
ORDER BY a.old_station_no
"""

# 样品查询（含规范站号）。单条语句 = 同一已提交快照：
# 样品与映射一定来自同一时点，交错写入不会造成拼接读。
# station_no 过滤同时命中规范站号与原站号，按站取样不被更名拆散。
_SAMPLES_HEAD = """
WITH RECURSIVE walk(orig, node, depth) AS (
    SELECT a.old_station_no, a.new_station_no, 1
    FROM public.station_aliases a
    UNION
    SELECT w.orig, a.new_station_no, w.depth + 1
    FROM walk w
    JOIN public.station_aliases a ON a.old_station_no = w.node
    WHERE w.depth < 100
),
term AS (
    SELECT DISTINCT ON (orig) orig, node AS canonical_station_no
    FROM walk
    ORDER BY orig, depth DESC
)
SELECT s.id, s.sample_no, s.station_no,
       COALESCE(t.canonical_station_no, s.station_no) AS canonical_station_no,
       s.ph::float8 AS ph, s.created_at
FROM public.samples s
LEFT JOIN term t ON t.orig = s.station_no
"""

# 旧核对点回退：恢复实例里没有映射表时按原站号供数（规范站号 = 原站号）
_SAMPLES_HEAD_NO_ALIAS = """
SELECT s.id, s.sample_no, s.station_no,
       s.station_no AS canonical_station_no,
       s.ph::float8 AS ph, s.created_at
FROM public.samples s
"""

# 恢复实例里映射表是否存在（旧核对点没有该表）
ALIASES_EXIST_SQL = "SELECT to_regclass('public.station_aliases') IS NOT NULL AS ok"


def samples_query(has_aliases: bool, station_no: str | None,
                  sample_no: str | None, limit: int) -> tuple[str, tuple]:
    """拼出样品查询（单条语句）。station_no 命中规范站号或原站号。"""
    sql = _SAMPLES_HEAD if has_aliases else _SAMPLES_HEAD_NO_ALIAS
    where, params = [], []
    if sample_no:
        where.append("s.sample_no = %s")
        params.append(sample_no)
    if station_no:
        if has_aliases:
            where.append("(COALESCE(t.canonical_station_no, s.station_no) = %s"
                         " OR s.station_no = %s)")
            params.extend([station_no, station_no])
        else:
            where.append("s.station_no = %s")
            params.append(station_no)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY s.id LIMIT %s"
    params.append(limit)
    return sql, tuple(params)


def _conn(autocommit: bool = True) -> psycopg.Connection:
    return psycopg.connect(
        host=config.SOURCE_HOST,
        port=config.SOURCE_PORT,
        dbname=config.SOURCE_DB,
        user=config.SOURCE_USER,
        password=config.SOURCE_PASSWORD,
        autocommit=autocommit,
        connect_timeout=5,
        application_name="pitr-api-merge",
        row_factory=dict_row,
    )


def ensure_table() -> None:
    """确保映射表存在（新部署由 01_schema.sql 建好；老部署升级在此兜底）。"""
    with _conn() as con:
        con.execute(_DDL)


def _resolve(con: psycopg.Connection, station_no: str) -> str:
    row = con.execute(_RESOLVE_SQL, (station_no,)).fetchone()
    return row["node"] if row else station_no


def register_merge(old: str, new: str) -> dict:
    """登记 old → new，返回映射及其解析到的现行号。

    - 同旧号同目标：幂等返回 created=False；
    - 同旧号不同目标：MergeConflict（同一旧号不能改指另一站）；
    - 会形成环路：MergeCycle。
    全程持有 advisory xact lock，并发登记相连映射也不会成环。
    """
    with _conn(autocommit=False) as con:
        con.execute("SELECT pg_advisory_xact_lock(%s)", (_LOCK_KEY,))
        con.execute(_DDL)  # 兜底：老部署升级后首次登记时建表
        row = con.execute(
            "SELECT new_station_no FROM public.station_aliases"
            " WHERE old_station_no = %s", (old,),
        ).fetchone()
        if row:
            if row["new_station_no"] == new:
                return {"old_station_no": old, "new_station_no": new,
                        "canonical_station_no": _resolve(con, old),
                        "created": False, "detail": "映射已存在（幂等返回）"}
            raise MergeConflict(
                f"旧站号 {old} 已归并到 {row['new_station_no']}，"
                f"同一旧号不能改指另一站（{new}）"
            )
        if con.execute(_CYCLE_SQL, (new, old)).fetchone():
            raise MergeCycle(f"登记 {old} → {new} 会形成环路，已拒绝")
        con.execute(
            "INSERT INTO public.station_aliases (old_station_no, new_station_no)"
            " VALUES (%s, %s)", (old, new),
        )
        return {"old_station_no": old, "new_station_no": new,
                "canonical_station_no": _resolve(con, old),
                "created": True, "detail": "已登记"}


def list_merges() -> list[dict]:
    """全量映射，含各自沿链解析到的现行号。"""
    with _conn() as con:
        return con.execute(_LIST_SQL).fetchall()


def current_samples(station_no: str | None, sample_no: str | None,
                    limit: int) -> list[dict]:
    """查询当前库样品（原站号 + 规范站号）。

    单条 SQL 语句读取样品与映射：同一已提交快照，
    样品站号变更与归并登记交错也不会读到拼接状态。
    """
    sql, params = samples_query(True, station_no, sample_no, limit)
    with _conn() as con:
        return con.execute(sql, params).fetchall()
