#!/usr/bin/env python3
"""站号归并端到端验证：并发归并、同一快照读取、旧核对点回退。

用法（Compose 环境，需干净环境运行）：
    docker compose down -v && docker compose up -d --build
    python3 scripts/demo.py            # 可选：先跑 PITR 基础演示
    python3 scripts/merge_demo.py      # 本脚本（会强制重做基准备份，旧核对点过期）

环境变量：
    API_URL    默认 http://localhost:8000
    DEMO_PSQL  在源库执行 SQL 的命令前缀
               默认 "docker compose exec -T db psql -U postgres -d water"

验证内容：
  1. 归并登记：幂等、拒改指、拒自环、拒环路、连续更名解析到现行号；
  2. 并发归并：互斥边并发必有一方被拒、三角并发恰好两条成功 —— 不会成环；
  3. 同一快照：样品站号变更与归并登记交错，当前查询只读已提交快照；
  4. 旧核对点：没有映射表的核对点按原站号供数；新核对点按映射规范站号；
     实例切换后旧任务查询被拒，不错标时点。
"""
import concurrent.futures as cf
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("API_URL", "http://localhost:8000")
DEMO_PSQL = os.environ.get(
    "DEMO_PSQL", "docker compose exec -T db psql -U postgres -d water")

FAILED: list[str] = []


# ------------------------------------------------------------------ 工具

def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def psql(sql: str) -> None:
    subprocess.run(shlex.split(DEMO_PSQL) + ["-v", "ON_ERROR_STOP=1", "-c", sql],
                   check=True, capture_output=True, text=True)


def psql_bg(sql: str) -> subprocess.Popen:
    return subprocess.Popen(shlex.split(DEMO_PSQL)
                            + ["-v", "ON_ERROR_STOP=1", "-c", sql],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)


def wait_for(desc: str, fn, timeout: int = 300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = fn()
        if r:
            return r
        time.sleep(2)
    raise SystemExit(f"超时：{desc}")


def check(desc: str, cond: bool) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {desc}")
    if not cond:
        FAILED.append(desc)


def merge(old: str, new: str) -> tuple[int, dict]:
    return api("POST", "/api/station-merges",
               {"old_station_no": old, "new_station_no": new})


def create_checkpoint(name: str) -> None:
    code, body = api("POST", "/api/checkpoints", {"name": name})
    assert code == 201, f"创建核对点 {name} 失败: {code} {body}"
    wait_for(f"核对点 {name} WAL 归档",
             lambda: api("GET", f"/api/checkpoints/{name}")[1].get("status") == "ready")
    print(f"  核对点 {name} 就绪")


def restore_and_wait(cp_name: str) -> dict:
    code, job = api("POST", "/api/recoveries", {"checkpoint": cp_name})
    assert code == 202, f"发起恢复 {cp_name} 失败: {code} {job}"
    jid = job["id"]
    wait_for(f"任务 #{jid} 完成",
             lambda: api("GET", f"/api/recoveries/{jid}")[1].get("status") in
             ("ready", "failed", "cancelled"))
    job = api("GET", f"/api/recoveries/{jid}")[1]
    assert job["status"] == "ready", f"恢复失败: {job.get('error')}\n{job.get('log_tail')}"
    print(f"  任务 #{jid} 就绪：{job['detail']}（as_of_lsn={job.get('as_of_lsn')}）")
    return job


def recovered_samples(job_id: int, **params) -> dict:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    code, body = api("GET", f"/api/recoveries/{job_id}/samples?{qs}")
    assert code == 200, f"查询恢复实例失败: {code} {body}"
    return body


# ------------------------------------------------------------------ 各验证段

def section_basic() -> None:
    print("== 1. 归并登记：幂等 / 拒改指 / 拒自环 / 拒环路 / 链式解析 ==")
    # 自带样品，不依赖其他脚本的数据（ST-07 设备更名 ST-07→ST-07A→ST-07B）
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-MRG-0000', 'ST-07', 7.07)")
    code, r = merge("ST-07", "ST-07A")
    check("登记 ST-07→ST-07A 返回 201", code == 201 and r.get("created") is True)
    code, r = merge("ST-07", "ST-07A")
    check("重复登记同目标幂等返回 200", code == 200 and r.get("created") is False)
    code, _ = merge("ST-07", "ST-07B")
    check("同一旧号改指他站被拒（409）", code == 409)
    code, _ = merge("ST-09", "ST-09")
    check("自环被拒（400）", code == 400)
    code, _ = merge("ST-07A", "ST-07")
    check("直接互指成环被拒（409）", code == 409)
    code, r = merge("ST-07A", "ST-07B")
    check("链式续登 ST-07A→ST-07B 返回 201", code == 201)
    check("登记响应即解析到现行号 ST-07B",
          r.get("canonical_station_no") == "ST-07B")

    rows = api("GET", "/api/station-merges")[1]["rows"]
    by_old = {m["old_station_no"]: m for m in rows}
    check("列表中 ST-07 沿链解析到现行号 ST-07B",
          by_old.get("ST-07", {}).get("canonical_station_no") == "ST-07B")
    check("列表中 ST-07A 解析到 ST-07B",
          by_old.get("ST-07A", {}).get("canonical_station_no") == "ST-07B")

    code, body = api("GET", "/api/samples?station_no=ST-07B&limit=100")
    rows = body["rows"]
    s0 = next((r for r in rows if r["sample_no"] == "S-MRG-0000"), None)
    check("按现行号 ST-07B 取样能取到旧号样品 S-MRG-0000（不被拆散）",
          s0 is not None)
    check("返回同时含原站号 ST-07 与规范站号 ST-07B",
          s0 is not None and s0["station_no"] == "ST-07"
          and s0["canonical_station_no"] == "ST-07B")
    code, body = api("GET", "/api/samples?station_no=ST-07&limit=100")
    check("按旧号 ST-07 过滤同样能命中该样品",
          any(r["sample_no"] == "S-MRG-0000" for r in body["rows"]))
    code, body = api("GET", "/api/samples?station_no=ST-01&limit=100")
    check("未归并站号规范值等于原值",
          all(r["canonical_station_no"] == "ST-01" for r in body["rows"]))


def section_concurrent() -> None:
    print("== 2. 并发归并：相连映射不得成环 ==")

    def post(pair):
        return pair, merge(*pair)

    # 2a. 互斥边并发：每对恰好一条成功、另一条因成环被拒
    pairs = [(f"PX-{i}", f"PY-{i}") for i in range(1, 5)]
    edges = [(a, b) for a, b in pairs] + [(b, a) for a, b in pairs]
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = dict(ex.map(post, edges))
    ok = True
    for a, b in pairs:
        fwd, rev = results[(a, b)][0], results[(b, a)][0]
        ok = ok and sorted([fwd, rev]) == [201, 409]
    check("4 对互指边并发登记：每对恰好一条 201、一条 409", ok)

    # 2b. 三角边并发：恰好两条成功，闭合边被拒
    tri = [("TC-1", "TC-2"), ("TC-2", "TC-3"), ("TC-3", "TC-1")]
    with cf.ThreadPoolExecutor(max_workers=3) as ex:
        tri_results = list(ex.map(post, tri))
    codes = sorted(c for _, (c, _) in tri_results)
    check("三角并发登记恰好两条 201、一条 409（环被拆开）",
          codes == [201, 201, 409])

    # 2c. 同一边并发重复登记：一个 201，其余 200（幂等）
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        dup = list(ex.map(post, [("IDEM-1", "IDEM-2")] * 4))
    codes = sorted(c for _, (c, _) in dup)
    check("同一映射并发登记：恰好一个 201，其余 200 幂等",
          codes == [200, 200, 200, 201])

    rows = api("GET", "/api/station-merges")[1]["rows"]
    by_old = {m["old_station_no"]: m for m in rows}
    check("IDEM-1 只登记一次且解析到 IDEM-2",
          by_old.get("IDEM-1", {}).get("canonical_station_no") == "IDEM-2")
    # 三角中存活的两条边必须仍能解析到链尾（无环）
    alive = [e for e, (c, _) in zip(tri, tri_results) if c == 201]
    term = {"TC-1": None, "TC-2": None, "TC-3": None}
    for a, b in alive:
        term[a] = by_old[a]["canonical_station_no"]
    check("并发后的映射图无环（存活边均有链尾）",
          all(v in ("TC-1", "TC-2", "TC-3") for v in term.values() if v))


def section_snapshot() -> None:
    print("== 3. 样品变更与归并交错：当前查询读同一已提交快照 ==")
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-MRG-0001', 'SN-01', 7.01)")
    code, _ = merge("SN-01", "SN-02")
    assert code == 201
    body = api("GET", "/api/samples?sample_no=S-MRG-0001")[1]["rows"][0]
    check("登记后查询：原站号 SN-01 / 规范站号 SN-02",
          body["station_no"] == "SN-01" and body["canonical_station_no"] == "SN-02")

    # 后台事务同时改样品站号 + 登记新映射，8 秒后才提交
    bg = psql_bg("BEGIN;"
                 " UPDATE samples SET station_no='SN-03'"
                 "  WHERE sample_no='S-MRG-0001';"
                 " INSERT INTO public.station_aliases (old_station_no, new_station_no)"
                 " VALUES ('SN-03', 'SN-08');"
                 " SELECT pg_sleep(8); COMMIT;")
    time.sleep(3)
    body = api("GET", "/api/samples?sample_no=S-MRG-0001")[1]["rows"][0]
    check("事务未提交期间查询仍见旧快照（SN-01 / SN-02）",
          body["station_no"] == "SN-01" and body["canonical_station_no"] == "SN-02")
    bg.wait()
    assert bg.returncode == 0, "后台事务执行失败"
    body = api("GET", "/api/samples?sample_no=S-MRG-0001")[1]["rows"][0]
    check("提交后样品站号与新映射在同一快照生效（SN-03 / SN-08）",
          body["station_no"] == "SN-03" and body["canonical_station_no"] == "SN-08")


def section_old_checkpoint() -> None:
    print("== 4. 旧核对点：无映射表按原站号供数；实例切换不错标时点 ==")
    # 模拟升级前状态：源库没有映射表的时代
    psql("DROP TABLE public.station_aliases")
    code, b = api("POST", "/api/basebackup?force=true")
    assert code == 202, f"强制基准备份失败: {code} {b}"
    bid = b["id"]
    wait_for("强制基准备份完成",
             lambda: (api("GET", "/api/basebackup")[1].get("latest_done") or {})
             .get("id") == bid)
    print("  已模拟升级前状态（无映射表）并重做基准备份")

    create_checkpoint("cp_merge_old")  # 没有映射表时代的核对点

    # 升级后：映射表由登记接口懒建，继续登记归并
    code, _ = merge("ST-07", "ST-07A")
    check("映射表缺失时登记接口懒建表并成功（201）", code == 201)
    code, _ = merge("ST-07A", "ST-07B")
    assert code == 201
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-MRG-0002', 'ST-07', 6.88)")
    create_checkpoint("cp_merge_new")  # 有映射表时代的核对点

    job_old = restore_and_wait("cp_merge_old")
    body = recovered_samples(job_old["id"], limit=1000)
    rows = {r["sample_no"]: r for r in body["rows"]}
    check("旧核对点查询标注 mapping_table=false", body["mapping_table"] is False)
    check("旧核对点按原站号供数（S-MRG-0000 规范站号=原站号 ST-07）",
          rows.get("S-MRG-0000", {}).get("canonical_station_no") == "ST-07")
    check("旧核对点不含核对点后新增的 S-MRG-0002", "S-MRG-0002" not in rows)
    check("旧核对点响应带 as_of_lsn（时点标注）", bool(body.get("as_of_lsn")))
    print(f"  旧核对点 restore_lsn={body['restore_lsn']}"
          f" as_of_lsn={body['as_of_lsn']}")

    job_new = restore_and_wait("cp_merge_new")
    code, _ = api("GET", f"/api/recoveries/{job_old['id']}/samples?limit=10")
    check("实例切换后旧任务查询被拒（409，不错标时点）", code == 409)

    body = recovered_samples(job_new["id"], limit=1000)
    rows = {r["sample_no"]: r for r in body["rows"]}
    check("新核对点查询标注 mapping_table=true", body["mapping_table"] is True)
    check("新核对点中 S-MRG-0000 规范站号沿链解析到 ST-07B",
          rows.get("S-MRG-0000", {}).get("canonical_station_no") == "ST-07B")
    check("新核对点含 S-MRG-0002 且规范站号 ST-07B",
          rows.get("S-MRG-0002", {}).get("canonical_station_no") == "ST-07B")
    check("新核对点 as_of_lsn 与旧核对点不同（实例身份可区分）",
          body.get("as_of_lsn") and body["as_of_lsn"]
          != api("GET", f"/api/recoveries/{job_old['id']}")[1].get("as_of_lsn"))

    body = api("GET", "/api/samples?station_no=ST-07B&limit=100")[1]
    check("当前库按 ST-07B 取样同时命中两条 ST-07 样品",
          sum(1 for r in body["rows"] if r["station_no"] == "ST-07") == 2)


# ------------------------------------------------------------------ 主流程

def main() -> int:
    print("== 0. 等待 API 与基准备份 ==")
    wait_for("API 健康", lambda: api("GET", "/api/health")[1].get("status") == "ok")
    wait_for("基准备份完成",
             lambda: (api("GET", "/api/basebackup")[1].get("latest_done") or {})
             .get("status") == "done")
    print("  基准备份就绪")

    section_basic()
    section_concurrent()
    section_snapshot()
    section_old_checkpoint()

    print()
    if FAILED:
        print(f"共 {len(FAILED)} 项校验失败：")
        for f in FAILED:
            print(f"  - {f}")
        return 1
    print("全部校验通过 ✅")
    print(f"Swagger 文档：{API}/docs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
