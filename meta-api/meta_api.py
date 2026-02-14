"""
元数据自动注册 API (v2 - 批量优化)
独立部署或运行在 Collection 容器内
接收 Vector HTTP Sink 发来的 NDJSON 事件，批量注册新项目/事件/属性到 PostgreSQL

v2 改进：
  - 连接池 max_size 3 → 20，支持多 Collection 节点并发
  - 批量 SQL：N 条事件只需 ~6 次 SQL（而非 N×M 次）
  - 使用 executemany 和 ANY() 数组查询
"""

import os
import json
import logging
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request, Response

logging.basicConfig(level=logging.INFO, format="[meta-api] %(asctime)s %(message)s")
logger = logging.getLogger(__name__)

PG_DSN = os.getenv(
    "PG_URI",
    "postgresql://{user}:{password}@{host}:{port}/{db}".format(
        user=os.getenv("PG_USER", "paimon"),
        password=os.getenv("PG_PASSWORD", "paimon123"),
        host=os.getenv("PG_HOST", "postgres"),
        port=os.getenv("PG_PORT", "5432"),
        db=os.getenv("PG_DB", "paimon_db"),
    ),
)

pool: asyncpg.Pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(
        PG_DSN,
        min_size=2,
        max_size=int(os.getenv("PG_POOL_MAX", "20")),
    )
    logger.info("连接池已创建 (max=%s): %s", os.getenv("PG_POOL_MAX", "20"), PG_DSN.split("@")[-1])
    yield
    await pool.close()


app = FastAPI(lifespan=lifespan)


# ---------- 批量 SQL 模板 ----------

# 批量 upsert 项目（使用 unnest 一次写入多行）
SQL_BATCH_UPSERT_PROJECTS = """
INSERT INTO user_track.project (name, is_auto_create, status, create_time)
SELECT unnest($1::text[]), 1, 1, NOW()
ON CONFLICT (name) DO NOTHING;
"""

SQL_BATCH_GET_PROJECT_IDS = """
SELECT id, name FROM user_track.project WHERE name = ANY($1::text[]) AND status = 1;
"""

# 批量 upsert 事件
SQL_BATCH_UPSERT_EVENTS = """
INSERT INTO user_track.event_define (project_id, name, accepted, create_time, update_time)
SELECT unnest($1::int[]), unnest($2::text[]), 1, NOW(), NOW()
ON CONFLICT (project_id, name) DO NOTHING;
"""

SQL_BATCH_GET_EVENT_IDS = """
SELECT id, project_id, name FROM user_track.event_define
WHERE (project_id, name) IN (SELECT unnest($1::int[]), unnest($2::text[]));
"""

# 批量 upsert 属性
SQL_BATCH_UPSERT_PROPERTIES = """
INSERT INTO user_track.property_define (project_id, name, data_type, is_in_use, is_load, create_time, update_time)
SELECT unnest($1::int[]), unnest($2::text[]), unnest($3::int[]), 1, 1, NOW(), NOW()
ON CONFLICT (name, project_id) DO NOTHING;
"""

SQL_BATCH_GET_PROPERTY_IDS = """
SELECT id, project_id, name FROM user_track.property_define
WHERE (project_id, name) IN (SELECT unnest($1::int[]), unnest($2::text[]));
"""

# 批量 upsert 事件-属性关联
SQL_BATCH_UPSERT_EVENT_PROPERTY = """
INSERT INTO user_track.event_property (event_id, property_id, update_time)
SELECT unnest($1::int[]), unnest($2::int[]), NOW()
ON CONFLICT (event_id, property_id) DO NOTHING;
"""

# 批量 upsert user_id_mapping
SQL_BATCH_UPSERT_USER_ID_MAPPING = """
INSERT INTO user_track.user_id_mapping (project_name, distinct_id, map_id, map_type, login_id, load_time)
SELECT unnest($1::text[]), unnest($2::text[]), unnest($3::text[]), unnest($4::text[]), unnest($5::text[]), NOW()
ON CONFLICT (project_name, distinct_id) DO NOTHING;
"""


def infer_data_type(value) -> int:
    """推断属性数据类型
    1=数值, 2=字符串, 3=数组, 4=对象, 5=日期, 6=布尔
    """
    if isinstance(value, bool):
        return 6
    if isinstance(value, (int, float)):
        return 1
    if isinstance(value, list):
        return 3
    if isinstance(value, dict):
        return 4
    if isinstance(value, str):
        s = value.strip()
        if s.lower() in ("true", "false", "yes", "no", "on", "off", "1", "0"):
            return 6
        if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
            return 5
        try:
            float(s)
            return 1
        except ValueError:
            pass
        return 2
    return 2


@app.post("/register")
async def register_metadata(request: Request):
    """接收 Vector HTTP Sink 的 NDJSON 批次 — 批量注册"""
    body = await request.body()
    events = []
    for line in body.decode("utf-8", errors="replace").strip().split("\n"):
        line = line.strip()
        if line:
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    if not events:
        return {"ok": True, "registered": 0}

    # ---- Phase 1: 收集所有唯一的 project / event / property ----
    unique_projects = set()
    event_tuples = set()          # (project_name, event_name)
    prop_tuples = []              # (project_name, prop_key, data_type)
    prop_tuples_dedup = set()
    user_mappings = []            # (project_name, distinct_id, map_id, map_type, login_id)
    event_prop_links = []         # (project_name, event_name, prop_key)

    for evt in events:
        project_name = evt.get("project", "")
        event_name = evt.get("event", "")
        event_type = evt.get("type", "")
        properties_keys = evt.get("properties_keys") or []
        properties_raw = evt.get("properties")

        if not project_name:
            continue

        unique_projects.add(project_name)

        if event_name:
            event_tuples.add((project_name, event_name))

        # 解析 properties
        props_dict = {}
        if isinstance(properties_raw, str):
            try:
                props_dict = json.loads(properties_raw)
            except (json.JSONDecodeError, TypeError):
                pass
        elif isinstance(properties_raw, dict):
            props_dict = properties_raw

        for key in properties_keys:
            if not key or not isinstance(key, str):
                continue
            dedup_key = (project_name, key)
            if dedup_key not in prop_tuples_dedup:
                prop_tuples_dedup.add(dedup_key)
                value = props_dict.get(key)
                prop_tuples.append((project_name, key, infer_data_type(value)))
            if event_name:
                event_prop_links.append((project_name, event_name, key))

        # user_id_mapping
        if event_type in ("track_signup", "track_id_bind"):
            distinct_id = evt.get("distinct_id", "")
            login_id = evt.get("login_id", "")
            original_id = evt.get("original_id", "")
            if distinct_id:
                if event_type == "track_signup" and original_id:
                    map_id = distinct_id if login_id else distinct_id
                    map_type = "login_id" if login_id and distinct_id == login_id else "anonymous_id"
                    user_mappings.append((project_name, original_id, map_id, map_type, login_id or ""))
                elif login_id and login_id != "-1":
                    user_mappings.append((project_name, distinct_id, login_id, "login_id", login_id))

    # ---- Phase 2: 批量 SQL ----
    async with pool.acquire() as conn:
        # 2a) 批量 upsert + 获取 project IDs
        project_names = list(unique_projects)
        if project_names:
            await conn.execute(SQL_BATCH_UPSERT_PROJECTS, project_names)
            rows = await conn.fetch(SQL_BATCH_GET_PROJECT_IDS, project_names)
            pid_map = {r["name"]: r["id"] for r in rows}
        else:
            pid_map = {}

        # 2b) 批量 upsert + 获取 event IDs
        if event_tuples:
            e_pids = [pid_map.get(pn) for pn, en in event_tuples]
            e_names = [en for pn, en in event_tuples]
            # 过滤掉 project 未找到的
            valid = [(p, n) for p, n in zip(e_pids, e_names) if p is not None]
            if valid:
                await conn.execute(SQL_BATCH_UPSERT_EVENTS,
                                   [p for p, n in valid],
                                   [n for p, n in valid])
                rows = await conn.fetch(SQL_BATCH_GET_EVENT_IDS,
                                        [p for p, n in valid],
                                        [n for p, n in valid])
                eid_map = {(r["project_id"], r["name"]): r["id"] for r in rows}
            else:
                eid_map = {}
        else:
            eid_map = {}

        # 2c) 批量 upsert + 获取 property IDs
        if prop_tuples:
            p_pids = [pid_map.get(pn) for pn, key, dt in prop_tuples]
            p_names = [key for pn, key, dt in prop_tuples]
            p_types = [dt for pn, key, dt in prop_tuples]
            valid_idx = [i for i, p in enumerate(p_pids) if p is not None]
            if valid_idx:
                await conn.execute(SQL_BATCH_UPSERT_PROPERTIES,
                                   [p_pids[i] for i in valid_idx],
                                   [p_names[i] for i in valid_idx],
                                   [p_types[i] for i in valid_idx])
                rows = await conn.fetch(SQL_BATCH_GET_PROPERTY_IDS,
                                        [p_pids[i] for i in valid_idx],
                                        [p_names[i] for i in valid_idx])
                propid_map = {(r["project_id"], r["name"]): r["id"] for r in rows}
            else:
                propid_map = {}
        else:
            propid_map = {}

        # 2d) 批量 upsert 事件-属性关联
        if event_prop_links:
            ep_eids = []
            ep_pids = []
            seen = set()
            for pn, en, key in event_prop_links:
                pid = pid_map.get(pn)
                if pid is None:
                    continue
                eid = eid_map.get((pid, en))
                prop_id = propid_map.get((pid, key))
                if eid is not None and prop_id is not None:
                    pair = (eid, prop_id)
                    if pair not in seen:
                        seen.add(pair)
                        ep_eids.append(eid)
                        ep_pids.append(prop_id)
            if ep_eids:
                await conn.execute(SQL_BATCH_UPSERT_EVENT_PROPERTY, ep_eids, ep_pids)

        # 2e) 批量 upsert user_id_mapping
        if user_mappings:
            await conn.execute(SQL_BATCH_UPSERT_USER_ID_MAPPING,
                               [m[0] for m in user_mappings],
                               [m[1] for m in user_mappings],
                               [m[2] for m in user_mappings],
                               [m[3] for m in user_mappings],
                               [m[4] for m in user_mappings])

    registered = len(events)
    logger.info("批量注册完成: %d 条事件, %d 项目, %d 事件定义, %d 属性",
                registered, len(unique_projects), len(event_tuples), len(prop_tuples))
    return {"ok": True, "registered": registered}


@app.get("/health")
async def health():
    return {"status": "ok"}
