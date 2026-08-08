# AGENTS.md — 项目事实

> 由 Vibe Coding scaffold 显式初始化。只填写本仓可核验的事实、命令和约束。
> 跨仓 SDD 流程以已安装插件 **skills**（入口 `vibe-coding`）为准，不在本文件复制全文。
> 本文件是宿主**唯一**项目事实面（含就绪度、共享资源、本仓例外）。

## SDD

- SDD docs root: docs
- **入口：** 仅显式 SDD 意图（`/vibe-coding`、许愿、确认方案、切 Spec、关版、polish、冷启动、人类验收、生产发布、派 Codex 等）→ 先加载插件 skill **`vibe-coding`**，再读本文件。普通调研 / 排障 / 实现问答 **不要**因此加载。
- 未明示「开始做 / 实现 / 按这个来 / 构建 / 就按这个做 / 确认方案」或「polish / 抛光 / 前端小改 / 按 refinement 修 / 修本批走查」、且无已确认 `<SDD docs root>/specs/<id>/` → 只写 `<SDD docs root>/product/`，**不改业务代码**。Polish 仅覆盖非 material UI；material 仍须 Shape/Spec。
- **许愿路径：** 确认方案后 Agent 编排至人类验收包（片级 Pack+Codex+结构化证伪）；**关版主权在人**。**Build ≠ Deploy**——仅「批准 Build / 确认方案 / 关版」**不得**上生产；生产须 Deploy（P2+P3 → 本轮 P4 → P5 → P6）。
- 无 `AGENTS.md` / SDD 文档树时先跑插件 `scripts/scaffold.sh`；scaffold **不算**编码许可。
- 技能里写的 `docs/product`、`docs/specs` 均相对 **SDD docs root**（默认 `docs`）。
- 关键技术判断未经证据 → `Unverified`；仅 `Verified` 进 P0 / Lock / 可实施宣称。
- **Codex：** Cursor/Claude 说「派 Codex」→ Skill `dispatch-codex`（`codex-dispatch.sh` / `wish-orchestrate.sh`）。**硬门：** 不经 MCP；一次一完成单元；证伪见 `falsify-attestation.md`。CLI 派发已授权执行，施工侧不得再等人批计划。
- 本仓命令、环境、单向门以下文为准。

## 项目

- project.kind: software
- 产品/服务：Track / Lakehouse-track — 埋点全链路 Lakehouse（采集 + 数据湖 + 数据仓库）
- 主要技术栈：Nginx + Vector、Flink 1.20、Apache Paimon、StarRocks 3.5（存算分离）、PostgreSQL、Hive Metastore、MinIO/S3、Docker Compose / Kubernetes
- 代码入口：`services/{collection,flink,hive,meta-api,postgres,starrocks}`、`deploy/k8s/`、根 `docker-compose.yml`、`Makefile`、`scripts/lakehouse.sh`
- 默认分支：main（以 `origin/main` 为准）

## 项目就绪度

| 能力 | 状态 | 负责人 / 入口 |
|---|---|---|
| 本地启动与定向验证 | ready | `make install` → `docker compose up -d --build` → `make run-sql` → `make verify`；说明见根 `README.md` |
| 用户可见页面验收 | n/a | 无产品 UI；查询面为 StarRocks SQL / 外部 BI |
| 日志与监控只读入口 | missing | Vector / Flink / Pod 日志；无统一可观测栈 |
| Preview / Staging | missing | 仅本地 / 实验室命名空间 `lakehouse-track` |
| 部署与回滚 | ready | `kubectl apply -k deploy/k8s`；回滚：`kubectl delete namespace lakehouse-track` 或按清单回退 |

## 环境与入口

| 环境 | URL / 访问方式 | 版本识别 | 日志 / 监控 |
|---|---|---|---|
| Local (Compose) | 采集 `:${COLLECTION_PORT:-8080}`；Flink UI `:8081`；MinIO `:9000/:9001`；StarRocks `:9030`；Postgres `:5432`；HMS `:9083` | 镜像 tag / compose / `.env` | `docker compose logs` |
| Local (K8s lab) | `kubectl … -n lakehouse-track` | 清单 + 镜像 tag | `kubectl logs` |
| Preview / Staging | — | — | — |
| Production | —（未在本仓定义） | — | — |

## 常用命令

```bash
cp .env.example .env          # 可选
make install                  # Flink/StarRocks JAR + GeoIP
docker compose up -d --build
make run-sql                  # Flink CDC + StarRocks DDL（ARGS=flink|starrocks 可拆）
make verify                   # 埋点链路冒烟
make fix                      # 修复 Flink CDC 任务
make replay                   # 重放 test_data

# K8s 实验室
kubectl apply -k deploy/k8s

# StarRocks
mysql -h 127.0.0.1 -P 9030 -u root
```

## 架构与写入边界

> 运行时真源：根 `README.md` + `services/` + `docker-compose.yml`。架构目标态：`docs/architecture/完整方案-可落地版.md`（实现细节与代码冲突时以代码为准）。运维：`docs/ops/`、`Makefile`。

- 进程 / 部署单元：collection（Nginx+Vector+MetaAPI+MetaSync）、Flink JM/TM、PostgreSQL、Hive Metastore、MinIO、StarRocks FE/CN
- 分层或目录约定：`services/*` → `deploy/k8s` → `scripts/` → `docs/{product,specs,architecture,ops}`
- 依赖方向（双链路）：
  - 埋点：SDK → collection → S3(TSV.gz) → StarRocks Pipe
  - CDC：PostgreSQL → Flink CDC → Paimon → StarRocks Paimon Catalog
- 公共 API / schema 真源：`services/collection/config/vector.yaml`、`services/flink/flink.sql`、`services/starrocks/starrocks.sql`、`services/starrocks/starrocks-pipes.sql`、`services/postgres/init/`
- 生成物及生成命令：依赖由 `make install`（`scripts/lakehouse.sh`）；K8s ConfigMap 在 `deploy/k8s/configmap-*.yaml`
- 写入边界 / 只读区域：勿提交密钥、`.env`、`services/flink/lib/*.jar`、`services/starrocks/jars/*.jar`、`services/hive/lib/*.jar`、GeoIP `.mmdb`；`docs/product`/`docs/specs` 仅 SDD 语义
- Default UI surface：n/a
- 产品寄存器：`docs/product/PRODUCT.md`
- 可选设计上下文：无（无 UI）

## 验证

| 变更 | 最低验证 |
|---|---|
| 采集 / Vector | `make verify` 或模拟神策 POST；检查 S3 staging / Pipe |
| Flink / 入湖 | `make run-sql ARGS=flink` + Paimon/MinIO 可见 |
| StarRocks / 数仓 | `make run-sql ARGS=starrocks` + 抽样查询 |
| 发布 | P0–P6；实验室冒烟；禁仅 health |

- 浏览器工具 / 账号：n/a
- CI / 发布门：missing

## 外部共享资源

| 资源 | 冲突规则 | 领取 / 释放方式 |
|---|---|---|
| 本地端口 8080/8081/9000/9001/9030/5432/9083 等 | 单实例占用 | 停 compose / 释放 LB |
| `lakehouse-track` 命名空间 | 实验室独占 | `kubectl delete namespace lakehouse-track` |
| MinIO / Postgres / data 卷 | 数据可清 | `docker compose down -v` 或 `make reset` |

## 单向门

- 事项 / 触发条件：生产集群部署、改 live Vector/Flink Job、清生产湖仓数据
- 批准人或政策：须本轮明示 Deploy / 生产批准（P4）；实验室 `lakehouse-track` 除外
- 风险与回滚入口：见根 `README.md`、`docs/ops/Mac-Docker-K8s运行手册.md`

## Git 与交付事实

- 分支命名规则：未强制（建议 `feat/` `fix/`）
- commit / push / PR：用户明示时才执行
- 部署与回滚授权：实验室可本地执行；生产须 Deploy 轨

## 本仓例外

- 无产品 UI → 验收以 `make verify` / 链路冒烟 + SQL 查询为准。
- 宿主技术文档在 `docs/architecture/`、`docs/ops/`，与 SDD `product`/`specs` 并存；与代码冲突时以 `services/` + `README.md` 为准。
- **范围：** 埋点采集 + 数据湖 + 数据仓库；对账门 / DataSage / Paperclip 等已移除或 parked。
