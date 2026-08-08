# Mac / Docker / K8s 运行手册

实验室跑通 Lakehouse-track（神策埋点 + Vector + Paimon + StarRocks）。**命令与目录以仓库根 `README.md`、`Makefile`、`AGENTS.md` 为准**；本文只补操作要点。

---

## 一、前置

- Memory ≥ 12GB（建议 16GB+），CPU ≥ 4，Disk ≥ 60GB SSD
- Docker Desktop / Colima；可选启用 Kubernetes + `kubectl`
- 可选：`mysql-client`（连 StarRocks）

---

## 二、Compose 快速路径

```bash
cp .env.example .env          # 可选
make install
docker compose up -d --build
make run-sql
make verify
```

**StarRocks 二选一（同抢 9030，勿同时起）：**

| 模式 | 命令 | 说明 |
|------|------|------|
| 存算分离 FE+CN | `docker compose up -d starrocks-fe starrocks-cn` | 本仓 Lakehouse 目标态 |
| all-in-one（已收编） | `docker compose --profile allin1 up -d starrocks` | 容器名 `flowgpt-starrocks`，数据卷沿用 flowgpt；DataSage/SQLMesh 共用 |

常用：

| 目标 | 作用 |
|------|------|
| `make fix` | 修复 Flink CDC |
| `make replay` | 重放 `test_data` |
| `make reset` | 清数据并重建 |
| `make bench` | wrk 压测 |

采集默认端口：`${COLLECTION_PORT:-8080}`。StarRocks：`mysql -h 127.0.0.1 -P 9030 -u root`。Flink UI：`:8081`。MinIO Console：`:9001`。

真源路径：

- Vector：`services/collection/config/vector.yaml`
- Flink：`services/flink/flink.sql`
- StarRocks：`services/starrocks/starrocks.sql`、`starrocks-pipes.sql`

---

## 三、K8s 实验室

### 本机 kind（推荐）

工具（已可装到 `~/.local/bin`）：`kubectl`、`kind`。

```bash
export PATH="$HOME/.local/bin:$PATH"

# 建集群
kind create cluster --config deploy/k8s/kind-config.yaml

# 构建 collection 并导入
docker build -t lakehouse-track/collection:latest -f services/collection/Dockerfile .
kind load docker-image lakehouse-track/collection:latest --name lakehouse

# 先把 StarRocks 镜像拉到宿主机再导入（kind 内直拉 Docker Hub 常很慢）
docker pull starrocks/fe-ubuntu:3.5.12
docker pull starrocks/cn-ubuntu:3.5.12
kind load docker-image starrocks/fe-ubuntu:3.5.12 --name lakehouse
kind load docker-image starrocks/cn-ubuntu:3.5.12 --name lakehouse

# 实验室 overlay（CN×1 / MinIO×1 / shared_data ConfigMap / 小 PVC）
kubectl apply -k deploy/k8s/overlays/lab
kubectl -n lakehouse-track get pods -w
```

宿主机端口映射（见 `deploy/k8s/kind-config.yaml`）：

| 服务 | 宿主机 |
|------|--------|
| StarRocks MySQL | `127.0.0.1:9030` |
| Collection HTTP | `127.0.0.1:18080` |
| MinIO API / Console | `127.0.0.1:19000` / `19001` |

说明见 [`deploy/k8s/overlays/lab/README.md`](../deploy/k8s/overlays/lab/README.md)。

### 通用 apply（非 lab）

```bash
make install
kubectl apply -k deploy/k8s/base   # 生产规格；本机内存/磁盘不够勿直接用
kubectl get pods -n lakehouse-track -w
```

命名空间：`lakehouse-track`。清理：`kind delete cluster --name lakehouse` 或 `kubectl delete namespace lakehouse-track`。

---

## 四、双链路核对

1. **埋点**：SDK/HTTP → collection → S3(TSV.gz) → StarRocks Pipe → `make verify`
2. **CDC**：PostgreSQL → Flink CDC → Paimon → StarRocks Paimon Catalog → `make run-sql ARGS=flink`

日志：`docker compose logs -f collection` / `flink-jobmanager`；K8s 用 `kubectl logs -n lakehouse-track …`。

---

*与 `origin/main` services 布局对齐；若与代码冲突，以 README / AGENTS 为准。*
