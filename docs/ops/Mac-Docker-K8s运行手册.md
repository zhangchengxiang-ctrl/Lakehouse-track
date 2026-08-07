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

```bash
make install
kubectl apply -k deploy/k8s
kubectl get pods -n lakehouse-track -w
```

命名空间：`lakehouse-track`。清单在 `deploy/k8s/`（collection Deployment/HPA、Postgres/MinIO StatefulSet、StarRocks FE/CN、ConfigMap/Secret）。

清理：

```bash
kubectl delete namespace lakehouse-track
```

---

## 四、双链路核对

1. **埋点**：SDK/HTTP → collection → S3(TSV.gz) → StarRocks Pipe → `make verify`
2. **CDC**：PostgreSQL → Flink CDC → Paimon → StarRocks Paimon Catalog → `make run-sql ARGS=flink`

日志：`docker compose logs -f collection` / `flink-jobmanager`；K8s 用 `kubectl logs -n lakehouse-track …`。

---

*与 `origin/main` services 布局对齐；若与代码冲突，以 README / AGENTS 为准。*
