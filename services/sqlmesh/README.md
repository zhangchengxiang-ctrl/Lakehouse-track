# SQLMesh（本仓独立安装）

不合并 `flowgpt-data-platform`。本目录提供 Docker 镜像；项目在 `projects/warehouse/`。

## 本地 CLI（推荐）

```bash
make sqlmesh-install          # python3.11 venv → .venv-sqlmesh
make sqlmesh-info             # 连通性
make sqlmesh-ui               # http://127.0.0.1:8082
```

## Compose 服务

需已有 StarRocks（`flowgpt-starrocks` 或 `starrocks-fe`）在 `lakehouse-net`：

```bash
docker compose --profile sqlmesh up -d --build sqlmesh
# UI: http://127.0.0.1:8082
```

容器内 `STARROCKS_HOST` 默认 `flowgpt-starrocks`。
