# Lab overlay — 本机 kind 可跑通

资源相对 base 缩容：CN×1、MinIO×1、collection×1、小 PVC、低 memory requests。
FE 注入 `shared_data` ConfigMap（MinIO in-cluster endpoint）。

```bash
export PATH="$HOME/.local/bin:$PATH"

# 1) 建集群（一次性）
kind create cluster --config deploy/k8s/kind-config.yaml

# 2) 构建并导入 collection 镜像
docker build -t lakehouse-track/collection:latest -f services/collection/Dockerfile .
kind load docker-image lakehouse-track/collection:latest --name lakehouse

# 3) 部署
kubectl apply -k deploy/k8s/overlays/lab

# 4) 观察
kubectl -n lakehouse-track get pods -w

# 5) 探活（kind 已映射 host:9030 → NodePort 30903）
mysql -h 127.0.0.1 -P 9030 -u root -e 'SELECT 1; SHOW FRONTENDS; SHOW COMPUTE NODES;'
```

清理：`kind delete cluster --name lakehouse`
