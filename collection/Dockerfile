# ============================================================
# Collection Node: Nginx + Vector + MetaSync + Logrotate
# 轻量多节点架构 — 无 Python / 无 Parquet Worker
# Vector 直写 S3 (JSONL gzip)，StarRocks 外表+异步MV 入库
#
# 构建上下文: 项目根目录
# 用法: docker compose up -d --build --scale collection=3
# ============================================================

# ---- Stage 1: Vector 二进制 ----
FROM timberio/vector:0.53.0-alpine AS vector-bin

# ---- Stage 2: 最终镜像 (Debian slim, 无 Python) ----
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx \
    postgresql-client \
    supervisor \
    logrotate \
    curl \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Vector 二进制
COPY --from=vector-bin /usr/local/bin/vector /usr/local/bin/vector

# ---- 配置文件 ----
COPY collection/config/nginx.conf /etc/nginx/nginx.conf
COPY collection/config/sdk_debug /etc/nginx/sdk_debug
COPY collection/config/vector.yaml /etc/vector/vector.yaml
COPY collection/config/geoip /etc/vector/geoip
COPY collection/config/logrotate-nginx.conf /etc/logrotate.d/nginx-tracking
COPY collection/config/supervisord.conf /etc/supervisord.conf

# ---- 脚本 ----
COPY collection/scripts/entrypoint.sh /opt/scripts/entrypoint.sh
COPY collection/scripts/meta_sync.sh /opt/scripts/meta_sync.sh
RUN chmod +x /opt/scripts/entrypoint.sh /opt/scripts/meta_sync.sh

# 数据目录
RUN mkdir -p /var/log/nginx /var/lib/vector /data/meta

EXPOSE 80

ENTRYPOINT ["/opt/scripts/entrypoint.sh"]
