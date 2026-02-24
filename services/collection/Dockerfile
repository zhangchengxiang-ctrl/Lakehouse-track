# ============================================================
# Collection Node: Nginx + Vector + MetaAPI + MetaSync + Logrotate
# 轻量多节点架构 — Vector 直写 S3，StarRocks 外表+异步MV 入库
# MetaAPI 内嵌运行，Vector 通过 localhost 调用注册元数据
#
# 构建上下文: 项目根目录
# 用法: docker compose up -d --build --scale collection=3
# ============================================================

# ---- Stage 1: Vector 二进制 ----
FROM timberio/vector:0.53.0-alpine AS vector-bin

# ---- Stage 2: 最终镜像 ----
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN for i in 1 2 3; do \
      apt-get update && apt-get install -y --no-install-recommends \
        nginx postgresql-client supervisor logrotate curl cron \
        ca-certificates procps python3-pip \
      && break || { echo "Retry $i..."; sleep 5; }; \
    done \
    && pip install --no-cache-dir --break-system-packages \
       fastapi==0.115.6 uvicorn==0.34.0 asyncpg==0.30.0 \
    && rm -rf /var/lib/apt/lists/*

# Vector 二进制
COPY --from=vector-bin /usr/local/bin/vector /usr/local/bin/vector

# ---- 配置文件 ----
COPY services/collection/config/nginx.conf /etc/nginx/nginx.conf
COPY services/collection/config/sdk_debug /etc/nginx/sdk_debug
COPY services/collection/config/vector.yaml /etc/vector/vector.yaml
COPY services/collection/config/ua_parser.vrl /etc/vector/ua_parser.vrl
COPY services/collection/config/geoip /etc/vector/geoip
COPY services/collection/config/logrotate-nginx.conf /etc/logrotate.d/nginx-tracking
COPY services/collection/config/supervisord.conf /etc/supervisord.conf

# ---- Meta API ----
COPY services/meta-api/meta_api.py /app/meta_api.py

# ---- 脚本 ----
COPY services/collection/scripts/entrypoint.sh /opt/scripts/entrypoint.sh
COPY services/collection/scripts/meta_sync.sh /opt/scripts/meta_sync.sh
RUN chmod +x /opt/scripts/entrypoint.sh /opt/scripts/meta_sync.sh

# 数据目录
RUN mkdir -p /var/log/nginx /var/lib/vector /data/meta

EXPOSE 80

ENTRYPOINT ["/opt/scripts/entrypoint.sh"]
