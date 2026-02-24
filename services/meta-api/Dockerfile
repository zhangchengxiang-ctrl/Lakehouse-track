# 此 Dockerfile 已废弃。
# MetaAPI 现已内嵌到 collection 容器中（通过 supervisor 管理 uvicorn 进程）。
# 源码 meta_api.py 仍保留在此目录，由 collection/Dockerfile COPY 使用。
# 如需独立部署，可基于以下命令构建：
#   FROM python:3.11-slim-bookworm
#   RUN pip install --no-cache-dir fastapi==0.115.6 uvicorn==0.34.0 asyncpg==0.30.0
#   COPY meta_api.py /app/meta_api.py
#   WORKDIR /app
#   EXPOSE 3000
#   CMD ["uvicorn", "meta_api:app", "--host", "0.0.0.0", "--port", "3000", "--workers", "2"]
