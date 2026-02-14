# Meta API: 独立轻量服务，接收 Vector HTTP Sink 的元数据注册请求
# 无状态，可多副本部署
FROM python:3.11-slim-bookworm

RUN pip install --no-cache-dir fastapi==0.115.6 uvicorn==0.34.0 asyncpg==0.30.0

COPY meta-api/meta_api.py /app/meta_api.py

WORKDIR /app
EXPOSE 3000

CMD ["uvicorn", "meta_api:app", "--host", "0.0.0.0", "--port", "3000", "--workers", "2", "--log-level", "warning"]
