# SQLMesh CLI + Web UI（实验室）
FROM python:3.12-slim-bookworm

ARG SQLMESH_VERSION=0.236.1

RUN apt-get update \
  && apt-get install -y --no-install-recommends curl \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --no-cache-dir "sqlmesh[web,starrocks]==${SQLMESH_VERSION}"

WORKDIR /project
ENV PYTHONUNBUFFERED=1

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
CMD ["ui"]
