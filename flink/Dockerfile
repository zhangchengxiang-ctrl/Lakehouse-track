# Flink 自定义镜像：在原有 lib 基础上增加 flink/lib 的 JAR
# 避免 bind mount 覆盖整个 lib 导致丢失 flink-dist
# 构建前需执行: bash flink/scripts/download-jars.sh

FROM flink:1.20.3-scala_2.12

# 将自定义 JAR 加入 lib（ADD 而非覆盖）
COPY --chown=flink:flink flink/lib/*.jar /opt/flink/lib/

# 配置 baked 进镜像，避免 bind mount 触发 Read-only file system
COPY flink/config/flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# SQL 脚本（重置后执行 -f /opt/flink/flink.sql）
COPY flink/flink.sql /opt/flink/flink.sql
