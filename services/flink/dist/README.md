# StreamPark Flink 挂载目录

StreamPark 需要挂载完整的 Flink 发行版。启动流程：

```bash
# 1. 先启动 Flink
docker compose up -d flink-jobmanager flink-taskmanager

# 2. 创建目录并从 JobManager 拷出 Flink 包
mkdir -p flink/dist/flink
docker cp $(docker compose ps -q flink-jobmanager):/opt/flink/. flink/dist/flink/

# 3. 启动 StreamPark
docker compose --profile streampark up -d streampark
```
