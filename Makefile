# ============================================================
# Lakehouse-track 统一入口
# 用法：make <目标> [ARGS="..."]
#
# 目标：
#   install              安装所有依赖（Flink JAR、GeoIP、StarRocks JAR、配置校验）
#   fix                 修复 Flink CDC（取消任务、重启、重新执行 flink.sql）
#   verify              验证埋点采集链路（Nginx→Vector→S3→StarRocks Pipe）
#   replay              重放 test_data 中的神策日志
#   reset               清除数据并重建（含 flink.sql、starrocks.sql）
#   run-sql             执行 SQL（无 ARGS 时执行 flink + starrocks）
#   download-starrocks-jars  仅下载 StarRocks 外部目录依赖
#   bench               压力测试（wrk 渐进式 100→5K 并发）
#   scale-cn            StarRocks CN 弹性伸缩
#   lifecycle-cleanup   按 event_group 清理过期事件数据
#   help                显示帮助（默认目标）
#
# 示例：
#   make install
#   make replay
#   make run-sql ARGS=flink
#   make bench
#   make scale-cn ARGS=4
# ============================================================

# 项目根目录与脚本路径（与 Makefile 同目录）
ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
LAKEHOUSE_SH := $(ROOT_DIR)scripts/lakehouse.sh

.PHONY: help install fix verify replay reset run-sql download-starrocks-jars bench scale-cn lifecycle-cleanup

.DEFAULT_GOAL := help

help:
	@echo "用法: make <目标> [ARGS=\"...\"]"
	@echo ""
	@echo "目标:"
	@echo "  install                安装所有依赖（Flink JAR、GeoIP、StarRocks JAR）"
	@echo "  fix                    修复 Flink CDC（取消任务、重启、重新执行 flink.sql）"
	@echo "  verify                 验证埋点采集链路（Nginx→Vector→S3→StarRocks Pipe）"
	@echo "  replay                 重放 test_data 中的神策日志"
	@echo "  reset                  清除数据并重建（含 flink.sql、starrocks.sql）"
	@echo "  run-sql [ARGS=flink|starrocks]  执行 SQL（无 ARGS 时执行 flink + starrocks）"
	@echo "  download-starrocks-jars  仅下载 StarRocks 外部目录依赖"
	@echo "  bench                  压力测试（wrk 渐进式 100→10K 并发）"
	@echo "  scale-cn ARGS=N        StarRocks CN 弹性伸缩到 N 个节点"
	@echo "  lifecycle-cleanup      清理过期事件（DEBUG>30天, TRACE>180天）"
	@echo "  help                   显示此帮助（默认）"
	@echo ""
	@echo "示例:"
	@echo "  make install"
	@echo "  make run-sql ARGS=flink"

install:
	@$(LAKEHOUSE_SH) install

fix:
	@$(LAKEHOUSE_SH) fix

verify:
	@$(LAKEHOUSE_SH) verify

replay:
	@$(LAKEHOUSE_SH) replay

reset:
	@$(LAKEHOUSE_SH) reset

run-sql:
	@$(LAKEHOUSE_SH) run-sql $(ARGS)

download-starrocks-jars:
	@$(LAKEHOUSE_SH) download-starrocks-jars

bench:
	@$(ROOT_DIR)scripts/bench.sh $(ARGS)

scale-cn:
	@N=$(or $(ARGS),2); \
	echo "⚡ Scaling StarRocks CN to $$N nodes..."; \
	docker compose up -d --scale starrocks-cn=$$N --no-recreate; \
	echo "✓ Done. Verify: mysql -h 127.0.0.1 -P 9030 -u root -e 'SHOW COMPUTE NODES;'"

lifecycle-cleanup:
	@echo "🧹 Cleaning expired events (DEBUG>30d, TRACE>180d)..."
	@docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -D ods \
		-e "DELETE FROM ods_events WHERE (event_group = 'DEBUG' AND dt < DATE_SUB(CURDATE(), INTERVAL 30 DAY)) OR (event_group = 'TRACE' AND dt < DATE_SUB(CURDATE(), INTERVAL 180 DAY));"
	@echo "✓ Lifecycle cleanup done."
