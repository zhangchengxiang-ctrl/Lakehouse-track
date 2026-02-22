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
#   create-project       创建埋点项目（从种子表克隆）
#   download-starrocks-jars  仅下载 StarRocks 外部目录依赖
#   help                显示帮助（默认目标）
#
# 示例：
#   make install
#   make replay
#   make run-sql
#   make run-sql ARGS=flink
#   make run-sql ARGS="starrocks"
#   make create-project ARGS="production 正式项目"
# ============================================================

# 项目根目录与脚本路径（与 Makefile 同目录）
ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
LAKEHOUSE_SH := $(ROOT_DIR)scripts/lakehouse.sh

.PHONY: help install fix verify replay reset run-sql create-project download-starrocks-jars

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
	@echo "  create-project ARGS=\"name [cname]\"  创建埋点项目（从种子表克隆）"
	@echo "  download-starrocks-jars  仅下载 StarRocks 外部目录依赖"
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

create-project:
	@$(ROOT_DIR)scripts/create_project.sh $(ARGS)

download-starrocks-jars:
	@$(LAKEHOUSE_SH) download-starrocks-jars
