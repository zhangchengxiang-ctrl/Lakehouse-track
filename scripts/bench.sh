#!/bin/bash
# ============================================================
# Collection 采集节点压力测试
# 支持 wrk (推荐) 和 ab (回退) 两种引擎
#
# 用法:
#   ./scripts/bench.sh                      # 默认渐进式测试
#   ./scripts/bench.sh -c 1000 -d 30        # 指定 1000 并发跑 30 秒
#   ./scripts/bench.sh -t 8 -c 5000 -d 60   # 8 线程 5000 连接 60 秒
# ============================================================
set -euo pipefail

TARGET="${TARGET:-http://localhost:8080}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LUA_SCRIPT="$SCRIPT_DIR/bench_wrk.lua"
DURATION="30"
THREADS=""
CONNECTIONS=""

usage() {
    echo "用法: $0 [-t threads] [-c connections] [-d duration_secs]"
    echo "  -t  线程数 (默认: auto)"
    echo "  -c  并发连接数 (默认: 渐进式 100→1000→2000→3000→5000)"
    echo "  -d  每轮持续时间秒 (默认: 30)"
    exit 1
}

while getopts "t:c:d:h" opt; do
    case $opt in
        t) THREADS="$OPTARG" ;;
        c) CONNECTIONS="$OPTARG" ;;
        d) DURATION="$OPTARG" ;;
        h) usage ;;
        *) usage ;;
    esac
done

echo ""
echo "================================================================"
echo "  Collection 采集节点压力测试"
echo "================================================================"

if ! curl -sf "$TARGET/health" >/dev/null 2>&1; then
    echo "错误: $TARGET/health 不可达，请确认 collection 容器已启动"
    exit 1
fi
echo "  目标: $TARGET"

USE_WRK=false
if command -v wrk &>/dev/null; then
    USE_WRK=true
    echo "  引擎: wrk"
elif command -v ab &>/dev/null; then
    echo "  引擎: ab (ApacheBench)"
else
    echo "错误: 需要 wrk 或 ab。安装: brew install wrk"
    exit 1
fi
echo "  测试时长: ${DURATION}s / 轮"

PAYLOAD_FILE="/tmp/_bench_sa_payload.txt"
echo -n 'data=W3siZGlzdGluY3RfaWQiOiJ0ZXN0XzAwMSIsImV2ZW50IjoiJHBhZ2V2aWV3IiwidHlwZSI6InRyYWNrIiwidGltZSI6MTcwOTIwMDAwMDAwMCwicHJvcGVydGllcyI6eyIkb3MiOiJpT1MiLCIkbGliIjoiaU9TIn0sImxvZ2luX2lkIjoidTEyMyIsImFub255bW91c19pZCI6ImExMjMifV0=' > "$PAYLOAD_FILE"

auto_threads() {
    local conns=$1
    local cpus
    cpus=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)
    local t=$((cpus > 8 ? 8 : cpus))
    [ "$conns" -le 100 ] && t=2
    [ "$conns" -le 500 ] && t=$((cpus > 4 ? 4 : cpus))
    echo $t
}

run_test() {
    local conns=$1
    echo ""
    echo "────────────────────────────────────────"
    echo "  并发: ${conns}, 持续: ${DURATION}s"
    echo "────────────────────────────────────────"

    if $USE_WRK; then
        local t=${THREADS:-$(auto_threads "$conns")}
        wrk -t"$t" -c"$conns" -d"${DURATION}s" --latency \
            -s "$LUA_SCRIPT" \
            "$TARGET/sa?project=default" 2>&1
    else
        local total=$((conns * DURATION * 3))
        [ "$total" -gt 200000 ] && total=200000
        ab -n "$total" -c "$conns" \
            -p "$PAYLOAD_FILE" \
            -T 'application/x-www-form-urlencoded' \
            -H 'User-Agent: SensorsAnalytics iOS SDK' \
            -k \
            "${TARGET}/sa?project=default" 2>&1 \
            | grep -E 'Requests per second|Time per request.*\(mean\)|Failed|Non-2xx|50%|90%|95%|99%|100%'
    fi
    echo ""
    sleep 3
}

if [ -n "$CONNECTIONS" ]; then
    run_test "$CONNECTIONS"
else
    echo ""
    echo "  渐进式测试: 100 → 1000 → 2000 → 3000 → 5000"
    for c in 100 1000 2000 3000 5000; do
        run_test "$c"
    done
fi

echo "================================================================"
echo "  压测完成"
echo "================================================================"
rm -f "$PAYLOAD_FILE"
