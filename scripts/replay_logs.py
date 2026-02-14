#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
神策埋点日志重放压测脚本
将 test_data 目录下的全部 Nginx 原始日志重放到当前项目的 Nginx 采集端

用法：
  python3 scripts/replay_logs.py                    # 全量重放所有文件
  python3 scripts/replay_logs.py --files 5          # 仅重放前 5 个文件
  python3 scripts/replay_logs.py --workers 20       # 20 并发
  python3 scripts/replay_logs.py --workers 50 --files 100  # 压测模式
"""

import re
import sys
import time
import argparse
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置
NGINX_URL = "http://localhost/sa"
TEST_DATA_DIR = "test_data"

# 禁用系统代理
SESSION = requests.Session()
SESSION.trust_env = False
# 连接池优化：适配高并发
adapter = requests.adapters.HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100,
    max_retries=2,
)
SESSION.mount("http://", adapter)

# Nginx 日志正则
NGINX_LOG_PATTERN = re.compile(
    r'"(?P<proxy_add_x_forwarded_for>[^"]*)" \+\+_ '
    r'"(?P<msec>[^"]*)" \+\+_ '
    r'"(?P<request_method>[^"]*)" \+\+_ '
    r'"(?P<arg_gzip>[^"]*)" \+\+_ '
    r'"(?P<arg_data>[^"]*)" \+\+_ '
    r'"(?P<arg_data_list>[^"]*)" \+\+_ '
    r'"(?P<request_body>[^"]*)" \+\+_ '
    r'"(?P<http_user_agent>[^"]*)" \+\+_ '
    r'"(?P<arg_project>[^"]*)" \+\+_ '
    r'"(?P<http_cookie>[^"]*)" \+\+_ '
    r'"(?P<arg_token>[^"]*)" \+\+_ '
    r'"(?P<arg_ext>[^"]*)"'
)


def replay_line(line):
    """解析并重放单行日志，返回 (success: bool, status_code: int)"""
    line = line.strip()
    if not line:
        return False, 0

    match = NGINX_LOG_PATTERN.match(line)
    if not match:
        return False, 0

    data = match.groupdict()
    method = data.get("request_method")

    params = {
        "project": data.get("arg_project"),
        "gzip": data.get("arg_gzip"),
        "token": data.get("arg_token"),
        "ext": data.get("arg_ext"),
    }
    params = {k: v for k, v in params.items() if v and v != "-"}

    headers = {
        "User-Agent": data.get("http_user_agent"),
        "X-Forwarded-For": data.get("proxy_add_x_forwarded_for"),
        "Cookie": data.get("http_cookie"),
    }
    headers = {k: v for k, v in headers.items() if v and v != "-"}

    try:
        if method == "GET":
            if data.get("arg_data") and data.get("arg_data") != "-":
                params["data"] = data.get("arg_data")
            elif data.get("arg_data_list") and data.get("arg_data_list") != "-":
                params["data_list"] = data.get("arg_data_list")
            resp = SESSION.get(NGINX_URL, params=params, headers=headers, timeout=10)
            return resp.status_code < 400, resp.status_code

        elif method == "POST":
            body = data.get("request_body")
            if body and body != "-":
                resp = SESSION.post(NGINX_URL, params=params, data=body, headers=headers, timeout=10)
                return resp.status_code < 400, resp.status_code
    except Exception:
        pass

    return False, 0


def process_file(file_path, max_workers):
    """处理单个日志文件，返回 (总行数, 成功数, 跳过数, 耗时)"""
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    start = time.time()
    success = 0
    skipped = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(replay_line, line): line for line in lines}
        for future in as_completed(futures):
            ok, code = future.result()
            if ok:
                success += 1
            elif code == 0:
                skipped += 1

    elapsed = time.time() - start
    return len(lines), success, skipped, elapsed


def main():
    parser = argparse.ArgumentParser(description="神策埋点日志重放压测")
    parser.add_argument("--files", type=int, default=0, help="重放文件数量 (0=全部)")
    parser.add_argument("--workers", type=int, default=20, help="并发线程数 (默认 20)")
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    test_data_path = project_root / TEST_DATA_DIR

    if not test_data_path.exists():
        print(f"错误: 测试数据目录不存在 {test_data_path}")
        return

    log_files = sorted(list(test_data_path.glob("nginx_log_part_*.txt")))
    if not log_files:
        print("未找到测试日志文件")
        return

    if args.files > 0:
        log_files = log_files[: args.files]

    total_files = len(log_files)
    print(f"{'=' * 60}")
    print(f"  压测配置: {total_files} 个文件, {args.workers} 并发")
    print(f"  目标地址: {NGINX_URL}")
    print(f"{'=' * 60}")

    grand_total = 0
    grand_success = 0
    grand_skipped = 0

    overall_start = time.time()

    for i, log_file in enumerate(log_files, 1):
        total, success, skipped, elapsed = process_file(log_file, args.workers)
        grand_total += total
        grand_success += success
        grand_skipped += skipped
        rps = success / elapsed if elapsed > 0 else 0
        print(
            f"  [{i:3d}/{total_files}] {log_file.name}: "
            f"{success}/{total} 成功, {skipped} 跳过, "
            f"{elapsed:.1f}s, {rps:.0f} req/s"
        )

    overall_elapsed = time.time() - overall_start
    overall_rps = grand_success / overall_elapsed if overall_elapsed > 0 else 0

    print(f"\n{'=' * 60}")
    print(f"  压测完成!")
    print(f"  总耗时:     {overall_elapsed:.1f} 秒")
    print(f"  总请求数:   {grand_total:,}")
    print(f"  成功数:     {grand_success:,}")
    print(f"  跳过数:     {grand_skipped:,} (无有效载荷)")
    print(f"  成功率:     {(grand_success / grand_total * 100):.1f}%" if grand_total > 0 else "  0%")
    print(f"  平均吞吐:   {overall_rps:,.0f} req/s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
