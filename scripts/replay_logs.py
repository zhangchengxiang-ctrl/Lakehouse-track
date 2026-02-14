#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
神策埋点日志重放压测脚本
将 test_data 目录下的全部 Nginx 原始日志重放到当前项目的 Nginx 采集端

用法：
  python3 scripts/replay_logs.py                    # 全量重放所有文件（保留原始时间）
  python3 scripts/replay_logs.py --files 5          # 仅重放前 5 个文件
  python3 scripts/replay_logs.py --workers 20       # 20 并发
  python3 scripts/replay_logs.py --workers 50 --files 100  # 压测模式
  python3 scripts/replay_logs.py --start-time "2026-02-14 00:00:00" --end-time "2026-02-14 23:59:59"
                                                    # 将所有事件时间戳映射到指定时间段
  python3 scripts/replay_logs.py --date 2026-02-14  # 简写：映射到当天 00:00~23:59
"""

import re
import sys
import time
import json
import gzip
import base64
import random
import argparse
import requests
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import unquote
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


# ---- 时间戳重写工具 ----

def _decode_payload(raw: str, is_gzip: bool) -> str | None:
    """解码 base64 (可能 gzip) 的神策数据，返回 JSON 字符串"""
    try:
        raw = unquote(raw)
        decoded = base64.b64decode(raw)
        if is_gzip:
            decoded = gzip.decompress(decoded)
        return decoded.decode("utf-8")
    except Exception:
        return None


def _encode_payload(json_str: str, is_gzip: bool) -> str:
    """将 JSON 字符串编码回 base64 (可选 gzip)"""
    data = json_str.encode("utf-8")
    if is_gzip:
        data = gzip.compress(data)
    return base64.b64encode(data).decode("utf-8")


def _rewrite_time_in_json(json_str: str, ts_start_ms: int, ts_end_ms: int) -> str:
    """在 JSON 字符串中替换 time 字段为指定范围内的随机时间戳"""
    try:
        obj = json.loads(json_str)
    except Exception:
        return json_str

    def _patch(item):
        if isinstance(item, dict) and "time" in item:
            item["time"] = random.randint(ts_start_ms, ts_end_ms)

    if isinstance(obj, list):
        for item in obj:
            _patch(item)
    else:
        _patch(obj)
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _rewrite_payload(raw: str, is_gzip: bool, ts_start_ms: int, ts_end_ms: int) -> str:
    """解码 → 改时间 → 重新编码"""
    json_str = _decode_payload(raw, is_gzip)
    if json_str is None:
        return raw
    json_str = _rewrite_time_in_json(json_str, ts_start_ms, ts_end_ms)
    return _encode_payload(json_str, is_gzip)


def _rewrite_form_body(body: str, ts_start_ms: int, ts_end_ms: int) -> str:
    """处理 POST 表单体：data=xxx 或 data_list=xxx，解码→改时间→重编码

    POST body 格式示例：
      data_list=H4sIAAAA...（URL 编码的 base64 gzip 数据）
      data=eyJkaXN...（URL 编码的 base64 纯文本数据）
    """
    from urllib.parse import parse_qs, quote

    parsed = parse_qs(body, keep_blank_values=True)

    for key in ("data_list", "data"):
        if key in parsed:
            raw_val = parsed[key][0]
            # data_list 通常是 gzip 压缩的
            is_gzip = (key == "data_list")
            # 也可能通过 gzip 参数标记
            if "gzip" in parsed and parsed["gzip"][0] not in ("", "-"):
                is_gzip = True
            rewritten = _rewrite_payload(raw_val, is_gzip, ts_start_ms, ts_end_ms)
            # 重建表单体
            parts = []
            for k, vs in parsed.items():
                if k == key:
                    parts.append(f"{k}={quote(rewritten, safe='')}")
                else:
                    for v in vs:
                        parts.append(f"{k}={quote(v, safe='')}")
            return "&".join(parts)

    # 无 data/data_list 字段，尝试当纯 base64 处理
    return _rewrite_payload(body, False, ts_start_ms, ts_end_ms)


# ---- 回放 ----

# 全局时间范围（由 main() 设置，None 表示不改写）
TIME_RANGE: tuple[int, int] | None = None


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
    is_gzip = data.get("arg_gzip") not in (None, "", "-")

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
                payload = data.get("arg_data")
                if TIME_RANGE:
                    payload = _rewrite_payload(payload, is_gzip, *TIME_RANGE)
                params["data"] = payload
            elif data.get("arg_data_list") and data.get("arg_data_list") != "-":
                payload = data.get("arg_data_list")
                if TIME_RANGE:
                    # data_list 在 GET 参数中也可能是 gzip 压缩
                    payload = _rewrite_payload(payload, True, *TIME_RANGE)
                params["data_list"] = payload
            resp = SESSION.get(NGINX_URL, params=params, headers=headers, timeout=10)
            return resp.status_code < 400, resp.status_code

        elif method == "POST":
            body = data.get("request_body")
            if body and body != "-":
                if TIME_RANGE:
                    # POST body 是表单格式 data=xxx 或 data_list=xxx
                    body = _rewrite_form_body(body, *TIME_RANGE)
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


def _parse_datetime(s: str) -> datetime:
    """解析时间字符串，支持 YYYY-MM-DD 和 YYYY-MM-DD HH:MM:SS"""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"无法解析时间: {s}，格式应为 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")


def main():
    global TIME_RANGE

    parser = argparse.ArgumentParser(description="神策埋点日志重放压测")
    parser.add_argument("--files", type=int, default=0, help="重放文件数量 (0=全部)")
    parser.add_argument("--workers", type=int, default=20, help="并发线程数 (默认 20)")
    parser.add_argument("--date", type=str, default=None,
                        help="将事件时间映射到指定日期 (YYYY-MM-DD)，简写，等价于 --start-time DATE 00:00:00 --end-time DATE 23:59:59")
    parser.add_argument("--start-time", type=str, default=None,
                        help="事件时间范围起始 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end-time", type=str, default=None,
                        help="事件时间范围结束 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)")
    args = parser.parse_args()

    # 处理时间范围参数
    time_label = "原始时间（不改写）"
    if args.date:
        dt = _parse_datetime(args.date)
        ts_start = int(dt.timestamp() * 1000)
        ts_end = int((dt + timedelta(hours=23, minutes=59, seconds=59)).timestamp() * 1000)
        TIME_RANGE = (ts_start, ts_end)
        time_label = f"{args.date} 00:00:00 ~ 23:59:59"
    elif args.start_time and args.end_time:
        dt_start = _parse_datetime(args.start_time)
        dt_end = _parse_datetime(args.end_time)
        if dt_end <= dt_start:
            print("错误: --end-time 必须晚于 --start-time")
            return
        ts_start = int(dt_start.timestamp() * 1000)
        ts_end = int(dt_end.timestamp() * 1000)
        TIME_RANGE = (ts_start, ts_end)
        time_label = f"{dt_start} ~ {dt_end}"
    elif args.start_time or args.end_time:
        print("错误: --start-time 和 --end-time 必须同时指定")
        return

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
    print(f"  时间范围: {time_label}")
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
