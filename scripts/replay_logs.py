#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
神策埋点日志重放压测脚本
将 test_data 目录下的 Nginx 原始日志重放到采集端，支持多轮压测和延迟统计

用法：
  python3 scripts/replay_logs.py                              # 全量重放（保留原始时间）
  python3 scripts/replay_logs.py --files 5                    # 仅重放前 5 个文件
  python3 scripts/replay_logs.py --workers 50 --files 20      # 50 并发 × 20 文件
  python3 scripts/replay_logs.py --workers 50 --rounds 3      # 50 并发，全量跑 3 轮
  python3 scripts/replay_logs.py --date 2026-02-22            # 时间戳映射到指定日期
  python3 scripts/replay_logs.py --warmup 2                   # 先预热 2 个文件再开始计量
"""

import re
import sys
import time
import json
import gzip
import base64
import random
import argparse
import threading
import requests
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import unquote, parse_qs, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

NGINX_URL = "http://localhost/sa"
TEST_DATA_DIR = "test_data"

SESSION = requests.Session()
SESSION.trust_env = False
adapter = requests.adapters.HTTPAdapter(
    pool_connections=200,
    pool_maxsize=200,
    max_retries=2,
)
SESSION.mount("http://", adapter)

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


# ---- 统计收集器（线程安全） ----

@dataclass
class Stats:
    lock: threading.Lock = field(default_factory=threading.Lock)
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    latencies_ms: list = field(default_factory=list)
    status_codes: dict = field(default_factory=dict)
    errors: int = 0

    def record(self, ok: bool, code: int, latency_ms: float):
        with self.lock:
            self.total += 1
            if code == 0:
                self.skipped += 1
            elif ok:
                self.success += 1
                self.latencies_ms.append(latency_ms)
            else:
                self.failed += 1
            self.status_codes[code] = self.status_codes.get(code, 0) + 1

    def record_error(self):
        with self.lock:
            self.total += 1
            self.errors += 1

    def percentile(self, p: float) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        idx = int(len(s) * p / 100)
        return s[min(idx, len(s) - 1)]

    def summary(self, elapsed: float) -> str:
        rps = self.success / elapsed if elapsed > 0 else 0
        avg = sum(self.latencies_ms) / len(self.latencies_ms) if self.latencies_ms else 0
        lines = [
            f"  请求总数:   {self.total:,}",
            f"  成功:       {self.success:,}",
            f"  失败:       {self.failed:,} (HTTP 4xx/5xx)",
            f"  异常:       {self.errors:,} (超时/连接错误)",
            f"  跳过:       {self.skipped:,} (无有效载荷)",
            f"  成功率:     {(self.success / max(self.total - self.skipped, 1) * 100):.1f}%",
            f"  吞吐量:     {rps:,.0f} req/s",
            f"  延迟 avg:   {avg:.1f} ms",
            f"  延迟 P50:   {self.percentile(50):.1f} ms",
            f"  延迟 P90:   {self.percentile(90):.1f} ms",
            f"  延迟 P95:   {self.percentile(95):.1f} ms",
            f"  延迟 P99:   {self.percentile(99):.1f} ms",
            f"  延迟 max:   {max(self.latencies_ms, default=0):.1f} ms",
        ]
        if self.failed > 0:
            codes = {k: v for k, v in self.status_codes.items() if k >= 400}
            if codes:
                lines.append(f"  错误码分布: {codes}")
        return "\n".join(lines)


# ---- 时间戳重写 ----

TIME_RANGE: tuple[int, int] | None = None


def _decode_payload(raw: str, is_gzip: bool) -> str | None:
    try:
        raw = unquote(raw)
        decoded = base64.b64decode(raw)
        if is_gzip:
            decoded = gzip.decompress(decoded)
        return decoded.decode("utf-8")
    except Exception:
        return None


def _encode_payload(json_str: str, is_gzip: bool) -> str:
    data = json_str.encode("utf-8")
    if is_gzip:
        data = gzip.compress(data)
    return base64.b64encode(data).decode("utf-8")


def _rewrite_time_in_json(json_str: str, ts_start_ms: int, ts_end_ms: int) -> str:
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
    json_str = _decode_payload(raw, is_gzip)
    if json_str is None:
        return raw
    json_str = _rewrite_time_in_json(json_str, ts_start_ms, ts_end_ms)
    return _encode_payload(json_str, is_gzip)


def _rewrite_form_body(body: str, ts_start_ms: int, ts_end_ms: int) -> str:
    parsed = parse_qs(body, keep_blank_values=True)
    for key in ("data_list", "data"):
        if key in parsed:
            raw_val = parsed[key][0]
            is_gzip = (key == "data_list")
            if "gzip" in parsed and parsed["gzip"][0] not in ("", "-"):
                is_gzip = True
            rewritten = _rewrite_payload(raw_val, is_gzip, ts_start_ms, ts_end_ms)
            parts = []
            for k, vs in parsed.items():
                if k == key:
                    parts.append(f"{k}={quote(rewritten, safe='')}")
                else:
                    for v in vs:
                        parts.append(f"{k}={quote(v, safe='')}")
            return "&".join(parts)
    return _rewrite_payload(body, False, ts_start_ms, ts_end_ms)


# ---- 回放核心 ----

def replay_line(line: str, stats: Stats):
    line = line.strip()
    if not line:
        stats.record(False, 0, 0)
        return

    match = NGINX_LOG_PATTERN.match(line)
    if not match:
        stats.record(False, 0, 0)
        return

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
        t0 = time.monotonic()
        if method == "GET":
            if data.get("arg_data") and data.get("arg_data") != "-":
                payload = data.get("arg_data")
                if TIME_RANGE:
                    payload = _rewrite_payload(payload, is_gzip, *TIME_RANGE)
                params["data"] = payload
            elif data.get("arg_data_list") and data.get("arg_data_list") != "-":
                payload = data.get("arg_data_list")
                if TIME_RANGE:
                    payload = _rewrite_payload(payload, True, *TIME_RANGE)
                params["data_list"] = payload
            resp = SESSION.get(NGINX_URL, params=params, headers=headers, timeout=10)
            latency_ms = (time.monotonic() - t0) * 1000
            stats.record(resp.status_code < 400, resp.status_code, latency_ms)

        elif method == "POST":
            body = data.get("request_body")
            if body and body != "-":
                if TIME_RANGE:
                    body = _rewrite_form_body(body, *TIME_RANGE)
                resp = SESSION.post(NGINX_URL, params=params, data=body, headers=headers, timeout=10)
                latency_ms = (time.monotonic() - t0) * 1000
                stats.record(resp.status_code < 400, resp.status_code, latency_ms)
            else:
                stats.record(False, 0, 0)
        else:
            stats.record(False, 0, 0)
    except Exception:
        stats.record_error()


def process_files(file_list: list[Path], max_workers: int, stats: Stats, label: str = ""):
    all_lines = []
    for fp in file_list:
        with open(fp, "r", encoding="utf-8") as f:
            all_lines.extend(f.readlines())

    random.shuffle(all_lines)
    total = len(all_lines)
    start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(replay_line, line, stats) for line in all_lines]
        done = 0
        for future in as_completed(futures):
            future.result()
            done += 1
            if done % 5000 == 0:
                elapsed = time.time() - start
                rps = stats.success / elapsed if elapsed > 0 else 0
                print(
                    f"\r  {label}进度: {done:,}/{total:,} "
                    f"({done * 100 // total}%) "
                    f"| 成功 {stats.success:,} "
                    f"| {rps:,.0f} req/s "
                    f"| P99 {stats.percentile(99):.0f}ms",
                    end="", flush=True,
                )

    print(f"\r  {label}进度: {total:,}/{total:,} (100%)" + " " * 40)
    return time.time() - start


def _parse_datetime(s: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"无法解析时间: {s}")


def main():
    global TIME_RANGE

    parser = argparse.ArgumentParser(description="神策埋点日志重放压测")
    parser.add_argument("--files", type=int, default=0, help="重放文件数量 (0=全部)")
    parser.add_argument("--workers", type=int, default=20, help="并发线程数 (默认 20)")
    parser.add_argument("--rounds", type=int, default=1, help="重复轮数 (默认 1)")
    parser.add_argument("--warmup", type=int, default=0, help="预热文件数 (不计入统计)")
    parser.add_argument("--date", type=str, default=None, help="时间映射到指定日期 (YYYY-MM-DD)")
    parser.add_argument("--start-time", type=str, default=None, help="事件时间范围起始")
    parser.add_argument("--end-time", type=str, default=None, help="事件时间范围结束")
    args = parser.parse_args()

    time_label = "原始时间（不改写）"
    if args.date:
        dt = _parse_datetime(args.date)
        ts_start = int(dt.timestamp() * 1000)
        ts_end = int((dt + timedelta(hours=23, minutes=59, seconds=59)).timestamp() * 1000)
        TIME_RANGE = (ts_start, ts_end)
        time_label = f"{args.date} 全天"
    elif args.start_time and args.end_time:
        dt_start = _parse_datetime(args.start_time)
        dt_end = _parse_datetime(args.end_time)
        if dt_end <= dt_start:
            print("错误: --end-time 必须晚于 --start-time")
            return
        TIME_RANGE = (int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000))
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
        log_files = log_files[:args.files]

    total_files = len(log_files)
    total_events = total_files * 1000
    print(f"\n{'=' * 64}")
    print(f"  压测配置")
    print(f"  {'─' * 60}")
    print(f"  目标地址:   {NGINX_URL}")
    print(f"  并发线程:   {args.workers}")
    print(f"  数据文件:   {total_files} 个 (~{total_events:,} 事件/轮)")
    print(f"  重复轮数:   {args.rounds}")
    print(f"  预热文件:   {args.warmup}")
    print(f"  时间范围:   {time_label}")
    print(f"  总事件量:   ~{total_events * args.rounds:,}")
    print(f"{'=' * 64}")

    # ---- 预热 ----
    if args.warmup > 0:
        warmup_files = log_files[:args.warmup]
        print(f"\n  预热中 ({args.warmup} 文件, {args.workers} 并发)...")
        warmup_stats = Stats()
        warmup_elapsed = process_files(warmup_files, args.workers, warmup_stats, "预热 ")
        warmup_rps = warmup_stats.success / warmup_elapsed if warmup_elapsed > 0 else 0
        print(f"  预热完成: {warmup_stats.success:,} 请求, {warmup_rps:,.0f} req/s, "
              f"P99={warmup_stats.percentile(99):.0f}ms\n")

    # ---- 正式压测 ----
    overall_stats = Stats()
    overall_start = time.time()

    for r in range(1, args.rounds + 1):
        round_label = f"[轮 {r}/{args.rounds}] " if args.rounds > 1 else ""
        print(f"\n  {round_label}开始...")
        round_stats = Stats()
        round_elapsed = process_files(log_files, args.workers, round_stats, round_label)
        rps = round_stats.success / round_elapsed if round_elapsed > 0 else 0
        print(f"  {round_label}完成: {round_stats.success:,} 成功, "
              f"{round_elapsed:.1f}s, {rps:,.0f} req/s, "
              f"P50={round_stats.percentile(50):.0f}ms "
              f"P99={round_stats.percentile(99):.0f}ms")

        with overall_stats.lock:
            overall_stats.total += round_stats.total
            overall_stats.success += round_stats.success
            overall_stats.failed += round_stats.failed
            overall_stats.skipped += round_stats.skipped
            overall_stats.errors += round_stats.errors
            overall_stats.latencies_ms.extend(round_stats.latencies_ms)
            for k, v in round_stats.status_codes.items():
                overall_stats.status_codes[k] = overall_stats.status_codes.get(k, 0) + v

    overall_elapsed = time.time() - overall_start

    print(f"\n{'=' * 64}")
    print(f"  压测报告 ({args.workers} 并发 × {args.rounds} 轮)")
    print(f"  {'─' * 60}")
    print(f"  总耗时:     {overall_elapsed:.1f} 秒")
    print(overall_stats.summary(overall_elapsed))
    print(f"{'=' * 64}\n")


if __name__ == "__main__":
    main()
