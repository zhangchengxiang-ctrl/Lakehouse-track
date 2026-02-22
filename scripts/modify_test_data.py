#!/usr/bin/env python3
"""批量修改 test_data 目录下的 nginx 日志测试数据：
1. 项目(project)统一为 "default"
2. 所有时间戳偏移到 2026-01-01（CST），保留日内相对偏移

用法：
    python scripts/modify_test_data.py
    python scripts/modify_test_data.py --dry-run   # 仅预览，不写入
"""

import os
import sys
import json
import gzip
import base64
import urllib.parse
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

CST = timezone(timedelta(hours=8))
TARGET_DAY_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=CST)
TARGET_EPOCH = TARGET_DAY_START.timestamp()
TARGET_PROJECT = "default"

_offset_seconds = None


def _ensure_offset(epoch_sec: float) -> float:
    """根据第一条记录的日期计算偏移量（保留日内时间）"""
    global _offset_seconds
    if _offset_seconds is not None:
        return _offset_seconds
    orig_dt = datetime.fromtimestamp(epoch_sec, tz=CST)
    orig_day_start = orig_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    _offset_seconds = (TARGET_DAY_START - orig_day_start).total_seconds()
    return _offset_seconds


def shift_sec(ts: float) -> float:
    return ts + _ensure_offset(ts)


def shift_ms(ts_ms: int) -> int:
    return int(ts_ms + _ensure_offset(ts_ms / 1000.0) * 1000)


# ── payload codec ──────────────────────────────────────────

def decode_payload(raw: str, is_gzip: bool):
    data = urllib.parse.unquote(raw)
    data = data.replace('-', '+').replace('_', '/')
    mod = len(data) % 4
    if mod:
        data += '=' * (4 - mod)
    binary = base64.b64decode(data)
    if is_gzip:
        binary = gzip.decompress(binary)
    return json.loads(binary)


def encode_payload(obj, should_gzip: bool) -> str:
    raw = json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    if should_gzip:
        raw = gzip.compress(raw)
    b64 = base64.b64encode(raw).decode('ascii')
    return urllib.parse.quote(b64, safe='')


def patch_payload(obj):
    """递归修改 payload 的 project / time / _flush_time"""
    if isinstance(obj, list):
        return [patch_payload(item) for item in obj]
    if not isinstance(obj, dict):
        return obj
    if 'project' in obj:
        obj['project'] = TARGET_PROJECT
    for key in ('time', '_flush_time'):
        if key in obj:
            try:
                obj[key] = shift_ms(int(obj[key]))
            except (ValueError, TypeError):
                pass
    return obj


# ── 行处理 ─────────────────────────────────────────────────

def process_line(line: str) -> str:
    line = line.rstrip('\n\r')
    if not line.strip():
        return line + '\n'

    parts = line.split(' ++_ ')
    if len(parts) < 12:
        return line + '\n'

    fields = []
    for p in parts:
        p = p.strip()
        if p.startswith('"') and p.endswith('"'):
            fields.append(p[1:-1])
        else:
            fields.append(p)

    # [1] nginx msec → 偏移到 2026-01-01
    try:
        fields[1] = f"{shift_sec(float(fields[1])):.3f}"
    except ValueError:
        pass

    # [8] arg_project → "default"
    fields[8] = TARGET_PROJECT

    # [6]/[4]/[5] 编码的 payload → 修改 project + time
    method = fields[2]
    try:
        if method == "POST" and fields[6] != "-":
            _patch_post_body(fields)
        elif method == "GET":
            _patch_get_data(fields)
    except Exception:
        pass  # payload 解码失败时仅修改 nginx 级字段

    return ' ++_ '.join(f'"{f}"' for f in fields) + '\n'


def _patch_post_body(fields: list):
    body = fields[6]
    params = urllib.parse.parse_qs(body, keep_blank_values=True)
    data_key = 'data_list' if 'data_list' in params else ('data' if 'data' in params else None)
    if not data_key or not params[data_key][0]:
        return
    is_gz = params.get('gzip', ['0'])[0] == '1'
    payload = decode_payload(params[data_key][0], is_gz)
    payload = patch_payload(payload)
    encoded = encode_payload(payload, is_gz)

    new_parts = [f"{data_key}={encoded}"]
    for k, vs in params.items():
        if k == data_key:
            continue
        for v in vs:
            new_parts.append(f"{k}={urllib.parse.quote(v, safe='')}")
    fields[6] = "&".join(new_parts)


def _patch_get_data(fields: list):
    is_gz = fields[3] == "1"
    for idx in (4, 5):
        if fields[idx] != "-" and fields[idx]:
            payload = decode_payload(fields[idx], is_gz)
            payload = patch_payload(payload)
            fields[idx] = encode_payload(payload, is_gz)
            break


# ── 文件/目录处理 ──────────────────────────────────────────

def process_file(filepath: Path, dry_run: bool = False):
    lines = filepath.read_text(encoding='utf-8').splitlines(keepends=True)
    new_lines = [process_line(l) for l in lines]
    if dry_run:
        changed = sum(1 for o, n in zip(lines, new_lines) if o != n)
        print(f"  [dry-run] {filepath.name}: {changed}/{len(lines)} lines would change")
    else:
        filepath.write_text(''.join(new_lines), encoding='utf-8')
        print(f"  {filepath.name}: {len(lines)} lines processed")


def main():
    parser = argparse.ArgumentParser(description="批量修改测试数据")
    parser.add_argument('--dry-run', action='store_true', help='仅预览，不写入文件')
    args = parser.parse_args()

    test_dir = Path(__file__).resolve().parent.parent / "test_data"
    if not test_dir.exists():
        print(f"Error: {test_dir} not found")
        sys.exit(1)

    files = sorted(test_dir.glob("nginx_log_part_*.txt"))
    print(f"Found {len(files)} test data files in {test_dir}")
    print(f"Target: project={TARGET_PROJECT}, date=2026-01-01 CST\n")

    success, fail = 0, 0
    for f in files:
        try:
            process_file(f, dry_run=args.dry_run)
            success += 1
        except Exception as e:
            print(f"  ERROR {f.name}: {e}")
            fail += 1

    print(f"\nDone! success={success}, fail={fail}")
    if _offset_seconds is not None:
        print(f"Time offset applied: {_offset_seconds:.0f}s ({_offset_seconds/86400:.1f} days)")


if __name__ == "__main__":
    main()
