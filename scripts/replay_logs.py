#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
神策埋点日志重放脚本
将 test_data 目录下的 Nginx 原始日志重放到当前项目的 Nginx 采集端
"""

import re
import time
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# 配置
NGINX_URL = "http://localhost/sa"
TEST_DATA_DIR = "test_data"
MAX_WORKERS = 10  # 并发重放线程数
BATCH_SIZE = 100  # 每批处理的行数

# 禁用系统代理，避免本地代理影响重放请求
SESSION = requests.Session()
SESSION.trust_env = False

# Nginx 日志格式解析正则 (与 StellarTrace 保持一致)
# "$proxy_add_x_forwarded_for" ++_ "$msec" ++_ "$request_method" ++_ "$arg_gzip" ++_ "$arg_data" ++_ "$arg_data_list" ++_ "$request_body" ++_ "$http_user_agent" ++_ "$arg_project" ++_ "$http_cookie" ++_ "$arg_token" ++_ "$arg_ext"
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
    """解析并重放单行日志"""
    line = line.strip()
    if not line:
        return False
    
    match = NGINX_LOG_PATTERN.match(line)
    if not match:
        return False
    
    data = match.groupdict()
    method = data.get("request_method")
    
    params = {
        "project": data.get("arg_project"),
        "gzip": data.get("arg_gzip"),
        "token": data.get("arg_token"),
        "ext": data.get("arg_ext")
    }
    
    # 移除空值参数
    params = {k: v for k, v in params.items() if v and v != "-"}
    
    headers = {
        "User-Agent": data.get("http_user_agent"),
        "X-Forwarded-For": data.get("proxy_add_x_forwarded_for"),
        "Cookie": data.get("http_cookie")
    }
    headers = {k: v for k, v in headers.items() if v and v != "-"}

    try:
        if method == "GET":
            # 构造 GET 请求
            if data.get("arg_data") and data.get("arg_data") != "-":
                params["data"] = data.get("arg_data")
            elif data.get("arg_data_list") and data.get("arg_data_list") != "-":
                params["data_list"] = data.get("arg_data_list")
            
            resp = SESSION.get(NGINX_URL, params=params, headers=headers, timeout=5)
            return resp.status_code < 400
            
        elif method == "POST":
            # 构造 POST 请求
            body = data.get("request_body")
            if body and body != "-":
                # 神策 POST body 通常是 data=...&gzip=...
                # 我们直接作为 raw body 发送，或者解析后重新封装
                resp = SESSION.post(NGINX_URL, params=params, data=body, headers=headers, timeout=5)
                return resp.status_code < 400
    except Exception as e:
        print(f"重放失败: {e}")
    
    return False

def process_file(file_path):
    """处理单个日志文件"""
    print(f"正在处理文件: {file_path}")
    count = 0
    success = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(replay_line, lines))
        count = len(results)
        success = sum(1 for r in results if r)
        
    print(f"文件 {file_path} 处理完成: 总计 {count}, 成功 {success}")
    return count, success

def main():
    project_root = Path(__file__).parent.parent
    test_data_path = project_root / TEST_DATA_DIR
    
    if not test_data_path.exists():
        print(f"错误: 测试数据目录不存在 {test_data_path}")
        return

    log_files = sorted(list(test_data_path.glob("nginx_log_part_*.txt")))
    if not log_files:
        print("未找到测试日志文件")
        return

    # 默认只重放第一个文件，避免一次性压测过大
    log_files = log_files[:1]
    print(f"找到 {len(log_files)} 个日志文件，准备开始重放...")
    total_count = 0
    total_success = 0
    
    start_time = time.time()
    for log_file in log_files:
        c, s = process_file(log_file)
        total_count += c
        total_success += s
    
    duration = time.time() - start_time
    print("\n" + "="*40)
    print(f"重放任务完成!")
    print(f"总耗时: {duration:.2f} 秒")
    print(f"总行数: {total_count}")
    print(f"成功数: {total_success}")
    print(f"成功率: {(total_success/total_count*100):.2f}%" if total_count > 0 else "0%")
    print("="*40)

if __name__ == "__main__":
    main()
