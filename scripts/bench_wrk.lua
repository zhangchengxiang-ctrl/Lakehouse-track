-- wrk Lua 脚本：模拟神策 SDK 埋点 POST 请求
-- 用法: wrk -t4 -c100 -d30s -s scripts/bench_wrk.lua http://localhost:8080/sa?project=default
--
-- 每个请求发送一个 base64 编码的 JSON 事件 payload（与真实 SDK 行为一致）

local counter = 0

-- 预编码的神策事件 payload (base64, 未压缩)
-- 包含 distinct_id, event, type, properties 等标准字段
local payloads = {
  "W3siZGlzdGluY3RfaWQiOiJ0ZXN0X3VzZXJfMDAxIiwiZXZlbnQiOiIkcGFnZXZpZXciLCJ0eXBlIjoidHJhY2siLCJ0aW1lIjoxNzA5MjAwMDAwMDAwLCJwcm9wZXJ0aWVzIjp7IiRvcyI6ImlPUyIsIiRvc192ZXJzaW9uIjoiMTcuMyIsIiRsaWIiOiJpT1MiLCIkbGliX3ZlcnNpb24iOiI0LjUuMCIsIiR1cmwiOiJodHRwczovL2V4YW1wbGUuY29tL2hvbWUiLCIkdGl0bGUiOiLpppbpobUiLCIkc2NyZWVuX3dpZHRoIjozOTAsIiRzY3JlZW5faGVpZ2h0Ijo4NDQsIiR3aWZpIjp0cnVlLCIkbmV0d29ya190eXBlIjoiV0lGSSJ9LCJsb2dpbl9pZCI6InVzZXJfMTIzNDUiLCJhbm9ueW1vdXNfaWQiOiIxOGY5OTc3M2Q3ZjJhOSJ9XQ==",
  "W3siZGlzdGluY3RfaWQiOiJ0ZXN0X3VzZXJfMDAyIiwiZXZlbnQiOiJidXR0b25fY2xpY2siLCJ0eXBlIjoidHJhY2siLCJ0aW1lIjoxNzA5MjAwMDAwMDAwLCJwcm9wZXJ0aWVzIjp7IiRvcyI6IkFuZHJvaWQiLCIkb3NfdmVyc2lvbiI6IjE0IiwiJGxpYiI6IkFuZHJvaWQiLCIkbGliX3ZlcnNpb24iOiI2LjcuMiIsIiRlbGVtZW50X2lkIjoiYnRuX3N1Ym1pdCIsIiRlbGVtZW50X3R5cGUiOiJidXR0b24iLCIkc2NyZWVuX25hbWUiOiJPcmRlclBhZ2UiLCIkd2lmaSI6ZmFsc2V9LCJsb2dpbl9pZCI6InVzZXJfNjc4OTAiLCJhbm9ueW1vdXNfaWQiOiIxOGZiYjEyM2M0ZDVlNiJ9XQ==",
  "W3siZGlzdGluY3RfaWQiOiJ0ZXN0X3VzZXJfMDAzIiwiZXZlbnQiOiIkQXBwU3RhcnQiLCJ0eXBlIjoidHJhY2siLCJ0aW1lIjoxNzA5MjAwMDAwMDAwLCJwcm9wZXJ0aWVzIjp7IiRvcyI6ImlPUyIsIiRvc192ZXJzaW9uIjoiMTYuNSIsIiRsaWIiOiJpT1MiLCIkbGliX3ZlcnNpb24iOiI0LjQuMSIsIiRyZXN1bWVfZnJvbV9iYWNrZ3JvdW5kIjpmYWxzZSwiJHNjcmVlbl93aWR0aCI6Mzc1LCIkc2NyZWVuX2hlaWdodCI6ODEyfSwibG9naW5faWQiOiIiLCJhbm9ueW1vdXNfaWQiOiIxOGZhYTk4N2I2YzNkMSJ9XQ==",
}

function setup(thread)
  thread:set("id", counter)
  counter = counter + 1
end

function init(args)
  math.randomseed(os.time() + id)
end

function request()
  local idx = math.random(#payloads)
  local body = "data=" .. payloads[idx]

  wrk.method = "POST"
  wrk.body = body
  wrk.headers["Content-Type"] = "application/x-www-form-urlencoded"
  wrk.headers["User-Agent"] = "SensorsAnalytics iOS SDK"
  wrk.headers["X-Forwarded-For"] = string.format("%d.%d.%d.%d",
    math.random(1, 223), math.random(0, 255),
    math.random(0, 255), math.random(1, 254))

  return wrk.format(nil, "/sa?project=default")
end

function done(summary, latency, requests)
  io.write("\n========== wrk 压测报告 ==========\n")
  io.write(string.format("  持续时间:     %.2f 秒\n", summary.duration / 1e6))
  io.write(string.format("  总请求数:     %d\n", summary.requests))
  io.write(string.format("  吞吐量:       %.0f req/s\n", summary.requests / (summary.duration / 1e6)))
  io.write(string.format("  传输数据:     %.2f MB\n", summary.bytes / 1024 / 1024))
  io.write(string.format("  延迟 avg:     %.2f ms\n", latency.mean / 1000))
  io.write(string.format("  延迟 P50:     %.2f ms\n", latency:percentile(50) / 1000))
  io.write(string.format("  延迟 P90:     %.2f ms\n", latency:percentile(90) / 1000))
  io.write(string.format("  延迟 P99:     %.2f ms\n", latency:percentile(99) / 1000))
  io.write(string.format("  延迟 max:     %.2f ms\n", latency.max / 1000))
  io.write(string.format("  连接错误:     %d\n", summary.errors.connect))
  io.write(string.format("  读取错误:     %d\n", summary.errors.read))
  io.write(string.format("  写入错误:     %d\n", summary.errors.write))
  io.write(string.format("  超时:         %d\n", summary.errors.timeout))
  io.write(string.format("  非2xx/3xx:    %d\n", summary.errors.status))
  io.write("==================================\n")
end
