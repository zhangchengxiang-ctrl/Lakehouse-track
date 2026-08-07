# PRODUCT.md — 宿主产品寄存器

## 产品

- 一句话产品：埋点数据平台——采集神策协议事件，落入数据湖，并在数仓层可查可分析。
- 主要用户：数据工程、数据分析师。
- 核心 Job（3 条内）：
  1. 稳定采集与协议解码 / enrichment（UA、GeoIP、基础校验）→ S3 → StarRocks Pipe
  2. CDC 入湖（PostgreSQL → Flink → Paimon），支持重放与 schema 演进
  3. 数仓查询面（StarRocks Catalog / Pipe / SQL）支撑分析取数

## 反参考

- 不要像什么：AI 问答助手、插件宿主、运营后台
- 明确不做：对话 UI、知识卡片、agent-kit、对账门 / AI 质量门（本期）

## 表面默认

- Default UI surface: n/a
- 典型 visitor_mode: operate（运维 / 取数）

## 成功信号

- `make verify` 通过：模拟埋点经 HTTP 进入后，S3 staging / Pipe 有数据，StarRocks 能查出对应事件；CDC 链路经 `make run-sql ARGS=flink` 后 Paimon Catalog 可读

## 备注

- 运行时真源：根 `README.md` + `services/`
- 设计草稿：`docs/architecture/完整方案-可落地版.md`（可能落后于代码）
