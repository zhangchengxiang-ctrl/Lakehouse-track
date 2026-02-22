# 预置事件/属性元数据 — 深度分析与补充建议

## 一、表结构 vs 当前填充情况

### 1. track_event_define（事件定义）

| 列名 | 类型 | 当前预置 INSERT | 文档可提供 | 建议 |
|------|------|-----------------|------------|------|
| project_id | INTEGER | NULL ✓ | - | 保持 |
| name | VARCHAR(100) | ✓ | - | 保持 |
| cname | VARCHAR(100) | ✓ | Display Name | 保持 |
| **comment** | TEXT | 未填 | 触发条件、说明、备注 | **必补**：神策各端文档的「描述/触发时机/Remarks」 |
| **track_info** | TEXT | 未填 | 适用 SDK、生命周期、主要属性 | **建议补**：如 "App: onLaunch/onShow; 微信: App.onLaunch"、"主要属性: $is_first_time, $resume_from_background" |
| visible | SMALLINT | 默认 1 | 文档有「是否默认展示」 | 可选：部分事件可标 0 |
| virtual | SMALLINT | 默认 0 | 虚拟事件标记 | 可选 |
| accepted | SMALLINT | 默认 1 | 文档有「入库校验/是否接受」 | 可选 |
| is_common | SMALLINT | 1 ✓ | - | 保持 |
| bucket_id, is_visualized, virtual_define, total_count, user_id | - | 默认 | - | 预置可不填 |

### 2. track_property_define（属性定义）

| 列名 | 类型 | 当前预置 INSERT | 文档可提供 | 建议 |
|------|------|-----------------|------------|------|
| project_id | INTEGER | NULL ✓ | - | 保持 |
| name | VARCHAR(100) | ✓ | - | 保持 |
| cname | VARCHAR(100) | ✓ | Display Name | 保持 |
| data_type | INTEGER | ✓ | 文档 Data Type (STRING/NUMBER/BOOL/DATETIME) | 已对齐，可校验 |
| unit | VARCHAR(16) | 部分 NULL | 文档 "in seconds", "px", "像素" | **建议补全**：如 $event_duration→秒, $screen_height→像素 |
| **comment** | TEXT | 未填 | Detailed Explanation、说明、取值示例 | **必补**：神策预置属性表的「详细说明」 |
| **track_info** | TEXT | 未填 | Data Source、适用 SDK、Privacy、Remarks | **建议补**：如 "IP Resolution"、"iOS/Android/Web SDK"、"隐私: High" |
| view_column_name | VARCHAR(128) | ✓ 规则生成 | - | 保持 |
| is_common | SMALLINT | 1 ✓ | - | 保持 |
| **is_dimension** | SMALLINT | 未填 | 维度/指标分类 | **可选**：用于分析模型时区分维度与指标 |
| **is_measure** | SMALLINT | 未填 | 同上 | 可选 |
| is_in_use, is_load, has_dict, default_value, type_flexible | - | 默认 | - | 预置可不填或按需 |

---

## 二、神策文档中可落库的信息维度

（来源：预置属性总表、Web/App/小程序/服务端/游戏 SDK 子文档）

| 文档字段/概念 | 建议落入列 | 说明 |
|---------------|------------|------|
| **Detailed Explanation** / 详细说明 | comment | 如 "Duration of the event (in seconds)"、"Whether the distinct_id is a Login ID" |
| **Display Name** | cname | 已有 |
| **Data Type** (STRING/NUMBER/BOOL/DATETIME/LIST) | data_type | 已映射 1/2/6/5 等 |
| **Unit**（单位） | unit | 如 秒、像素、KB；文档多处写 "in seconds", "in pixels" |
| **Trigger condition / Trigger timing** | comment 或 track_info | 事件专用：如 "退到后台或关闭 App 等待 30 秒触发" |
| **Main attributes**（事件主要属性） | track_info | 事件专用：如 "$is_first_time, $resume_from_background" |
| **对应生命周期** | track_info | 如 "App.onLaunch"、"onShow/onHide" |
| **Data Source** | track_info | 如 "Nginx Parsing"、"IP Resolution"、"Backend SDK" |
| **适用 SDK / 平台** | track_info | 如 "iOS SDK, Android SDK, Web SDK, Mini Program SDK" |
| **Privacy Risk** (High/Medium/Low) | track_info | 如 "隐私: High" 便于合规与脱敏 |
| **Auto Tracking** (Yes/No) | track_info | 是否自动采集 |
| **Supports Disabling** | track_info | 是否支持关闭采集 |
| **Remarks / 备注** | comment 或 track_info | 版本要求、特殊说明（如 iOS 16.4 后 $carrier 不可用） |
| **Category**（分类） | comment 前缀或 track_info | 如 "Device-related"、"Channel Related"、"IP-related" |
| **Property Value Example** | comment 或 default_value | 取值示例，便于校验与文档 |

---

## 三、建议的补充优先级

| 优先级 | 内容 | 落地方式 |
|--------|------|----------|
| P0 | **comment**（事件+属性） | 用神策「详细说明/触发条件/备注」填充，便于产品与分析师理解 |
| P0 | **unit** 补全（属性） | 按文档为带单位的属性补 unit（秒、像素、KB 等） |
| P1 | **track_info**（事件+属性） | 结构化或短文本：适用 SDK、Data Source、隐私等级、是否自动采集等 |
| P2 | **is_dimension / is_measure**（属性） | 从文档或约定区分「维度」与「指标」，利于分析模型 |
| P3 | 事件 **visible/accepted** | 若有「默认不展示」或「不参与入库」的预置事件，可单独标 |

---

## 四、实施建议（不局限于 comment）

1. **comment**  
   - 事件：触发条件 + 简短说明 + 备注（含生命周期/版本限制）。  
   - 属性：神策「Detailed Explanation」为主，必要时加取值示例或单位说明。

2. **track_info**  
   - 用约定格式存多段信息，便于解析与展示，例如：  
     - `sdk: iOS,Android,Web`  
     - `source: IP Resolution`  
     - `privacy: Low`  
     - `auto_tracking: yes`  
   - 或存 JSON 片段，便于后续扩展（如 `{"sdks":["iOS","Android"],"source":"IP Resolution"}`）。

3. **unit**  
   - 与神策文档和现有枚举一致：秒、像素、KB 等；缺失的用 NULL，不做猜测。

4. **数据来源与维护**  
   - 从神策「预置属性总表」+ 各端（Web/App/小程序/服务端/游戏）子文档批量解析，生成 (name, comment, unit, track_info, is_dimension, is_measure) 等；  
   - 预置 INSERT 脚本按「名称」关联上述结果，只更新可空列，不改变主键与 is_common 逻辑。

5. **校验与扩展**  
   - 用文档中的 Data Type 校验并修正现有 data_type 枚举（1/2/3/5/6 与 STRING/NUMBER/BOOL/DATETIME 对应）；  
   - 若后续有「预置事件-预置属性」关系（如某事件固定带某属性），可考虑恢复或新增 event–property 关联表/配置，用于校验与展示。

---

## 五、小结

除 **comment** 外，建议同步补充：

- **track_info**：适用 SDK、Data Source、隐私等级、是否自动采集、备注等。  
- **unit**：按文档补全带单位的属性。  
- **is_dimension / is_measure**：按分析需求区分为维度/指标（可选）。  
- 事件的 **track_info**：触发条件摘要、主要属性、生命周期（与 comment 可分工：comment 给人看，track_info 给系统/报表用）。

这样预置元数据既「可读」（comment + cname），又「可算可管」（unit、track_info、维度/指标），且与神策文档对齐，便于后续维护与扩展。
