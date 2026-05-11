# Windows 时区环境变量修复设计 (Deprecated)

## 问题

`taos-ws-sys` 在 `taos_ws_init()` 初始化时（`src/ws/mod.rs:438`），通过 `std::env::set_var("TZ", ...)` 设置时区环境变量，值为 IANA 格式（如 `Asia/Shanghai`）。

Windows CRT 的 `_tzset()` 函数不识别 IANA 时区名，导致 TZ 环境变量在 Windows 上无效。需要将 IANA 时区名转换为 Windows 支持的 POSIX 偏移量格式。

## 方案

在 `taos-ws-sys/src/ws/` 下新建 `tz.rs` 模块。该模块提供 `set_tz_env(tz: &str)` 函数，负责将用户配置的时区字符串转换为 POSIX 偏移量格式并设置到 TZ 环境变量。

- 在 **Windows** 上：执行 IANA→POSIX 偏移量转换，设置 TZ，然后调用 `_tzset()` 使 CRT 生效。
- 在**非 Windows** 上：直接设置 TZ 为原始 IANA 时区名（行为不变）。

参考实现：项目根目录 `c_tz.c` 中 `taosSetGlobalTimezone()` 函数的 Windows 分支（第 1010-1124 行）。

## 模块结构

```
taos-ws-sys/src/ws/tz.rs
│
├── set_tz_env(tz: &str)
│   公开入口函数，所有平台可调用：
│   - Windows：调用 iana_to_posix_tz 转换后设置 TZ，再调用 _tzset()
│   - 非 Windows：直接设置 TZ 为原值
│
├── [#[cfg(windows)]] iana_to_posix_tz(tz: &str) -> Option<String>
│   Windows 专用转换函数，按以下优先级处理：
│   1. "UTC" / "GMT"（不区分大小写）→ Some("+0:00")
│   2. 带 UTC/GMT 前缀的偏移量（"UTC-8"、"GMT+05:30"、"UTC-08:30"）
│      → 直接提取符号和数值，**不反转符号**（输入已是 POSIX 约定）
│      → sscanf 等效：支持 "8"（无冒号无前导零）和 "08:30"（带冒号）
│   3. 裸偏移量（"+08:00"、"-5:00"）→ 直接提取，不反转
│   4. IANA 名 → 查 IANA_TO_WIN_TZ 映射表 → 查注册表 → 提取偏移量
│      → **反转符号**（注册表 Display 使用 UTC 方向，需转为 POSIX 方向）
│   5. 直接作为 Windows 时区名查注册表 → 同样反转符号
│   6. 未匹配 → None
│
├── [#[cfg(windows)]] IANA_TO_WIN_TZ: &[(&str, &str)]
│   554 条 IANA → Windows 时区名映射（来自 c_tz.c 的 tz_win 数组）
│   注意：c_tz.c 的 tz_win 第一项 "Asia/Shanghai" 打破字母序（特殊优先项），
│   其余按字母排序。Rust 实现中将整个表按字母序排列，查找使用线性扫描
│   （与 c_tz.c 第 858-861 行 for 循环一致）。
│
└── [#[cfg(windows)]] query_registry_offset(win_tz: &str) -> Option<(char, i32, i32)>
    通过 FFI 调用 RegGetValueA 查注册表 Display 字段：
    1. 先验证返回的 keyValueSize > 10（与 c_tz.c 第 815 行一致）
    2. 验证 keyValue[4] 是 '+' 或 '-'
    3. 验证 keyValue[5..7] 和 keyValue[8..10] 是合法数字
    4. 验证小时 0-23、分钟 0-59
    任一验证失败 → 返回 None（不 panic）
    返回：(注册表中的原始符号, 小时, 分钟)
```

## 关键行为

### POSIX TZ 符号约定

POSIX TZ 环境变量使用 **东为负、西为正** 约定，与 UTC 偏移量方向相反：

| 时区 | UTC 方向 | POSIX TZ 值 |
|------|---------|------------|
| Asia/Shanghai | UTC+08:00 (东八区) | `-8:00` |
| America/New_York | UTC-05:00 (西五区) | `+5:00` |
| UTC | UTC±00:00 | `+0:00` |

### 符号反转规则（重要）

**两条路径有不同的符号处理逻辑：**

1. **UTC±N / GMT±N / 裸偏移量路径**：符号**不反转**，因为 TDengine 配置文件中的 "UTC-8" 表示东八区，已经是 POSIX 约定（东为负）。TZ 直接设为 "-8:00"。
   - 对应 c_tz.c 第 1039-1051 行：`snprintf(winStr, ..., "%c%d:%02d", sign, ...)`

2. **IANA / Windows 时区名→注册表路径**：符号**需要反转**，因为注册表 Display 的 "+08:00" 表示 UTC+8（UTC 方向，东为正），需要反转为 POSIX 的 "-8:00"（东为负）。
   - 对应 c_tz.c 第 1073-1081 行：`tzSign = (origSign == '+') ? '-' : '+'`

### 注册表查询

- 注册表路径：`HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Time Zones\{Windows时区名}`
- 查询字段：`Display`（类型 REG_SZ）
- Display 值格式示例：`(UTC+08:00) Beijing, Chongqing, Hong Kong, Urumqi`
- **安全解析**：
  1. 先检查 `keyValueSize > 10`（与 c_tz.c 第 815 行一致）
  2. 在 Rust 中使用 `get()` 安全索引或 slice bounds check，不直接 `[]` 索引
  3. 验证字符内容合法后再解析数字
- 使用 `RegGetValueA` Win32 API（通过 `extern "system"` FFI 声明）

### _tzset() 调用

设置 TZ 环境变量后**必须调用 `_tzset()`**（对应 c_tz.c 第 1107 行），否则 CRT 不会刷新内部时区缓存。通过 FFI 声明 `extern "C" { fn _tzset(); }` 调用。

### 未知时区处理

当时区名无法匹配任何已知格式时：

- 记录 `warn!` 日志，包含原始时区值
- 回退到 UTC（设置 TZ 为 "+0:00"）
- **与 c_tz.c 的差异**：c_tz.c 的 `taosSetGlobalTimezone` 返回 `TSDB_CODE_INVALID_CFG` 失败退出（第 1102 行）。但在 taos-ws-sys 的 `taos_ws_init()` 场景中，失败会阻止整个库初始化，影响面过大，因此选择 warn + 回退 UTC。

### 映射表

IANA→Windows 时区名映射表共 554 条，来源于 `c_tz.c` 的 `tz_win` 数组（第 178-731 行）。

- c_tz.c 中该表第一项 `("Asia/Shanghai", "China Standard Time")` 打破字母序（其余按字母排列）
- Rust 实现中将整个表按字母序排列，查找使用线性扫描（与 c_tz.c 第 858-861 行 `for` 循环一致）
- 排序仅为可读性，不使用二分查找

### 支持的输入格式

| 输入格式 | 示例 | TZ 输出 | 说明 |
|---------|------|---------|-----|
| 纯 UTC/GMT | `"UTC"`, `"GMT"`, `"utc"` | `"+0:00"` | 不区分大小写 |
| UTC/GMT 带整数偏移 | `"UTC-8"`, `"GMT+5"` | `"-8:00"`, `"+5:00"` | 无冒号、无前导零 |
| UTC/GMT 带完整偏移 | `"UTC-08:30"`, `"GMT+05:30"` | `"-8:30"`, `"+5:30"` | 带冒号 |
| 裸偏移量 | `"+08:00"`, `"-5:00"` | `"+8:00"`, `"-5:00"` | 直接传递 |
| IANA 时区名 | `"Asia/Shanghai"` | `"-8:00"` | 查映射表→注册表 |
| Windows 时区名 | `"China Standard Time"` | `"-8:00"` | 直接查注册表 |
| 未知 | `"invalid_tz"` | `"+0:00"` | warn 日志 + UTC 回退 |

偏移量验证范围：小时 0-23，分钟 0-59（与 c_tz.c 第 784 行一致）。

## 修改点

### `taos-ws-sys/src/ws/mod.rs`

1. 添加 `mod tz;` 模块声明。

2. `taos_ws_init()` 中设置 TZ 环境变量处（第 438 行）：

```rust
// 修改前：
unsafe { std::env::set_var("TZ", config::timezone().as_str()) };

// 修改后：
tz::set_tz_env(config::timezone().as_str());
```

### 新建 `taos-ws-sys/src/ws/tz.rs`

包含上述转换逻辑和映射表。公开接口 `set_tz_env` 在所有平台编译，内部转换逻辑通过 `#[cfg(windows)]` 条件编译。

## 测试

### 单元测试（`tz.rs` 内）

所有平台可运行的测试（测试映射表查找和格式解析逻辑）：
- `test_utc_gmt`：验证 "UTC"、"GMT"、"utc" → "+0:00"
- `test_utc_offset_integer`：验证 "UTC-8"、"GMT+5" → "-8:00"、"+5:00"
- `test_utc_offset_full`：验证 "UTC-08:30"、"GMT+05:30"
- `test_raw_offset`：验证 "+08:00"、"-5:00" 直接传递
- `test_offset_validation`：验证超出范围的偏移量（如 ±24:00）被拒绝
- `test_iana_table_lookup`：验证映射表中能找到 "Asia/Shanghai"、"America/New_York" 等
- `test_unknown_fallback`：验证未知时区回退到 "+0:00" 并记录 warn 日志

仅 Windows 可运行的测试（需要注册表）：
- `test_registry_query`：验证注册表查询 "China Standard Time" 返回正确偏移量
- `test_iana_to_posix_full`：端到端验证 "Asia/Shanghai" → "-8:00"
- `test_set_tz_env`：验证 set_tz_env 后 TZ 环境变量值正确

## DST（夏令时）限制

POSIX TZ 环境变量本质上表示一个固定偏移量，不支持 DST 动态切换。这意味着：

- **在 DST 生效的时区**（如 `America/New_York`，EST/EDT），TZ 偏移量是固定的（基于注册表中的标准时间偏移），**不会在夏令时期间自动调整**。
- **潜在影响**：在夏令时生效期间，基于 TZ 环境变量的时间计算可能偏差 1 小时。
- **与 c_tz.c 行为一致**：c_tz.c 的 `taosSetGlobalTimezone` 在 Windows 上同样设置固定偏移量到 TZ。
- 这是 TZ 环境变量机制的固有限制，不属于本次修复范围。

## 不在范围内

- `chrono::Local` 在 Windows 上不读 TZ 环境变量的问题（`to_datetime_with_tz()` 相关）— 后续单独处理
- `taos-ws`（非 sys）层的时区处理
