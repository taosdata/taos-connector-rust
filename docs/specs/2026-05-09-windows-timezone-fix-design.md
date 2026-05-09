# Windows 时区环境变量修复设计

## 问题

`taos-ws-sys` 在 `taos_ws_init()` 初始化时（`src/ws/mod.rs:438`），通过 `std::env::set_var("TZ", ...)` 设置时区环境变量，值为 IANA 格式（如 `Asia/Shanghai`）。

Windows CRT 的 `_tzset()` 函数不识别 IANA 时区名，导致 TZ 环境变量在 Windows 上无效。需要将 IANA 时区名转换为 Windows 支持的 POSIX 偏移量格式。

## 方案

在 `taos-ws-sys/src/ws/` 下新建 `tz.rs` 模块，仅在 `#[cfg(windows)]` 下编译。该模块负责将用户配置的时区字符串（IANA 名、UTC 偏移量等各种格式）转换为 POSIX 偏移量格式的 TZ 值。

参考实现：项目根目录 `c_tz.c` 中 `taosSetGlobalTimezone()` 函数的 Windows 分支。

## 模块结构

```
taos-ws-sys/src/ws/tz.rs  (仅 #[cfg(windows)] 编译)
├── IANA_TO_WIN_TZ: &[(&str, &str)]
│   554 条排序的 IANA → Windows 时区名映射（来自 c_tz.c 的 tz_win 表）
│
├── iana_to_posix_tz(tz: &str) -> String
│   转换入口函数，按以下优先级处理：
│   1. UTC / GMT → "+0:00"
│   2. 已是 POSIX 偏移量（如 "+08:00"、"-5:00"）→ 直接返回
│   3. UTC±N / GMT±N 格式（如 "UTC-8"、"GMT+05:30"）→ 解析并返回偏移量
│   4. IANA 名 → 查映射表得 Windows 时区名 → 查注册表 → 提取偏移量 → 转 POSIX
│   5. 直接作为 Windows 时区名查注册表
│   6. 兜底：未知时区 → "+0:00"（UTC），并记录 warn 日志
│
└── query_registry_offset(win_tz_name: &str) -> Option<(char, i32, i32)>
    通过 FFI 调用 RegGetValueA 查询注册表：
    路径：HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Time Zones\{name}
    字段：Display
    格式：(UTC+08:00) Beijing, Chongqing, Hong Kong, Urumqi
    返回：(符号, 小时, 分钟)
```

## 关键行为

### POSIX TZ 符号约定

POSIX TZ 环境变量使用 **东为负** 约定，与 UTC 偏移量方向相反：
- UTC+8（东八区，如 `Asia/Shanghai`）→ TZ 值为 `-8:00`
- UTC-5（西五区，如 `America/New_York`）→ TZ 值为 `+5:00`

这与 `c_tz.c` 中 `taosSetGlobalTimezone()` 的行为一致。

### 注册表查询

- 注册表路径：`SOFTWARE\Microsoft\Windows NT\CurrentVersion\Time Zones\{Windows时区名}`
- 查询字段：`Display`（类型 REG_SZ）
- Display 值格式示例：`(UTC+08:00) Beijing, Chongqing, Hong Kong, Urumqi`
- 解析位置：`[4]` 为符号（`+`/`-`），`[5:7]` 为小时，`[8:10]` 为分钟
- 使用 `RegGetValueA` Win32 API（通过 `extern "system"` FFI 声明）

### 映射表

IANA→Windows 时区名映射表共 554 条，来源于 `c_tz.c` 的 `tz_win` 数组。表按 IANA 名排序，可使用二分查找。

## 修改点

### `taos-ws-sys/src/ws/mod.rs`

`taos_ws_init()` 中设置 TZ 环境变量处（第 438 行）：

```rust
// 修改前：
unsafe { std::env::set_var("TZ", config::timezone().as_str()) };

// 修改后：
let tz_value = tz::iana_to_posix_tz(config::timezone().as_str());
unsafe { std::env::set_var("TZ", &tz_value) };
```

在 Windows 上 `iana_to_posix_tz` 执行转换；在非 Windows 上该函数直接返回原值。

### `taos-ws-sys/src/ws/mod.rs` 模块声明

添加 `mod tz;` 声明。

### 新建 `taos-ws-sys/src/ws/tz.rs`

包含上述转换逻辑和映射表。

## 测试

### 单元测试（`tz.rs` 内）

- `test_utc_gmt`：验证 "UTC"、"GMT" → "+0:00"
- `test_posix_passthrough`：验证 "+08:00"、"-5:00" 直接返回
- `test_utc_offset_format`：验证 "UTC-8"、"GMT+5:30" 等格式的解析
- `test_iana_lookup`：验证 IANA 名查映射表（如 "Asia/Shanghai"、"America/New_York"）
- `test_unknown_fallback`：验证未知时区回退到 UTC

注意：涉及注册表查询的测试仅在 Windows CI 上有意义。映射表查找的测试可以在所有平台运行。

## 不在范围内

- `chrono::Local` 在 Windows 上不读 TZ 环境变量的问题（`to_datetime_with_tz()` 相关）— 后续单独处理
- `taos-ws`（非 sys）层的时区处理
- 夏令时（DST）动态切换支持（TZ 环境变量本身不支持 DST 动态切换）
