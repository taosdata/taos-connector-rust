# Windows 时区修复设计（chrono_tz 方案）

## 问题

`taos-ws-sys` 存在两个时区相关问题：

1. **Windows TZ 环境变量无效**：`taos_ws_init()` 通过 `std::env::set_var("TZ", ...)` 设置 IANA 格式时区名（如 `Asia/Shanghai`），但 Windows CRT 不识别 IANA 时区名，导致 TZ 环境变量在 Windows 上无效。
2. **默认时区硬编码**：`DEFAULT_TIMEZONE` 硬编码为 `"Asia/Shanghai"`，非中国时区的用户如果未显式配置时区，会得到错误的时区设置。

## 方案

### 核心思路

使用 `chrono_tz` crate 解析 IANA 时区名并动态计算 UTC 偏移量，生成 Windows CRT 兼容的 POSIX TZ 格式。同时使用 `iana-time-zone` crate 自动检测系统时区替代硬编码默认值。

### 修复 1：Windows TZ 环境变量转换

在 `taos-ws-sys/src/ws/` 下新建 `tz.rs` 模块，提供 `set_tz_env(tz: &str)` 函数：

- **非 Windows**：直接 `std::env::set_var("TZ", tz)`，行为不变。
- **Windows**：
  1. 用 `chrono_tz` 解析 IANA 名：`tz.parse::<chrono_tz::Tz>()`
  2. 用 `chrono::Utc::now()` 获取当前 UTC 偏移量（秒数）
  3. 符号取反生成 POSIX 格式：`UTC{±}{hours}:{minutes:02}`
  4. `_putenv` 设置 CRT 环境变量
  5. `std::env::set_var` 设置进程环境变量
  6. 调用 `_tzset()` 刷新 CRT 时区缓存

### 修复 2：默认时区自动检测

将 `DEFAULT_TIMEZONE` 从硬编码 `"Asia/Shanghai"` 改为使用 `iana_time_zone::get_timezone()` 动态获取系统时区。检测失败时回退到 `"UTC"`。

## 模块结构

```
taos-ws-sys/src/ws/tz.rs
│
├── pub fn set_tz_env(tz: &str)
│   公开入口函数，所有平台可调用：
│   - 非 Windows：直接设置 TZ 为原值
│   - Windows：
│     1. 调用 iana_to_posix_tz 转换
│     2. _putenv 设置 CRT 环境
│     3. std::env::set_var 设置进程环境
│     4. 调用 _tzset()
│
└── [#[cfg(windows)]] fn iana_to_posix_tz(tz: &str) -> String
    Windows 专用转换函数：
    1. 解析 IANA 时区名为 chrono_tz::Tz
    2. 计算当前 UTC 偏移量（local_minus_utc 秒数）
    3. 符号取反，格式化为 POSIX TZ 字符串
    4. 解析失败 → warn 日志 + 回退 "UTC+0:00"
```

## 关键行为

### POSIX TZ 符号约定

POSIX TZ 使用 **东为负、西为正** 约定（`UTC` 前缀 + 偏移量），与常规 UTC 偏移量方向相反：

| 时区 | chrono `local_minus_utc()` | POSIX TZ 值 |
|------|---------------------------|------------|
| Asia/Shanghai (UTC+8) | +28800 (+8h) | `UTC-8:00` |
| America/New_York (UTC-5) | -18000 (-5h) | `UTC+5:00` |
| UTC | 0 | `UTC+0:00` |

**转换公式**：取反 `local_minus_utc()` 的符号即为 POSIX TZ 的符号。

### _putenv + std::env::set_var

Windows 上需要同时调用两者：
- `_putenv`：更新 CRT 环境变量（`_tzset()` 从 CRT 环境读取）
- `std::env::set_var`：更新进程环境变量（保持 Rust 侧一致性）

### _tzset() 调用

设置 TZ 后**必须调用 `_tzset()`**，否则 CRT 不会刷新时区缓存。通过 FFI 声明 `extern "C" { fn _tzset(); }` 调用。

### 未知时区处理

当 IANA 时区名无法被 `chrono_tz` 解析时：
- 记录 `warn!` 日志，包含原始时区值
- 回退到 UTC（设置 TZ 为 `UTC+0:00`）

### DST 说明

`chrono::Utc::now()` 获取的偏移量反映 init 时刻的 DST 状态。如果进程运行期间 DST 发生切换，TZ 不会自动更新。这是 POSIX TZ 环境变量机制的固有限制。

## 修改点

### 新增依赖（`taos-ws-sys/Cargo.toml`）

```toml
chrono = "0.4"
chrono-tz = "0.10.4"
iana-time-zone = "0.1"
```

### 新建 `taos-ws-sys/src/ws/tz.rs`

包含 `set_tz_env` 和 Windows 条件编译的 `iana_to_posix_tz` 转换逻辑。

### 修改 `taos-ws-sys/src/ws/mod.rs`

1. 添加 `mod tz;`
2. 替换 `taos_ws_init()` 中的 TZ 设置：

```rust
// 修改前：
unsafe { std::env::set_var("TZ", config::timezone().as_str()) };

// 修改后：
tz::set_tz_env(config::timezone().as_str());
```

### 修改 `taos-ws-sys/src/ws/config.rs`

将 `DEFAULT_TIMEZONE` 从硬编码常量改为动态获取系统时区：

```rust
// 修改前：
const DEFAULT_TIMEZONE: &str = "Asia/Shanghai";

// 修改后：
fn default_timezone() -> String {
    iana_time_zone::get_timezone().unwrap_or_else(|_| "UTC".to_string())
}
```

## 测试

### 所有平台

| 测试 | 验证内容 |
|------|---------|
| `test_set_tz_env_sets_env_var` | `set_tz_env` 后 `std::env::var("TZ")` 返回正确值 |
| `test_default_timezone` | `default_timezone()` 返回非空有效 IANA 时区名 |

### 仅 Windows（`#[cfg(windows)]`）

| 测试 | 验证内容 |
|------|---------|
| `test_iana_to_posix_shanghai` | `"Asia/Shanghai"` → `"UTC-8:00"` |
| `test_iana_to_posix_new_york` | `"America/New_York"` → `"UTC+5:00"` 或 `"UTC+4:00"`（夏令时） |
| `test_iana_to_posix_utc` | `"UTC"` → `"UTC+0:00"` |
| `test_unknown_fallback` | 无法解析 → `"UTC+0:00"` + warn 日志 |

## 不在范围内

- `chrono::Local` 在 Windows 上不读 TZ 环境变量的问题
- `taos-ws`（非 sys）层的时区处理
