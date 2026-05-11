# Windows 时区修复设计（chrono_tz 方案）

## 问题

`taos-ws-sys` 存在两个时区相关问题：

1. **Windows TZ 环境变量无效**：`taos_init()` (mod.rs:406) 调用 `taos_init_impl()` (mod.rs:417)，其中通过 `std::env::set_var("TZ", ...)` 设置 IANA 格式时区名（如 `Asia/Shanghai`），但 Windows CRT 不识别 IANA 时区名，导致 TZ 环境变量在 Windows 上无效。
2. **默认时区硬编码**：`DEFAULT_TIMEZONE` 硬编码为 `"Asia/Shanghai"`，非中国时区的用户如果未显式配置时区，会得到错误的时区设置。

## 方案

### 核心思路

使用 `chrono_tz` crate 解析 IANA 时区名并动态计算 UTC 偏移量，生成 Windows CRT 兼容的 POSIX TZ 格式。同时使用 `iana-time-zone` crate 自动检测系统时区替代硬编码默认值。

### 修复 1：Windows TZ 环境变量转换

在 `taos-ws-sys/src/ws/` 下新建 `tz.rs` 模块，提供 `set_tz_env(tz: &str)` 函数：

- **非 Windows**：直接 `std::env::set_var("TZ", tz)`，行为不变。
- **Windows**：
  1. 用 `chrono_tz` 解析 IANA 名：`tz.parse::<chrono_tz::Tz>()`
  2. 计算目标时区的当前 UTC 偏移量（秒数）：
     `chrono::Utc::now().with_timezone(&parsed_tz).offset().fix().local_minus_utc()`
  3. 符号取反生成 POSIX 格式：`UTC{±}{hours}:{minutes:02}`
  4. 通过 FFI 调用 `_putenv` 设置 CRT 环境变量
  5. `std::env::set_var` 设置进程环境变量
  6. 通过 FFI 调用 `_tzset()` 刷新 CRT 时区缓存

### 修复 2：默认时区自动检测

将 `DEFAULT_TIMEZONE` 从硬编码 `"Asia/Shanghai"` 改为使用 `iana_time_zone::get_timezone()` 动态获取系统 IANA 时区名。检测失败时回退到 IANA 名 `"UTC"`。

## 模块结构

```
taos-ws-sys/src/ws/tz.rs
│
├── pub fn set_tz_env(tz: &str)
│   公开入口函数，所有平台可调用：
│   - 非 Windows：直接 std::env::set_var("TZ", tz)（IANA 名原样传递）
│   - Windows：
│     1. 调用 iana_to_posix_tz(tz) 将 IANA → POSIX 格式
│     2. FFI 调用 _putenv("TZ={posix}") 更新 CRT 环境
│     3. std::env::set_var("TZ", posix) 更新进程环境
│     4. FFI 调用 _tzset() 刷新 CRT 时区缓存
│
└── [#[cfg(windows)]] fn iana_to_posix_tz(tz: &str) -> String
    Windows 专用转换函数：
    1. 解析 IANA 时区名：tz.parse::<chrono_tz::Tz>()
    2. 计算偏移量：Utc::now().with_timezone(&tz).offset().fix().local_minus_utc()
    3. 符号取反，格式化为 POSIX TZ 字符串
    4. 解析失败 → warn! 日志 + 回退 POSIX 格式 "UTC+0:00"
```

## 关键行为

### 两层回退策略

| 层 | 正常值 | 回退值 | 格式 |
|----|--------|--------|------|
| 配置层（`default_timezone()`） | 系统 IANA 名（如 `"Asia/Shanghai"`） | `"UTC"` | IANA |
| 环境变量层（`iana_to_posix_tz()`） | 计算得到的 POSIX 偏移量（如 `"UTC-8:00"`） | `"UTC+0:00"` | POSIX |

配置层始终使用 IANA 格式，环境变量层（仅 Windows）始终使用 POSIX 格式。两层各自独立回退。

### POSIX TZ 符号约定

POSIX TZ 使用 **东为负、西为正** 约定（`UTC` 前缀 + 偏移量），与常规 UTC 偏移量方向相反：

| 时区 | chrono `local_minus_utc()` | POSIX TZ 值 |
|------|---------------------------|------------|
| Asia/Shanghai (UTC+8) | +28800 (+8h) | `UTC-8:00` |
| America/New_York (UTC-5, 标准时) | -18000 (-5h) | `UTC+5:00` |
| America/New_York (UTC-4, 夏令时) | -14400 (-4h) | `UTC+4:00` |
| UTC | 0 | `UTC+0:00` |

**转换公式**：取反 `local_minus_utc()` 的符号即为 POSIX TZ 的符号。

### _putenv FFI 声明与调用

```rust
#[cfg(windows)]
extern "C" {
    fn _putenv(envstring: *const std::ffi::c_char) -> std::ffi::c_int;
    fn _tzset();
}
```

调用方式：

```rust
let env_str = std::ffi::CString::new(format!("TZ={posix}")).unwrap();
unsafe { _putenv(env_str.as_ptr()) };
unsafe { std::env::set_var("TZ", &posix) };
unsafe { _tzset() };
```

`_putenv` 接受 `"KEY=VALUE"` 格式的 C 字符串，更新 CRT 内部环境表。`_tzset()` 随后从该表读取 TZ 值并刷新时区缓存。`std::env::set_var` 则更新 Win32 进程环境块（通过 `SetEnvironmentVariableW`），保持 Rust 侧 `std::env::var("TZ")` 的一致性。

### 未知时区处理

当 IANA 时区名无法被 `chrono_tz` 解析时：
- 记录 `warn!("Unknown timezone: {tz}, falling back to UTC")` 日志
- 环境变量层回退到 POSIX 格式 `"UTC+0:00"`

### DST（夏令时）说明

`chrono::Utc::now().with_timezone(&tz).offset()` 获取的偏移量反映 **调用时刻** 的 DST 状态：

- 若 init 时处于标准时间，偏移量为标准时间偏移（如 New York EST = UTC-5 → POSIX `UTC+5:00`）
- 若 init 时处于夏令时，偏移量为夏令时偏移（如 New York EDT = UTC-4 → POSIX `UTC+4:00`）
- 进程运行期间 DST 切换**不会**自动更新 TZ 环境变量

这是 POSIX TZ 固定偏移量机制的固有限制，与 c_tz.c 行为一致。

## 修改点

### 新增依赖（`taos-ws-sys/Cargo.toml`）

```toml
chrono = "0.4"
chrono-tz = "0.10.4"
iana-time-zone = "0.1"
```

### 新建 `taos-ws-sys/src/ws/tz.rs`

包含 `set_tz_env` 和 Windows 条件编译的 `iana_to_posix_tz` 转换逻辑及 FFI 声明。

### 修改 `taos-ws-sys/src/ws/mod.rs`

1. 添加 `mod tz;`
2. 替换 `taos_init_impl()` 中的 TZ 设置（第 438 行）：

```rust
// 修改前：
unsafe { std::env::set_var("TZ", config::timezone().as_str()) };

// 修改后：
tz::set_tz_env(config::timezone().as_str());
```

### 修改 `taos-ws-sys/src/ws/config.rs`

#### 接口变更分析

当前 `Config::timezone()` 返回 `&FastStr`，使用 `static FastStr` 持有默认值：

```rust
fn timezone(&self) -> &FastStr {
    static TIMEZONE: FastStr = FastStr::from_static_str(DEFAULT_TIMEZONE);
    self.timezone.as_ref().unwrap_or(&TIMEZONE)
}
```

`FastStr::from_static_str` 要求 `&'static str`，无法用于动态生成的 `String`。

**修改方案**：使用 `OnceLock<FastStr>` 延迟初始化默认时区值：

```rust
fn timezone(&self) -> &FastStr {
    static DEFAULT_TZ: OnceLock<FastStr> = OnceLock::new();
    self.timezone.as_ref().unwrap_or_else(|| {
        DEFAULT_TZ.get_or_init(|| {
            FastStr::new(
                iana_time_zone::get_timezone().unwrap_or_else(|_| "UTC".to_string())
            )
        })
    })
}
```

**外部接口不变**：`config::timezone()` 仍返回 `FastStr`（通过 `clone()`），所有调用方（`taos_init_impl` 中的 `.as_str()` 调用、config `show!` 宏、测试）无需修改。

移除不再使用的 `const DEFAULT_TIMEZONE: &str = "Asia/Shanghai";`。

## 测试

### 所有平台

| 测试 | 验证内容 |
|------|---------|
| `test_set_tz_env_sets_env_var` | `set_tz_env("Asia/Shanghai")` 后 `std::env::var("TZ")` 返回正确值（非 Windows 为 IANA，Windows 为 POSIX） |
| `test_default_timezone_non_empty` | 系统时区检测返回非空有效字符串 |

### 仅 Windows（`#[cfg(windows)]`）

| 测试 | 验证内容 |
|------|---------|
| `test_iana_to_posix_shanghai` | `"Asia/Shanghai"` → `"UTC-8:00"` |
| `test_iana_to_posix_new_york_dst` | `"America/New_York"` → `"UTC+5:00"` 或 `"UTC+4:00"`（取决于当前 DST 状态） |
| `test_iana_to_posix_utc` | `"UTC"` → `"UTC+0:00"` |
| `test_iana_to_posix_half_hour_offset` | `"Asia/Kolkata"` → `"UTC-5:30"`（验证非整数小时偏移） |
| `test_iana_to_posix_negative_offset` | `"Pacific/Apia"` (UTC+13) → `"UTC-13:00"`（验证大偏移量） |
| `test_unknown_fallback` | `"invalid_tz"` → `"UTC+0:00"` + warn 日志 |
| `test_set_tz_env_windows` | `set_tz_env("Asia/Shanghai")` 后 TZ 环境变量为 POSIX 格式 |

## 不在范围内

- `chrono::Local` 在 Windows 上不读 TZ 环境变量的问题
- `taos-ws`（非 sys）层的时区处理
