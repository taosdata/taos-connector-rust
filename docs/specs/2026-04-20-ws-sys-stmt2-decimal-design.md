# taos-ws-sys stmt2 Decimal 类型支持

## 背景

`taos-ws-sys/src/ws/stmt2.rs` 提供 C FFI 层的 stmt2 参数绑定接口。当前不支持 Decimal/Decimal64 类型。query.rs 的查询路径已支持 decimal 读取，但 stmt2 的写入路径尚未实现。

参考文档：
- `docs/fs/decimal-data-type-fs.md`
- `docs/fs/stmt2-support-decimal-write-fs.md`

## 问题分析

`TAOS_STMT2_BINDV::to_bytes()` 将 C 层绑定数据序列化为 WebSocket 二进制协议。序列化逻辑中通过 `Ty::fixed_length()` 判断类型是定长还是变长：

- `fixed_length() == 0` → 变长类型（VarChar, NChar 等），需写入每行长度数组
- `fixed_length() > 0` → 定长类型（Int, BigInt 等），按固定字节数复制

问题在于 `Ty::Decimal64.fixed_length() = 8`，`Ty::Decimal.fixed_length() = 16`，都被当作定长处理。但根据 FS 文档，C API 用户以变长 UTF-8 字符串绑定 decimal 数据（`buffer_type` 设为 `TSDB_DATA_TYPE_DECIMAL64/DECIMAL`，buffer 内容为字符串，长度通过 `length` 数组指定）。

## 设计方案

在 `stmt2.rs` 的序列化路径中，将 Decimal/Decimal64 视为变长类型处理。

### 约束

根据 FS 文档（`docs/fs/stmt2-support-decimal-write-fs.md` 约束 1），**DECIMAL 类型仅支持普通列，不支持 tag 列**。实现中需确保：
- 仅在 **列（column）路径** 启用 Decimal 变长处理
- 若用户尝试将 Decimal 类型绑定为 tag，应返回 `INVALID_PARA` 错误

### 修改点

**1. `calc_tag_or_col_lens` 方法（约第 702 行）**

```rust
// 修改前
let have_len = bind.ty().fixed_length() == 0;

// 修改后
let have_len = bind.ty().fixed_length() == 0 || bind.ty().is_decimal();
```

**2. `write_tags_or_cols` 方法（约第 812 行）**

```rust
// 修改前
let have_len = bind.ty().fixed_length() == 0;

// 修改后
let have_len = bind.ty().fixed_length() == 0 || bind.ty().is_decimal();
```

以上两处改动在 tag 和 column 路径共用，因为 `calc_tag_or_col_lens` / `write_tags_or_cols` 是通用函数。Decimal tag 的拦截由服务端 `taos_stmt2_get_fields` 保证——服务端不会为 tag 列返回 Decimal 类型字段，因此合法的 tag 绑定不会包含 Decimal 类型。

这两处改动让 Decimal 类型走变长路径：
- `calc_tag_or_col_lens`：按变长方式累加实际字符串长度
- `write_tags_or_cols`：写入 `HaveLength=1` 标志、每行长度数组、以及变长 buffer 数据

### 测试

在 `stmt2.rs` 的 `#[cfg(test)]` 模块中添加测试，覆盖：
- Decimal64（precision ≤ 18）字符串绑定
- Decimal128（precision > 18）字符串绑定
- 多行绑定（含 NULL 行）
- 科学计数法输入（如 `1.23e+5`）
- 整数位溢出错误透传（如对 `DECIMAL(4,2)` 绑定 `100.1`，预期服务端返回 overflow 错误）
- 四舍五入进位溢出（如对 `DECIMAL(3,1)` 绑定 `99.99`，四舍五入为 `100.0`，预期 overflow）
- 参考现有 `test_stmt2_bind_param` 测试的模式和 WsProxy 基础设施

## 不修改的部分

- `Ty::fixed_length()` 保持不变（Decimal64=8, Decimal=16），其他模块（raw block 解析、TMQ）依赖这些值
- `stmt.rs`（stmt v1）不需要支持 decimal
- `query.rs` 的 decimal 查询路径已完成，无需修改
