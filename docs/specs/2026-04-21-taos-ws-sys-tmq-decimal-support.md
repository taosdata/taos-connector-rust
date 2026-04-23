# taos-ws-sys TMQ Decimal 数据类型支持

## 问题描述

`taos-ws-sys` 的 C FFI 层中，`QueryResultSet` 已完整支持 decimal 数据类型（包括 `Decimal64` 和 `Decimal128`），但 `TmqResultSet`（TMQ 订阅结果集）的 `get_fields_e()` 方法直接返回空指针，导致 TMQ 消费者无法通过 `taos_fetch_fields_e()` 获取 decimal 列的 precision 和 scale 信息。

## 现状分析

### 已正常工作的路径

以下 TMQ 数据路径通过 `RawBlock` 透传机制已正确支持 decimal：

| 路径 | 机制 | 说明 |
|------|------|------|
| `fetch_raw_block()` | 直接传递原始字节 | decimal 数据在 raw block 中已正确编码 |
| `fetch_block()` | 返回列原始指针 | 通过 `col.as_raw_ptr()` 透传 |
| `fetch_row()` | `block.get_raw_value_unchecked()` | 返回 `(Ty::Decimal/Decimal64, len, ptr)` |
| `get_raw_value()` | 同上 | 类型和数据都正确 |
| `taos_print_row_with_size()` | query.rs 统一处理 | 已有 `Ty::Decimal \| Ty::Decimal64` 分支 |

### 缺口

`TmqResultSet::get_fields_e()` 返回 `ptr::null_mut()`（tmq.rs:1454-1456），而 `QueryResultSet::get_fields_e()`（query.rs:1277-1288）正确构建了包含 precision/scale 的 `TAOS_FIELD_E` 数组。

## 修改方案

### 数据来源

TMQ 的 decimal precision/scale 信息来源不同于 Query：

- **Query**：从 WebSocket 响应消息的 `fields_precisions` / `fields_scales` 字段获取
- **TMQ**：从 `RawBlock` 的 `schemas()` 获取，每个 `DataType` 提供 `.precision()` 和 `.scale()` 方法

### 代码修改

#### 1. 结构体扩展（tmq.rs `TmqResultSet`）

```rust
pub struct TmqResultSet {
    // ... 现有字段 ...
    fields_e: Vec<TAOS_FIELD_E>,  // 新增
}
```

#### 2. 初始化（tmq.rs `TmqResultSet::new()`）

```rust
fn new(block: Block, offset: Offset, data: Data) -> Self {
    // ... 现有初始化 ...
    Self {
        // ... 现有字段 ...
        fields_e: Vec::new(),  // 新增
    }
}
```

#### 3. 实现 `get_fields_e()`

```rust
fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E {
    if self.fields_e.is_empty() {
        if let Some(block) = &self.block {
            let schemas = block.schemas();
            let fields = block.fields();
            for (i, field) in fields.iter().enumerate() {
                let precision = schemas[i].precision();
                let scale = schemas[i].scale();
                self.fields_e.push(TAOS_FIELD_E::new(field, precision, scale));
            }
        }
    }
    self.fields_e.as_mut_ptr()
}
```

关键设计决策：
- 使用惰性初始化（首次调用时构建），与 `QueryResultSet` 一致
- 从 `block.fields()` 获取 `Field` 引用（`TAOS_FIELD_E::new` 要求 `&Field` 类型）
- 从 `block.schemas()` 获取对应的 precision 和 scale
- 非 decimal 类型的 precision/scale 为 0（`DataType::precision()` / `scale()` 对非 decimal 类型返回 0）
- `fields_e` 缓存后不会失效——同一 `TmqResultSet` 生命周期内 schema 不会改变

### 影响范围

- 仅修改 `taos-ws-sys/src/ws/tmq.rs`，共涉及 3 处改动
- 不影响任何已有的数据读取路径
- 不影响其他 crate

## 测试方案

添加集成测试 `test_tmq_decimal_fields_e`（需要本地 taosd + taosAdapter），流程：

1. 创建含 decimal 列的表：`DECIMAL(10, 2)` (Decimal64) 和 `DECIMAL(38, 20)` (Decimal128)
2. 创建 topic 订阅该表
3. 插入 decimal 数据
4. TMQ poll 获取结果
5. 调用 `taos_fetch_fields_e()` 验证：
   - decimal64 列：type = 21, precision = 10, scale = 2, bytes = 8
   - decimal128 列：type = 17, precision = 38, scale = 20, bytes = 16
6. 清理资源

参考现有 query.rs 中的 `test_taos_fetch_fields_e`（query.rs:3160-3208）编写。
