# WS Adapter HA 设计

## 问题

当前 WebSocket 连接的端点列表在 DSN 解析阶段确定，运行期不可扩展。如果 adapter 集群动态扩缩容，客户端无法感知新增实例，导致负载不均或可用性下降。

## 方案

在 CONN / TMQ SUBSCRIBE 协议中引入 `list_instances` 字段，让 adapter 在首次连接时返回可用实例列表，客户端合并到端点池供后续连接和重连使用。

通过 DSN 参数 `adapterHA=true` 控制是否开启。

## DSN 参数

| 参数 | 类型 | 默认值 | 作用 |
|---|---|---|---|
| `adapterHA` | `bool` | `false` | 开启后，首次连接时向 adapter 请求实例列表 |

示例：
```
taos+ws://root:taosdata@host1:6041,host2:6041/db?adapterHA=true
```

## 协议变更

### 请求

`WsConnReq` 新增字段（CONN 和 TMQ SUBSCRIBE 共用）：

```rust
#[serde(skip_serializing_if = "Option::is_none")]
pub(crate) list_instances: Option<bool>,
```

- `adapter_ha=true` 且首次连接（`!instances_fetched`）：`list_instances = Some(true)`
- `adapter_ha=true` 且已拉取过（`instances_fetched`）：`list_instances = Some(false)`
- `adapter_ha=false`（默认）：`list_instances = None`（不序列化）

### 响应

**WsRecvData::Conn**（从 unit variant 改为 struct variant）：

```rust
Conn {
    #[serde(default)]
    list_instances: Option<Vec<String>>, // ["host1:port1", "host2:port2"]
},
```

**TmqRecvData::Subscribe**（从 unit variant 改为 struct variant）：

```rust
Subscribe {
    #[serde(default)]
    list_instances: Option<Vec<String>>,
},
```

## TaosBuilder 变更

### 新增字段

```rust
adapter_ha: bool,                          // 是否开启 adapter HA
instances_fetched: Arc<AtomicBool>,        // 是否已完成首次实例拉取
```

### addrs 改为可共享可变

```rust
// 原
addrs: Vec<String>,
// 新
addrs: Arc<tokio::sync::RwLock<Vec<String>>>,
```

使用 `tokio::sync::RwLock`，与项目中的异步上下文保持一致。所有读取 `addrs` 的位置（`active_addr()`、`connect_with_opt_cb` 的遍历循环等）需要适配为 `.read().await` / `.write().await`。

### merge_instances 方法

```rust
async fn merge_instances(&self, instances: Vec<String>) {
    let mut addrs = self.addrs.write().await;
    let existing: HashSet<&str> = addrs.iter().map(String::as_str).collect();
    let new_instances: Vec<String> = instances
        .into_iter()
        .filter(|s| !s.is_empty() && !existing.contains(s.as_str()))
        .collect();
    if !new_instances.is_empty() {
        tracing::info!("adapter HA: discovered {} new instances: {:?}", new_instances.len(), new_instances);
        addrs.extend(new_instances);
    }
    // 设置 instances_fetched = true
    self.instances_fetched.store(true, Ordering::Relaxed);
}
```

### build_conn_request 变更

```rust
pub(crate) fn build_conn_request(&self) -> WsConnReq {
    WsConnReq {
        // ...existing fields...
        list_instances: if self.adapter_ha {
            Some(!self.instances_fetched.load(Ordering::Relaxed))
        } else {
            None
        },
    }
}
```

## 回调签名变更

### 原签名

```rust
F: for<'a> Fn(&'a mut WsStream) -> Pin<Box<dyn Future<Output = RawResult<()>> + Send + 'a>>
```

### 新签名

```rust
F: for<'a> Fn(&'a mut WsStream) -> Pin<Box<dyn Future<Output = RawResult<Option<Vec<String>>>> + Send + 'a>>
```

返回值含义：
- `Ok(Some(instances))` — 响应包含实例列表
- `Ok(None)` — 响应不包含实例列表（老版本 adapter 或 `list_instances=false`）

### connect_with_opt_cb 变更

```rust
if let Some(ref cb) = cb {
    let instances = call!(cb(&mut ws_stream), "call callback");
    if let Some(instances) = instances {
        self.merge_instances(instances);
    }
}
```

## 各连接流程的处理

### WS SQL 连接（首次）

1. `build_conn_request()` 生成 `list_instances = Some(true)`
2. `send_conn_request` 发送 CONN 请求，解析响应中的 `WsRecvData::Conn { list_instances }`
3. 返回 `Ok(list_instances)`
4. `connect_with_opt_cb` 调用 `merge_instances` 合并新端点

### WS SQL 重连

1. `build_conn_request()` 生成 `list_instances = Some(false)`（因为 `instances_fetched=true`）
2. `send_conn_request` 同样解析响应，此时 adapter 通常不返回实例列表
3. 返回 `Ok(None)`，不触发合并

### TMQ 首次订阅

1. `WsConnReq` 中携带 `list_instances = Some(true)`
2. `send_subscribe_request` 解析 `TmqRecvData::Subscribe { list_instances }`
3. 返回 `Ok(list_instances)`
4. `connect_with_opt_cb` 合并新端点

### TMQ 重连

1. `list_instances = Some(false)`
2. 同重连逻辑

### Consumer::subscribe() 方法

`Consumer::subscribe()` 通过 `self.sender.send_recv(action)` 发送消息，不走 `connect_with_opt_cb` 流程。此处暂不处理 `list_instances`（该方法在已连接状态下调用，端点池已在初始连接时更新）。

## 向后兼容

| 场景 | 行为 |
|---|---|
| `adapterHA` 未设置（默认） | `list_instances` 不序列化，请求/响应与当前完全一致 |
| 老版本 adapter（不支持 `list_instances`） | 响应中无此字段，`#[serde(default)]` 反序列化为 `None`，连接正常，不做动态扩容 |
| `list_instances` 返回空列表 | `merge_instances` 过滤后无新端点，不操作 |

## 影响文件

| 文件 | 变更 |
|---|---|
| `taos-ws/src/lib.rs` | `TaosBuilder` 新增字段、DSN 解析、`merge_instances()`、`connect()` 合并逻辑、`connect_with_opt_cb` 回调返回类型、`addrs` 读取适配 |
| `taos-ws/src/query/messages.rs` | `WsConnReq` 添加 `list_instances`；`WsRecvData::Conn` 改为 struct variant |
| `taos-ws/src/query/conn.rs` | `send_conn_request` 返回类型变更，解析 `list_instances` |
| `taos-ws/src/consumer/messages.rs` | `TmqRecvData::Subscribe` 改为 struct variant |
| `taos-ws/src/consumer/conn.rs` | `send_subscribe_request` 返回类型变更，解析 `list_instances` |
