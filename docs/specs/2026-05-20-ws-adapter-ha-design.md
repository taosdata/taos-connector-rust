# WS Adapter HA 设计

## 问题

当前 WebSocket 连接的端点列表在 DSN 解析阶段确定，运行期不可扩展。如果 adapter 集群动态扩缩容，客户端无法感知新增实例，导致负载不均或可用性下降。

## 方案

在 CONN / TMQ SUBSCRIBE 协议中引入 `list_instances` 字段，让 adapter 在首次连接时返回可用实例列表，客户端合并到端点池供后续连接和重连使用。

通过 DSN 参数 `adapter_ha=true` 控制是否开启。

## DSN 参数

| 参数 | 类型 | 默认值 | 作用 |
|---|---|---|---|
| `adapter_ha` | `bool` | `false` | 开启后，首次连接时向 adapter 请求实例列表 |

示例：
```
taos+ws://root:taosdata@host1:6041,host2:6041/db?adapter_ha=true
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

    let mut seen = HashSet::new();
    let new_instances: Vec<String> = instances
        .into_iter()
        .filter(|s| {
            is_valid_host_port(s)
                && !existing.contains(s.as_str())
                && seen.insert(s.clone())
        })
        .collect();

    if !new_instances.is_empty() {
        tracing::info!("adapter HA: discovered {} new instances: {:?}", new_instances.len(), new_instances);
        addrs.extend(new_instances);
    }
}

/// host:port 格式校验：包含 `:`，端口部分为正整数
fn is_valid_host_port(s: &str) -> bool {
    if let Some(idx) = s.rfind(':') {
        let host = &s[..idx];
        let port = &s[idx + 1..];
        !host.is_empty() && port.parse::<u16>().is_ok()
    } else {
        false
    }
}
```

注意：`merge_instances` 不负责设置 `instances_fetched`。该标记由调用方（`connect_with_opt_cb` / `Consumer::subscribe()`）在首次成功握手后统一设置（见下文），无论响应是否包含实例列表。

### build_conn_request 变更

```rust
pub(crate) fn build_conn_request(&self) -> WsConnReq {
    WsConnReq {
        // ...existing fields...
        list_instances: if self.adapter_ha {
            Some(!self.instances_fetched.load(Ordering::Acquire))
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
- `Ok(Some(instances))` — 响应包含实例列表（`list_instances=true` 时 adapter 必定返回）
- `Ok(None)` — 响应不包含实例列表（`list_instances=false`，或老版本 adapter 不支持该字段）

### connect_with_opt_cb 变更

```rust
if let Some(ref cb) = cb {
    let instances = call!(cb(&mut ws_stream), "call callback");

    // 无论是否收到实例列表，首次成功握手后即标记"已尝试拉取"。
    // 使用 compare_exchange 作为一次性门闩，防止并发建连时多次 merge。
    if self.adapter_ha
        && self
            .instances_fetched
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    {
        if let Some(instances) = instances {
            self.merge_instances(instances).await;
        }
    }
}
```

并发安全说明：

- `build_conn_request` 中的 `load(Acquire)` 是快照读，并发建连时多个线程可能都读到 `false` 并发送 `list_instances=true`。
- `connect_with_opt_cb` 中的 `compare_exchange` 保证只有第一个成功完成握手的连接执行 merge。
- 其他并发连接发送的 `list_instances=true` 不会造成副作用（adapter 返回相同列表，但客户端不执行 merge）。

## 各连接流程的处理

### WS SQL 连接（首次）

1. `build_conn_request()` 生成 `list_instances = Some(true)`
2. `send_conn_request` 发送 CONN 请求，解析响应中的 `WsRecvData::Conn { list_instances }`
3. 返回 `Ok(list_instances)`
4. `connect_with_opt_cb` 调用 `merge_instances` 合并新端点

### WS SQL 重连

1. `build_conn_request()` 生成 `list_instances = Some(false)`（因为 `instances_fetched=true`）
2. `send_conn_request` 同样解析响应，adapter 收到 `list_instances=false` 不返回实例列表
3. 返回 `Ok(None)`，不触发合并

### TMQ 首次订阅（通过 connect_with_cb 路径）

当 TMQ 重连且存在缓存消息时，`consumer/conn.rs` 使用 `send_subscribe_request` 作为回调调用 `connect_with_cb`。此路径与 WS SQL 连接一致，由 `connect_with_opt_cb` 统一处理 `list_instances` 和 `merge_instances`。

1. `WsConnReq` 中携带 `list_instances = Some(true)`（首次）或 `Some(false)`（已拉取）
2. `send_subscribe_request` 解析 `TmqRecvData::Subscribe { list_instances }`
3. 返回 `Ok(list_instances)`
4. `connect_with_opt_cb` 通过 `compare_exchange` 门闩决定是否合并

### TMQ 重连

1. `list_instances = Some(false)`（`instances_fetched=true`）
2. 回调返回 `Ok(None)`，不触发合并

### Consumer::subscribe() 方法

`Consumer::subscribe()` 通过 `self.sender.send_recv(action)` 发送消息，不走 `connect_with_opt_cb`。为支持 `list_instances`，需要以下变更：

1. **Consumer 新增 `builder: TaosBuilder` 字段**（从 `TmqBuilder::build_consumer` 传入）
2. `Consumer::subscribe()` 从 `TmqRecvData::Subscribe { list_instances }` 中提取实例列表
3. 调用 `self.builder.merge_instances(instances).await` 合并端点
4. 同样受 `instances_fetched` 的 `compare_exchange` 门闩保护

```rust
// Consumer::subscribe() 中处理 list_instances
let action = TmqSend::Subscribe {
    req_id: self.sender.req_id(),
    req: self.tmq_conf.clone().disable_auto_commit(),
    topics: topics.clone(),
    conn: self.conn.clone(), // WsConnReq 中已含 list_instances
};

match self.sender.send_recv(action).await {
    Ok(TmqRecvData::Subscribe { list_instances }) => {
        if self.builder.adapter_ha
            && self.builder.instances_fetched
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            if let Some(instances) = list_instances {
                self.builder.merge_instances(instances).await;
            }
        }
    }
    // ...existing error handling...
}
```

**协议边界说明**：`list_instances` 仅在首次建连/订阅请求中携带 `true`。已连接状态下 `Consumer::subscribe()` 仍可能是首次订阅（TMQ 的 WebSocket 建连和 subscribe 是分离的：`build_consumer` 只建连不订阅），因此需要在此处理。后续对同一 Consumer 的重复 `subscribe()` 调用中 `instances_fetched=true`，`list_instances=false`。

## 向后兼容

| 场景 | 行为 |
|---|---|
| `adapter_ha` 未设置（默认） | `list_instances` 不序列化，请求/响应与当前完全一致 |
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
| `taos-ws/src/consumer/mod.rs` | `Consumer` 新增 `builder` 字段；`subscribe()` 处理 `list_instances` 并合并端点 |
