use std::fmt::Debug;
use std::future::Future;
use std::io::Write;
use std::mem::transmute;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::bail;
use byteorder::{ByteOrder, LittleEndian};
use flume::Sender;
use futures::channel::oneshot;
use futures::stream::SplitStream;
use futures::{FutureExt, SinkExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use oneshot::channel as query_channel;
use taos_query::common::{Field, Precision, RawBlock, RawMeta, SmlData};
use taos_query::prelude::{Code, RawError, RawResult};
use taos_query::util::{generate_req_id, InlinableWrite};
use taos_query::{AsyncFetchable, AsyncQueryable, DeError, DsnError, IntoDsn};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::watch;
use tokio::time::{self, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{instrument, trace, Instrument};

use super::infra::*;
use super::TaosBuilder;

type WsSender = flume::Sender<Message>;

type QueryChannelSender = oneshot::Sender<RawResult<WsRecvData>>;
type QueryInner = scc::HashMap<ReqId, QueryChannelSender>;
type QueryAgent = Arc<QueryInner>;
type QueryResMapper = scc::HashMap<ResId, ReqId>;

type WebSocketStreamReader = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

#[derive(Debug, Clone)]
struct Version {
    version: String,
    is_support_binary_sql: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct WsQuerySender {
    version: Version,
    req_id: Arc<AtomicU64>,
    results: Arc<QueryResMapper>,
    sender: WsSender,
    queries: QueryAgent,
}

const SEND_TIMEOUT: Duration = Duration::from_millis(1000);

impl WsQuerySender {
    fn req_id(&self) -> ReqId {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn req_id_ref(&self) -> &Arc<AtomicU64> {
        &self.req_id
    }

    #[instrument(skip_all)]
    async fn send_recv(&self, msg: WsSend) -> RawResult<WsRecvData> {
        let req_id = msg.req_id();
        let (tx, rx) = query_channel();

        let _ = self.queries.insert_async(req_id, tx).await;

        match msg {
            WsSend::FetchBlock(args) => {
                tracing::trace!("[req id: {req_id}] prepare message {msg:?}");
                if self.results.contains_async(&args.id).await {
                    Err(RawError::from_string(format!(
                        "there's a result with id {}",
                        args.id
                    )))?;
                }
                let _ = self.results.insert_async(args.id, args.req_id).await;

                timeout(SEND_TIMEOUT, self.sender.send_async(msg.to_msg()))
                    .await
                    .map_err(Error::from)?
                    .map_err(Error::from)?;
            }
            WsSend::Binary(bytes) => {
                timeout(SEND_TIMEOUT, self.sender.send_async(Message::Binary(bytes)))
                    .await
                    .map_err(Error::from)?
                    .map_err(Error::from)?;
            }
            _ => {
                tracing::trace!("[req id: {req_id}] prepare message: {msg:?}");
                timeout(SEND_TIMEOUT, self.sender.send_async(msg.to_msg()))
                    .await
                    .map_err(Error::from)?
                    .map_err(Error::from)?;
            }
        }
        // handle the error
        tracing::trace!("[req id: {req_id}] message sent, wait for receiving");
        let res = rx
            .await
            .map_err(|_| RawError::from_string(format!("{req_id} request cancelled")))?
            .map_err(Error::from)?;
        tracing::trace!("[req id: {req_id}] message received: {res:?}");
        Ok(res)
    }

    async fn send_only(&self, msg: WsSend) -> RawResult<()> {
        timeout(SEND_TIMEOUT, self.sender.send_async(msg.to_msg()))
            .await
            .map_err(Error::from)?
            .map_err(Error::from)?;
        Ok(())
    }

    fn send_blocking(&self, msg: WsSend) -> RawResult<()> {
        let _ = self.sender.send(msg.to_msg());
        Ok(())
    }
}

#[derive(Debug)]
pub struct WsTaos {
    close_signal: watch::Sender<bool>,
    sender: WsQuerySender,
}

impl Drop for WsTaos {
    fn drop(&mut self) {
        trace!("dropping connection");
        // Send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct QueryMetrics {
    pub(crate) num_of_fetches: usize,
    pub(crate) time_cost_in_fetch: Duration,
    pub(crate) time_cost_in_block_parse: Duration,
    pub(crate) time_cost_in_flume: Duration,
}

type BlockFuture = Pin<Box<dyn Future<Output = RawResult<Option<RawBlock>>> + Send>>;

pub struct ResultSet {
    pub(crate) sender: WsQuerySender,
    pub(crate) args: WsResArgs,
    pub(crate) fields: Option<Vec<Field>>,
    pub(crate) fields_count: usize,
    pub(crate) affected_rows: usize,
    pub(crate) precision: Precision,
    pub(crate) summary: (usize, usize),
    pub(crate) timing: Duration,
    pub(crate) block_future: Option<BlockFuture>,
    pub(crate) closer: Option<oneshot::Sender<()>>,
    pub(crate) completed: bool,
    pub(crate) metrics: QueryMetrics,
    pub(crate) blocks_buffer: Option<flume::Receiver<RawResult<(RawBlock, Duration)>>>,
}

unsafe impl Sync for ResultSet {}
unsafe impl Send for ResultSet {}

impl Debug for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultSet")
            .field("args", &self.args)
            .field("fields", &self.fields)
            .field("fields_count", &self.fields_count)
            .field("affected_rows", &self.affected_rows)
            .field("precision", &self.precision)
            .finish_non_exhaustive()
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        trace!("dropping, metrics: {:?}", self.metrics);
        let (sender, closer, args, completed) = (
            self.sender.clone(),
            self.closer.take(),
            self.args,
            self.completed,
        );
        let clean = move || {
            if let Some((_, req_id)) = sender.results.remove(&args.id) {
                sender.queries.remove(&req_id);
            }

            if !completed {
                let _ = sender.send_blocking(WsSend::FreeResult(args));
            }

            if let Some(closer) = closer {
                let _ = closer.send(());
            }
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(clean);
        } else {
            std::thread::spawn(clean);
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Dsn(#[from] DsnError),
    #[error("Authentication failure: \"{0}\"")]
    Unauthorized(String),
    #[error("{0}")]
    FetchError(#[from] tokio::sync::oneshot::error::RecvError),
    #[error(transparent)]
    FlumeSendError(#[from] flume::SendError<Message>),
    #[error("Send data via websocket timeout")]
    SendTimeoutError(#[from] tokio::time::error::Elapsed),
    #[error("Query timed out with sql: {0}")]
    QueryTimeout(String),
    #[error("{0}")]
    TaosError(#[from] RawError),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("WebSocket internal error: {0}")]
    TungsteniteError(#[from] tokio_tungstenite::tungstenite::Error),
    #[error(transparent)]
    TungsteniteSendTimeoutError(
        #[from] tokio::sync::mpsc::error::SendTimeoutError<tokio_tungstenite::tungstenite::Message>,
    ),
    #[error(transparent)]
    TungsteniteSendError(
        #[from] tokio::sync::mpsc::error::SendError<tokio_tungstenite::tungstenite::Message>,
    ),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Websocket has been closed: {0}")]
    WsClosed(String),
    #[error("Common error: {0}")]
    CommonError(String),
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
#[allow(non_camel_case_types)]
pub enum WS_ERROR_NO {
    DSN_ERROR = 0xE000,
    WEBSOCKET_ERROR = 0xE001,
    CONN_CLOSED = 0xE002,
    SEND_MESSAGE_TIMEOUT = 0xE003,
    RECV_MESSAGE_TIMEOUT = 0xE004,
    IO_ERROR = 0xE005,
    UNAUTHORIZED = 0xE006,
}

impl WS_ERROR_NO {
    pub fn as_code(&self) -> Code {
        Code::new(*self as _)
    }
}

impl Error {
    pub const fn errno(&self) -> Code {
        match self {
            Error::TaosError(error) => error.code(),
            Error::Unauthorized(_) => Code::new(WS_ERROR_NO::UNAUTHORIZED as _),
            Error::Dsn(_) => Code::new(WS_ERROR_NO::DSN_ERROR as _),
            Error::IoError(_) => Code::new(WS_ERROR_NO::IO_ERROR as _),
            Error::TungsteniteError(_) => Code::new(WS_ERROR_NO::WEBSOCKET_ERROR as _),
            Error::SendTimeoutError(_) => Code::new(WS_ERROR_NO::SEND_MESSAGE_TIMEOUT as _),
            Error::FlumeSendError(_) => Code::new(WS_ERROR_NO::CONN_CLOSED as _),
            _ => Code::FAILED,
        }
    }

    pub fn errstr(&self) -> String {
        match self {
            Error::TaosError(error) => error.message(),
            _ => format!("{self}"),
        }
    }
}

impl From<Error> for RawError {
    fn from(value: Error) -> Self {
        match value {
            Error::TaosError(error) => error,
            error => {
                let code = error.errno();
                if code == Code::FAILED {
                    RawError::from_any(error)
                } else {
                    RawError::new(code, error.to_string())
                }
            }
        }
    }
}

#[instrument(skip_all)]
async fn read_queries(
    mut reader: WebSocketStreamReader,
    queries_sender: QueryAgent,
    fetches_sender: Arc<QueryResMapper>,
    ws2: WsSender,
    is_v3: bool,
    mut close_listener: watch::Receiver<bool>,
) {
    let parse_frame = |frame: Message| {
        match frame {
            Message::Text(text) => {
                tracing::trace!("received json response: {text}",);
                // 如果text 序列化失败，打印日志，继续处理下一个消息
                let v = serde_json::from_str::<WsRecv>(&text);
                if let Err(err) = v {
                    tracing::error!("failed to deserialize json text: {text}, error: {err:?}");
                    return ControlFlow::Continue(());
                }
                let v = v.unwrap();
                let queries_sender = queries_sender.clone();
                let ws2 = ws2.clone();
                let (req_id, data, ok) = v.ok();
                match &data {
                    WsRecvData::Insert(_) => {
                        if let Some((_, sender)) = queries_sender.remove(&req_id) {
                            sender.send(ok.map(|_| data)).unwrap();
                        } else {
                            debug_assert!(!queries_sender.contains(&req_id));
                            tracing::warn!("req_id {req_id} not detected, message might be lost");
                        }
                    }
                    WsRecvData::Query(_) => {
                        if let Some((_, sender)) = queries_sender.remove(&req_id) {
                            if let Err(err) = sender.send(ok.map(|_| data)) {
                                tracing::error!("send data with error: {err:?}");
                            }
                        } else {
                            debug_assert!(!queries_sender.contains(&req_id));
                            tracing::warn!("req_id {req_id} not detected, message might be lost");
                        }
                    }
                    WsRecvData::Fetch(fetch) => {
                        let id = fetch.id;
                        if fetch.completed {
                            tokio::spawn(async move {
                                let _ = ws2
                                    .send_async(
                                        WsSend::FreeResult(WsResArgs { req_id, id }).to_msg(),
                                    )
                                    .await;
                            });
                        }
                        if let Some((_, sender)) = queries_sender.remove(&req_id) {
                            let _ = sender.send(ok.map(|_| data));
                        } else {
                            tracing::warn!("req_id {req_id} not detected, message might be lost");
                        }
                    }
                    WsRecvData::FetchBlock => {
                        assert!(ok.is_err());
                        if let Some((_, sender)) = queries_sender.remove(&req_id) {
                            let _ = sender.send(ok.map(|_| data));
                        } else {
                            tracing::warn!("req_id {req_id} not detected, message might be lost");
                        }
                    }
                    WsRecvData::WriteMeta => {
                        if let Some((_, sender)) = queries_sender.remove(&req_id) {
                            let _ = sender.send(ok.map(|_| data));
                        } else {
                            tracing::warn!("req_id {req_id} not detected, message might be lost");
                        }
                    }
                    WsRecvData::WriteRaw => {
                        if let Some((_, sender)) = queries_sender.remove(&req_id) {
                            let _ = sender.send(ok.map(|_| data));
                        } else {
                            tracing::warn!("req_id {req_id} not detected, message might be lost");
                        }
                    }
                    WsRecvData::WriteRawBlock | WsRecvData::WriteRawBlockWithFields => {
                        if let Some((_, sender)) = queries_sender.remove(&req_id) {
                            let _ = sender.send(ok.map(|_| data));
                        } else {
                            tracing::warn!("req_id {req_id} not detected, message might be lost");
                        }
                    }
                    WsRecvData::Stmt2Init { .. }
                    | WsRecvData::Stmt2Prepare { .. }
                    | WsRecvData::Stmt2Bind { .. }
                    | WsRecvData::Stmt2Exec { .. }
                    | WsRecvData::Stmt2Result { .. }
                    | WsRecvData::Stmt2Close { .. } => match queries_sender.remove(&req_id) {
                        Some((_, sender)) => {
                            let _ = sender.send(ok.map(|_| data));
                        }
                        None => {
                            tracing::warn!("req_id {req_id} not detected, message might be lost");
                        }
                    },
                    // Block type is for binary.
                    _ => unreachable!(),
                }
            }
            Message::Binary(payload) => {
                let queries_sender = queries_sender.clone();
                let fetches_sender = fetches_sender.clone();
                let block = payload;
                tokio::spawn(async move {
                    use taos_query::util::InlinableRead;
                    let offset = if is_v3 { 16 } else { 8 };

                    let mut slice = block.as_slice();
                    let mut is_block_new = false;

                    let timing = if is_v3 {
                        let timing = slice.read_u64().unwrap();
                        if timing == u64::MAX {
                            is_block_new = true;
                            Duration::ZERO
                        } else {
                            Duration::from_nanos(timing as _)
                        }
                    } else {
                        Duration::ZERO
                    };

                    if is_block_new {
                        let _action = slice.read_u64().unwrap();
                        let block_version = slice.read_u16().unwrap();
                        let timing = Duration::from_nanos(slice.read_u64().unwrap());
                        let block_req_id = slice.read_u64().unwrap();
                        let block_code = slice.read_u32().unwrap();
                        let block_message = slice.read_inlined_str::<4>().unwrap();
                        let _result_id = slice.read_u64().unwrap();
                        let finished = slice.read_u8().unwrap() == 1;
                        let result_block = if finished {
                            Vec::new()
                        } else {
                            slice.read_inlined_bytes::<4>().unwrap()
                        };
                        if let Some((_, sender)) = queries_sender.remove(&block_req_id) {
                            sender
                                .send(Ok(WsRecvData::BlockNew {
                                    block_version,
                                    timing,
                                    block_req_id,
                                    block_code,
                                    block_message,
                                    finished,
                                    raw: result_block,
                                }))
                                .unwrap();
                        } else {
                            tracing::warn!(
                                "req_id {block_req_id} not detected, message might be lost"
                            );
                        }
                    } else {
                        let res_id = slice.read_u64().unwrap();
                        if let Some((_, req_id)) = fetches_sender.remove(&res_id) {
                            if is_v3 {
                                // v3
                                if let Some((_, sender)) = queries_sender.remove(&req_id) {
                                    tracing::trace!("send data to fetches with id {}", res_id);
                                    sender
                                        .send(Ok(WsRecvData::Block {
                                            timing,
                                            raw: block[offset..].to_vec(),
                                        }))
                                        .unwrap();
                                } else {
                                    tracing::warn!(
                                        "req_id {res_id} not detected, message might be lost"
                                    );
                                }
                            } else {
                                // v2
                                if let Some((_, sender)) = queries_sender.remove(&req_id) {
                                    tracing::trace!("send data to fetches with id {}", res_id);
                                    sender
                                        .send(Ok(WsRecvData::BlockV2 {
                                            timing,
                                            raw: block[offset..].to_vec(),
                                        }))
                                        .unwrap();
                                } else {
                                    tracing::warn!(
                                        "req_id {res_id} not detected, message might be lost"
                                    );
                                }
                            }
                        } else {
                            tracing::warn!("result id {res_id} not found");
                        }
                    }
                });
            }
            Message::Close(_) => {
                // taosAdapter should never send close frame to client.
                // So all close frames should be treated as error.

                tracing::warn!("websocket connection is closed normally");
                let mut keys = Vec::new();
                queries_sender.scan(|k, _| {
                    keys.push(*k);
                });
                for k in keys {
                    if let Some((_, sender)) = queries_sender.remove(&k) {
                        let _ = sender.send(Err(RawError::new(
                            WS_ERROR_NO::CONN_CLOSED.as_code(),
                            "received close message",
                        )));
                    }
                }

                return ControlFlow::Break(());
            }
            Message::Ping(data) => {
                let ws2 = ws2.clone();
                tokio::spawn(async move {
                    let _ = ws2.send_async(Message::Pong(data)).await;
                });
            }
            Message::Pong(_) => tracing::trace!("received pong message, do nothing"),
            Message::Frame(_) => {
                tracing::warn!("received (unexpected) frame message, do nothing");
                tracing::trace!("* frame data: {frame:?}");
            }
        }

        ControlFlow::Continue(())
    };
    loop {
        tokio::select! {
            res = reader.try_next() => {
                match res {
                    Ok(frame) => {
                        if let Some(frame) = frame {
                            if let ControlFlow::Break(()) = parse_frame(frame) {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("reader err: {e:?}");
                        break;
                    }
                }
            }
            _ = close_listener.changed() => {
                tracing::trace!("close reader task");
                let mut keys = Vec::with_capacity(queries_sender.len());
                queries_sender.scan(|k, _| {
                    keys.push(*k);
                });
                // queries_sender.for_each_async(|k, _| {
                //     keys.push(*k);
                // }).await;
                for k in keys {
                    if let Some((_, sender)) = queries_sender.remove(&k) {
                        let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), "close signal received")));
                    }
                }
                break;
            }
        }
    }
    if queries_sender.is_empty() {
        return;
    }

    let mut keys = Vec::with_capacity(queries_sender.len());
    queries_sender.scan(|k, _| {
        keys.push(*k);
    });
    // queries_sender
    //     .for_each_async(|k, _| {
    //         keys.push(*k);
    //     })
    //     .await;
    for k in keys {
        if let Some((_, sender)) = queries_sender.remove(&k) {
            let _ = sender.send(Err(RawError::from_string("websocket connection is closed")));
        }
    }
}

pub fn compare_versions(v1: &str, v2: &str) -> std::cmp::Ordering {
    let nums1: Vec<u32> = v1
        .split('.')
        .take(4)
        .map(|s| s.parse().expect(v1))
        .collect();
    let nums2: Vec<u32> = v2
        .split('.')
        .take(4)
        .map(|s| s.parse().expect(v2))
        .collect();

    nums1.cmp(&nums2)
}

pub fn is_greater_than_or_equal_to(v1: &str, v2: &str) -> bool {
    !matches!(compare_versions(v1, v2), std::cmp::Ordering::Less)
}

pub fn is_support_binary_sql(v1: &str) -> bool {
    is_greater_than_or_equal_to(v1, "3.3.2.0")
}

impl WsTaos {
    /// Build TDengine websocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041/
    /// ```
    pub async fn from_dsn<T: IntoDsn>(dsn: T) -> RawResult<Self> {
        let dsn = dsn.into_dsn()?;
        let info = TaosBuilder::from_dsn(dsn)?;
        Self::from_wsinfo(&info).await
    }

    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {
        let ws = info.build_stream(info.to_query_url()).await?;

        let req_id = 0;
        let (mut sender, mut reader) = ws.split();

        let version = WsSend::Version;
        sender.send(version.to_msg()).await.map_err(|err| {
            RawError::any(err)
                .with_code(WS_ERROR_NO::WEBSOCKET_ERROR.as_code())
                .context("Send version request message error")
        })?;
        let duration = Duration::from_secs(8);
        let version_future = async {
            let max_non_version = 5;
            let mut count = 0;
            loop {
                count += 1;
                if let Some(message) = reader.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            let v: WsRecv = serde_json::from_str(&text).map_err(|err| {
                                RawError::any(err)
                                    .with_code(WS_ERROR_NO::WEBSOCKET_ERROR.as_code())
                                    .context("Parser text as json error")
                            })?;
                            let (_req_id, data, ok) = v.ok();
                            match data {
                                WsRecvData::Version { version } => {
                                    ok?;
                                    return Ok(version);
                                }
                                _ => return Ok("2.x".to_string()),
                            }
                        }
                        Ok(Message::Ping(bytes)) => {
                            sender.send(Message::Pong(bytes)).await.map_err(|err| {
                                RawError::any(err)
                                    .with_code(WS_ERROR_NO::WEBSOCKET_ERROR.as_code())
                                    .context("Send pong message error")
                            })?;
                            if count >= max_non_version {
                                return Ok("2.x".to_string());
                            }
                            count += 1;
                        }
                        _ => return Ok("2.x".to_string()),
                    }
                } else {
                    bail!("Expect version message, but got nothing");
                }
            }
        }
        .in_current_span();
        let version = match tokio::time::timeout(duration, version_future).await {
            Ok(Ok(version)) => version,
            Ok(Err(err)) => {
                return Err(RawError::any(err).context("Version fetching error"));
            }
            Err(_) => "2.x".to_string(),
        };
        let is_v3 = !version.starts_with('2');
        let is_support_binary_sql = is_v3 && is_support_binary_sql(&version);

        let login = WsSend::Conn {
            req_id,
            req: info.to_conn_request(),
        };
        sender.send(login.to_msg()).await.map_err(Error::from)?;
        while let Some(Ok(message)) = reader.next().await {
            match message {
                Message::Text(text) => {
                    let v: WsRecv = serde_json::from_str(&text).unwrap();
                    let (_req_id, data, ok) = v.ok();
                    match data {
                        WsRecvData::Conn => {
                            ok?;
                            break;
                        }
                        WsRecvData::Version { .. } => {}
                        data => {
                            return Err(RawError::from_string(format!(
                                "Unexpected login result: {data:?}"
                            )))
                        }
                    }
                }
                Message::Ping(bytes) => {
                    sender
                        .send(Message::Pong(bytes))
                        .await
                        .map_err(RawError::from_any)?;
                }
                _ => {
                    return Err(RawError::from_string(format!(
                        "unexpected message on login: {message:?}"
                    )));
                }
            }
        }

        let queries2 = Arc::new(QueryInner::new());

        let fetches_sender = Arc::new(QueryResMapper::new());
        let results = fetches_sender.clone();

        let queries2_cloned = queries2.clone();
        let queries3 = queries2.clone();

        let (ws, msg_recv) = flume::bounded(64);
        let ws2 = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let close_listener = rx.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(53));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(err) = sender.send(Message::Ping(b"TAOS".to_vec())).await {
                            tracing::error!("Write websocket ping error: {}", err);
                            break;
                        }
                        let _ = sender.flush().await;
                    }
                    _ = rx.changed() => {
                        let _ = sender.close().await;
                        tracing::trace!("close sender task");
                        break;
                    }
                    msg = msg_recv.recv_async() => {
                        match msg {
                            Ok(msg) => {
                                if let Err(err) = sender.send(msg).await {
                                    tracing::error!("Write websocket error: {}", err);
                                    let mut keys = Vec::new();
                                    queries3.scan(|k, _| keys.push(*k));
                                    for k in keys {
                                        if let Some((_, sender)) = queries3.remove_async(&k).await {
                                            let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), err.to_string())));
                                        }
                                    }
                                    break;
                                }
                            }
                            Err(_) => {
                                break;
                            }
                        }
                        // dbg!(&msg);
                    }
                }
            }
        }.in_current_span());

        tokio::spawn(read_queries(
            reader,
            queries2,
            fetches_sender,
            ws2,
            is_v3,
            close_listener,
        ));

        Ok(Self {
            close_signal: tx,
            sender: WsQuerySender {
                version: Version {
                    version,
                    is_support_binary_sql,
                },
                req_id: Arc::default(),
                sender: ws,
                queries: queries2_cloned,
                results,
            },
        })
    }

    pub async fn write_meta(&self, raw: &RawMeta) -> RawResult<()> {
        let req_id = self.sender.req_id();
        let message_id = req_id;
        let raw_meta_message = 3; // magic number from taosAdapter.

        let mut meta = Vec::new();
        meta.write_u64_le(req_id).map_err(Error::from)?;
        meta.write_u64_le(message_id).map_err(Error::from)?;
        meta.write_u64_le(raw_meta_message as u64)
            .map_err(Error::from)?;
        meta.write_all(&raw.as_bytes()).map_err(Error::from)?;
        let len = meta.len();

        tracing::trace!("write meta with req_id: {req_id}, raw data length: {len}",);

        let h = self
            .sender
            .send_recv(WsSend::Binary(meta))
            .in_current_span();
        tokio::pin!(h);
        let mut interval = time::interval(Duration::from_secs(60));
        const MAX_WAIT_TICKS: usize = 5; // means 5 minutes
        const TIMEOUT_ERROR: &str = "Write raw meta timeout, maybe the connection has been lost";
        let mut ticks = 0;
        loop {
            select! {
                _ = interval.tick() => {
                    ticks += 1;
                    if ticks >= MAX_WAIT_TICKS {
                        tracing::warn!("{}", TIMEOUT_ERROR);
                        return Err(RawError::new(
                            0xE002, // Connection closed
                            TIMEOUT_ERROR,
                        ));
                    }
                    if let Err(err) = time::timeout(Duration::from_secs(30), self.exec("select server_version()").in_current_span()).await {
                        tracing::warn!(error = format!("{err:#}"), TIMEOUT_ERROR);
                        return Err(RawError::new(
                            0xE002, // Connection closed
                            TIMEOUT_ERROR,
                        ));
                    }
                }
                res = &mut h => {
                    res?;
                    return Ok(())
                }
            }
        }
    }

    async fn s_write_raw_block(&self, raw: &RawBlock) -> RawResult<()> {
        let req_id = self.sender.req_id();
        self.s_write_raw_block_with_req_id(raw, req_id)
            .in_current_span()
            .await
    }

    async fn s_write_raw_block_with_req_id(&self, raw: &RawBlock, req_id: u64) -> RawResult<()> {
        let message_id = req_id;

        if self.version().starts_with("3.0.1.") {
            let raw_block_message = 4; // action number from `taosAdapter/controller/rest/const.go:L56`.

            let mut meta = Vec::new();
            meta.write_u64_le(req_id).map_err(Error::from)?;
            meta.write_u64_le(message_id).map_err(Error::from)?;
            meta.write_u64_le(raw_block_message as u64)
                .map_err(Error::from)?;
            meta.write_u32_le(raw.nrows() as u32).map_err(Error::from)?;
            meta.write_inlined_str::<2>(raw.table_name().unwrap())
                .map_err(Error::from)?;
            meta.write_all(raw.as_raw_bytes()).map_err(Error::from)?;

            let len = meta.len();
            tracing::trace!("write block with req_id: {req_id}, raw data len: {len}",);

            match self.sender.send_recv(WsSend::Binary(meta)).await? {
                WsRecvData::WriteRawBlock | WsRecvData::WriteRawBlockWithFields => Ok(()),
                _ => Err(RawError::from_string("write raw block error"))?,
            }
        } else {
            let raw_block_message = 5; // action number from `taosAdapter/controller/rest/const.go:L56`.

            let mut meta = Vec::new();
            meta.write_u64_le(req_id).map_err(Error::from)?;
            meta.write_u64_le(message_id).map_err(Error::from)?;
            meta.write_u64_le(raw_block_message as u64)
                .map_err(Error::from)?;
            meta.write_u32_le(raw.nrows() as u32).map_err(Error::from)?;
            meta.write_inlined_str::<2>(raw.table_name().unwrap())
                .map_err(Error::from)?;
            meta.write_all(raw.as_raw_bytes()).map_err(Error::from)?;
            let fields = raw
                .fields()
                .into_iter()
                .map(|f| f.to_c_field())
                .collect_vec();

            let fields =
                unsafe { std::slice::from_raw_parts(fields.as_ptr() as _, fields.len() * 72) };
            meta.write_all(fields).map_err(Error::from)?;
            let len = meta.len();
            tracing::trace!("write block with req_id: {req_id}, raw data len: {len}",);

            match time::timeout(
                Duration::from_secs(60),
                self.sender.send_recv(WsSend::Binary(meta)),
            )
            .in_current_span()
            .await
            .map_err(|_| {
                tracing::warn!("Write raw data timeout, maybe the connection has been lost");
                RawError::new(
                    0xE002, // Connection closed
                    "Write raw data timeout, maybe the connection has been lost",
                )
            })?? {
                WsRecvData::WriteRawBlock | WsRecvData::WriteRawBlockWithFields => Ok(()),
                _ => Err(RawError::from_string("write raw block error"))?,
            }
        }
    }

    pub async fn s_query(&self, sql: &str) -> RawResult<ResultSet> {
        let req_id = self.sender.req_id();
        self.s_query_with_req_id(sql, req_id)
            .in_current_span()
            .await
    }

    #[instrument(skip(self))]
    pub async fn s_query_with_req_id(&self, sql: &str, req_id: u64) -> RawResult<ResultSet> {
        let req = if self.is_support_binary_sql() {
            let mut req_vec = Vec::with_capacity(sql.len() + 30);
            req_vec.write_u64_le(req_id).map_err(Error::from)?;
            req_vec.write_u64_le(0).map_err(Error::from)?; //ResultID, uesless here
            req_vec.write_u64_le(6).map_err(Error::from)?; //ActionID, 6 for query
            req_vec.write_u16_le(1).map_err(Error::from)?; //Version
            req_vec
                .write_u32_le(sql.len().try_into().unwrap())
                .map_err(Error::from)?; //SQL length
            req_vec.write_all(sql.as_bytes()).map_err(Error::from)?;

            self.sender.send_recv(WsSend::Binary(req_vec)).await?
        } else {
            let action = WsSend::Query {
                req_id,
                sql: sql.to_string(),
            };
            self.sender.send_recv(action).await?
        };

        let resp = match req {
            WsRecvData::Query(resp) => resp,
            _ => unreachable!(),
        };

        let result_id = resp.id;
        //  for drop task.
        let (closer, rx) = oneshot::channel();
        tokio::task::spawn(
            async move {
                let t = Instant::now();
                let _ = rx.await;
                tracing::trace!("result {result_id} lives {:?}", t.elapsed());
            }
            .in_current_span(),
        );

        if resp.fields_count > 0 {
            let names = resp.fields_names.unwrap();
            let types = resp.fields_types.unwrap();
            let lens = resp.fields_lengths.unwrap();
            let fields: Vec<Field> = names
                .iter()
                .zip(types)
                .zip(lens)
                .map(|((name, ty), len)| Field::new(name, ty, len))
                .collect();

            // Start query.
            let req_id_ref = self.sender.req_id_ref().clone();
            let sender = self.sender.clone();
            let res_id = resp.id;
            let precision = resp.precision;
            let (tx, rx) = flume::bounded(64);
            if sender.version.is_support_binary_sql {
                tokio::spawn(fetch(sender, res_id, tx, precision, names).in_current_span());
            } else {
                // Start query.
                let fields = fields.clone();
                tokio::spawn(
                    async move {
                        let mut metrics = QueryMetrics::default();
                        loop {
                            let now = Instant::now();
                            let args = WsResArgs {
                                req_id: req_id_ref
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                                id: res_id,
                            };
                            let fetch = WsSend::Fetch(args);
                            let fetch_resp = match sender.send_recv(fetch).await {
                                Ok(WsRecvData::Fetch(fetch)) => fetch,
                                Err(err) => {
                                    let _ = tx.send_async(Err(err)).await;
                                    break;
                                }
                                _ => unreachable!("fetch action result error"),
                            };
                            if fetch_resp.completed {
                                drop(tx);
                                break;
                            }
                            let args = WsResArgs {
                                req_id: req_id_ref
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                                id: res_id,
                            };

                            let fetch_block = WsSend::FetchBlock(args);
                            match sender.send_recv(fetch_block).await {
                                Ok(WsRecvData::Block { timing, raw }) => {
                                    metrics.time_cost_in_fetch += now.elapsed();
                                    let mut raw = RawBlock::parse_from_raw_block(raw, precision);
                                    raw.with_field_names(&names);
                                    if tx.send_async(Ok((raw, timing))).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(WsRecvData::BlockV2 { timing, raw }) => {
                                    metrics.time_cost_in_fetch += now.elapsed();
                                    let mut raw = RawBlock::parse_from_raw_block_v2(
                                        raw,
                                        &fields,
                                        fetch_resp.lengths.as_ref().unwrap(),
                                        fetch_resp.rows,
                                        precision,
                                    );

                                    raw.with_field_names(&names);
                                    if tx.send_async(Ok((raw, timing))).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(_) => {}
                                Err(err) => {
                                    metrics.time_cost_in_fetch += now.elapsed();
                                    if tx.send_async(Err(err)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        trace!("Spawn metrics: {:?}", metrics);
                    }
                    .in_current_span(),
                );
            }
            let blocks_buffer = Some(rx);
            Ok(ResultSet {
                fields: Some(fields),
                fields_count: resp.fields_count,
                precision: resp.precision,
                affected_rows: resp.affected_rows,
                args: WsResArgs {
                    req_id,
                    id: resp.id,
                },
                summary: (0, 0),
                sender: self.sender.clone(),
                timing: resp.timing,
                block_future: None,
                closer: Some(closer),
                completed: false,
                metrics: QueryMetrics::default(),
                blocks_buffer,
            })
        } else {
            Ok(ResultSet {
                affected_rows: resp.affected_rows,
                args: WsResArgs {
                    req_id,
                    id: resp.id,
                },
                fields: None,
                fields_count: 0,
                precision: resp.precision,
                summary: (0, 0),
                sender: self.sender.clone(),
                timing: resp.timing,
                block_future: None,
                closer: Some(closer),
                completed: true,
                metrics: QueryMetrics::default(),
                blocks_buffer: None,
            })
        }
    }

    pub async fn s_exec(&self, sql: &str) -> RawResult<usize> {
        let req_id = self.sender.req_id();
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };
        match self.sender.send_recv(action).await? {
            WsRecvData::Query(query) => Ok(query.affected_rows),
            _ => unreachable!(),
        }
    }

    pub async fn s_put(&self, sml: &SmlData) -> RawResult<()> {
        let action = WsSend::Insert {
            protocol: sml.protocol() as u8,
            precision: sml.precision().into(),
            data: sml.data().join("\n"),
            ttl: sml.ttl(),
            req_id: sml.req_id(),
        };
        tracing::trace!("put send: {:?}", action);
        let req = self.sender.send_recv(action).await?;

        match req {
            WsRecvData::Insert(res) => {
                tracing::trace!("put resp : {:?}", res);
                Ok(())
            }
            _ => {
                unreachable!()
            }
        }
    }

    pub fn version(&self) -> &str {
        &self.sender.version.version
    }

    pub fn is_support_binary_sql(&self) -> bool {
        self.sender.version.is_support_binary_sql
    }

    pub fn get_req_id(&self) -> ReqId {
        self.sender.req_id()
    }

    pub(crate) async fn send_request(&self, req: WsSend) -> RawResult<WsRecvData> {
        self.sender.send_recv(req).await
    }

    pub(crate) fn sender(&self) -> WsQuerySender {
        self.sender.clone()
    }
}

pub(crate) async fn fetch(
    sender: WsQuerySender,
    res_id: ResId,
    raw_block_tx: Sender<Result<(RawBlock, Duration), RawError>>,
    precision: Precision,
    field_names: Vec<String>,
) -> RawResult<()> {
    const ACTION: u64 = 7;
    const VERSION: u16 = 1;

    let mut bytes = vec![0u8; 26];
    LittleEndian::write_u64(&mut bytes[8..], res_id);
    LittleEndian::write_u64(&mut bytes[16..], ACTION);
    LittleEndian::write_u16(&mut bytes[24..], VERSION);

    let mut metrics = QueryMetrics::default();

    loop {
        LittleEndian::write_u64(&mut bytes, generate_req_id());

        let fetch_start = Instant::now();
        match sender.send_recv(WsSend::Binary(bytes.clone())).await {
            Ok(WsRecvData::BlockNew {
                block_code,
                block_message,
                timing,
                finished,
                raw,
                ..
            }) => {
                metrics.num_of_fetches += 1;
                metrics.time_cost_in_fetch += fetch_start.elapsed();

                if block_code != 0 {
                    return Err(RawError::new(block_code, block_message));
                }

                if finished {
                    tracing::trace!("Finished processing result:{res_id}");
                    drop(raw_block_tx);
                    break;
                }

                let parse_start = Instant::now();
                let mut raw_block = RawBlock::parse_from_raw_block(raw, precision);
                metrics.time_cost_in_block_parse += parse_start.elapsed();

                raw_block.with_field_names(&field_names);
                if raw_block_tx
                    .send_async(Ok((raw_block, timing)))
                    .await
                    .is_err()
                {
                    tracing::warn!("Failed to send raw block; receiver may be closed");
                    break;
                }
            }
            Ok(_) => tracing::warn!("Unexpected response for result:{res_id}"),
            Err(err) => {
                if raw_block_tx.send_async(Err(err)).await.is_err() {
                    tracing::error!("Failed to send err; receiver may be closed");
                    break;
                }
            }
        }
    }

    tracing::trace!("Metrics for result:{res_id}: {metrics:?}");
    Ok(())
}

impl ResultSet {
    async fn fetch(&mut self) -> RawResult<Option<RawBlock>> {
        if self.blocks_buffer.is_none() {
            return Ok(None);
        }
        let now = Instant::now();
        match self.blocks_buffer.as_mut().unwrap().recv_async().await {
            Ok(Ok((raw, timing))) => {
                self.timing = timing;
                self.metrics.time_cost_in_flume += now.elapsed();
                Ok(Some(raw))
            }
            Ok(Err(err)) => Err(err),
            Err(_) => Ok(None),
        }
    }

    pub fn take_timing(&self) -> Duration {
        self.timing
    }

    pub async fn stop(&self) {
        if let Some((_, req_id)) = self.sender.results.remove_async(&self.args.id).await {
            self.sender.queries.remove_async(&req_id).await;
        }

        let _ = self.sender.send_only(WsSend::FreeResult(self.args)).await;
    }

    pub fn affected_rows64(&self) -> i64 {
        self.affected_rows as _
    }
}

impl AsyncFetchable for ResultSet {
    fn affected_rows(&self) -> i32 {
        self.affected_rows as i32
    }

    fn precision(&self) -> taos_query::common::Precision {
        self.precision
    }

    fn fields(&self) -> &[Field] {
        static EMPTY_FIELDS: Vec<Field> = Vec::new();
        self.fields.as_ref().unwrap_or(&EMPTY_FIELDS)
    }

    fn summary(&self) -> (usize, usize) {
        self.summary
    }

    fn update_summary(&mut self, nrows: usize) {
        self.summary.0 += 1;
        self.summary.1 += nrows;
    }

    fn fetch_raw_block(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<RawResult<Option<RawBlock>>> {
        if let Some(mut f) = self.block_future.take() {
            // let mut f = self.block_future.take().unwrap();
            let res = f.poll_unpin(cx);
            match res {
                std::task::Poll::Ready(v) => Poll::Ready(v),
                std::task::Poll::Pending => {
                    self.block_future = Some(f);
                    Poll::Pending
                }
            }
        } else {
            let mut f = self.fetch().boxed();
            let res = f.poll_unpin(cx);
            match res {
                std::task::Poll::Ready(v) => Poll::Ready(v),
                std::task::Poll::Pending => {
                    self.block_future = Some(unsafe { transmute(f) });
                    Poll::Pending
                }
            }
        }
        // let future = self.fetch().boxed();
        // // .poll_unpin(cx)
        // todo!()
    }
}

impl taos_query::Fetchable for ResultSet {
    fn affected_rows(&self) -> i32 {
        self.affected_rows as i32
    }

    fn precision(&self) -> taos_query::common::Precision {
        self.precision
    }

    fn fields(&self) -> &[Field] {
        static EMPTY: Vec<Field> = Vec::new();
        self.fields.as_deref().unwrap_or(EMPTY.as_slice())
    }

    fn summary(&self) -> (usize, usize) {
        self.summary
    }

    fn update_summary(&mut self, nrows: usize) {
        self.summary.0 += 1;
        self.summary.1 += nrows;
    }

    fn fetch_raw_block(&mut self) -> RawResult<Option<RawBlock>> {
        taos_query::block_in_place_or_global(self.fetch())
    }
}

#[async_trait::async_trait]
impl AsyncQueryable for WsTaos {
    type AsyncResultSet = ResultSet;

    #[instrument(skip_all)]
    async fn query<T: AsRef<str> + Send + Sync>(&self, sql: T) -> RawResult<Self::AsyncResultSet> {
        self.s_query(sql.as_ref()).in_current_span().await
    }

    #[instrument(skip_all)]
    async fn query_with_req_id<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
        req_id: u64,
    ) -> RawResult<Self::AsyncResultSet> {
        self.s_query_with_req_id(sql.as_ref(), req_id).await
    }

    #[instrument(skip_all)]
    async fn write_raw_meta(&self, raw: &RawMeta) -> RawResult<()> {
        self.write_meta(raw).in_current_span().await
    }

    #[instrument(skip_all)]
    async fn write_raw_block(&self, block: &RawBlock) -> RawResult<()> {
        self.s_write_raw_block(block).await
    }

    #[instrument(skip_all)]
    async fn write_raw_block_with_req_id(&self, block: &RawBlock, req_id: u64) -> RawResult<()> {
        self.s_write_raw_block_with_req_id(block, req_id)
            .in_current_span()
            .await
    }

    async fn put(&self, data: &SmlData) -> RawResult<()> {
        self.s_put(data).in_current_span().await
    }
}

#[cfg(test)]
mod tests {
    use flume::unbounded;
    use futures::TryStreamExt;

    use super::*;

    #[test]
    fn test_errno() {
        let (tx, rx) = unbounded();
        drop(rx);

        let send_err = tx.send(Message::Text("oh, no!".to_string())).unwrap_err();
        let err = Error::FlumeSendError(send_err);
        let errno: i32 = err.errno().into();
        assert_eq!(WS_ERROR_NO::CONN_CLOSED as i32, errno);
    }

    #[test]
    fn test_is_support_binary_sql() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");

        let version_a: &str = "3.3.0.0";
        let version_b: &str = "3.3.3.0";
        let version_c: &str = "2.6.0";

        assert!(!is_support_binary_sql(version_a));
        assert!(is_support_binary_sql(version_b));
        assert!(!is_support_binary_sql(version_c));

        Ok(())
    }

    #[tokio::test]
    async fn test_client() -> anyhow::Result<()> {
        use futures::TryStreamExt;
        std::env::set_var("RUST_LOG", "debug");
        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        // pretty_env_logger::init();

        let client = WsTaos::from_dsn(dsn).await?;

        let _version = client.version();
        assert_eq!(client.exec("drop database if exists abc_a").await?, 0);
        assert_eq!(client.exec("create database abc_a").await?, 0);
        assert_eq!(
            client
                .exec("create table abc_a.tb1(ts timestamp, v int)")
                .await?,
            0
        );
        assert_eq!(
            client
                .exec("insert into abc_a.tb1 values(1655793421375, 1)")
                .await?,
            1
        );

        let mut rs = client.query("select * from abc_a.tb1").await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: i32,
        }

        let values: Vec<A> = rs.deserialize().try_collect().await?;

        dbg!(values);

        assert_eq!(client.exec("drop database abc_a").await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_client_cloud() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let dsn = std::env::var("TDENGINE_ClOUD_DSN");
        if dsn.is_err() {
            println!("Skip test when not in cloud");
            return Ok(());
        }
        let dsn = dsn.unwrap();
        let client = WsTaos::from_dsn(dsn).await?;
        let mut rs = client.query("select * from test.meters limit 10").await?;

        let values = rs.to_records().await?;
        for row in values {
            use itertools::Itertools;
            println!(
                "{}",
                row.into_iter()
                    .map(|value| format!("{value:?}"))
                    .join(" | ")
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn ws_show_databases() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");
        use futures::TryStreamExt;
        // let _ = pretty_env_logger::try_init_timed();
        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        let client = WsTaos::from_dsn(dsn).await?;
        let mut rs = client.query("show databases").await?;

        let mut blocks = rs.blocks();
        while let Some(block) = blocks.try_next().await? {
            let values = block.to_values();
            dbg!(values);
        }
        Ok(())
    }

    #[tokio::test]
    async fn ws_write_raw_block() -> anyhow::Result<()> {
        let mut raw = RawBlock::parse_from_raw_block_v2(
            &[0, 0, 0, 0, 0, 0, 0, 0, 2][..],
            &[
                Field::new("ts", taos_query::common::Ty::Timestamp, 8),
                Field::new("v", taos_query::common::Ty::Bool, 1),
            ],
            &[8, 1],
            1,
            Precision::Millisecond,
        );
        raw.with_table_name("tb1");
        dbg!(&raw);

        use futures::TryStreamExt;
        std::env::set_var("RUST_LOG", "debug");
        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        // pretty_env_logger::init();

        let client = WsTaos::from_dsn(dsn).await?;

        let _version = client.version();

        client
            .exec_many([
                "drop database if exists write_raw_block_test",
                "create database write_raw_block_test keep 36500",
                "use write_raw_block_test",
                "create table if not exists tb1(ts timestamp, v bool)",
            ])
            .await?;

        client.write_raw_block(&raw).await?;

        let mut rs = client.query("select * from tb1").await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: Option<bool>,
        }

        let values: Vec<A> = rs.deserialize().try_collect().await?;

        dbg!(values);

        assert_eq!(client.exec("drop database write_raw_block_test").await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn ws_write_raw_block_with_req_id() -> anyhow::Result<()> {
        let mut raw = RawBlock::parse_from_raw_block_v2(
            &[0, 0, 0, 0, 0, 0, 0, 0, 2][..],
            &[
                Field::new("ts", taos_query::common::Ty::Timestamp, 8),
                Field::new("v", taos_query::common::Ty::Bool, 1),
            ],
            &[8, 1],
            1,
            Precision::Millisecond,
        );
        raw.with_table_name("tb1");
        dbg!(&raw);

        use futures::TryStreamExt;
        std::env::set_var("RUST_LOG", "debug");
        let dsn = std::env::var("TDENGINE_TEST_DSN").unwrap_or("http://localhost:6041".to_string());
        // pretty_env_logger::init();

        let client = WsTaos::from_dsn(dsn).await?;

        let _version = client.version();

        client
            .exec_many([
                "drop database if exists test_ws_write_raw_block_with_req_id",
                "create database test_ws_write_raw_block_with_req_id keep 36500",
                "use test_ws_write_raw_block_with_req_id",
                "create table if not exists tb1(ts timestamp, v bool)",
            ])
            .await?;

        let req_id = 10003;
        client.write_raw_block_with_req_id(&raw, req_id).await?;

        let mut rs = client.query("select * from tb1").await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: Option<bool>,
        }

        let values: Vec<A> = rs.deserialize().try_collect().await?;

        dbg!(values);

        assert_eq!(
            client
                .exec("drop database test_ws_write_raw_block_with_req_id")
                .await?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn ws_persistent_connection() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "trace");
        pretty_env_logger::init();
        let client = WsTaos::from_dsn("taosws://localhost:6041/").await?;
        let db = "ws_persistent_connection";
        assert_eq!(
            client.exec(format!("drop database if exists {db}")).await?,
            0
        );
        assert_eq!(
            client
                .exec(format!("create database {db} keep 36500"))
                .await?,
            0
        );
        assert_eq!(
            client.exec(
                format!("create table {db}.stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16)) tags (jt json)")
            ).await?,
            0
        );

        // loop n times to test persistent connection
        // do not run in ci env
        let n = 100;
        let interval = Duration::from_secs(3);
        for _ in 0..n {
            assert_eq!(
                client
                    .exec(format!(
                        r#"insert into {db}.tb1 using {db}.stb1 tags('{{"key":"数据"}}')
                   values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                    ))
                    .await?,
                2
            );
            assert_eq!(
                client
                    .exec(format!(
                        r#"insert into {db}.tb2 using {db}.stb1 tags(NULL)
                   values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                    ))
                    .await?,
                2
            );
            // wait to test persistent connection
            tokio::time::sleep(interval).await;
        }

        client.exec(format!("drop database {db}")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ws_async_data_flow() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let client = WsTaos::from_dsn("taosws://localhost:6041/").await?;
        let db = "ws_async_data_flow";
        assert_eq!(
            client.exec(format!("drop database if exists {db}")).await?,
            0
        );
        assert_eq!(
            client
                .exec(format!("create database {db} keep 36500"))
                .await?,
            0
        );
        assert_eq!(
            client.exec(
                format!("create table {db}.stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16)) tags (jt json)")
            ).await?,
            0
        );
        assert_eq!(
            client
                .exec(format!(
                    r#"insert into {db}.tb1 using {db}.stb1 tags('{{"key":"数据"}}')
                   values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                ))
                .await?,
            2
        );
        assert_eq!(
            client
                .exec(format!(
                    r#"insert into {db}.tb2 using {db}.stb1 tags(NULL)
                   values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                ))
                .await?,
            2
        );

        let mut rs = client
            .query(format!("select * from {db}.tb1 order by ts limit 1"))
            .await?;

        #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            b1: bool,
            c8i1: i8,
            c16i1: i16,
            c32i1: i32,
            c64i1: i64,
            c8u1: u8,
            c16u1: u16,
            c32u1: u32,
            c64u1: u64,

            c8i2: i8,
            c16i2: i16,
            c32i2: i32,
            c64i2: i64,
            c8u2: u8,
            c16u2: u16,
            c32u2: u32,
            c64u2: u64,

            cb1: String,
            cb2: String,
            cn1: String,
            cn2: String,
        }

        let values: Vec<A> = rs.deserialize().try_collect().await?;

        assert_eq!(
            values[0],
            A {
                ts: "1970-01-01T08:00:00+08:00".to_string(),
                b1: true,
                c8i1: -1,
                c16i1: -2,
                c32i1: -3,
                c64i1: -4,
                c8u1: 1,
                c16u1: 2,
                c32u1: 3,
                c64u1: 4,
                c8i2: -5,
                c16i2: -6,
                c32i2: -7,
                c64i2: -8,
                c8u2: 5,
                c16u2: 6,
                c32u2: 7,
                c64u2: 8,
                cb1: "abc".to_string(),
                cb2: "def".to_string(),
                cn1: "涛思".to_string(),
                cn2: "数据".to_string(),
            }
        );

        client.exec(format!("drop database {db}")).await?;
        Ok(())
    }
}
