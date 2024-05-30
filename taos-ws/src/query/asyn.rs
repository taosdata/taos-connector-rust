use futures::{FutureExt, SinkExt, StreamExt};
// use scc::HashMap;
use dashmap::DashMap as HashMap;
use itertools::Itertools;
use std::future::Future;
use taos_query::common::{Field, Precision, RawBlock, RawMeta, SmlData};
use taos_query::prelude::{Code, RawError, RawResult};
use taos_query::util::InlinableWrite;
use taos_query::{AsyncFetchable, AsyncQueryable, DeError, DsnError, IntoDsn};
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

use tokio::io::BufStream;
use tokio::io::ReadHalf;
use tokio::sync::watch;

use tokio::time;

use ws_tool::{
    codec::AsyncDeflateRecv, errors::WsError as WsErrorWst, frame::OpCode, stream::AsyncStream,
    Message as WsMessage,
};

use super::{infra::*, TaosBuilder};

use std::fmt::Debug;
use std::io::Write;
use std::mem::transmute;
use std::pin::Pin;
// use std::io::Write;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

type WsSender = tokio::sync::mpsc::Sender<WsMessage<bytes::Bytes>>;

use futures::channel::oneshot;
use oneshot::channel as query_channel;
type QueryChannelSender = oneshot::Sender<RawResult<WsRecvData>>;
// use tokio::sync::mpsc::unbounded_channel as query_channel;
// type QueryChannelSender = tokio::sync::mpsc::UnboundedSender<Result<WsRecvData, RawError>>;
type QueryInner = HashMap<ReqId, QueryChannelSender>;
type QueryAgent = Arc<QueryInner>;
type QueryResMapper = HashMap<ResId, ReqId>;

#[derive(Debug, Clone)]
struct Version{
    version: String,
    is_support_binary_sql: bool,
}

// impl Version {
//     pub fn is_v3(&self) -> bool {
//         !self.0.starts_with("2")
//     }
// }
#[derive(Debug, Clone)]
struct WsQuerySender {
    version: Version,
    req_id: Arc<AtomicU64>,
    results: Arc<QueryResMapper>,
    sender: WsSender,
    queries: QueryAgent,
}

impl WsQuerySender {
    fn req_id(&self) -> ReqId {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    async fn send_recv(&self, msg: WsSend) -> RawResult<WsRecvData> {
        let send_timeout = Duration::from_millis(1000);
        let req_id = msg.req_id();
        let (tx, rx) = query_channel();

        self.queries.insert(req_id, tx);

        match msg {
            WsSend::FetchBlock(args) => {
                log::trace!("[req id: {req_id}] prepare message {msg:?}");
                if self.results.contains_key(&args.id) {
                    Err(RawError::from_string(format!(
                        "there's a result with id {}",
                        args.id
                    )))?;
                }
                self.results.insert(args.id, args.req_id);

                self.sender
                    .send_timeout(msg.to_msg(), send_timeout)
                    .await
                    .map_err(Error::from)?;
                //
            }
            WsSend::Binary(bytes) => {
                self.sender
                    .send_timeout(
                        WsMessage {
                            code: OpCode::Binary,
                            data: bytes.into(),
                            close_code: None,
                        },
                        send_timeout,
                    )
                    .await
                    .map_err(Error::from)?;
            }
            _ => {
                log::trace!("[req id: {req_id}] prepare  message: {msg:?}");
                self.sender
                    .send_timeout(msg.to_msg(), send_timeout)
                    .await
                    .map_err(Error::from)?;
            }
        }
        // handle the error
        log::trace!("[req id: {req_id}] message sent, wait for receiving");
        let res = rx.await.unwrap().map_err(Error::from)?;
        log::trace!("[req id: {req_id}] message received: {res:?}");
        Ok(res)
    }
    async fn send_only(&self, msg: WsSend) -> RawResult<()> {
        let send_timeout = Duration::from_millis(1000);
        self.sender
            .send_timeout(msg.to_msg(), send_timeout)
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    fn send_blocking(&self, msg: WsSend) -> RawResult<()> {
        let _ = self.sender.blocking_send(msg.to_msg());
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
        log::trace!("dropping connection");
        // send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

pub struct ResultSet {
    sender: WsQuerySender,
    args: WsResArgs,
    fields: Option<Vec<Field>>,
    fields_count: usize,
    affected_rows: usize,
    precision: Precision,
    summary: (usize, usize),
    timing: Duration,
    block_future: Option<Pin<Box<dyn Future<Output = RawResult<Option<RawBlock>>> + Send>>>,
    closer: Option<oneshot::Sender<()>>,
    completed: bool,
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
            .finish()
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        if let Some((_, req_id)) = self.sender.results.remove(&self.args.id) {
            self.sender.queries.remove(&req_id);
        }

        if !self.completed {
            std::thread::scope(|s| {
                s.spawn(|| {
                    self.free_result();
                });
            });
        }

        let _ = self.closer.take().unwrap().send(());
        // let _ = self
        //     .sender
        //     .blocking_send_only(WsSend::FreeResult(self.args));

        // tokio::spawn(async move { sender.send_only(WsSend::FreeResult(self.args)).await });
        // taos_query::block_in_place_or_global(async move {
        //     let _ = self.sender.send_only(WsSend::FreeResult(self.args)).await;
        // });
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
    #[error("{0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<WsMessage<bytes::Bytes>>),
    #[error(transparent)]
    SendTimeoutError(#[from] tokio::sync::mpsc::error::SendTimeoutError<WsMessage<bytes::Bytes>>),
    #[error("Query timed out with sql: {0}")]
    QueryTimeout(String),
    #[error("{0}")]
    TaosError(#[from] RawError),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("WebSocket internal[ws-tool] error: {0}")]
    WsErrorWst(#[from] WsErrorWst),
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
            Error::WsErrorWst(_) => Code::new(WS_ERROR_NO::WEBSOCKET_ERROR as _),
            Error::SendTimeoutError(_) => Code::new(WS_ERROR_NO::SEND_MESSAGE_TIMEOUT as _),
            // Error::RecvTimeout(_) => Code::new(WS_ERROR_NO::RECV_MESSAGE_TIMEOUT as _),
            _ => Code::FAILED,
        }
    }
    pub fn errstr(&self) -> String {
        match self {
            Error::TaosError(error) => error.message().to_string(),
            _ => format!("{}", self),
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

async fn read_queries(
    mut reader: AsyncDeflateRecv<ReadHalf<BufStream<AsyncStream>>>,
    queries_sender: QueryAgent,
    fetches_sender: Arc<QueryResMapper>,
    ws2: WsSender,
    is_v3: bool,
    mut close_listener: watch::Receiver<bool>,
) {
    let ws3 = ws2.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(29));
        loop {
            interval.tick().await;
            if let Err(err) = ws3
                .send(WsMessage {
                    code: OpCode::Ping,
                    data: b"TAOS".to_vec().into(),
                    close_code: None,
                })
                .await
            {
                log::trace!("sending ping message error: {err:?}");
                break;
            }
        }
    });
    'ws: loop {
        tokio::select! {
            Ok(frame) = reader.receive() => {
                let (header, payload) = frame;
                let code = header.code;
                match code {
                    OpCode::Text => {

                        log::trace!("received json response: {payload}", payload = String::from_utf8_lossy(&payload));
                        let v: WsRecv = serde_json::from_slice(&payload).unwrap();
                        
                        let (req_id, data, ok) = v.ok();
                        match &data {
                            WsRecvData::Query(_) => {
                                if let Some((_, sender)) = queries_sender.remove(&req_id)
                                {
                                    if let Err(err) = sender.send(ok.map(|_| data)) {
                                        log::error!("send data with error: {err:?}");
                                    }
                                } else {
                                    debug_assert!(!queries_sender.contains_key(&req_id));
                                    log::warn!("req_id {req_id} not detected, message might be lost");
                                }
                            }
                            WsRecvData::Fetch(fetch) => {
                                let id = fetch.id;
                                if fetch.completed {
                                    ws2.send(
                                        WsSend::FreeResult(WsResArgs {
                                            req_id,
                                            id,
                                        })
                                        .to_msg(),
                                    )
                                    .await
                                    .unwrap();
                                }
                                // dbg!(&queries_sender);
                                if let Some((_, sender)) = queries_sender.remove(&req_id)
                                {
                                    sender.send(ok.map(|_| data)).unwrap();
                                } else {
                                    log::warn!("req_id {req_id} not detected, message might be lost");
                                }
                            }
                            WsRecvData::FetchBlock => {
                                assert!(ok.is_err());
                                if let Some((_, sender)) = queries_sender.remove(&req_id)
                                {
                                    sender.send(ok.map(|_| data)).unwrap();
                                } else {
                                    log::warn!("req_id {req_id} not detected, message might be lost");
                                }
                            }
                            WsRecvData::WriteMeta => {
                                if let Some((_, sender)) = queries_sender.remove(&req_id)
                                {
                                    sender.send(ok.map(|_| data)).unwrap();
                                } else {
                                    log::warn!("req_id {req_id} not detected, message might be lost");
                                }
                            }
                            WsRecvData::WriteRaw => {
                                if let Some((_, sender)) = queries_sender.remove(&req_id)
                                {
                                    sender.send(ok.map(|_| data)).unwrap();
                                } else {
                                    log::warn!("req_id {req_id} not detected, message might be lost");
                                }
                            }
                            WsRecvData::WriteRawBlock | WsRecvData::WriteRawBlockWithFields => {
                                if let Some((_, sender)) = queries_sender.remove(&req_id)
                                {
                                    sender.send(ok.map(|_| data)).unwrap();
                                } else {
                                    log::warn!("req_id {req_id} not detected, message might be lost");
                                }
                            }

                            // Block type is for binary.
                            _ => unreachable!(),
                        }
                    }
                    OpCode::Binary => {
                        let block = payload;
                        let mut slice = block;
                        use taos_query::util::InlinableRead;
                        let offset = if is_v3 { 16 } else { 8 };

                        let mut is_block_new : bool = false;

                        let timing = if is_v3 {
                            let timing = slice.read_u64().unwrap();
                            if timing == std::u64::MAX {
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
                            let result_block : Vec<u8>;
                            if finished {
                                result_block = Vec::<u8>::new();
                            } else {
                                result_block = slice.read_inlined_bytes::<4>().unwrap();
                            }
              
                            if let Some((_, sender)) = queries_sender.remove(&block_req_id) {
                                sender.send(Ok(WsRecvData::BlockNew {
                                    block_version,
                                    timing,
                                    block_req_id,
                                    block_code,
                                    block_message,
                                    finished,
                                    raw: result_block.to_vec(),
                                })).unwrap();
                            } else {
                                log::warn!("req_id {block_req_id} not detected, message might be lost");
                            }
                            continue;
                        }

                        let res_id = slice.read_u64().unwrap();
                        if let Some((_, req_id)) =  fetches_sender.remove(&res_id) {
                            if is_v3 {
                                // v3
                                if let Some((_, sender)) = queries_sender.remove(&req_id) {
                                    log::trace!("send data to fetches with id {}", res_id);
                                    sender.send(Ok(WsRecvData::Block { timing, raw: block[offset..].to_vec() })).unwrap();
                                } else {
                                    log::warn!("req_id {res_id} not detected, message might be lost");
                                }
                            } else {
                                // v2
                                if let Some((_, sender)) = queries_sender.remove(&req_id) {
                                    log::trace!("send data to fetches with id {}", res_id);
                                    sender.send(Ok(WsRecvData::BlockV2 { timing, raw: block[offset..].to_vec() })).unwrap();
                                } else {
                                    log::warn!("req_id {res_id} not detected, message might be lost");
                                }
                            }
                        } else {
                            log::warn!("result id {res_id} not found");
                        }
                    }
                    OpCode::Close => {
                        // taosAdapter should never send close frame to client.
                        // So all close frames should be treated as error.

                        log::warn!("websocket connection is closed normally");
                        let mut keys = Vec::new();
                        for e in queries_sender.iter() {
                            keys.push(*e.key());
                        }
                        for k in keys {
                            if let Some((_, sender)) = queries_sender.remove(&k) {
                                let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), "received close message")));
                            }
                        }

                        break 'ws;
                    }
                    OpCode::Ping => {
                        let bytes = payload.to_vec();
                        ws2.send(WsMessage{
                            code: OpCode::Pong,
                            data: bytes.into(),
                            close_code: None
                        }).await.unwrap();
                    }
                    OpCode::Pong => {
                        // do nothing
                        log::trace!("received pong message, do nothing");
                    }
                    _ => {
                        let frame = payload;
                        // do nothing
                        log::warn!("received (unexpected) frame message, do nothing");
                        log::trace!("* frame data: {frame:?}");
                    }
                }
            }
            _ = close_listener.changed() => {
                log::trace!("close reader task");
                let mut keys = Vec::new();
                for e in queries_sender.iter() {
                                    keys.push(*e.key());
                                }
                // queries_sender.for_each_async(|k, _| {
                //     keys.push(*k);
                // }).await;
                for k in keys {
                    if let Some((_, sender)) = queries_sender.remove(&k) {
                        let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), "close signal received")));
                    }
                }
                break 'ws;
            }
        }
    }
    if queries_sender.is_empty() {
        return;
    }

    let mut keys = Vec::new();
    for e in queries_sender.iter() {
        keys.push(*e.key());
    }
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
    let nums1: Vec<u32> = v1.split('.').map(|s| s.parse().unwrap()).collect();
    let nums2: Vec<u32> = v2.split('.').map(|s| s.parse().unwrap()).collect();

    nums1.cmp(&nums2)
}

pub fn is_greater_than_or_equal_to(v1: &str, v2: &str) -> bool {
    match compare_versions(v1, v2) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => true,
        std::cmp::Ordering::Greater => true,
    }
}

pub fn is_support_binary_sql(_v1: &str) -> bool {
    // is_greater_than_or_equal_to(v1, "3.3.0.8")
    true
}

impl WsTaos {
    /// Build TDengine websocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041/
    /// ```
    ///
    pub async fn from_dsn(dsn: impl IntoDsn) -> RawResult<Self> {
        let dsn = dsn.into_dsn()?;
        let info = TaosBuilder::from_dsn(dsn)?;
        Self::from_wsinfo(&info).await
    }
    #[allow(dead_code)]
    pub(crate) async fn tung_from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {
        let ws = info.build_stream(info.to_query_url()).await?;

        let req_id = 0;
        let (mut sender, mut reader) = ws.split();

        let version = WsSend::Version;
        sender
            .send(version.to_tungstenite_msg())
            .await
            .map_err(Error::from)?;

        let duration = Duration::from_secs(2);
        let version = match tokio::time::timeout(duration, reader.next()).await {
            Ok(Some(Ok(message))) => match message {
                Message::Text(text) => {
                    let v: WsRecv = serde_json::from_str(&text).unwrap();
                    let (_, data, ok) = v.ok();
                    match data {
                        WsRecvData::Version { version } => {
                            ok?;
                            version
                        }
                        _ => "2.x".to_string(),
                    }
                }
                _ => "2.x".to_string(),
            },
            _ => "2.x".to_string(),
        };
        let _is_v3 = !version.starts_with('2');
        let is_support_binary_sql = is_support_binary_sql(&version);

        let login = WsSend::Conn {
            req_id,
            req: info.to_conn_request(),
        };
        sender
            .send(login.to_tungstenite_msg())
            .await
            .map_err(Error::from)?;
        if let Some(Ok(message)) = reader.next().await {
            match message {
                Message::Text(text) => {
                    let v: WsRecv = serde_json::from_str(&text).unwrap();
                    let (_req_id, data, ok) = v.ok();
                    match data {
                        WsRecvData::Conn => ok?,
                        _ => unreachable!(),
                    }
                }
                _ => {
                    return Err(RawError::from_string(format!(
                        "unexpected message on login: {:?}",
                        message
                    )));
                }
            }
        }

        let queries2 = Arc::new(QueryInner::new());

        let fetches_sender = Arc::new(QueryResMapper::new());
        let results = fetches_sender.clone();

        let queries2_cloned = queries2.clone();
        let queries3 = queries2.clone();

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel(100);
        let _ws2 = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let _close_listener = rx.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(10));

            'ws: loop {
                tokio::select! {
                    _ = interval.tick() => {
                        //
                        // println!("10ms passed");
                    }
                    Some(msg) = msg_recv.recv() => {
                        // dbg!(&msg);
                        if let Err(err) = sender.send(msg).await {
                            log::error!("Write websocket error: {}", err);
                                let mut keys = Vec::new();
                                queries3.iter().for_each(|r| keys.push(*r.key()));
                                // queries3.for_each_async(|k, _| {
                                //     keys.push(*k);
                                // }).await;
                                for k in keys {
                                    if let Some((_, sender)) = queries3.remove(&k) {
                                        let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), err.to_string())));
                                    }
                                }
                                break 'ws;
                            }
                    }
                    _ = rx.changed() => {
                        let _ = sender.close().await;
                        log::trace!("close sender task");
                        break 'ws;
                    }
                }
            }
        });

        tokio::spawn(async move {
            // read_queries(reader, queries2, fetches_sender, ws2, is_v3, close_listener).await
        });
        let (ws, mut _msg_recv) = tokio::sync::mpsc::channel(100);
        let ws_cloned: tokio::sync::mpsc::Sender<WsMessage<bytes::Bytes>> = ws.clone();

        Ok(Self {
            close_signal: tx,
            sender: WsQuerySender {
                version: Version{version : version,
                        is_support_binary_sql : is_support_binary_sql},
                req_id: Default::default(),
                sender: ws_cloned,
                queries: queries2_cloned,
                results,
            },
        })
    }

    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {
        let ws = info.ws_tool_build_stream(info.to_query_url()).await?;

        let req_id = 0;
        let (mut reader, mut sender) = ws.split();

        let version = WsSend::Version;
        sender
            .send(OpCode::Text, &serde_json::to_vec(&version).unwrap())
            .await
            .map_err(Error::from)?;

        let duration = Duration::from_secs(2);
        let version = match tokio::time::timeout(duration, reader.receive()).await {
            Ok(Ok(frame)) => {
                let (header, payload) = frame;
                let code = header.code;
                match code {
                    OpCode::Text => {
                        let v: WsRecv = serde_json::from_slice(&payload).unwrap();
                        let (_, data, ok) = v.ok();
                        match data {
                            WsRecvData::Version { version } => {
                                ok?;
                                version
                            }
                            _ => "2.x".to_string(),
                        }
                    }
                    _ => "2.x".to_string(),
                }
            }
            _ => "2.x".to_string(),
        };
        let is_support_binary_sql = is_support_binary_sql(&version);
        let is_v3 = !version.starts_with('2');

        let login = WsSend::Conn {
            req_id,
            req: info.to_conn_request(),
        };
        sender
            .send(OpCode::Text, &serde_json::to_vec(&login).unwrap())
            .await
            .map_err(Error::from)?;

        if let Ok(frame) = reader.receive().await {
            let (header, payload) = frame;
            let code = header.code;
            match code {
                OpCode::Text => {
                    let v: WsRecv = serde_json::from_slice(&payload).unwrap();
                    let (_req_id, data, ok) = v.ok();
                    match data {
                        WsRecvData::Conn => ok?,
                        _ => unreachable!(),
                    }
                }
                _ => {
                    return Err(RawError::from_string(format!(
                        "unexpected frame on login: {:?}",
                        frame
                    )));
                }
            }
        }

        let queries2 = Arc::new(QueryInner::new());

        let fetches_sender = Arc::new(QueryResMapper::new());
        let results = fetches_sender.clone();

        let queries2_cloned = queries2.clone();
        let queries3 = queries2.clone();

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel(100);
        let ws2: tokio::sync::mpsc::Sender<WsMessage<bytes::Bytes>> = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let close_listener = rx.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(10));

            'ws: loop {
                tokio::select! {
                    _ = interval.tick() => {
                        //
                        // println!("10ms passed");
                    }
                    Some(msg) = msg_recv.recv() => {
                        // dbg!(&msg);
                        let opcode = msg.code;
                        let msg = msg.data;
                        if let Err(err) = sender.send(opcode, &msg).await {
                            log::error!("Write websocket error: {}", err);
                                let mut keys = Vec::new();
                                queries3.iter().for_each(|r| keys.push(*r.key()));

                                for k in keys {
                                    if let Some((_, sender)) = queries3.remove(&k) {
                                        let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), err.to_string())));
                                    }
                                }
                                break 'ws;
                            }
                    }
                    _ = rx.changed() => {
                        let _ = sender.send(OpCode::Close, b"").await;
                        log::trace!("close sender task");
                        break 'ws;
                    }
                }
            }
        });

        tokio::spawn(async move {
            read_queries(reader, queries2, fetches_sender, ws2, is_v3, close_listener).await
        });
        let ws_cloned = ws.clone();

        Ok(Self {
            close_signal: tx,
            sender: WsQuerySender {
                version: Version{version : version,
                    is_support_binary_sql : is_support_binary_sql},
                req_id: Default::default(),
                sender: ws_cloned,
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

        log::trace!("write meta with req_id: {req_id}, raw data length: {len}",);

        match self.sender.send_recv(WsSend::Binary(meta)).await? {
            WsRecvData::WriteMeta => Ok(()),
            WsRecvData::WriteRaw => Ok(()),
            _ => unreachable!(),
        }
    }
    async fn s_write_raw_block(&self, raw: &RawBlock) -> RawResult<()> {
        let req_id = self.sender.req_id();
        let message_id = req_id;
        // if self.version().starts_with('2') {
        //     panic!("TDengine v2.x does not support to write_raw_block");
        // }
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
            log::trace!("write block with req_id: {req_id}, raw data len: {len}",);

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
            log::trace!("write block with req_id: {req_id}, raw data len: {len}",);

            match self.sender.send_recv(WsSend::Binary(meta)).await? {
                WsRecvData::WriteRawBlock | WsRecvData::WriteRawBlockWithFields => Ok(()),
                _ => Err(RawError::from_string("write raw block error"))?,
            }
        }
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
            log::trace!("write block with req_id: {req_id}, raw data len: {len}",);

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
            log::trace!("write block with req_id: {req_id}, raw data len: {len}",);

            match self.sender.send_recv(WsSend::Binary(meta)).await? {
                WsRecvData::WriteRawBlock | WsRecvData::WriteRawBlockWithFields => Ok(()),
                _ => Err(RawError::from_string("write raw block error"))?,
            }
        }
    }

    pub async fn s_query(&self, sql: &str) -> RawResult<ResultSet> {
        let req_id = self.sender.req_id();
        return self.s_query_with_req_id(sql, req_id).await
    }

    pub async fn s_query_with_req_id(&self, sql: &str, req_id: u64) -> RawResult<ResultSet> {
        let req;
        if self.is_support_binary_sql() {
            let mut req_vec = Vec::with_capacity(sql.len() + 30);
            req_vec.write_u64_le(req_id).map_err(Error::from)?;
            req_vec.write_u64_le(0).map_err(Error::from)?; //ResultID, uesless here
            req_vec.write_u64_le(6).map_err(Error::from)?; //ActionID, 6 for query
            req_vec.write_u16_le(1).map_err(Error::from)?; //Version
            req_vec.write_u32_le(sql.len().try_into().unwrap()).map_err(Error::from)?; //SQL length
            req_vec.write_all(sql.as_bytes()).map_err(Error::from)?;

            req = self.sender.send_recv(WsSend::Binary(req_vec)).await?;            
        } else{
            let action = WsSend::Query {
                req_id,
                sql: sql.to_string(),
            };
            req = self.sender.send_recv(action).await?;            
        }

        let resp = match req {
            WsRecvData::Query(resp) => resp,
            _ => unreachable!(),
        };

        let result_id = resp.id;
        //  for drop task.
        let (closer, rx) = oneshot::channel();
        tokio::task::spawn(async move {
            let t = Instant::now();
            let _ = rx.await;
            log::trace!("result {result_id} lives {:?}", t.elapsed());
        });

        if resp.fields_count > 0 {
            let names = resp.fields_names.unwrap();
            let types = resp.fields_types.unwrap();
            let bytes = resp.fields_lengths.unwrap();
            let fields: Vec<_> = names
                .into_iter()
                .zip(types)
                .zip(bytes)
                .map(|((name, ty), bytes)| Field::new(name, ty, bytes))
                .collect();
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
                completed: false,
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

    pub fn version(&self) -> &str {
        &self.sender.version.version
    }

    pub fn is_support_binary_sql(&self) -> bool {
        self.sender.version.is_support_binary_sql
    }
}

impl ResultSet {
    async fn fetch(&mut self) -> RawResult<Option<RawBlock>> {
        if self.sender.version.is_support_binary_sql {
            self.fetch_new().await
        } else {
            self.fetch_old().await
        }        
    }
    async fn fetch_new(&mut self) -> RawResult<Option<RawBlock>> {

        let mut req_vec = Vec::with_capacity(26);
        req_vec.write_u64_le(self.sender.req_id()).map_err(Error::from)?;
        req_vec.write_u64_le(self.args.id).map_err(Error::from)?; //ResultID
        req_vec.write_u64_le(7).map_err(Error::from)?; //ActionID, 7 for fetch
        req_vec.write_u16_le(1).map_err(Error::from)?; //Version
      
        match self.sender.send_recv(WsSend::Binary(req_vec)).await? {
            WsRecvData::BlockNew { block_code, block_message, timing, finished, raw, .. } => {
                if block_code != 0 {
                    return Err(RawError::new(block_code, block_message))
                }
        
                if finished {
                    self.timing = timing;
                    self.completed = true;
                    return Ok(None);
                }        
                let mut raw = RawBlock::parse_from_raw_block(raw, self.precision);        
                raw.with_field_names(self.fields.as_ref().unwrap().iter().map(Field::name));
                Ok(Some(raw))
            }           
            _ => unreachable!(),
        }
    }

    async fn fetch_old(&mut self) -> RawResult<Option<RawBlock>> {
        let args = WsResArgs {
            req_id: self.sender.req_id(),
            id: self.args.id,
        };
        let fetch = WsSend::Fetch(args);
        let fetch = self.sender.send_recv(fetch).await?;

        let fetch_resp = match fetch {
            WsRecvData::Fetch(fetch) => fetch,
            data => panic!("unexpected result {data:?}"),
        };

        if fetch_resp.completed {
            self.timing = fetch_resp.timing;
            self.completed = true;
            return Ok(None);
        }

        let args = WsResArgs {
            req_id: self.sender.req_id(),
            id: self.args.id,
        };

        let fetch_block = WsSend::FetchBlock(args);

        match self.sender.send_recv(fetch_block).await? {
            WsRecvData::Block { timing, raw } => {
                let mut raw = RawBlock::parse_from_raw_block(raw, self.precision);

                raw.with_field_names(self.fields.as_ref().unwrap().iter().map(Field::name));
                self.timing = timing + fetch_resp.timing;
                Ok(Some(raw))
            }
            WsRecvData::BlockV2 { timing, raw } => {
                let mut raw = RawBlock::parse_from_raw_block_v2(
                    raw,
                    self.fields.as_ref().unwrap(),
                    fetch_resp.lengths.as_ref().unwrap(),
                    fetch_resp.rows,
                    self.precision,
                );

                raw.with_field_names(self.fields.as_ref().unwrap().iter().map(Field::name));
                self.timing = timing + fetch_resp.timing;
                Ok(Some(raw))
            }
            _ => unreachable!(),
        }
    }

    pub fn take_timing(&self) -> Duration {
        self.timing
    }

    pub async fn stop(&self) {
        if let Some((_, req_id)) = self.sender.results.remove(&self.args.id) {
            self.sender.queries.remove(&req_id);
        }

        let _ = self.sender.send_only(WsSend::FreeResult(self.args)).await;
    }

    fn free_result(&self) {
        let _ = self.sender.send_blocking(WsSend::FreeResult(self.args));
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
        self.fields.as_ref().unwrap()
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

    async fn query<T: AsRef<str> + Send + Sync>(&self, sql: T) -> RawResult<Self::AsyncResultSet> {
        self.s_query(sql.as_ref()).await
    }

    async fn query_with_req_id<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
        req_id: u64,
    ) -> RawResult<Self::AsyncResultSet> {
        self.s_query_with_req_id(sql.as_ref(), req_id).await
    }

    async fn write_raw_meta(&self, raw: &RawMeta) -> RawResult<()> {
        self.write_meta(raw).await
    }

    async fn write_raw_block(&self, block: &RawBlock) -> RawResult<()> {
        self.s_write_raw_block(block).await
    }

    async fn write_raw_block_with_req_id(&self, block: &RawBlock, req_id: u64) -> RawResult<()> {
        self.s_write_raw_block_with_req_id(block, req_id).await
    }

    async fn put(&self, _data: &SmlData) -> RawResult<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::*;

    #[test]
    fn test_is_support_binary_sql() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");

        let version_a: &str    = "3.3.0.0";
        let version_b: &str    = "3.3.1.0";
        let version_c: &str = "2.6.0";
        
        assert_eq!(is_support_binary_sql(version_a), false);
        assert_eq!(is_support_binary_sql(version_b), true);
        assert_eq!(is_support_binary_sql(version_c), false);
        
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
