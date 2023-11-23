use derive_more::Deref;
use futures::stream::SplitStream;
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
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use taos_query::prelude::tokio;
use tokio::net::TcpStream;
use tokio::sync::watch;

use tokio::time;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{
    tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
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

type WsSender = tokio::sync::mpsc::Sender<Message>;

use futures::channel::oneshot;
use oneshot::channel as query_channel;
type QueryChannelSender = oneshot::Sender<RawResult<WsRecvData>>;
// use tokio::sync::mpsc::unbounded_channel as query_channel;
// type QueryChannelSender = tokio::sync::mpsc::UnboundedSender<Result<WsRecvData, RawError>>;
type QueryInner = HashMap<ReqId, QueryChannelSender>;
type QueryAgent = Arc<QueryInner>;
type QueryResMapper = HashMap<ResId, ReqId>;

#[derive(Debug, Clone, Deref)]
struct Version(String);

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
                    .send_timeout(Message::Binary(bytes), send_timeout)
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
    SendError(#[from] tokio::sync::mpsc::error::SendError<Message>),
    #[error(transparent)]
    SendTimeoutError(#[from] tokio::sync::mpsc::error::SendTimeoutError<Message>),
    #[error("Query timed out with sql: {0}")]
    QueryTimeout(String),
    #[error("{0}")]
    TaosError(#[from] RawError),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("WebSocket internal error: {0}")]
    WsError(#[from] WsError),
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
            Error::WsError(_) => Code::new(WS_ERROR_NO::WEBSOCKET_ERROR as _),
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
    mut reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
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
            if let Err(err) = ws3.send(Message::Ping(b"TAOSX".to_vec())).await {
                log::trace!("sending ping message error: {err:?}");
                break;
            }
        }
    });
    'ws: loop {
        tokio::select! {
            Some(message) = reader.next() => {
                match message {
                    Ok(message) => match message {
                        Message::Text(text) => {
                            log::trace!("received json response: {text}");
                            let v: WsRecv = serde_json::from_str(&text).unwrap();
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
                        Message::Binary(block) => {
                            let mut slice = block.as_slice();
                            use taos_query::util::InlinableRead;
                            let offset = if is_v3 { 16 } else { 8 };

                            let timing = if is_v3 {
                                let timing = slice.read_u64().unwrap();
                                Duration::from_nanos(timing as _)
                            } else {
                                Duration::ZERO
                            };

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
                        Message::Close(close) => {
                            // taosAdapter should never send close frame to client.
                            //   So all close frames should be treated as error.
                            if let Some(close) = close {
                                log::warn!("websocket received close frame: {close:?}");

                                let mut keys = Vec::new();
                                for e in queries_sender.iter() {
                                    keys.push(*e.key());
                                }
                                let reason = match close.code {
                                    CloseCode::Size => {
                                        format!("Message length reaches max limit (code: {})", close.code)
                                    }
                                    _ => format!("{}", close),
                                };
                                for k in keys {
                                    if let Some((_, sender)) = queries_sender.remove(&k) {
                                        let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), reason.to_string())));
                                    }
                                }
                            } else {
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
                            }
                            break 'ws;
                        }
                        Message::Ping(bytes) => {
                            ws2.send(Message::Pong(bytes)).await.unwrap();
                        }
                        Message::Pong(_) => {
                            // do nothing
                            log::trace!("received pong message, do nothing");
                        }
                        Message::Frame(frame) => {
                            // do nothing
                            log::warn!("received (unexpected) frame message, do nothing");
                            log::trace!("* frame data: {frame:?}");
                        }
                    },
                    Err(err) => {
                        let mut keys = Vec::new();
                        for e in queries_sender.iter() {
                                    keys.push(*e.key());
                                }
                        // queries_sender.for_each_async(|k, _| {
                        //     keys.push(*k);
                        // }).await;
                        for k in keys {
                            if let Some((_, sender)) = queries_sender.remove(&k) {
                                let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), err.to_string())));
                            }
                        }
                        break 'ws;
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
    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {

        let ws = info.build_stream(info.to_query_url()).await?;

        let req_id = 0;
        let (mut sender, mut reader) = ws.split();

        let version = WsSend::Version;
        sender.send(version.to_msg()).await.map_err(Error::from)?;

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
        let is_v3 = !version.starts_with('2');

        let login = WsSend::Conn {
            req_id,
            req: info.to_conn_request(),
        };
        sender.send(login.to_msg()).await.map_err(Error::from)?;
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
        let ws2 = ws.clone();

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
            read_queries(reader, queries2, fetches_sender, ws2, is_v3, close_listener).await
        });
        let ws_cloned = ws.clone();

        Ok(Self {
            close_signal: tx,
            sender: WsQuerySender {
                version: Version(version),
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
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };

        let req = self.sender.send_recv(action).await?;

        let resp = match req {
            WsRecvData::Query(resp) => resp,
            _ => unreachable!(),
        };
        log::trace!("resp: {resp:?}");

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
            // log::info!("resp: {resp:?}");
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

    pub async fn s_query_with_req_id(&self, sql: &str, req_id: u64) -> RawResult<ResultSet> {
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };

        let req = self.sender.send_recv(action).await?;

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
        &self.sender.version.0
    }
}

impl ResultSet {
    async fn fetch(&mut self) -> RawResult<Option<RawBlock>> {
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

#[tokio::test]
async fn test_client() -> anyhow::Result<()> {
    use futures::TryStreamExt;
    std::env::set_var("RUST_LOG", "debug");
    let dsn = std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
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

    // let mut rs = client.s_query("select * from abc_a.tb1").unwrap().unwrap();
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
    let _ = pretty_env_logger::try_init_timed();
    let dsn = std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
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
    let dsn = std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
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

    assert_eq!(client.exec("drop database test_ws_write_raw_block_with_req_id").await?, 0);
    Ok(())
}
