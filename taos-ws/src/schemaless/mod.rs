use taos_query::common::SmlData;

pub(crate) mod infra;
// pub mod sync;

use crate::TaosBuilder;

use derive_more::Deref;
use futures::stream::SplitStream;
use futures::{SinkExt, StreamExt};
// use scc::HashMap;
use dashmap::DashMap as HashMap;

use taos_query::prelude::{Code, RawError};

use taos_query::prelude::tokio;
use tokio::net::TcpStream;
use tokio::sync::watch;

use tokio::time;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{
    connect_async_with_config, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

use infra::*;

use std::fmt::Debug;

use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use crate::query::Error;

type WsSender = tokio::sync::mpsc::Sender<Message>;

use futures::channel::oneshot;
use oneshot::channel as query_channel;
type QueryChannelSender = oneshot::Sender<StdResult<WsRecvData, RawError>>;
type QueryInner = HashMap<ReqId, QueryChannelSender>;
type QueryAgent = Arc<QueryInner>;
type QueryResMapper = HashMap<ResId, ReqId>;

#[derive(Debug, Clone, Deref)]
struct Version(String);

#[derive(Debug, Clone)]
struct WsQuerySender {
    sender: WsSender,
    queries: QueryAgent,
}

impl WsQuerySender {
    async fn send_recv(&self, msg: WsSend) -> Result<WsRecvData> {
        let send_timeout = Duration::from_millis(1000);
        let req_id = msg.req_id();
        let (tx, rx) = query_channel();

        self.queries.insert(req_id, tx);

        match msg {
            _ => {
                log::trace!("[req id: {req_id}] prepare message: {msg:?}");
                self.sender.send_timeout(msg.to_msg(), send_timeout).await?;
            }
        }
        // handle the error
        log::trace!("[req id: {req_id}] message sent, wait for receiving");
        Ok(rx.await.unwrap()?)
    }
}

#[derive(Debug)]
pub(crate) struct WsTaos {
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

type Result<T> = std::result::Result<T, Error>;

async fn read_queries(
    mut reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    queries_sender: QueryAgent,
    fetches_sender: Arc<QueryResMapper>,
    ws2: WsSender,
    is_v3: bool,
    mut close_listener: watch::Receiver<bool>,
) {
    'ws: loop {
        tokio::select! {
            Some(message) = reader.next() => {
                match message {
                    Ok(message) => match message {
                        Message::Text(text) => {
                            log::trace!("received raw response: {text}");
                            let v: WsRecv = serde_json::from_str(&text).unwrap();
                            log::trace!("raw response deserialize: {v:?}");
                            let (req_id, data, ok) = v.ok();
                            match &data {

                                WsRecvData::Insert(_) => {
                                    if let Some((_, sender)) = queries_sender.remove(&req_id)
                                    {
                                        sender.send(ok.map(|_| data)).unwrap();
                                    } else {
                                        debug_assert!(!queries_sender.contains_key(&req_id));
                                        log::warn!("req_id {req_id} not detected, message might be lost");
                                    }
                                }

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
                            if let Some(close) = close {
                                log::warn!("websocket received close frame: {close:?}");

                                let mut keys = Vec::new();
                                for e in queries_sender.iter() {
                                    keys.push(*e.key());
                                }
                                // queries_sender.for_each_async(|k, _| {
                                //     keys.push(*k);
                                // }).await;
                                for k in keys {
                                    if let Some((_, sender)) = queries_sender.remove(&k) {
                                        let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), close.reason.to_string())));
                                    }
                                }
                            } else {
                                log::warn!("websocket connection is closed normally");
                                let mut keys = Vec::new();
                                for e in queries_sender.iter() {
                                    keys.push(*e.key());
                                }
                                // queries_sender.for_each_async(|k, _| {
                                //     keys.push(*k);
                                // }).await;
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
                            log::warn!("received (unexpected) pong message, do nothing");
                        }
                        Message::Frame(frame) => {
                            // do nothing
                            log::warn!("received (unexpected) frame message, do nothing");
                            log::trace!("* frame data: {frame:?}");
                        }
                    },
                    Err(err) => {
                        log::error!("reading websocket error: {}", err);
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

    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> Result<Self> {
        let mut config = WebSocketConfig::default();
        config.max_frame_size = Some(1024 * 1024 * 16);

        let (ws, _) = connect_async_with_config(info.to_schemaless_url(), Some(config))
            .await
            .map_err(|err| {
                let err_string = err.to_string();
                if err_string.contains("401 Unauthorized") {
                    Error::Unauthorized(info.to_schemaless_url())
                } else {
                    err.into()
                }
            })?;
        let req_id = 0;
        let (mut sender, mut reader) = ws.split();

        let version = WsSend::Version;
        sender.send(version.to_msg()).await?;

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
        let is_v3 = !version.starts_with("2");

        // use unsafe to transmute
        let login = WsSend::Conn {
            req_id,
            req: unsafe { std::mem::transmute(info.to_conn_request()) },
        };
        log::trace!("login send: {:?}", login);
        sender.send(login.to_msg()).await?;
        if let Some(Ok(message)) = reader.next().await {
            match message {
                Message::Text(text) => {
                    log::trace!("login resp: {}", text);
                    let v: WsRecv = serde_json::from_str(&text).unwrap();
                    let (_req_id, data, ok) = v.ok();
                    match data {
                        WsRecvData::Conn => ok?,
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        }

        let queries2 = Arc::new(QueryInner::new());

        let fetches_sender = Arc::new(QueryResMapper::new());

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
                                log::error!("send websocket message packet error: {}", err);
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
                sender: ws_cloned,
                queries: queries2_cloned,
            },
        })
    }

    pub async fn s_put(&self, sml: &SmlData) -> Result<()> {
        let action = WsSend::Insert {
            protocol: sml.protocol() as u8,
            precision: sml.precision().into(),
            data: sml.data().join("\n").to_string(),
            ttl: sml.ttl(),
            req_id: sml.req_id(),
        };
        log::trace!("put send: {:?}", action);
        let req = self.sender.send_recv(action).await?;

        match req {
            WsRecvData::Insert(res) => {
                log::trace!("put resp : {:?}", res);
                Ok(())
            }
            _ => {
                unreachable!()
            }
        }
    }
}
