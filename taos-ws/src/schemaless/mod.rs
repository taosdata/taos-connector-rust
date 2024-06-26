use futures::stream::SplitStream;
use taos_query::common::SmlData;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub(crate) mod infra;
// pub mod sync;

use crate::TaosBuilder;

use futures::{SinkExt, StreamExt, TryStreamExt};
// use scc::HashMap;
use dashmap::DashMap as HashMap;

use taos_query::prelude::{Code, RawError};

use taos_query::prelude::RawResult;
use tokio::sync::watch;

use tokio::time;
use tokio_tungstenite::tungstenite::protocol::Message;

use infra::*;

use std::fmt::Debug;

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use crate::query::Error;

type WsSender = tokio::sync::mpsc::Sender<Message>;

use futures::channel::oneshot;
use oneshot::channel as query_channel;
type QueryChannelSender = oneshot::Sender<RawResult<WsRecvData>>;
type QueryInner = HashMap<ReqId, QueryChannelSender>;
type QueryAgent = Arc<QueryInner>;
type QueryResMapper = HashMap<ResId, ReqId>;

type WebSocketStreamReader = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

#[derive(Debug, Clone)]
struct WsQuerySender {
    sender: WsSender,
    queries: QueryAgent,
}

impl WsQuerySender {
    async fn send_recv(&self, msg: WsSend) -> RawResult<WsRecvData> {
        let send_timeout = Duration::from_millis(1000);
        let req_id = msg.req_id();
        let (tx, rx) = query_channel();

        self.queries.insert(req_id, tx);

        {
            log::trace!("[req id: {req_id}] prepare message: {msg:?}");
            self.sender
                .send_timeout(msg.to_msg(), send_timeout)
                .await
                .map_err(Error::from)?;
        }
        // handle the error
        log::trace!("[req id: {req_id}] message sent, wait for receiving");
        rx.await.unwrap()
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
                log::trace!("received json response: {text}",);
                let v: WsRecv = sonic_rs::from_str(&text).unwrap();
                let queries_sender = queries_sender.clone();
                tokio::task::spawn_blocking(move || {
                    let (req_id, data, ok) = v.ok();
                    match &data {
                        WsRecvData::Insert(_) => {
                            if let Some((_, sender)) = queries_sender.remove(&req_id) {
                                sender.send(ok.map(|_| data)).unwrap();
                            } else {
                                debug_assert!(!queries_sender.contains_key(&req_id));
                                log::warn!("req_id {req_id} not detected, message might be lost");
                            }
                        }
                        _ => unreachable!(),
                    }
                });
            }
            Message::Binary(payload) => {
                let block = payload;
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
                if let Some((_, req_id)) = fetches_sender.remove(&res_id) {
                    if is_v3 {
                        // v3
                        if let Some((_, sender)) = queries_sender.remove(&req_id) {
                            log::trace!("send data to fetches with id {}", res_id);
                            sender
                                .send(Ok(WsRecvData::Block {
                                    timing,
                                    raw: block[offset..].to_vec(),
                                }))
                                .unwrap();
                        } else {
                            log::warn!("req_id {res_id} not detected, message might be lost");
                        }
                    } else {
                        // v2
                        if let Some((_, sender)) = queries_sender.remove(&req_id) {
                            log::trace!("send data to fetches with id {}", res_id);
                            sender
                                .send(Ok(WsRecvData::BlockV2 {
                                    timing,
                                    raw: block[offset..].to_vec(),
                                }))
                                .unwrap();
                        } else {
                            log::warn!("req_id {res_id} not detected, message might be lost");
                        }
                    }
                } else {
                    log::warn!("result id {res_id} not found");
                }
            }
            Message::Close(_) => {
                // taosAdapter should never send close frame to client.
                // So all close frames should be treated as error.

                log::warn!("websocket connection is closed normally");
                let mut keys = Vec::new();
                for e in queries_sender.iter() {
                    keys.push(*e.key());
                }
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
                    let _ = ws2.send(Message::Pong(data)).await;
                });
            }
            Message::Pong(_) => {
                // do nothing
                log::trace!("received pong message, do nothing");
            }
            _ => {
                // do nothing
                log::warn!("received (unexpected) frame message, do nothing");
                log::trace!("* frame data: {frame:?}");
            }
        }

        return ControlFlow::Continue(());
    };
    'ws: loop {
        tokio::select! {
            Ok(frame) = reader.try_next() => {
                if let Some(frame) = frame {
                match parse_frame(frame) {
                    ControlFlow::Break(()) => {
                        break;
                    }
                    _ => {}
                }}
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

    #[allow(dead_code)]
    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {
        let ws = info.build_stream(info.to_schemaless_url()).await?;

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

        // use unsafe to transmute
        let login = WsSend::Conn {
            req_id,
            req: unsafe { std::mem::transmute(info.to_conn_request()) },
        };
        log::trace!("login send: {:?}", login);
        sender.send(login.to_msg()).await.map_err(Error::from)?;
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
        Ok(Self {
            close_signal: tx,
            sender: WsQuerySender {
                sender: ws,
                queries: queries2_cloned,
            },
        })
    }

    pub async fn s_put(&self, sml: &SmlData) -> RawResult<()> {
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
