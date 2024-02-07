use taos_query::common::SmlData;

pub(crate) mod infra;
// pub mod sync;

use crate::TaosBuilder;

use derive_more::Deref;
use futures::{SinkExt, StreamExt};
// use scc::HashMap;
use dashmap::DashMap as HashMap;

use taos_query::prelude::{Code, RawError};

use taos_query::prelude::{tokio, RawResult};
use tokio::sync::watch;

use tokio::io::BufStream;
use tokio::io::ReadHalf;
use tokio::time;
use tokio_tungstenite::tungstenite::protocol::Message;

use ws_tool::{codec::AsyncDeflateRecv, frame::OpCode, stream::AsyncStream, Message as WsMessage};

use infra::*;

use std::fmt::Debug;

use std::sync::Arc;
use std::time::Duration;

use crate::query::Error;

type WsSender = tokio::sync::mpsc::Sender<WsMessage<bytes::Bytes>>;

use futures::channel::oneshot;
use oneshot::channel as query_channel;
type QueryChannelSender = oneshot::Sender<RawResult<WsRecvData>>;
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
    mut reader: AsyncDeflateRecv<ReadHalf<BufStream<AsyncStream>>>,
    queries_sender: QueryAgent,
    fetches_sender: Arc<QueryResMapper>,
    ws2: WsSender,
    is_v3: bool,
    mut close_listener: watch::Receiver<bool>,
) {
    'ws: loop {
        tokio::select! {
            Ok(frame) = reader.receive() => {
                let (header, payload) = frame;
                let code = header.code;
                match code {
                    OpCode::Text => {
                        log::trace!("received raw response: {payload}", payload = String::from_utf8_lossy(&payload));
                        let v: WsRecv = serde_json::from_slice(&payload).unwrap();
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
                    OpCode::Binary => {
                        let block = payload.to_vec();
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
                        log::warn!("received (unexpected) pong message, do nothing");
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

impl WsTaos {
    /// Build TDengine websocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041/
    /// ```
    ///

    #[allow(dead_code)]
    pub(crate) async fn tung_from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {
        let ws = info.build_stream(info.to_schemaless_url()).await?;

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

        // use unsafe to transmute
        let login = WsSend::Conn {
            req_id,
            req: unsafe { std::mem::transmute(info.to_conn_request()) },
        };
        log::trace!("login send: {:?}", login);
        sender
            .send(login.to_tungstenite_msg())
            .await
            .map_err(Error::from)?;
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

        let _fetches_sender = Arc::new(QueryResMapper::new());

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
            // read_queries(reader, queries2, fetches_sender, ws2, is_v3, close_listener).await
        });
        let _ws_cloned = ws.clone();

        let (ws_integration, mut _msg_recv_integration) = tokio::sync::mpsc::channel(100);
        let ws2_integration: tokio::sync::mpsc::Sender<WsMessage<bytes::Bytes>> =
            ws_integration.clone();

        Ok(Self {
            close_signal: tx,
            sender: WsQuerySender {
                sender: ws2_integration,
                queries: queries2_cloned,
            },
        })
    }

    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {
        let ws = info.ws_tool_build_stream(info.to_schemaless_url()).await?;

        let req_id = 0;
        let (mut sink, mut source) = ws.split();

        let version = WsSend::Version;
        source
            .send(OpCode::Text, &serde_json::to_vec(&version).unwrap())
            .await
            .map_err(Error::from)?;

        let duration = Duration::from_secs(2);
        let version = match tokio::time::timeout(duration, sink.receive()).await {
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
        let is_v3 = !version.starts_with('2');

        // use unsafe to transmute
        let login = WsSend::Conn {
            req_id,
            req: unsafe { std::mem::transmute(info.to_conn_request()) },
        };
        log::trace!("login send: {:?}", login);
        source
            .send(OpCode::Text, &serde_json::to_vec(&login).unwrap())
            .await
            .map_err(Error::from)?;
        if let Ok(frame) = sink.receive().await {
            let (header, payload) = frame;
            let code = header.code;
            match code {
                OpCode::Text => {
                    log::trace!("login resp: {}", String::from_utf8_lossy(&payload));
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
                        if let Err(err) = source.send(opcode, &msg).await {
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
                        let _ = source.send(OpCode::Close, b"").await;
                        log::trace!("close sender task");
                        break 'ws;
                    }
                }
            }
        });

        tokio::spawn(async move {
            read_queries(sink, queries2, fetches_sender, ws2, is_v3, close_listener).await
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
