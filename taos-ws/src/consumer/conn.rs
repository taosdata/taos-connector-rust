use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use itertools::Itertools;
use taos_query::prelude::{Code, RawError};

use taos_query::util::generate_req_id;
use taos_query::RawResult;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::time;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::Instrument;

use crate::consumer::messages::{TmqInit, TmqRecv, TmqRecvData, TmqSend, WsMessage};
use crate::consumer::{WsTmqError, WsTmqSender};
use crate::query::asyn::{is_support_binary_sql, WS_ERROR_NO};
use crate::query::messages::ToMessage;
use crate::query::WsConnReq;
use crate::{handle_disconnect_error, EndpointType, TaosBuilder};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsStreamReader = SplitStream<WsStream>;
type WsStreamSender = SplitSink<WsStream, Message>;

#[derive(Debug, Clone)]
struct MessageCache {
    message: Arc<Mutex<Option<Message>>>,
}

impl MessageCache {
    fn new() -> Self {
        Self {
            message: Arc::default(),
        }
    }

    async fn insert(&self, message: Message) {
        *self.message.lock().await = Some(message);
    }

    async fn remove(&self) {
        *self.message.lock().await = None;
    }

    async fn message(&self) -> Option<Message> {
        self.message.lock().await.clone()
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    builder: TaosBuilder,
    mut ws_stream: WsStream,
    tmq_sender: WsTmqSender,
    poll_cache_sender: mpsc::Sender<Option<TmqRecvData>>,
    message_reader: flume::Receiver<WsMessage>,
    mut close_reader: watch::Receiver<bool>,
    tmq_conf: TmqInit,
    topics: Arc<RwLock<Vec<String>>>,
    support_fetch_raw: Arc<AtomicBool>,
) {
    let cache = MessageCache::new();

    loop {
        let (ws_stream_tx, ws_stream_rx) = ws_stream.split();
        let (err_tx, mut err_rx) = mpsc::channel(2);
        let (close_tx, close_rx) = watch::channel(false);

        let send_handle = tokio::spawn(
            send_messages(
                ws_stream_tx,
                message_reader.clone(),
                close_rx.clone(),
                err_tx.clone(),
                cache.clone(),
            )
            .in_current_span(),
        );

        let recv_handle = tokio::spawn(
            read_messages(
                ws_stream_rx,
                tmq_sender.clone(),
                poll_cache_sender.clone(),
                close_rx,
                err_tx,
                cache.clone(),
            )
            .in_current_span(),
        );

        tokio::select! {
            err = err_rx.recv() => {
                if let Some(err) = err {
                    tracing::error!("WebSocket error: {err}");
                    let _ = close_tx.send(true);
                    if !is_disconnect_error(&err) {
                        tracing::error!("non-disconnect error detected, cleaning up all pending queries");
                        // cleanup_after_disconnect(agent.clone());
                        return;
                    }
                    tracing::warn!("disconnect error detected, attempting to reconnect");
                }
            }
            _ = close_reader.changed() => {
                tracing::info!("WebSocket received close signal");
                let _ = close_tx.send(true);
                return;
            }
        }

        if let Err(err) = send_handle.await {
            tracing::error!("send messages task failed: {err:?}");
        }
        if let Err(err) = recv_handle.await {
            tracing::error!("read messages task failed: {err:?}");
        }

        tracing::warn!("WebSocket disconnected, starting to reconnect");

        let res = if cache.message().await.is_some() {
            let cb = send_subscribe_request(
                builder.build_conn_request(),
                tmq_conf.clone(),
                topics.clone(),
            );
            builder.connect_with_cb(EndpointType::Tmq, cb).await
        } else {
            builder.connect_with_ty(EndpointType::Tmq).await
        };

        match res {
            Ok((ws, ver)) => {
                ws_stream = ws;
                support_fetch_raw.store(is_fetch_raw_supported(&ver), Ordering::Relaxed);
            }
            Err(err) => {
                tracing::error!("WebSocket reconnection failed: {err}");
                // cleanup_after_disconnect(agent.clone());
                return;
            }
        }

        tracing::info!("WebSocket reconnected successfully");

        // cleanup_after_reconnect(agent.clone(), cache.clone());
    }
}

async fn send_messages(
    mut ws_stream_sender: WsStreamSender,
    message_reader: flume::Receiver<WsMessage>,
    mut close_reader: watch::Receiver<bool>,
    err_sender: mpsc::Sender<WsTmqError>,
    cache: MessageCache,
) {
    tracing::trace!("start sending messages to WebSocket stream");

    let mut interval = time::interval(Duration::from_secs(29));

    if let Some(message) = cache.message().await {
        if let Err(err) = ws_stream_sender.send(message).await {
            tracing::error!("failed to send poll message: {err}");
            let _ = err_sender.send(err.into()).await;
        }
    }

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(err) = send_ping_message(&mut ws_stream_sender).await {
                    tracing::error!("failed to send WebSocket ping message: {err}");
                    let _ = err_sender.send(err).await;
                    break;
                }

                // tracing::trace!("Check websocket message sender alive");
                // if let Err(err) = ws_stream_sender.send(Message::Ping(PING.to_vec())).await {
                //     tracing::trace!("sending ping message to {sending_url} error: {err:?}");
                //     let keys = agent.iter().map(|r| *r.key()).collect_vec();
                //     for k in keys {
                //         if let Some((_, sender)) = agent.remove(&k) {
                //             let _ = sender.send(Err(RawError::new(
                //                 WS_ERROR_NO::CONN_CLOSED.as_code(),
                //                 format!("WebSocket internal error: {err}"),
                //             ))).await;
                //         }
                //     }
                // }
            }
            message = message_reader.recv_async() => {
                match message {
                    Ok(message) => {
                        let should_cache = message.should_cache();
                        let message = message.into_message();
                        if should_cache {
                            cache.insert(message.clone()).await;
                        }
                        if let Err(err) = ws_stream_sender.send(message).await {
                            tracing::error!("WebSocket sender error: {err:?}");
                            let _ = err_sender.send(err.into()).await;
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::error!("failed to receive message from channel: {err}");
                        let _ = err_sender.send(WsTmqError::ChannelClosedError).await;
                        break;
                    }
                }

                // if message.is_close() {
                //     let _ = ws_stream_sender.send(message).await;
                //     let _ = ws_stream_sender.close().await;
                //     break;
                // }
                // tracing::trace!("send message {message:?}");
                // if let Err(err) = ws_stream_sender.send(message).await {
                //     tracing::trace!("sending message to {sending_url} error: {err:?}");
                //     let keys = agent.iter().map(|r| *r.key()).collect_vec();
                //     for k in keys {
                //         if let Some((_, sender)) = agent.remove(&k) {
                //             let _ = sender.send(Err(RawError::new(
                //                 WS_ERROR_NO::CONN_CLOSED.as_code(),
                //                 format!("WebSocket internal error: {err}",
                //             )))).await;
                //         }
                //     }
                // }
                // tracing::trace!("send message done");
            }
            _ = close_reader.changed() => {
                let _= ws_stream_sender.send(Message::Close(None)).await;
                let _ = ws_stream_sender.close().await;
                tracing::trace!("close tmq sender");
                break;
            }
        }
    }
}

async fn send_ping_message(ws_stream_sender: &mut WsStreamSender) -> Result<(), WsTmqError> {
    ws_stream_sender
        .send(Message::Ping(b"TAOS".to_vec()))
        .await
        .map_err(Into::<WsTmqError>::into)?;

    Ok(())
}

async fn read_messages(
    mut ws_stream_reader: WsStreamReader,
    tmq_sender: WsTmqSender,
    poll_cache_sender: mpsc::Sender<Option<TmqRecvData>>,
    mut close_reader: watch::Receiver<bool>,
    _err_sender: mpsc::Sender<WsTmqError>,
    cache: MessageCache,
) {
    let instant = Instant::now();
    let agent = tmq_sender.queries.clone();

    'ws: loop {
        tokio::select! {
            Some(message) = ws_stream_reader.next() => {
                match message {
                    Ok(message) => match message {
                        Message::Text(text) => {
                            tracing::trace!("json response: {}", text);
                            let v: TmqRecv = serde_json::from_str(&text).expect(&text);
                            let (req_id, recv, ok) = v.ok();
                            match &recv {
                                TmqRecvData::Subscribe => {
                                    tracing::trace!("subscribe with: {:?}", req_id);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        // We don't care about the result of the sender for subscribe
                                        let _ = sender.send(ok.map(|_|recv)).await;
                                    } else {
                                        tracing::warn!("subscribe message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::Unsubscribe => {
                                    tracing::trace!("unsubscribe with: {:?} success", req_id);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        // We don't care about the result of the sender for unsubscribe
                                        let _ = sender.send(ok.map(|_|recv)).await;
                                    } else {
                                        tracing::warn!("unsubscribe message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::Poll(_) => {
                                    cache.remove().await;
                                    let data = match agent.remove(&req_id) {
                                        Some((_, sender)) => {
                                            #[cfg(test)]
                                            #[allow(static_mut_refs)]
                                            {
                                                if std::env::var("TEST_POLLING_LOST").is_ok() {
                                                    static mut POLLING_LOST_IDX: u64 = 0;
                                                    unsafe {
                                                        POLLING_LOST_IDX += 1;
                                                        tokio::time::sleep(Duration::from_millis(500)).await;
                                                        tracing::warn!(lost = POLLING_LOST_IDX, "polling lost");
                                                    }
                                                }
                                            }

                                            match sender.send(ok.map(|_| recv)).await {
                                                Ok(_) => None,
                                                Err(err) => match err.0 {
                                                    Ok(data) => Some(data),
                                                    Err(err) => {
                                                        tracing::warn!("poll message received, err: {err:?}");
                                                        None
                                                    }
                                                },
                                            }
                                        },
                                        None => Some(recv),
                                    };

                                    tracing::trace!("poll end: {data:?}");
                                    if let Err(err) = poll_cache_sender.send(data).await {
                                        tracing::error!("poll end notification failed, break the connection, err: {err:?}");
                                        let keys = agent.iter().map(|r| *r.key()).collect_vec();
                                        for key in keys {
                                            if let Some((_, sender)) = agent.remove(&key) {
                                                let _ = sender
                                                    .send(Err(RawError::new(
                                                        WS_ERROR_NO::CONN_CLOSED.as_code(),
                                                        "Consumer messages lost",
                                                    )))
                                                    .await;
                                            }
                                        }
                                        break 'ws;
                                    }
                                }
                                TmqRecvData::FetchJsonMeta { data } => {
                                    tracing::trace!("fetch json meta data: {:?}", data);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        if let Err(err) = sender.send(ok.map(|_|recv)).await {
                                            tracing::warn!(req_id, kind = "fetch_json_meta", "fetch json meta message received but no receiver alive: {:?}", err);
                                        }
                                    } else {
                                        tracing::warn!("fetch json meta message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::FetchRaw { .. } => {
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        if let Err(err) = sender.send(ok.map(|_|recv)).await {
                                            tracing::warn!(req_id, kind = "fetch_raw_meta", "fetch raw meta message received but no receiver alive: {:?}", err);
                                        }
                                    } else {
                                        tracing::warn!("fetch raw message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::Commit => {
                                    tracing::trace!("commit done: {:?}", recv);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        // We don't care about the result of the sender for commit
                                        let _ = sender.send(ok.map(|_|recv)).await;
                                    } else {
                                        tracing::warn!("commit message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::Fetch(fetch) => {
                                    tracing::trace!("fetch done: {:?}", fetch);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        // We don't care about the result of the sender for fetch
                                        let _ = sender.send(ok.map(|_|recv)).await;
                                    } else {
                                        tracing::warn!("fetch message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::FetchBlock{ .. } => {
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        let _ = sender.send(Err(RawError::new(
                                            WS_ERROR_NO::WEBSOCKET_ERROR.as_code(),
                                            format!("WebSocket internal error: {:?}", &text)
                                        ))).await;
                                    }
                                    break 'ws;
                                }
                                TmqRecvData::Assignment(assignment) => {
                                    tracing::trace!("assignment done: {:?}", assignment);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        let _ = sender.send(ok.map(|_|recv)).await;
                                    } else {
                                        tracing::warn!("assignment message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::Seek { timing } => {
                                    tracing::trace!("seek done: req_id {:?} timing {:?}", &req_id, timing);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        let _ = sender.send(ok.map(|_|recv)).await;
                                    } else {
                                        tracing::warn!("seek message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::Committed { committed } => {
                                    tracing::trace!("committed done: {:?}", committed);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        let _ = sender.send(ok.map(|_|recv)).await;
                                    } else {
                                        tracing::warn!("committed message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::Position { position } => {
                                    tracing::trace!("position done: {:?}", position);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        let _ = sender.send(ok.map(|_|recv)).await;
                                    } else {
                                        tracing::warn!("position message received but no receiver alive");
                                    }
                                }
                                TmqRecvData::CommitOffset { timing } => {
                                    tracing::trace!("commit offset done: {:?}", timing);
                                    if let Some((_, sender)) = agent.remove(&req_id) {
                                        let _ = sender.send(ok.map(|_|recv)).await;
                                    } else {
                                        tracing::warn!("commit offset message received but no receiver alive");
                                    }
                                }
                                _ => unreachable!("unknown tmq response"),
                            }
                        }
                        Message::Binary(data) => {
                            let block = data;
                            let mut slice = block.as_slice();
                            use taos_query::util::InlinableRead;

                            let timing = slice.read_u64().unwrap();
                            let part: Vec<u8>;
                            if timing != u64::MAX{
                                let offset = 16;
                                part = slice[offset..].to_vec();
                            } else {
                                // new version
                                let offset = 26;
                                part = slice[offset..].to_vec();
                                let _action = slice.read_u64().unwrap();
                                let _version = slice.read_u16().unwrap();
                                let _time = slice.read_u64().unwrap();
                            }
                            let req_id = slice.read_u64().unwrap();

                            if let Some((_, sender)) = agent.remove(&req_id) {
                                tracing::trace!("send data to fetches with id {}", req_id);
                                if sender.send(Ok(TmqRecvData::Bytes(part.into()))).await.is_err() {
                                    tracing::warn!(req_id, kind = "binary", "req_id {req_id} not detected, message might be lost");
                                }
                            } else {
                                tracing::warn!("req_id {req_id} not detected, message might be lost");
                            }
                        }
                        Message::Close(close) => {
                            tracing::warn!("websocket connection is closed (unexpected?)");

                            let keys = agent.iter().map(|r| *r.key()).collect_vec();
                            let err = if let Some(close) = close {
                                format!("WebSocket internal error: {close}")
                            } else {
                                "WebSocket internal error, connection is reset by server".to_string()
                            };
                            for k in keys {
                                if let Some((_, sender)) = agent.remove(&k) {
                                    let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), err.clone()))).await;
                                }
                            }
                            break 'ws;
                        }
                        Message::Ping(bytes) => {
                            let _ = tmq_sender.sender.send_async(WsMessage::Raw(Message::Pong(bytes))).await;
                        }
                        Message::Pong(_bytes) => {
                            // if bytes == PING {
                            tracing::trace!("ping/pong handshake success");
                            // } else {
                            //     tracing::warn!("received (unexpected) pong message, do nothing");
                            // }
                        }
                        Message::Frame(frame) => {
                            tracing::warn!("received (unexpected) frame message, do nothing");
                            tracing::trace!("* frame data: {frame:?}");
                        }
                    },
                    Err(err) => {
                        let keys = agent.iter().map(|r| *r.key()).collect_vec();
                        for k in keys {
                            if let Some((_, sender)) = agent.remove(&k) {
                                let _ = sender.send(Err(RawError::new(
                                    WS_ERROR_NO::CONN_CLOSED.as_code(),
                                    format!("WebSocket internal error: {err}")
                                ))).await;
                            }
                        }
                        break 'ws;
                    }
                }
            }
            _ = close_reader.changed() => {
                tracing::trace!("close reader task");
                break 'ws;
            }
        }
    }
    tracing::trace!("Consuming done in {:?}", instant.elapsed());
}

fn send_subscribe_request(
    conn_req: WsConnReq,
    tmq_conf: TmqInit,
    topics: Arc<RwLock<Vec<String>>>,
) -> impl for<'a> Fn(&'a mut WsStream) -> Pin<Box<dyn Future<Output = RawResult<()>> + Send + 'a>> {
    move |ws_stream| {
        let conn_req = conn_req.clone();
        let tmq_conf = tmq_conf.clone();
        let topics = topics.clone();

        Box::pin(async move {
            let topics = topics.read().await.clone();
            let req = TmqSend::Subscribe {
                req_id: generate_req_id(),
                req: tmq_conf.clone().disable_auto_commit(),
                topics: topics.clone(),
                conn: conn_req.clone(),
            };

            let resp = send_recv(ws_stream, req.to_msg()).await?;
            let (_, data, ok) = resp.ok();

            if let TmqRecvData::Subscribe = data {
                if let Err(err) = ok {
                    tracing::error!("subscribe error: {err:?}");
                    if tmq_conf.enable_batch_meta.is_none() {
                        return Err(err);
                    }

                    let code: i32 = err.code().into();
                    if code & 0xFFFF == 0xFFFE {
                        let req = TmqSend::Subscribe {
                            req_id: generate_req_id(),
                            req: tmq_conf.disable_batch_meta().disable_auto_commit(),
                            topics,
                            conn: conn_req,
                        };
                        let resp = send_recv(ws_stream, req.to_msg()).await?;
                        let (_, _, ok) = resp.ok();
                        ok?;
                    } else {
                        return Err(err);
                    }
                }
            }

            Ok(())
        })
    }
}

async fn send_recv(ws_stream: &mut WsStream, message: Message) -> RawResult<TmqRecv> {
    let timeout = Duration::from_secs(8);

    time::timeout(timeout, ws_stream.send(message))
        .await
        .map_err(|_| {
            RawError::from_code(WS_ERROR_NO::SEND_MESSAGE_TIMEOUT.as_code())
                .context("timeout sending subscribe request")
        })?
        .map_err(handle_disconnect_error)?;

    loop {
        let res = time::timeout(timeout, ws_stream.next())
            .await
            .map_err(|_| {
                RawError::from_code(WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code())
                    .context("timeout waiting for subscribe response")
            })?;

        let Some(res) = res else {
            return Err(RawError::from_code(Code::WS_DISCONNECTED));
        };

        let message = res.map_err(handle_disconnect_error)?;
        tracing::trace!("send_subscribe_request, received message: {message}");

        match message {
            Message::Text(text) => {
                if text.contains("version") {
                    continue;
                }

                return serde_json::from_str(&text).map_err(|err| {
                    RawError::any(err)
                        .with_code(WS_ERROR_NO::DE_ERROR.as_code())
                        .context("invalid json response")
                });
            }
            Message::Ping(bytes) => {
                ws_stream
                    .send(Message::Pong(bytes))
                    .await
                    .map_err(handle_disconnect_error)?;
            }
            _ => {
                return Err(RawError::from_string(format!(
                    "unexpected message during subscribe: {message:?}"
                )))
            }
        }
    }
}

#[inline]
pub(super) fn is_fetch_raw_supported(version: &str) -> bool {
    !version.starts_with('2') && is_support_binary_sql(version)
}

fn is_disconnect_error(err: &WsTmqError) -> bool {
    match err {
        WsTmqError::WsError(err) => matches!(
            err,
            WsError::ConnectionClosed
                | WsError::AlreadyClosed
                | WsError::Io(_)
                | WsError::Tls(_)
                | WsError::Protocol(_)
        ),
        _ => false,
    }
}
