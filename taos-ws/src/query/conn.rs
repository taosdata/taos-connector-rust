use std::time::Duration;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use flume::Receiver;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt, TryStreamExt};
use taos_query::prelude::RawError;
use tokio::time;
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
};
use tokio_tungstenite::{
    tungstenite::{Error as WsError, Message},
    MaybeTlsStream, WebSocketStream,
};
use tracing::Instrument;

use crate::query::{
    asyn::{WsQuerySender, WS_ERROR_NO},
    messages::{MessageId, ReqId, WsMessage, WsRecv, WsRecvData},
    Error,
};
use crate::{EndpointType, TaosBuilder};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsStreamReader = SplitStream<WsStream>;
type WsStreamSender = SplitSink<WsStream, Message>;

#[derive(Default, Clone)]
struct MessageCache {
    next_msg_id: Arc<AtomicU64>,
    req_to_msg: Arc<scc::HashMap<ReqId, MessageId>>,
    messages: Arc<scc::HashMap<MessageId, Message>>,
}

impl MessageCache {
    fn new() -> Self {
        MessageCache::default()
    }

    fn insert(&self, req_id: ReqId, message: Message) {
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        tracing::trace!("insert message into cache, req_id: {req_id}, msg_id: {msg_id}");
        let _ = self.req_to_msg.insert(req_id, msg_id);
        let _ = self.messages.insert(msg_id, message);
    }

    fn remove(&self, req_id: &ReqId) {
        tracing::trace!("remove message from cache, req_id: {req_id}");
        if let Some((_, msg_id)) = self.req_to_msg.remove(req_id) {
            self.messages.remove(&msg_id);
        }
    }

    fn messages(&self) -> Vec<Message> {
        if self.messages.is_empty() {
            return Vec::new();
        }

        let mut msg_ids = Vec::with_capacity(self.messages.len());
        self.messages.scan(|msg_id, _| {
            msg_ids.push(*msg_id);
        });
        msg_ids.sort_unstable();
        msg_ids
            .into_iter()
            .filter_map(|msg_id| self.messages.get(&msg_id).map(|msg| msg.clone()))
            .collect()
    }
}

pub(super) async fn run(
    builder: TaosBuilder,
    mut ws_stream: WsStream,
    query_sender: WsQuerySender,
    message_reader: Receiver<WsMessage>,
    mut close_reader: watch::Receiver<bool>,
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
                query_sender.clone(),
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
                        cleanup_after_disconnect(query_sender.clone());
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

        match builder.connect(EndpointType::Ws).await {
            Ok((ws, ver)) => {
                ws_stream = ws;
                query_sender.version_info.update(ver).await;
            }
            Err(err) => {
                tracing::error!("WebSocket reconnection failed: {err}");
                cleanup_after_disconnect(query_sender.clone());
                return;
            }
        };

        tracing::info!("WebSocket reconnected successfully");

        cleanup_after_reconnect(query_sender.clone(), cache.clone());
    }
}

async fn send_messages(
    mut ws_stream_sender: WsStreamSender,
    message_reader: Receiver<WsMessage>,
    mut close_reader: watch::Receiver<bool>,
    err_sender: mpsc::Sender<Error>,
    cache: MessageCache,
) {
    tracing::trace!("start sending messages to WebSocket stream");

    let mut interval = time::interval(Duration::from_secs(53));

    for message in cache.messages() {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(err) = send_ping_message(&mut ws_stream_sender).await {
                    tracing::error!("failed to send WebSocket ping message: {err}");
                    let _ = err_sender.send(err).await;
                    return;
                }
            }
            _ = close_reader.changed() => {
                tracing::info!("WebSocket sender received close signal");
                send_close_message(&mut ws_stream_sender).await;
                return;
            }
            res = ws_stream_sender.send(message) => {
                if let Err(err) = res {
                    tracing::error!("WebSocket sender error: {err:?}");
                    let _ = err_sender.send(err.into()).await;
                    return;
                }
            }
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
            }
            _ = close_reader.changed() => {
                tracing::info!("WebSocket sender received close signal");
                send_close_message(&mut ws_stream_sender).await;
                break;
            }
            message = message_reader.recv_async() => {
                match message {
                    Ok(message) => {
                        let req_id = message.req_id();
                        let should_cache = message.should_cache();
                        let message = message.into_message();
                        if should_cache {
                            cache.insert(req_id, message.clone());
                        }
                        if let Err(err) = ws_stream_sender.send(message).await {
                            tracing::error!("WebSocket sender error: {err:?}");
                            let _ = err_sender.send(err.into()).await;
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::error!("failed to receive message from channel: {err}");
                        let _ = err_sender.send(Error::CommonError(err.to_string())).await;
                        break;
                    }
                }
            }
        }
    }

    tracing::trace!("stop sending messages to WebSocket stream");
}

async fn send_ping_message(ws_stream_sender: &mut WsStreamSender) -> Result<(), Error> {
    ws_stream_sender
        .send(Message::Ping(b"TAOS".to_vec()))
        .await
        .map_err(Into::<Error>::into)?;

    ws_stream_sender.flush().await.map_err(Into::<Error>::into)
}

async fn send_close_message(ws_stream_sender: &mut WsStreamSender) {
    if let Err(err) = ws_stream_sender.send(Message::Close(None)).await {
        tracing::error!("failed to send close message: {err:?}");
    }
    if let Err(err) = ws_stream_sender.close().await {
        tracing::error!("failed to close WebSocket stream: {err:?}");
    }
}

async fn read_messages(
    mut ws_stream_reader: WsStreamReader,
    query_sender: WsQuerySender,
    mut close_reader: watch::Receiver<bool>,
    err_sender: mpsc::Sender<Error>,
    cache: MessageCache,
) {
    tracing::trace!("start reading messages from WebSocket stream");

    let (message_tx, message_rx) = mpsc::channel(64);

    let message_handle =
        tokio::spawn(handle_messages(message_rx, query_sender.clone(), cache).in_current_span());

    let mut closed_normally = false;

    loop {
        tokio::select! {
            res = ws_stream_reader.try_next() => {
                match res {
                    Ok(Some(message)) => {
                        if let Err(err) = message_tx.send(message).await {
                            tracing::error!("failed to send message to handler, err: {err:?}");
                            let _ = err_sender.send(WsError::ConnectionClosed.into()).await;
                            break;
                        }
                    }
                    Ok(None) => {
                        tracing::info!("WebSocket stream closed by peer");
                        closed_normally = true;
                        break;
                    }
                    Err(err) => {
                        tracing::error!("WebSocket reader error: {err:?}");
                        let _ = err_sender.send(err.into()).await;
                        break;
                    }
                }
            }
            _ = close_reader.changed() => {
                tracing::info!("WebSocket reader received close signal");
                closed_normally = true;
                break;
            }
        }
    }

    drop(message_tx);

    if let Err(err) = message_handle.await {
        tracing::error!("handle messages task failed: {err:?}");
    }

    if closed_normally {
        cleanup_after_disconnect(query_sender.clone());
    }

    tracing::trace!("stop reading messages from WebSocket stream");
}

async fn handle_messages(
    mut message_reader: mpsc::Receiver<Message>,
    query_sender: WsQuerySender,
    cache: MessageCache,
) {
    while let Some(message) = message_reader.recv().await {
        parse_message(message, query_sender.clone(), cache.clone());
    }
}

fn parse_message(message: Message, query_sender: WsQuerySender, cache: MessageCache) {
    match message {
        Message::Text(text) => parse_text_message(text, query_sender, cache),
        Message::Binary(payload) => parse_binary_message(payload, query_sender, cache),
        Message::Ping(data) => {
            tokio::spawn(async move {
                let _ = query_sender
                    .sender
                    .send_async(WsMessage::Raw(Message::Pong(data)))
                    .await;
            });
        }
        Message::Close(_) => {
            // taosAdapter should never send a close frame to the client.
            // Therefore all close frames should be treated as errors.
            tracing::warn!("received unexpected close message, this should not happen");
        }
        Message::Pong(_) => tracing::trace!("received pong message, do nothing"),
        Message::Frame(_) => {
            tracing::warn!("received unexpected frame message, do nothing");
            tracing::trace!("frame message: {message:?}");
        }
    }
}

fn parse_text_message(text: String, query_sender: WsQuerySender, cache: MessageCache) {
    tracing::trace!("received text message, text: {text}");

    let resp = match serde_json::from_str::<WsRecv>(&text) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!("failed to deserialize json text: {text}, err: {err:?}");
            return;
        }
    };

    let (req_id, data, ok) = resp.ok();

    cache.remove(&req_id);

    match &data {
        WsRecvData::Conn
        | WsRecvData::Version { .. }
        | WsRecvData::Block { .. }
        | WsRecvData::BlockNew { .. }
        | WsRecvData::BlockV2 { .. } => unreachable!("unexpected data type: {:?}", data),
        _ => {
            if let Some((_, sender)) = query_sender.queries.remove(&req_id) {
                if let Err(err) = sender.send(ok.map(|_| data)) {
                    tracing::warn!("failed to send data, req_id: {req_id}, err: {err:?}");
                }
            } else {
                tracing::warn!("no sender found for req_id: {req_id}, the message may be lost");
            }
        }
    }
}

fn parse_binary_message(payload: Vec<u8>, query_sender: WsQuerySender, cache: MessageCache) {
    tracing::trace!("received binary message, len: {}", payload.len());

    let is_v3 = query_sender.version_info.is_v3();

    tokio::spawn(async move {
        use taos_query::util::InlinableRead;

        let mut slice = payload.as_slice();
        let mut is_block_new = false;

        let timing = if is_v3 {
            let timing = slice.read_u64().unwrap();
            if timing == u64::MAX {
                is_block_new = true;
                Duration::ZERO
            } else {
                Duration::from_nanos(timing)
            }
        } else {
            Duration::ZERO
        };

        let (req_id, data) = if is_block_new {
            let _action = slice.read_u64().unwrap();
            let block_version = slice.read_u16().unwrap();
            let timing = Duration::from_nanos(slice.read_u64().unwrap());
            let block_req_id = slice.read_u64().unwrap();
            let block_code = slice.read_u32().unwrap();
            let block_message = slice.read_inlined_str::<4>().unwrap();
            let _res_id = slice.read_u64().unwrap();
            let finished = slice.read_u8().unwrap() == 1;
            let block = if finished {
                Vec::new()
            } else {
                slice.read_inlined_bytes::<4>().unwrap()
            };

            cache.remove(&block_req_id);

            (
                block_req_id,
                WsRecvData::BlockNew {
                    block_version,
                    timing,
                    block_req_id,
                    block_code,
                    block_message,
                    finished,
                    raw: block,
                },
            )
        } else {
            let res_id = slice.read_u64().unwrap();
            if let Some((_, req_id)) = query_sender.results.remove(&res_id) {
                cache.remove(&req_id);

                let data = if is_v3 {
                    WsRecvData::Block {
                        timing,
                        raw: payload[16..].to_vec(),
                    }
                } else {
                    WsRecvData::BlockV2 {
                        timing,
                        raw: payload[8..].to_vec(),
                    }
                };

                (req_id, data)
            } else {
                tracing::warn!("no request id found for result id: {res_id}");
                return;
            }
        };

        if let Some((_, sender)) = query_sender.queries.remove(&req_id) {
            if let Err(err) = sender.send(Ok(data)) {
                tracing::warn!("failed to send data, req_id: {req_id}, err: {err:?}");
            }
        } else {
            tracing::warn!("no sender found for req_id: {req_id}, the message may be lost");
        }
    });
}

fn is_disconnect_error(err: &Error) -> bool {
    match err {
        Error::TungsteniteError(ws_err) => matches!(
            ws_err,
            WsError::ConnectionClosed
                | WsError::AlreadyClosed
                | WsError::Io(_)
                | WsError::Tls(_)
                | WsError::Protocol(_)
        ),
        _ => false,
    }
}

fn cleanup_after_disconnect(query_sender: WsQuerySender) {
    let mut req_ids = Vec::with_capacity(query_sender.queries.len());
    query_sender.queries.scan(|req_id, _| {
        req_ids.push(*req_id);
    });

    for req_id in req_ids {
        if let Some((_, sender)) = query_sender.queries.remove(&req_id) {
            let _ = sender.send(Err(RawError::from_code(WS_ERROR_NO::CONN_CLOSED.as_code())
                .context("WebSocket connection is closed")));
        }
    }
}

fn cleanup_after_reconnect(query_sender: WsQuerySender, cache: MessageCache) {
    let mut req_ids = HashSet::new();

    query_sender.queries.scan(|req_id, _| {
        if !cache.req_to_msg.contains(req_id) {
            req_ids.insert(*req_id);
        }
    });

    query_sender.results.scan(|_, req_id| {
        req_ids.insert(*req_id);
    });

    query_sender.results.clear();

    for req_id in req_ids {
        if let Some((_, sender)) = query_sender.queries.remove(&req_id) {
            let _ = sender.send(Err(RawError::from_code(WS_ERROR_NO::CONN_CLOSED.as_code())
                .context("WebSocket connection is closed")));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use serde_json::json;
    use taos_query::{AsyncQueryable, AsyncTBuilder};
    use tokio::task::JoinHandle;
    use tracing::Instrument;
    use warp::ws::Message;
    use warp::Filter;

    use crate::TaosBuilder;

    #[tokio::test]
    async fn test_ws_auto_reconnect() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let _handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(
            async move {
                let taos = TaosBuilder::from_dsn("ws://127.0.0.1:9980")?
                    .build()
                    .await?;
                let _ = taos.query("select * from meters").await?;
                Ok(())
            }
            .in_current_span(),
        );

        let (close_tx, close_rx) = flume::bounded(1);

        let routes = warp::path("ws").and(warp::ws()).map({
            move |ws: warp::ws::Ws| {
                let close = close_tx.clone();
                ws.on_upgrade(move |ws| async {
                    let close = close;
                    let (mut ws_tx, mut ws_rx) = ws.split();
                    while let Some(res) = ws_rx.next().await {
                        let message = res.unwrap();
                        tracing::debug!("ws recv message: {message:?}");
                        if message.is_text() {
                            let text = message.to_str().unwrap();
                            if text.contains("version") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "version",
                                    "req_id": 100,
                                    "version": "3.0"
                                });
                                let message = Message::text(data.to_string());
                                let _ = ws_tx.send(message).await;
                            } else if text.contains("conn") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "conn",
                                    "req_id": 100
                                });
                                let message = Message::text(data.to_string());
                                let _ = ws_tx.send(message).await;
                            } else if text.contains("query") {
                                let _ = close.send_async(()).await;
                                break;
                            }
                        }
                    }
                })
            }
        });

        let close = close_rx.clone();
        let (_, server) = warp::serve(routes.clone()).bind_with_graceful_shutdown(
            ([127, 0, 0, 1], 9980),
            async move {
                let _ = close.recv_async().await;
                tracing::debug!("shutting down...");
            },
        );

        server.await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (_, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 9980), async move {
                let _ = close_rx.recv_async().await;
                tracing::debug!("restarted server shutting down...");
            });

        server.await;

        Ok(())
    }
}
