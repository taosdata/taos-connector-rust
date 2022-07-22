use bytes::{Bytes, BytesMut};
use futures::{FutureExt, SinkExt, StreamExt};
use itertools::Itertools;
use scc::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use taos_query::common::{Column, Field, JsonMeta, Precision, RawMeta};
use taos_query::util::Inlinable;
use taos_query::{
    AsyncFetchable, AsyncQueryable, Connectable, DeError, Dsn, DsnError, IntoDsn, RawData,
};
use thiserror::Error;
use tokio::sync::{oneshot, watch};

use tokio::time;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::infra::ToMessage;
use crate::{infra::WsConnReq, WsInfo};
use messages::*;

use std::fmt::Debug;
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

mod messages;

type StmtResult = StdResult<Option<usize>, taos_error::Error>;
type StmtSender = std::sync::mpsc::SyncSender<StmtResult>;
type StmtReceiver = std::sync::mpsc::Receiver<StmtResult>;

type WsSender = tokio::sync::mpsc::Sender<Message>;
type WsTmqAgent = Arc<HashMap<ReqId, oneshot::Sender<StdResult<TmqRecvData, taos_error::Error>>>>;

#[derive(Debug, Clone)]
pub struct WsTmqSender {
    req_id: Arc<AtomicU64>,
    sender: WsSender,
    queries: WsTmqAgent,
    timeout: Duration,
}

impl WsTmqSender {
    fn req_id(&self) -> ReqId {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    async fn send_recv(&self, msg: TmqSend) -> Result<TmqRecvData> {
        self.send_recv_timeout(msg, self.timeout).await
    }
    async fn send_recv_timeout(&self, msg: TmqSend, timeout: Duration) -> Result<TmqRecvData> {
        let send_timeout = Duration::from_millis(500);
        if let TmqSend::Close = msg {
            log::debug!("send close message");
            self.sender
                .send(Message::Close(None))
                .await?;
            return Ok(TmqRecvData::Close);
        }
        let req_id = msg.req_id();
        let (tx, rx) = oneshot::channel();

        self.queries.insert(req_id, tx).unwrap();

        self.sender.send_timeout(msg.to_msg(), send_timeout).await?;

        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);
        let data = tokio::select! {
            _ = &mut sleep, if !sleep.is_elapsed() => {
               log::debug!("poll timed out");
               Err(Error::QueryTimeout("poll".to_string()))?
            }
            message = rx => {
                message??
            }
        };
        Ok(data)
    }
}

pub struct WsBuilder {
    info: WsInfo,
    conf: TmqInit,
}

pub struct WsMessageBase {
    sender: WsTmqSender,
    message_id: MessageId,
}

impl WsMessageBase {
    async fn fetch_json_meta(&self) -> Result<JsonMeta> {
        let req_id = self.sender.req_id();
        let msg = TmqSend::FetchJsonMeta(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        if let TmqRecvData::FetchJsonMeta { data } = data {
            let json: JsonMeta = serde_json::from_value(data)?;
            dbg!(&json);
            return Ok(json);
            // dbg!(data);
            // Ok(data)
        }
        unreachable!()
    }
    async fn fetch_raw_meta(&self) -> Result<RawMeta> {
        let req_id = self.sender.req_id();
        let msg = TmqSend::FetchRawMeta(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        if let TmqRecvData::FetchRawMeta { meta } = data {
            let raw = RawMeta::new(meta);
            return Ok(raw);
            // let json: JsonMeta = serde_json::from_value(data)?;
            // dbg!(&json);
            // dbg!(data);
            // Ok(data)
        }
        unreachable!()
    }
}

pub struct WsMetaMessage(WsMessageBase);

impl WsMetaMessage {
    pub async fn as_raw_meta(&self) -> Result<RawMeta> {
        self.0.fetch_raw_meta().await
    }
    pub async fn as_json_meta(&self) -> Result<JsonMeta> {
        self.0.fetch_json_meta().await
    }
}

pub struct WsDataMessage(WsMessageBase);

impl WsDataMessage {
    pub async fn fetch_block(&self) -> Result<Option<RawData>> {
        todo!()
    }
}

pub enum WsMessageSet {
    Meta(WsMetaMessage),
    Data(WsDataMessage),
}

impl WsMessageSet {
    pub const fn message_type(&self) -> MessageType {
        match self {
            WsMessageSet::Meta(_) => MessageType::Meta,
            WsMessageSet::Data(_) => MessageType::Data,
        }
    }
}

impl WsConsumerBuilder {
    pub async fn subscribe<Item: Into<String>, Iter: IntoIterator<Item = Item>>(
        &mut self,
        topics: Iter,
    ) -> Result<()> {
        let req_id = self.sender.req_id();
        let action = TmqSend::Subscribe {
            req_id,
            req: self.tmq_conf.clone(),
            topics: topics.into_iter().map(Into::into).collect_vec(),
            conn: self.conn.clone(),
        };
        self.sender.send_recv(action).await?;
        Ok(())
    }

    pub async fn poll_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<(Offset, WsMessageSet)>> {
        let req_id = self.sender.req_id();
        let action = TmqSend::Poll {
            req_id,
            blocking_time: -1,
        };

        let data = self.sender.send_recv_timeout(action, timeout).await;
        if data.is_err() {
            let err = data.unwrap_err();
            match err {
                Error::QueryTimeout(_) => return Ok(None),
                _ => return Err(err),
            }
        }
        let data = data.unwrap();

        match data {
            TmqRecvData::Poll(TmqPoll {
                message_id,
                database,
                have_message,
                topic,
                vgroup_id,
                message_type,
            }) => {
                if have_message {
                    let offset = Offset {
                        message_id,
                        database,
                        topic,
                        vgroup_id,
                    };
                    let message = WsMessageBase {
                        sender: self.sender.clone(),
                        message_id,
                    };
                    match message_type {
                        MessageType::Meta => {
                            Ok(Some((offset, WsMessageSet::Meta(WsMetaMessage(message)))))
                        }
                        MessageType::Data => {
                            Ok(Some((offset, WsMessageSet::Data(WsDataMessage(message)))))
                        }
                        _ => unreachable!(),
                    }
                } else {
                    Ok(None)
                }
            }
            _ => unreachable!(),
        }
    }

    pub async fn commit(&mut self, offset: Offset) -> Result<()> {
        let req_id = self.sender.req_id();
        let action = TmqSend::Commit(MessageArgs {
            req_id,
            message_id: offset.message_id,
        });

        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    pub async fn unsubscribe(&mut self) -> Result<()> {
        let action = TmqSend::Close;
        self.sender.send_recv(action).await?;
        Ok(())
    }
}

impl WsBuilder {
    pub fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self> {
        let dsn = dsn.into_dsn()?;
        let info = WsInfo::from_dsn(&dsn)?;
        let group_id = dsn
            .params
            .get("group.id")
            .map(ToString::to_string)
            .ok_or(DsnError::RequireParam("group.id".to_string()))?;
        let client_id = dsn.params.get("client.id").map(ToString::to_string);
        let offset_reset = dsn.params.get("auto.offset.reset").map(ToString::to_string);

        let conf = TmqInit {
            group_id,
            client_id,
            offset_reset,
        };

        Ok(Self { info, conf })
    }

    pub async fn build(&self) -> Result<WsConsumerBuilder> {
        let url = self.info.to_tmq_url();
        let (ws, _) = connect_async(url).await?;
        let (mut sender, mut reader) = ws.split();

        let queries = Arc::new(HashMap::<ReqId, tokio::sync::oneshot::Sender<_>>::new(
            100,
            RandomState::new(),
        ));

        use std::collections::hash_map::RandomState;
        let fetches = Arc::new(HashMap::<StmtId, StmtSender>::new(100, RandomState::new()));

        let queries_sender = queries.clone();
        let fetches_sender = fetches.clone();

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel::<Message>(100);
        let ws2 = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let mut close_listener = rx.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        //
                        log::trace!("Check websocket message sender alive");
                    }
                    Some(msg) = msg_recv.recv() => {
                        dbg!(&msg);
                        if msg.is_close() {
                            sender.close().await.unwrap();
                            panic!();
                            break;
                        }
                        sender.send(msg).await.unwrap();

                        // sender.close().await;
                        log::info!("send done");
                    }
                    _ = rx.changed() => {
                        log::info!("close sender task");
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(message) = reader.next() => {
                        match message {
                            Ok(message) => match message {
                                Message::Text(text) => {
                                    log::info!("json response: {}", text);
                                    let v: TmqRecv = serde_json::from_str(&text).unwrap();
                                    let (req_id, recv, ok) = v.ok();
                                    match &recv {
                                        TmqRecvData::Subscribe => {
                                               log::info!("subscribe with: {:?}", req_id);

                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                sender.send(ok.map(|_|recv)).unwrap();
                                            }  else {
                                                log::warn!("subscribe message received but no receiver alive");
                                            }
                                        },
                                        TmqRecvData::Poll(poll) => {
                                            dbg!(poll);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                sender.send(ok.map(|_|recv)).unwrap();
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }
                                        },
                                        TmqRecvData::FetchJsonMeta { data }=> {
                                               log::info!("fetch json meta data: {:?}", data);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                sender.send(ok.map(|_|recv)).unwrap();
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::FetchRawMeta { meta }=> {
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                sender.send(ok.map(|_|recv)).unwrap();
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Commit=> {
                                               log::info!("commit done: {:?}", recv);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                sender.send(ok.map(|_|recv)).unwrap();
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        _ => unreachable!("unknown tmq response"),
                                    }
                                }
                                Message::Binary(data) => {
                                        // writeUint64(message.buffer, req.ReqID)
                                        // writeUint64(message.buffer, req.MessageID)
                                        // writeUint64(message.buffer, TMQRawMetaMessage)
                                        // writeUint32(message.buffer, length)
                                        // writeUint16(message.buffer, metaType)
                                    let mut bytes = Bytes::from(data);
                                    let meta = bytes.slice(24..);
                                    // dbg!(&bytes);
                                    use bytes::Buf;
                                    let req_id = bytes.get_u64_le();
                                    let message_id = bytes.get_u64_le();
                                    let message_type = bytes.get_u64_le();
                                    log::debug!("receive binary message with req_id {} message_id {}", req_id, message_id);
                                    let data = if message_type == 3 {
                                        TmqRecvData::FetchRawMeta{ meta }
                                    } else {
                                        todo!()
                                    };
                                    if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                sender.send(Ok(data)).unwrap();
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }

                                }
                                Message::Close(_) => {
                                    log::warn!("websocket connection is closed (unexpected?)");
                                    break;
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
                                    log::debug!("* frame data: {frame:?}");
                                }
                            },
                            Err(err) => {
                                log::error!("receiving cause error: {err:?}");
                                break;
                            }
                        }
                    }
                    _ = close_listener.changed() => {
                        log::info!("close reader task");
                        break
                    }
                }
            }
        });
        Ok(WsConsumerBuilder {
            conn: self.info.to_conn_request(),
            tmq_conf: self.conf.clone(),
            sender: WsTmqSender {
                req_id: Arc::new(AtomicU64::new(1)),
                queries,
                sender: ws,
                timeout: Duration::from_secs(5),
            },
            fetches,
            close_signal: tx,
            timeout: Duration::from_secs(5),
        })
    }
}

pub struct WsConsumerBuilder {
    conn: WsConnReq,
    tmq_conf: TmqInit,
    sender: WsTmqSender,
    close_signal: watch::Sender<bool>,
    fetches: Arc<HashMap<StmtId, StmtSender>>,
    timeout: Duration,
}

pub struct Offset {
    message_id: MessageId,
    database: String,
    topic: String,
    vgroup_id: u64,
}

pub struct WsConsumer {
    /// Message sending timeout, default is 5min.
    timeout: Duration,
    ws: WsSender,
    fetches: Arc<HashMap<StmtId, StmtSender>>,
    receiver: StmtReceiver,
    args: StmtArgs,
}
impl Debug for WsConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsConsumer")
            .field("ws", &"...")
            .field("fetches", &"...")
            .field("receiver", &self.receiver)
            .field("args", &self.args)
            .finish()
    }
}

impl Drop for WsConsumer {
    fn drop(&mut self) {
        self.fetches.remove(&self.args.stmt_id);
        let args = self.args;
        let ws = self.ws.clone();
        let _ = ws.blocking_send(StmtSend::Close(args).to_msg());
    }
}

impl Debug for WsConsumerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsClient")
            .field("sender", &self.sender)
            .field("...", &"...")
            .finish()
    }
}
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Dsn(#[from] DsnError),
    #[error("{0}")]
    FetchError(#[from] oneshot::error::RecvError),
    #[error("{0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<Message>),
    #[error(transparent)]
    SendTimeoutError(#[from] tokio::sync::mpsc::error::SendTimeoutError<Message>),
    #[error("{0}")]
    StdSendError(#[from] std::sync::mpsc::SendError<tokio_tungstenite::tungstenite::Message>),
    #[error("{0}")]
    RecvError(#[from] std::sync::mpsc::RecvError),
    #[error("{0}")]
    RecvTimeout(#[from] std::sync::mpsc::RecvTimeoutError),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("Deserialize json error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("{0}")]
    WsError(#[from] WsError),
    #[error("{0}")]
    TaosError(#[from] taos_error::Error),
    #[error("Receive timeout in {0}")]
    QueryTimeout(String),
}

impl Error {
    pub const fn errno(&self) -> taos_error::Code {
        match self {
            Error::TaosError(error) => error.code(),
            _ => taos_error::Code::Failed,
        }
    }
    pub fn errstr(&self) -> String {
        match self {
            Error::TaosError(error) => error.message().to_string(),
            _ => format!("{}", self),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

impl Drop for WsConsumerBuilder {
    fn drop(&mut self) {
        // send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

#[tokio::test]
async fn test_ws_tmq_meta() -> anyhow::Result<()> {
    pretty_env_logger::formatted_builder()
        .filter_level(log::LevelFilter::Trace)
        .init();

    // let taos = WsInfo::from_dsn("taos://localhost:6041")?.connect()?;
    // taos.exec_many([
    //     "drop database if exists ws_abc1",
    //     "create database ws_abc1",
    //     "create topic ws_abc1 with meta as database ws_abc1",
    //     "use ws_abc1",
    //     // kind 1: create super table using all types
    //     "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
    //         c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
    //         c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
    //         tags(t1 json)",
    //     // kind 2: create child table with json tag
    //     "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
    //     "create table tb1 using stb1 tags(NULL)",
    //     "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
    //         NULL, NULL, NULL, NULL, NULL,
    //         NULL, NULL, NULL, NULL)
    //         tb1 values(now, true, -2, -3, -4, -5, \
    //         '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
    //         254, 65534, 1, 1)",
    //     // kind 3: create super table with all types except json (especially for tags)
    //     "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
    //         c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
    //         c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
    //         tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
    //         t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
    //         t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
    //     // kind 4: create child table with all types except json
    //     "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
    //         '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
    //         254, 65534, 1, 1)",
    //     // kind 5: create common table
    //     "create table `table` (ts timestamp, v int)",
    //     // kind 6: column in super table
    //     "alter table stb1 add column new1 bool",
    //     "alter table stb1 add column new2 tinyint",
    //     "alter table stb1 add column new10 nchar(16)",
    //     "alter table stb1 modify column new10 nchar(32)",
    //     "alter table stb1 drop column new10",
    //     "alter table stb1 drop column new2",
    //     "alter table stb1 drop column new1",
    //     // kind 7: add tag in super table
    //     "alter table `stb2` add tag new1 bool",
    //     "alter table `stb2` rename tag new1 new1_new",
    //     "alter table `stb2` modify tag t10 nchar(32)",
    //     "alter table `stb2` drop tag new1_new",
    //     // kind 8: column in common table
    //     "alter table `table` add column new1 bool",
    //     "alter table `table` add column new2 tinyint",
    //     "alter table `table` add column new10 nchar(16)",
    //     "alter table `table` modify column new10 nchar(32)",
    //     "alter table `table` rename column new10 new10_new",
    //     "alter table `table` drop column new10_new",
    //     "alter table `table` drop column new2",
    //     "alter table `table` drop column new1",
    //     // kind 9: drop normal table
    //     "drop table `table`",
    //     // kind 10: drop child table
    //     "drop table `tb2` `tb1`",
    //     // kind 11: drop super table
    //     "drop table `stb2`",
    //     "drop table `stb1`",
    // ])
    // .await?;

    // taos.exec_many([
    //     "drop database if exists db2",
    //     "create database if not exists db2",
    //     "use db2",
    // ])
    // .await?;

    let builder = WsBuilder::from_dsn("taos://localhost:6041?group.id=10")?;
    let mut consumer = builder.build().await?;
    consumer.subscribe(["ws_abc1"]).await?;

    while let Some((offset, message)) = consumer.poll_timeout(Duration::from_secs(5)).await? {
        match message {
            WsMessageSet::Meta(meta) => {
                let json = meta.as_json_meta().await?;
                let sql = dbg!(json.to_string());
                // taos.exec(sql).await?;
                let raw = meta.as_raw_meta().await?;
                // taos.write_meta(raw).await?;
            }
            WsMessageSet::Data(data) => {
                // data message may have more than one data block for various tables.
                while let Some(data) = data.fetch_block().await? {
                    dbg!(data);
                }
            }
        }
        consumer.commit(offset).await?;
    }

    consumer.unsubscribe().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // taos.exec_many(["drop database db2", "drop database ws_abc1"])
    //     .await?;
    Ok(())
}
