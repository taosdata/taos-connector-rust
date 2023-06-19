//! TMQ consumer.
//!
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use itertools::Itertools;
// use scc::HashMap;
use dashmap::DashMap as HashMap;

use taos_query::block_in_place_or_global;
use taos_query::common::{JsonMeta, RawMeta};
use taos_query::prelude::{Code, RawError};
use taos_query::tmq::{
    AsAsyncConsumer, AsConsumer, Assignment, IsAsyncData, IsAsyncMeta, IsOffset, MessageSet,
    SyncOnAsync, Timeout, VGroupId,
};
use taos_query::util::InlinableRead;
use taos_query::{DeError, DsnError, IntoDsn, RawBlock, TBuilder};
use thiserror::Error;

use taos_query::prelude::tokio;
use tokio::sync::{oneshot, watch};

use tokio::time;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::query::asyn::WS_ERROR_NO;
use crate::query::infra::{ToMessage, WsConnReq};
use crate::TaosBuilder;
use messages::*;

use std::fmt::Debug;
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod messages;

type WsSender = tokio::sync::mpsc::Sender<Message>;
type WsTmqAgent = Arc<HashMap<ReqId, oneshot::Sender<StdResult<TmqRecvData, RawError>>>>;

#[derive(Debug, Clone)]
struct WsTmqSender {
    req_id: Arc<AtomicU64>,
    sender: WsSender,
    queries: WsTmqAgent,
    #[allow(dead_code)]
    timeout: Timeout,
}

impl WsTmqSender {
    fn req_id(&self) -> ReqId {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    async fn send_recv(&self, msg: TmqSend) -> Result<TmqRecvData> {
        self.send_recv_timeout(msg, Duration::MAX).await
    }
    async fn send_recv_timeout(&self, msg: TmqSend, timeout: Duration) -> Result<TmqRecvData> {
        let send_timeout = Duration::from_millis(5000);
        let req_id = msg.req_id();
        let (tx, rx) = oneshot::channel();

        self.queries.insert(req_id, tx);

        self.sender.send_timeout(msg.to_msg(), send_timeout).await?;

        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);
        let data = tokio::select! {
            _ = &mut sleep, if !sleep.is_elapsed() => {
               log::trace!("poll timed out");
               Err(Error::QueryTimeout("poll".to_string()))?
            }
            message = rx => {
                message??
            }
        };
        Ok(data)
    }
}

pub struct TmqBuilder {
    info: TaosBuilder,
    conf: TmqInit,
    timeout: Timeout,
}

impl TBuilder for TmqBuilder {
    type Target = Consumer;

    type Error = Error;

    fn available_params() -> &'static [&'static str] {
        &["token", "timeout", "group.id", "client.id"]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> StdResult<Self, Self::Error> {
        Self::new(dsn)
    }

    fn client_version() -> &'static str {
        "0"
    }

    fn ping(&self, _: &mut Self::Target) -> StdResult<(), Self::Error> {
        Ok(())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> StdResult<Self::Target, Self::Error> {
        block_in_place_or_global(self.build_consumer())
    }

    fn server_version(&self) -> StdResult<&str, Self::Error> {
        todo!()
    }

    fn is_enterprise_edition(&self) -> StdResult<bool, Self::Error> {
        todo!()
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TmqBuilder {
    type Target = Consumer;

    type Error = Error;

    fn from_dsn<D: IntoDsn>(dsn: D) -> StdResult<Self, Self::Error> {
        Self::new(dsn)
    }

    fn client_version() -> &'static str {
        "0"
    }

    async fn ping(&self, _: &mut Self::Target) -> StdResult<(), Self::Error> {
        Ok(())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> StdResult<Self::Target, Self::Error> {
        self.build_consumer().await
    }

    async fn server_version(&self) -> StdResult<&str, Self::Error> {
        todo!()
    }

    async fn is_enterprise_edition(&self) -> StdResult<bool, Self::Error> {
        todo!()
    }
}

struct WsMessageBase {
    sender: WsTmqSender,
    message_id: MessageId,
}

impl WsMessageBase {
    async fn fetch_raw_block(&self) -> Result<Option<RawBlock>> {
        let req_id = self.sender.req_id();

        let msg = TmqSend::Fetch(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        let fetch = if let TmqRecvData::Fetch(fetch) = data {
            fetch
        } else {
            unreachable!()
        };

        if fetch.completed {
            return Ok(None);
        }

        let msg = TmqSend::FetchBlock(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        if let TmqRecvData::Bytes(bytes) = data {
            let mut raw = RawBlock::parse_from_raw_block(bytes, fetch.precision);

            // for row in 0..raw.nrows() {
            //     for col in 0..raw.ncols() {
            //         log::trace!("at ({}, {})", row, col);
            //         let v = unsafe { raw.get_ref_unchecked(row, col) };
            //         println!("({}, {}): {:?}", row, col, v);
            //     }
            // }
            raw.with_field_names(fetch.fields().iter().map(|f| f.name()));
            if let Some(name) = fetch.table_name {
                raw.with_table_name(name);
            }
            return Ok(Some(raw));
        }
        todo!()
    }
    async fn fetch_json_meta(&self) -> Result<JsonMeta> {
        let req_id = self.sender.req_id();
        let msg = TmqSend::FetchJsonMeta(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        if let TmqRecvData::FetchJsonMeta { data } = data {
            let json: JsonMeta = serde_json::from_value(data)?;
            return Ok(json);
        }
        unreachable!()
    }
    async fn fetch_raw_meta(&self) -> Result<RawMeta> {
        let req_id = self.sender.req_id();
        let msg = TmqSend::FetchRaw(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        if let TmqRecvData::Bytes(bytes) = data {
            let message_type = bytes.as_ref().read_u64().unwrap();
            debug_assert_eq!(message_type, 3, "should be meta message type");
            let raw = RawMeta::new(bytes.slice(8..)); // first u64 is message type.
            return Ok(raw);
        }
        unreachable!()
    }
}

pub struct Meta(WsMessageBase);

// impl WsMetaMessage {
//     pub async fn as_raw_meta(&self) -> Result<RawMeta> {
//         self.0.fetch_raw_meta().await
//     }
//     pub async fn as_json_meta(&self) -> Result<JsonMeta> {
//         self.0.fetch_json_meta().await
//     }
// }

#[async_trait::async_trait]
impl IsAsyncMeta for Meta {
    type Error = Error;

    async fn as_raw_meta(&self) -> StdResult<RawMeta, Self::Error> {
        self.0.fetch_raw_meta().await
    }

    async fn as_json_meta(&self) -> StdResult<JsonMeta, Self::Error> {
        self.0.fetch_json_meta().await
    }
}

impl SyncOnAsync for Meta {}

pub struct Data(WsMessageBase);

impl Data {
    pub async fn fetch_block(&self) -> Result<Option<RawBlock>> {
        self.0.fetch_raw_block().await
    }
}

impl Iterator for Data {
    type Item = Result<RawBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place_or_global(self.fetch_block()).transpose()
    }
}

#[async_trait::async_trait]
impl IsAsyncData for Data {
    type Error = Error;

    async fn as_raw_data(&self) -> StdResult<taos_query::common::RawData, Self::Error> {
        self.0
            .fetch_raw_meta()
            .await
            .map(|raw| unsafe { std::mem::transmute(raw) })
    }

    async fn fetch_raw_block(&self) -> StdResult<Option<RawBlock>, Self::Error> {
        self.fetch_block().await
    }
}

pub enum WsMessageSet {
    Meta(Meta),
    Data(Data),
}

impl WsMessageSet {
    pub const fn message_type(&self) -> MessageType {
        match self {
            WsMessageSet::Meta(_) => MessageType::Meta,
            WsMessageSet::Data(_) => MessageType::Data,
        }
    }
}

impl Consumer {
    // async fn init_poll(&self, timeout: Duration) -> Result<()> {
    //     let req_id = self.sender.req_id();
    //     let action = TmqSend::Poll {
    //         req_id,
    //         blocking_time: 0,
    //     };

    //     let data = self.sender.send_recv_timeout(action, timeout).await?;
    //     match data {
    //         TmqRecvData::Poll(TmqPoll {
    //             message_id,
    //             database,
    //             have_message,
    //             topic,
    //             vgroup_id,
    //             message_type,
    //         }) => {
    //             assert!(!have_message);
    //         }
    //         _ => unreachable!(),
    //     }
    //     Ok(())
    // }
    async fn poll_wait(&self) -> Result<(Offset, MessageSet<Meta, Data>)> {
        let elapsed = tokio::time::Instant::now();
        loop {
            let req_id = self.sender.req_id();
            let action = TmqSend::Poll {
                req_id,
                blocking_time: 0,
            };

            let data = self.sender.send_recv(action).await?;

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
                        let dur = elapsed.elapsed();
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
                        log::trace!("Got message in {}ms", dur.as_millis());
                        break match message_type {
                            MessageType::Meta => Ok((offset, MessageSet::Meta(Meta(message)))),
                            MessageType::Data => Ok((offset, MessageSet::Data(Data(message)))),
                            MessageType::MetaData => Ok((
                                offset,
                                MessageSet::MetaData(
                                    Meta(message),
                                    Data(WsMessageBase {
                                        sender: self.sender.clone(),
                                        message_id,
                                    }),
                                ),
                            )),
                            MessageType::Invalid => unreachable!(),
                            // _ => unreachable!(),
                        };
                    } else {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                }
                _ => unreachable!(),
            }
        }
    }
    pub(crate) async fn poll_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<(Offset, MessageSet<Meta, Data>)>> {
        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);
        return tokio::select! {
            _ = &mut sleep, if !sleep.is_elapsed() => {
               Ok(None)
            }
            message = self.poll_wait() => {
                Ok(Some(message?))
            }
        };
    }
}

#[async_trait::async_trait]
impl AsAsyncConsumer for Consumer {
    type Error = Error;

    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> Result<()> {
        self.topics = topics.into_iter().map(Into::into).collect_vec();
        let req_id = self.sender.req_id();
        let action = TmqSend::Subscribe {
            req_id,
            req: self.tmq_conf.clone(),
            topics: self.topics.clone(),
            conn: self.conn.clone(),
        };
        self.sender.send_recv(action).await?;

        // dbg!(&self.tmq_conf);

        if let Some(offset) = self.tmq_conf.offset_seek.clone() {
            // dbg!(offset);
            let offsets = offset
                .split(",")
                .map(|s| {
                    s.split(":")
                        .map(|i| i.parse::<i64>().unwrap())
                        .collect_vec()
                })
                .collect_vec();
            let topic_name = &self.topics[0];
            for offset in offsets {
                let vgroup_id = offset[0] as i32;
                let offset = offset[1];
                log::debug!(
                    "topic {} seeking to offset {} for vgroup {}",
                    &topic_name,
                    offset,
                    vgroup_id
                );

                let req_id = self.sender.req_id();
                let action = TmqSend::Seek(OffsetSeekArgs {
                    req_id,
                    topic: topic_name.to_string(),
                    vgroup_id,
                    offset,
                });

                let _ = self
                    .sender
                    .send_recv(action)
                    .await
                    .unwrap_or(crate::consumer::messages::TmqRecvData::Seek { timing: 0 });
            }
        }

        Ok(())
    }

    async fn unsubscribe(self) {
        let req_id = self.sender.req_id();
        log::trace!("unsubscribe {} start", req_id);
        let action = TmqSend::Unsubscribe { req_id };
        self.sender.send_recv(action).await.unwrap();
        drop(self)
    }

    async fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> StdResult<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
        Self::Error,
    > {
        match timeout {
            Timeout::Never | Timeout::None => self.poll_timeout(Duration::MAX).await,
            Timeout::Duration(timeout) => self.poll_timeout(timeout).await,
        }
    }

    async fn commit(&self, offset: Self::Offset) -> StdResult<(), Self::Error> {
        let req_id = self.sender.req_id();
        let action = TmqSend::Commit(MessageArgs {
            req_id,
            message_id: offset.message_id,
        });

        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    async fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        let topics = self.topics.clone();
        log::trace!("topics: {:?}", topics);

        let mut ret = Vec::new();
        for topic in topics {
            let assignments = self.topic_assignment(&topic).await;
            ret.push((topic, assignments));
        }

        Some(ret)
    }

    async fn topic_assignment(&self, topic: &str) -> Vec<Assignment> {
        let req_id = self.sender.req_id();
        let action = TmqSend::Assignment(TopicAssignmentArgs {
            req_id,
            topic: topic.to_string(),
        });

        let recv = self.sender.send_recv(action).await.ok();
        match recv {
            Some(TmqRecvData::Assignment(TopicAssignment {
                assignment,
                timing,
            })) => {
                // assert_eq!(topic, topic);
                log::trace!("timing: {:?}", timing);
                log::trace!("assignment: {:?}", assignment);
                assignment
            }
            _ => { vec![] },
        }
    }

    async fn offset_seek(
        &mut self,
        topic: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> StdResult<(), Self::Error> {
        let req_id = self.sender.req_id();
        let action = TmqSend::Seek(OffsetSeekArgs {
            req_id,
            topic: topic.to_string(),
            vgroup_id,
            offset,
        });

        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    fn default_timeout(&self) -> Timeout {
        self.timeout
    }
}

impl AsConsumer for Consumer {
    type Error = Error;

    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> StdResult<(), Self::Error> {
        block_in_place_or_global(<Consumer as AsAsyncConsumer>::subscribe(self, topics))
    }

    fn recv_timeout(
        &self,
        timeout: Timeout,
    ) -> StdResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>, Self::Error> {
        block_in_place_or_global(<Consumer as AsAsyncConsumer>::recv_timeout(self, timeout))
    }

    fn commit(&self, offset: Self::Offset) -> StdResult<(), Self::Error> {
        block_in_place_or_global(<Consumer as AsAsyncConsumer>::commit(self, offset))
    }

    fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        block_in_place_or_global(<Consumer as AsAsyncConsumer>::assignments(self))
    }

    fn offset_seek(
        &mut self,
        topic: &str,
        vg_id: VGroupId,
        offset: i64,
    ) -> StdResult<(), Self::Error> {
        block_in_place_or_global(<Consumer as AsAsyncConsumer>::offset_seek(
            self, topic, vg_id, offset,
        ))
    }
}

impl TmqBuilder {
    pub fn new<D: IntoDsn>(dsn: D) -> Result<Self> {
        let dsn = dsn.into_dsn()?;
        let info = TaosBuilder::from_dsn(&dsn)?;
        let group_id = dsn
            .params
            .get("group.id")
            .map(ToString::to_string)
            .ok_or_else(|| DsnError::RequireParam("group.id".to_string()))?;
        let client_id = dsn.params.get("client.id").map(ToString::to_string);
        let offset_reset = dsn.params.get("auto.offset.reset").map(ToString::to_string);
        let auto_commit = dsn
            .params
            .get("enable.auto.commit")
            .map(|s| {
                if s.is_empty() {
                    "false".to_string()
                } else {
                    s.to_string()
                }
            })
            .unwrap_or("false".to_string());
        let auto_commit_interval_ms = dsn.params.get("auto.commit.interval.ms").and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        });
        let snapshot_enable = dsn
            .params
            .get("experimental.snapshot.enable")
            .and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            })
            .unwrap_or("true".to_string());
        let with_table_name = dsn
            .params
            .get("with.table.name")
            .and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            })
            .unwrap_or("true".to_string());
        let timeout = if let Some(timeout) = dsn.get("timeout") {
            Timeout::from_str(&timeout).map_err(RawError::from_any)?
        } else {
            Timeout::Duration(Duration::from_secs(5))
        };
        let offset_seek = dsn.params.get("offset").and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        });
        let conf = TmqInit {
            group_id,
            client_id,
            offset_reset,
            auto_commit,
            auto_commit_interval_ms,
            snapshot_enable,
            with_table_name,
            offset_seek,
        };

        Ok(Self {
            info,
            conf,
            timeout,
        })
    }

    async fn build_consumer(&self) -> Result<Consumer> {
        let url = self.info.to_tmq_url();
        // let (ws, _) = futures::executor::block_on(connect_async(url))?;
        let (ws, _) = connect_async(&url).await?;
        let (mut sender, mut reader) = ws.split();

        let queries = Arc::new(HashMap::<ReqId, tokio::sync::oneshot::Sender<_>>::new());

        let queries_sender = queries.clone();
        let msg_handler = queries.clone();

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel::<Message>(100);
        let ws2 = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let mut close_listener = rx.clone();

        let sending_url = url.clone();
        static PING_INTERVAL: u64 = 30;
        const PING: &'static [u8] = b"TAOSX";

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(PING_INTERVAL));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        log::trace!("Check websocket message sender alive");
                        if let Err(err) = sender.send(Message::Ping(PING.to_vec())).await {
                            log::trace!("sending ping message to {sending_url} error: {err:?}");
                            // let mut keys = Vec::new();
                            let keys = msg_handler.iter().map(|r| *r.key()).collect_vec();

                            // msg_handler.for_each_async(|k, _| {
                            //     keys.push(*k);
                            // }).await;
                            for k in keys {
                                if let Some((_, sender)) = msg_handler.remove(&k) {
                                    let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(),
                                        format!("WebSocket internal error: {err}"))));
                                }
                            }
                        }
                    }
                    Some(msg) = msg_recv.recv() => {
                        if msg.is_close() {
                            let _ = sender.send(msg).await;
                            let _ = sender.close().await;
                            break;
                        }
                        log::trace!("send message {msg:?}");
                        if let Err(err) = sender.send(msg).await {
                            log::trace!("sending message to {sending_url} error: {err:?}");
                            let keys = msg_handler.iter().map(|r| *r.key()).collect_vec();
                            for k in keys {
                                if let Some((_, sender)) = msg_handler.remove(&k) {
                                    let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(),
                                        format!("WebSocket internal error: {err}"))));
                                }
                            }
                        }
                        log::trace!("send message done");
                    }
                    _ = rx.changed() => {
                        let _= sender.send(Message::Close(None)).await;
                        let _ = sender.close().await;
                        log::trace!("close tmq sender");
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            let instant = Instant::now();
            'ws: loop {
                tokio::select! {
                    Some(message) = reader.next() => {
                        match message {
                            Ok(message) => match message {
                                Message::Text(text) => {
                                    log::trace!("json response: {}", text);
                                    let v: TmqRecv = serde_json::from_str(&text).expect(&text);
                                    let (req_id, recv, ok) = v.ok();
                                    match &recv {
                                        TmqRecvData::Subscribe => {
                                            log::trace!("subscribe with: {:?}", req_id);

                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                log::warn!("subscribe message received but no receiver alive");
                                            }
                                        },
                                        TmqRecvData::Unsubscribe => {
                                            log::trace!("unsubscribe with: {:?} successed", req_id);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                log::warn!("unsubscribe message received but no receiver alive");
                                            }
                                        },
                                        TmqRecvData::Poll(_) => {
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }
                                        },
                                        TmqRecvData::FetchJsonMeta { data }=> {
                                            log::trace!("fetch json meta data: {:?}", data);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::FetchRaw { meta: _ }=> {
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Commit=> {
                                            log::trace!("commit done: {:?}", recv);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Fetch(fetch)=> {
                                            log::trace!("fetch done: {:?}", fetch);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                log::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Assignment(assignment)=> {
                                            log::trace!("assignment done: {:?}", assignment);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                log::warn!("assignment message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Seek { timing }=> {
                                            log::trace!("seek done: req_id {:?} timing {:?}", &req_id, timing);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                log::warn!("seek message received but no receiver alive");
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
                                    let part = bytes.slice(24..);
                                    // dbg!(&bytes);
                                    use bytes::Buf;
                                    let timing = bytes.get_u64_le();
                                    let req_id = bytes.get_u64_le();
                                    let message_id = bytes.get_u64_le();


                                    log::trace!("[{:.2}ms] receive binary message with req_id {} message_id {}",
                                        Duration::from_nanos(timing).as_secs_f64() / 1000.,
                                        req_id, message_id);

                                    if let Some((_, sender)) = queries_sender.remove(&req_id)
                                    {
                                        sender.send(Ok(TmqRecvData::Bytes(part))).unwrap();
                                    }  else {
                                        log::warn!("poll message received but no receiver alive");
                                    }


                                }
                                Message::Close(close) => {
                                    log::warn!("websocket connection is closed (unexpected?)");

                                    let keys = queries_sender.iter().map(|r| *r.key()).collect_vec();
                                    let err = if let Some(close) = close {
                                        format!("WebSocket internal error: {}", close.reason)
                                    } else {
                                        "WebSocket internal error, connection is reset by server".to_string()
                                    };
                                    for k in keys {
                                        if let Some((_, sender)) = queries_sender.remove(&k) {
                                            let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(), err.clone())));
                                        }
                                    }
                                    break 'ws;
                                }
                                Message::Ping(bytes) => {
                                    ws2.send(Message::Pong(bytes)).await.unwrap();
                                }
                                Message::Pong(bytes) => {
                                    if bytes == PING {
                                        log::trace!("ping/pong handshake success");
                                    } else {
                                        // do nothing
                                        log::warn!("received (unexpected) pong message, do nothing");
                                    }
                                }
                                Message::Frame(frame) => {
                                    // do no`thing
                                    log::warn!("received (unexpected) frame message, do nothing");
                                    log::trace!("* frame data: {frame:?}");
                                }
                            },
                            Err(err) => {
                                let keys = queries_sender.iter().map(|r| *r.key()).collect_vec();
                                for k in keys {
                                    if let Some((_, sender)) = queries_sender.remove(&k) {
                                        let _ = sender.send(Err(RawError::new(
                                            WS_ERROR_NO::CONN_CLOSED.as_code(),
                                            format!("WebSocket internal error: {err}")
                                        )));
                                    }
                                }
                                break 'ws;
                            }
                        }
                    }
                    _ = close_listener.changed() => {
                        log::trace!("close reader task");
                        break 'ws;
                    }
                }
            }
            log::trace!("Consuming done in {:?}", instant.elapsed());
        });
        let consumer = Consumer {
            conn: self.info.to_conn_request(),
            tmq_conf: self.conf.clone(),
            sender: WsTmqSender {
                req_id: Arc::new(AtomicU64::new(1)),
                queries,
                sender: ws,
                timeout: Timeout::Duration(Duration::MAX),
            },
            // fetches,
            close_signal: tx,
            timeout: self.timeout,
            topics: vec![],
        };

        Ok(consumer)
    }
}

pub struct Consumer {
    conn: WsConnReq,
    tmq_conf: TmqInit,
    sender: WsTmqSender,
    close_signal: watch::Sender<bool>,
    timeout: Timeout,
    topics: Vec<String>,
}

impl Drop for Consumer {
    fn drop(&mut self) {
        let _ = self.close_signal.send(true);
    }
}

pub struct Offset {
    message_id: MessageId,
    database: String,
    topic: String,
    vgroup_id: i32,
}

impl IsOffset for Offset {
    fn database(&self) -> &str {
        &self.database
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn vgroup_id(&self) -> i32 {
        self.vgroup_id
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
    RecvTimeout(#[from] std::sync::mpsc::RecvTimeoutError),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("Deserialize json error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("{0}")]
    WsError(#[from] WsError),
    #[error("{0}")]
    TaosError(#[from] RawError),
    #[error("Receive timeout in {0}")]
    QueryTimeout(String),
}

unsafe impl Send for Error {}

unsafe impl Sync for Error {}

impl Error {
    pub const fn errno(&self) -> Code {
        match self {
            Error::TaosError(error) => error.code(),
            _ => Code::Failed,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{TaosBuilder, TmqBuilder};
    use taos_query::prelude::tokio;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_ws_tmq_meta() -> anyhow::Result<()> {
        use taos_query::prelude::*;
        // pretty_env_logger::formatted_builder()
        //     .filter_level(log::LevelFilter::Info)
        //     .init();

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?
            .build()
            .await?;
        taos.exec_many([
            "drop topic if exists ws_tmq_meta",
            "drop database if exists ws_tmq_meta",
            "create database ws_tmq_meta wal_retention_period 3600",
            "create topic ws_tmq_meta with meta as database ws_tmq_meta",
            "use ws_tmq_meta",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // // kind 9: drop normal table
            // "drop table `table`",
            // // kind 10: drop child table
            // "drop table `tb2` `tb1`",
            // // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists ws_tmq_meta2",
            "create database if not exists ws_tmq_meta2 wal_retention_period 3600",
            "use ws_tmq_meta2",
        ])
        .await?;

        let builder = TmqBuilder::new("taos://localhost:6041?group.id=10&timeout=5s")?;
        let mut consumer = builder.build_consumer().await?;
        consumer.subscribe(["ws_tmq_meta"]).await?;

        {
            let mut stream = consumer.stream();

            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                // 3. meta + data
                match message {
                    MessageSet::Meta(meta) => {
                        let _raw = meta.as_raw_meta().await?;
                        // taos.write_meta(raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = dbg!(json.to_string());
                        if let Err(err) = taos.exec(sql).await {
                            match err.errno() {
                                Code::TAG_ALREADY_EXIST => log::trace!("tag already exists"),
                                Code::TAG_NOT_EXIST => log::trace!("tag not exist"),
                                Code::COLUMN_EXISTS => log::trace!("column already exists"),
                                Code::COLUMN_NOT_EXIST => log::trace!("column not exists"),
                                Code::INVALID_COLUMN_NAME => log::trace!("invalid column name"),
                                Code::MODIFIED_ALREADY => log::trace!("modified already done"),
                                Code::TABLE_NOT_EXIST => log::trace!("table does not exists"),
                                Code::STABLE_NOT_EXIST => log::trace!("stable does not exists"),
                                _ => {
                                    panic!("{}", err);
                                }
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_block().await? {
                            dbg!(data.table_name());
                            dbg!(data);
                        }
                    }
                    _ => unreachable!(),
                }
                consumer.commit(offset).await?;
            }
        }
        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        taos.exec_many([
            "drop database ws_tmq_meta2",
            "drop topic ws_tmq_meta",
            "drop database ws_tmq_meta",
        ])
        .await?;
        Ok(())
    }

    #[test]
    fn test_ws_tmq_meta_sync() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        // pretty_env_logger::formatted_builder()
        //     .filter_level(log::LevelFilter::Debug)
        //     .init();

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?.build()?;
        taos.exec_many([
            "drop topic if exists ws_tmq_meta_sync",
            "drop database if exists ws_tmq_meta_sync",
            "create database ws_tmq_meta_sync wal_retention_period 3600",
            "create topic ws_tmq_meta_sync with meta as database ws_tmq_meta_sync",
            "use ws_tmq_meta_sync",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // kind 9: drop normal table
            "drop table `table`",
            // kind 10: drop child table
            "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            "drop table `stb2`",
            "drop table `stb1`",
        ])?;

        taos.exec_many([
            "drop database if exists ws_tmq_meta_sync2",
            "create database if not exists ws_tmq_meta_sync2 wal_retention_period 3600",
            "use ws_tmq_meta_sync2",
        ])?;

        let builder = TmqBuilder::new("taos://localhost:6041?group.id=10&timeout=1000ms")?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["ws_tmq_meta_sync"])?;

        let iter = consumer.iter_with_timeout(Timeout::from_secs(5));

        for msg in iter {
            let (offset, message) = msg?;
            // Offset contains information for topic name, database name and vgroup id,
            //  similar to kafka topic/partition/offset.
            let _ = offset.topic();
            let _ = offset.database();
            let _ = offset.vgroup_id();

            // Different to kafka message, TDengine consumer would consume two kind of messages.
            //
            // 1. meta
            // 2. data
            match message {
                MessageSet::Meta(meta) => {
                    let _raw = meta.as_raw_meta()?;
                    // taos.write_meta(raw)?;

                    // meta data can be write to an database seamlessly by raw or json (to sql).
                    let json = meta.as_json_meta()?;
                    let sql = dbg!(json.to_string());
                    if let Err(err) = taos.exec(sql) {
                        match err.errno() {
                            Code::TAG_ALREADY_EXIST => log::trace!("tag already exists"),
                            Code::TAG_NOT_EXIST => log::trace!("tag not exist"),
                            Code::COLUMN_EXISTS => log::trace!("column already exists"),
                            Code::COLUMN_NOT_EXIST => log::trace!("column not exists"),
                            Code::INVALID_COLUMN_NAME => log::trace!("invalid column name"),
                            Code::MODIFIED_ALREADY => log::trace!("modified already done"),
                            Code::TABLE_NOT_EXIST => log::trace!("table does not exists"),
                            Code::STABLE_NOT_EXIST => log::trace!("stable does not exists"),
                            _ => {
                                panic!("{}", err);
                            }
                        }
                    }
                }
                MessageSet::Data(data) => {
                    // data message may have more than one data block for various tables.
                    for block in data {
                        let block = block?;
                        dbg!(block.table_name());
                        dbg!(block);
                    }
                }
                _ => unreachable!(),
            }
            consumer.commit(offset)?;
        }
        consumer.unsubscribe();

        std::thread::sleep(Duration::from_secs(2));

        taos.exec_many([
            "drop database ws_tmq_meta_sync2",
            // "drop topic ws_tmq_meta_sync",
            "drop database ws_tmq_meta_sync",
        ])?;
        Ok(())
    }

    #[test]
    fn test_ws_tmq_metadata() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        // pretty_env_logger::formatted_builder()
        //     .filter_level(log::LevelFilter::Debug)
        //     .init();

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?.build()?;
        taos.exec_many([
            "drop topic if exists ws_tmq_meta_sync3",
            "drop database if exists ws_tmq_meta_sync3",
            "create database ws_tmq_meta_sync3 wal_retention_period 3600",
            "create topic ws_tmq_meta_sync3 with meta as database ws_tmq_meta_sync3",
            "use ws_tmq_meta_sync3",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // kind 9: drop normal table
            "drop table `table`",
            // kind 10: drop child table
            "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            "drop table `stb2`",
            "drop table `stb1`",
        ])?;

        taos.exec_many([
            "drop database if exists ws_tmq_meta_sync32",
            "create database if not exists ws_tmq_meta_sync32 wal_retention_period 3600",
            "use ws_tmq_meta_sync32",
        ])?;

        let builder = TmqBuilder::new("taos://localhost:6041?group.id=10&timeout=1000ms")?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["ws_tmq_meta_sync3"])?;

        let iter = consumer.iter_with_timeout(Timeout::from_secs(5));

        for msg in iter {
            let (offset, message) = msg?;
            // Offset contains information for topic name, database name and vgroup id,
            //  similar to kafka topic/partition/offset.
            let _ = offset.topic();
            let _ = offset.database();
            let _ = offset.vgroup_id();

            // Different to kafka message, TDengine consumer would consume two kind of messages.
            //
            // 1. meta
            // 2. data
            match message {
                MessageSet::Meta(meta) => {
                    let _raw = meta.as_raw_meta()?;
                    // taos.write_meta(raw)?;

                    // meta data can be write to an database seamlessly by raw or json (to sql).
                    let json = meta.as_json_meta()?;
                    let sql = dbg!(json.to_string());
                    if let Err(err) = taos.exec(sql) {
                        match err.errno() {
                            Code::TAG_ALREADY_EXIST => log::trace!("tag already exists"),
                            Code::TAG_NOT_EXIST => log::trace!("tag not exist"),
                            Code::COLUMN_EXISTS => log::trace!("column already exists"),
                            Code::COLUMN_NOT_EXIST => log::trace!("column not exists"),
                            Code::INVALID_COLUMN_NAME => log::trace!("invalid column name"),
                            Code::MODIFIED_ALREADY => log::trace!("modified already done"),
                            Code::TABLE_NOT_EXIST => log::trace!("table does not exists"),
                            Code::STABLE_NOT_EXIST => log::trace!("stable does not exists"),
                            _ => {
                                panic!("{}", err);
                            }
                        }
                    }
                }
                MessageSet::Data(data) => {
                    // data message may have more than one data block for various tables.
                    for block in data {
                        let block = block?;
                        dbg!(block.table_name());
                        dbg!(block);
                    }
                }
                _ => unreachable!(),
            }
            consumer.commit(offset)?;
        }
        consumer.unsubscribe();

        std::thread::sleep(Duration::from_secs(2));

        taos.exec_many([
            "drop database ws_tmq_meta_sync32",
            // "drop topic ws_tmq_meta_sync3",
            "drop database ws_tmq_meta_sync3",
        ])?;
        Ok(())
    }
}
