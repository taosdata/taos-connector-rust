//! TMQ consumer.
//!
mod messages;

use std::collections::VecDeque;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use dashmap::DashMap as HashMap;
use futures::{SinkExt, StreamExt};
use itertools::Itertools;
use messages::*;
use taos_query::common::{Field, JsonMeta, RawMeta};
use taos_query::prelude::{Code, RawError};
use taos_query::tmq::{
    AsAsyncConsumer, AsConsumer, Assignment, IsAsyncData, IsAsyncMeta, IsData, IsOffset,
    MessageSet, SyncOnAsync, Timeout, VGroupId,
};
use taos_query::util::{Edition, InlinableRead};
use taos_query::{DeError, DsnError, IntoDsn, RawBlock, RawResult, TBuilder};
use thiserror::Error;
use tokio::sync::{oneshot, watch, Mutex};
use tokio::time;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::Error as WsError;
use tracing::warn;

use crate::query::asyn::{is_support_binary_sql, WS_ERROR_NO};
use crate::query::infra::{ToMessage, WsConnReq, WsRecv, WsRecvData, WsSend};
use crate::TaosBuilder;

type WsSender = tokio::sync::mpsc::Sender<Message>;
type WsTmqAgent = Arc<HashMap<ReqId, oneshot::Sender<RawResult<TmqRecvData>>>>;

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

    async fn send_recv(&self, msg: TmqSend) -> RawResult<TmqRecvData> {
        self.send_recv_timeout(msg, Duration::MAX).await
    }

    async fn send_recv_timeout(&self, msg: TmqSend, timeout: Duration) -> RawResult<TmqRecvData> {
        let send_timeout = Duration::from_millis(5000);
        let req_id = msg.req_id();
        let (tx, rx) = oneshot::channel();

        self.queries.insert(req_id, tx);

        self.sender
            .send_timeout(msg.to_msg(), send_timeout)
            .await
            .map_err(WsTmqError::from)?;

        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);
        let data = tokio::select! {
            _ = &mut sleep, if !sleep.is_elapsed() => {
               tracing::trace!("poll timed out");
               Err(WsTmqError::QueryTimeout("poll".to_string()))?
            }
            message = rx => {
                message.map_err(WsTmqError::from)??
            }
        };
        Ok(data)
    }
}

#[derive(Debug)]
pub struct TmqBuilder {
    info: TaosBuilder,
    conf: TmqInit,
    timeout: Timeout,
    server_version: std::sync::OnceLock<String>,
}

impl TBuilder for TmqBuilder {
    type Target = Consumer;

    fn available_params() -> &'static [&'static str] {
        &["token", "timeout", "group.id", "client.id"]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::new(dsn)
    }

    fn client_version() -> &'static str {
        "0"
    }

    fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        Ok(())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> RawResult<Self::Target> {
        taos_query::block_in_place_or_global(self.build_consumer())
    }

    fn server_version(&self) -> RawResult<&str> {
        Ok("3.0.0.0")
    }

    fn is_enterprise_edition(&self) -> RawResult<bool> {
        todo!()
    }

    fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        if self
            .info
            .addr
            .matches(".cloud.tdengine.com")
            .next()
            .is_some()
            || self
                .info
                .addr
                .matches(".cloud.taosdata.com")
                .next()
                .is_some()
        {
            let edition = Edition::new("cloud", false);
            return Ok(edition);
        }

        let taos = TBuilder::build(&self.info)?;

        use taos_query::prelude::sync::Queryable;
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(&taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TmqBuilder {
    type Target = Consumer;

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::new(dsn)
    }

    fn client_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    async fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        Ok(())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> RawResult<Self::Target> {
        self.build_consumer().await
    }

    async fn server_version(&self) -> RawResult<&str> {
        match self.server_version.get().map(String::as_str) {
            Some(v) => Ok(v),
            None => {
                use taos_query::prelude::AsyncQueryable;

                let taos = taos_query::AsyncTBuilder::build(&self.info).await?;
                // Ensure server is ready.
                let version: Option<String> = taos.query_one("select server_version()").await?;
                if let Some(version) = version {
                    let _ = self.server_version.set(version);
                }
                self.server_version
                    .get()
                    .map(String::as_str)
                    .ok_or_else(|| RawError::from_string("Server version is unknown"))
            }
        }
    }

    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        taos_query::AsyncTBuilder::get_edition(self)
            .await
            .map(|edition| edition.is_enterprise_edition())
    }

    async fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        use taos_query::prelude::AsyncQueryable;

        let taos = taos_query::AsyncTBuilder::build(&self.info).await?;
        // Ensure server is ready.
        taos.exec("select server_version()").await?;

        if self
            .info
            .addr
            .matches(".cloud.tdengine.com")
            .next()
            .is_some()
            || self
                .info
                .addr
                .matches(".cloud.taosdata.com")
                .next()
                .is_some()
        {
            let edition = Edition::new("cloud", false);
            return Ok(edition);
        }

        let grant: RawResult<Option<(String, bool)>> = AsyncQueryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        )
        .await;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                AsyncQueryable::query_one(&taos, "show grants").await;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

#[derive(Debug)]
struct WsMessageBase {
    is_support_fetch_raw: bool,
    sender: WsTmqSender,
    message_id: MessageId,
    raw_blocks: Arc<Mutex<Option<VecDeque<RawBlock>>>>,
}

impl WsMessageBase {
    async fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        if self.is_support_fetch_raw {
            self.fetch_raw_block_new().await
        } else {
            self.fetch_raw_block_old().await
        }
    }

    async fn fetch_raw_block_new(&self) -> RawResult<Option<RawBlock>> {
        let raw_blocks_option = &mut *self.raw_blocks.lock().await;
        if let Some(raw_blocks) = raw_blocks_option {
            if !raw_blocks.is_empty() {
                return Ok(raw_blocks.pop_front());
            }
            return Ok(None);
        }

        let req_id = self.sender.req_id();
        let msg = TmqSend::FetchRawData(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;

        if let TmqRecvData::Bytes(bytes) = data {
            let raw = RawBlock::parse_from_multi_raw_block(bytes)
                .map_err(|_| RawError::from_string("parse multi raw blocks error!"))?;
            if !raw.is_empty() {
                raw_blocks_option.replace(raw);
                return Ok(raw_blocks_option.as_mut().unwrap().pop_front());
            }
        }
        Ok(None)
    }

    async fn fetch_raw_block_old(&self) -> RawResult<Option<RawBlock>> {
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
            raw.with_field_names(fetch.fields().iter().map(Field::name));
            if let Some(name) = fetch.table_name {
                raw.with_table_name(name);
            }
            return Ok(Some(raw));
        }
        todo!()
    }
    async fn fetch_json_meta(&self) -> RawResult<JsonMeta> {
        let req_id = self.sender.req_id();
        let msg = TmqSend::FetchJsonMeta(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        if let TmqRecvData::FetchJsonMeta { data } = data {
            let json: JsonMeta = serde_json::from_value(data).map_err(WsTmqError::from)?;
            return Ok(json);
        }
        unreachable!()
    }
    async fn fetch_raw_meta(&self) -> RawResult<RawMeta> {
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

#[derive(Debug)]
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
    async fn as_raw_meta(&self) -> RawResult<RawMeta> {
        self.0.fetch_raw_meta().await
    }

    async fn as_json_meta(&self) -> RawResult<JsonMeta> {
        self.0.fetch_json_meta().await
    }
}

impl SyncOnAsync for Meta {}

#[derive(Debug)]
pub struct Data(WsMessageBase);

impl Data {
    pub async fn fetch_block(&self) -> RawResult<Option<RawBlock>> {
        self.0.fetch_raw_block().await
    }
}

impl Iterator for Data {
    type Item = RawResult<RawBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        taos_query::block_in_place_or_global(self.fetch_block()).transpose()
    }
}

#[async_trait::async_trait]
impl IsAsyncData for Data {
    async fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        self.0
            .fetch_raw_meta()
            .await
            .map(|raw| unsafe { std::mem::transmute(raw) })
    }

    async fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        self.fetch_block().await
    }
}

impl IsData for Data {
    fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        taos_query::block_in_place_or_global(self.0.fetch_raw_meta())
            .map(|raw| unsafe { std::mem::transmute(raw) })
    }

    fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        taos_query::block_in_place_or_global(self.fetch_block())
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
    async fn poll_wait(&self) -> RawResult<(Offset, MessageSet<Meta, Data>)> {
        let elapsed = tokio::time::Instant::now();

        let is_in_polling =
            self.polling_mutex
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed);

        if is_in_polling.is_err() {
            if let Ok(data) = self.cache.recv_async().await {
                match data {
                    TmqRecvData::Poll(TmqPoll {
                        message_id,
                        database,
                        have_message,
                        topic,
                        vgroup_id,
                        message_type,
                        offset,
                        timing,
                    }) => {
                        if have_message {
                            let dur = elapsed.elapsed();
                            let offset = Offset {
                                message_id,
                                database,
                                topic,
                                vgroup_id,
                                offset,
                                timing,
                            };
                            let message = WsMessageBase {
                                is_support_fetch_raw: self.support_fetch_raw,
                                sender: self.sender.clone(),
                                message_id,
                                raw_blocks: Arc::new(Mutex::new(None)),
                            };

                            tracing::trace!("Got message in {}ms", dur.as_millis());

                            // Release the lock
                            self.polling_mutex.store(false, Ordering::Release);

                            return match message_type {
                                MessageType::Meta => Ok((offset, MessageSet::Meta(Meta(message)))),
                                MessageType::Data => Ok((offset, MessageSet::Data(Data(message)))),
                                MessageType::MetaData => Ok((
                                    offset,
                                    MessageSet::MetaData(
                                        Meta(message),
                                        Data(WsMessageBase {
                                            is_support_fetch_raw: self.support_fetch_raw,
                                            sender: self.sender.clone(),
                                            message_id,
                                            raw_blocks: Arc::new(Mutex::new(None)),
                                        }),
                                    ),
                                )),
                                MessageType::Invalid => unreachable!(),
                            };
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        loop {
            let req_id = self.sender.req_id();
            let action = TmqSend::Poll {
                req_id,
                blocking_time: 500,
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
                    offset,
                    timing,
                }) => {
                    if have_message {
                        let dur = elapsed.elapsed();
                        let offset = Offset {
                            message_id,
                            database,
                            topic,
                            vgroup_id,
                            offset,
                            timing,
                        };
                        let message = WsMessageBase {
                            is_support_fetch_raw: self.support_fetch_raw,
                            sender: self.sender.clone(),
                            message_id,
                            raw_blocks: Arc::new(Mutex::new(None)),
                        };

                        tracing::trace!("Got message in {}ms", dur.as_millis());

                        // Release the lock
                        self.polling_mutex.store(false, Ordering::Release);

                        break match message_type {
                            MessageType::Meta => Ok((offset, MessageSet::Meta(Meta(message)))),
                            MessageType::Data => Ok((offset, MessageSet::Data(Data(message)))),
                            MessageType::MetaData => Ok((
                                offset,
                                MessageSet::MetaData(
                                    Meta(message),
                                    Data(WsMessageBase {
                                        is_support_fetch_raw: self.support_fetch_raw,
                                        sender: self.sender.clone(),
                                        message_id,
                                        raw_blocks: Arc::new(Mutex::new(None)),
                                    }),
                                ),
                            )),
                            MessageType::Invalid => unreachable!(),
                        };
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    pub(crate) async fn poll_timeout(
        &self,
        timeout: Duration,
    ) -> RawResult<Option<(Offset, MessageSet<Meta, Data>)>> {
        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);
        tokio::select! {
            _ = &mut sleep, if !sleep.is_elapsed() => {
               Ok(None)
            }
            message = self.poll_wait() => {
                Ok(Some(message?))
            }
        }
    }
}

#[async_trait::async_trait]
impl AsAsyncConsumer for Consumer {
    type Offset = Offset;
    type Meta = Meta;
    type Data = Data;

    async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        self.topics = topics.into_iter().map(Into::into).collect_vec();
        let req_id = self.sender.req_id();
        let action = TmqSend::Subscribe {
            req_id,
            req: Box::new(self.tmq_conf.clone()),
            topics: self.topics.clone(),
            conn: self.conn.clone(),
        };
        if let Err(err) = self.sender.send_recv(action).await {
            tracing::error!("subscribe error: {:?}", err);
            if self.tmq_conf.enable_batch_meta.is_none() {
                return Err(err);
            }
            let code: i32 = err.code().into();

            if code & 0xFFFF == 0xFFFE {
                // subscribe conf error -2.
                let action = TmqSend::Subscribe {
                    req_id: self.sender.req_id(),
                    req: Box::new(self.tmq_conf.clone().disable_batch_meta()),
                    topics: self.topics.clone(),
                    conn: self.conn.clone(),
                };
                self.sender.send_recv(action).await?;
            } else {
                return Err(err);
            }
        }

        if let Some(offset) = self.tmq_conf.offset_seek.as_deref() {
            let offsets = offset
                .split(',')
                .map(|s| {
                    s.split(':')
                        .map(|i| i.parse::<i64>().unwrap())
                        .collect_vec()
                })
                .collect_vec();
            let topic_name = &self.topics[0];
            for offset in offsets {
                let vgroup_id = offset[0] as i32;
                let offset = offset[1];
                tracing::trace!(
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
        tracing::trace!("unsubscribe {} start", req_id);
        let action = TmqSend::Unsubscribe { req_id };
        if let Err(err) = self.sender.send_recv(action).await {
            tracing::warn!("unsubscribe error: {:?}", err);
        }
        drop(self);
    }

    async fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> RawResult<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
    > {
        match timeout {
            Timeout::Never | Timeout::None => self.poll_timeout(Duration::MAX).await,
            Timeout::Duration(timeout) => self.poll_timeout(timeout).await,
        }
    }

    async fn commit_all(&self) -> RawResult<()> {
        if self.polling_mutex.load(Ordering::Acquire) {
            return Err(RawError::from_string(
                "polling data is in queue, can't commit all",
            ));
        }
        let req_id = self.sender.req_id();
        let action = TmqSend::Commit(MessageArgs {
            req_id,
            message_id: 0,
        });

        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    async fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        if self.polling_mutex.load(Ordering::Acquire) {
            return Err(RawError::from_string(
                "polling data is in queue, can't commit current message",
            ));
        }
        let req_id = self.sender.req_id();
        let action = TmqSend::Commit(MessageArgs {
            req_id,
            message_id: offset.message_id,
        });

        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    async fn commit_offset(
        &self,
        topic_name: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
        let req_id = self.sender.req_id();
        let action = TmqSend::CommitOffset(OffsetSeekArgs {
            req_id,
            topic: topic_name.to_string(),
            vgroup_id,
            offset,
        });

        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    async fn list_topics(&self) -> RawResult<Vec<String>> {
        let topics = self.topics.clone();
        Ok(topics)
    }

    async fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        let topics = self.topics.clone();
        tracing::trace!("topics: {:?}", topics);

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
            Some(TmqRecvData::Assignment(TopicAssignment { assignment, timing })) => {
                tracing::trace!("timing: {:?}", timing);
                tracing::trace!("assignment: {:?}", assignment);
                assignment
            }
            _ => {
                vec![]
            }
        }
    }

    async fn offset_seek(
        &mut self,
        topic: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
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

    async fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        let req_id = self.sender.req_id();
        let action = TmqSend::Committed(OffsetArgs {
            req_id,
            topic_vgroup_ids: vec![OffsetInnerArgs {
                topic: topic.to_string(),
                vgroup_id,
            }],
        });

        let data = self.sender.send_recv(action).await?;
        if let TmqRecvData::Committed { committed } = data {
            let offset = committed[0];
            Ok(offset)
        } else {
            Ok(0)
        }
    }

    async fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        let req_id = self.sender.req_id();
        let action = TmqSend::Position(OffsetArgs {
            req_id,
            topic_vgroup_ids: vec![OffsetInnerArgs {
                topic: topic.to_string(),
                vgroup_id,
            }],
        });

        let data = self.sender.send_recv(action).await?;
        if let TmqRecvData::Position { position } = data {
            let offset = position[0];
            Ok(offset)
        } else {
            Ok(0)
        }
    }

    fn default_timeout(&self) -> Timeout {
        self.timeout
    }
}

impl AsConsumer for Consumer {
    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::subscribe(self, topics))
    }

    fn recv_timeout(
        &self,
        timeout: Timeout,
    ) -> RawResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::recv_timeout(
            self, timeout,
        ))
    }

    fn commit_all(&self) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::commit_all(self))
    }

    fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::commit(self, offset))
    }

    fn commit_offset(&self, topic_name: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::commit_offset(
            self, topic_name, vgroup_id, offset,
        ))
    }

    fn list_topics(&self) -> RawResult<Vec<String>> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::list_topics(self))
    }

    fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::assignments(self))
    }

    fn offset_seek(&mut self, topic: &str, vg_id: VGroupId, offset: i64) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::offset_seek(
            self, topic, vg_id, offset,
        ))
    }

    fn committed(&self, topic: &str, vg_id: VGroupId) -> RawResult<i64> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::committed(
            self, topic, vg_id,
        ))
    }

    fn position(&self, topic: &str, vg_id: VGroupId) -> RawResult<i64> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::position(
            self, topic, vg_id,
        ))
    }

    fn unsubscribe(self) {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::unsubscribe(self));
    }
}

impl TmqBuilder {
    pub fn new<D: IntoDsn>(dsn: D) -> RawResult<Self> {
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
            .map_or("false".to_string(), |s| {
                if s.is_empty() {
                    "false".to_string()
                } else {
                    s.to_string()
                }
            });
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
            .or_else(|| dsn.params.get("snapshot"))
            .and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            })
            .unwrap_or("false".to_string());
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
            Timeout::from_str(timeout).map_err(RawError::from_any)?
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
        let enable_batch_meta = dsn
            .get("msg.enable.batchmeta")
            .or_else(|| dsn.params.get("batchmeta"))
            .or_else(|| dsn.params.get("enable.batch.meta"))
            .map(|s| {
                let s = s.trim();
                if s.is_empty() {
                    "1".to_string()
                } else {
                    s.to_string()
                }
            })
            .or_else(|| Some("1".to_string()))
            .and_then(|s| {
                if matches!(
                    s.as_str(),
                    "0" | "F" | "false" | "FALSE" | "no" | "N" | "NO"
                ) {
                    None
                } else {
                    Some(s)
                }
            });
        let msg_consume_excluded = dsn
            .get("msg.consume.excluded")
            .or_else(|| dsn.params.get("replica"))
            .map(|s| {
                let s = s.trim();
                if s.is_empty() {
                    "1".to_string()
                } else {
                    s.to_string()
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
            enable_batch_meta,
            msg_consume_excluded,
        };

        Ok(Self {
            info,
            conf,
            timeout,
            server_version: std::sync::OnceLock::new(),
        })
    }

    async fn build_consumer(&self) -> RawResult<Consumer> {
        let url = self.info.to_tmq_url();

        let ws = self
            .info
            .build_stream_opt(self.info.to_tmq_url(), false)
            .await?;

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
        };

        let version = match tokio::time::timeout(duration, version_future).await {
            Ok(Ok(version)) => version,
            Ok(Err(err)) => {
                return Err(RawError::any(err).context("Version fetching error"));
            }
            Err(_) => "2.x".to_string(),
        };

        let queries = Arc::new(HashMap::<ReqId, tokio::sync::oneshot::Sender<_>>::new());

        let queries_sender = queries.clone();
        let msg_handler = queries.clone();

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel::<Message>(100);
        let ws2 = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let mut close_listener = rx.clone();

        let sending_url = url.clone();
        static PING_INTERVAL: u64 = 29;
        const PING: &[u8] = b"TAOSX";

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(PING_INTERVAL));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        tracing::trace!("Check websocket message sender alive");
                        if let Err(err) = sender.send(Message::Ping(PING.to_vec())).await {
                            tracing::trace!("sending ping message to {sending_url} error: {err:?}");
                            let keys = msg_handler.iter().map(|r| *r.key()).collect_vec();
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
                        tracing::trace!("send message {msg:?}");
                        if let Err(err) = sender.send(msg).await {
                            tracing::trace!("sending message to {sending_url} error: {err:?}");
                            let keys = msg_handler.iter().map(|r| *r.key()).collect_vec();
                            for k in keys {
                                if let Some((_, sender)) = msg_handler.remove(&k) {
                                    let _ = sender.send(Err(RawError::new(WS_ERROR_NO::CONN_CLOSED.as_code(),
                                        format!("WebSocket internal error: {err}"))));
                                }
                            }
                        }
                        tracing::trace!("send message done");
                    }
                    _ = rx.changed() => {
                        let _= sender.send(Message::Close(None)).await;
                        let _ = sender.close().await;
                        tracing::trace!("close tmq sender");
                        break;
                    }
                }
            }
        });

        let (cache_tx, cache_rx) = flume::bounded(1);
        let polling_mutex = Arc::new(AtomicBool::new(false));
        let polling_mutex2 = polling_mutex.clone();

        tokio::spawn(async move {
            let instant = Instant::now();
            'ws: loop {
                tokio::select! {
                    Some(message) = reader.next() => {
                        match message {
                            Ok(message) => match message {
                                Message::Text(text) => {
                                    tracing::trace!("json response: {}", text);
                                    let v: TmqRecv = serde_json::from_str(&text).expect(&text);
                                    let (req_id, recv, ok) = v.ok();
                                    match &recv {
                                        TmqRecvData::Subscribe => {
                                            tracing::trace!("subscribe with: {:?}", req_id);

                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                // We don't care about the result of the sender for subscribe
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                tracing::warn!("subscribe message received but no receiver alive");
                                            }
                                        },
                                        TmqRecvData::Unsubscribe => {
                                            tracing::trace!("unsubscribe with: {:?} success", req_id);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                // We don't care about the result of the sender for unsubscribe
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                tracing::warn!("unsubscribe message received but no receiver alive");
                                            }
                                        },
                                        TmqRecvData::Poll(_) => {
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
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
                                                if let Err(Ok(data)) = sender.send(ok.map(|_|recv)) {
                                                    tracing::warn!(req_id, kind = "poll", "poll message received but no receiver alive: {:?}", data);
                                                    if let TmqRecvData::Poll(TmqPoll {have_message, ..}) = &data {
                                                        if !have_message {
                                                            polling_mutex2.store(false, Ordering::Release);
                                                            continue;
                                                        }
                                                    }

                                                    if let Err(err) = cache_tx.send(data) {
                                                        tracing::error!(req_id, %err, kind = "poll", "poll message received but no receiver alive, message may lost, break the connection");

                                                        let keys = queries_sender.iter().map(|r| *r.key()).collect_vec();
                                                        for k in keys {
                                                            if let Some((_, sender)) = queries_sender.remove(&k) {
                                                                let _ = sender.send(Err(RawError::new(
                                                                    WS_ERROR_NO::CONN_CLOSED.as_code(),
                                                                    "Consumer messages lost",
                                                                )));
                                                            }
                                                        }
                                                        break 'ws;
                                                    }
                                                    polling_mutex2.store(true, Ordering::Release);
                                                }
                                            }  else {
                                                if let TmqRecvData::Poll(TmqPoll {have_message, ..}) = &recv {
                                                    if !have_message {
                                                        polling_mutex2.store(false, Ordering::Release);
                                                        continue;
                                                    }
                                                }
                                                if let Err(err) = cache_tx.send(recv) {
                                                    tracing::error!(req_id, %err, kind = "poll", "poll message received but no receiver alive, message may lost, break the connection");

                                                    let keys = queries_sender.iter().map(|r| *r.key()).collect_vec();
                                                    for k in keys {
                                                        if let Some((_, sender)) = queries_sender.remove(&k) {
                                                            let _ = sender.send(Err(RawError::new(
                                                                WS_ERROR_NO::CONN_CLOSED.as_code(),
                                                                "Consumer messages lost",
                                                            )));
                                                        }
                                                    }
                                                    break 'ws;
                                                }
                                                polling_mutex2.store(true, Ordering::Release);
                                            }
                                        },
                                        TmqRecvData::FetchJsonMeta { data }=> {
                                            tracing::trace!("fetch json meta data: {:?}", data);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                if let Err(err) = sender.send(ok.map(|_|recv)) {
                                                    tracing::warn!(req_id, kind = "fetch_json_meta", "fetch json meta message received but no receiver alive: {:?}", err);
                                                }
                                            }  else {
                                                tracing::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::FetchRaw { .. }=> {
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                if let Err(err) = sender.send(ok.map(|_|recv)) {
                                                    tracing::warn!(req_id, kind = "fetch_raw_meta", "fetch raw meta message received but no receiver alive: {:?}", err);
                                                }
                                            }  else {
                                                tracing::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Commit=> {
                                            tracing::trace!("commit done: {:?}", recv);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                // We don't care about the result of the sender for commit
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                tracing::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Fetch(fetch)=> {
                                            tracing::trace!("fetch done: {:?}", fetch);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                // We don't care about the result of the sender for fetch
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                tracing::warn!("poll message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::FetchBlock{ .. }=> {
                                            if let Some((_, sender)) = queries_sender.remove(&req_id) {
                                                let _ = sender.send(Err(RawError::new(
                                                    WS_ERROR_NO::WEBSOCKET_ERROR.as_code(),
                                                    format!("WebSocket internal error: {:?}", &text)
                                                )));
                                            }
                                            break 'ws;
                                        }
                                        TmqRecvData::Assignment(assignment)=> {
                                            tracing::trace!("assignment done: {:?}", assignment);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                tracing::warn!("assignment message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Seek { timing }=> {
                                            tracing::trace!("seek done: req_id {:?} timing {:?}", &req_id, timing);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                tracing::warn!("seek message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Committed { committed }=> {
                                            tracing::trace!("committed done: {:?}", committed);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                tracing::warn!("committed message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::Position { position }=> {
                                            tracing::trace!("position done: {:?}", position);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                let _ = sender.send(ok.map(|_|recv));
                                            }  else {
                                                tracing::warn!("position message received but no receiver alive");
                                            }
                                        }
                                        TmqRecvData::CommitOffset { timing }=> {
                                            tracing::trace!("commit offset done: {:?}", timing);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id) {
                                                let _ = sender.send(ok.map(|_|recv));
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

                                    if let Some((_, sender)) = queries_sender.remove(&req_id) {
                                        tracing::trace!("send data to fetches with id {}", req_id);
                                        if sender.send(Ok(TmqRecvData::Bytes(part.into()))).is_err() {
                                            tracing::warn!(req_id, kind = "binary", "req_id {req_id} not detected, message might be lost");
                                        }
                                    } else {
                                        tracing::warn!("req_id {req_id} not detected, message might be lost");
                                    }
                                }
                                Message::Close(close) => {
                                    tracing::warn!("websocket connection is closed (unexpected?)");

                                    let keys = queries_sender.iter().map(|r| *r.key()).collect_vec();
                                    let err = if let Some(close) = close {
                                        format!("WebSocket internal error: {close}")
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
                                    let _ = ws2.send(Message::Pong(bytes)).await;
                                }
                                Message::Pong(bytes) => {
                                    if bytes == PING {
                                        tracing::trace!("ping/pong handshake success");
                                    } else {
                                        // do nothing
                                        tracing::warn!("received (unexpected) pong message, do nothing");
                                    }
                                }
                                Message::Frame(frame) => {
                                    // do nothing
                                    tracing::warn!("received (unexpected) frame message, do nothing");
                                    tracing::trace!("* frame data: {frame:?}");
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
                        tracing::trace!("close reader task");
                        break 'ws;
                    }
                }
            }
            tracing::trace!("Consuming done in {:?}", instant.elapsed());
        });

        Ok(Consumer {
            conn: self.info.to_conn_request(),
            tmq_conf: self.conf.clone(),
            sender: WsTmqSender {
                req_id: Arc::new(AtomicU64::new(1)),
                queries,
                sender: ws,
                timeout: Timeout::Duration(Duration::MAX),
            },
            close_signal: tx,
            timeout: self.timeout,
            topics: vec![],
            support_fetch_raw: is_support_binary_sql(&version),
            polling_mutex,
            cache: cache_rx,
        })
    }
}

#[derive(Debug)]
pub struct Consumer {
    conn: WsConnReq,
    tmq_conf: TmqInit,
    sender: WsTmqSender,
    close_signal: watch::Sender<bool>,
    timeout: Timeout,
    topics: Vec<String>,
    support_fetch_raw: bool,
    /// Polling, if true, the consumer will wait for the message to be consumed.
    polling_mutex: Arc<AtomicBool>,
    cache: flume::Receiver<TmqRecvData>,
}

impl Drop for Consumer {
    fn drop(&mut self) {
        let _ = self.close_signal.send(true);
    }
}

#[derive(Debug, Clone)]
pub struct Offset {
    message_id: MessageId,
    database: String,
    topic: String,
    vgroup_id: i32,
    offset: i64,
    timing: i64,
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

    fn offset(&self) -> i64 {
        self.offset
    }

    fn timing(&self) -> i64 {
        self.timing
    }
}

#[derive(Debug, Error)]
pub enum WsTmqError {
    #[error("{0}")]
    Dsn(#[from] DsnError),
    #[error("{0}")]
    FetchError(#[from] oneshot::error::RecvError),
    #[error("{0}")]
    Send2Error(#[from] tokio::sync::mpsc::error::SendError<Message>),
    #[error(transparent)]
    SendTimeoutError(#[from] tokio::sync::mpsc::error::SendTimeoutError<Message>),
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

unsafe impl Send for WsTmqError {}

unsafe impl Sync for WsTmqError {}

impl WsTmqError {
    pub const fn errno(&self) -> Code {
        match self {
            WsTmqError::TaosError(error) => error.code(),
            _ => Code::FAILED,
        }
    }
    pub fn errstr(&self) -> String {
        match self {
            WsTmqError::TaosError(error) => error.message(),
            _ => format!("{self}"),
        }
    }
}

impl From<WsTmqError> for RawError {
    fn from(value: WsTmqError) -> Self {
        match value {
            WsTmqError::TaosError(error) => error,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{TaosBuilder, TmqBuilder};

    #[tokio::test]
    async fn test_ws_tmq_meta_batch() -> anyhow::Result<()> {
        use taos_query::prelude::{AsyncTBuilder, *};

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::DEBUG)
            .compact()
            .try_init();

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists ws_tmq_meta_batch",
            "drop database if exists ws_tmq_meta_batch",
            "create database ws_tmq_meta_batch wal_retention_period 3600",
            "create topic ws_tmq_meta_batch with meta as database ws_tmq_meta_batch",
            "use ws_tmq_meta_batch",
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
        ])
        .await?;

        taos.exec_many([
            "drop database if exists ws_tmq_meta_batch2",
            "create database if not exists ws_tmq_meta_batch2 wal_retention_period 3600",
            "use ws_tmq_meta_batch2",
        ])
        .await?;

        let builder = TmqBuilder::new(
            "taos://localhost:6041?group.id=10&timeout=5s&auto.offset.reset=earliest&msg.enable.batchmeta=true&experimental.snapshot.enable=true",
        )?;

        let _ = dbg!(builder.get_edition().await);
        let _ = dbg!(builder.server_version().await);
        assert!(builder.ready().await);
        let mut consumer = builder.build_consumer().await?;
        consumer.subscribe(["ws_tmq_meta_batch"]).await?;

        {
            let mut stream = consumer.stream();

            let mut count = 0;
            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();
                count += 1;

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
                        tracing::info!(count, batch.size = json.iter().len(), "{json:?}");
                        for meta in &json {
                            let sql = meta.to_string();
                            tracing::debug!(count, "sql: {}", sql);
                            if let Err(err) = taos.exec(sql).await {
                                match err.code() {
                                    Code::TAG_ALREADY_EXIST => {
                                        tracing::trace!("tag already exists")
                                    }
                                    Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                    Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                    Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                    Code::INVALID_COLUMN_NAME => {
                                        tracing::trace!("invalid column name")
                                    }
                                    Code::MODIFIED_ALREADY => {
                                        tracing::trace!("modified already done")
                                    }
                                    Code::TABLE_NOT_EXIST => {
                                        tracing::trace!("table does not exists")
                                    }
                                    Code::STABLE_NOT_EXIST => {
                                        tracing::trace!("stable does not exists")
                                    }
                                    _ => tracing::error!(count, "{}", err),
                                }
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(_data) = data.fetch_block().await? {}
                    }
                    _ => unreachable!(),
                }
                consumer.commit(offset).await?;
            }
        }

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        taos.exec_many([
            "drop database ws_tmq_meta_batch2",
            "drop topic ws_tmq_meta_batch",
            "drop database ws_tmq_meta_batch",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ws_tmq_meta() -> anyhow::Result<()> {
        use taos_query::prelude::*;
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::DEBUG)
            .compact()
            .try_init();

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

        let builder = TmqBuilder::new(
            "taos://localhost:6041?group.id=10&timeout=5s&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build_consumer().await?;
        consumer.subscribe(["ws_tmq_meta"]).await?;

        {
            let mut stream = consumer.stream();

            let mut count = 0;
            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();
                count += 1;

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
                        for meta in &json {
                            let sql = meta.to_string();
                            tracing::debug!("sql: {}", sql);
                            if let Err(err) = taos.exec(sql).await {
                                match err.code() {
                                    Code::TAG_ALREADY_EXIST => {
                                        tracing::trace!("tag already exists")
                                    }
                                    Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                    Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                    Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                    Code::INVALID_COLUMN_NAME => {
                                        tracing::trace!("invalid column name")
                                    }
                                    Code::MODIFIED_ALREADY => {
                                        tracing::trace!("modified already done")
                                    }
                                    Code::TABLE_NOT_EXIST => {
                                        tracing::trace!("table does not exists")
                                    }
                                    Code::STABLE_NOT_EXIST => {
                                        tracing::trace!("stable does not exists")
                                    }
                                    _ => tracing::error!(count, "{}", err),
                                }
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(_data) = data.fetch_block().await? {}
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

        let builder = TmqBuilder::new(
            "taos://localhost:6041?group.id=10&timeout=1000ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["ws_tmq_meta_sync"])?;

        let topics = consumer.list_topics();
        tracing::debug!("topics: {:?}", topics);

        let iter = consumer.iter_with_timeout(Timeout::from_secs(1));

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
                    for meta in &json {
                        let sql = meta.to_string();
                        tracing::debug!("sql: {}", sql);
                        if let Err(err) = taos.exec(sql) {
                            match err.code() {
                                Code::TAG_ALREADY_EXIST => tracing::trace!("tag already exists"),
                                Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                Code::INVALID_COLUMN_NAME => tracing::trace!("invalid column name"),
                                Code::MODIFIED_ALREADY => tracing::trace!("modified already done"),
                                Code::TABLE_NOT_EXIST => tracing::trace!("table does not exists"),
                                Code::STABLE_NOT_EXIST => tracing::trace!("stable does not exists"),
                                _ => {
                                    tracing::error!("{}", err);
                                }
                            }
                        }
                    }
                }
                MessageSet::Data(data) => {
                    // data message may have more than one data block for various tables.
                    for block in data {
                        let _block = block?;
                        // dbg!(block.table_name());
                        // dbg!(block);
                    }
                }
                _ => unreachable!(),
            }
            consumer.commit(offset)?;
        }

        // get assignments
        let assignments = consumer.assignments();
        tracing::debug!("assignments all: {:?}", assignments);

        if let Some(assignments) = assignments {
            for (topic, assignment_vec) in assignments {
                for assignment in assignment_vec {
                    tracing::debug!("assignment: {:?} {:?}", topic, assignment);
                    let vgroup_id = assignment.vgroup_id();
                    let end = assignment.end();

                    let position = consumer.position(&topic, vgroup_id);
                    tracing::debug!("position: {:?}", position);
                    let committed = consumer.committed(&topic, vgroup_id);
                    tracing::debug!("committed: {:?}", committed);

                    let res = consumer.offset_seek(&topic, vgroup_id, end);
                    tracing::debug!("seek: {:?}", res);

                    let position = consumer.position(&topic, vgroup_id);
                    tracing::debug!("after seek position: {:?}", position);
                    let committed = consumer.committed(&topic, vgroup_id);
                    tracing::debug!("after seek committed: {:?}", committed);

                    let res = consumer.commit_offset(&topic, vgroup_id, end);
                    tracing::debug!("commit offset: {:?}", res);

                    let position = consumer.position(&topic, vgroup_id);
                    tracing::debug!("after commit offset position: {:?}", position);
                    let committed = consumer.committed(&topic, vgroup_id);
                    tracing::debug!("after commit offset committed: {:?}", committed);
                }
            }
        }

        consumer.unsubscribe();

        std::thread::sleep(Duration::from_secs(5));

        taos.exec_many([
            "drop database ws_tmq_meta_sync2",
            "drop topic ws_tmq_meta_sync",
            "drop database ws_tmq_meta_sync",
        ])?;
        Ok(())
    }

    #[test]
    fn test_ws_tmq_metadata() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

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

        let builder = TmqBuilder::new(
            "taos://localhost:6041?group.id=10&timeout=1000ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["ws_tmq_meta_sync3"])?;

        let iter = consumer.iter_with_timeout(Timeout::from_secs(1));

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
                    for json in &json {
                        let sql = json.to_string();
                        tracing::debug!("sql: {}", sql);
                        if let Err(err) = taos.exec(sql) {
                            match err.code() {
                                Code::TAG_ALREADY_EXIST => tracing::trace!("tag already exists"),
                                Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                Code::INVALID_COLUMN_NAME => tracing::trace!("invalid column name"),
                                Code::MODIFIED_ALREADY => tracing::trace!("modified already done"),
                                Code::TABLE_NOT_EXIST => tracing::trace!("table does not exists"),
                                Code::STABLE_NOT_EXIST => tracing::trace!("stable does not exists"),
                                _ => {
                                    tracing::error!("{}", err);
                                }
                            }
                        }
                    }
                }
                MessageSet::Data(data) => {
                    // data message may have more than one data block for various tables.
                    for block in data {
                        let _block = block?;
                        // dbg!(block.table_name());
                        // dbg!(block);
                    }
                }
                _ => unreachable!(),
            }
            consumer.commit(offset)?;
        }
        consumer.unsubscribe();

        std::thread::sleep(Duration::from_secs(5));

        taos.exec_many([
            "drop database ws_tmq_meta_sync32",
            "drop topic ws_tmq_meta_sync3",
            "drop database ws_tmq_meta_sync3",
        ])?;
        Ok(())
    }

    #[cfg(feature = "rustls")]
    #[tokio::test]
    async fn test_consumer_cloud() -> anyhow::Result<()> {
        use taos_query::prelude::*;
        std::env::set_var("RUST_LOG", "debug");

        let dsn = std::env::var("TDENGINE_ClOUD_DSN");
        if dsn.is_err() {
            println!("Skip test when not in cloud");
            return Ok(());
        }
        let dsn = dsn.unwrap();

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;
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
        ])
        .await?;

        taos.exec_many([
            "drop database if exists ws_tmq_meta2",
            "create database if not exists ws_tmq_meta2 wal_retention_period 3600",
            "use ws_tmq_meta2",
        ])
        .await?;

        let builder = TmqBuilder::new(&dsn)?;
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

                match message {
                    MessageSet::Meta(meta) => {
                        let _raw = meta.as_raw_meta().await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        for meta in &json {
                            let sql = meta.to_string();
                            tracing::debug!("sql: {}", sql);
                            if let Err(err) = taos.exec(sql).await {
                                match err.code() {
                                    Code::TAG_ALREADY_EXIST => {
                                        tracing::trace!("tag already exists")
                                    }
                                    Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                    Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                    Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                    Code::INVALID_COLUMN_NAME => {
                                        tracing::trace!("invalid column name")
                                    }
                                    Code::MODIFIED_ALREADY => {
                                        tracing::trace!("modified already done")
                                    }
                                    Code::TABLE_NOT_EXIST => {
                                        tracing::trace!("table does not exists")
                                    }
                                    Code::STABLE_NOT_EXIST => {
                                        tracing::trace!("stable does not exists")
                                    }
                                    _ => tracing::error!("{}", err),
                                }
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(_data) = data.fetch_block().await? {}
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

    #[cfg(feature = "rustls")]
    #[cfg(feature = "rustls-aws-lc-crypto-provider")]
    #[tokio::test]
    async fn test_consumer_cloud_conn() -> anyhow::Result<()> {
        use std::env;

        use taos_query::prelude::*;

        std::env::set_var("RUST_LOG", "trace");

        let dsn = env::var("TDENGINE_CLOUD_DSN")
            .expect("TDENGINE_CLOUD_DSN environment variable not set");

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        let r = taos.server_version().await?;

        tracing::info!("server version: {}", r);

        Ok(())
    }

    #[tokio::test]
    async fn test_ws_tmq_poll_lost() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .compact()
            .try_init();

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists test_ws_tmq_poll_lost",
            "drop database if exists test_ws_tmq_poll_lost",
            "create database test_ws_tmq_poll_lost wal_retention_period 3600",
            "create topic test_ws_tmq_poll_lost with meta as database test_ws_tmq_poll_lost",
            "use test_ws_tmq_poll_lost",
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
            "insert into tb1 using stb1 tags(NULL) values(now, NULL, NULL, NULL, NULL, NULL,
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
        ])
        .await?;

        taos.exec_many([
            "drop database if exists test_ws_tmq_poll_lost2",
            "create database if not exists test_ws_tmq_poll_lost2 wal_retention_period 3600",
            "use test_ws_tmq_poll_lost2",
        ])
        .await?;

        let builder = TmqBuilder::new(
            "taos://localhost:6041?group.id=10&timeout=200ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build_consumer().await?;
        consumer.subscribe(["test_ws_tmq_poll_lost"]).await?;
        std::env::set_var("TEST_POLLING_LOST", "1");

        {
            let sleep = tokio::time::sleep(Duration::from_millis(400));
            tokio::pin!(sleep);
            let _ = tokio::select! {
                res = consumer.recv_timeout(Timeout::Duration(Duration::from_millis(400))) => {
                    res
                }
                _ = &mut sleep => {
                    println!("sleep");
                    Ok(None)
                }
            }
            .inspect_err(|err| {
                println!("err: {:?}", err);
            })?;

            let _ = tokio::select! {
                res = consumer.recv_timeout(Timeout::Duration(Duration::from_millis(400))) => {
                    res
                }
                _ = &mut sleep => {
                    println!("sleep");
                    Ok(None)
                }
            }
            .inspect_err(|err| {
                println!("err: {:?}", err);
            })?;

            let _ = tokio::select! {
                res = consumer.recv_timeout(Timeout::Duration(Duration::from_millis(400))) => {
                    res
                }
                _ = &mut sleep => {
                    println!("sleep");
                    Ok(None)
                }
            }
            .inspect_err(|err| {
                println!("err: {:?}", err);
            })?;

            let mut stream = consumer.stream();

            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                // similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();

                match message {
                    MessageSet::Meta(meta) => {
                        let _raw = meta.as_raw_meta().await?;
                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        for meta in &json {
                            let sql = meta.to_string();
                            tracing::debug!("sql: {}", sql);
                        }
                    }
                    MessageSet::Data(data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(block) = data.fetch_block().await? {
                            println!("{}", block.pretty_format());
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
            "drop database test_ws_tmq_poll_lost2",
            "drop topic test_ws_tmq_poll_lost",
            "drop database test_ws_tmq_poll_lost",
        ])
        .await?;

        Ok(())
    }
}
