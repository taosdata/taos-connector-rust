// pub(crate) mod ffi;

use std::{fmt::Debug, str::FromStr, sync::Arc, time::Duration};

// pub(crate) use ffi::*;

use anyhow::Context;
use itertools::Itertools;
use taos_query::{
    common::{raw_data_t, RawMeta},
    prelude::{tokio::time, RawError, RawResult},
    tmq::{
        AsAsyncConsumer, AsConsumer, Assignment, AsyncOnSync, IsAsyncData, IsData, IsMeta,
        IsOffset, MessageSet, Timeout, VGroupId,
    },
    util::Edition,
    Dsn, IntoDsn, RawBlock,
};

use crate::{raw::ApiEntry, raw::RawRes, types::tmq_res_t, TaosBuilder};

// use taos_error::Error;

mod raw;

use raw::RawTmq;

use self::raw::{Conf, Topics};

#[derive(Debug)]
pub struct TmqBuilder {
    dsn: Dsn,
    builder: TaosBuilder,
    lib: Arc<ApiEntry>,
    conf: Conf,
    timeout: Timeout,
}

unsafe impl Send for TmqBuilder {}
unsafe impl Sync for TmqBuilder {}

impl taos_query::TBuilder for TmqBuilder {
    type Target = Consumer;

    fn available_params() -> &'static [&'static str] {
        &["group.id", "client.id", "timeout", "enable.auto.commit"]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        let mut dsn = dsn
            .into_dsn()
            .map_err(|e| RawError::from_string(format!("Parse dsn error: {}", e)))?;
        let lib = if let Some(path) = dsn.params.remove("libraryPath") {
            ApiEntry::dlopen(path).map_err(|err| taos_query::RawError::any(err))?
        } else {
            ApiEntry::open_default().map_err(|err| taos_query::RawError::any(err))?
        };
        let conf = Conf::from_dsn(&dsn, lib.tmq.unwrap().conf_api)?;
        let timeout = if let Some(timeout) = dsn.params.remove("timeout") {
            Timeout::from_str(&timeout).map_err(RawError::from_any)?
        } else {
            Timeout::from_millis(500)
        };
        Ok(Self {
            builder: TaosBuilder::from_dsn(&dsn).map_err(RawError::from_any)?,
            dsn,
            lib: Arc::new(lib),
            conf,
            timeout,
        })
    }

    fn client_version() -> &'static str {
        ""
    }

    fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        self.build().map(|_| ())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> RawResult<Self::Target> {
        let ptr = self.conf.build()?;
        let tmq = RawTmq {
            c: self.lib.clone(),
            tmq: self.lib.tmq.unwrap(),
            ptr,
        };
        Ok(Consumer {
            tmq,
            timeout: self.timeout,
            dsn: self.dsn.clone(),
        })
    }

    fn server_version(&self) -> RawResult<&str> {
        self.builder.server_version().map_err(RawError::from_any)
    }

    fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        let taos = self.builder.inner_connection()?;
        use taos_query::prelude::sync::Queryable;
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            taos,
            "select version, (expire_time < now) as valid from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                tracing::warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
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
        let mut dsn = dsn
            .into_dsn()
            .map_err(|e| RawError::from_string(format!("Parse dsn error: {}", e)))?;
        let lib = if let Some(path) = dsn.params.remove("libraryPath") {
            ApiEntry::dlopen(path).map_err(|err| taos_query::RawError::any(err))?
        } else {
            ApiEntry::open_default().map_err(|err| taos_query::RawError::any(err))?
        };
        let conf = Conf::from_dsn(&dsn, lib.tmq.unwrap().conf_api)?;
        let timeout = if let Some(timeout) = dsn.params.remove("timeout") {
            Timeout::from_str(&timeout).map_err(RawError::from_any)?
        } else {
            Timeout::from_millis(500)
        };
        Ok(Self {
            builder: TaosBuilder::from_dsn(&dsn).map_err(RawError::from_any)?,
            dsn,
            lib: Arc::new(lib),
            conf,
            timeout,
        })
    }

    fn client_version() -> &'static str {
        ""
    }

    async fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        self.build().await.map(|_| ())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> RawResult<Self::Target> {
        let ptr = self.conf.build()?;
        let tmq = RawTmq {
            c: self.lib.clone(),
            tmq: self.lib.tmq.unwrap(),
            ptr,
        };
        Ok(Consumer {
            tmq,
            timeout: self.timeout,
            dsn: self.dsn.clone(),
        })
    }

    async fn server_version(&self) -> RawResult<&str> {
        self.builder
            .server_version()
            .await
            .map_err(RawError::from_any)
    }

    async fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        let taos = self.builder.inner_connection()?;
        use taos_query::prelude::AsyncQueryable;

        // the latest version of 3.x should work
        let grant: RawResult<Option<(String, bool)>> = time::timeout(
            Duration::from_secs(60),
            AsyncQueryable::query_one(
                taos,
                "select version, (expire_time < now) as valid from information_schema.ins_cluster",
            ),
        )
        .await
        .context("Check cluster edition timeout")?;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> = time::timeout(
                Duration::from_secs(60),
                AsyncQueryable::query_one(taos, "show grants"),
            )
            .await
            .context("Check legacy grants timeout")?;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                tracing::warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

/// Consumer offset.
///
/// When offset is dropped, the message is destroyed.
pub struct Offset(RawRes);

unsafe impl Send for Offset {}
unsafe impl Sync for Offset {}

impl Debug for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Offset")
            .field("ptr", &self.0)
            .field("topic", &self.topic())
            .field("vgroup_id", &self.vgroup_id())
            .field("database", &self.database())
            .finish()
    }
}

impl IsOffset for Offset {
    fn database(&self) -> &str {
        self.0
            .tmq_db_name()
            .expect("a message should belong to a database")
    }
    fn topic(&self) -> &str {
        self.0
            .tmq_topic_name()
            .expect("a message should belong to a topic")
    }
    fn vgroup_id(&self) -> VGroupId {
        self.0
            .tmq_vgroup_id()
            .expect("a message should belong to a vgroup")
    }
}

impl Drop for Offset {
    fn drop(&mut self) {
        self.0.free_result();
    }
}

#[derive(Debug)]
pub struct Consumer {
    tmq: RawTmq,
    timeout: Timeout,
    dsn: Dsn,
}

unsafe impl Send for Consumer {}
unsafe impl Sync for Consumer {}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.tmq.unsubscribe();
        self.tmq.close();
    }
}

pub struct Messages {
    tmq: RawTmq,
    timeout: Option<Duration>,
}

impl Iterator for Messages {
    type Item = (Offset, MessageSet<Meta, Data>);

    fn next(&mut self) -> Option<Self::Item> {
        self.tmq
            .poll_timeout(self.timeout.map(|t| t.as_millis() as i64).unwrap_or(-1))
            .map(|raw| (Offset(raw.clone()), MessageSet::from(raw)))
    }
}

#[derive(Debug)]
pub struct Meta {
    raw: RawRes,
}

impl AsyncOnSync for Meta {}

impl IsMeta for Meta {
    fn as_raw_meta(&self) -> RawResult<RawMeta> {
        let raw = self.raw.tmq_get_raw();

        let mut data = Vec::new();

        data.extend(raw.raw_len.to_le_bytes());

        data.extend(raw.raw_type.to_le_bytes());

        data.extend(unsafe {
            std::slice::from_raw_parts(raw.raw as *const u8, raw.raw_len as usize)
        });
        Ok(RawMeta::new(data.into()))
    }

    fn as_json_meta(&self) -> RawResult<taos_query::common::JsonMeta> {
        let meta = serde_json::from_slice(self.raw.tmq_get_json_meta().as_bytes())
            .map_err(|err| RawError::from_string(err.to_string()))?;
        Ok(meta)
    }
}
impl Meta {
    fn new(raw: RawRes) -> Self {
        Self { raw }
    }

    pub fn to_raw(&self) -> raw_data_t {
        self.raw.tmq_get_raw()
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::from_slice(self.raw.tmq_get_json_meta().as_bytes())
            .expect("meta json should always be valid json format")
    }

    pub fn to_sql(&self) -> String {
        todo!()
    }
}
#[derive(Debug)]
pub struct Data {
    raw: RawRes,
}

impl Data {
    fn new(raw: RawRes) -> Self {
        Self { raw }
    }
}

#[async_trait::async_trait]
impl IsAsyncData for Data {
    async fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        Ok(self.raw.tmq_get_raw().into())
    }

    async fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        Ok(self.raw.fetch_raw_message())
    }
}

impl IsData for Data {
    fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        Ok(self.raw.tmq_get_raw().into())
    }

    fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        Ok(self.raw.fetch_raw_message())
    }
}
// pub enum MessageSet {
//     Meta(Meta),
//     Data(Data),
// }

impl From<RawRes> for MessageSet<Meta, Data> {
    fn from(raw: RawRes) -> Self {
        match raw.tmq_message_type() {
            tmq_res_t::TMQ_RES_INVALID => unreachable!(),
            tmq_res_t::TMQ_RES_DATA => Self::Data(Data::new(raw)),
            tmq_res_t::TMQ_RES_TABLE_META => Self::Meta(Meta::new(raw)),
            tmq_res_t::TMQ_RES_METADATA => Self::MetaData(Meta::new(raw.clone()), Data::new(raw)),
        }
    }
}

impl Iterator for Data {
    type Item = RawResult<RawBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.fetch_raw_message().map(Ok)
    }
}

// impl Iterator for MessageSet {
//     type Item = RawBlock;

//     fn next(&mut self) -> Option<Self::Item> {
//         match self {
//             MessageSet::Meta(data) => None,
//             MessageSet::Data(data) => data.raw.fetch_raw_message(data.precision),
//         }
//     }
// }

impl AsConsumer for Consumer {
    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        let topics = topics.into_iter().map(|item| item.into()).collect_vec();
        let topics = Topics::from_topics(self.tmq.tmq.list_api, topics)?;
        // dbg!(&topics);
        self.tmq.subscribe(&topics)
    }

    fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> RawResult<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
    > {
        Ok(self.tmq.poll_timeout(timeout.as_raw_timeout()).map(|raw| {
            (
                Offset(raw.clone()),
                match raw.tmq_message_type() {
                    tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                    tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
                    tmq_res_t::TMQ_RES_TABLE_META => {
                        taos_query::tmq::MessageSet::Meta(Meta::new(raw))
                    }
                    tmq_res_t::TMQ_RES_METADATA => taos_query::tmq::MessageSet::MetaData(
                        Meta::new(raw.clone()),
                        Data::new(raw),
                    ),
                },
            )
        }))
    }

    fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        self.tmq.commit_sync(offset.0.clone()).map(|_| ())
    }

    fn commit_offset(&self, topic_name: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()> {
        self.tmq.commit_offset_sync(topic_name, vgroup_id, offset)
    }

    fn list_topics(&self) -> RawResult<Vec<String>> {
        let topics = self.tmq.subscription();
        let topics = topics.to_strings();
        Ok(topics)
    }

    fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        let topics = self.tmq.subscription();
        let topics = topics.to_strings();
        let ret = topics
            .into_iter()
            .map(|topic| {
                let assignments = self.tmq.get_topic_assignment(&topic);
                (topic, assignments)
            })
            .collect();
        Some(ret)
    }

    fn offset_seek(&mut self, topic: &str, vg_id: VGroupId, offset: i64) -> RawResult<()> {
        self.tmq.offset_seek(topic, vg_id, offset)
    }

    fn committed(&self, topic: &str, vg_id: VGroupId) -> RawResult<i64> {
        self.tmq.committed(topic, vg_id)
    }

    fn position(&self, topic: &str, vg_id: VGroupId) -> RawResult<i64> {
        self.tmq.position(topic, vg_id)
    }
}

// impl AsyncOnSync for Consumer {}

#[async_trait::async_trait]
impl AsAsyncConsumer for Consumer {
    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        let topics =
            Topics::from_topics(self.tmq.tmq.list_api, topics.into_iter().map(|s| s.into()))?;
        let r = self.tmq.subscribe(&topics);

        if let Some(offset) = self.dsn.get("offset") {
            // dbg!(offset);
            let offsets = offset
                .split(',')
                .map(|s| {
                    s.split(':')
                        .map(|i| i.parse::<i64>().unwrap())
                        .collect_vec()
                })
                .collect_vec();
            let topic_name = &self.tmq.subscription().to_strings()[0];
            for offset in offsets {
                let vgroup_id = offset[0];
                let offset = offset[1];
                tracing::trace!(
                    "topic {} seeking to offset {} for vgroup {}",
                    &topic_name,
                    offset,
                    vgroup_id
                );
                let _ = self.tmq.offset_seek(topic_name, vgroup_id as i32, offset);
            }
        }

        r
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
        use taos_query::prelude::tokio;
        tracing::trace!("Waiting for next message");
        let res = match timeout {
            Timeout::Never | Timeout::None => {
                let timeout = Duration::MAX;
                let sleep = tokio::time::sleep(timeout);
                tokio::pin!(sleep);
                tokio::select! {
                    _ = &mut sleep, if !sleep.is_elapsed() => {
                       Ok(None)
                    }
                    raw = self.tmq.poll_async() => {
                        let message =    (
                            Offset(raw.clone()),
                            match raw.tmq_message_type() {
                                tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                                tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
                                tmq_res_t::TMQ_RES_TABLE_META => {
                                    taos_query::tmq::MessageSet::Meta(Meta::new(raw))
                                }
                                tmq_res_t::TMQ_RES_METADATA => taos_query::tmq::MessageSet::MetaData(Meta::new(raw.clone()), Data::new(raw))
                            },
                        );
                        Ok(Some(message))
                    }
                }
            }
            Timeout::Duration(timeout) => {
                let sleep = tokio::time::sleep(timeout);
                tokio::pin!(sleep);
                tokio::select! {
                    _ = &mut sleep, if !sleep.is_elapsed() => {
                       Ok(None)
                    }
                    raw = self.tmq.poll_async() => {
                        let message =    (
                            Offset(raw.clone()),
                            match raw.tmq_message_type() {
                                tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                                tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
                                tmq_res_t::TMQ_RES_TABLE_META => {
                                    taos_query::tmq::MessageSet::Meta(Meta::new(raw))
                                }
                                tmq_res_t::TMQ_RES_METADATA => taos_query::tmq::MessageSet::MetaData(Meta::new(raw.clone()), Data::new(raw))
                            },
                        );
                        Ok(Some(message))
                    }
                }
            }
        };
        match res {
            Ok(res) => {
                tracing::trace!("Got a new message");
                Ok(res)
            }
            Err(err) => {
                tracing::warn!("Polling message error: {err:?}");
                Err(err)
            }
        }
    }

    async fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        self.tmq.commit(offset.0.clone()).await.map(|_| ())
    }

    async fn commit_offset(
        &self,
        topic_name: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
        self.tmq
            .commit_offset_async(topic_name, vgroup_id, offset)
            .await
            .map(|_| ())
    }

    fn default_timeout(&self) -> Timeout {
        self.timeout
    }

    async fn list_topics(&self) -> RawResult<Vec<String>> {
        let topics = self.tmq.subscription();
        let topics = topics.to_strings();
        Ok(topics)
    }

    async fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        let topics = self.tmq.subscription();
        let topics = topics.to_strings();
        // tracing::info!("topics: {:?}", topics);
        let ret: Vec<(String, Vec<Assignment>)> = topics
            .into_iter()
            .map(|topic| {
                let assignments = self.tmq.get_topic_assignment(&topic);
                (topic, assignments)
            })
            .collect();
        Some(ret)
    }

    async fn topic_assignment(&self, topic: &str) -> Vec<Assignment> {
        self.tmq.get_topic_assignment(topic)
    }

    async fn offset_seek(
        &mut self,
        topic: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
        self.tmq.offset_seek(topic, vgroup_id, offset)
    }

    async fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        self.tmq.committed(topic, vgroup_id)
    }

    async fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        self.tmq.position(topic, vgroup_id)
    }
}
#[cfg(test)]
mod tests {
    use super::TmqBuilder;

    use crate::TaosBuilder;

    #[test]
    fn test_write_raw_block_with_req_id() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        let db = "test_write_raw_block_req_id";
        taos.query(format!("drop topic if exists {db}"))?;
        taos.query(format!("drop database if exists {db}"))?;
        taos.query(format!("create database {db} keep 36500 vgroups 1"))?;
        taos.query(format!("use {db}"))?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create stable stb1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "insert into tb2 using stb1 tags(2, 2.2) values(now, 0) (now + 1s, 0) tb3 using stb1 tags (3, 3.3) values (now, 3) (now +1s, 3)",
        )?;

        taos.query(format!("create topic {db} with meta as database {db}"))?;

        taos.query(format!("drop database if exists {db}2"))?;
        taos.query(format!("create database {db}2"))?;
        taos.query(format!("use {db}2"))?;

        let builder = TmqBuilder::from_dsn(
            "taos://localhost:6030/db?group.id=5&experimental.snapshot.enable=false&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            tracing::debug!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        dbg!(raw.table_name().unwrap());
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                        let req_id = 1002;
                        taos.write_raw_block_with_req_id(&raw, req_id)?;
                    }
                }
                MessageSet::MetaData(meta, data) => {
                    // meta
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;

                    // data
                    for raw in data {
                        let raw = raw?;
                        tracing::debug!("raw: {:?}", raw);
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                tracing::debug!("value in col {}\n", value);
                            }
                        }
                        println!();
                        let req_id = 1003;
                        taos.write_raw_block_with_req_id(&raw, req_id)?;
                    }
                }
            }

            let _ = consumer.commit(offset);
        }

        consumer.unsubscribe();

        let mut query = taos.query("describe stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }
        let mut query = taos.query("select count(*) from stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }

        taos.query(format!("drop database {db}2"))?;
        taos.query(format!("drop topic {db}")).unwrap();
        taos.query(format!("drop database {db}"))?;
        Ok(())
    }

    #[test]
    fn metadata() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        let db = "tmq_metadata";
        taos.query(format!("drop topic if exists {db}"))?;
        taos.query(format!("drop database if exists {db}"))?;
        taos.query(format!("create database {db} keep 36500 vgroups 1"))?;
        taos.query(format!("use {db}"))?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create stable stb1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "insert into tb2 using stb1 tags(2, 2.2) values(now, 0) (now + 1s, 0) tb3 using stb1 tags (3, 3.3) values (now, 3) (now +1s, 3)",
        )?;

        taos.query(format!("create topic {db} with meta as database {db}"))?;

        taos.query(format!("drop database if exists {db}2"))?;
        taos.query(format!("create database {db}2"))?;
        taos.query(format!("use {db}2"))?;

        let builder = TmqBuilder::from_dsn(
            "taos://localhost:6030/db?group.id=5&experimental.snapshot.enable=false&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            tracing::debug!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        dbg!(raw.table_name().unwrap());
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                        taos.write_raw_block(&raw)?;
                    }
                }
                MessageSet::MetaData(meta, data) => {
                    // meta
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;

                    // data
                    for raw in data {
                        let raw = raw?;
                        tracing::debug!("raw: {:?}", raw);
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                tracing::debug!("value in col {}\n", value);
                            }
                        }
                        println!();
                        taos.write_raw_block(&raw)?;
                    }
                }
            }

            let _ = consumer.commit(offset);
        }

        consumer.unsubscribe();

        let mut query = taos.query("describe stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }
        let mut query = taos.query("select count(*) from stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }

        taos.query(format!("drop database {db}2"))?;
        taos.query(format!("drop topic {db}")).unwrap();
        taos.query(format!("drop database {db}"))?;
        Ok(())
    }

    #[test]
    fn meta() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        // let _ = pretty_env_logger::formatted_timed_builder().try_init();

        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        let db = "tmq_meta";
        taos.query(format!("drop topic if exists {db}"))?;
        taos.query(format!("drop database if exists {db}"))?;
        taos.query(format!("create database {db} keep 36500"))?;
        taos.query(format!("use {db}"))?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create stable stb1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create table tb1 using stb1 tags(1, 1.1)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create table cb1 (ts timestamp, v int, c2 bool, c3 varchar(10))",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "alter table cb1 add column c4 nchar(10)",
        )?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "alter table cb1 drop column c4",
        )?;

        taos.query("alter table cb1 modify column c3 varchar(100)")?;
        taos.query("alter table cb1 rename column c2 n2")?;

        taos.query(format!("create topic {db} with meta as database {db}"))?;

        taos.query(format!("drop database if exists {db}2"))?;
        taos.query(format!("create database {db}2"))?;
        taos.query(format!("use {db}2"))?;

        let builder =
            TmqBuilder::from_dsn("taos://localhost:6030/db?group.id=5&experimental.snapshot.enable=false&auto.offset.reset=earliest")?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        tracing::trace!("polling start");

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            tracing::debug!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                    }
                }
                MessageSet::MetaData(meta, data) => {
                    let json = meta.to_json();
                    dbg!(json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;
                    for raw in data {
                        let raw = raw?;
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                    }
                }
            }

            let _ = consumer.commit(offset);
        }

        consumer.unsubscribe();

        let mut query = taos.query("describe stb1")?;
        for row in query.rows() {
            let raw = row?;
            tracing::debug!("raw: {:?}", raw);
        }

        taos.query("drop database tmq_meta2")?;
        taos.query("drop topic tmq_meta").unwrap();
        taos.query("drop database tmq_meta")?;
        Ok(())
    }

    #[test]
    fn test_tmq_meta_sync() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        // pretty_env_logger::formatted_builder()
        //     .filter_level(tracing::LevelFilter::Debug)
        //     .init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build()?;
        taos.exec_many([
            "drop topic if exists sys_tmq_meta_sync",
            "drop database if exists sys_tmq_meta_sync",
            "create database sys_tmq_meta_sync vgroups 1",
            "use sys_tmq_meta_sync",
            "show databases",
            "select database()",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            "describe stb1",
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
            "create topic if not exists sys_tmq_meta_sync with meta as database sys_tmq_meta_sync",
        ])?;

        taos.exec_many([
            "drop database if exists sys_tmq_meta_sync2",
            "create database if not exists sys_tmq_meta_sync2",
            "use sys_tmq_meta_sync2",
        ])?;

        let builder = TmqBuilder::from_dsn("taos://localhost:6030?group.id=10&timeout=1000ms&experimental.snapshot.enable=false&auto.offset.reset=earliest")?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["sys_tmq_meta_sync"])?;

        let iter = consumer.iter_with_timeout(Timeout::from_millis(500));

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
                    let raw = meta.as_raw_meta()?;
                    taos.write_raw_meta(&raw)?;

                    let json = meta.as_json_meta()?;
                    match &json {
                        taos_query::common::JsonMeta::Create(m) => match m {
                            taos_query::common::MetaCreate::Super {
                                table_name,
                                columns: _,
                                tags: _,
                            } => {
                                let desc = taos.describe(table_name.as_str())?;
                                dbg!(desc);
                            }
                            taos_query::common::MetaCreate::Child {
                                table_name,
                                using: _,
                                tags: _,
                                tag_num: _,
                            } => {
                                let desc = taos.describe(table_name.as_str())?;
                                dbg!(desc);
                            }
                            taos_query::common::MetaCreate::Normal {
                                table_name,
                                columns: _,
                            } => {
                                let desc = taos.describe(table_name.as_str())?;
                                dbg!(desc);
                            }
                        },
                        _ => (),
                    }

                    // meta data can be write to an database seamlessly by raw or json (to sql).
                    let sql = dbg!(json.to_string());
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
                            Code::INVALID_ROW_BYTES => tracing::trace!("invalid row bytes"),
                            Code::DUPLICATED_COLUMN_NAMES => {
                                tracing::trace!("duplicated column names")
                            }
                            Code::NO_COLUMN_CAN_BE_DROPPED => {
                                tracing::trace!("no column can be dropped")
                            }
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
                        // let block = block?;
                        dbg!(block.table_name());
                        dbg!(block);
                    }
                }
                _ => (),
            }
            consumer.commit(offset)?;
        }

        consumer.unsubscribe();

        taos.exec_many([
            "drop database sys_tmq_meta_sync2",
            "drop topic sys_tmq_meta_sync",
            "drop database sys_tmq_meta_sync",
        ])?;
        Ok(())
    }

    #[test]
    fn test_tmq_committed() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        // let _ = pretty_env_logger::formatted_builder()
        //     .filter_level(tracing::log::LevelFilter::Info)
        //     .try_init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build()?;

        let source = "tmq_source_sync_1";
        let target = "tmq_target_sync_1";

        taos.exec_many([
            format!("drop topic if exists {source}").as_str(),
            format!("drop database if exists {source}").as_str(),
            format!("create database {source} wal_retention_period 3600").as_str(),
            format!("create topic {source} with meta as database {source}").as_str(),
            format!("use {source}").as_str(),
            "show databases",
            "select database()",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            "describe stb1",
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
            format!("drop database if exists {target}").as_str(),
            format!("create database if not exists {target} wal_retention_period 3600").as_str(),
            format!("use {target}").as_str(),
        ])?;

        let builder = TmqBuilder::from_dsn("taos://localhost:6030?group.id=10&timeout=1000ms&auto.offset.reset=earliest&experimental.snapshot.enable=false")?;
        let mut consumer = builder.build()?;

        let topics = consumer.list_topics()?;
        tracing::debug!("topics before subcribe:{:?}", topics);

        consumer.subscribe([source])?;

        let topics = consumer.list_topics()?;
        tracing::debug!("topics after subcribe:{:?}", topics);

        let iter = consumer.iter_with_timeout(Timeout::from_millis(500));

        for msg in iter {
            let (offset, message) = msg?;
            // Offset contains information for topic name, database name and vgroup id,
            //  similar to kafka topic/partition/offset.
            let topic: &str = offset.topic();
            let _database = offset.database();
            let vgroup_id = offset.vgroup_id();

            // Different to kafka message, TDengine consumer would consume two kind of messages.
            //
            // 1. meta
            // 2. data
            match message {
                MessageSet::Meta(meta) => {
                    let raw = meta.as_raw_meta()?;
                    taos.write_raw_meta(&raw)?;

                    let json = meta.as_json_meta()?;
                    match &json {
                        taos_query::common::JsonMeta::Create(m) => match m {
                            taos_query::common::MetaCreate::Super {
                                table_name,
                                columns: _,
                                tags: _,
                            } => {
                                let desc = taos.describe(table_name.as_str())?;
                                tracing::trace!("{:?}", desc);
                            }
                            taos_query::common::MetaCreate::Child {
                                table_name,
                                using: _,
                                tags: _,
                                tag_num: _,
                            } => {
                                let desc = taos.describe(table_name.as_str())?;
                                tracing::trace!("{:?}", desc);
                            }
                            taos_query::common::MetaCreate::Normal {
                                table_name,
                                columns: _,
                            } => {
                                let desc = taos.describe(table_name.as_str())?;
                                tracing::trace!("{:?}", desc);
                            }
                        },
                        _ => (),
                    }

                    // meta data can be write to an database seamlessly by raw or json (to sql).
                    let sql = json.to_string();
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
                            Code::INVALID_ROW_BYTES => tracing::trace!("invalid row bytes"),
                            Code::DUPLICATED_COLUMN_NAMES => {
                                tracing::trace!("duplicated column names")
                            }
                            Code::NO_COLUMN_CAN_BE_DROPPED => {
                                tracing::trace!("no column can be dropped")
                            }
                            _ => {
                                tracing::debug!("{}", err);
                            }
                        }
                    }
                }
                MessageSet::Data(data) => {
                    // data message may have more than one data block for various tables.
                    for block in data {
                        let block = block?;
                        // let block = block?;
                        tracing::trace!("{:?}", block.table_name());
                        tracing::trace!("{:?}", block);
                    }
                }
                _ => (),
            }
            let committed = consumer.committed(topic, vgroup_id);
            tracing::debug!("committed: {:?}", committed);

            let pos = consumer.position(topic, vgroup_id);
            tracing::debug!("position: {:?}", pos);

            consumer.commit(offset)?;
        }

        let assignments = consumer.assignments().unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        for (topic, vec_assignment) in assignments {
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let end = assignment.end();

                let committed = consumer.committed(topic.as_str(), vgroup_id);
                tracing::debug!("committed before: {:?}", committed);

                let _ = consumer.commit_offset(&topic, vgroup_id, end);

                let committed = consumer.committed(topic.as_str(), vgroup_id);
                tracing::debug!("committed after: {:?}", committed);
            }
        }

        let assignments = consumer.assignments().unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        consumer.unsubscribe();

        taos.exec_many([
            format!("drop database {target}").as_str(),
            format!("drop topic {source}").as_str(),
            format!("drop database {source}").as_str(),
        ])?;
        Ok(())
    }
}

#[cfg(test)]
mod async_tests {

    use std::time::Duration;

    use super::TmqBuilder;

    #[tokio::test]
    async fn test_write_raw_block_with_req_id() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build().await?;
        let db = "test_write_raw_block_req_id_async";
        taos.exec_many([
            format!("drop topic if exists {db}").as_str(),
            format!("drop database if exists {db}").as_str(),
            format!("create database {db} keep 36500 vgroups 1").as_str(),
            format!("use {db}").as_str(),
            "create stable stb1(ts timestamp, v int) tags(jt int, t1 float)",
            "insert into tb2 using stb1 tags(2, 2.2) values(now, 0) (now + 1s, 0) tb3 using stb1 tags (3, 3.3) values (now, 3) (now +1s, 3)",
            format!("create topic {db} with meta as database {db}").as_str(),
            format!("drop database if exists {db}2").as_str(),
            format!("create database {db}2").as_str(),
            format!("use {db}2").as_str(),

        ]).await?;

        let builder = TmqBuilder::from_dsn(
            "taos://localhost:6030/db?group.id=5&experimental.snapshot.enable=false&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build().await?;

        consumer.subscribe([db]).await?;

        for message in
            taos_query::tmq::AsConsumer::iter_with_timeout(&consumer, Timeout::from_secs(1))
        {
            let (offset, msg) = message?;
            tracing::debug!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta().await?).await?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        dbg!(raw.table_name().unwrap());
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
                            }
                        }
                        println!();
                        let req_id = 1002;
                        taos.write_raw_block_with_req_id(&raw, req_id).await?;
                    }
                }
                MessageSet::MetaData(meta, data) => {
                    // meta
                    let json = meta.to_json();
                    tracing::debug!("json: {:?}", json);
                    taos.write_raw_meta(&meta.as_raw_meta().await?).await?;

                    // data
                    for raw in data {
                        let raw = raw?;
                        tracing::debug!("raw: {:?}", raw);
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                tracing::debug!("value in col {}\n", value);
                            }
                        }
                        println!();
                        let req_id = 1003;
                        taos.write_raw_block_with_req_id(&raw, req_id).await?;
                    }
                }
            }

            let _ = consumer.commit(offset).await;
        }

        consumer.unsubscribe().await;

        taos.exec_many([
            format!("drop database {db}2").as_str(),
            format!("drop topic {db}").as_str(),
            format!("drop database {db}").as_str(),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_meta() -> anyhow::Result<()> {
        // use futures::TryStreamExt;
        use taos_query::prelude::*;

        // pretty_env_logger::formatted_builder()
        //     .filter_level(tracing::LevelFilter::Debug)
        //     .init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop topic if exists sys_tmq_meta",
            "drop database if exists sys_tmq_meta",
            "create database sys_tmq_meta",
            "use sys_tmq_meta",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL, \
            NULL, NULL, NULL, NULL, NULL, \
            NULL, NULL, NULL, NULL) \
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
            "insert into tb4 using stb2 tags(false, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)  (ts, c1) values (now, false)",
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
            "drop table `tb2`,`tb1`",
            // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
            "create topic if not exists sys_tmq_meta with meta as database sys_tmq_meta",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists sys_tmq_meta2",
            "create database if not exists sys_tmq_meta2",
            "use sys_tmq_meta2",
        ])
        .await?;

        let builder = TmqBuilder::from_dsn("taos:///?group.id=10&timeout=1000ms&experimental.snapshot.enable=false&auto.offset.reset=earliest")?;
        let mut consumer = builder.build().await?;
        consumer.subscribe(["sys_tmq_meta"]).await?;

        consumer
            .stream_with_timeout(Timeout::from_millis(500))
            .try_for_each(|(offset, message)| async {
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
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        // dbg!(json);
                        let sql = dbg!(json.to_string());
                        if let Err(err) = taos.exec(sql).await {
                            match err.code() {
                                Code::TAG_ALREADY_EXIST => tracing::trace!("tag already exists"),
                                Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                Code::INVALID_COLUMN_NAME => tracing::trace!("invalid column name"),
                                Code::MODIFIED_ALREADY => tracing::trace!("modified already done"),
                                Code::TABLE_NOT_EXIST => tracing::trace!("table does not exists"),
                                Code::STABLE_NOT_EXIST => tracing::trace!("stable does not exists"),
                                Code::INVALID_ROW_BYTES => tracing::trace!("invalid row bytes"),
                                Code::DUPLICATED_COLUMN_NAMES => {
                                    tracing::trace!("duplicated column names")
                                }
                                Code::NO_COLUMN_CAN_BE_DROPPED => {
                                    tracing::trace!("no column can be dropped")
                                }
                                _ => {
                                    panic!("{}", err);
                                }
                            }
                        }
                    }
                    MessageSet::Data(mut data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.next().transpose()? {
                            dbg!(data.table_name());
                            dbg!(data);
                        }
                    }
                    _ => (),
                }
                consumer.commit(offset).await?;
                Ok(())
            })
            .await?;

        consumer.unsubscribe().await;

        taos.exec_many([
            "drop database sys_tmq_meta2",
            "drop topic sys_tmq_meta",
            "drop database sys_tmq_meta",
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq() -> anyhow::Result<()> {
        // pretty_env_logger::formatted_timed_builder()
        //     .filter_level(tracing::LevelFilter::Info)
        //     .init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let mut dsn = "taos://localhost:6030".to_string();
        tracing::info!("dsn: {}", dsn);

        let taos = crate::TaosBuilder::from_dsn(&dsn)?.build().await?;
        let db_name = "test_tmq";
        let topic_name = "test_tmq";
        taos.exec_many([
            format!("drop topic if exists {db_name}").as_str(),
            format!("drop database if exists {db_name}").as_str(),
            format!("create database {db_name}").as_str(),
            format!("create topic {topic_name} with meta as database {db_name}").as_str(),
            format!("use {db_name}").as_str(),
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
            // "drop table `table`",
            // kind 10: drop child table
            // "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
        ])
        .await?;

        let db2_name = "test_tmq2";
        taos.exec_many([
            format!("drop database if exists {db2_name}").as_str(),
            format!("create database {db2_name} wal_retention_period 3600").as_str(),
            format!("use {db2_name}").as_str(),
        ])
        .await?;

        dsn.push_str("?group.id=10&timeout=1000ms&experimental.snapshot.enable=false&auto.offset.reset=earliest");
        let builder = TmqBuilder::from_dsn(&dsn)?;
        let mut consumer = builder.build().await?;
        consumer.subscribe([topic_name]).await?;

        {
            let mut stream = consumer.stream_with_timeout(Timeout::from_secs(1));

            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let topic: &str = offset.topic();
                let database = offset.database();
                let vgroup_id = offset.vgroup_id();
                tracing::debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    topic,
                    database,
                    vgroup_id
                );

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                match message {
                    MessageSet::Meta(meta) => {
                        tracing::debug!("Meta");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = json.to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
                        }
                    }
                    MessageSet::Data(data) => {
                        tracing::debug!("Data");
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                    MessageSet::MetaData(meta, data) => {
                        tracing::debug!("MetaData");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = json.to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
                        }
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                }
                consumer.commit(offset).await?;
            }
        }

        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::debug!(
                    "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                    topic,
                    vgroup_id,
                    current,
                    begin,
                    end
                );
                let res = consumer.offset_seek(topic, vgroup_id, end).await;
                if res.is_err() {
                    tracing::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    tracing::error!("assignments: {:?}", a);
                    // panic!()
                }
            }

            let topic_assignment = consumer.topic_assignment(topic).await;
            tracing::debug!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop topic {topic_name}").as_str(),
            format!("drop database {db_name}").as_str(),
            format!("drop database {db2_name}").as_str(),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_committed() -> anyhow::Result<()> {
        // let _ = pretty_env_logger::formatted_builder()
        //     .filter_level(tracing::log::LevelFilter::Info)
        //     .try_init();

        use taos_query::prelude::*;
        let mut dsn = "tmq://localhost:6030?".to_string();
        tracing::info!("dsn: {}", dsn);

        let taos = crate::TaosBuilder::from_dsn(&dsn)?.build().await?;

        let source = "tmq_source_1";
        let target = "tmq_target_1";

        taos.exec_many([
            format!("drop topic if exists {source}").as_str(),
            format!("drop database if exists {source}").as_str(),
            format!("create database {source} wal_retention_period 3600").as_str(),
            format!("create topic {source} with meta as database {source}").as_str(),
            format!("use {source}").as_str(),
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
        ])
        .await?;

        taos.exec_many([
            format!("drop database if exists {target}").as_str(),
            format!("create database if not exists {target} wal_retention_period 3600").as_str(),
            format!("use {target}").as_str(),
        ])
        .await?;

        dsn.push_str("&group.id=10&timeout=1000ms&auto.offset.reset=earliest&experimental.snapshot.enable=false");
        let builder = TmqBuilder::from_dsn(&dsn)?;

        let mut consumer = builder.build().await?;

        let topics = consumer.list_topics().await?;
        tracing::debug!("topics before subcribe:{:?}", topics);

        consumer.subscribe([source]).await?;

        let topics = consumer.list_topics().await?;
        tracing::debug!("topics after subcribe:{:?}", topics);

        {
            let mut stream = consumer.stream_with_timeout(Timeout::from_secs(1));

            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let topic: &str = offset.topic();
                let database = offset.database();
                let vgroup_id = offset.vgroup_id();
                tracing::debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    topic,
                    database,
                    vgroup_id
                );

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                match message {
                    MessageSet::Meta(meta) => {
                        tracing::debug!("Meta");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = json.to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
                        }
                    }
                    MessageSet::Data(data) => {
                        tracing::debug!("Data");
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                    MessageSet::MetaData(meta, data) => {
                        tracing::debug!("MetaData");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = json.to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
                        }
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                }
                let committed = consumer.committed(topic, vgroup_id).await;
                tracing::debug!("committed: {:?} vgroup_id: {}", committed, vgroup_id);

                let pos = consumer.position(topic, vgroup_id).await;
                tracing::debug!("position: {:?} vgroup_id: {}", pos, vgroup_id);

                consumer.commit(offset).await?;
            }
        }

        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::debug!(
                    "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                    topic,
                    vgroup_id,
                    current,
                    begin,
                    end
                );

                let committed = consumer.committed(topic, vgroup_id).await;
                tracing::debug!(
                    "before commit_offset committed: {:?} vgroup_id: {}",
                    committed,
                    vgroup_id
                );

                let pos = consumer.position(topic, vgroup_id).await;
                tracing::debug!(
                    "before commit_offset position: {:?} vgroup_id: {}",
                    pos,
                    vgroup_id
                );

                let res = consumer.commit_offset(topic, vgroup_id, end).await;

                let committed = consumer.committed(topic, vgroup_id).await;
                tracing::debug!(
                    "after commit_offset committed: {:?} vgroup_id: {}",
                    committed,
                    vgroup_id
                );

                let pos = consumer.position(topic, vgroup_id).await;
                tracing::debug!(
                    "after commit_offset position: {:?} vgroup_id: {}",
                    pos,
                    vgroup_id
                );

                if res.is_err() {
                    tracing::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    tracing::error!("assignments: {:?}", a);
                }

                // commit offset out of range, expect error

                let res = consumer.commit_offset(topic, vgroup_id, end + 1).await;

                let committed = consumer.committed(topic, vgroup_id).await;
                tracing::info!(
                    "after commit_offset committed: {:?} vgroup_id: {}",
                    committed,
                    vgroup_id
                );

                let pos = consumer.position(topic, vgroup_id).await;
                tracing::info!(
                    "after commit_offset position: {:?} vgroup_id: {}",
                    pos,
                    vgroup_id
                );

                if res.is_err() {
                    tracing::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    tracing::error!("assignments: {:?}", a);
                }
            }

            let topic_assignment = consumer.topic_assignment(topic).await;
            tracing::debug!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            format!("drop database {target}").as_str(),
            format!("drop topic {source}").as_str(),
            format!("drop database {source}").as_str(),
        ])
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_offset() -> anyhow::Result<()> {
        // pretty_env_logger::formatted_timed_builder()
        //     .filter_level(tracing::LevelFilter::Info)
        //     .init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let mut dsn = "tmq://localhost:6030?offset=10:20,11:40".to_string();
        tracing::info!("dsn: {}", dsn);

        let taos = crate::TaosBuilder::from_dsn(&dsn)?.build().await?;
        taos.exec_many([
            "drop topic if exists ws_abc1",
            "drop database if exists ws_abc1",
            "create database ws_abc1 wal_retention_period 3600",
            "create topic ws_abc1 with meta as database ws_abc1",
            "use ws_abc1",
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
            // "drop table `table`",
            // kind 10: drop child table
            // "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists db2",
            "create database if not exists db2 wal_retention_period 3600",
            "use db2",
        ])
        .await?;

        dsn.push_str("&group.id=10&timeout=1000ms&auto.offset.reset=earliest&experimental.snapshot.enable=false");
        let builder = TmqBuilder::from_dsn(&dsn)?;
        // dbg!(&builder.dsn);
        let mut consumer = builder.build().await?;
        consumer.subscribe(["ws_abc1"]).await?;

        {
            let mut stream = consumer.stream_with_timeout(Timeout::from_secs(1));

            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let topic: &str = offset.topic();
                let database = offset.database();
                let vgroup_id = offset.vgroup_id();
                tracing::debug!(
                    "topic: {}, database: {}, vgroup_id: {}",
                    topic,
                    database,
                    vgroup_id
                );

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                match message {
                    MessageSet::Meta(meta) => {
                        tracing::debug!("Meta");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = json.to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
                        }
                    }
                    MessageSet::Data(data) => {
                        tracing::debug!("Data");
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                    MessageSet::MetaData(meta, data) => {
                        tracing::debug!("MetaData");
                        let raw = meta.as_raw_meta().await?;
                        taos.write_raw_meta(&raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        let sql = json.to_string();
                        if let Err(err) = taos.exec(sql).await {
                            println!("maybe error: {}", err);
                        }
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            tracing::debug!("table_name: {:?}", data.table_name());
                            tracing::debug!("data: {:?}", data);
                        }
                    }
                }
                consumer.commit(offset).await?;
            }
        }

        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                tracing::debug!(
                    "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                    topic,
                    vgroup_id,
                    current,
                    begin,
                    end
                );
                let res = consumer.offset_seek(topic, vgroup_id, end).await;
                if res.is_err() {
                    tracing::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    tracing::error!("assignments: {:?}", a);
                    // panic!()
                }
            }

            let topic_assignment = consumer.topic_assignment(topic).await;
            tracing::debug!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        tracing::debug!("after seek offset assignments: {:?}", assignments);

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        taos.exec_many([
            "drop database db2",
            "drop topic ws_abc1",
            "drop database ws_abc1",
        ])
        .await?;
        Ok(())
    }
}
