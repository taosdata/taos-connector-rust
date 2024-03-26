pub(crate) mod ffi;

use std::{
    ffi::{CStr, CString},
    fmt::Debug,
    str::FromStr,
    time::Duration,
};

pub(crate) use ffi::*;

use itertools::Itertools;
use taos_query::{
    common::{raw_data_t, Precision, RawMeta},
    prelude::tokio,
    tmq::{
        AsAsyncConsumer, AsConsumer, Assignment, AsyncOnSync, IsAsyncData, IsData, IsMeta,
        IsOffset, MessageSet, Timeout, VGroupId,
    },
    util::Edition,
    Dsn, IntoDsn, RawBlock, RawResult,
};

use crate::{conn::RawTaos, query::RawRes, TaosBuilder};

use taos_query::prelude::RawError;

mod raw;

use raw::RawTmq;

use self::raw::{Conf, Topics};

impl RawRes {
    #[inline]
    pub fn tmq_topic_name(&self) -> Option<&str> {
        unsafe {
            let c = tmq_get_topic_name(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }
    #[inline]
    pub fn tmq_vgroup_id(&self) -> Option<VGroupId> {
        unsafe {
            let c = tmq_get_vgroup_id(self.as_ptr());
            if c == -1 {
                None
            } else {
                Some(c)
            }
        }
    }

    #[inline]
    pub fn tmq_table_name(&self) -> Option<&str> {
        unsafe {
            let c = tmq_get_table_name(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }
    #[inline]
    pub(crate) fn tmq_db_name(&self) -> Option<&str> {
        unsafe {
            let c = tmq_get_db_name(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }

    #[inline]
    pub(crate) fn tmq_message_type(&self) -> tmq_res_t {
        unsafe { tmq_get_res_type(self.as_ptr()) }
    }

    #[inline]
    pub(crate) fn tmq_get_json_meta(&self) -> CString {
        unsafe {
            let meta = tmq_get_json_meta(self.0);
            CString::from_raw(meta)
        }
    }

    #[inline]
    pub(crate) fn tmq_get_raw(&self) -> raw_data_t {
        let mut meta = raw_data_t {
            raw: std::ptr::null_mut(),
            raw_len: 0,
            raw_type: 0,
        };
        unsafe {
            let code = tmq_get_raw(self.0, &mut meta as _);
        }
        meta
    }
    #[inline]
    pub(crate) fn tmq_free_raw(&self, raw: raw_data_t) {
        unsafe {
            tmq_free_raw(raw);
        }
    }
}

#[derive(Debug)]
pub struct TmqBuilder {
    builder: TaosBuilder,
    dsn: Dsn,
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
        let conf = Conf::from_dsn(&dsn)?;
        let timeout = if let Some(timeout) = dsn.params.remove("timeout") {
            Timeout::from_str(&timeout).map_err(RawError::from_any)?
        } else {
            Timeout::from_millis(500)
        };
        Ok(Self {
            builder: TaosBuilder::from_dsn(&dsn)?,
            dsn,
            conf,
            timeout,
        })
    }

    fn client_version() -> &'static str {
        RawTaos::version()
    }

    fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        self.build().map(|_| ())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> RawResult<Self::Target> {
        self.conf.build().map(|tmq| Consumer {
            tmq,
            timeout: self.timeout,
            dsn: self.dsn.clone(),
        })
    }

    fn is_enterprise_edition(&self) -> RawResult<bool> {
        unimplemented!()
    }

    fn server_version(&self) -> RawResult<&str> {
        unimplemented!()
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
                log::warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
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
        let conf = Conf::from_dsn(&dsn)?;
        let timeout = if let Some(timeout) = dsn.params.remove("timeout") {
            Timeout::from_str(&timeout).map_err(RawError::from_any)?
        } else {
            Timeout::from_millis(500)
        };
        Ok(Self {
            builder: TaosBuilder::from_dsn(&dsn)?,
            dsn,
            conf,
            timeout,
        })
    }

    fn client_version() -> &'static str {
        RawTaos::version()
    }

    async fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        self.build().await.map(|_| ())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> RawResult<Self::Target> {
        self.conf.build().map(|tmq| Consumer {
            tmq,
            timeout: self.timeout,
            dsn: self.dsn.clone(),
        })
    }

    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        unimplemented!()
    }

    async fn server_version(&self) -> RawResult<&str> {
        unimplemented!()
    }

    async fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
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
                log::warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
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
        self.0.drop();
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
            .map(|raw| (Offset(raw), MessageSet::from(raw)))
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
        self.raw.tmq_free_raw(raw);
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
    precision: Precision,
}

impl Data {
    fn new(raw: RawRes) -> Self {
        Self {
            precision: raw.precision(),
            raw,
        }
    }
}

#[async_trait::async_trait]
impl IsAsyncData for Data {
    async fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        Ok(self.raw.tmq_get_raw().into())
    }

    async fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        Ok(self.raw.fetch_raw_message(self.precision))
    }
}
impl IsData for Data {
    fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        Ok(self.raw.tmq_get_raw().into())
    }

    fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        Ok(self.raw.fetch_raw_message(self.precision))
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
            tmq_res_t::TMQ_RES_METADATA => Self::MetaData(Meta::new(raw), Data::new(raw)),
        }
    }
}

pub struct MessageSetIter {
    raw: RawRes,
    msg_type: tmq_res_t,
    precision: Precision,
}

impl Iterator for Data {
    type Item = RawResult<RawBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.fetch_raw_message(self.precision).map(Ok)
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
        let topics = Topics::from_topics(topics.into_iter().map(|s| s.into()))?;
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
                Offset(raw),
                match raw.tmq_message_type() {
                    tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                    tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
                    tmq_res_t::TMQ_RES_TABLE_META => {
                        taos_query::tmq::MessageSet::Meta(Meta::new(raw))
                    }
                    tmq_res_t::TMQ_RES_METADATA => {
                        taos_query::tmq::MessageSet::MetaData(Meta::new(raw), Data::new(raw))
                    }
                },
            )
        }))
    }

    fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        self.tmq.commit_sync(offset.0).map(|_| ())
    }

    fn commit_offset(&self, topic_name: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()> {
        unimplemented!("commit_offset")
    }

    fn list_topics(&self) -> RawResult<Vec<String>> {
        unimplemented!("list_topics")
    }

    fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        let topics = self.tmq.subscription();
        let topics = topics.into_strings();
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
        unimplemented!("committed")
    }

    fn position(&self, topic: &str, vg_id: VGroupId) -> RawResult<i64> {
        unimplemented!("position")
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
        let topics = Topics::from_topics(topics.into_iter().map(|s| s.into()))?;

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
            let topic_name = &self.tmq.subscription().into_strings()[0];
            // let topic_name = topic_name.into_strings();
            for offset in offsets {
                let vgroup_id = offset[0];
                let offset = offset[1];
                log::trace!(
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
        log::trace!("waiting for next message");
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
                            Offset(raw),
                            match raw.tmq_message_type() {
                                tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                                tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
                                tmq_res_t::TMQ_RES_TABLE_META => {
                                    taos_query::tmq::MessageSet::Meta(Meta::new(raw))
                                }
                                tmq_res_t::TMQ_RES_METADATA => taos_query::tmq::MessageSet::MetaData(Meta::new(raw), Data::new(raw))
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
                            Offset(raw),
                            match raw.tmq_message_type() {
                                tmq_res_t::TMQ_RES_INVALID => unreachable!(),
                                tmq_res_t::TMQ_RES_DATA => taos_query::tmq::MessageSet::Data(Data::new(raw)),
                                tmq_res_t::TMQ_RES_TABLE_META => {
                                    taos_query::tmq::MessageSet::Meta(Meta::new(raw))
                                }
                                tmq_res_t::TMQ_RES_METADATA => taos_query::tmq::MessageSet::MetaData(Meta::new(raw), Data::new(raw))
                            },
                        );
                        Ok(Some(message))
                    }
                }
            }
        };
        log::trace!("waiting for next message");
        res
    }

    async fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        self.tmq.commit(offset.0).await.map(|_| ())
    }

    async fn commit_offset(
        &self,
        topic_name: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
        unimplemented!("commit_offset")
    }

    fn default_timeout(&self) -> Timeout {
        self.timeout
    }

    async fn list_topics(&self) -> RawResult<Vec<String>> {
        unimplemented!("list_topics")
    }

    async fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        let topics = self.tmq.subscription();
        let topics = topics.into_strings();
        log::trace!("topics: {:?}", topics);
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
        unimplemented!("committed")
    }

    async fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        unimplemented!("position")
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use crate::RawTaos;

    use super::TmqBuilder;

    #[test]
    fn metadata() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        use std::ptr::null;
        let host = null();
        let user = null();
        let pass = null();
        let db = null();
        let port = 0;
        let taos = RawTaos::connect(host, user, pass, db, port)?;
        let db = "tmq_metadata";
        taos.query(format!("drop topic if exists {db}"))?;
        taos.query(format!("drop database if exists {db}"))?;
        taos.query(format!(
            "create database {db} keep 36500 vgroups 1 wal_retention_period 3600"
        ))?;
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
        taos.query(format!("create database {db}2 wal_retention_period 3600"))?;
        taos.query(format!("use {db}2"))?;

        let builder = TmqBuilder::from_dsn(
            "taos://localhost:6030/db?group.id=5&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            println!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    dbg!(json);
                    taos.write_raw_meta(meta.to_raw())?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        dbg!(raw.table_name().unwrap());
                        let (nrows, ncols) = (raw.nrows(), raw.ncols());
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
                    dbg!(json);
                    taos.write_raw_meta(meta.to_raw())?;

                    // data
                    for raw in data {
                        let raw = raw?;
                        dbg!(raw.table_name().unwrap());
                        let (nrows, ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                print!("{}\t", value);
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

        let query = taos.query("describe stb1")?;
        for row in query {
            let raw = row?;
            dbg!(raw);
        }
        let query = taos.query("select count(*) from stb1")?;
        for row in query {
            let raw = row?;
            dbg!(raw);
        }

        taos.query(format!("drop database {db}2"))?;
        taos.query(format!("drop topic {db}")).unwrap();
        taos.query(format!("drop database {db}"))?;
        Ok(())
    }

    #[test]
    fn meta() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        use std::ptr::null;
        let host = null();
        let user = null();
        let pass = null();
        let db = null();
        let port = 0;
        let taos = RawTaos::connect(host, user, pass, db, port)?;
        let db = "tmq_meta";
        taos.query(format!("drop topic if exists {db}"))?;
        taos.query(format!("drop database if exists {db}"))?;
        taos.query(format!(
            "create database {db} keep 36500 wal_retention_period 3600"
        ))?;
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
        taos.query(format!("create database {db}2 wal_retention_period 3600"))?;
        taos.query(format!("use {db}2"))?;

        let builder =
            TmqBuilder::from_dsn("taos://localhost:6030/db?group.id=5&auto.offset.reset=earliest")?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            println!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    dbg!(json);
                    taos.write_raw_meta(meta.to_raw())?;
                    // taos.w
                }
                MessageSet::Data(data) => {
                    for raw in data {
                        let raw = raw?;
                        let (nrows, ncols) = (raw.nrows(), raw.ncols());
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
                    taos.write_raw_meta(meta.to_raw())?;
                    for raw in data {
                        let raw = raw?;
                        let (nrows, ncols) = (raw.nrows(), raw.ncols());
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

        let query = taos.query("describe stb1")?;
        for row in query {
            let raw = row?;
            dbg!(raw);
        }

        taos.query("drop database tmq_meta2")?;
        taos.query("drop topic tmq_meta").unwrap();
        taos.query("drop database tmq_meta")?;
        Ok(())
    }

    #[test]
    fn test_tmq_meta_sync() -> anyhow::Result<()> {
        use crate::sync::*;
        // pretty_env_logger::formatted_builder()
        //     .filter_level(log::LevelFilter::Debug)
        //     .init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build()?;
        taos.exec_many([
            "drop topic if exists sys_tmq_meta_sync",
            "drop database if exists sys_tmq_meta_sync",
            "create database sys_tmq_meta_sync vgroups 1 wal_retention_period 3600",
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
            "create database if not exists sys_tmq_meta_sync2 wal_retention_period 3600",
            "use sys_tmq_meta_sync2",
        ])?;

        let builder = TmqBuilder::from_dsn("taos://localhost:6030?group.id=10&timeout=1000ms&auto.offset.reset=earliest")?;
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
                                columns,
                                tags,
                            } => {
                                let _desc = taos.describe(table_name.as_str())?;
                                // dbg!(desc);
                            }
                            taos_query::common::MetaCreate::Child {
                                table_name,
                                using,
                                tags,
                                tag_num,
                            } => {
                                let _desc = taos.describe(table_name.as_str())?;
                                // dbg!(desc);
                            }
                            taos_query::common::MetaCreate::Normal {
                                table_name,
                                columns,
                            } => {
                                let _desc = taos.describe(table_name.as_str())?;
                                // dbg!(desc);
                            }
                        },
                        taos_query::common::JsonMeta::Alter(_) => (),
                        taos_query::common::JsonMeta::Drop(_) => (),
                        taos_query::common::JsonMeta::Delete(_) => (),
                    }

                    // meta data can be write to an database seamlessly by raw or json (to sql).
                    let sql = dbg!(json.to_string());
                    if let Err(err) = taos.exec(sql) {
                        match err.code() {
                            Code::TAG_ALREADY_EXIST => log::trace!("tag already exists"),
                            Code::TAG_NOT_EXIST => log::trace!("tag not exist"),
                            Code::COLUMN_EXISTS => log::trace!("column already exists"),
                            Code::COLUMN_NOT_EXIST => log::trace!("column not exists"),
                            Code::INVALID_COLUMN_NAME => log::trace!("invalid column name"),
                            Code::MODIFIED_ALREADY => log::trace!("modified already done"),
                            Code::TABLE_NOT_EXIST => log::trace!("table does not exists"),
                            Code::STABLE_NOT_EXIST => log::trace!("stable does not exists"),
                            Code::INVALID_ROW_BYTES => log::trace!("invalid row bytes"),
                            Code::DUPLICATED_COLUMN_NAMES => log::trace!("duplicated column names"),
                            Code::NO_COLUMN_CAN_BE_DROPPED => {
                                log::trace!("no column can be dropped")
                            }
                            _ => {
                                log::error!("{:?}", err);
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
                _ => (),
            }
            consumer.commit(offset)?;
        }

        let assignments = consumer.assignments().unwrap();
        log::debug!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                log::debug!(
                    "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                    topic,
                    vgroup_id,
                    current,
                    begin,
                    end
                );
                let res = consumer.offset_seek(topic, vgroup_id, end);
                if res.is_err() {
                    log::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().unwrap();
                    log::error!("assignments: {:?}", a);
                }
            }

        }

        // after seek offset
        let assignments = consumer.assignments().unwrap();
        log::debug!("after seek offset assignments: {:?}", assignments);
        

        consumer.unsubscribe();

        taos.exec_many([
            "drop database sys_tmq_meta_sync2",
            "drop topic sys_tmq_meta_sync",
            "drop database sys_tmq_meta_sync",
        ])?;
        Ok(())
    }

    /// Partial update a record with different columns.
    #[tokio::test]
    async fn test_ts2035() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build().await?;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        taos.exec_many([
            "drop topic if exists sys_ts2035",
            "drop database if exists sys_ts2035",
            "create database sys_ts2035 wal_retention_period 3600",
            "create topic sys_ts2035 as database sys_ts2035",
            "use sys_ts2035",
            "create table tb1 (ts timestamp, c1 int, c2 int)",
        ])
        .await?;

        taos.exec_many([
            format!("insert into tb1 (ts, c1) values({ts}, 0)"),
            format!("insert into tb1 (ts, c2) values({ts}, 1)"),
        ])
        .await?;

        // target
        let target = crate::TaosBuilder::from_dsn("taos:///")?.build().await?;
        target
            .exec_many([
                "drop database if exists sys_ts2035_target",
                "create database sys_ts2035_target wal_retention_period 3600",
                "use sys_ts2035_target",
                "create table tb1 (ts timestamp, c1 int, c2 int)",
            ])
            .await?;

        let builder = TmqBuilder::from_dsn(
            "taos:///?group.id=10&timeout=1000ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build().await?;
        consumer.subscribe(["sys_ts2035"]).await?;

        consumer
            .stream_with_timeout(Timeout::from_millis(500))
            .try_for_each(|(offset, message)| async {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();

                match message {
                    MessageSet::Meta(meta) => {
                        unreachable!()
                    }
                    MessageSet::Data(mut data) => {
                        let raw = data.as_raw_data().await?;
                        // target
                        //     .write_raw_meta(unsafe { std::mem::transmute(raw) })
                        //     .await?;
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.next().transpose()? {
                            dbg!(data.table_name());
                            dbg!(&data);
                            target.write_raw_block(&data).await?;
                        }
                    }
                    _ => (),
                }
                consumer.commit(offset).await?;
                Ok(())
            })
            .await?;

        consumer.unsubscribe().await;

        let (c1, c2) = target
            .query_one::<_, (Option<i32>, Option<i32>)>("select c1, c2 from tb1")
            .await?
            .expect("should have data");

        assert_eq!(c1, Some(0));
        assert_eq!(c2, Some(1));

        taos.exec_many([
            "drop topic sys_ts2035", // drop topic before dropping database
            "drop database sys_ts2035",
        ])
        .await?;
        target.exec("drop database sys_ts2035_target").await?;
        Ok(())
    }

    /// Partial update a record with different columns.
    #[tokio::test]
    async fn test_delete_meta() -> anyhow::Result<()> {
        use taos_query::prelude::*;
        // pretty_env_logger::init_timed();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build().await?;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        taos.exec_many([
            "drop topic if exists sys_delete_meta",
            "drop database if exists sys_delete_meta",
            "create database sys_delete_meta wal_retention_period 3600",
            "create topic sys_delete_meta with meta as database sys_delete_meta",
            "use sys_delete_meta",
            "create table tb1 (ts timestamp, c1 int, c2 int)",
        ])
        .await?;

        taos.exec_many([
            format!("insert into tb1 (ts, c1) values({ts}, 0)"),
            format!("delete from tb1 where ts = {ts}"),
        ])
        .await?;

        // target
        let target = crate::TaosBuilder::from_dsn("taos:///")?.build().await?;
        target
            .exec_many([
                "drop database if exists sys_delete_meta_target",
                "create database sys_delete_meta_target wal_retention_period 3600",
                "use sys_delete_meta_target",
                // "create table tb1 (ts timestamp, c1 int, c2 int)",
            ])
            .await?;

        let builder = TmqBuilder::from_dsn(
            "taos:///?group.id=10&timeout=1000ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build().await?;
        consumer.subscribe(["sys_delete_meta"]).await?;

        consumer
            .stream()
            .try_for_each(|(offset, message)| async {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();

                match message {
                    MessageSet::Meta(meta) => {
                        dbg!(&meta.as_json_meta().await?);
                        target.write_raw_meta(&meta.as_raw_meta().await?).await?;
                    }
                    MessageSet::Data(mut data) => {
                        println!("is data");
                        let raw = data.as_raw_data().await?;
                        let mut has_blocks = false;
                        // target
                        //     .write_raw_meta(unsafe { std::mem::transmute(raw) })
                        //     .await?;
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.next().transpose()? {
                            if !has_blocks {
                                has_blocks = true;
                            }
                            dbg!(data.table_name());
                            dbg!(&data);
                            target.write_raw_block(&data).await?;
                        }
                        if !has_blocks {
                            target
                                .write_raw_meta(&unsafe { std::mem::transmute(raw) })
                                .await?;
                        }
                    }
                    _ => unreachable!(),
                }
                consumer.commit(offset).await?;
                Ok(())
            })
            .await?;

        consumer.unsubscribe().await;

        assert!(target
            .query_one::<_, (Option<i32>, Option<i32>)>("select c1, c2 from tb1")
            .await?
            .is_none());

        // assert_eq!(c1, None);

        taos.exec_many([
            "drop topic sys_delete_meta", // drop topic before dropping database
            "drop database sys_delete_meta",
        ])
        .await?;
        target.exec("drop database sys_delete_meta_target").await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_meta() -> anyhow::Result<()> {
        use futures::TryStreamExt;
        use taos_query::prelude::*;

        // pretty_env_logger::formatted_builder()
        //     .filter_level(log::LevelFilter::Debug)
        //     .init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop topic if exists sys_tmq_meta",
            "drop database if exists sys_tmq_meta",
            "create database sys_tmq_meta wal_retention_period 3600",
            "use sys_tmq_meta",
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
            "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
            "create topic if not exists sys_tmq_meta with meta as database sys_tmq_meta",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists sys_tmq_meta2",
            "create database if not exists sys_tmq_meta2 wal_retention_period 3600",
            "use sys_tmq_meta2",
        ])
        .await?;

        let builder = TmqBuilder::from_dsn("taos:///?group.id=10&timeout=1000ms&auto.offset.reset=earliest")?;
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
                        let sql = json.to_string();
                        log::debug!("{}", sql);
                        if let Err(err) = taos.exec(sql).await {
                            match err.code() {
                                Code::TAG_ALREADY_EXIST => log::trace!("tag already exists"),
                                Code::TAG_NOT_EXIST => log::trace!("tag not exist"),
                                Code::COLUMN_EXISTS => log::trace!("column already exists"),
                                Code::COLUMN_NOT_EXIST => log::trace!("column not exists"),
                                Code::INVALID_COLUMN_NAME => log::trace!("invalid column name"),
                                Code::MODIFIED_ALREADY => log::trace!("modified already done"),
                                Code::TABLE_NOT_EXIST => log::trace!("table does not exists"),
                                Code::STABLE_NOT_EXIST => log::trace!("stable does not exists"),
                                Code::INVALID_ROW_BYTES => log::trace!("invalid row bytes"),
                                Code::DUPLICATED_COLUMN_NAMES => {
                                    log::trace!("duplicated column names")
                                }
                                Code::NO_COLUMN_CAN_BE_DROPPED => {
                                    log::trace!("no column can be dropped")
                                }
                                _ => {
                                    log::error!("{:?}", err);
                                }
                            }
                        }
                    }
                    MessageSet::Data(mut data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(_data) = data.next().transpose()? {
                            // dbg!(data.table_name());
                            // dbg!(data);
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
    #[ignore]
    async fn test_tmq() -> anyhow::Result<()> {
        // pretty_env_logger::formatted_timed_builder()
        //     .filter_level(log::LevelFilter::Info)
        //     .init();

        use taos_query::prelude::*;
        // let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = "taos://localhost:6030".to_string();
        log::info!("dsn: {}", dsn);
        let mut dsn = crate::Dsn::from_str(&dsn)?;

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

        dsn.params.insert("group.id".to_string(), "abc".to_string());
        dsn.params
            .insert("auto.offset.reset".to_string(), "earliest".to_string());

        let builder = TmqBuilder::from_dsn(&dsn)?;
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
                log::debug!(
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
                        log::debug!("Meta");
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
                        log::debug!("Data");
                        // data message may have more than one data block for various tables.
                        while let Some(data) = data.fetch_raw_block().await? {
                            log::debug!("table_name: {:?}", data.table_name());
                            log::debug!("data: {:?}", data);
                        }
                    }
                    MessageSet::MetaData(meta, data) => {
                        log::debug!("MetaData");
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
                            log::debug!("table_name: {:?}", data.table_name());
                            log::debug!("data: {:?}", data);
                        }
                    }
                }
                consumer.commit(offset).await?;
            }
        }

        let assignments = consumer.assignments().await.unwrap();
        log::debug!("assignments: {:?}", assignments);

        // seek offset
        for topic_vec_assignment in assignments {
            let topic = &topic_vec_assignment.0;
            let vec_assignment = topic_vec_assignment.1;
            for assignment in vec_assignment {
                let vgroup_id = assignment.vgroup_id();
                let current = assignment.current_offset();
                let begin = assignment.begin();
                let end = assignment.end();
                log::debug!(
                    "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                    topic,
                    vgroup_id,
                    current,
                    begin,
                    end
                );
                let res = consumer.offset_seek(topic, vgroup_id, end).await;
                if res.is_err() {
                    log::error!("seek offset error: {:?}", res);
                    let a = consumer.assignments().await.unwrap();
                    log::error!("assignments: {:?}", a);
                    // panic!()
                }
            }

            let topic_assignment = consumer.topic_assignment(topic).await;
            log::debug!("topic assignment: {:?}", topic_assignment);
        }

        // after seek offset
        let assignments = consumer.assignments().await.unwrap();
        log::debug!("after seek offset assignments: {:?}", assignments);

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
