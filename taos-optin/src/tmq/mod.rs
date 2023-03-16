// pub(crate) mod ffi;

use std::{fmt::Debug, str::FromStr, sync::Arc, time::Duration};

// pub(crate) use ffi::*;

use itertools::Itertools;
use taos_query::{
    common::{raw_data_t, RawMeta},
    prelude::RawError,
    tmq::{
        AsAsyncConsumer, AsConsumer, AsyncOnSync, IsAsyncData, IsMeta, IsOffset, MessageSet,
        Timeout, VGroupId,
    },
    IntoDsn, RawBlock, TBuilder,
};

use crate::{raw::ApiEntry, raw::RawRes, types::tmq_res_t, TaosBuilder};

// use taos_error::Error;

mod raw;

use raw::RawTmq;

use self::raw::{Conf, Topics};

pub struct TmqBuilder {
    // dsn: Dsn,
    builder: TaosBuilder,
    lib: Arc<ApiEntry>,
    conf: Conf,
    timeout: Timeout,
}

unsafe impl Send for TmqBuilder {}
unsafe impl Sync for TmqBuilder {}

impl TBuilder for TmqBuilder {
    type Target = Consumer;

    type Error = RawError;

    fn available_params() -> &'static [&'static str] {
        &["group.id", "client.id", "timeout", "enable.auto.commit"]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        let mut dsn = dsn
            .into_dsn()
            .map_err(|e| RawError::from_string(format!("Parse dsn error: {}", e)))?;
        let lib = if let Some(path) = dsn.params.remove("libraryPath") {
            ApiEntry::dlopen(path).unwrap()
        } else {
            ApiEntry::default()
        };
        let conf = Conf::from_dsn(&dsn, lib.tmq.unwrap().conf_api)?;
        let timeout = if let Some(timeout) = dsn.params.remove("timeout") {
            Timeout::from_str(&timeout).map_err(RawError::from_any)?
        } else {
            Timeout::from_millis(500)
        };
        Ok(Self {
            builder: TaosBuilder::from_dsn(&dsn).map_err(RawError::from_any)?,
            // dsn,
            lib: Arc::new(lib),
            conf,
            timeout,
        })
    }

    fn client_version() -> &'static str {
        ""
    }

    fn ping(&self, _: &mut Self::Target) -> Result<(), Self::Error> {
        self.build().map(|_| ())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> Result<Self::Target, Self::Error> {
        let ptr = self.conf.build()?;
        let tmq = RawTmq {
            c: self.lib.clone(),
            tmq: self.lib.tmq.unwrap(),
            ptr,
        };
        Ok(Consumer {
            tmq,
            timeout: self.timeout,
        })
    }

    fn server_version(&self) -> Result<&str, Self::Error> {
        self.builder
            .server_version()
            .map_err(|err| RawError::from_any(err))
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

pub struct Meta {
    raw: RawRes,
}

impl AsyncOnSync for Meta {}

impl IsMeta for Meta {
    type Error = RawError;

    fn as_raw_meta(&self) -> Result<RawMeta, Self::Error> {
        let raw = self.raw.tmq_get_raw();

        let mut data = Vec::new();

        data.extend(raw.raw_len.to_le_bytes());

        data.extend(raw.raw_type.to_le_bytes());

        data.extend(unsafe {
            std::slice::from_raw_parts(raw.raw as *const u8, raw.raw_len as usize)
        });
        Ok(RawMeta::new(data.into()))
    }

    fn as_json_meta(&self) -> Result<taos_query::common::JsonMeta, Self::Error> {
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
    type Error = RawError;

    async fn as_raw_data(&self) -> Result<taos_query::common::RawData, Self::Error> {
        Ok(self.raw.tmq_get_raw().into())
    }

    async fn fetch_raw_block(&self) -> Result<Option<RawBlock>, Self::Error> {
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
    type Item = Result<RawBlock, RawError>;

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
    type Error = RawError;

    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> Result<(), Self::Error> {
        let topics = topics.into_iter().map(|item| item.into()).collect_vec();
        let topics = Topics::from_topics(self.tmq.tmq.list_api, &topics)?;
        dbg!(&topics);
        self.tmq.subscribe(&topics)
    }

    fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> Result<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
        Self::Error,
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

    fn commit(&self, offset: Self::Offset) -> Result<(), Self::Error> {
        self.tmq.commit_sync(offset.0.clone()).map(|_| ())
    }
}

// impl AsyncOnSync for Consumer {}

#[async_trait::async_trait]
impl AsAsyncConsumer for Consumer {
    type Error = RawError;

    type Offset = Offset;

    type Meta = Meta;

    type Data = Data;

    async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> Result<(), Self::Error> {
        let topics =
            Topics::from_topics(self.tmq.tmq.list_api, topics.into_iter().map(|s| s.into()))?;
        self.tmq.subscribe(&topics)
    }

    async fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> Result<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
        Self::Error,
    > {
        use taos_query::prelude::tokio;
        log::trace!("Waiting for next message");
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
                log::trace!("Got a new message");
                Ok(res)
            }
            Err(err) => {
                log::warn!("Polling message error: {err:?}");
                Err(err)
            }
        }
    }

    async fn commit(&self, offset: Self::Offset) -> Result<(), Self::Error> {
        self.tmq.commit(offset.0.clone()).await.map(|_| ())
    }

    fn default_timeout(&self) -> Timeout {
        self.timeout
    }
}
#[cfg(test)]
mod tests {
    use super::TmqBuilder;
    use crate::TaosBuilder;

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
            "taos://localhost:6030/db?group.id=5&experimental.snapshot.enable=false",
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
                    dbg!(json);
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;

                    // data
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
            }

            let _ = consumer.commit(offset);
        }

        consumer.unsubscribe();

        let mut query = taos.query("describe stb1")?;
        for row in query.rows() {
            let raw = row?;
            dbg!(raw);
        }
        let mut query = taos.query("select count(*) from stb1")?;
        for row in query.rows() {
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
        pretty_env_logger::formatted_timed_builder()
            .filter_level(log::LevelFilter::Trace)
            .init();

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

        let builder = TmqBuilder::from_dsn("taos://localhost:6030/db?group.id=5")?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        log::trace!("polling start");

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            println!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
                    let json = meta.to_json();
                    dbg!(json);
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
            dbg!(raw);
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
        //     .filter_level(log::LevelFilter::Debug)
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

        let builder = TmqBuilder::from_dsn("taos://localhost:6030?group.id=10&timeout=1000ms")?;
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
                        match err.errno() {
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

    #[tokio::test]
    async fn test_tmq_meta() -> anyhow::Result<()> {
        // use futures::TryStreamExt;
        use taos_query::prelude::*;

        // pretty_env_logger::formatted_builder()
        //     .filter_level(log::LevelFilter::Debug)
        //     .init();

        let taos = crate::TaosBuilder::from_dsn("taos:///")?.build()?;
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

        let builder = TmqBuilder::from_dsn("taos:///?group.id=10&timeout=1000ms")?;
        let mut consumer = builder.build()?;
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
                            match err.errno() {
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
}
