use once_cell::sync::OnceCell;
use taos_query::common::SmlData;
use taos_query::{block_in_place_or_global, common::RawMeta, AsyncQueryable};

pub mod asyn;
pub(crate) mod infra;
// pub mod sync;

pub use asyn::Error;
pub use asyn::ResultSet;
pub(crate) use asyn::WsTaos;
pub(crate) use infra::WsConnReq;

use crate::TaosBuilder;

#[derive(Debug)]
pub struct Taos {
    pub(crate) dsn: TaosBuilder,
    pub(crate) async_client: OnceCell<WsTaos>,
    pub(crate) async_sml: OnceCell<crate::schemaless::WsTaos>,
}

impl Taos {
    pub fn version(&self) -> &str {
        block_in_place_or_global(self.client()).version()
    }

    async fn client(&self) -> &WsTaos {
        if let Some(ws) = self.async_client.get() {
            ws
        } else {
            let async_client = WsTaos::from_wsinfo(&self.dsn).await.unwrap();
            self.async_client.get_or_init(|| async_client)
        }
    }
}

unsafe impl Send for Taos {}
unsafe impl Sync for Taos {}

#[async_trait::async_trait]
impl taos_query::AsyncQueryable for Taos {
    type Error = asyn::Error;

    type AsyncResultSet = asyn::ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        if let Some(ws) = self.async_client.get() {
            ws.s_query(sql.as_ref()).await
        } else {
            let async_client = WsTaos::from_wsinfo(&self.dsn).await?;
            self.async_client
                .get_or_init(|| async_client)
                .s_query(sql.as_ref())
                .await
        }
    }

    async fn query_with_req_id<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
        req_id: u64,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        if let Some(ws) = self.async_client.get() {
            ws.s_query_with_req_id(sql.as_ref(), req_id).await
        } else {
            let async_client = WsTaos::from_wsinfo(&self.dsn).await?;
            self.async_client
                .get_or_init(|| async_client)
                .s_query_with_req_id(sql.as_ref(), req_id)
                .await
        }
    }

    async fn write_raw_meta(&self, raw: &RawMeta) -> Result<(), Self::Error> {
        if let Some(ws) = self.async_client.get() {
            ws.write_meta(raw).await
        } else {
            let async_client = WsTaos::from_wsinfo(&self.dsn).await?;
            self.async_client
                .get_or_init(|| async_client)
                .write_meta(raw)
                .await
        }
    }

    async fn write_raw_block(&self, block: &taos_query::RawBlock) -> Result<(), Self::Error> {
        if let Some(ws) = self.async_client.get() {
            ws.write_raw_block(block).await
        } else {
            let async_client = WsTaos::from_wsinfo(&self.dsn).await?;
            self.async_client
                .get_or_init(|| async_client)
                .write_raw_block(block)
                .await
        }
    }

    async fn put(&self, data: &SmlData) -> Result<(), Self::Error> {
        if let Some(ws) = self.async_sml.get() {
            ws.s_put(data).await
        } else {
            let async_sml = crate::schemaless::WsTaos::from_wsinfo(&self.dsn).await?;
            self.async_sml.get_or_init(|| async_sml).s_put(data).await
        }
    }
}

impl taos_query::Queryable for Taos {
    type Error = asyn::Error;

    type ResultSet = asyn::ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> Result<Self::ResultSet, Self::Error> {
        let sql = sql.as_ref();
        block_in_place_or_global(<Self as AsyncQueryable>::query(self, sql))
    }

    fn query_with_req_id<T: AsRef<str>>(
        &self,
        sql: T,
        req_id: u64,
    ) -> Result<Self::ResultSet, Self::Error> {
        let sql = sql.as_ref();
        block_in_place_or_global(<Self as AsyncQueryable>::query_with_req_id(
            self, sql, req_id,
        ))
    }

    fn write_raw_meta(&self, meta: &RawMeta) -> Result<(), Self::Error> {
        block_in_place_or_global(<Self as AsyncQueryable>::write_raw_meta(self, meta))
    }

    fn write_raw_block(&self, block: &taos_query::RawBlock) -> Result<(), Self::Error> {
        block_in_place_or_global(<Self as AsyncQueryable>::write_raw_block(self, block))
    }

    fn put(&self, _data: &taos_query::common::SmlData) -> Result<(), Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use crate::TaosBuilder;
    #[test]
    fn ws_sync_json() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        use taos_query::prelude::sync::*;
        let client = TaosBuilder::from_dsn("taosws://localhost:6041/")?.build()?;
        let db = "ws_sync_json";
        assert_eq!(client.exec(format!("drop database if exists {db}"))?, 0);
        assert_eq!(client.exec(format!("create database {db} keep 36500"))?, 0);
        assert_eq!(
            client.exec(
                format!("create table {db}.stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16)) tags (jt json)")
            )?,
            0
        );
        assert_eq!(
            client.exec(format!(
                r#"insert into {db}.tb1 using {db}.stb1 tags('{{"key":"数据"}}')
                   values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
            ))?,
            2
        );
        assert_eq!(
            client.exec(format!(
                r#"insert into {db}.tb2 using {db}.stb1 tags(NULL)
                   values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
            ))?,
            2
        );

        // let mut rs = client.s_query("select * from ws_sync.tb1").unwrap().unwrap();
        let mut rs = client.query(format!("select * from {db}.tb1 order by ts limit 1"))?;

        #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            b1: bool,
            c8i1: i8,
            c16i1: i16,
            c32i1: i32,
            c64i1: i64,
            c8u1: u8,
            c16u1: u16,
            c32u1: u32,
            c64u1: u64,

            c8i2: i8,
            c16i2: i16,
            c32i2: i32,
            c64i2: i64,
            c8u2: u8,
            c16u2: u16,
            c32u2: u32,
            c64u2: u64,

            cb1: String,
            cb2: String,
            cn1: String,
            cn2: String,
        }

        use itertools::Itertools;
        let values: Vec<A> = rs.deserialize::<A>().try_collect()?;

        dbg!(&values);

        assert_eq!(
            values[0],
            A {
                ts: "1970-01-01T08:00:00+08:00".to_string(),
                b1: true,
                c8i1: -1,
                c16i1: -2,
                c32i1: -3,
                c64i1: -4,
                c8u1: 1,
                c16u1: 2,
                c32u1: 3,
                c64u1: 4,
                c8i2: -5,
                c16i2: -6,
                c32i2: -7,
                c64i2: -8,
                c8u2: 5,
                c16u2: 6,
                c32u2: 7,
                c64u2: 8,
                cb1: "abc".to_string(),
                cb2: "def".to_string(),
                cn1: "涛思".to_string(),
                cn2: "数据".to_string(),
            }
        );

        assert_eq!(client.exec(format!("drop database {db}"))?, 0);
        Ok(())
    }

    #[test]
    fn ws_sync() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let client = TaosBuilder::from_dsn("ws://localhost:6041/")?.build()?;
        assert_eq!(client.exec("drop database if exists ws_sync")?, 0);
        assert_eq!(client.exec("create database ws_sync keep 36500")?, 0);
        assert_eq!(
            client.exec(
                "create table ws_sync.tb1(ts timestamp,\
                    c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16))"
            )?,
            0
        );
        assert_eq!(
            client.exec(
                "insert into ws_sync.tb1 values(65535,\
                -1,-2,-3,-4, 1,2,3,4, 'abc', '涛思',\
                -5,-6,-7,-8, 5,6,7,8, 'def', '数据')"
            )?,
            1
        );

        let mut rs = client.query("select * from ws_sync.tb1")?;

        #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            c8i1: i8,
            c16i1: i16,
            c32i1: i32,
            c64i1: i64,
            c8u1: u8,
            c16u1: u16,
            c32u1: u32,
            c64u1: u64,

            c8i2: i8,
            c16i2: i16,
            c32i2: i32,
            c64i2: i64,
            c8u2: u8,
            c16u2: u16,
            c32u2: u32,
            c64u2: u64,

            cb1: String,
            cb2: String,
            cn1: String,
            cn2: String,
        }

        use itertools::Itertools;
        let values: Vec<A> = rs.deserialize::<A>().try_collect()?;

        dbg!(&values);

        assert_eq!(
            values[0],
            A {
                ts: "1970-01-01T08:01:05.535+08:00".to_string(),
                c8i1: -1,
                c16i1: -2,
                c32i1: -3,
                c64i1: -4,
                c8u1: 1,
                c16u1: 2,
                c32u1: 3,
                c64u1: 4,
                c8i2: -5,
                c16i2: -6,
                c32i2: -7,
                c64i2: -8,
                c8u2: 5,
                c16u2: 6,
                c32u2: 7,
                c64u2: 8,
                cb1: "abc".to_string(),
                cb2: "def".to_string(),
                cn1: "涛思".to_string(),
                cn2: "数据".to_string(),
            }
        );

        assert_eq!(client.exec("drop database ws_sync")?, 0);
        Ok(())
    }

    #[test]
    fn ws_show_databases() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos:///".to_string());

        let client = TaosBuilder::from_dsn(dsn)?.build()?;
        let mut rs = client.query("show databases")?;
        let values = rs.to_rows_vec()?;

        dbg!(values);
        Ok(())
    }

    // #[tokio::test(flavor = "multi_thread")]
    async fn _ws_select_from_meters() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "info");
        // pretty_env_logger::init_timed();
        use taos_query::prelude::*;
        let dsn = "taos+ws:///test";
        let client = TaosBuilder::from_dsn(dsn)?.build().await?;

        let mut rs = client.query("select * from meters").await?;

        let mut blocks = rs.blocks();
        let mut nb = 0;
        let mut nr = 0;
        while let Some(block) = blocks.try_next().await? {
            nb += 1;
            nr += block.nrows();
        }
        let summary = rs.summary();
        dbg!(summary, (nb, nr));
        Ok(())
    }
    #[cfg(feature = "async")]
    // !Websocket tests should always use `multi_thread`
    #[tokio::test(flavor = "multi_thread")]
    async fn test_client() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        use futures::TryStreamExt;
        use taos_query::{AsyncFetchable, AsyncQueryable};

        let client = TaosBuilder::from_dsn("ws://localhost:6041/")?.build()?;
        assert_eq!(
            client
                .exec("create database if not exists ws_test_client")
                .await?,
            0
        );
        assert_eq!(
            client
                .exec("create table if not exists ws_test_client.tb1(ts timestamp, v int)")
                .await?,
            0
        );
        assert_eq!(
            client
                .exec("insert into ws_test_client.tb1 values(1655793421375, 1)")
                .await?,
            1
        );

        // let mut rs = client.s_query("select * from ws_test_client.tb1").unwrap().unwrap();
        let mut rs = client.query("select * from ws_test_client.tb1").await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: i32,
        }

        let values: Vec<A> = rs.deserialize_stream().try_collect().await?;

        dbg!(values);

        assert_eq!(client.exec("drop database ws_test_client").await?, 0);
        Ok(())
    }
}
