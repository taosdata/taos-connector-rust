use std::time::Duration;

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Dsn(#[from] DsnError),
    #[error(transparent)]
    Raw(#[from] RawError),
    #[error(transparent)]
    Native(#[from] crate::sys::Error),
    #[error(transparent)]
    Ws(#[from] taos_ws::Error),
    #[error(transparent)]
    WsQueryError(#[from] taos_ws::query::asyn::Error),
    #[error(transparent)]
    WsTmqError(#[from] taos_ws::consumer::Error),
    #[error(transparent)]
    Any(#[from] anyhow::Error),
}
#[derive(Debug)]
enum TaosBuilderInner {
    Native(crate::sys::TaosBuilder),
    Ws(taos_ws::TaosBuilder),
}
#[derive(Debug)]
pub(super) enum TaosInner {
    Native(crate::sys::Taos),
    Ws(taos_ws::Taos),
}

enum ResultSetInner {
    Native(crate::sys::ResultSet),
    Ws(taos_ws::ResultSet),
}
#[derive(Debug)]
pub struct TaosBuilder(TaosBuilderInner);
#[derive(Debug)]
pub struct Taos(pub(super) TaosInner);
pub struct ResultSet(ResultSetInner);

impl taos_query::TBuilder for TaosBuilder {
    type Target = Taos;

    type Error = Error;

    fn available_params() -> &'static [&'static str] {
        &[]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        let mut dsn = dsn.into_dsn()?;
        if dsn.params.contains_key("token") {
            dsn.protocol = Some("ws".to_string());
        }
        // dbg!(&dsn);
        use taos_query::TBuilder;
        match (dsn.driver.as_str(), dsn.protocol.as_deref()) {
            ("ws" | "wss" | "http" | "https" | "taosws" | "taoswss", _) => Ok(Self(
                TaosBuilderInner::Ws(taos_ws::TaosBuilder::from_dsn(dsn)?),
            )),
            ("taos" | "tmq", None) => Ok(Self(TaosBuilderInner::Native(
                <crate::sys::TaosBuilder as TBuilder>::from_dsn(dsn)?,
            ))),
            ("taos" | "tmq", Some("ws" | "wss" | "http" | "https")) => Ok(Self(
                TaosBuilderInner::Ws(taos_ws::TaosBuilder::from_dsn(dsn)?),
            )),
            (driver, _) => Err(DsnError::InvalidDriver(driver.to_string()).into()),
        }
    }

    fn client_version() -> &'static str {
        ""
    }

    fn ping(&self, conn: &mut Self::Target) -> Result<(), Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => match &mut conn.0 {
                TaosInner::Native(taos) => {
                    Ok(<sys::TaosBuilder as taos_query::TBuilder>::ping(b, taos)?)
                }
                _ => unreachable!(),
            },
            TaosBuilderInner::Ws(b) => match &mut conn.0 {
                TaosInner::Ws(taos) => Ok(<taos_ws::TaosBuilder as taos_query::TBuilder>::ping(
                    b, taos,
                )?),
                _ => unreachable!(),
            },
        }
    }

    fn ready(&self) -> bool {
        match &self.0 {
            TaosBuilderInner::Native(b) => <sys::TaosBuilder as taos_query::TBuilder>::ready(b),
            TaosBuilderInner::Ws(b) => <taos_ws::TaosBuilder as taos_query::TBuilder>::ready(b),
        }
    }

    fn build(&self) -> Result<Self::Target, Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => Ok(Taos(TaosInner::Native(
                <sys::TaosBuilder as taos_query::TBuilder>::build(b)?,
            ))),
            TaosBuilderInner::Ws(b) => Ok(Taos(TaosInner::Ws(
                <taos_ws::TaosBuilder as taos_query::TBuilder>::build(b)?,
            ))),
        }
    }

    fn server_version(&self) -> Result<&str, Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => Ok(
                <sys::TaosBuilder as taos_query::TBuilder>::server_version(b)?,
            ),
            TaosBuilderInner::Ws(b) => {
                Ok(<taos_ws::TaosBuilder as taos_query::TBuilder>::server_version(b)?)
            }
        }
    }

    fn is_enterprise_edition(&self) -> Result<bool, Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => {
                Ok(<sys::TaosBuilder as taos_query::TBuilder>::is_enterprise_edition(b)?)
            }
            TaosBuilderInner::Ws(b) => {
                Ok(<taos_ws::TaosBuilder as taos_query::TBuilder>::is_enterprise_edition(b)?)
            }
        }
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TaosBuilder {
    type Target = Taos;

    type Error = Error;

    fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        let mut dsn = dsn.into_dsn()?;
        if dsn.params.contains_key("token") {
            dsn.protocol = Some("ws".to_string());
        }
        // dbg!(&dsn);
        use taos_query::TBuilder;
        match (dsn.driver.as_str(), dsn.protocol.as_deref()) {
            ("ws" | "wss" | "http" | "https" | "taosws" | "taoswss", _) => Ok(Self(
                TaosBuilderInner::Ws(taos_ws::TaosBuilder::from_dsn(dsn)?),
            )),
            ("taos" | "tmq", None) => Ok(Self(TaosBuilderInner::Native(
                <crate::sys::TaosBuilder as TBuilder>::from_dsn(dsn)?,
            ))),
            ("taos" | "tmq", Some("ws" | "wss" | "http" | "https")) => Ok(Self(
                TaosBuilderInner::Ws(taos_ws::TaosBuilder::from_dsn(dsn)?),
            )),
            (driver, _) => Err(DsnError::InvalidDriver(driver.to_string()).into()),
        }
    }

    fn client_version() -> &'static str {
        ""
    }

    async fn ping(&self, conn: &mut Self::Target) -> Result<(), Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => match &mut conn.0 {
                TaosInner::Native(taos) => Ok(b.ping(taos).await?),
                _ => unreachable!(),
            },
            TaosBuilderInner::Ws(b) => match &mut conn.0 {
                TaosInner::Ws(taos) => Ok(b.ping(taos).await?),
                _ => unreachable!(),
            },
        }
    }

    async fn ready(&self) -> bool {
        match &self.0 {
            TaosBuilderInner::Native(b) => {
                <sys::TaosBuilder as taos_query::AsyncTBuilder>::ready(b).await
            }
            TaosBuilderInner::Ws(b) => b.ready().await,
        }
    }

    async fn build(&self) -> Result<Self::Target, Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => Ok(Taos(TaosInner::Native(b.build().await?))),
            TaosBuilderInner::Ws(b) => Ok(Taos(TaosInner::Ws(b.build().await?))),
        }
    }

    async fn server_version(&self) -> Result<&str, Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => {
                Ok(<sys::TaosBuilder as taos_query::AsyncTBuilder>::server_version(b).await?)
            }
            TaosBuilderInner::Ws(b) => {
                Ok(<taos_ws::TaosBuilder as taos_query::AsyncTBuilder>::server_version(b).await?)
            }
        }
    }

    async fn is_enterprise_edition(&self) -> Result<bool, Self::Error> {
        match &self.0 {
            TaosBuilderInner::Native(b) => Ok(
                <sys::TaosBuilder as taos_query::AsyncTBuilder>::is_enterprise_edition(b).await?,
            ),
            TaosBuilderInner::Ws(b) => Ok(b.is_enterprise_edition().await?),
        }
    }
}

impl AsyncFetchable for ResultSet {
    type Error = Error;

    fn affected_rows(&self) -> i32 {
        match &self.0 {
            ResultSetInner::Native(rs) => {
                <crate::sys::ResultSet as AsyncFetchable>::affected_rows(rs)
            }
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::affected_rows(rs),
        }
    }

    fn precision(&self) -> Precision {
        match &self.0 {
            ResultSetInner::Native(rs) => <crate::sys::ResultSet as AsyncFetchable>::precision(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::precision(rs),
        }
    }

    fn fields(&self) -> &[Field] {
        match &self.0 {
            ResultSetInner::Native(rs) => <crate::sys::ResultSet as AsyncFetchable>::fields(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::fields(rs),
        }
    }

    fn summary(&self) -> (usize, usize) {
        match &self.0 {
            ResultSetInner::Native(rs) => <crate::sys::ResultSet as AsyncFetchable>::summary(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::summary(rs),
        }
    }

    fn update_summary(&mut self, nrows: usize) {
        match &mut self.0 {
            ResultSetInner::Native(rs) => {
                <crate::sys::ResultSet as AsyncFetchable>::update_summary(rs, nrows)
            }
            ResultSetInner::Ws(rs) => {
                <taos_ws::ResultSet as AsyncFetchable>::update_summary(rs, nrows)
            }
        }
    }

    fn fetch_raw_block(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<RawBlock>, Self::Error>> {
        match &mut self.0 {
            ResultSetInner::Native(rs) => {
                <crate::sys::ResultSet as AsyncFetchable>::fetch_raw_block(rs, cx)
                    .map_err(Into::into)
            }
            ResultSetInner::Ws(rs) => {
                <taos_ws::ResultSet as AsyncFetchable>::fetch_raw_block(rs, cx).map_err(Into::into)
            }
        }
    }
}

impl taos_query::Fetchable for ResultSet {
    type Error = Error;

    fn affected_rows(&self) -> i32 {
        match &self.0 {
            ResultSetInner::Native(rs) => {
                <crate::sys::ResultSet as AsyncFetchable>::affected_rows(rs)
            }
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::affected_rows(rs),
        }
    }

    fn precision(&self) -> Precision {
        match &self.0 {
            ResultSetInner::Native(rs) => <crate::sys::ResultSet as AsyncFetchable>::precision(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::precision(rs),
        }
    }

    fn fields(&self) -> &[Field] {
        match &self.0 {
            ResultSetInner::Native(rs) => <crate::sys::ResultSet as AsyncFetchable>::fields(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::fields(rs),
        }
    }

    fn summary(&self) -> (usize, usize) {
        match &self.0 {
            ResultSetInner::Native(rs) => <crate::sys::ResultSet as AsyncFetchable>::summary(rs),
            ResultSetInner::Ws(rs) => <taos_ws::ResultSet as AsyncFetchable>::summary(rs),
        }
    }

    fn update_summary(&mut self, nrows: usize) {
        match &mut self.0 {
            ResultSetInner::Native(rs) => {
                <crate::sys::ResultSet as AsyncFetchable>::update_summary(rs, nrows)
            }
            ResultSetInner::Ws(rs) => {
                <taos_ws::ResultSet as AsyncFetchable>::update_summary(rs, nrows)
            }
        }
    }

    fn fetch_raw_block(&mut self) -> Result<Option<RawBlock>, Self::Error> {
        match &mut self.0 {
            ResultSetInner::Native(rs) => {
                <crate::sys::ResultSet as taos_query::Fetchable>::fetch_raw_block(rs)
                    .map_err(Into::into)
            }
            ResultSetInner::Ws(rs) => {
                <taos_ws::ResultSet as taos_query::Fetchable>::fetch_raw_block(rs)
                    .map_err(Into::into)
            }
        }
    }
}

#[async_trait::async_trait]
impl AsyncQueryable for Taos {
    type Error = Error;

    type AsyncResultSet = ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        log::trace!("Query with SQL: {}", sql.as_ref());
        match &self.0 {
            TaosInner::Native(taos) => taos
                .query(sql)
                .await
                .map(ResultSetInner::Native)
                .map(ResultSet)
                .map_err(Into::into),
            TaosInner::Ws(taos) => taos
                .query(sql)
                .await
                .map(ResultSetInner::Ws)
                .map(ResultSet)
                .map_err(Into::into),
        }
    }

    async fn query_with_req_id<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
        req_id: u64,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        log::trace!("Query with SQL: {}", sql.as_ref());
        match &self.0 {
            TaosInner::Native(_) => todo!(),
            TaosInner::Ws(taos) => taos
                .query_with_req_id(sql, req_id)
                .await
                .map(ResultSetInner::Ws)
                .map(ResultSet)
                .map_err(Into::into),
        }
    }

    async fn write_raw_meta(&self, meta: &RawMeta) -> Result<(), Self::Error> {
        loop {
            let ok: Result<(), Self::Error> = match &self.0 {
                TaosInner::Native(taos) => taos.write_raw_meta(meta).await.map_err(Into::into),
                TaosInner::Ws(taos) => taos.write_raw_meta(meta).await.map_err(Into::into),
            };
            if let Err(err) = ok {
                if err.to_string().contains("0x032C") {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                } else {
                    break Err(err);
                }
            }
            break Ok(());
        }
    }

    async fn write_raw_block(&self, block: &RawBlock) -> Result<(), Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => taos.write_raw_block(block).await.map_err(Into::into),
            TaosInner::Ws(taos) => taos.write_raw_block(block).await.map_err(Into::into),
        }
    }

    async fn put(&self, data: &taos_query::common::SmlData) -> Result<(), Self::Error> {
        match &self.0 {
            TaosInner::Native(_) => todo!(),
            TaosInner::Ws(taos) => taos.put(data).await.map_err(Into::into),
        }
    }
}

impl taos_query::Queryable for Taos {
    type Error = Error;

    type ResultSet = ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> Result<Self::ResultSet, Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => {
                <crate::sys::Taos as taos_query::Queryable>::query(taos, sql)
                    .map(ResultSetInner::Native)
                    .map(ResultSet)
                    .map_err(Into::into)
            }
            TaosInner::Ws(taos) => <taos_ws::Taos as taos_query::Queryable>::query(taos, sql)
                .map(ResultSetInner::Ws)
                .map(ResultSet)
                .map_err(Into::into),
        }
    }

    fn query_with_req_id<T: AsRef<str>>(
        &self,
        sql: T,
        req_id: u64,
    ) -> Result<Self::ResultSet, Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => {
                <crate::sys::Taos as taos_query::Queryable>::query_with_req_id(taos, sql, req_id)
                    .map(ResultSetInner::Native)
                    .map(ResultSet)
                    .map_err(Into::into)
            }
            TaosInner::Ws(taos) => {
                <taos_ws::Taos as taos_query::Queryable>::query_with_req_id(taos, sql, req_id)
                    .map(ResultSetInner::Ws)
                    .map(ResultSet)
                    .map_err(Into::into)
            }
        }
    }

    fn write_raw_meta(&self, meta: &RawMeta) -> Result<(), Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => {
                <crate::sys::Taos as taos_query::Queryable>::write_raw_meta(taos, meta)
                    .map_err(Into::into)
            }
            TaosInner::Ws(taos) => {
                <taos_ws::Taos as taos_query::Queryable>::write_raw_meta(taos, meta)
                    .map_err(Into::into)
            }
        }
    }

    fn write_raw_block(&self, block: &RawBlock) -> Result<(), Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => {
                <crate::sys::Taos as taos_query::Queryable>::write_raw_block(taos, block)
                    .map_err(Into::into)
            }
            TaosInner::Ws(taos) => {
                <taos_ws::Taos as taos_query::Queryable>::write_raw_block(taos, block)
                    .map_err(Into::into)
            }
        }
    }

    fn put(&self, data: &taos_query::common::SmlData) -> Result<(), Self::Error> {
        match &self.0 {
            TaosInner::Native(taos) => {
                <crate::sys::Taos as taos_query::Queryable>::put(taos, data).map_err(Into::into)
            }
            TaosInner::Ws(taos) => {
                <taos_ws::Taos as taos_query::Queryable>::put(taos, data).map_err(Into::into)
            }
        }
    }
}
#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use taos_query::common::SchemalessPrecision;
    use taos_query::common::SchemalessProtocol;
    use taos_query::common::SmlDataBuilder;
    use taos_query::{common::Timestamp, TBuilder};

    use super::TaosBuilder;

    #[test]
    fn builder() {
        let builder = TaosBuilder::from_dsn("taos://").unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());
    }

    #[test]
    fn test_server_version() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(&dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        println!("server version: {:?}", builder.server_version()?);
        assert!(builder.server_version().is_ok());

        Ok(())
    }

    #[test]
    fn test_server_is_enterprise_edition() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(&dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        println!(
            "is enterprise edition: {:?}",
            builder.is_enterprise_edition()
        );

        Ok(())
    }

    #[test]
    fn test_server_version_ws() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_WS_DSN").unwrap_or("taosws://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(&dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        println!("server version: {:?}", builder.server_version()?);
        assert!(builder.server_version().is_ok());

        Ok(())
    }

    #[test]
    fn test_server_is_enterprise_edition_ws() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_WS_DSN").unwrap_or("taosws://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(&dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        println!(
            "is enterprise edition: {:?}",
            builder.is_enterprise_edition()
        );

        Ok(())
    }

    #[test]
    fn sync_json_test_native() -> anyhow::Result<()> {
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://".to_string());
        sync_json_test(&dsn, "taos")
    }

    #[cfg(feature = "ws")]
    #[test]
    fn sync_json_test_ws() -> anyhow::Result<()> {
        sync_json_test("ws://", "ws")
    }

    #[cfg(feature = "ws")]
    #[test]
    fn sync_json_test_taosws() -> anyhow::Result<()> {
        sync_json_test("taosws://", "taosws")
    }

    #[test]
    fn null_test() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let taos = TaosBuilder::from_dsn(&dsn)?.build()?;
        taos.exec_many(["drop database if exists db", "create database db", "use db"])?;

        taos.exec(
            "create table st(ts timestamp, c1 TINYINT UNSIGNED) tags(utntag TINYINT UNSIGNED)",
        )?;
        taos.exec("create table t1 using st tags(0)")?;
        taos.exec("insert into t1 values(1640000000000, 0)")?;
        taos.exec("create table t2 using st tags(254)")?;
        taos.exec("insert into t2 values(1640000000000, 254)")?;
        taos.exec("create table t3 using st tags(NULL)")?;
        taos.exec("insert into t3 values(1640000000000, NULL)")?;

        let mut rs = taos.query("select * from st where utntag is null")?;
        for row in rs.rows() {
            let row = row?;
            let values = row.into_values();
            assert_eq!(values[1], Value::Null(Ty::UTinyInt));
            assert_eq!(values[2], Value::Null(Ty::UTinyInt));
        }
        Ok(())
    }

    #[test]
    fn query_with_req_id_ws() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_WS_DSN").unwrap_or("taosws://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let taos = TaosBuilder::from_dsn(&dsn)?.build()?;
        let db = "test_reqid_ws";
        taos.exec_many([
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("use {db}"),
        ])?;

        taos.exec(
            "create table st(ts timestamp, c1 TINYINT UNSIGNED) tags(utntag TINYINT UNSIGNED)",
        )?;
        taos.exec("create table t1 using st tags(0)")?;
        taos.exec("insert into t1 values(1640000000000, 0)")?;
        taos.exec("create table t2 using st tags(254)")?;
        taos.exec("insert into t2 values(1640000000000, 254)")?;
        taos.exec("create table t3 using st tags(NULL)")?;
        taos.exec("insert into t3 values(1640000000000, NULL)")?;

        let mut rs = taos.query_with_req_id("select * from st where utntag is null", 123)?;
        for row in rs.rows() {
            let row = row?;
            let values = row.into_values();
            assert_eq!(values[1], Value::Null(Ty::UTinyInt));
            assert_eq!(values[2], Value::Null(Ty::UTinyInt));
        }
        taos.exec(format!("drop database {db}"))?;
        Ok(())
    }

    #[test]
    fn query_with_req_id_native() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let taos = TaosBuilder::from_dsn(&dsn)?.build()?;
        let db = "test_reqid_native";
        taos.exec_many([
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("use {db}"),
        ])?;

        taos.exec(
            "create table st(ts timestamp, c1 TINYINT UNSIGNED) tags(utntag TINYINT UNSIGNED)",
        )?;
        taos.exec("create table t1 using st tags(0)")?;
        taos.exec("insert into t1 values(1640000000000, 0)")?;
        taos.exec("create table t2 using st tags(254)")?;
        taos.exec("insert into t2 values(1640000000000, 199)")?;
        taos.exec("create table t3 using st tags(NULL)")?;
        taos.exec("insert into t3 values(1640000000000, NULL)")?;

        let mut rs = taos.query_with_req_id("select * from st where utntag is null", 123)?;
        for row in rs.rows() {
            let row = row?;
            let values = row.into_values();
            assert_eq!(values[1], Value::Null(Ty::UTinyInt));
            assert_eq!(values[2], Value::Null(Ty::UTinyInt));
        }
        taos.exec(format!("drop database {db}"))?;
        Ok(())
    }

    fn sync_json_test(dsn: &str, db: &str) -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        use taos_query::{Fetchable, Queryable};
        let client = TaosBuilder::from_dsn(dsn)?.build()?;
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

        // let mut rs = client.s_query("select * from wsabc.tb1").unwrap().unwrap();
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
                ts: format!(
                    "{}",
                    Value::Timestamp(Timestamp::new(0, Precision::Millisecond))
                ),
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
    fn test_put_line() -> anyhow::Result<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        use taos_query::prelude::sync::*;

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_schemaless";

        client.exec(format!("drop database if exists {db}"))?;

        client.exec(format!("create database if not exists {db}"))?;

        // should specify database before insert
        client.exec(format!("use {db}"))?;

        let data = [
            "measurement,host=host1 field1=2i,field2=2.0 1577837300000",
            "measurement,host=host1 field1=2i,field2=2.0 1577837400000",
            "measurement,host=host1 field1=2i,field2=2.0 1577837500000",
            "measurement,host=host1 field1=2i,field2=2.0 1577837600000",
        ]
        .map(String::from)
        .to_vec();

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .req_id(100u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .req_id(101u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .data(data)
            .req_id(103u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        client.exec(format!("drop database if exists {db}"))?;

        Ok(())
    }

    #[test]
    fn test_put_telnet() -> anyhow::Result<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        use taos_query::prelude::sync::*;

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_schemaless";

        client.exec(format!("drop database if exists {db}"))?;

        client.exec(format!("create database if not exists {db}"))?;

        // should specify database before insert
        client.exec(format!("use {db}"))?;

        let data = [
            "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2",
            "meters.current 1648432611250 12.6 location=California.SanFrancisco group=2",
            "meters.current 1648432611249 10.8 location=California.LosAngeles group=3",
            "meters.current 1648432611250 11.3 location=California.LosAngeles group=3",
            "meters.voltage 1648432611249 219 location=California.SanFrancisco group=2",
            "meters.voltage 1648432611250 218 location=California.SanFrancisco group=2",
            "meters.voltage 1648432611249 221 location=California.LosAngeles group=3",
            "meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
        ]
        .map(String::from)
        .to_vec();

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .req_id(100u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .req_id(101u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .data(data)
            .req_id(103u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        client.exec(format!("drop database if exists {db}"))?;

        Ok(())
    }

    #[test]
    fn test_put_json() -> anyhow::Result<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        use taos_query::prelude::sync::*;

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_schemaless";

        client.exec(format!("drop database if exists {db}"))?;

        client.exec(format!("create database if not exists {db}"))?;

        // should specify database before insert
        client.exec(format!("use {db}"))?;

        // SchemalessProtocol::Json
        let data = [
            r#"[{"metric": "meters.current", "timestamp": 1681345954000, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "California.LosAngeles", "groupid": 1}}, {"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}}]"#
        ]
        .map(String::from)
        .to_vec();

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .req_id(300u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .data(data.clone())
            .ttl(1000)
            .req_id(301u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .data(data.clone())
            .req_id(302u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .data(data.clone())
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        client.exec(format!("drop database if exists {db}"))?;

        Ok(())
    }
}

#[cfg(test)]
mod async_tests {
    use taos_query::common::SchemalessPrecision;
    use taos_query::common::SchemalessProtocol;
    use taos_query::common::SmlDataBuilder;

    use crate::AsyncQueryable;
    use crate::AsyncTBuilder;
    use crate::TaosBuilder;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_put() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        put_line().await?;
        put_telnet().await?;
        put_json().await
    }

    async fn put_line() -> anyhow::Result<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        // std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build().await?;

        let db = "test_schemaless_ws_line";

        client.exec(format!("drop database if exists {db}")).await?;

        client
            .exec(format!("create database if not exists {db}"))
            .await?;

        // should specify database before insert
        client.exec(format!("use {db}")).await?;

        let data = [
            "measurement,host=host1 field1=2i,field2=2.0 1577837300000",
            "measurement,host=host1 field1=2i,field2=2.0 1577837400000",
            "measurement,host=host1 field1=2i,field2=2.0 1577837500000",
            "measurement,host=host1 field1=2i,field2=2.0 1577837600000",
        ]
        .map(String::from)
        .to_vec();

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .req_id(100u64)
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .req_id(101u64)
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .data(data)
            .req_id(103u64)
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        client.exec(format!("drop database if exists {db}")).await?;

        Ok(())
    }

    async fn put_telnet() -> anyhow::Result<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build().await?;

        let db = "test_schemaless_ws_telnet";

        client.exec(format!("drop database if exists {db}")).await?;

        client
            .exec(format!("create database if not exists {db}"))
            .await?;

        // should specify database before insert
        client.exec(format!("use {db}")).await?;

        let data = [
            "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2",
            "meters.current 1648432611250 12.6 location=California.SanFrancisco group=2",
            "meters.current 1648432611249 10.8 location=California.LosAngeles group=3",
            "meters.current 1648432611250 11.3 location=California.LosAngeles group=3",
            "meters.voltage 1648432611249 219 location=California.SanFrancisco group=2",
            "meters.voltage 1648432611250 218 location=California.SanFrancisco group=2",
            "meters.voltage 1648432611249 221 location=California.LosAngeles group=3",
            "meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
        ]
        .map(String::from)
        .to_vec();

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .req_id(200u64)
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .data(data.clone())
            .ttl(1000)
            .req_id(201u64)
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .data(data.clone())
            .req_id(202u64)
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .data(data.clone())
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        client.exec(format!("drop database if exists {db}")).await?;

        Ok(())
    }

    async fn put_json() -> anyhow::Result<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build().await?;

        let db = "test_schemaless_ws";

        client.exec(format!("drop database if exists {db}")).await?;

        client
            .exec(format!("create database if not exists {db}"))
            .await?;

        // should specify database before insert
        client.exec(format!("use {db}")).await?;

        // SchemalessProtocol::Json
        let data = [
            r#"[{"metric": "meters.current", "timestamp": 1681345954000, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "California.LosAngeles", "groupid": 1}}, {"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}}]"#
        ]
        .map(String::from)
        .to_vec();

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .req_id(300u64)
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .data(data.clone())
            .ttl(1000)
            .req_id(301u64)
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .data(data.clone())
            .req_id(302u64)
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .data(data.clone())
            .build()?;
        assert_eq!(client.put(&sml_data).await?, ());

        client.exec(format!("drop database if exists {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_is_enterprise_edition() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?;
        log::debug!("client: {:?}", &client);
        log::debug!("is_enterprise: {:?}", client.is_enterprise_edition().await?);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_is_enterprise_edition_ws() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?;
        log::debug!("client: {:?}", &client);
        log::debug!("is_enterprise: {:?}", client.is_enterprise_edition().await?);
        Ok(())
    }
}
