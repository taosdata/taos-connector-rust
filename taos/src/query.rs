use std::time::Duration;

use taos_query::util::Edition;

use super::*;

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

impl Taos {
    /// The connection uses native protocol.
    pub fn is_native(&self) -> bool {
        matches!(&self.0, TaosInner::Native(_))
    }

    /// The connection uses websocket protocol.
    pub fn is_ws(&self) -> bool {
        matches!(&self.0, TaosInner::Native(_))
    }
}
pub struct ResultSet(ResultSetInner);

impl taos_query::TBuilder for TaosBuilder {
    type Target = Taos;

    fn available_params() -> &'static [&'static str] {
        &[]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
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

    fn ping(&self, conn: &mut Self::Target) -> RawResult<()> {
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

    fn build(&self) -> RawResult<Self::Target> {
        match &self.0 {
            TaosBuilderInner::Native(b) => Ok(Taos(TaosInner::Native(
                <sys::TaosBuilder as taos_query::TBuilder>::build(b)?,
            ))),
            TaosBuilderInner::Ws(b) => Ok(Taos(TaosInner::Ws(
                <taos_ws::TaosBuilder as taos_query::TBuilder>::build(b)?,
            ))),
        }
    }

    fn server_version(&self) -> RawResult<&str> {
        match &self.0 {
            TaosBuilderInner::Native(b) => Ok(
                <sys::TaosBuilder as taos_query::TBuilder>::server_version(b)?,
            ),
            TaosBuilderInner::Ws(b) => {
                Ok(<taos_ws::TaosBuilder as taos_query::TBuilder>::server_version(b)?)
            }
        }
    }

    fn is_enterprise_edition(&self) -> RawResult<bool> {
        match &self.0 {
            TaosBuilderInner::Native(b) => {
                Ok(<sys::TaosBuilder as taos_query::TBuilder>::is_enterprise_edition(b)?)
            }
            TaosBuilderInner::Ws(b) => {
                Ok(<taos_ws::TaosBuilder as taos_query::TBuilder>::is_enterprise_edition(b)?)
            }
        }
    }

    fn get_edition(&self) -> RawResult<Edition> {
        match &self.0 {
            TaosBuilderInner::Native(b) => {
                Ok(<sys::TaosBuilder as taos_query::TBuilder>::get_edition(b)?)
            }
            TaosBuilderInner::Ws(b) => Ok(
                <taos_ws::TaosBuilder as taos_query::TBuilder>::get_edition(b)?,
            ),
        }
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TaosBuilder {
    type Target = Taos;

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        let mut dsn = dsn.into_dsn()?;
        if dsn.params.contains_key("token") && dsn.protocol.is_none() {
            dsn.protocol.replace("wss".to_string());
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

    async fn ping(&self, conn: &mut Self::Target) -> RawResult<()> {
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

    async fn build(&self) -> RawResult<Self::Target> {
        match &self.0 {
            TaosBuilderInner::Native(b) => Ok(Taos(TaosInner::Native(b.build().await?))),
            TaosBuilderInner::Ws(b) => Ok(Taos(TaosInner::Ws(b.build().await?))),
        }
    }

    async fn server_version(&self) -> RawResult<&str> {
        match &self.0 {
            TaosBuilderInner::Native(b) => {
                Ok(<sys::TaosBuilder as taos_query::AsyncTBuilder>::server_version(b).await?)
            }
            TaosBuilderInner::Ws(b) => {
                Ok(<taos_ws::TaosBuilder as taos_query::AsyncTBuilder>::server_version(b).await?)
            }
        }
    }

    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        match &self.0 {
            TaosBuilderInner::Native(b) => Ok(
                <sys::TaosBuilder as taos_query::AsyncTBuilder>::is_enterprise_edition(b).await?,
            ),
            TaosBuilderInner::Ws(b) => Ok(b.is_enterprise_edition().await?),
        }
    }

    async fn get_edition(&self) -> RawResult<Edition> {
        match &self.0 {
            TaosBuilderInner::Native(b) => {
                Ok(<sys::TaosBuilder as taos_query::AsyncTBuilder>::get_edition(b).await?)
            }
            TaosBuilderInner::Ws(b) => Ok(b.get_edition().await?),
        }
    }
}

impl AsyncFetchable for ResultSet {
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
    ) -> std::task::Poll<RawResult<Option<RawBlock>>> {
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

    fn fetch_raw_block(&mut self) -> RawResult<Option<RawBlock>> {
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
    type AsyncResultSet = ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(&self, sql: T) -> RawResult<Self::AsyncResultSet> {
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
    ) -> RawResult<Self::AsyncResultSet> {
        log::trace!("Query with SQL: {}", sql.as_ref());
        match &self.0 {
            TaosInner::Native(taos) => taos
                .query_with_req_id(sql, req_id)
                .await
                .map(ResultSetInner::Native)
                .map(ResultSet)
                .map_err(Into::into),
            TaosInner::Ws(taos) => taos
                .query_with_req_id(sql, req_id)
                .await
                .map(ResultSetInner::Ws)
                .map(ResultSet)
                .map_err(Into::into),
        }
    }

    async fn write_raw_meta(&self, meta: &RawMeta) -> RawResult<()> {
        loop {
            let ok: RawResult<()> = match &self.0 {
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

    async fn write_raw_block(&self, block: &RawBlock) -> RawResult<()> {
        match &self.0 {
            TaosInner::Native(taos) => taos.write_raw_block(block).await.map_err(Into::into),
            TaosInner::Ws(taos) => taos.write_raw_block(block).await.map_err(Into::into),
        }
    }

    async fn write_raw_block_with_req_id(&self, block: &RawBlock, req_id: u64) -> RawResult<()> {
        match &self.0 {
            TaosInner::Native(taos) => taos
                .write_raw_block_with_req_id(block, req_id)
                .await
                .map_err(Into::into),
            TaosInner::Ws(taos) => taos
                .write_raw_block_with_req_id(block, req_id)
                .await
                .map_err(Into::into),
        }
    }

    async fn put(&self, data: &taos_query::common::SmlData) -> RawResult<()> {
        match &self.0 {
            TaosInner::Native(taos) => taos.put(data).await.map_err(Into::into),
            TaosInner::Ws(taos) => taos.put(data).await.map_err(Into::into),
        }
    }

    async fn table_vgroup_id(&self, db: &str, table: &str) -> Option<i32> {
        match &self.0 {
            TaosInner::Native(taos) => taos.table_vgroup_id(db, table).await,
            TaosInner::Ws(taos) => taos.table_vgroup_id(db, table).await,
        }
    }

    async fn tables_vgroup_ids<T: AsRef<str> + Sync>(&self, db: &str, tables: &[T]) -> Option<Vec<i32>> {
        match &self.0 {
            TaosInner::Native(taos) => taos.tables_vgroup_ids(db, tables).await,
            TaosInner::Ws(taos) => taos.tables_vgroup_ids(db, tables).await,
        }
    }
}

impl taos_query::Queryable for Taos {
    type ResultSet = ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> RawResult<Self::ResultSet> {
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

    fn query_with_req_id<T: AsRef<str>>(&self, sql: T, req_id: u64) -> RawResult<Self::ResultSet> {
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

    fn write_raw_meta(&self, meta: &RawMeta) -> RawResult<()> {
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

    fn write_raw_block(&self, block: &RawBlock) -> RawResult<()> {
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

    fn write_raw_block_with_req_id(&self, block: &RawBlock, req_id: u64) -> RawResult<()> {
        match &self.0 {
            TaosInner::Native(taos) => {
                <crate::sys::Taos as taos_query::Queryable>::write_raw_block_with_req_id(
                    taos, block, req_id,
                )
                .map_err(Into::into)
            }
            TaosInner::Ws(taos) => {
                <taos_ws::Taos as taos_query::Queryable>::write_raw_block_with_req_id(
                    taos, block, req_id,
                )
                .map_err(Into::into)
            }
        }
    }

    fn put(&self, data: &taos_query::common::SmlData) -> RawResult<()> {
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
    use taos_query::{common::Timestamp, RawResult, TBuilder};

    use super::TaosBuilder;

    #[test]
    fn builder() {
        let builder = TaosBuilder::from_dsn("taos://").unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());
    }

    #[test]
    fn test_server_version() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        println!("server version: {:?}", builder.server_version()?);
        assert!(builder.server_version().is_ok());

        Ok(())
    }

    #[test]
    fn test_server_is_enterprise_edition() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
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
    fn test_get_edition() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        println!("get enterprise edition: {:?}", builder.get_edition());

        Ok(())
    }

    #[test]
    fn test_get_edition_ws() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("http://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        println!("get enterprise edition: {:?}", builder.get_edition());

        Ok(())
    }

    #[test]
    #[ignore]
    fn test_get_edition_cloud() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_CLOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        println!("get enterprise edition: {:?}", builder.get_edition());

        Ok(())
    }

    #[test]
    fn test_assert_enterprise_edition() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        let res = builder.assert_enterprise_edition();
        println!("assert enterprise edition: {:?}", res);

        Ok(())
    }

    #[test]
    fn test_assert_enterprise_edition_ws() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_DSN").unwrap_or("http://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        let res = builder.assert_enterprise_edition();
        println!("assert enterprise edition: {:?}", res);

        Ok(())
    }

    #[test]
    #[ignore]
    fn test_assert_enterprise_edition_cloud() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_CLOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        let res = builder.assert_enterprise_edition();
        println!("assert enterprise edition: {:?}", res);

        Ok(())
    }

    #[test]
    fn test_server_version_ws() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_WS_DSN").unwrap_or("taosws://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
        assert!(builder.ready());

        let mut conn = builder.build().unwrap();
        assert!(builder.ping(&mut conn).is_ok());

        println!("server version: {:?}", builder.server_version()?);
        assert!(builder.server_version().is_ok());

        Ok(())
    }

    #[test]
    fn test_server_is_enterprise_edition_ws() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let dsn = std::env::var("TEST_WS_DSN").unwrap_or("taosws://localhost:6041".to_string());
        let dsn = Dsn::from_str(&dsn)?;
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
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
    fn sync_json_test_native() -> RawResult<()> {
        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://".to_string());
        sync_json_test(&dsn, "taos")
    }

    #[cfg(feature = "ws")]
    #[test]
    fn sync_json_test_ws() -> RawResult<()> {
        sync_json_test("ws://", "ws")
    }

    #[cfg(feature = "ws")]
    #[test]
    fn sync_json_test_taosws() -> RawResult<()> {
        sync_json_test("taosws://", "taosws")
    }

    #[test]
    fn null_test() -> RawResult<()> {
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
    fn query_with_req_id_ws() -> RawResult<()> {
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
    fn query_with_req_id_native() -> RawResult<()> {
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

    fn sync_json_test(dsn: &str, db: &str) -> RawResult<()> {
        use taos_query::prelude::sync::*;

        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
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
    fn test_put_line() -> RawResult<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        use taos_query::prelude::sync::*;

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_schemaless";

        client.exec(format!("drop database if exists {db}"))?;

        std::thread::sleep(std::time::Duration::from_millis(10));

        client.exec(format!("create database if not exists {db}"))?;

        std::thread::sleep(std::time::Duration::from_millis(10));

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
    fn test_put_telnet() -> RawResult<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        use taos_query::prelude::sync::*;

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_schemaless_put_telnet";

        client.exec(format!("drop database if exists {db}"))?;

        std::thread::sleep(std::time::Duration::from_secs(3));

        client.exec(format!("create database if not exists {db}"))?;

        std::thread::sleep(std::time::Duration::from_secs(3));

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
    fn test_put_json() -> RawResult<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        use taos_query::prelude::sync::*;

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_schemaless_put_json";

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

    #[test]
    fn test_ws_write_raw_block_with_req_id() -> anyhow::Result<()> {
        use crate::TmqBuilder;
        use taos_query::prelude::sync::*;

        std::env::set_var("RUST_LOG", "taos=trace");
        // pretty_env_logger::init();

        let taos = TaosBuilder::from_dsn("http://localhost:6041")?.build()?;
        let db = "test_ws_write_raw_block_req_id";
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

        let builder =
            TmqBuilder::from_dsn("taos://localhost:6030/db?group.id=5&auto.offset.reset=earliest")?;
        let mut consumer = builder.build()?;

        consumer.subscribe([db])?;

        for message in consumer.iter_with_timeout(Timeout::from_secs(1)) {
            let (offset, msg) = message?;
            log::debug!("offset: {:?}", offset);

            match msg {
                MessageSet::Meta(meta) => {
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
                    taos.write_raw_meta(&meta.as_raw_meta()?)?;

                    // data
                    for raw in data {
                        let raw = raw?;
                        log::debug!("raw: {:?}", raw);
                        let (_nrows, _ncols) = (raw.nrows(), raw.ncols());
                        for col in raw.columns() {
                            for value in col {
                                log::debug!("value in col {}\n", value);
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
            log::debug!("raw: {:?}", raw);
        }
        let mut query = taos.query("select count(*) from stb1")?;
        for row in query.rows() {
            let raw = row?;
            log::debug!("raw: {:?}", raw);
        }

        taos.query(format!("drop database {db}2"))?;
        taos.query(format!("drop topic {db}")).unwrap();
        taos.query(format!("drop database {db}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod async_tests {
    use anyhow::Context;
    use taos_query::common::SchemalessPrecision;
    use taos_query::common::SchemalessProtocol;
    use taos_query::common::SmlDataBuilder;
    use taos_query::RawResult;

    use crate::AsyncQueryable;
    use crate::AsyncTBuilder;
    use crate::TaosBuilder;

    #[tokio::test()]
    #[ignore]
    async fn test_recycle() -> RawResult<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        let builder = TaosBuilder::from_dsn("taos+ws://localhost:6041/")?;
        let pool = builder.pool()?;

        let max_time = 5;
        for i in 0..max_time {
            log::debug!("------ loop: {} ------", i);
            let taos = pool.get().await.context("taos connection error")?;
            // log taos memory location
            log::debug!("taos: {:p}", &taos);
            let r = taos.exec("select server_version()").await;
            match r {
                Ok(r) => {
                    log::debug!("rows: {:?}", r);
                }
                Err(e) => {
                    log::error!("error: {:?}", e);
                }
            }
            drop(taos);

            if i < max_time - 1 {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            }
        }

        Ok(())
    }

    #[tokio::test()]
    #[ignore]
    async fn test_put() -> RawResult<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        put_line().await?;
        put_telnet().await?;
        put_json().await
    }

    async fn put_line() -> RawResult<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        // std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "test_schemaless_ws_line";

        client.exec(format!("drop database if exists {db}")).await?;

        client
            .exec(format!("create database if not exists {db}"))
            .await?;

        // should specify database before insert
        // client.exec(format!("use {db}")).await?;

        let dsn_with_db = format!("{dsn}/{db}");

        let client = TaosBuilder::from_dsn(dsn_with_db)?.build().await?;
        log::debug!("client: {:?}", &client);

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

    async fn put_telnet() -> RawResult<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        // std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "test_schemaless_ws_telnet";

        client.exec(format!("drop database if exists {db}")).await?;

        client
            .exec(format!("create database if not exists {db}"))
            .await?;

        // should specify database before insert
        // client.exec(format!("use {db}")).await?;

        let dsn_with_db = format!("{dsn}/{db}");

        let client = TaosBuilder::from_dsn(dsn_with_db)?.build().await?;
        log::debug!("client: {:?}", &client);

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

    async fn put_json() -> RawResult<()> {
        // std::env::set_var("RUST_LOG", "taos=trace");
        // std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();
        let dsn =
            std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(&dsn)?.build().await?;

        let db = "test_schemaless_ws";

        client.exec(format!("drop database if exists {db}")).await?;

        client
            .exec(format!("create database if not exists {db}"))
            .await?;

        // should specify database before insert
        // client.exec(format!("use {db}")).await?;

        let dsn_with_db = format!("{dsn}/{db}");

        let client = TaosBuilder::from_dsn(dsn_with_db)?.build().await?;
        log::debug!("client: {:?}", &client);

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

    #[tokio::test]
    async fn test_is_enterprise_edition() -> RawResult<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?;
        log::debug!("client: {:?}", &client);
        log::debug!("is_enterprise: {:?}", client.is_enterprise_edition().await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_is_enterprise_edition_ws() -> RawResult<()> {
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

    #[tokio::test]
    async fn test_get_edition() -> RawResult<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?;
        log::debug!("client: {:?}", &client);
        log::debug!("get enterprise edition: {:?}", client.get_edition().await);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_edition_ws() -> RawResult<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn = std::env::var("TEST_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?;
        log::debug!("client: {:?}", &client);
        log::debug!("get enterprise edition: {:?}", client.get_edition().await);

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_edition_cloud() -> RawResult<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn = std::env::var("TEST_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?;
        log::debug!("client: {:?}", &client);
        log::debug!("get enterprise edition: {:?}", client.get_edition().await);

        Ok(())
    }

    #[tokio::test]
    async fn test_assert_enterprise_edition() -> RawResult<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?;
        log::debug!("client: {:?}", &client);

        let res = client.assert_enterprise_edition().await;
        log::debug!("assert enterprise edition: {:?}", res);

        Ok(())
    }

    #[tokio::test]
    async fn test_assert_enterprise_edition_ws() -> RawResult<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn = std::env::var("TEST_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?;
        log::debug!("client: {:?}", &client);

        let res = client.assert_enterprise_edition().await;
        log::debug!("assert enterprise edition: {:?}", res);

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_assert_enterprise_edition_cloud() -> RawResult<()> {
        std::env::set_var("RUST_LOG", "taos=debug");
        // pretty_env_logger::init();

        let dsn = std::env::var("TEST_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        log::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?;
        log::debug!("client: {:?}", &client);

        let res = client.assert_enterprise_edition().await;
        log::debug!("assert enterprise edition: {:?}", res);

        Ok(())
    }

    #[tokio::test]
    async fn test_varchar() -> anyhow::Result<()> {
        // pretty_env_logger::formatted_timed_builder()
        //     .filter_level(log::LevelFilter::Trace)
        //     .init();
        let dsn = "taos://";

        let pool = TaosBuilder::from_dsn(dsn)?.pool()?;

        let taos = pool.get().await?;

        let db = "test_varchar";

        macro_rules! assert_eq {
            ($left:expr, $right:expr) => {
                let _ = taos
                    .exec_with_req_id(&format!("drop database {db}"), 1)
                    .await;
                if $left != $right {
                    panic!(
                        "assertion failed: `(left == right)` (left: `{:?}`, right: `{:?}`)",
                        $left, $right
                    )
                }
            };
        }

        // prepare database
        taos.exec_many([
            format!("DROP DATABASE IF EXISTS `{db}`"),
            format!("CREATE DATABASE `{db}`"),
            format!("USE `{db}`"),
        ])
        .await?;

        taos.exec_with_req_id("create table `tb0001` (`ts` TIMESTAMP,`data` FLOAT,`quality` INT) tags (`varchar` BINARY(64),`aid` INT,`bid` INT)", 0).await?;

        let sql = // create child table
        r#"create table if not exists `subtable00000000001` using `tb0001` (`varchar`,`aid`,`bid`) tags("涛思数据-涛思数据-涛思数据-涛思数据-涛思数据-涛思数据-涛思数据-涛思数据",1,2)"#;
        match taos.exec_with_req_id(&sql, 0).await {
            Err(e) => {
                dbg!(&e);
                assert_eq!(e.code(), 0x2605);
            }
            Ok(_) => {
                // Actually, the sql should return error 0x2605, but it success.

                // If not error, the table is created.
                let desc = taos.describe("subtable00000000001").await;
                dbg!(&desc);
                let len = desc.unwrap().len();
                assert_eq!(len, 6);
            }
        }

        Ok(())
    }
}
