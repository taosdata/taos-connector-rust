use taos_query::{stmt2::Stmt2BindData, AsyncQueryable, RawResult};
use taos_ws::Stmt2 as WsStmt2;

use crate::TaosInner;

#[derive(Debug)]
enum Stmt2Inner {
    Ws(WsStmt2),
}

#[derive(Debug)]
pub struct Stmt2(Stmt2Inner);

impl taos_query::stmt2::Bindable<super::Taos> for Stmt2 {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        match &taos.0 {
            TaosInner::Native(_) => todo!(),
            TaosInner::Ws(taos) => WsStmt2::init(taos)
                .map(Stmt2Inner::Ws)
                .map(Stmt2)
                .map_err(Into::into),
        }
    }

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        match &mut self.0 {
            Stmt2Inner::Ws(stmt) => {
                stmt.prepare(sql)?;
            }
        }
        Ok(self)
    }

    fn bind(&mut self, datas: &[Stmt2BindData]) -> RawResult<&mut Self> {
        match &mut self.0 {
            Stmt2Inner::Ws(stmt) => {
                stmt.bind(datas)?;
            }
        }
        Ok(self)
    }

    fn exec(&mut self) -> RawResult<usize> {
        match &mut self.0 {
            Stmt2Inner::Ws(stmt) => Ok(stmt.exec()?),
        }
    }

    fn affected_rows(&self) -> usize {
        match &self.0 {
            Stmt2Inner::Ws(stmt) => stmt.affected_rows(),
        }
    }

    fn result_set(&mut self) -> RawResult<<super::Taos as taos_query::Queryable>::ResultSet> {
        todo!()
    }
}

#[async_trait::async_trait]
impl taos_query::stmt2::AsyncBindable<super::Taos> for Stmt2 {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        match &taos.0 {
            TaosInner::Native(_) => todo!(),
            TaosInner::Ws(taos) => WsStmt2::init(taos)
                .await
                .map(Stmt2Inner::Ws)
                .map(Stmt2)
                .map_err(Into::into),
        }
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        match &mut self.0 {
            Stmt2Inner::Ws(stmt) => {
                stmt.prepare(sql).await?;
            }
        }
        Ok(self)
    }

    async fn bind(&mut self, datas: &[Stmt2BindData]) -> RawResult<&mut Self> {
        match &mut self.0 {
            Stmt2Inner::Ws(stmt) => {
                stmt.bind(datas).await?;
            }
        }
        Ok(self)
    }

    async fn exec(&mut self) -> RawResult<usize> {
        match &mut self.0 {
            Stmt2Inner::Ws(stmt) => Ok(stmt.exec().await?),
        }
    }

    async fn affected_rows(&self) -> usize {
        match &self.0 {
            Stmt2Inner::Ws(stmt) => stmt.affected_rows().await,
        }
    }

    async fn result_set(&mut self) -> RawResult<<super::Taos as AsyncQueryable>::AsyncResultSet> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use taos_query::{
        common::ColumnView,
        stmt2::{Bindable, Stmt2BindData},
        Queryable, TBuilder,
    };

    use crate::TaosBuilder;

    use super::Stmt2;

    #[test]
    fn test_stmt2_with_insert() -> anyhow::Result<()> {
        let db = "stmt2_202411222151";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build()?;
        taos.exec_many(vec![
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("create table {db}.t0 (ts timestamp, a int)"),
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare(&format!("insert into {db}.t0 values(?, ?)"))?;

        let cols = &[
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803357466,
                1726803358466,
                1726803359466,
            ]),
            ColumnView::from_ints(vec![99, 100, 101, 102]),
        ];

        let data = Stmt2BindData::new(None, None, Some(cols));
        stmt2.bind(&[data])?;

        let affected = stmt2.exec()?;
        assert_eq!(affected, cols[0].len());

        Ok(())
    }

    #[test]
    fn test_stmt2_with_query() -> anyhow::Result<()> {
        let db = "stmt2_202411222202";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build()?;
        taos.exec_many(vec![
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("create table {db}.t1 (ts timestamp, a int)"),
            format!("insert into {db}.t1 values(now, 100)"),
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare(&format!("select * from {db}.t1 where a > ?"))?;

        let cols = &[ColumnView::from_ints(vec![0])];
        let data = Stmt2BindData::new(None, None, Some(cols));
        stmt2.bind(&[data])?;

        let affected = stmt2.exec()?;
        assert_eq!(affected, 0);

        Ok(())
    }
}

#[cfg(test)]
mod async_tests {
    use taos_query::{
        common::ColumnView,
        stmt2::{AsyncBindable, Stmt2BindData},
        AsyncQueryable, AsyncTBuilder,
    };

    use crate::TaosBuilder;

    use super::Stmt2;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_with_insert() -> anyhow::Result<()> {
        let db = "stmt2_202411222208";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("create table {db}.t0 (ts timestamp, a int)"),
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2
            .prepare(&format!("insert into {db}.t0 values(?, ?)"))
            .await?;

        let cols = &[
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803357466,
                1726803358466,
                1726803359466,
            ]),
            ColumnView::from_ints(vec![99, 100, 101, 102]),
        ];

        let data = Stmt2BindData::new(None, None, Some(cols));
        stmt2.bind(&[data]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, cols[0].len());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_with_query() -> anyhow::Result<()> {
        pretty_env_logger::env_logger::init();
        let db = "stmt2_202411222213";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("create table {db}.t1 (ts timestamp, a int)"),
            format!("insert into {db}.t1 values(now, 100)"),
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2
            .prepare(&format!("select * from {db}.t1 where a > ?"))
            .await?;

        let cols = &[ColumnView::from_ints(vec![0])];
        let data = Stmt2BindData::new(None, None, Some(cols));
        stmt2.bind(&[data]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 0);

        Ok(())
    }
}
