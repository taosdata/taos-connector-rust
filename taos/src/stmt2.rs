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
    use serde::Deserialize;
    use taos_query::{
        common::ColumnView,
        stmt2::{Bindable, Stmt2BindData},
        Queryable, TBuilder,
    };

    use crate::sync::*;
    use crate::TaosBuilder;

    use super::Stmt2;

    #[test]
    fn test_stmt2_insert_single_row() -> anyhow::Result<()> {
        let db = "stmt2_202411231337";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build()?;
        taos.exec_many([
            &format!("drop database if exists {db}"),
            &format!("create database {db} keep 36500"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare("insert into t0 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;

        let views = &[
            ColumnView::from_millis_timestamp(vec![164000000000]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![i8::MAX]),
            ColumnView::from_small_ints(vec![i16::MAX]),
            ColumnView::from_ints(vec![i32::MAX]),
            ColumnView::from_big_ints(vec![i64::MAX]),
            ColumnView::from_unsigned_tiny_ints(vec![u8::MAX]),
            ColumnView::from_unsigned_small_ints(vec![u16::MAX]),
            ColumnView::from_unsigned_ints(vec![u32::MAX]),
            ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
            ColumnView::from_floats(vec![f32::MAX]),
            ColumnView::from_doubles(vec![f64::MAX]),
            ColumnView::from_varchar(vec!["hello"]),
            ColumnView::from_nchar(vec!["中文"]),
        ];

        let data = Stmt2BindData::new(None, None, Some(views));
        let affected = stmt2.bind(&[data])?.exec()?;
        assert_eq!(affected, 1);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: String,
            c1: bool,
            c2: i8,
            c3: i16,
            c4: i32,
            c5: i64,
            c6: u8,
            c7: u16,
            c8: u32,
            c9: u64,
            c10: Option<f32>,
            c11: f64,
            c12: String,
            c13: String,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")?
            .deserialize()
            .try_collect()?;

        assert_eq!(rows.len(), 1);

        let row = &rows[0];

        assert_eq!(row.ts, "1975-03-14T11:33:20+08:00");
        assert_eq!(row.c1, true);
        assert_eq!(row.c2, i8::MAX);
        assert_eq!(row.c3, i16::MAX);
        assert_eq!(row.c4, i32::MAX);
        assert_eq!(row.c5, i64::MAX);
        assert_eq!(row.c6, u8::MAX);
        assert_eq!(row.c7, u16::MAX);
        assert_eq!(row.c8, u32::MAX);
        assert_eq!(row.c9, u64::MAX);
        assert_eq!(row.c10.unwrap(), f32::MAX);
        assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "hello");
        assert_eq!(row.c13, "中文");

        taos.exec(format!("drop database {db}"))?;

        Ok(())
    }

    #[test]
    fn test_stmt2_insert_multi_row() -> anyhow::Result<()> {
        let db = "stmt2_202411222151";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build()?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare("insert into t0 values(?, ?)")?;

        let views = &[
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803357466,
                1726803358466,
                1726803359466,
            ]),
            ColumnView::from_ints(vec![99, 100, 101, 102]),
        ];

        let data = Stmt2BindData::new(None, None, Some(views));
        let affected = stmt2.bind(&[data])?.exec()?;
        assert_eq!(affected, views[0].len());

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: String,
            c1: i32,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")?
            .deserialize()
            .try_collect()?;

        assert_eq!(rows.len(), views[0].len());

        assert_eq!(rows[0].ts, "2024-09-20T11:35:56.466+08:00");
        assert_eq!(rows[1].ts, "2024-09-20T11:35:57.466+08:00");
        assert_eq!(rows[2].ts, "2024-09-20T11:35:58.466+08:00");
        assert_eq!(rows[3].ts, "2024-09-20T11:35:59.466+08:00");

        assert_eq!(rows[0].c1, 99);
        assert_eq!(rows[1].c1, 100);
        assert_eq!(rows[2].c1, 101);
        assert_eq!(rows[3].c1, 102);

        taos.exec(format!("drop database {db}"))?;

        Ok(())
    }

    #[test]
    fn test_stmt2_query() -> anyhow::Result<()> {
        let db = "stmt2_202411222202";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build()?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values(now, 100)",
        ])?;

        let mut stmt2 = Stmt2::init(&taos)?;
        stmt2.prepare("select * from t0 where c1 > ?")?;

        let views = &[ColumnView::from_ints(vec![0])];
        let data = Stmt2BindData::new(None, None, Some(views));
        stmt2.bind(&[data])?;

        let affected = stmt2.exec()?;
        assert_eq!(affected, 0);

        taos.exec(format!("drop database {db}"))?;

        Ok(())
    }
}

#[cfg(test)]
mod async_tests {
    use serde::Deserialize;
    use taos_query::{
        common::ColumnView,
        stmt2::{AsyncBindable, Stmt2BindData},
        AsyncQueryable, AsyncTBuilder,
    };

    use crate::*;

    use super::Stmt2;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_insert_single_row() -> anyhow::Result<()> {
        let db = "stmt2_202411231450";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many([
            &format!("drop database if exists {db}"),
            &format!("create database {db} keep 36500"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2
            .prepare("insert into t0 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .await?;

        let views = &[
            ColumnView::from_millis_timestamp(vec![164000000000]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![i8::MAX]),
            ColumnView::from_small_ints(vec![i16::MAX]),
            ColumnView::from_ints(vec![i32::MAX]),
            ColumnView::from_big_ints(vec![i64::MAX]),
            ColumnView::from_unsigned_tiny_ints(vec![u8::MAX]),
            ColumnView::from_unsigned_small_ints(vec![u16::MAX]),
            ColumnView::from_unsigned_ints(vec![u32::MAX]),
            ColumnView::from_unsigned_big_ints(vec![u64::MAX]),
            ColumnView::from_floats(vec![f32::MAX]),
            ColumnView::from_doubles(vec![f64::MAX]),
            ColumnView::from_varchar(vec!["hello"]),
            ColumnView::from_nchar(vec!["中文"]),
        ];

        let data = Stmt2BindData::new(None, None, Some(views));
        let affected = stmt2.bind(&[data]).await?.exec().await?;
        assert_eq!(affected, 1);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: String,
            c1: bool,
            c2: i8,
            c3: i16,
            c4: i32,
            c5: i64,
            c6: u8,
            c7: u16,
            c8: u32,
            c9: u64,
            c10: Option<f32>,
            c11: f64,
            c12: String,
            c13: String,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 1);

        let row = &rows[0];

        assert_eq!(row.ts, "1975-03-14T11:33:20+08:00");
        assert_eq!(row.c1, true);
        assert_eq!(row.c2, i8::MAX);
        assert_eq!(row.c3, i16::MAX);
        assert_eq!(row.c4, i32::MAX);
        assert_eq!(row.c5, i64::MAX);
        assert_eq!(row.c6, u8::MAX);
        assert_eq!(row.c7, u16::MAX);
        assert_eq!(row.c8, u32::MAX);
        assert_eq!(row.c9, u64::MAX);
        assert_eq!(row.c10.unwrap(), f32::MAX);
        assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "hello");
        assert_eq!(row.c13, "中文");

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_insert_multi_row() -> anyhow::Result<()> {
        let db = "stmt2_202411222208";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2.prepare("insert into t0 values(?, ?)").await?;

        let views = &[
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803357466,
                1726803358466,
                1726803359466,
            ]),
            ColumnView::from_ints(vec![99, 100, 101, 102]),
        ];

        let data = Stmt2BindData::new(None, None, Some(views));
        let affected = stmt2.bind(&[data]).await?.exec().await?;
        assert_eq!(affected, views[0].len());

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: String,
            c1: i32,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), views[0].len());

        assert_eq!(rows[0].ts, "2024-09-20T11:35:56.466+08:00");
        assert_eq!(rows[1].ts, "2024-09-20T11:35:57.466+08:00");
        assert_eq!(rows[2].ts, "2024-09-20T11:35:58.466+08:00");
        assert_eq!(rows[3].ts, "2024-09-20T11:35:59.466+08:00");

        assert_eq!(rows[0].c1, 99);
        assert_eq!(rows[1].c1, 100);
        assert_eq!(rows[2].c1, 101);
        assert_eq!(rows[3].c1, 102);

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_query() -> anyhow::Result<()> {
        let db = "stmt2_202411222213";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values(now, 100)",
        ])
        .await?;

        let mut stmt2 = Stmt2::init(&taos).await?;
        stmt2.prepare("select * from t0 where c1 > ?").await?;

        let views = &[ColumnView::from_ints(vec![0])];
        let data = Stmt2BindData::new(None, None, Some(views));
        let affected = stmt2.bind(&[data]).await?.exec().await?;
        assert_eq!(affected, 0);

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }
}
