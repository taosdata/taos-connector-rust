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

    fn execute(&mut self) -> RawResult<usize> {
        match &mut self.0 {
            Stmt2Inner::Ws(stmt) => Ok(stmt.execute()?),
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

    async fn execute(&mut self) -> RawResult<usize> {
        match &mut self.0 {
            Stmt2Inner::Ws(stmt) => Ok(stmt.execute().await?),
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
