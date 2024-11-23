mod bind;

use std::fmt::Debug;
use std::sync::Arc;

use taos_query::block_in_place_or_global;
use taos_query::prelude::RawResult;
use taos_query::stmt2::{AsyncBindable, Bindable, Stmt2BindData};
use taos_query::util::generate_req_id;

use crate::query::infra::{Stmt2Field, StmtId, WsRecvData, WsSend};
use crate::query::WsTaos;

#[derive(Debug)]
pub struct Stmt2 {
    client: Arc<WsTaos>,
    stmt_id: Option<StmtId>,
    is_insert: Option<bool>,
    fields: Option<Vec<Stmt2Field>>,
    fields_count: Option<usize>,
    affected_rows: usize,
    affected_rows_once: usize,
}

impl Stmt2 {
    fn new(client: Arc<WsTaos>) -> Self {
        Self {
            client,
            stmt_id: None,
            is_insert: None,
            fields: None,
            fields_count: None,
            affected_rows: 0,
            affected_rows_once: 0,
        }
    }

    async fn init(&mut self) -> RawResult<()> {
        let req = WsSend::Stmt2Init {
            req_id: generate_req_id(),
            single_stb_insert: true,
            single_table_bind_once: false,
        };
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Init { stmt_id, .. } = resp {
            self.stmt_id = Some(stmt_id);
        }
        Ok(())
    }

    async fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<()> {
        let req = WsSend::Stmt2Prepare {
            req_id: generate_req_id(),
            stmt_id: self.stmt_id.unwrap(),
            sql: sql.as_ref().to_string(),
            get_fields: true,
        };
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Prepare {
            is_insert,
            fields,
            fields_count,
            ..
        } = resp
        {
            self.is_insert = Some(is_insert);
            self.fields = fields;
            self.fields_count = Some(fields_count);
        }
        Ok(())
    }

    async fn bind<'b>(&mut self, datas: &[Stmt2BindData<'b>]) -> RawResult<()> {
        let bytes = bind::bind_datas_as_bytes(
            datas,
            generate_req_id(),
            self.stmt_id.unwrap(),
            self.is_insert.unwrap(),
            self.fields.as_ref(),
            self.fields_count.unwrap(),
        )?;
        let req = WsSend::Binary(bytes);
        let _ = self.client.send_request(req).await?;
        Ok(())
    }

    async fn exec(&mut self) -> RawResult<usize> {
        let req = WsSend::Stmt2Exec {
            req_id: generate_req_id(),
            stmt_id: self.stmt_id.unwrap(),
        };
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Exec { affected, .. } = resp {
            self.affected_rows += affected;
            self.affected_rows_once = affected;
        }
        Ok(self.affected_rows)
    }

    async fn result(&mut self) -> RawResult<()> {
        if self.is_insert.unwrap() {
            return Err("Can't use result for insert stmt2".into());
        }
        let req = WsSend::Stmt2Result {
            req_id: generate_req_id(),
            stmt_id: self.stmt_id.unwrap(),
        };
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Result { result_id, .. } = resp {
            // TODO
            println!("stmt2 result res_id: {result_id}");
        }
        Ok(())
    }

    async fn close(&mut self) {
        let req = WsSend::Stmt2Close {
            req_id: generate_req_id(),
            stmt_id: self.stmt_id.unwrap(),
        };
        let resp = self.client.send_request(req).await;
        if let Err(e) = resp {
            tracing::error!("Failed to close Stmt2: {e:?}");
        }
    }
}

impl Drop for Stmt2 {
    fn drop(&mut self) {
        block_in_place_or_global(self.close());
    }
}

impl Bindable<super::Taos> for Stmt2 {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        let mut stmt2 = Self::new(taos.client());
        block_in_place_or_global(stmt2.init())?;
        Ok(stmt2)
    }

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        block_in_place_or_global(self.prepare(sql))?;
        Ok(self)
    }

    fn bind(&mut self, datas: &[Stmt2BindData]) -> RawResult<&mut Self> {
        block_in_place_or_global(self.bind(datas))?;
        Ok(self)
    }

    fn exec(&mut self) -> RawResult<usize> {
        block_in_place_or_global(self.exec())
    }

    fn affected_rows(&self) -> usize {
        self.affected_rows
    }

    fn result_set(&mut self) -> RawResult<<super::Taos as taos_query::Queryable>::ResultSet> {
        todo!()
    }
}

#[async_trait::async_trait]
impl AsyncBindable<super::Taos> for Stmt2 {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        let mut stmt2 = Self::new(taos.client());
        stmt2.init().await?;
        Ok(stmt2)
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.prepare(sql).await?;
        Ok(self)
    }

    async fn bind(&mut self, datas: &[Stmt2BindData]) -> RawResult<&mut Self> {
        self.bind(datas).await?;
        Ok(self)
    }

    async fn exec(&mut self) -> RawResult<usize> {
        self.exec().await
    }

    async fn affected_rows(&self) -> usize {
        self.affected_rows
    }

    async fn result_set(
        &mut self,
    ) -> RawResult<<super::Taos as taos_query::AsyncQueryable>::AsyncResultSet> {
        let _ = self.result().await?;
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use taos_query::common::ColumnView;
    use taos_query::stmt2::Stmt2BindData;
    use taos_query::{AsyncQueryable, AsyncTBuilder};

    use crate::stmt2::Stmt2;
    use crate::TaosBuilder;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_with_insert() -> anyhow::Result<()> {
        let db = "stmt2_insert";
        let dsn = "taos://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("create table {db}.t0 (ts timestamp, a int)"),
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare(format!("insert into {db}.t0 values(?, ?)"))
            .await?;

        let cols = &[
            ColumnView::from_millis_timestamp(vec![1726803356466]),
            ColumnView::from_ints(vec![99]),
        ];
        let data = Stmt2BindData::new(None, None, Some(cols));
        stmt2.bind(&[data]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_with_query() -> anyhow::Result<()> {
        let db = "stmt2_query";
        let dsn = "taos://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("create table {db}.t1 (ts timestamp, a int)"),
            format!("insert into {db}.t1 values(now, 100)"),
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare(format!("select * from {db}.t1 where a > ?"))
            .await?;

        let cols = &[ColumnView::from_ints(vec![0])];
        let data = Stmt2BindData::new(None, None, Some(cols));
        stmt2.bind(&[data]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 0);

        stmt2.result().await?;

        Ok(())
    }
}
