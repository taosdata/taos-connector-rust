use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use taos_query::common::{Field, Precision};
use taos_query::stmt2::{Stmt2AsyncBindable, Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_query::{block_in_place_or_global, AsyncQueryable, Queryable, RawResult};
use tokio::sync::{mpsc, Mutex};
use tracing::Instrument;

use crate::query::asyn::{fetch_binary, QueryMetrics};
use crate::query::messages::{Stmt2Field, WsRecvData, WsResArgs, WsSend};
use crate::query::WsTaos;
use crate::{ResultSet, Taos};

mod bind;

#[derive(Debug, Clone)]
enum Stmt2Request {
    Init,
    Prepare,
    Bind,
    Exec,
    ResultSet,
}

#[derive(Debug)]
struct Stmt2Cache {
    req_id: Option<u64>,
    req: Option<Stmt2Request>,
    init_options: Option<(bool, bool)>,
    sql: Option<String>,
    params: Option<Vec<Vec<Stmt2BindParam>>>,
}

impl Stmt2Cache {
    fn new() -> Self {
        todo!()
    }

    fn build_conn_req(&self) -> WsSend {
        let (single_stb_insert, single_table_bind_once) = self.init_options.unwrap();
        WsSend::Stmt2Init {
            req_id: self.req_id.unwrap(),
            single_stb_insert,
            single_table_bind_once,
        }
    }
}

#[derive(Debug)]
pub struct Stmt2 {
    id: u64,
    client: Arc<WsTaos>,
    stmt_id: Option<Arc<AtomicU64>>,
    is_insert: Option<bool>,
    fields: Option<Vec<Stmt2Field>>,
    fields_count: Option<usize>,
    affected_rows: usize,
    affected_rows_once: usize,
    cache: Arc<Mutex<Stmt2Cache>>,
}

impl Clone for Stmt2 {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            client: self.client.clone(),
            stmt_id: self.stmt_id.clone(),
            cache: self.cache.clone(),
            is_insert: None,
            fields: None,
            fields_count: None,
            affected_rows: 0,
            affected_rows_once: 0,
        }
    }
}

impl Stmt2 {
    // reconnect
    // init + prepare + bind + exec + result_set
    // close not need

    pub fn new(client: Arc<WsTaos>) -> Self {
        let stmt2 = Self {
            id: generate_req_id(),
            client,
            stmt_id: None,
            is_insert: None,
            fields: None,
            fields_count: None,
            affected_rows: 0,
            affected_rows_once: 0,
            cache: Arc::new(Mutex::new(Stmt2Cache::new())),
        };

        // let a = Arc::downgrade(&Arc::new(stmt2));

        stmt2
    }

    async fn init(&mut self) -> RawResult<()> {
        self.init_with_options(generate_req_id(), true, false).await
    }

    pub async fn init_with_options(
        &mut self,
        req_id: u64,
        single_stb_insert: bool,
        single_table_bind_once: bool,
    ) -> RawResult<()> {
        // TODO: determine whether it has been registered
        self.client.insert_stmt2(self.clone());
        self.client.wait_for_reconnect().await?;

        // not need update
        let mut guard = self.cache.lock().await;
        // req_id cannot be modified
        guard.req_id = Some(req_id);
        guard.req = Some(Stmt2Request::Init);
        guard.init_options = Some((single_stb_insert, single_table_bind_once));

        let req = WsSend::Stmt2Init {
            req_id,
            single_stb_insert,
            single_table_bind_once,
        };
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Init { stmt_id, .. } = resp {
            self.stmt_id = Some(Arc::new(AtomicU64::new(stmt_id)));
            return Ok(());
        }
        unreachable!()
    }

    async fn prepare<S: AsRef<str> + Send>(&mut self, sql: S) -> RawResult<()> {
        let req_id = generate_req_id();
        self.client.wait_for_reconnect().await?;

        let mut guard = self.cache.lock().await;
        guard.req_id = Some(req_id);
        guard.req = Some(Stmt2Request::Prepare);
        guard.sql = Some(sql.as_ref().to_string());

        let req = WsSend::Stmt2Prepare {
            req_id,
            stmt_id: self.stmt_id().unwrap(),
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
            return Ok(());
        }
        unreachable!()
    }

    async fn bind(&self, params: &[Stmt2BindParam]) -> RawResult<()> {
        // TODO: cache bytes, update stmt_id and req_id
        let req_id = generate_req_id();
        self.client.wait_for_reconnect().await?;

        let mut guard = self.cache.lock().await;
        guard.req_id = Some(req_id);
        guard.req = Some(Stmt2Request::Bind);
        if guard.params.is_none() {
            guard.params = Some(vec![params.to_vec()]);
        } else {
            guard.params.as_mut().unwrap().push(params.to_vec());
        }

        let bytes = bind::bind_params_to_bytes(
            params,
            req_id,
            self.stmt_id().unwrap(),
            self.is_insert.unwrap(),
            self.fields.as_ref(),
            self.fields_count.unwrap(),
        )?;
        let req = WsSend::Binary(bytes);
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Bind { .. } = resp {
            return Ok(());
        }
        unreachable!()
    }

    async fn exec(&mut self) -> RawResult<usize> {
        let req_id = generate_req_id();
        self.client.wait_for_reconnect().await?;

        let mut guard = self.cache.lock().await;
        guard.req_id = Some(req_id);
        guard.req = Some(Stmt2Request::Exec);

        let req = WsSend::Stmt2Exec {
            req_id,
            stmt_id: self.stmt_id().unwrap(),
        };
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Exec { affected, .. } = resp {
            self.affected_rows += affected;
            self.affected_rows_once = affected;
            return Ok(affected);
        }
        unreachable!()
    }

    fn close(&self) {
        if self.client.state() != crate::query::asyn::ConnState::Connected {
            tracing::warn!("Cannot close Stmt2 when client is not connected");
            return;
        }

        // self.client.wait_until_connected().await;
        let req = WsSend::Stmt2Close {
            req_id: generate_req_id(),
            stmt_id: self.stmt_id().unwrap(),
        };
        let resp = block_in_place_or_global(self.client.send_request(req));
        match resp {
            Ok(WsRecvData::Stmt2Close { .. }) => tracing::trace!("Stmt2 closed successfully"),
            Err(err) => tracing::error!("Failed to close Stmt2: {err:?}"),
            _ => unreachable!(),
        }
    }

    async fn result_set(&self) -> RawResult<ResultSet> {
        if self.is_insert.unwrap_or(false) {
            return Err("Only query can use result".into());
        }

        self.client.wait_for_reconnect().await?;
        let req_id = generate_req_id();

        let mut guard = self.cache.lock().await;
        guard.req_id = Some(req_id);
        guard.req = Some(Stmt2Request::ResultSet);

        let req = WsSend::Stmt2Result {
            req_id,
            stmt_id: self.stmt_id().unwrap(),
        };

        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Result {
            id,
            precision,
            fields_count,
            fields_names,
            fields_types,
            fields_lengths,
            fields_precisions,
            fields_scales,
            timing,
            ..
        } = resp
        {
            let (close_tx, close_rx) = oneshot::channel();
            tokio::spawn(
                async move {
                    let start = Instant::now();
                    let _ = close_rx.await;
                    tracing::trace!("stmt2 result:{} lived {:?}", id, start.elapsed());
                }
                .in_current_span(),
            );

            let fields: Vec<Field> = fields_names
                .iter()
                .zip(fields_types)
                .zip(fields_lengths)
                .map(|((name, ty), len)| Field::new(name, ty, len as _))
                .collect();

            let (raw_block_tx, raw_block_rx) = mpsc::channel(64);
            let (fetch_done_tx, fetch_done_rx) = mpsc::channel(1);

            fetch_binary(
                self.client.sender(),
                id,
                raw_block_tx,
                precision,
                fields_names,
                fetch_done_tx,
            )
            .await;

            let timing = match precision {
                Precision::Millisecond => Duration::from_millis(timing),
                Precision::Microsecond => Duration::from_micros(timing),
                Precision::Nanosecond => Duration::from_nanos(timing),
            };

            return Ok(ResultSet {
                sender: self.client.sender(),
                args: WsResArgs { req_id, id },
                fields: Some(fields),
                fields_count: fields_count as _,
                affected_rows: self.affected_rows_once,
                precision,
                summary: (0, 0),
                timing,
                block_future: None,
                closer: Some(close_tx),
                metrics: QueryMetrics::default(),
                blocks_buffer: Some(raw_block_rx),
                fields_precisions,
                fields_scales,
                fetch_done_reader: Some(fetch_done_rx),
                tz: self.client.timezone(),
            });
        }

        unreachable!()
    }

    fn stmt_id(&self) -> Option<u64> {
        self.stmt_id.as_ref().map(|id| id.load(Ordering::Relaxed))
    }

    pub async fn recover(&self) -> RawResult<()> {
        // TODO: add log
        let guard = self.cache.lock().await;
        if let Some(req) = &guard.req {
            match req {
                Stmt2Request::Init => {
                    let req = guard.build_conn_req();
                    self.client.send_only(req).await?;
                }
                // _ => {
                // }
                Stmt2Request::Prepare => {
                    // let req = guard.build_conn_req();
                    // let resp = self.client.send_request(req).await?;
                }
                Stmt2Request::Bind => todo!(),
                Stmt2Request::Exec => todo!(),
                Stmt2Request::ResultSet => todo!(),
            }
        }

        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn is_insert(&self) -> Option<bool> {
        self.is_insert
    }

    pub fn fields(&self) -> Option<&Vec<Stmt2Field>> {
        self.fields.as_ref()
    }

    pub fn fields_count(&self) -> Option<usize> {
        self.fields_count
    }

    pub fn affected_rows_once(&self) -> usize {
        self.affected_rows_once
    }
}

impl Drop for Stmt2 {
    fn drop(&mut self) {
        self.close();
        self.client.remove_stmt2(self.id);
    }
}

impl Stmt2Bindable<super::Taos> for Stmt2 {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        let mut stmt2 = Self::new(taos.client());
        block_in_place_or_global(stmt2.init())?;
        Ok(stmt2)
    }

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        block_in_place_or_global(self.prepare(sql))?;
        Ok(self)
    }

    fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        block_in_place_or_global(Stmt2::bind(self, params))?;
        Ok(self)
    }

    fn exec(&mut self) -> RawResult<usize> {
        block_in_place_or_global(self.exec())
    }

    fn affected_rows(&self) -> usize {
        self.affected_rows
    }

    fn result_set(&self) -> RawResult<<Taos as Queryable>::ResultSet> {
        block_in_place_or_global(self.result_set())
    }
}

#[async_trait::async_trait]
impl Stmt2AsyncBindable<super::Taos> for Stmt2 {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        let mut stmt2 = Self::new(taos.client());
        stmt2.init().await?;
        Ok(stmt2)
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.prepare(sql).await?;
        Ok(self)
    }

    async fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        Stmt2::bind(self, params).await?;
        Ok(self)
    }

    async fn exec(&mut self) -> RawResult<usize> {
        self.exec().await
    }

    async fn affected_rows(&self) -> usize {
        self.affected_rows
    }

    async fn result_set(&self) -> RawResult<<Taos as AsyncQueryable>::AsyncResultSet> {
        self.result_set().await
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use serde::Deserialize;
    use taos_query::common::{ColumnView, Ty, Value};
    use taos_query::stmt2::Stmt2BindParam;
    use taos_query::{AsyncFetchable, AsyncQueryable, AsyncTBuilder};

    use crate::stmt2::Stmt2;
    use crate::TaosBuilder;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_insert_single_row() -> anyhow::Result<()> {
        let db = "stmt2_202411231503";
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

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("insert into t0 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![None]),
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

        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 1);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: bool,
            c2: Option<i8>,
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

        assert_eq!(row.ts, 1726803356466);
        assert_eq!(row.c1, true);
        assert_eq!(row.c2, None);
        assert_eq!(row.c3, i16::MAX);
        assert_eq!(row.c4, i32::MAX);
        assert_eq!(row.c5, i64::MAX);
        assert_eq!(row.c6, u8::MAX);
        assert_eq!(row.c7, u16::MAX);
        assert_eq!(row.c8, u32::MAX);
        assert_eq!(row.c9, u64::MAX);
        assert_eq!(row.c10, Some(f32::MAX));
        assert_eq!(row.c11, f64::MAX);
        assert_eq!(row.c12, "hello");
        assert_eq!(row.c13, "中文");

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_insert_multi_row() -> anyhow::Result<()> {
        let db = "stmt2_202411231455";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2.prepare("insert into t0 values(?, ?)").await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803357466,
                1726803358466,
                1726803359466,
            ]),
            ColumnView::from_ints(vec![99, 100, 101, 102]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 4);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 4);

        assert_eq!(rows[0].ts, 1726803356466);
        assert_eq!(rows[1].ts, 1726803357466);
        assert_eq!(rows[2].ts, 1726803358466);
        assert_eq!(rows[3].ts, 1726803359466);

        assert_eq!(rows[0].c1, 99);
        assert_eq!(rows[1].c1, 100);
        assert_eq!(rows[2].c1, 101);
        assert_eq!(rows[3].c1, 102);

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_query_single_row() -> anyhow::Result<()> {
        let db = "stmt2_202411231501";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
            &format!(
                "insert into t0 values(1726803356466, 1, NULL, 2, 3, 4, 5, 6, 7, 8, 1.1, 2.2, 'hello', '中文')"
            ),
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("select * from t0 where c8 > ? and c10 > ? and c12 = ?")
            .await?;

        let cols = vec![
            ColumnView::from_ints(vec![0]),
            ColumnView::from_floats(vec![0f32]),
            ColumnView::from_varchar(vec!["hello"]),
        ];

        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 0);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: bool,
            c2: Option<u8>,
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

        let rows: Vec<Row> = stmt2
            .result_set()
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 1);

        let row = &rows[0];

        assert_eq!(row.ts, 1726803356466);
        assert_eq!(row.c1, true);
        assert_eq!(row.c2, None);
        assert_eq!(row.c3, 2);
        assert_eq!(row.c4, 3);
        assert_eq!(row.c5, 4);
        assert_eq!(row.c6, 5);
        assert_eq!(row.c7, 6);
        assert_eq!(row.c8, 7);
        assert_eq!(row.c9, 8);
        assert_eq!(row.c10, Some(1.1));
        assert_eq!(row.c11, 2.2);
        assert_eq!(row.c12, "hello");
        assert_eq!(row.c13, "中文");

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_query_multi_row() -> anyhow::Result<()> {
        let db = "stmt2_202411281646";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values(1726803356466, 99)",
            "insert into t0 values(1726803357466, 100)",
            "insert into t0 values(1726803358466, 101)",
            "insert into t0 values(1726803359466, 102)",
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2.prepare("select * from t0 where c1 > ?").await?;

        let cols = vec![ColumnView::from_ints(vec![100])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 0);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = stmt2
            .result_set()
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].ts, 1726803358466);
        assert_eq!(rows[1].ts, 1726803359466);

        assert_eq!(rows[0].c1, 101);
        assert_eq!(rows[1].c1, 102);

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_insert_with_subtable_names() -> anyhow::Result<()> {
        let db = "stmt2_202412021600";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create stable s0 (ts timestamp, c1 int) tags(t1 int)",
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("insert into ? using s0 tags(?) values(?, ?)")
            .await?;

        let tbname = "d0";
        let tags = vec![Value::Int(100)];
        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466, 1726803357466]),
            ColumnView::from_ints(vec![100, 200]),
        ];
        let param = Stmt2BindParam::new(Some(tbname.to_owned()), Some(tags), Some(cols));

        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 2);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = taos
            .query(format!("select * from {tbname}"))
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].ts, 1726803356466);
        assert_eq!(rows[1].ts, 1726803357466);

        assert_eq!(rows[0].c1, 100);
        assert_eq!(rows[1].c1, 200);

        #[derive(Debug, Deserialize)]
        struct TagInfo {
            table_name: String,
            db_name: String,
            stable_name: String,
            tag_name: String,
            tag_type: Ty,
            tag_value: String,
        }

        let tag_infos: Vec<TagInfo> = taos
            .query(format!("show tags from {tbname}"))
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(tag_infos.len(), 1);

        assert_eq!(tag_infos[0].table_name, tbname);
        assert_eq!(tag_infos[0].db_name, db);
        assert_eq!(tag_infos[0].stable_name, "s0");
        assert_eq!(tag_infos[0].tag_name, "t1");
        assert_eq!(tag_infos[0].tag_type, Ty::Int);
        assert_eq!(tag_infos[0].tag_value, "100");

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_insert_with_col_tbname() -> anyhow::Result<()> {
        let db = "stmt2_202412061117";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create stable s0 (ts timestamp, c1 int) tags(t1 int)",
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("insert into s0 (tbname, ts, c1, t1) values(?, ?, ?, ?)")
            .await?;

        let tbname = "d0";
        let tags = vec![Value::Int(100)];
        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466, 1726803357466]),
            ColumnView::from_ints(vec![100, 200]),
        ];
        let param = Stmt2BindParam::new(Some(tbname.to_owned()), Some(tags), Some(cols));

        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 2);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = taos
            .query(format!("select * from {tbname}"))
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].ts, 1726803356466);
        assert_eq!(rows[1].ts, 1726803357466);

        assert_eq!(rows[0].c1, 100);
        assert_eq!(rows[1].c1, 200);

        #[derive(Debug, Deserialize)]
        struct TagInfo {
            table_name: String,
            db_name: String,
            stable_name: String,
            tag_name: String,
            tag_type: Ty,
            tag_value: String,
        }

        let tag_infos: Vec<TagInfo> = taos
            .query(format!("show tags from {tbname}"))
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(tag_infos.len(), 1);

        assert_eq!(tag_infos[0].table_name, tbname);
        assert_eq!(tag_infos[0].db_name, db);
        assert_eq!(tag_infos[0].stable_name, "s0");
        assert_eq!(tag_infos[0].tag_name, "t1");
        assert_eq!(tag_infos[0].tag_type, Ty::Int);
        assert_eq!(tag_infos[0].tag_value, "100");

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_result() -> anyhow::Result<()> {
        let db = "stmt2_202412021700";
        let dsn = "ws://localhost:6041";

        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        taos.exec_many(vec![
            &format!("drop database if exists {db}"),
            &format!("create database {db}"),
            &format!("use {db}"),
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2.prepare("insert into t0 values(?, ?)").await?;

        let res = stmt2.result_set().await;
        assert!(res.is_err());

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_blob() -> anyhow::Result<()> {
        use serde::Deserialize;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many(&[
            "drop database if exists test_1753080278",
            "create database test_1753080278",
            "use test_1753080278",
            "create table t0 (ts timestamp, c1 blob)",
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2.prepare("insert into t0 values(?, ?)").await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![
                1726803356466,
                1726803356467,
                1726803356468,
                1726803356469,
            ]),
            ColumnView::from_blob_bytes::<Vec<u8>, _, _, _>(vec![
                None,
                Some(vec![]),
                Some(vec![0x68, 0x65, 0x6C, 0x6C, 0x6F]),
                Some(vec![0x12, 0x34, 0x56, 0x78]),
            ]),
        ];
        let param = Stmt2BindParam::new(None, None, Some(cols));

        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 4);

        stmt2.prepare("select * from t0 where ts > ?").await?;

        let cols = vec![ColumnView::from_millis_timestamp(vec![1726803356465])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let _ = stmt2.exec().await?;

        #[derive(Debug, Deserialize)]
        struct Record {
            ts: i64,
            c1: Option<Vec<u8>>,
        }

        let records: Vec<Record> = stmt2
            .result_set()
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(records.len(), 4);

        assert_eq!(records[0].ts, 1726803356466);
        assert_eq!(records[1].ts, 1726803356467);
        assert_eq!(records[2].ts, 1726803356468);
        assert_eq!(records[3].ts, 1726803356469);

        assert_eq!(records[0].c1, None);
        assert_eq!(records[1].c1, Some(vec![]));
        assert_eq!(records[2].c1, Some(vec![0x68, 0x65, 0x6C, 0x6C, 0x6F]));
        assert_eq!(records[3].c1, Some(vec![0x12, 0x34, 0x56, 0x78]));

        taos.exec("drop database test_1753080278").await?;

        Ok(())
    }
}

#[cfg(feature = "rustls-aws-lc-crypto-provider")]
#[cfg(test)]
mod cloud_tests {
    use futures::TryStreamExt;
    use serde::Deserialize;
    use taos_query::common::ColumnView;
    use taos_query::stmt2::Stmt2BindParam;
    use taos_query::{AsyncFetchable, AsyncQueryable, AsyncTBuilder};

    use crate::stmt2::Stmt2;
    use crate::TaosBuilder;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_stmt2");
            return Ok(());
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_stmt2");
            return Ok(());
        }

        let dsn = format!("{}/rust_test?token={}", url.unwrap(), token.unwrap());
        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let tbname = format!("t_stmt2_{ts}");

        taos.exec_many([
            format!("drop table if exists {tbname}"),
            format!("create table {tbname} (ts timestamp, c1 int)"),
        ])
        .await?;

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;

        stmt2
            .prepare(format!("insert into {tbname} values(?, ?)"))
            .await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466]),
            ColumnView::from_ints(vec![100]),
        ];
        let param = Stmt2BindParam::new(Some(tbname.clone()), None, Some(cols));
        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 1);

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;

        stmt2
            .prepare(format!("select * from {tbname} where c1 > ?"))
            .await?;

        let cols = vec![ColumnView::from_ints(vec![0])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let affected = stmt2.exec().await?;
        assert_eq!(affected, 0);

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = stmt2
            .result_set()
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts, 1726803356466);
        assert_eq!(rows[0].c1, 100);

        taos.exec(format!("drop table {tbname}")).await?;

        Ok(())
    }
}
