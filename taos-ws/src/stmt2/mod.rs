use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use byteorder::{ByteOrder, LittleEndian};
use futures::channel::oneshot;
use taos_query::common::{Field, Precision};
use taos_query::stmt2::{Stmt2AsyncBindable, Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_query::{block_in_place_or_global, AsyncQueryable, Queryable, RawResult};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::Instrument;

use crate::query::asyn::{fetch_binary, ConnState, QueryMetrics};
use crate::query::messages::{ReqId, Stmt2Field, StmtId, WsRecvData, WsResArgs, WsSend};
use crate::query::WsTaos;
use crate::{ResultSet, Taos};

mod bind;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Stmt2Action {
    #[default]
    Init,
    Prepare,
    Bind,
    Exec,
    Result,
}

#[derive(Debug, Default)]
struct Stmt2Cache {
    req_id: ReqId,
    action: Stmt2Action,
    single_stb_insert: bool,
    single_table_bind_once: bool,
    sql: String,
    bind_bytes: Vec<Vec<u8>>,
}

impl Stmt2Cache {
    fn build_req_and_bind_bytes(&self, stmt_id: StmtId) -> (WsSend, Vec<Vec<u8>>) {
        match self.action {
            Stmt2Action::Init => (self.build_init_req(), Vec::new()),
            Stmt2Action::Prepare => (self.build_prepare_req(stmt_id), Vec::new()),
            Stmt2Action::Bind => {
                let mut bind_bytes = self.build_bind_bytes(stmt_id);
                let req = WsSend::Binary(bind_bytes.remove(bind_bytes.len() - 1));
                (req, bind_bytes)
            }
            Stmt2Action::Exec => (self.build_exec_req(stmt_id), self.build_bind_bytes(stmt_id)),
            Stmt2Action::Result => (
                self.build_result_req(stmt_id),
                self.build_bind_bytes(stmt_id),
            ),
        }
    }

    fn build_init_req(&self) -> WsSend {
        WsSend::Stmt2Init {
            req_id: self.req_id_with_action(Stmt2Action::Init),
            single_stb_insert: self.single_stb_insert,
            single_table_bind_once: self.single_table_bind_once,
        }
    }

    fn build_prepare_req(&self, stmt_id: StmtId) -> WsSend {
        WsSend::Stmt2Prepare {
            req_id: self.req_id_with_action(Stmt2Action::Prepare),
            stmt_id,
            sql: self.sql.clone(),
            get_fields: true,
        }
    }

    fn build_bind_bytes(&self, stmt_id: StmtId) -> Vec<Vec<u8>> {
        let len = self.bind_bytes.len();
        let mut res = Vec::with_capacity(len);
        for (i, bytes) in self.bind_bytes.iter().enumerate() {
            let req_id = if i == len - 1 {
                self.req_id_with_action(Stmt2Action::Bind)
            } else {
                generate_req_id()
            };
            let mut bytes = bytes.clone();
            LittleEndian::write_u64(&mut bytes[bind::REQ_ID_POS..], req_id);
            LittleEndian::write_u64(&mut bytes[bind::STMT_ID_POS..], stmt_id);
            res.push(bytes);
        }
        res
    }

    fn build_exec_req(&self, stmt_id: StmtId) -> WsSend {
        WsSend::Stmt2Exec {
            req_id: self.req_id_with_action(Stmt2Action::Exec),
            stmt_id,
        }
    }

    fn build_result_req(&self, stmt_id: StmtId) -> WsSend {
        WsSend::Stmt2Result {
            req_id: self.req_id_with_action(Stmt2Action::Result),
            stmt_id,
        }
    }

    fn req_id_with_action(&self, action: Stmt2Action) -> ReqId {
        if action == self.action {
            self.req_id
        } else {
            generate_req_id()
        }
    }
}

#[derive(Debug)]
pub(super) struct Stmt2Inner {
    id: u64,
    client: Arc<WsTaos>,
    stmt_id: Arc<AtomicU64>,
    is_insert: Arc<AtomicBool>,
    fields: Arc<RwLock<Option<Vec<Stmt2Field>>>>,
    fields_count: Arc<AtomicUsize>,
    affected_rows: Arc<AtomicUsize>,
    affected_rows_once: Arc<AtomicUsize>,
    cache: Arc<Mutex<Stmt2Cache>>,
    is_complete: Arc<AtomicBool>,
}

impl Stmt2Inner {
    fn new(client: Arc<WsTaos>) -> Self {
        Self {
            id: generate_req_id(),
            client,
            stmt_id: Arc::default(),
            is_insert: Arc::default(),
            fields: Arc::default(),
            fields_count: Arc::default(),
            affected_rows: Arc::default(),
            affected_rows_once: Arc::default(),
            is_complete: Arc::default(),
            cache: Arc::default(),
        }
    }

    async fn init(self: Arc<Self>) -> RawResult<()> {
        self.init_with_options(generate_req_id(), true, false).await
    }

    async fn init_with_options(
        self: Arc<Self>,
        req_id: u64,
        single_stb_insert: bool,
        single_table_bind_once: bool,
    ) -> RawResult<()> {
        self.client.wait_for_reconnect().await?;
        self.client.insert_stmt2(self.clone());

        let mut cache = self.cache.lock().await;
        cache.req_id = req_id;
        cache.action = Stmt2Action::Init;
        cache.single_stb_insert = single_stb_insert;
        cache.single_table_bind_once = single_table_bind_once;
        drop(cache);

        self._init_with_options(req_id, single_stb_insert, single_table_bind_once)
            .await
    }

    async fn _init_with_options(
        &self,
        req_id: ReqId,
        single_stb_insert: bool,
        single_table_bind_once: bool,
    ) -> RawResult<()> {
        let req = WsSend::Stmt2Init {
            req_id,
            single_stb_insert,
            single_table_bind_once,
        };
        tracing::trace!(
            "stmt2 init, id: {}, old stmt_id: {}, req: {req:?}",
            self.id,
            self.stmt_id(),
        );
        let resp = self.send_request(req).await?;
        if let WsRecvData::Stmt2Init { stmt_id, .. } = resp {
            self.stmt_id.store(stmt_id, Ordering::Release);
            tracing::trace!(
                "stmt2 init, id: {}, new stmt_id: {}",
                self.id,
                self.stmt_id()
            );
            return Ok(());
        }
        unreachable!("unexpected stmt2 init response: {resp:?}");
    }

    async fn prepare<S: AsRef<str> + Send>(&self, sql: S) -> RawResult<()> {
        self.client.wait_for_reconnect().await?;

        let req_id = generate_req_id();
        let mut cache = self.cache.lock().await;
        cache.req_id = req_id;
        cache.action = Stmt2Action::Prepare;
        cache.sql = sql.as_ref().to_string();
        drop(cache);

        self._prepare(req_id, sql).await
    }

    async fn _prepare<S: AsRef<str> + Send>(&self, req_id: ReqId, sql: S) -> RawResult<()> {
        let req = WsSend::Stmt2Prepare {
            req_id,
            stmt_id: self.stmt_id(),
            sql: sql.as_ref().to_string(),
            get_fields: true,
        };
        tracing::trace!("stmt2 prepare, id: {}, req: {req:?}", self.id);
        let resp = self.send_request(req).await?;
        if let WsRecvData::Stmt2Prepare {
            is_insert,
            fields,
            fields_count,
            ..
        } = resp
        {
            self.is_insert.store(is_insert, Ordering::Relaxed);
            *self.fields.write().await = fields;
            self.fields_count.store(fields_count, Ordering::Relaxed);
            return Ok(());
        }
        unreachable!("unexpected stmt2 prepare response: {resp:?}");
    }

    async fn bind(&self, params: &[Stmt2BindParam]) -> RawResult<()> {
        self.client.wait_for_reconnect().await?;

        let req_id = generate_req_id();
        let mut cache = self.cache.lock().await;
        cache.req_id = req_id;
        cache.action = Stmt2Action::Bind;

        let is_insert = self.is_insert.load(Ordering::Relaxed);
        let fields_guard = self.fields.read().await;
        let fields = fields_guard.as_ref();
        let fields_count = self.fields_count.load(Ordering::Relaxed);

        let bytes = bind::bind_params_to_bytes(
            params,
            req_id,
            self.stmt_id(),
            is_insert,
            fields,
            fields_count,
        )?;

        drop(fields_guard);

        cache.bind_bytes.push(bytes.clone());
        drop(cache);

        tracing::trace!("stmt2 bind, id: {}, req_id: {req_id}", self.id);

        self._bind(bytes).await
    }

    async fn _bind(&self, bytes: Vec<u8>) -> RawResult<()> {
        let req = WsSend::Binary(bytes);
        tracing::trace!("stmt2 bind, id: {}, req: {req:?}", self.id);
        let resp = self.send_request(req).await?;
        if let WsRecvData::Stmt2Bind { .. } = resp {
            return Ok(());
        }
        unreachable!("unexpected stmt2 bind response: {resp:?}");
    }

    async fn exec(&self) -> RawResult<usize> {
        self.client.wait_for_reconnect().await?;

        let req_id = generate_req_id();
        let mut cache = self.cache.lock().await;
        cache.req_id = req_id;
        cache.action = Stmt2Action::Exec;
        drop(cache);

        self._exec(req_id, false).await
    }

    async fn _exec(&self, req_id: ReqId, recovering: bool) -> RawResult<usize> {
        let req = WsSend::Stmt2Exec {
            req_id,
            stmt_id: self.stmt_id(),
        };
        tracing::trace!("stmt2 exec, id: {}, req: {req:?}", self.id);
        let resp = self.send_request(req).await?;
        if let WsRecvData::Stmt2Exec { affected, .. } = resp {
            if self.is_insert() {
                self.cache.lock().await.bind_bytes.clear();
            }
            if !recovering {
                self.affected_rows.fetch_add(affected, Ordering::Relaxed);
                self.affected_rows_once.store(affected, Ordering::Relaxed);
            }
            return Ok(affected);
        }
        unreachable!("unexpected stmt2 exec response: {resp:?}");
    }

    fn close(&self) {
        if self.client.state() != ConnState::Connected {
            tracing::warn!("cannot close Stmt2 when client is not connected");
            return;
        }

        let req = WsSend::Stmt2Close {
            req_id: generate_req_id(),
            stmt_id: self.stmt_id(),
        };
        tracing::trace!("stmt2 close, id: {}, req: {req:?}", self.id);
        let resp = block_in_place_or_global(self.client.send_request(req));
        match resp {
            Ok(WsRecvData::Stmt2Close { .. }) => tracing::trace!("close Stmt2 successfully"),
            Err(err) => tracing::error!("failed to close Stmt2: {err:?}"),
            resp => unreachable!("unexpected stmt2 close response: {resp:?}"),
        }
    }

    async fn result_set(&self) -> RawResult<ResultSet> {
        self.client.wait_for_reconnect().await?;

        let req_id = generate_req_id();
        let mut cache = self.cache.lock().await;
        cache.req_id = req_id;
        cache.action = Stmt2Action::Result;
        drop(cache);

        self._result_set(req_id).await
    }

    async fn _result_set(&self, req_id: ReqId) -> RawResult<ResultSet> {
        if self.is_insert() {
            return Err("Only query can use result".into());
        }

        let req = WsSend::Stmt2Result {
            req_id,
            stmt_id: self.stmt_id(),
        };
        tracing::trace!("stmt2 result, id: {}, req: {req:?}", self.id);
        let resp = self.send_request(req).await?;
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
            self.cache.lock().await.bind_bytes.clear();

            let (close_tx, close_rx) = oneshot::channel();
            tokio::spawn(
                async move {
                    let start = Instant::now();
                    let _ = close_rx.await;
                    tracing::trace!("stmt2 result:{id} lived {:?}", start.elapsed());
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
                affected_rows: self.affected_rows_once(),
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

        unreachable!("unexpected stmt2 result response: {resp:?}");
    }

    pub(super) async fn recover(&self) -> RawResult<()> {
        use Stmt2Action::*;

        let (action, single_stb_insert, single_table_bind_once, sql) = {
            let cache = self.cache.lock().await;
            (
                cache.action,
                cache.single_stb_insert,
                cache.single_table_bind_once,
                cache.sql.clone(),
            )
        };

        tracing::trace!(
            "stmt2 recover, id: {}, action: {action:?}, single_stb_insert: {single_stb_insert}, \
            single_table_bind_once: {single_table_bind_once}, sql: {sql}",
            self.id
        );

        if !self.is_complete() {
            if matches!(action, Prepare | Bind | Exec | Result) {
                self._init_with_options(
                    generate_req_id(),
                    single_stb_insert,
                    single_table_bind_once,
                )
                .await?;
            }

            if matches!(action, Bind | Exec | Result) {
                self._prepare(generate_req_id(), sql).await?;
            }

            let (req, bind_bytes) = {
                self.cache
                    .lock()
                    .await
                    .build_req_and_bind_bytes(self.stmt_id())
            };

            for bytes in bind_bytes {
                self._bind(bytes).await?;
            }

            if matches!(action, Result) {
                self._exec(generate_req_id(), true).await?;
            }

            self.client.send_only(req).await?;
        } else {
            self._init_with_options(generate_req_id(), single_stb_insert, single_table_bind_once)
                .await?;

            if matches!(action, Prepare | Bind | Exec | Result) {
                self._prepare(generate_req_id(), sql.clone()).await?;
            }

            let bind_bytes = { self.cache.lock().await.build_bind_bytes(self.stmt_id()) };
            for bytes in bind_bytes {
                self._bind(bytes).await?;
            }

            if action == Exec && !self.is_insert() {
                self._exec(generate_req_id(), true).await?;
            }
        }

        Ok(())
    }

    async fn send_request(&self, req: WsSend) -> RawResult<WsRecvData> {
        self.set_complete(false);
        let res = self.client.send_request(req).await;
        self.set_complete(true);
        res
    }

    fn set_complete(&self, completed: bool) {
        self.is_complete.store(completed, Ordering::Relaxed);
    }

    fn is_complete(&self) -> bool {
        self.is_complete.load(Ordering::Relaxed)
    }

    fn stmt_id(&self) -> StmtId {
        self.stmt_id.load(Ordering::Acquire)
    }

    fn is_insert(&self) -> bool {
        self.is_insert.load(Ordering::Relaxed)
    }

    fn affected_rows_once(&self) -> usize {
        self.affected_rows_once.load(Ordering::Relaxed)
    }

    pub(super) fn id(&self) -> u64 {
        self.id
    }

    pub(super) async fn req_id(&self) -> ReqId {
        self.cache.lock().await.req_id
    }
}

impl Drop for Stmt2Inner {
    fn drop(&mut self) {
        tracing::trace!("dropping Stmt2Inner, closing Stmt2");
        self.client.remove_stmt2(self.id);
        self.close();
    }
}

#[derive(Debug, Clone)]
pub struct Stmt2 {
    inner: Arc<Stmt2Inner>,
}

impl Stmt2 {
    pub fn new(client: Arc<WsTaos>) -> Self {
        Self {
            inner: Arc::new(Stmt2Inner::new(client)),
        }
    }

    pub async fn init_with_options(
        &self,
        req_id: u64,
        single_stb_insert: bool,
        single_table_bind_once: bool,
    ) -> RawResult<()> {
        self.inner
            .clone()
            .init_with_options(req_id, single_stb_insert, single_table_bind_once)
            .await
    }

    async fn init(&self) -> RawResult<()> {
        self.inner.clone().init().await
    }

    async fn prepare<S: AsRef<str> + Send>(&self, sql: S) -> RawResult<()> {
        self.inner.prepare(sql).await
    }

    async fn bind(&self, params: &[Stmt2BindParam]) -> RawResult<()> {
        self.inner.bind(params).await
    }

    async fn exec(&self) -> RawResult<usize> {
        self.inner.exec().await
    }

    async fn result_set(&self) -> RawResult<ResultSet> {
        self.inner.result_set().await
    }

    pub fn is_insert(&self) -> Option<bool> {
        Some(self.inner.is_insert())
    }

    pub fn fields(&self) -> Option<Vec<Stmt2Field>> {
        block_in_place_or_global(self.inner.fields.read()).clone()
    }

    pub fn fields_count(&self) -> Option<usize> {
        Some(self.inner.fields_count.load(Ordering::Relaxed))
    }

    pub fn affected_rows(&self) -> usize {
        self.inner.affected_rows.load(Ordering::Relaxed)
    }

    pub fn affected_rows_once(&self) -> usize {
        self.inner.affected_rows_once()
    }
}

impl Stmt2Bindable<super::Taos> for Stmt2 {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        let stmt2 = Self::new(taos.client());
        block_in_place_or_global(stmt2.init())?;
        Ok(stmt2)
    }

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        block_in_place_or_global(Stmt2::prepare(self, sql))?;
        Ok(self)
    }

    fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        block_in_place_or_global(Stmt2::bind(self, params))?;
        Ok(self)
    }

    fn exec(&mut self) -> RawResult<usize> {
        block_in_place_or_global(Stmt2::exec(self))
    }

    fn affected_rows(&self) -> usize {
        self.affected_rows()
    }

    fn result_set(&self) -> RawResult<<Taos as Queryable>::ResultSet> {
        block_in_place_or_global(self.result_set())
    }
}

#[async_trait::async_trait]
impl Stmt2AsyncBindable<super::Taos> for Stmt2 {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        let stmt2 = Self::new(taos.client());
        stmt2.init().await?;
        Ok(stmt2)
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        Stmt2::prepare(self, sql).await?;
        Ok(self)
    }

    async fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        Stmt2::bind(self, params).await?;
        Ok(self)
    }

    async fn exec(&mut self) -> RawResult<usize> {
        Stmt2::exec(self).await
    }

    async fn affected_rows(&self) -> usize {
        self.affected_rows()
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

        let stmt2 = Stmt2::new(taos.client());
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

        let stmt2 = Stmt2::new(taos.client());
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

        let stmt2 = Stmt2::new(taos.client());
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

        let stmt2 = Stmt2::new(taos.client());
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

        let stmt2 = Stmt2::new(taos.client());
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

        let stmt2 = Stmt2::new(taos.client());
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

        let stmt2 = Stmt2::new(taos.client());
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

#[cfg(test)]
mod ws_proxy {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio::sync::{Mutex, Notify};
    use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

    pub type InterceptFn = Arc<dyn Fn(&Message, &mut ProxyContext) -> ProxyAction + Send + Sync>;

    #[non_exhaustive]
    #[derive(Debug, Clone, Copy)]
    pub enum ProxyAction {
        Restart,
        Forward,
    }

    #[derive(Debug, Clone)]
    pub struct ProxyContext {
        pub req_count: usize,
    }

    pub struct WsProxy {
        stop: Arc<Notify>,
    }

    impl WsProxy {
        pub async fn start(
            listen_addr: &str,
            backend_url: &str,
            intercept_fn: InterceptFn,
        ) -> Self {
            tracing::info!("starting WebSocket proxy on {listen_addr}, backend: {backend_url}");

            let listen_addr = listen_addr.parse().unwrap();
            let backend_url = backend_url.to_string();
            let running = Arc::new(AtomicBool::new(true));
            let ctx = Arc::new(Mutex::new(ProxyContext { req_count: 0 }));
            let stop_notify = Arc::new(Notify::new());
            let stop = stop_notify.clone();

            tokio::spawn(async move {
                loop {
                    run_proxy_server(
                        listen_addr,
                        backend_url.clone(),
                        intercept_fn.clone(),
                        running.clone(),
                        ctx.clone(),
                        stop_notify.clone(),
                    )
                    .await;

                    if running.load(Ordering::Relaxed) {
                        tracing::info!("stopping WebSocket proxy...");
                        break;
                    } else {
                        tracing::info!("restarting WebSocket proxy...");
                        running.store(true, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            });

            Self { stop }
        }
    }

    impl Drop for WsProxy {
        fn drop(&mut self) {
            tracing::info!("dropping WebSocket proxy");
            self.stop.notify_waiters();
        }
    }

    async fn run_proxy_server(
        listen_addr: SocketAddr,
        backend_url: String,
        intercept_fn: InterceptFn,
        running: Arc<AtomicBool>,
        ctx: Arc<Mutex<ProxyContext>>,
        stop_notify: Arc<Notify>,
    ) {
        let listener = match TcpListener::bind(listen_addr).await {
            Ok(listener) => listener,
            Err(err) => {
                tracing::error!("failed to bind to {listen_addr}: {err}");
                return;
            }
        };

        loop {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    let backend_url = backend_url.clone();
                    let intercept_fn = intercept_fn.clone();
                    let running = running.clone();
                    let ctx = ctx.clone();
                    let stop_notify = stop_notify.clone();

                    tokio::spawn(async move {
                        let client = match accept_async(stream).await {
                            Ok(ws) => ws,
                            Err(err) => {
                                tracing::error!("failed to accept WebSocket connection: {err}");
                                return;
                            }
                        };
                        let (backend, _) = match connect_async(backend_url).await {
                            Ok(ws) => ws,
                            Err(err) => {
                                tracing::error!("failed to connect to backend: {err}");
                                return;
                            }
                        };

                        let (mut client_sink, mut client_stream) = client.split();
                        let (mut backend_sink, mut backend_stream) = backend.split();

                        let req_to_backend = async {
                            while let Some(Ok(msg)) = client_stream.next().await {
                                tracing::trace!("received message from client: {msg:?}");
                                let mut ctx = ctx.lock().await;
                                match intercept_fn(&msg, &mut *ctx) {
                                    ProxyAction::Restart => {
                                        running.store(false, Ordering::Relaxed);
                                        stop_notify.notify_waiters();
                                        break;
                                    }
                                    ProxyAction::Forward => {
                                        if let Err(err) = backend_sink.send(msg).await {
                                            tracing::error!("failed to send message to backend: {err:?}");
                                            break;
                                        }
                                    }
                                }
                            }
                        };

                        let resp_to_client = async {
                            while let Some(Ok(msg)) = backend_stream.next().await {
                                tracing::trace!("received message from backend: {msg:?}");
                                if let Err(err) = client_sink.send(msg).await {
                                    tracing::error!("failed to send message to client: {err:?}");
                                    break;
                                }
                            }
                        };

                        tokio::select! {
                            _ = req_to_backend => {},
                            _ = resp_to_client => {},
                        }
                    });
                }
                _ = stop_notify.notified() => {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod recover_tests {
    use std::sync::Arc;

    use futures::future::try_join_all;
    use futures::TryStreamExt;
    use rand::Rng;
    use serde::Deserialize;
    use serde_json::Value;
    use taos_query::common::ColumnView;
    use taos_query::stmt2::Stmt2BindParam;
    use taos_query::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, RawError};
    use tokio_tungstenite::tungstenite::Message;

    use super::ws_proxy::{InterceptFn, ProxyAction, WsProxy};
    use crate::{Stmt2, TaosBuilder};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_init() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, ctx| {
                if let Message::Text(text) = msg {
                    let req = serde_json::from_str::<Value>(text).unwrap();
                    let action = req.get("action").and_then(|v| v.as_str()).unwrap_or("");
                    if action == "stmt2_init" {
                        ctx.req_count += 1;
                        if ctx.req_count == 1 {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        WsProxy::start("127.0.0.1:8811", "ws://localhost:6041/ws", intercept_fn).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8811")?
            .build()
            .await?;

        let stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_prepare() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, ctx| {
                if let Message::Text(text) = msg {
                    let req = serde_json::from_str::<Value>(text).unwrap();
                    let action = req.get("action").and_then(|v| v.as_str()).unwrap_or("");
                    if action == "stmt2_prepare" {
                        ctx.req_count += 1;
                        if ctx.req_count == 1 {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        WsProxy::start("127.0.0.1:8812", "ws://localhost:6041/ws", intercept_fn).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8812")?
            .build()
            .await?;

        taos.exec_many(&[
            "drop database if exists test_1755136975",
            "create database test_1755136975",
            "use test_1755136975",
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values(1726803356466, 99)",
            "insert into t0 values(1726803357466, 100)",
        ])
        .await?;

        let stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("select * from test_1755136975.t0 where c1 > ?")
            .await?;

        taos.exec("drop database test_1755136975").await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bind() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, ctx| {
                if let Message::Binary(bytes) = msg {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    if action == 9 {
                        ctx.req_count += 1;
                        if ctx.req_count == 1 {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        WsProxy::start("127.0.0.1:8813", "ws://localhost:6041/ws", intercept_fn).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8813")?
            .build()
            .await?;

        taos.exec_many(&[
            "drop database if exists test_1755137215",
            "create database test_1755137215",
            "use test_1755137215",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("insert into test_1755137215.t0 values(?, ?)")
            .await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466, 1726803357466]),
            ColumnView::from_ints(vec![100, 200]),
        ];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        taos.exec("drop database test_1755137215").await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, ctx| {
                if let Message::Text(text) = msg {
                    let req = serde_json::from_str::<Value>(text).unwrap();
                    let action = req.get("action").and_then(|v| v.as_str()).unwrap_or("");
                    if action == "stmt2_exec" {
                        ctx.req_count += 1;
                        if ctx.req_count == 1 {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        WsProxy::start("127.0.0.1:8814", "ws://localhost:6041/ws", intercept_fn).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8814")?
            .build()
            .await?;

        taos.exec_many(&[
            "drop database if exists test_1755137720",
            "create database test_1755137720",
            "use test_1755137720",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("insert into test_1755137720.t0 values(?, ?)")
            .await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466, 1726803357466]),
            ColumnView::from_ints(vec![100, 200]),
        ];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803358466, 1726803359466]),
            ColumnView::from_ints(vec![100, 200]),
        ];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        let rows = stmt2.exec().await?;
        assert_eq!(rows, 4);

        taos.exec("drop database test_1755137720").await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_result() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, ctx| {
                if let Message::Text(text) = msg {
                    let req = serde_json::from_str::<Value>(text).unwrap();
                    let action = req.get("action").and_then(|v| v.as_str()).unwrap_or("");
                    if action == "stmt2_result" {
                        ctx.req_count += 1;
                        if ctx.req_count == 1 {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        WsProxy::start("127.0.0.1:8815", "ws://localhost:6041/ws", intercept_fn).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8815")?
            .build()
            .await?;

        taos.exec_many(&[
            "drop database if exists test_1755138202",
            "create database test_1755138202",
            "use test_1755138202",
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values(1726803356466, 99)",
            "insert into t0 values(1726803357466, 100)",
        ])
        .await?;

        let stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("select * from test_1755138202.t0 where c1 > ?")
            .await?;

        let cols = vec![ColumnView::from_ints(vec![99])];
        let param = Stmt2BindParam::new(None, None, Some(cols));
        stmt2.bind(&[param]).await?;

        stmt2.exec().await?;

        #[derive(Debug, Deserialize)]
        struct Record {
            ts: i64,
            c1: i32,
        }

        let records: Vec<Record> = stmt2
            .result_set()
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].ts, 1726803357466);
        assert_eq!(records[0].c1, 100);

        taos.exec("drop database test_1755138202").await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, _ctx| {
                if let Message::Text(text) = msg {
                    if text.contains("stmt") {
                        if rand::rng().random_bool(0.05) {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        WsProxy::start("127.0.0.1:8816", "ws://localhost:6041/ws", intercept_fn).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8816")?
            .build()
            .await?;

        taos.exec_many(&[
            "drop database if exists test_1755138447",
            "create database test_1755138447",
            "use test_1755138447",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let n = 20;
        let mut tasks = Vec::with_capacity(n);
        for i in 0..n {
            let client = taos.client();
            tasks.push(tokio::spawn(async move {
                let stmt2 = Stmt2::new(client);
                stmt2.init().await?;
                stmt2
                    .prepare("insert into test_1755138447.t0 values(?, ?)")
                    .await?;

                let ts = 1726803356466 + i as i64;
                let c1 = 100 + i as i32;
                let cols = vec![
                    ColumnView::from_millis_timestamp(vec![ts]),
                    ColumnView::from_ints(vec![c1]),
                ];
                let param = Stmt2BindParam::new(None, None, Some(cols));
                stmt2.bind(&[param]).await?;

                let affected = stmt2.exec().await?;
                assert_eq!(affected, 1);

                stmt2
                    .prepare("select * from test_1755138447.t0 where c1 = ?")
                    .await?;

                let cols = vec![ColumnView::from_ints(vec![c1])];
                let param = Stmt2BindParam::new(None, None, Some(cols));
                stmt2.bind(&[param]).await?;

                stmt2.exec().await?;

                #[derive(Debug, Deserialize)]
                struct Record {
                    ts: i64,
                    c1: i32,
                }

                let res: Result<Vec<Record>, RawError> =
                    stmt2.result_set().await?.deserialize().try_collect().await;

                if let Err(err) = res {
                    tracing::error!("failed to deserialize records: {err:?}");
                } else {
                    let records = res.unwrap();
                    assert_eq!(records.len(), 1);
                    assert_eq!(records[0].ts, ts);
                    assert_eq!(records[0].c1, c1);
                }

                Ok::<_, anyhow::Error>(())
            }));
        }

        let results = try_join_all(tasks).await.unwrap();
        for res in results {
            res?;
        }

        taos.exec("drop database test_1755138447").await?;

        Ok(())
    }
}
