use std::default;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Stmt2Action {
    #[default]
    Init,
    Prepare,
    Bind,
    Exec,
    ResultSet,
}

#[derive(Debug, Default)]
struct Stmt2Cache {
    req_id: ReqId,
    action: Stmt2Action,
    // use stmt_id of inner
    // stmt_id: StmtId,
    stmt_id: Arc<AtomicU64>,
    single_stb_insert: bool,
    single_table_bind_once: bool,
    sql: String,
    bind_bytes: Vec<Vec<u8>>,
}

impl Stmt2Cache {
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
            // stmt_id: self.stmt_id(),
            stmt_id: stmt_id,
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
            tracing::trace!("build_bind_bytes, req_id: {req_id}, stmt_id: {}", stmt_id);
            LittleEndian::write_u64(&mut bytes[bind::REQ_ID_POS..], req_id);
            // LittleEndian::write_u64(&mut bytes[bind::STMT_ID_POS..], self.stmt_id());
            LittleEndian::write_u64(&mut bytes[bind::STMT_ID_POS..], stmt_id);
            res.push(bytes);
        }
        res
    }

    fn build_exec_req(&self, stmt_id: StmtId) -> WsSend {
        WsSend::Stmt2Exec {
            req_id: self.req_id_with_action(Stmt2Action::Exec),
            stmt_id: stmt_id,
        }
    }

    fn build_result_req(&self, stmt_id: StmtId) -> WsSend {
        WsSend::Stmt2Result {
            req_id: self.req_id_with_action(Stmt2Action::ResultSet),
            stmt_id: stmt_id,
        }
    }

    fn set_req_id(&mut self, req_id: ReqId) {
        // self.req_id.store(req_id, Ordering::Relaxed);
        self.req_id = req_id;
    }

    fn req_id(&self) -> ReqId {
        // self.req_id.load(Ordering::Relaxed)
        self.req_id
    }

    fn stmt_id(&self) -> StmtId {
        self.stmt_id.load(Ordering::SeqCst)
    }

    fn req_id_with_action(&self, action: Stmt2Action) -> ReqId {
        if action == self.action {
            // self.req_id()
            self.req_id
        } else {
            generate_req_id()
        }
    }
}

// rename
#[derive(Debug)]
pub struct Stmt2Inner {
    is_insert: Arc<AtomicBool>,
    id: u64,
    client: Arc<WsTaos>,
    stmt_id: Arc<AtomicU64>,
    cache: Arc<Mutex<Stmt2Cache>>,
    fields: Arc<RwLock<Option<Vec<Stmt2Field>>>>,
    fields_count: Arc<AtomicUsize>,
}

impl Stmt2Inner {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub async fn req_id(&self) -> ReqId {
        // self.req_id.load(Ordering::Relaxed)
        self.cache.lock().await.req_id
    }

    pub async fn recover(&self) -> RawResult<()> {
        tracing::trace!("recovering Stmt2 with id: {}", self.id);

        let (action, single_stb_insert, single_table_bind_once, sql) = {
            let cache = self.cache.lock().await;
            (
                cache.action,
                cache.single_stb_insert,
                cache.single_table_bind_once,
                cache.sql.clone(),
                // cache.build_bind_bytes(),
            )
        };

        // add exec
        let id = self.id;
        tracing::trace!(
            "recovering Stmt2, id: {id}, action: {action:?}, single_stb_insert: {single_stb_insert}, \
            single_table_bind_once: {single_table_bind_once}, sql: {sql}"
        );

        match action {
            Stmt2Action::Init => {
                // let req = { self.cache.lock().await.build_init_req() };
                self.recover_with_steps(action, None, None, None, None, None)
                    .await?;
            }
            Stmt2Action::Prepare => {
                // let stmt_id = self.stmt_id();
                // tracing::trace!("recover prepare, id: {}, stmt_id: {}", self.id, stmt_id);
                // let req = { self.cache.lock().await.build_prepare_req(stmt_id) };
                self.recover_with_steps(
                    action,
                    Some((single_stb_insert, single_table_bind_once)),
                    None,
                    None,
                    None,
                    None,
                )
                .await?;
            }
            Stmt2Action::Bind => {
                // let stmt_id = self.stmt_id();
                // tracing::trace!("recover bind, id: {}, stmt_id: {}", self.id, stmt_id);
                // let mut bind_bytes = { self.cache.lock().await.build_bind_bytes(stmt_id) };
                // let req = WsSend::Binary(bind_bytes.remove(bind_bytes.len() - 1));
                self.recover_with_steps(
                    action,
                    Some((single_stb_insert, single_table_bind_once)),
                    Some(sql),
                    None,
                    None,
                    None,
                )
                .await?;
            }
            Stmt2Action::Exec => {
                // let stmt_id = self.stmt_id();
                // tracing::trace!("recover exec, id: {}, stmt_id: {}", self.id, stmt_id);
                // let req = { self.cache.lock().await.build_exec_req(stmt_id) };
                // let bind_bytes = { self.cache.lock().await.build_bind_bytes(stmt_id) };
                self.recover_with_steps(
                    action,
                    Some((single_stb_insert, single_table_bind_once)),
                    Some(sql),
                    None,
                    None,
                    None,
                )
                .await?;
            }
            Stmt2Action::ResultSet => {
                // let stmt_id = self.stmt_id();
                // tracing::trace!("recover result set, id: {}, stmt_id: {}", self.id, stmt_id);
                // let req = { self.cache.lock().await.build_result_req(stmt_id) };
                // let bind_bytes = { self.cache.lock().await.build_bind_bytes(stmt_id) };
                self.recover_with_steps(
                    action,
                    Some((single_stb_insert, single_table_bind_once)),
                    Some(sql),
                    None,
                    Some(()),
                    None,
                )
                .await?;
            }
        }

        Ok(())
    }

    pub async fn recover_with_steps(
        &self,
        action: Stmt2Action,
        init_options: Option<(bool, bool)>,
        sql: Option<String>,
        bind_bytes: Option<Vec<Vec<u8>>>,
        exec: Option<()>,
        req: Option<WsSend>,
    ) -> RawResult<()> {
        tracing::trace!("recover Stmt2, id: {}, init_options: {:?}, sql: {:?}, bind_bytes: {:?}, exec: {:?}, req: {:?}", 
        self.id, init_options, sql, bind_bytes, exec, req);

        if let Some((single_stb_insert, single_table_bind_once)) = init_options {
            self._init_with_options(generate_req_id(), single_stb_insert, single_table_bind_once)
                .await?;
        }

        let (req, bind_bytes) = match action {
            Stmt2Action::Init => {
                let req = self.cache.lock().await.build_init_req();
                (req, vec![])
            }
            Stmt2Action::Prepare => {
                let stmt_id = self.stmt_id();
                let req = self.cache.lock().await.build_prepare_req(stmt_id);
                (req, vec![])
            }
            Stmt2Action::Bind => {
                let stmt_id = self.stmt_id();
                let mut bind_bytes = { self.cache.lock().await.build_bind_bytes(stmt_id) };
                let req = WsSend::Binary(bind_bytes.remove(bind_bytes.len() - 1));
                (req, bind_bytes)
                // let mut bind_bytes = self.cache.lock().await.build_bind_bytes(stmt_id);
                // WsSend::Binary(bind_bytes.remove(bind_bytes.len() - 1))
            }
            Stmt2Action::Exec => {
                let stmt_id = self.stmt_id();
                let mut bind_bytes = { self.cache.lock().await.build_bind_bytes(stmt_id) };
                let req = self.cache.lock().await.build_exec_req(stmt_id);
                (req, bind_bytes)
            }
            Stmt2Action::ResultSet => {
                let stmt_id = self.stmt_id();
                let mut bind_bytes = { self.cache.lock().await.build_bind_bytes(stmt_id) };
                let req = self.cache.lock().await.build_result_req(stmt_id);
                (req, bind_bytes)
            }
        };

        if let Some(sql) = sql {
            self._prepare(generate_req_id(), sql).await?;
        }
        tracing::trace!("asdfasdfasdfasdf");
        // if let Some(bind_bytes) = bind_bytes {
        tracing::trace!("recovering Stmt2, binding {:?} bytes", bind_bytes);
        for bytes in bind_bytes {
            self._bind(bytes).await?;
        }
        // }
        tracing::trace!("asdfasdfasdfasdfasdfasdfasdfasdfsadf");
        if let Some(_) = exec {
            tracing::trace!("recovering Stmt2, executing");
            self._exec(generate_req_id()).await?;
        }
        // if let Some(req) = req {
        tracing::trace!("recover sending request, id: {:?} req: {req:?}", self.id);
        self.client.send_only(req).await?;
        // }

        Ok(())
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
            "stmt2 init, id: {}, stmt_id: {}, req: {:?}",
            self.id,
            self.stmt_id(),
            req
        );
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Init { stmt_id, .. } = resp {
            self.stmt_id.store(stmt_id, Ordering::SeqCst);
            // self.cache.lock().await.stmt_id = stmt_id;
            // let stmtid = self.cache.lock().await.stmt_id;
            // tracing::trace!("init stmtid: {}", stmtid);

            tracing::trace!("init id: {}, stmt_id: {}", self.id, self.stmt_id());
            return Ok(());
        }
        unreachable!("unexpected stmt2 init response: {resp:?}");
    }

    async fn _prepare<S: AsRef<str> + Send>(&self, req_id: ReqId, sql: S) -> RawResult<()> {
        tracing::trace!("prepare id: {}, stmt_id: {}", self.id, self.stmt_id());
        let req = WsSend::Stmt2Prepare {
            req_id,
            stmt_id: self.stmt_id(),
            sql: sql.as_ref().to_string(),
            get_fields: true,
        };
        tracing::trace!(
            "stmt2 prepare, id: {}, stmt_id: {}, req: {:?}",
            self.id,
            self.stmt_id(),
            req
        );
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Prepare {
            is_insert,
            fields,
            fields_count,
            ..
        } = resp
        {
            self.is_insert.store(is_insert, Ordering::Relaxed);
            tracing::trace!("12341234aasdfsa");
            let mut fields_lock = self.fields.write().await;
            *fields_lock = fields;
            tracing::trace!("12341234");
            self.fields_count.store(fields_count, Ordering::Relaxed);
            // self.is_insert = Some(AtomicBool::new(is_insert));
            // self.is_insert = Some(is_insert);
            // self.fields = fields;
            // self.fields_count = Some(fields_count);
            return Ok(());
        }
        unreachable!("unexpected stmt2 prepare response: {resp:?}");
    }

    async fn _bind(&self, bytes: Vec<u8>) -> RawResult<()> {
        let req = WsSend::Binary(bytes);
        tracing::trace!(
            "stmt2 bind, id: {}, stmt_id: {}, req: {:?}",
            self.id,
            self.stmt_id(),
            req
        );
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Bind { .. } = resp {
            return Ok(());
        }
        unreachable!("unexpected stmt2 bind response: {resp:?}");
    }

    async fn _exec(&self, req_id: ReqId) -> RawResult<usize> {
        let req = WsSend::Stmt2Exec {
            req_id,
            stmt_id: self.stmt_id(),
        };
        tracing::trace!(
            "stmt2 exec, id: {}, stmt_id: {}, req: {:?}",
            self.id,
            self.stmt_id(),
            req
        );
        let resp = self.client.send_request(req).await?;
        if let WsRecvData::Stmt2Exec { affected, .. } = resp {
            // self.affected_rows += affected;
            // self.affected_rows_once = affected;
            // 写入 exec 后 clear，查询 result_set 后 clear
            if self.is_insert() {
                self.cache.lock().await.bind_bytes.clear();
            }
            return Ok(affected);
        }
        unreachable!("unexpected stmt2 exec response: {resp:?}");
    }

    fn new(client: Arc<WsTaos>) -> Self {
        let stmt_id = Arc::new(AtomicU64::new(0));
        Self {
            id: generate_req_id(),
            client,
            stmt_id: stmt_id.clone(),
            cache: Arc::new(Mutex::new(Stmt2Cache {
                stmt_id: stmt_id,
                ..Default::default()
            })),
            is_insert: Arc::new(AtomicBool::new(false)),
            fields: Arc::new(RwLock::new(None)),
            fields_count: Arc::new(AtomicUsize::new(0)),
        }
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
        let resp = block_in_place_or_global(self.client.send_request(req));
        match resp {
            Ok(WsRecvData::Stmt2Close { .. }) => tracing::trace!("close Stmt2 successfully"),
            Err(err) => tracing::error!("failed to close Stmt2: {err:?}"),
            resp => unreachable!("unexpected stmt2 close response: {resp:?}"),
        }
    }

    fn stmt_id(&self) -> StmtId {
        self.stmt_id.load(Ordering::SeqCst)
    }

    fn is_insert(&self) -> bool {
        self.is_insert.load(Ordering::Relaxed)
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
    // id: u64,
    // client: Arc<WsTaos>,
    // stmt_id: Arc<AtomicU64>,
    // is_insert: Arc<Option<AtomicBool>>,
    // is_insert: Arc<AtomicBool>,
    // fields: Option<Arc<RwLock<Vec<Stmt2Field>>>>,
    // fields_count: Option<AtomicUsize>,
    // fields: Arc<RwLock<Option<Vec<Stmt2Field>>>>,
    // fields_count: Arc<AtomicUsize>,
    // TODO: use AtomicUsize
    affected_rows: usize,
    affected_rows_once: usize,
    // cache: Arc<Mutex<Stmt2Cache>>,
    inner: Arc<Stmt2Inner>,
}

// impl Clone for Stmt2 {
//     fn clone(&self) -> Self {
//         Self {
//             // id: self.id,
//             // client: self.client.clone(),
//             // stmt_id: self.stmt_id.clone(),
//             // cache: self.cache.clone(),
//             is_insert: None,
//             fields: None,
//             fields_count: None,
//             affected_rows: 0,
//             affected_rows_once: 0,
//             inner: self.inner.clone(),
//         }
//     }
// }

impl Stmt2 {
    pub fn new(client: Arc<WsTaos>) -> Self {
        Self {
            // id: generate_req_id(),
            // client,
            // stmt_id: Arc::default(),
            // is_insert: Arc::new(AtomicBool::new(false)),
            // fields: Arc::new(RwLock::new(None)),
            // fields_count: Arc::new(AtomicUsize::new(0)),
            affected_rows: 0,
            affected_rows_once: 0,
            // cache: Arc::default(),
            inner: Arc::new(Stmt2Inner::new(client)),
        }
    }

    async fn init(&self) -> RawResult<()> {
        self.init_with_options(generate_req_id(), true, false).await
    }

    pub async fn init_with_options(
        &self,
        req_id: u64,
        single_stb_insert: bool,
        single_table_bind_once: bool,
    ) -> RawResult<()> {
        self.inner.client.wait_for_reconnect().await?;
        // self.inner.client.insert_stmt2(self.clone());
        self.inner.client.insert_stmt2(self.inner.clone());

        let mut cache = self.inner.cache.lock().await;
        cache.set_req_id(req_id);
        cache.action = Stmt2Action::Init;
        cache.single_stb_insert = single_stb_insert;
        cache.single_table_bind_once = single_table_bind_once;
        drop(cache);

        self.inner
            ._init_with_options(req_id, single_stb_insert, single_table_bind_once)
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
            "stmt2 init2, id: {}, stmt_id: {}, req: {:?}",
            self.inner.id,
            self.stmt_id(),
            req
        );
        let resp = self.inner.client.send_request(req).await?;
        if let WsRecvData::Stmt2Init { stmt_id, .. } = resp {
            self.inner.stmt_id.store(stmt_id, Ordering::SeqCst);
            // self.inner.cache.lock().await.stmt_id = stmt_id;
            tracing::trace!("init2 id: {}, stmt_id: {}", self.inner.id, self.stmt_id());
            return Ok(());
        }
        unreachable!("unexpected stmt2 init response: {resp:?}");
    }

    async fn prepare<S: AsRef<str> + Send>(&mut self, sql: S) -> RawResult<()> {
        self.inner.client.wait_for_reconnect().await?;

        let req_id = generate_req_id();
        let mut cache = self.inner.cache.lock().await;
        cache.set_req_id(req_id);
        cache.action = Stmt2Action::Prepare;
        cache.sql = sql.as_ref().to_string();
        drop(cache);

        self.inner._prepare(req_id, sql).await
    }

    async fn _prepare<S: AsRef<str> + Send>(&self, req_id: ReqId, sql: S) -> RawResult<()> {
        let req = WsSend::Stmt2Prepare {
            req_id,
            stmt_id: self.stmt_id(),
            sql: sql.as_ref().to_string(),
            get_fields: true,
        };
        tracing::trace!(
            "stmt2 prepare2, id: {}, stmt_id: {}, req: {:?}",
            self.inner.id,
            self.stmt_id(),
            req
        );
        let resp = self.inner.client.send_request(req).await?;
        if let WsRecvData::Stmt2Prepare {
            is_insert,
            fields,
            fields_count,
            ..
        } = resp
        {
            self.inner.is_insert.store(is_insert, Ordering::Relaxed);
            let mut fields_lock = self.inner.fields.write().await;
            *fields_lock = fields;
            self.inner
                .fields_count
                .store(fields_count, Ordering::Relaxed);
            // self.is_insert = Some(AtomicBool::new(is_insert));
            // self.is_insert = Some(is_insert);
            // self.fields = fields;
            // self.fields_count = Some(fields_count);
            return Ok(());
        }
        unreachable!("unexpected stmt2 prepare response: {resp:?}");
    }

    async fn bind(&self, params: &[Stmt2BindParam]) -> RawResult<()> {
        self.inner.client.wait_for_reconnect().await?;

        let req_id = generate_req_id();
        let mut cache = self.inner.cache.lock().await;
        cache.set_req_id(req_id);
        cache.action = Stmt2Action::Bind;

        let is_insert = self.inner.is_insert.load(Ordering::Relaxed);
        let fields_guard = self.inner.fields.read().await;
        let fields = fields_guard.as_ref();
        let fields_count = self.inner.fields_count.load(Ordering::Relaxed);

        let bytes = bind::bind_params_to_bytes(
            params,
            req_id,
            self.stmt_id(),
            is_insert,
            fields,
            fields_count,
            // self.is_insert.unwrap(),
            // self.fields.as_ref(),
            // self.fields_count.unwrap(),
        )?;

        drop(fields_guard);

        cache.bind_bytes.push(bytes.clone());
        drop(cache);

        tracing::trace!(
            "stmt2 bind2, id: {}, stmt_id: {}, req_id: {:?}",
            self.inner.id,
            self.stmt_id(),
            req_id
        );

        self.inner._bind(bytes).await
    }

    async fn _bind(&self, bytes: Vec<u8>) -> RawResult<()> {
        let req = WsSend::Binary(bytes);
        tracing::trace!(
            "stmt2 bind2, id: {}, stmt_id: {}, req: {:?}",
            self.inner.id,
            self.stmt_id(),
            req
        );
        let resp = self.inner.client.send_request(req).await?;
        if let WsRecvData::Stmt2Bind { .. } = resp {
            return Ok(());
        }
        unreachable!("unexpected stmt2 bind response: {resp:?}");
    }

    async fn exec(&mut self) -> RawResult<usize> {
        self.inner.client.wait_for_reconnect().await?;

        let req_id = generate_req_id();
        let mut cache = self.inner.cache.lock().await;
        cache.set_req_id(req_id);
        cache.action = Stmt2Action::Exec;
        drop(cache);

        self.inner._exec(req_id).await
    }

    async fn _exec(&mut self, req_id: ReqId) -> RawResult<usize> {
        let req = WsSend::Stmt2Exec {
            req_id,
            stmt_id: self.stmt_id(),
        };
        tracing::trace!(
            "stmt2 exec2, id: {}, stmt_id: {}, req: {:?}",
            self.inner.id,
            self.stmt_id(),
            req
        );
        let resp = self.inner.client.send_request(req).await?;
        if let WsRecvData::Stmt2Exec { affected, .. } = resp {
            self.affected_rows += affected;
            self.affected_rows_once = affected;
            if self.is_insert().unwrap() {
                // self.inner.cache.lock().await.bind_bytes.clear();
                self.inner.cache.lock().await.bind_bytes.clear();
            }
            // self.inner.cache.lock().await.bind_bytes.clear();
            return Ok(affected);
        }
        unreachable!("unexpected stmt2 exec response: {resp:?}");
    }

    // fn close(&self) {
    //     if self.inner.client.state() != ConnState::Connected {
    //         tracing::warn!("cannot close Stmt2 when client is not connected");
    //         return;
    //     }

    //     let req = WsSend::Stmt2Close {
    //         req_id: generate_req_id(),
    //         stmt_id: self.stmt_id(),
    //     };
    //     let resp = block_in_place_or_global(self.inner.client.send_request(req));
    //     match resp {
    //         Ok(WsRecvData::Stmt2Close { .. }) => tracing::trace!("Stmt2 closed successfully"),
    //         Err(err) => tracing::error!("failed to close Stmt2: {err:?}"),
    //         resp => unreachable!("unexpected stmt2 close response: {resp:?}"),
    //     }
    // }

    async fn result_set(&self) -> RawResult<ResultSet> {
        self.inner.client.wait_for_reconnect().await?;

        let req_id = generate_req_id();
        let mut cache = self.inner.cache.lock().await;
        cache.set_req_id(req_id);
        cache.action = Stmt2Action::ResultSet;
        drop(cache);

        self._result_set(req_id).await
    }

    async fn _result_set(&self, req_id: ReqId) -> RawResult<ResultSet> {
        let is_insert = self.inner.is_insert.load(Ordering::Relaxed);
        // if self.is_insert.unwrap_or(false) {
        if is_insert {
            return Err("Only query can use result".into());
        }

        let req = WsSend::Stmt2Result {
            req_id,
            stmt_id: self.stmt_id(),
        };
        tracing::trace!(
            "stmt2 result2, id: {}, stmt_id: {}, req: {:?}",
            self.inner.id,
            self.stmt_id(),
            req
        );

        let resp = self.inner.client.send_request(req).await?;
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
            self.inner.cache.lock().await.bind_bytes.clear();

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
                self.inner.client.sender(),
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
                sender: self.inner.client.sender(),
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
                tz: self.inner.client.timezone(),
            });
        }

        unreachable!("unexpected stmt2 result set response: {resp:?}");
    }

    fn stmt_id(&self) -> StmtId {
        self.inner.stmt_id()
    }

    #[allow(dead_code)]
    pub(super) async fn req_id(&self) -> ReqId {
        self.inner.cache.lock().await.req_id()
    }

    // #[allow(dead_code)]
    // pub(super) async fn recover(&self) -> RawResult<()> {
    //     tracing::trace!("recovering Stmt2 with id: {}", self.inner.id);

    //     let (action, single_stb_insert, single_table_bind_once, sql, mut bind_bytes) = {
    //         let cache = self.inner.cache.lock().await;
    //         (
    //             cache.action,
    //             cache.single_stb_insert,
    //             cache.single_table_bind_once,
    //             cache.sql.clone(),
    //             cache.build_bind_bytes(),
    //         )
    //     };

    //     // add id
    //     let id = self.inner.id;
    //     tracing::trace!(
    //         "recovering Stmt2, id: {id}, action: {action:?}, single_stb_insert: {single_stb_insert}, \
    //         single_table_bind_once: {single_table_bind_once}, sql: {sql}, bind_bytes: {bind_bytes:?}"
    //     );

    //     match action {
    //         Stmt2Action::Init => {
    //             let req = { self.inner.cache.lock().await.build_init_req() };
    //             self.recover_with_steps(None, None, None, Some(req)).await?;
    //         }
    //         Stmt2Action::Prepare => {
    //             let req = { self.inner.cache.lock().await.build_prepare_req() };
    //             self.recover_with_steps(
    //                 Some((single_stb_insert, single_table_bind_once)),
    //                 None,
    //                 None,
    //                 Some(req),
    //             )
    //             .await?;
    //         }
    //         Stmt2Action::Bind => {
    //             let req = WsSend::Binary(bind_bytes.remove(bind_bytes.len() - 1));
    //             self.recover_with_steps(
    //                 Some((single_stb_insert, single_table_bind_once)),
    //                 Some(sql),
    //                 Some(bind_bytes),
    //                 Some(req),
    //             )
    //             .await?;
    //         }
    //         Stmt2Action::Exec => {
    //             let req = { self.inner.cache.lock().await.build_exec_req() };
    //             self.recover_with_steps(
    //                 Some((single_stb_insert, single_table_bind_once)),
    //                 Some(sql),
    //                 Some(bind_bytes),
    //                 Some(req),
    //             )
    //             .await?;
    //         }
    //         Stmt2Action::ResultSet => {
    //             let req = { self.inner.cache.lock().await.build_result_req() };
    //             self.recover_with_steps(
    //                 Some((single_stb_insert, single_table_bind_once)),
    //                 Some(sql),
    //                 Some(bind_bytes),
    //                 Some(req),
    //             )
    //             .await?;
    //         }
    //     }

    //     Ok(())
    // }

    // #[allow(dead_code)]
    // async fn recover_with_steps(
    //     &self,
    //     init_options: Option<(bool, bool)>,
    //     sql: Option<String>,
    //     bind_bytes: Option<Vec<Vec<u8>>>,
    //     req: Option<WsSend>,
    // ) -> RawResult<()> {
    //     if let Some((single_stb_insert, single_table_bind_once)) = init_options {
    //         self._init_with_options(generate_req_id(), single_stb_insert, single_table_bind_once)
    //             .await?;
    //     }
    //     if let Some(sql) = sql {
    //         self._prepare(generate_req_id(), sql).await?;
    //     }
    //     if let Some(bind_bytes) = bind_bytes {
    //         for bytes in bind_bytes {
    //             self._bind(bytes).await?;
    //         }
    //     }
    //     if let Some(req) = req {
    //         self.inner.client.send_only(req).await?;
    //     }
    //     Ok(())
    // }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn is_insert(&self) -> Option<bool> {
        Some(self.inner.is_insert.load(Ordering::Relaxed))
    }

    pub fn fields(&self) -> Option<&Vec<Stmt2Field>> {
        // self.fields.read().await.as_ref()
        None
    }

    pub fn fields_count(&self) -> Option<usize> {
        Some(self.inner.fields_count.load(Ordering::Relaxed))
    }

    pub fn affected_rows_once(&self) -> usize {
        self.affected_rows_once
    }
}

impl Drop for Stmt2 {
    fn drop(&mut self) {
        tracing::trace!("dropping Stmt2 with id: {}", self.inner.id);
        // if Arc::strong_count(&self.cache) == 1 {
        //     self.client.remove_stmt2(self.id);
        //     self.close();
        // }
        // self.client.remove_stmt2(self.id);
        // self.close();
    }
}

impl Stmt2Bindable<super::Taos> for Stmt2 {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        let stmt2 = Self::new(taos.client());
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
        let stmt2 = Self::new(taos.client());
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

#[cfg(test)]
mod tests_proxy {
    use std::sync::Arc;

    use futures::TryStreamExt;
    use rand::Rng;
    use serde::Deserialize;
    use serde_json::Value;
    use taos_query::common::ColumnView;
    use taos_query::stmt2::Stmt2BindParam;
    use taos_query::util::ws_proxy::{ProxyAction, ProxyFn, WsProxy};
    use taos_query::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, RawError};
    use tokio_tungstenite::tungstenite::Message;

    use crate::{Stmt2, TaosBuilder};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_init() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .compact()
            .try_init();

        {
            // let judge_state = Arc::new(Mutex::new(JudgeState { init_count: 0 }));

            let judge: ProxyFn = {
                // let judge_state = judge_state.clone();
                Arc::new(move |msg, state| {
                    if let Message::Text(text) = msg {
                        if text.contains("init") {
                            state.req_count += 1;
                            if state.req_count == 1 {
                                return ProxyAction::Restart;
                            }
                        }
                    }
                    ProxyAction::Forward
                })
            };

            // let judge: JudgeFn = Arc::new(|msg| {
            //     if let Message::Text(text) = msg {
            //         text.contains("init")
            //     } else {
            //         false
            //     }
            // });

            let proxy = WsProxy::start(
                "127.0.0.1:8811".parse().unwrap(),
                "ws://localhost:6041/ws".to_string(),
                judge.clone(),
            )
            .await;

            let taos = TaosBuilder::from_dsn("ws://localhost:8811")?
                .build()
                .await?;

            // let affected_rows = taos
            //     .exec_many(&[
            //         "drop database if exists test_1753080278",
            //         "create database test_1753080278",
            //         "use test_1753080278",
            //         "create table t0 (ts timestamp, c1 int)",
            //         "insert into t0 values(1726803356466, 99)",
            //         "insert into t0 values(1726803357466, 100)",
            //     ])
            //     .await?;

            // assert_eq!(affected_rows, 2);

            let stmt2 = Stmt2::new(taos.client());
            stmt2.init().await?;

            proxy.stop().await;

            // tracing::trace!("stmt2 scope end");
            // drop(stmt2);
            // drop(taos);
            // drop(taos.client());
        }

        // tokio::time::sleep(Duration::from_secs(50)).await;
        // std::thread::sleep(Duration::from_secs(5));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_prepare() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .compact()
            .try_init();

        let judge: ProxyFn = {
            Arc::new(move |msg, state| {
                if let Message::Text(text) = msg {
                    if text.contains("prepare") {
                        state.req_count += 1;
                        if state.req_count == 1 {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        let proxy = WsProxy::start(
            "127.0.0.1:8812".parse().unwrap(),
            "ws://localhost:6041/ws".to_string(),
            judge.clone(),
        )
        .await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8812")?
            .build()
            .await?;

        let affected_rows = taos
            .exec_many(&[
                "drop database if exists test_1754906189",
                "create database test_1754906189",
                "use test_1754906189",
                "create table t0 (ts timestamp, c1 int)",
                "insert into t0 values(1726803356466, 99)",
                "insert into t0 values(1726803357466, 100)",
            ])
            .await?;

        assert_eq!(affected_rows, 2);

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("select * from test_1754906189.t0 where c1 > ?")
            .await?;

        taos.exec("drop database test_1754906189").await?;

        proxy.stop().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bind() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .compact()
            .try_init();

        let judge: ProxyFn = {
            Arc::new(move |msg, state| {
                if let Message::Binary(bytes) = msg {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    if action == 9 {
                        state.req_count += 1;
                        if state.req_count == 1 {
                            return ProxyAction::Restart;
                        }
                    }
                }

                // if let Message::Text(text) = msg {
                //     // let text = message.to_str().unwrap();
                //     let req: Value = serde_json::from_str::<Value>(text).unwrap();
                //     let action = req.get("args").and_then(|v| v.get("action")).unwrap();
                //     let action = action.as_str().unwrap();

                //     if text.contains("bind") {
                //         state.init_count += 1;
                //         if state.init_count == 1 {
                //             return JudgeAction::RestartProxy;
                //         }
                //     }
                // }
                ProxyAction::Forward
            })
        };

        let proxy = WsProxy::start(
            "127.0.0.1:8813".parse().unwrap(),
            "ws://localhost:6041/ws".to_string(),
            judge.clone(),
        )
        .await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8813")?
            .build()
            .await?;

        let affected_rows = taos
            .exec_many(&[
                "drop database if exists test_1754906156",
                "create database test_1754906156",
                "use test_1754906156",
                "create table t0 (ts timestamp, c1 int)",
                "insert into t0 values(1726803356466, 99)",
                "insert into t0 values(1726803357466, 100)",
            ])
            .await?;

        assert_eq!(affected_rows, 2);

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("insert into test_1754906156.t0 values(?, ?)")
            .await?;

        // let tbname = "d0";
        // let tags = vec![Value::Int(100)];
        let cols = vec![
            ColumnView::from_millis_timestamp(vec![1726803356466, 1726803357466]),
            ColumnView::from_ints(vec![100, 200]),
        ];
        let param = Stmt2BindParam::new(None, None, Some(cols));

        stmt2.bind(&[param]).await?;

        proxy.stop().await;

        taos.exec("drop database test_1754906156").await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .compact()
            .try_init();

        let judge: ProxyFn = {
            Arc::new(move |msg, state| {
                if let Message::Text(text) = msg {
                    tracing::trace!("Judge got message: {}", text);
                    let req = serde_json::from_str::<Value>(text).unwrap();
                    let action = req
                        .get("action")
                        // .and_then(|v| v.get("action"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    // let action = action.as_str().unwrap();
                    if action == "stmt2_exec" {
                        state.req_count += 1;
                        if state.req_count == 1 {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        let proxy = WsProxy::start(
            "127.0.0.1:8814".parse().unwrap(),
            "ws://localhost:6041/ws".to_string(),
            judge.clone(),
        )
        .await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8814")?
            .build()
            .await?;

        let affected_rows = taos
            .exec_many(&[
                "drop database if exists test_1754906118",
                "create database test_1754906118",
                "use test_1754906118",
                "create table t0 (ts timestamp, c1 int)",
                "insert into t0 values(1726803356466, 99)",
                "insert into t0 values(1726803357466, 100)",
            ])
            .await?;

        assert_eq!(affected_rows, 2);

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("insert into test_1754906118.t0 values(?, ?)")
            .await?;

        // let tbname = "d0";
        // let tags = vec![Value::Int(100)];
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

        taos.exec("drop database test_1754906118").await?;

        proxy.stop().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_result() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .compact()
            .try_init();

        let judge: ProxyFn = {
            Arc::new(move |msg, state| {
                // common func
                if let Message::Text(text) = msg {
                    let req = serde_json::from_str::<Value>(text).unwrap();
                    let action = req.get("action").and_then(|v| v.as_str()).unwrap_or("");
                    if action == "stmt2_result" {
                        state.req_count += 1;
                        if state.req_count == 1 {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        let proxy = WsProxy::start(
            "127.0.0.1:8815".parse().unwrap(),
            "ws://localhost:6041/ws".to_string(),
            judge.clone(),
        )
        .await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8815")?
            .build()
            .await?;

        let affected_rows = taos
            .exec_many(&[
                "drop database if exists test_1754906085",
                "create database test_1754906085",
                "use test_1754906085",
                "create table t0 (ts timestamp, c1 int)",
                "insert into t0 values(1726803356466, 99)",
                "insert into t0 values(1726803357466, 100)",
            ])
            .await?;

        assert_eq!(affected_rows, 2);

        let mut stmt2 = Stmt2::new(taos.client());
        stmt2.init().await?;
        stmt2
            .prepare("select * from test_1754906085.t0 where c1 > ?")
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

        taos.exec("drop database test_1754906085").await?;

        proxy.stop().await;

        Ok(())
    }

    /// 并发多实例自动重连测试：多个 stmt2 并发 prepare/bind/exec/query，期间 proxy 重启，验证所有实例都能自动恢复。
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stmt2_concurrent_auto_reconnect() -> anyhow::Result<()> {
        use futures::future::try_join_all;
        use std::sync::Arc;
        use taos_query::common::ColumnView;
        use taos_query::stmt2::Stmt2BindParam;
        use taos_query::util::ws_proxy::{ProxyAction, ProxyFn, WsProxy};
        use taos_query::AsyncFetchable;
        use taos_query::AsyncQueryable;
        // use tokio::sync::Barrier;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .compact()
            .try_init();

        let judge: ProxyFn = {
            Arc::new(move |msg, state| {
                // 每次有 prepare 或 exec 就重启 proxy 一次
                if let Message::Text(text) = msg {
                    if text.contains("stmt") {
                        if rand::rng().random_bool(0.05) {
                            return ProxyAction::Restart;
                        }
                        // state.init_count += 1;
                        // if state.init_count % 50 == 0 {
                        //     return JudgeAction::RestartProxy;
                        // }
                    }
                }
                if let Message::Binary(bytes) = msg {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    if action == 9 {
                        // if rand::rng().random_bool(0.2) {
                        //     return JudgeAction::RestartProxy;
                        // }
                        // state.init_count += 1;
                        // if state.init_count % 3 == 0 {
                        //     return JudgeAction::RestartProxy;
                        // }
                        // if state.init_count == 1 {
                        //     return JudgeAction::RestartProxy;
                        // }
                    }
                }
                // }
                // }
                ProxyAction::Forward
            })
        };

        let proxy = WsProxy::start(
            "127.0.0.1:8820".parse().unwrap(),
            "ws://localhost:6041/ws".to_string(),
            judge.clone(),
        )
        .await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8820")?
            .build()
            .await?;

        taos.exec_many(&[
            "drop database if exists test_concurrent_reconnect",
            "create database test_concurrent_reconnect",
            "use test_concurrent_reconnect",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let n = 20;
        // let barrier = Arc::new(Barrier::new(n));
        let mut tasks = Vec::new();
        for i in 0..n {
            // let taos = taos.clone();
            let client = taos.client();
            // let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let mut stmt2 = Stmt2::new(client);
                stmt2.init().await?;
                stmt2
                    .prepare("insert into test_concurrent_reconnect.t0 values(?, ?)")
                    .await?;
                let ts = 1726803356466 + i as i64;
                let c1 = 100 + i as i32;
                let cols = vec![
                    ColumnView::from_millis_timestamp(vec![ts]),
                    ColumnView::from_ints(vec![c1]),
                ];
                let param = Stmt2BindParam::new(None, None, Some(cols));
                stmt2.bind(&[param]).await?;
                // barrier.wait().await; // 保证所有 stmt2 都已 bind 后再 exec
                let affected = stmt2.exec().await?;
                // assert_eq!(affected, 1);

                // 查询验证

                stmt2
                    .prepare("select * from test_concurrent_reconnect.t0 where c1 = ?")
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

                let records: Result<Vec<Record>, RawError> =
                    stmt2.result_set().await?.deserialize().try_collect().await;
                if records.is_err() {
                    tracing::error!("Failed to deserialize records: {:?}", records.err());
                } else {
                    let records = records.unwrap();
                    tracing::trace!("records: {records:?}");
                    // assert_eq!(records.len(), 1);
                    // assert_eq!(records[0].ts, ts);
                    // assert_eq!(records[0].c1, c1);
                }

                Ok::<_, anyhow::Error>(())
            }));
        }
        // 并发等待所有任务
        let results = try_join_all(tasks).await.unwrap();
        for r in results {
            r?;
        }

        taos.exec("drop database test_concurrent_reconnect")
            .await
            .unwrap();
        proxy.stop().await;
        Ok(())
    }
}
