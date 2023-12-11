use futures::{SinkExt, StreamExt};
use itertools::Itertools;
// use scc::HashMap;
use dashmap::DashMap as HashMap;

use serde::Deserialize;

use taos_query::common::views::views_to_raw_block;
use taos_query::common::{ColumnView, Precision, Ty};
use taos_query::prelude::{InlinableWrite, RawResult};
use taos_query::stmt::{AsyncBindable, Bindable};
use taos_query::{block_in_place_or_global, IntoDsn, RawBlock};

use taos_query::prelude::tokio;
use tokio::sync::{oneshot, watch};

use tokio_tungstenite::tungstenite::protocol::Message;

use crate::query::infra::ToMessage;
use crate::{Taos, TaosBuilder};
use messages::*;

use std::fmt::Debug;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

mod messages;

use crate::query::asyn::Error;
type StmtResult = RawResult<Option<usize>>;
type StmtSender = tokio::sync::mpsc::Sender<StmtResult>;
type StmtReceiver = tokio::sync::mpsc::Receiver<StmtResult>;

type StmtFieldResult = RawResult<Vec<StmtField>>;
type StmtFieldSender = tokio::sync::mpsc::Sender<StmtFieldResult>;
type StmtFieldReceiver = tokio::sync::mpsc::Receiver<StmtFieldResult>;

type StmtParamResult = RawResult<StmtParam>;
type StmtParamSender = tokio::sync::mpsc::Sender<StmtParamResult>;
type StmtParamReceiver = tokio::sync::mpsc::Receiver<StmtParamResult>;

type StmtUseResultResult = RawResult<StmtUseResult>;
type StmtUseSender = tokio::sync::mpsc::Sender<StmtUseResultResult>;
type StmtUseReceiver = tokio::sync::mpsc::Receiver<StmtUseResultResult>;

type WsSender = tokio::sync::mpsc::Sender<Message>;

trait ToJsonValue {
    fn to_json_value(&self) -> serde_json::Value;
}

impl ToJsonValue for ColumnView {
    fn to_json_value(&self) -> serde_json::Value {
        match self {
            ColumnView::Bool(view) => serde_json::json!(view.to_vec()),
            ColumnView::TinyInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::SmallInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::Int(view) => serde_json::json!(view.to_vec()),
            ColumnView::BigInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::Float(view) => serde_json::json!(view.to_vec()),
            ColumnView::Double(view) => serde_json::json!(view.to_vec()),
            ColumnView::VarChar(view) => serde_json::json!(view.to_vec()),
            ColumnView::Timestamp(view) => serde_json::json!(view
                .iter()
                .map(|ts| ts.map(|ts| ts.to_datetime_with_tz()))
                .collect_vec()),
            ColumnView::NChar(view) => serde_json::json!(view.to_vec()),
            ColumnView::UTinyInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::USmallInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::UInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::UBigInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::Json(view) => serde_json::json!(view.to_vec()),
        }
    }
}

impl Bindable<super::Taos> for Stmt {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        let mut dsn = taos.dsn.clone();
        let database: Option<String> = block_in_place_or_global(
            <Taos as taos_query::AsyncQueryable>::query_one(taos, "select database()"),
        )?;
        dsn.database = database;
        let mut stmt = block_in_place_or_global(Self::from_wsinfo(&dsn))?;
        crate::block_in_place_or_global(stmt.stmt_init())?;
        Ok(stmt)
    }

    fn init_with_req_id(taos: &super::Taos, req_id: u64) -> RawResult<Self> {
        let mut dsn = taos.dsn.clone();
        let database: Option<String> = block_in_place_or_global(
            <Taos as taos_query::AsyncQueryable>::query_one(taos, "select database()"),
        )?;
        dsn.database = database;
        let mut stmt = block_in_place_or_global(Self::from_wsinfo(&dsn))?;
        crate::block_in_place_or_global(stmt.taos_stmt_init_with_req_id(req_id))?;
        Ok(stmt)
    }

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self> {
        crate::block_in_place_or_global(self.stmt_prepare(sql.as_ref()))?;
        Ok(self)
    }

    fn set_tbname<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self> {
        block_in_place_or_global(self.stmt_set_tbname(sql.as_ref()))?;
        Ok(self)
    }

    fn set_tags(&mut self, tags: &[taos_query::common::Value]) -> RawResult<&mut Self> {
        let tags = tags.iter().map(|tag| tag.to_json_value()).collect_vec();
        block_in_place_or_global(self.stmt_set_tags(tags))?;
        Ok(self)
    }

    fn bind(&mut self, params: &[taos_query::common::ColumnView]) -> RawResult<&mut Self> {
        // This json method for bind

        // let columns = params
        //     .into_iter()
        //     .map(|tag| tag.to_json_value())
        //     .collect_vec();
        // crate::block_in_place_or_global(self.stmt_bind(columns))?;

        crate::block_in_place_or_global(self.stmt_bind_block(params))?;
        Ok(self)
    }

    fn add_batch(&mut self) -> RawResult<&mut Self> {
        crate::block_in_place_or_global(self.stmt_add_batch())?;
        Ok(self)
    }

    fn execute(&mut self) -> RawResult<usize> {
        block_in_place_or_global(self.stmt_exec())
    }

    fn affected_rows(&self) -> usize {
        self.affected_rows
    }
}

#[async_trait::async_trait]
impl AsyncBindable<super::Taos> for Stmt {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        let mut dsn = taos.dsn.clone();
        let database: Option<String> =
            <Taos as taos_query::AsyncQueryable>::query_one(taos, "select database()").await?;
        dsn.database = database;
        let mut stmt = Self::from_wsinfo(&dsn).await?;
        stmt.stmt_init().await?;
        Ok(stmt)
    }

    async fn init_with_req_id(taos: &super::Taos, req_id: u64) -> RawResult<Self> {
        let mut dsn = taos.dsn.clone();
        let database: Option<String> =
            <Taos as taos_query::AsyncQueryable>::query_one(taos, "select database()").await?;
        dsn.database = database;
        let mut stmt = Self::from_wsinfo(&dsn).await?;
        stmt.taos_stmt_init_with_req_id(req_id).await?;
        Ok(stmt)
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.stmt_prepare(sql.as_ref()).await?;
        Ok(self)
    }

    async fn set_tbname(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.stmt_set_tbname(sql.as_ref()).await?;
        Ok(self)
    }

    async fn set_tags(&mut self, tags: &[taos_query::common::Value]) -> RawResult<&mut Self> {
        let tags = tags.iter().map(|tag| tag.to_json_value()).collect_vec();
        self.stmt_set_tags(tags).await?;
        Ok(self)
    }

    async fn bind(&mut self, params: &[taos_query::common::ColumnView]) -> RawResult<&mut Self> {
        // This json method for bind

        // let columns = params
        //     .into_iter()
        //     .map(|tag| tag.to_json_value())
        //     .collect_vec();
        // crate::block_in_place_or_global(self.stmt_bind(columns))?;

        self.stmt_bind_block(params).await?;
        Ok(self)
    }

    async fn add_batch(&mut self) -> RawResult<&mut Self> {
        self.stmt_add_batch().await?;
        Ok(self)
    }

    async fn execute(&mut self) -> RawResult<usize> {
        self.stmt_exec().await
    }

    async fn affected_rows(&self) -> usize {
        self.affected_rows
    }
}

pub trait WsFieldsable {
    fn get_tag_fields(&mut self) -> RawResult<Vec<StmtField>>;

    fn get_col_fields(&mut self) -> RawResult<Vec<StmtField>>;
}

impl WsFieldsable for Stmt {
    fn get_tag_fields(&mut self) -> RawResult<Vec<StmtField>> {
        block_in_place_or_global(self.stmt_get_tag_fields())
    }

    fn get_col_fields(&mut self) -> RawResult<Vec<StmtField>> {
        block_in_place_or_global(self.stmt_get_col_fields())
    }
}

pub struct Stmt {
    req_id: Arc<AtomicU64>,
    timeout: Duration,
    ws: WsSender,
    close_signal: watch::Sender<bool>,
    queries: Arc<HashMap<ReqId, oneshot::Sender<RawResult<StmtId>>>>,
    fetches: Arc<HashMap<StmtId, StmtSender>>,
    receiver: Option<StmtReceiver>,
    args: Option<StmtArgs>,
    affected_rows: usize,
    affected_rows_once: usize,
    fields_fetches: Arc<HashMap<StmtId, StmtFieldSender>>,
    fields_receiver: Option<StmtFieldReceiver>,
    param_fetches: Arc<HashMap<StmtId, StmtParamSender>>,
    param_receiver: Option<StmtParamReceiver>,
    use_result_fetches: Arc<HashMap<StmtId, StmtUseSender>>,
    use_result_receiver: Option<StmtUseReceiver>,
}

#[repr(C)]
#[derive(Debug, Deserialize, Clone)]
pub struct StmtParam {
    pub index: i64,
    pub data_type: Ty,
    pub length: i64,
}

#[repr(C)]
#[derive(Debug, Deserialize, Clone)]
pub struct StmtUseResult {
    pub result_id: u64,
    pub fields_count: i64,
    pub fields_names: Option<Vec<String>>,
    pub fields_types: Option<Vec<Ty>>,
    pub fields_lengths: Option<Vec<u32>>,
    pub precision: Precision,
}

#[repr(C)]
#[derive(Debug, Deserialize, Clone)]
pub struct StmtField {
    pub name: String,
    pub field_type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
}
impl Debug for Stmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsClient")
            .field("req_id", &self.req_id)
            .field("...", &"...")
            .finish()
    }
}
impl Drop for Stmt {
    fn drop(&mut self) {
        // send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

impl Stmt {
    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {
        let ws = info.build_stream(info.to_stmt_url()).await?;

        let req_id = 0;
        let (mut sender, mut reader) = ws.split();

        let login = StmtSend::Conn {
            req_id,
            req: info.to_conn_request(),
        };
        sender.send(login.to_msg()).await.map_err(Error::from)?;
        if let Some(Ok(message)) = reader.next().await {
            match message {
                Message::Text(text) => {
                    let v: StmtRecv = serde_json::from_str(&text).unwrap();
                    match v.data {
                        StmtRecvData::Conn => (),
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        }

        let queries = Arc::new(HashMap::<ReqId, tokio::sync::oneshot::Sender<_>>::new());
        let fetches = Arc::new(HashMap::<StmtId, StmtSender>::new());

        let queries_sender = queries.clone();
        let fetches_sender = fetches.clone();

        let fields_fetches = Arc::new(HashMap::<StmtId, StmtFieldSender>::new());
        let fields_fetches_sender = fields_fetches.clone();

        let param_fetches = Arc::new(HashMap::<StmtId, StmtParamSender>::new());
        let param_fetches_sender = param_fetches.clone();

        let use_result_fetches = Arc::new(HashMap::<StmtId, StmtUseSender>::new());
        let use_result_fetches_sender = use_result_fetches.clone();

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel(100);
        let ws2 = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let mut close_listener = rx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(msg) = msg_recv.recv() => {
                        if let Err(err) = sender.send(msg).await {
                            //
                            log::warn!("Sender error: {err:#}");
                            break;
                        }
                    }
                    _ = rx.changed() => {
                        log::trace!("close sender task");
                        break;
                    }
                }
            }
        });

        // message handler for query/fetch/fetch_block
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(message) = reader.next() => {
                        match message {
                            Ok(message) => match message {
                                Message::Text(text) => {
                                    // FIXME: change to trace before release
                                    log::debug!("json response: {}", text);
                                    let v: StmtRecv = serde_json::from_str(&text).unwrap();
                                    match v.ok() {
                                        StmtOk::Conn(_) => {
                                            log::warn!("[{req_id}] received connected response in message loop");
                                        },
                                        StmtOk::Init(req_id, stmt_id) => {
                                            log::trace!("stmt init done: {{ req_id: {}, stmt_id: {:?}}}", req_id, stmt_id);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                sender.send(stmt_id).unwrap();
                                            }  else {
                                                log::trace!("Stmt init failed because req id {req_id} not exist");
                                            }
                                        }
                                        StmtOk::Stmt(stmt_id, res) => {
                                            if let Some(sender) = fetches_sender.get(&stmt_id) {
                                                log::trace!("send data to fetches with id {}", stmt_id);
                                                // let res = res.clone();
                                                sender.send(res).await.unwrap();
                                            // }) {

                                            } else {
                                                log::trace!("Got unknown stmt id: {stmt_id} with result: {res:?}");
                                            }
                                        }
                                        StmtOk::StmtFields(stmt_id, res) => {
                                            if let Some(sender) = fields_fetches_sender.get(&stmt_id) {
                                                log::trace!("send data to fetches with id {}", stmt_id);
                                                // let res = res.clone();
                                                sender.send(res).await.unwrap();

                                            } else {
                                                log::trace!("Got unknown stmt id: {stmt_id} with result: {res:?}");
                                            }
                                        }
                                        StmtOk::StmtParam(stmt_id, res) => {
                                            if let Some(sender) = param_fetches_sender.get(&stmt_id) {
                                                log::trace!("send data to fetches with id {}", stmt_id);
                                                sender.send(res).await.unwrap();
                                            } else {
                                                log::trace!("Got unknown stmt id: {stmt_id} with result: {res:?}");
                                            }
                                        }
                                        StmtOk::StmtUseResult(stmt_id, res) => {
                                            if let Some(sender) = use_result_fetches_sender.get(&stmt_id) {
                                                log::trace!("send data to fetches with id {}", stmt_id);
                                                sender.send(res).await.unwrap();
                                            } else {
                                                log::trace!("Got unknown stmt id: {stmt_id} with result: {res:?}");
                                            }
                                        }
                                    }
                                }
                                Message::Binary(_) => {
                                    log::warn!("received (unexpected) binary message, do nothing");
                                }
                                Message::Close(_) => {
                                    log::warn!("websocket connection is closed (unexpected?)");
                                    break;
                                }
                                Message::Ping(bytes) => {
                                    ws2.send(Message::Pong(bytes)).await.unwrap();
                                }
                                Message::Pong(_) => {
                                    // do nothing
                                    log::warn!("received (unexpected) pong message, do nothing");
                                }
                                Message::Frame(frame) => {
                                    // do nothing
                                    log::warn!("received (unexpected) frame message, do nothing");
                                    log::trace!("* frame data: {frame:?}");
                                }
                            },
                            Err(err) => {
                                log::trace!("receiving cause error: {err:?}");
                                break;
                            }
                        }
                    }
                    _ = close_listener.changed() => {
                        log::trace!("close reader task");
                        break
                    }
                }
            }
        });

        Ok(Self {
            req_id: Arc::new(AtomicU64::new(req_id + 1)),
            queries,
            timeout: Duration::from_secs(5),
            fetches,
            ws,
            close_signal: tx,
            receiver: None,
            args: None,
            affected_rows: 0,
            affected_rows_once: 0,
            fields_fetches,
            fields_receiver: None,
            param_fetches,
            param_receiver: None,
            use_result_fetches,
            use_result_receiver: None,
        })
    }
    /// Build TDengine websocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041/
    /// ```
    ///
    pub async fn from_dsn(dsn: impl IntoDsn) -> RawResult<Self> {
        let info = TaosBuilder::from_dsn(dsn)?;
        Self::from_wsinfo(&info).await
    }

    fn req_id(&self) -> u64 {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn stmt_init(&mut self) -> RawResult<&mut Self> {
        let req_id = self.req_id();
        self.taos_stmt_init_with_req_id(req_id).await
    }

    pub async fn taos_stmt_init_with_req_id(&mut self, req_id: u64) -> RawResult<&mut Self> {
        let action = StmtSend::Init { req_id };
        let (tx, rx) = oneshot::channel();
        {
            self.queries.insert(req_id, tx);
            self.ws.send(action.to_msg()).await.map_err(Error::from)?;
        }
        let stmt_id = rx.await.map_err(Error::from)??; // 1. RecvError, 2. TaosError
        let args = StmtArgs { req_id, stmt_id };

        let (sender, receiver) = tokio::sync::mpsc::channel(2);

        let _ = self.fetches.insert(stmt_id, sender);

        self.args = Some(args);
        self.receiver = Some(receiver);

        let (fields_sender, fields_receiver) = tokio::sync::mpsc::channel(2);
        let _ = self.fields_fetches.insert(stmt_id, fields_sender);
        self.fields_receiver = Some(fields_receiver);

        let (param_sender, param_receiver) = tokio::sync::mpsc::channel(2);
        let _ = self.param_fetches.insert(stmt_id, param_sender);
        self.param_receiver = Some(param_receiver);

        let (use_result_sender, use_result_receiver) = tokio::sync::mpsc::channel(2);
        let _ = self.use_result_fetches.insert(stmt_id, use_result_sender);
        self.use_result_receiver = Some(use_result_receiver);

        Ok(self)
    }

    pub async fn s_stmt<'a>(&'a mut self, sql: &'a str) -> RawResult<&mut Self> {
        let stmt = self.stmt_init().await?;
        stmt.stmt_prepare(sql).await?;
        Ok(self)
    }

    pub fn set_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = timeout;
        self
    }
    pub async fn stmt_prepare(&mut self, sql: &str) -> RawResult<()> {
        let prepare = StmtSend::Prepare {
            args: self.args.unwrap(),
            sql: sql.to_string(),
        };
        self.ws.send(prepare.to_msg()).await.map_err(Error::from)?;
        let _ = self.receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt prepare response"),
        )??;
        Ok(())
    }
    pub async fn stmt_add_batch(&mut self) -> RawResult<()> {
        log::trace!("add batch");
        let message = StmtSend::AddBatch(self.args.unwrap());
        self.ws.send(message.to_msg()).await.map_err(Error::from)?;
        let _ = self.receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt add batch response"),
        )??;
        Ok(())
    }
    pub async fn stmt_bind(&mut self, columns: Vec<serde_json::Value>) -> RawResult<()> {
        let message = StmtSend::Bind {
            args: self.args.unwrap(),
            columns,
        };
        {
            log::trace!("bind with: {message:?}");
            log::trace!("bind string: {}", message.to_msg());
            self.ws.send(message.to_msg()).await.map_err(Error::from)?;
        }
        log::trace!("begin receive");
        let _ = self.receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt bind response"),
        )??;
        Ok(())
    }

    async fn stmt_bind_block(&mut self, columns: &[ColumnView]) -> RawResult<()> {
        let args = self.args.unwrap();

        let mut bytes = Vec::new();
        // p0 uin64  req_id
        // p0+8 uint64 stmt_id
        // p0+16 uint64 (1 (set tag) 2 (bind))
        // p0+24 raw block
        bytes.write_u64_le(args.req_id).map_err(Error::from)?;
        bytes.write_u64_le(args.stmt_id).map_err(Error::from)?;
        bytes.write_u64_le(2).map_err(Error::from)?; // bind: 2

        let block = views_to_raw_block(columns);

        bytes.extend(&block);
        log::trace!("block: {:?}", block);
        // dbg!(bytes::Bytes::copy_from_slice(&block));
        log::trace!(
            "{:#?}",
            RawBlock::parse_from_raw_block(block, taos_query::prelude::Precision::Millisecond)
        );

        self.ws
            .send(Message::Binary(bytes))
            .await
            .map_err(Error::from)?;
        let _ = self.receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt bind block response"),
        )??;

        Ok(())
    }

    /// Call bind and add batch.
    pub async fn bind_all(&mut self, columns: Vec<serde_json::Value>) -> RawResult<()> {
        self.stmt_bind(columns).await?;
        self.stmt_add_batch().await
    }

    pub async fn stmt_set_tbname(&mut self, name: &str) -> RawResult<()> {
        let message = StmtSend::SetTableName {
            args: self.args.unwrap(),
            name: name.to_string(),
        };
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let _ = self.receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt set_tbname response"),
        )??;
        Ok(())
    }

    pub async fn stmt_set_tags(&mut self, tags: Vec<serde_json::Value>) -> RawResult<()> {
        let message = StmtSend::SetTags {
            args: self.args.unwrap(),
            tags,
        };
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let _ = self.receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt set_tags response"),
        )??;
        Ok(())
    }

    pub async fn stmt_exec(&mut self) -> RawResult<usize> {
        log::trace!("exec");
        let message = StmtSend::Exec(self.args.unwrap());
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        if let Some(affected) = self.receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt exec response"),
        )?? {
            self.affected_rows += affected;
            self.affected_rows_once = affected;
            Ok(affected)
        } else {
            panic!("")
        }
    }

    pub async fn stmt_get_tag_fields(&mut self) -> RawResult<Vec<StmtField>> {
        log::trace!("get tag fields");
        let message = StmtSend::GetTagFields(self.args.unwrap());
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let fields = self.fields_receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt get_tag_fields response"),
        )??;
        Ok(fields)
    }

    pub async fn stmt_get_col_fields(&mut self) -> RawResult<Vec<StmtField>> {
        log::trace!("get col fields");
        let message = StmtSend::GetColFields(self.args.unwrap());
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let fields = self.fields_receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt get_col_fields response"),
        )??;
        Ok(fields)
    }

    pub fn affected_rows_once(&self) -> usize {
        self.affected_rows_once
    }

    pub fn s_num_params(&mut self) -> RawResult<usize> {
        block_in_place_or_global(self.stmt_num_params())
    }

    pub fn s_get_param(&mut self, index: i64) -> RawResult<StmtParam> {
        block_in_place_or_global(self.stmt_get_param(index))
    }

    pub fn s_use_result(&mut self) -> RawResult<StmtUseResult> {
        block_in_place_or_global(self.use_result())
    }

    pub async fn use_result(&mut self) -> RawResult<StmtUseResult> {
        let message = StmtSend::UseResult(self.args.unwrap());
        // FIXME: change to trace before release
        log::debug!("use result message: {:#?}", &message);
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let use_result = self
            .use_result_receiver
            .as_mut()
            .unwrap()
            .recv()
            .await
            .ok_or(taos_query::RawError::from_string(
                "Can't receive stmt use_result response",
            ))??;
        Ok(use_result)
    }

    pub async fn stmt_num_params(&mut self) -> RawResult<usize> {
        let message = StmtSend::StmtNumParams(self.args.unwrap());
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let num_params = self.receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt num_params response"),
        )??;
        match num_params {
            Some(num_params) => Ok(num_params),
            None => Err(taos_query::RawError::from_string(
                "Can't receive stmt num_params response",
            )),
        }
    }

    pub async fn stmt_get_param(&mut self, index: i64) -> RawResult<StmtParam> {
        let message = StmtSend::StmtGetParam {
            args: self.args.unwrap(),
            index,
        };
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let param = self.param_receiver.as_mut().unwrap().recv().await.ok_or(
            taos_query::RawError::from_string("Can't receive stmt get_param response"),
        )??;
        Ok(param)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use taos_query::{common::ColumnView, Dsn, TBuilder};

    use crate::{stmt::Stmt, TaosBuilder};

    use taos_query::prelude::tokio;

    #[tokio::test()]
    async fn test_client() -> anyhow::Result<()> {
        use taos_query::AsyncQueryable;

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?.build()?;
        taos.exec("drop database if exists stmt").await?;
        taos.exec("create database stmt").await?;
        taos.exec("create table stmt.ctb (ts timestamp, v int)")
            .await?;

        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let mut client = Stmt::from_dsn("taos+ws://localhost:6041/stmt").await?;
        let stmt = client.s_stmt("insert into stmt.ctb values(?, ?)").await?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])
        .await?;
        let res = stmt.stmt_exec().await?;

        assert_eq!(res, 2);
        Ok(())
    }

    #[tokio::test()]
    async fn test_stmt_stable_with_json() -> anyhow::Result<()> {
        use taos_query::AsyncQueryable;

        let dsn = Dsn::try_from("taos://localhost:6041")?;
        dbg!(&dsn);

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?.build()?;
        taos.exec("drop database if exists stmt_sj").await?;
        taos.exec("create database stmt_sj").await?;
        taos.exec("create table stmt_sj.stb (ts timestamp, v int) tags(tj json)")
            .await?;

        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let mut client = Stmt::from_dsn("taos+ws://localhost:6041/stmt_sj").await?;
        let stmt = client
            .s_stmt("insert into ? using stb tags(?) values(?, ?)")
            .await?;

        stmt.stmt_set_tbname("tb1").await?;

        stmt.stmt_set_tags(vec![json!(r#"{"name": "value"}"#)])
            .await?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])
        .await?;
        let res = stmt.stmt_exec().await?;

        assert_eq!(res, 2);
        let row: (String, i32, std::collections::HashMap<String, String>) =
            taos.query_one("select * from stmt_sj.stb").await?.unwrap();
        dbg!(row);
        taos.exec("drop database stmt_sj").await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stmt_get_tag_and_col_fields() -> anyhow::Result<()> {
        use taos_query::AsyncQueryable;

        let dsn = Dsn::try_from("taos://localhost:6041")?;
        dbg!(&dsn);

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?.build()?;
        taos.exec("drop database if exists ws_stmt_sj2").await?;
        taos.exec("create database ws_stmt_sj2").await?;
        taos.exec("create table ws_stmt_sj2.stb (ts timestamp, v int) tags(tj json)")
            .await?;

        std::env::set_var("RUST_LOG", "trace");
        // pretty_env_logger::init();
        let mut client = Stmt::from_dsn("taos+ws://localhost:6041/ws_stmt_sj2").await?;
        let stmt = client
            .s_stmt("insert into ? using stb tags(?) values(?, ?)")
            .await?;

        let tag_fields = stmt.stmt_get_tag_fields().await;
        if let Err(err) = tag_fields {
            log::error!("tag fields error: {:?}", err);
        } else {
            log::debug!("tag fields: {:?}", tag_fields);
        }

        stmt.stmt_set_tbname("tb1").await?;

        stmt.stmt_set_tags(vec![json!(r#"{"name": "value"}"#)])
            .await?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])
        .await?;
        let res = stmt.stmt_exec().await?;

        assert_eq!(res, 2);
        let row: (String, i32, std::collections::HashMap<String, String>) = taos
            .query_one("select * from ws_stmt_sj2.stb")
            .await?
            .unwrap();
        dbg!(row);

        let tag_fields = stmt.stmt_get_tag_fields().await?;

        log::debug!("tag fields: {:?}", tag_fields);

        let col_fields = stmt.stmt_get_col_fields().await?;

        log::debug!("col fields: {:?}", col_fields);

        taos.exec("drop database ws_stmt_sj2").await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stmt_use_result() -> anyhow::Result<()> {
        use taos_query::AsyncQueryable;

        let dsn = Dsn::try_from("taos://localhost:6041")?;

        let db = "ws_stmt_use_result";

        let taos = TaosBuilder::from_dsn(&dsn)?.build()?;
        taos.exec(format!("drop database if exists {db}")).await?;
        taos.exec(format!("create database {db}")).await?;
        taos.exec(format!(
            "create table {db}.stb (ts timestamp, v int) tags(utntag TINYINT UNSIGNED)"
        ))
        .await?;
        taos.exec(format!("use {db}")).await?;
        taos.exec("create table t1 using stb tags(0)").await?;
        taos.exec("insert into t1 values(1640000000000, 0)").await?;

        std::env::set_var("RUST_LOG", "debug");
        // only init for debug
        pretty_env_logger::init();
        let mut client = Stmt::from_dsn(format!("{dsn}/{db}", dsn = &dsn)).await?;
        let stmt = client.s_stmt("select * from t1 where v < ?").await?;

        let params = vec![ColumnView::from_ints(vec![10])];
        stmt.stmt_bind_block(&params).await?;

        let res = stmt.stmt_exec().await?;

        dbg!(res);

        assert_eq!(res, 0);

        let res = stmt.use_result().await;

        log::debug!("use result: {:?}", res);

        taos.exec(format!("drop database {db}")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stmt_use_result_usage_error() -> anyhow::Result<()> {
        use taos_query::AsyncQueryable;

        let dsn = Dsn::try_from("taos://localhost:6041")?;

        let db = "ws_stmt_use_result_usage_error";

        let taos = TaosBuilder::from_dsn(&dsn)?.build()?;
        taos.exec(format!("drop database if exists {db}")).await?;
        taos.exec(format!("create database {db}")).await?;
        taos.exec(format!(
            "create table {db}.stb (ts timestamp, v int) tags(utntag TINYINT UNSIGNED)"
        ))
        .await?;
        taos.exec(format!("use {db}")).await?;
        taos.exec("create table t1 using stb tags(0)").await?;
        taos.exec("insert into t1 values(1640000000000, 0)").await?;

        std::env::set_var("RUST_LOG", "debug");
        // only init for debug
        // pretty_env_logger::init();
        let mut client = Stmt::from_dsn(format!("{dsn}/{db}", dsn = &dsn)).await?;
        let stmt = client
            .s_stmt("insert into ? using stb tags(?) values(?, ?)")
            .await?;

        stmt.stmt_set_tbname("tb1").await?;

        stmt.stmt_set_tags(vec![json!(1)])
            .await?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])
        .await?;

        let res = stmt.stmt_exec().await?;

        dbg!(res);

        assert_eq!(res, 2);

        let res = stmt.use_result().await;

        log::debug!("use result: {:?}", res);

        assert!(res.is_err());

        taos.exec(format!("drop database {db}")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stmt_num_params_and_get_param() -> anyhow::Result<()> {
        use taos_query::AsyncQueryable;

        let dsn = Dsn::try_from("taos://localhost:6041")?;

        let db = "ws_stmt_num_params";

        let taos = TaosBuilder::from_dsn(&dsn)?.build()?;
        taos.exec(format!("drop database if exists {db}")).await?;
        taos.exec(format!("create database {db}")).await?;
        taos.exec(format!(
            "create table {db}.stb (ts timestamp, v int) tags(tj json)"
        ))
        .await?;

        std::env::set_var("RUST_LOG", "debug");
        // only init for debug
        // pretty_env_logger::init();
        let mut client = Stmt::from_dsn(format!("{dsn}/{db}", dsn = &dsn)).await?;
        let stmt = client
            .s_stmt("insert into ? using stb tags(?) values(?, ?)")
            .await?;

        stmt.stmt_set_tbname("tb1").await?;

        stmt.stmt_set_tags(vec![json!(r#"{"name": "value"}"#)])
            .await?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])
        .await?;
        let res = stmt.stmt_exec().await?;

        assert_eq!(res, 2);
        let row: (String, i32, std::collections::HashMap<String, String>) = taos
            .query_one(format!("select * from {db}.stb"))
            .await?
            .unwrap();
        dbg!(row);

        let num_params = stmt.stmt_num_params().await?;

        log::debug!("stmt num params: {:?}", num_params);

        for i in 0..num_params {
            let param = stmt.stmt_get_param(i as i64).await?;
            log::debug!("param {}: {:?}", i, param);
        }

        taos.exec(format!("drop database {db}")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stmt_stable() -> anyhow::Result<()> {
        use taos_query::AsyncQueryable;

        let dsn = Dsn::try_from("taos://localhost:6041")?;
        dbg!(&dsn);

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?.build()?;
        taos.exec("drop database if exists stmt_s").await?;
        taos.exec("create database stmt_s").await?;
        taos.exec("create table stmt_s.stb (ts timestamp, v int) tags(t1 int)")
            .await?;

        std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let mut client = Stmt::from_dsn("taos+ws://localhost:6041/stmt_s").await?;
        let stmt = client
            .s_stmt("insert into ? using stb tags(?) values(?, ?)")
            .await?;

        stmt.stmt_set_tbname("tb1").await?;

        // stmt.set_tags(vec![json!({"name": "value"})]).await?;

        stmt.stmt_set_tags(vec![json!(1)]).await?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])
        .await?;
        let res = stmt.stmt_exec().await?;

        assert_eq!(res, 2);
        taos.exec("drop database stmt_s").await?;
        Ok(())
    }
}
