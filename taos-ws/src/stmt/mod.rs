use futures::{SinkExt, StreamExt};
use itertools::Itertools;
// use scc::HashMap;
use dashmap::DashMap as HashMap;

use serde::Deserialize;

use taos_query::common::views::views_to_raw_block;
use taos_query::common::ColumnView;
use taos_query::prelude::{InlinableWrite, RawResult};
use taos_query::stmt::Bindable;
use taos_query::{block_in_place_or_global, IntoDsn, RawBlock};

use taos_query::prelude::tokio;
use tokio::sync::{oneshot, watch};

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

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
type StmtSender = std::sync::mpsc::SyncSender<StmtResult>;
type StmtReceiver = std::sync::mpsc::Receiver<StmtResult>;

type StmtFieldResult = RawResult<Vec<StmtField>>;
type StmtFieldSender = std::sync::mpsc::SyncSender<StmtFieldResult>;
type StmtFieldReceiver = std::sync::mpsc::Receiver<StmtFieldResult>;

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
        let database: Option<String> =
            <Taos as taos_query::Queryable>::query_one(taos, "select database()")?;
        dsn.database = database;
        let mut stmt = block_in_place_or_global(Self::from_wsinfo(&dsn))?;
        block_in_place_or_global(stmt.stmt_init())?;
        Ok(stmt)
    }

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self> {
        block_in_place_or_global(self.stmt_prepare(sql.as_ref()))?;
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
        // block_in_place_or_global(self.stmt_bind(columns))?;

        block_in_place_or_global(self.stmt_bind_block(params))?;
        Ok(self)
    }

    fn add_batch(&mut self) -> RawResult<&mut Self> {
        block_in_place_or_global(self.stmt_add_batch())?;
        Ok(self)
    }

    fn execute(&mut self) -> RawResult<usize> {
        block_in_place_or_global(self.stmt_exec())
    }

    fn affected_rows(&self) -> usize {
        self.affected_rows
    }
}

pub trait WsFieldsable {
    fn get_tag_fields(&self) -> RawResult<Vec<StmtField>>;

    fn get_col_fields(&self) -> RawResult<Vec<StmtField>>;
}

impl WsFieldsable for Stmt {
    fn get_tag_fields(&self) -> RawResult<Vec<StmtField>> {
        block_in_place_or_global(self.stmt_get_tag_fields())
    }

    fn get_col_fields(&self) -> RawResult<Vec<StmtField>> {
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
    fields_fetches: Arc<HashMap<StmtId, StmtFieldSender>>,
    fields_receiver: Option<StmtFieldReceiver>,
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

// pub struct WsAsyncStmt {
//     /// Message sending timeout, default is 5min.
//     timeout: Duration,
//     ws: WsSender,
//     fetches: Arc<HashMap<StmtId, StmtSender>>,
//     receiver: StmtReceiver,
//     args: StmtArgs,
// }
// impl Debug for WsAsyncStmt {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("WsAsyncStmt")
//             .field("ws", &"...")
//             .field("fetches", &"...")
//             .field("receiver", &self.receiver)
//             .field("args", &self.args)
//             .finish()
//     }
// }

// impl Drop for WsAsyncStmt {
//     fn drop(&mut self) {
//         self.fetches.remove(&self.args.stmt_id);
//         let args = self.args;
//         let ws = self.ws.clone();
//         let _ = taos_query::block_in_place_or_global(ws.send(StmtSend::Close(args).to_msg()));
//     }
// }

impl Debug for Stmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsClient")
            .field("req_id", &self.req_id)
            .field("...", &"...")
            .finish()
    }
}

// #[derive(Debug, Error)]
// pub enum Error {
//     #[error("{0}")]
//     Dsn(#[from] DsnError),
//     #[error("{0}")]
//     FetchError(#[from] oneshot::error::RecvError),
//     #[error("{0}")]
//     SendError(#[from] tokio::sync::mpsc::error::SendError<Message>),
//     #[error(transparent)]
//     SendTimeoutError(#[from] tokio::sync::mpsc::error::SendTimeoutError<Message>),
//     #[error("{0}")]
//     StdSendError(#[from] std::sync::mpsc::SendError<tokio_tungstenite::tungstenite::Message>),
//     #[error("{0}")]
//     RecvError(#[from] std::sync::mpsc::RecvError),
//     #[error("{0}")]
//     RecvTimeout(#[from] std::sync::mpsc::RecvTimeoutError),
//     #[error("{0}")]
//     DeError(#[from] DeError),
//     #[error("{0}")]
//     WsError(#[from] WsError),
//     #[error("{0}")]
//     TaosError(#[from] RawError),
// }

// impl Error {
//     pub const fn errno(&self) -> taos_error::Code {
//         match self {
//             Error::TaosError(error) => error.code(),
//             _ => taos_error::Code::FAILED,
//         }
//     }
//     pub fn errstr(&self) -> String {
//         match self {
//             Error::TaosError(error) => error.message().to_string(),
//             _ => format!("{}", self),
//         }
//     }
// }

impl Drop for Stmt {
    fn drop(&mut self) {
        // send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

impl Stmt {
    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {
        let (ws, _) = connect_async(info.to_stmt_url())
            .await
            .map_err(Error::from)?;
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

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel(100);
        let ws2 = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let mut close_listener = rx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(msg) = msg_recv.recv() => {
                        sender.send(msg).await.unwrap();
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
                                    log::trace!("json response: {}", text);
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
                                                sender.send(res).unwrap();
                                            // }) {

                                            } else {
                                                log::trace!("Got unknown stmt id: {stmt_id} with result: {res:?}");
                                            }
                                        }
                                        StmtOk::StmtFields(stmt_id, res) => {
                                            if let Some(sender) = fields_fetches_sender.get(&stmt_id) {
                                                log::trace!("send data to fetches with id {}", stmt_id);
                                                // let res = res.clone();
                                                sender.send(res).unwrap();
                                            // }) {

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
            fields_fetches,
            fields_receiver: None,
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
        let action = StmtSend::Init { req_id };
        let (tx, rx) = oneshot::channel();
        {
            self.queries.insert(req_id, tx);
            self.ws.send(action.to_msg()).await.map_err(Error::from)?;
        }
        let stmt_id = rx.await.map_err(Error::from)??; // 1. RecvError, 2. TaosError
        let args = StmtArgs { req_id, stmt_id };

        let (sender, receiver) = std::sync::mpsc::sync_channel(2);

        let _ = self.fetches.insert(stmt_id, sender);

        self.args = Some(args);
        self.receiver = Some(receiver);

        let (fields_sender, fields_receiver) = std::sync::mpsc::sync_channel(2);
        let _ = self.fields_fetches.insert(stmt_id, fields_sender);
        self.fields_receiver = Some(fields_receiver);
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
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)
            .map_err(Error::from)??;
        Ok(())
    }
    pub async fn stmt_add_batch(&mut self) -> RawResult<()> {
        log::trace!("add batch");
        let message = StmtSend::AddBatch(self.args.unwrap());
        self.ws.send(message.to_msg()).await.map_err(Error::from)?;
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)
            .map_err(Error::from)??;
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
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)
            .map_err(Error::from)??;
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
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)
            .map_err(Error::from)??;

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
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)
            .map_err(Error::from)??;
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
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)
            .map_err(Error::from)?;
        Ok(())
    }

    pub async fn stmt_exec(&mut self) -> RawResult<usize> {
        log::trace!("exec");
        let message = StmtSend::Exec(self.args.unwrap());
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        if let Some(affected) = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)
            .map_err(Error::from)??
        {
            self.affected_rows += affected;
            Ok(affected)
        } else {
            panic!("")
        }
    }

    pub async fn stmt_get_tag_fields(&self) -> RawResult<Vec<StmtField>> {
        log::trace!("get tag fields");
        let message = StmtSend::GetTagFields(self.args.unwrap());
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let fields = self
            .fields_receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)
            .map_err(Error::from)??;
        Ok(fields)
    }

    pub async fn stmt_get_col_fields(&self) -> RawResult<Vec<StmtField>> {
        log::trace!("get col fields");
        let message = StmtSend::GetColFields(self.args.unwrap());
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let fields = self
            .fields_receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)
            .map_err(Error::from)??;
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use taos_query::{Dsn, TBuilder};

    use crate::{stmt::Stmt, TaosBuilder};

    use taos_query::prelude::tokio;

    // Websocket tests should always use `multi_thread`
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_stmt_get_tag_and_col_fields() -> anyhow::Result<()> {
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

        let tag_fields = stmt.stmt_get_tag_fields().await?;

        log::debug!("tag fields: {:?}", tag_fields);

        let col_fields = stmt.stmt_get_col_fields().await?;

        log::debug!("col fields: {:?}", col_fields);

        taos.exec("drop database stmt_sj").await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
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
