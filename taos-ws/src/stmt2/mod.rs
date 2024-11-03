mod bind;
mod messages;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use bind::{bind_datas_as_bytes, Stmt2BindData};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use messages::*;
use serde::Serialize;
use taos_query::prelude::RawResult;
use taos_query::IntoDsn;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, warn};

use crate::query::asyn::Error;
use crate::query::infra::ToMessage;
use crate::TaosBuilder;

type StmtNumResult = RawResult<Option<usize>>;
type StmtNumResultSender = mpsc::Sender<StmtNumResult>;
type StmtNumResultReceiver = mpsc::Receiver<StmtNumResult>;

type StmtPrepareResultResult = RawResult<Stmt2PrepareResult>;
type StmtPrepareResultSender = mpsc::Sender<StmtPrepareResultResult>;
type StmtPrepareResultReceiver = mpsc::Receiver<StmtPrepareResultResult>;

type StmtResultResult = RawResult<Stmt2Result>;
type StmtResultResultSender = mpsc::Sender<StmtResultResult>;
type StmtResultResultReceiver = mpsc::Receiver<StmtResultResult>;

type WsSender = mpsc::Sender<Message>;
type StmtIdSender = oneshot::Sender<RawResult<StmtId>>;

pub type ReqId = u64;
pub type StmtId = u64;

pub struct Stmt2 {
    ws: WsSender,
    timeout: Duration,
    close_signal: watch::Sender<bool>,
    args: Option<Stmt2Args>,
    stmt_id_sender_map: Arc<DashMap<ReqId, StmtIdSender>>,
    stmt_num_res_receiver: Option<StmtNumResultReceiver>,
    stmt_num_res_sender_map: Arc<DashMap<StmtId, StmtNumResultSender>>,
    stmt_prepare_res_receiver: Option<StmtPrepareResultReceiver>,
    stmt_prepare_res_sender_map: Arc<DashMap<StmtId, StmtPrepareResultSender>>,
    stmt_res_res_receiver: Option<StmtResultResultReceiver>,
    stmt_res_res_sender_map: Arc<DashMap<StmtId, StmtResultResultSender>>,
    affected_rows: usize,
    affected_rows_once: usize,
    is_insert: Option<bool>,
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct Stmt2Args {
    pub req_id: ReqId,
    pub stmt_id: StmtId,
}

impl Stmt2 {
    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> RawResult<Self> {
        let ws = info.build_stream(info.to_stmt_url()).await?;
        let (mut ws_sender, mut ws_recevier) = ws.split();

        let req_id = 0;
        let login = Stmt2Send::Conn {
            req_id,
            req: info.to_conn_request(),
        };
        ws_sender.send(login.to_msg()).await.map_err(Error::from)?;
        if let Some(Ok(message)) = ws_recevier.next().await {
            match message {
                Message::Text(text) => {
                    let v: Stmt2Recv = serde_json::from_str(&text).unwrap();
                    match v.data {
                        Stmt2RecvData::Conn => (),
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        }

        let stmt_id_sender_map = Arc::new(DashMap::<ReqId, StmtIdSender>::new());
        let stmt_num_res_sender_map = Arc::new(DashMap::<StmtId, StmtNumResultSender>::new());
        let stmt_prepare_res_sender_map =
            Arc::new(DashMap::<StmtId, StmtPrepareResultSender>::new());
        let stmt_res_res_sender_map = Arc::new(DashMap::<StmtId, StmtResultResultSender>::new());

        let stmt_id_sender_map_clone = stmt_id_sender_map.clone();
        let stmt_num_res_sender_map_clone = stmt_num_res_sender_map.clone();
        let stmt_prepare_res_sender_map_clone = stmt_prepare_res_sender_map.clone();
        let stmt_res_res_sender_map_clone = stmt_res_res_sender_map.clone();

        let (msg_sender, mut msg_receiver) = mpsc::channel(100);
        let (watch_sender, mut watch_receiver) = watch::channel(false);
        let mut close_listener = watch_receiver.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(msg) = msg_receiver.recv() => {
                        debug!("receive msg: {msg}"); // delete
                        if let Err(err) = ws_sender.send(msg).await {
                            warn!("sender error: {err:#}");
                            break;
                        }
                    }
                    _ = watch_receiver.changed() => {
                        debug!("close sender task");
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(message) = ws_recevier.next() => {
                        match message {
                            Ok(message) => match message {
                                Message::Text(text) => {
                                    tracing::trace!("json response: {}", text);
                                    debug!("json response: {}", text);
                                    let v: Stmt2Recv = serde_json::from_str(&text).unwrap();
                                    debug!("object Stmt2Recv: {v:?}");
                                    match v.ok() {
                                        Stmt2Ok::Conn(_) => {
                                            warn!("[{req_id}] received connected response in message loop");
                                        },
                                        Stmt2Ok::Stmt2Init(req_id, stmt_id) => {
                                            debug!("stmt2 init done: {{ req_id: {}, stmt_id: {:?} }}", req_id, stmt_id);
                                            if let Some((_, sender)) = stmt_id_sender_map.remove(&req_id) {
                                                sender.send(stmt_id).unwrap();
                                            } else {
                                                debug!("stmt2 init failed because req id {req_id} not exist");
                                            }
                                        }
                                        Stmt2Ok::Stmt2PrepareRes(stmt_id, res) => {
                                            if let Some(sender) = stmt_prepare_res_sender_map.get(&stmt_id) {
                                                debug!("send data to fetches with id {}", stmt_id);
                                                sender.send(res).await.unwrap();
                                            } else {
                                                debug!("got unknown stmt id: {stmt_id} with result: {res:?}");
                                            }
                                        }
                                        Stmt2Ok::Stmt2Res(stmt_id, res) => {
                                            if let Some(sender) = stmt_res_res_sender_map.get(&stmt_id) {
                                                debug!("send data to fetches with id {}", stmt_id);
                                                sender.send(res).await.unwrap();
                                            } else {
                                                debug!("got unknown stmt id: {stmt_id} with result: {res:?}");
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                Message::Binary(_) => {
                                    warn!("received (unexpected) binary message, do nothing");
                                }
                                Message::Close(_) => {
                                    warn!("websocket connection is closed (unexpected?)");
                                    break;
                                }
                                Message::Ping(_) => {
                                    warn!("received (unexpected) ping message, do nothing");
                                }
                                Message::Pong(_) => {
                                    warn!("received (unexpected) pong message, do nothing");
                                }
                                Message::Frame(frame) => {
                                    warn!("received (unexpected) frame message, do nothing");
                                    debug!("* frame data: {frame:?}");
                                }
                            },
                            Err(err) => {
                                debug!("receiving cause error: {err:?}");
                                break;
                            }
                        }
                    }
                    _ = close_listener.changed() => {
                        debug!("close reader task");
                        break
                    }
                }
            }
        });

        Ok(Self {
            ws: msg_sender,
            args: None,
            timeout: Duration::from_secs(5),
            stmt_id_sender_map: stmt_id_sender_map_clone,
            stmt_num_res_receiver: None,
            stmt_num_res_sender_map: stmt_num_res_sender_map_clone,
            stmt_prepare_res_receiver: None,
            stmt_prepare_res_sender_map: stmt_prepare_res_sender_map_clone,
            stmt_res_res_receiver: None,
            stmt_res_res_sender_map: stmt_res_res_sender_map_clone,
            is_insert: None,
            affected_rows: 0,
            affected_rows_once: 0,
            close_signal: watch_sender,
        })
    }

    pub async fn from_dsn(dsn: impl IntoDsn) -> RawResult<Self> {
        let info = TaosBuilder::from_dsn(dsn)?;
        Self::from_wsinfo(&info).await
    }

    pub async fn stmt2_init(
        &mut self,
        req_id: u64,
        single_stb_insert: bool,
        single_table_bind_once: bool,
    ) -> RawResult<&mut Self> {
        let init = Stmt2Send::Stmt2Init {
            req_id,
            single_stb_insert,
            single_table_bind_once,
        };
        let (stmt_id_sender, stmt_id_recviver) = oneshot::channel();
        self.stmt_id_sender_map.insert(req_id, stmt_id_sender);
        self.ws.send(init.to_msg()).await.map_err(Error::from)?;
        let stmt_id = stmt_id_recviver.await.map_err(Error::from)??;
        let args = Stmt2Args { req_id, stmt_id };
        self.args = Some(args);

        let (stmt_num_res_sender, stmt_num_res_receiver) = mpsc::channel(2);
        let _ = self
            .stmt_num_res_sender_map
            .insert(stmt_id, stmt_num_res_sender);
        self.stmt_num_res_receiver = Some(stmt_num_res_receiver);

        let (stmt_prepare_res_sender, stmt_prepare_res_receiver) = mpsc::channel(2);
        let _ = self
            .stmt_prepare_res_sender_map
            .insert(stmt_id, stmt_prepare_res_sender);
        self.stmt_prepare_res_receiver = Some(stmt_prepare_res_receiver);

        let (stmt_res_res_sender, stmt_res_res_recevier) = mpsc::channel(2);
        let _ = self
            .stmt_res_res_sender_map
            .insert(stmt_id, stmt_res_res_sender);
        self.stmt_res_res_receiver = Some(stmt_res_res_recevier);

        Ok(self)
    }

    pub async fn stmt2_prepare(&mut self, sql: &str, get_fields: bool) -> RawResult<()> {
        let prepare = Stmt2Send::Stmt2Prepare {
            args: self.args.unwrap(),
            sql: sql.to_string(),
            get_fields,
        };
        self.ws.send(prepare.to_msg()).await.map_err(Error::from)?;
        let res = self
            .stmt_prepare_res_receiver
            .as_mut()
            .unwrap()
            .recv()
            .await
            .ok_or(taos_query::RawError::from_string(
                "Can't receive stmt2 prepare result response",
            ))??;
        self.is_insert = Some(res.is_insert);
        Ok(())
    }

    pub async fn stmt2_exec(&mut self) -> RawResult<usize> {
        let message = Stmt2Send::Stmt2Exec(self.args.unwrap());
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        if let Some(affected) = self
            .stmt_num_res_receiver
            .as_mut()
            .unwrap()
            .recv()
            .await
            .ok_or(taos_query::RawError::from_string(
                "Can't receive stmt2 exec response",
            ))??
        {
            self.affected_rows += affected;
            self.affected_rows_once = affected;
            Ok(affected)
        } else {
            panic!("xxx")
        }
    }

    pub async fn stmt2_result(&mut self) -> RawResult<Stmt2Result> {
        if self.is_insert.unwrap() {
            return Err(taos_query::RawError::from_string(
                "Can't use result for insert stmt2",
            ));
        }
        let message = Stmt2Send::Stmt2Result(self.args.unwrap());
        debug!("use result message: {:#?}", &message);
        self.ws
            .send_timeout(message.to_msg(), self.timeout)
            .await
            .map_err(Error::from)?;
        let use_result = self
            .stmt_res_res_receiver
            .as_mut()
            .unwrap()
            .recv()
            .await
            .ok_or(taos_query::RawError::from_string(
                "Can't receive stmt use_result response",
            ))??;
        Ok(use_result)
    }

    pub async fn stmt2_bind_param<'a>(
        &mut self,
        datas: &[Stmt2BindData<'a>],
        is_insert: bool,
    ) -> RawResult<()> {
        let bytes = bind_datas_as_bytes(datas, is_insert)?;

        self.ws
            .send(Message::binary(bytes))
            .await
            .map_err(Error::from)?;

        let _ = self
            .stmt_num_res_receiver
            .as_mut()
            .unwrap()
            .recv()
            .await
            .ok_or(taos_query::RawError::from_string(
                "Can't receive stmt2 bind param response",
            ))??;

        Ok(())
    }

    pub async fn stmt2_get_fields(&self) {
        todo!()
    }

    pub async fn stmt2_close(&self) {
        todo!()
    }
}

impl Drop for Stmt2 {
    fn drop(&mut self) {
        // send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

#[cfg(test)]
mod tests {
    use taos_query::{AsyncQueryable, AsyncTBuilder};

    use crate::stmt2::Stmt2;
    use crate::TaosBuilder;

    #[tokio::test]
    async fn test_stmt2() -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();

        let db = "stmt2";
        let default_dsn = "taos://localhost:6041";
        let dsn = std::env::var("TDENGINE_ClOUD_DSN").unwrap_or(default_dsn.to_string());

        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;
        taos.exec(format!("drop database if exists {db}")).await?;
        taos.exec(format!("create database {db}")).await?;
        taos.exec(format!("create table {db}.ctb (ts timestamp, a int)"))
            .await?;

        std::env::set_var("RUST_LOG", "trace");
        let dsn_stmt = format!("{dsn}/{db}");
        let mut stmt2 = Stmt2::from_dsn(dsn_stmt).await?;
        stmt2.stmt2_init(100, false, false).await?;
        stmt2
            .stmt2_prepare("insert into ctb values (?, ?)", false)
            .await?;
        Ok(())
    }
}
