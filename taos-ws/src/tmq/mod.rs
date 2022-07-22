use futures::stream::SplitSink;
use futures::{FutureExt, SinkExt, StreamExt};
use scc::HashMap;
// use std::sync::Mutex;
use taos_query::common::{Field, Precision};
use taos_query::{AsyncFetchable, AsyncQueryable, DeError, DsnError, IntoDsn};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::{oneshot, watch};

use tokio::time;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::infra::ToMessage;
use crate::WsInfo;

use std::fmt::Debug;
use std::hash::Hash;
use std::result::Result as StdResult;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

pub(crate) mod message;
use message::*;

type WsFetchResult = std::result::Result<(), taos_error::Error>;
type PollSender = std::sync::mpsc::SyncSender<WsFetchResult>;
type PollReceiver = std::sync::mpsc::Receiver<WsFetchResult>;
// type TmqSenderStream = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

type TmqSender = tokio::sync::mpsc::Sender<Message>;

type TmqMsgResult = std::result::Result<TmqMsgData, taos_error::Error>;
type TmqMsgSender = tokio::sync::mpsc::Sender<TmqMsgResult>;

#[test]
fn test() {
    #[repr(C, u8)]
    enum A {
        Ok(u16),
        None,
    }
    dbg!(std::mem::size_of::<A>());
}

#[derive(Debug)]
struct Closer(watch::Sender<bool>);

pub struct AsyncTmqBuilder {
    req_id: Arc<AtomicU64>,
    ws: TmqSender,
    close_signal: watch::Sender<bool>,
    queries: Arc<HashMap<ReqId, oneshot::Sender<std::result::Result<TmqPoll, taos_error::Error>>>>,
    fetches: Arc<HashMap<ResId, PollSender>>,
    messages: Arc<HashMap<TmqArgs, TmqMsgSender>>,
}

pub struct ResultSet {
    ws: TmqSender,
    fetches: Arc<HashMap<ResId, PollSender>>,
    receiver: Option<PollReceiver>,
    args: TmqArgs,
    fields: Option<Vec<Field>>,
    fields_count: usize,
    affected_rows: usize,
    precision: Precision,
}
impl Debug for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultSet")
            .field("ws", &"...")
            .field("fetches", &"...")
            .field("receiver", &self.receiver)
            .field("args", &self.args)
            .field("fields", &self.fields)
            .field("fields_count", &self.fields_count)
            .field("affected_rows", &self.affected_rows)
            .field("precision", &self.precision)
            .finish()
    }
}
pub struct ResultSetRef {
    ws: TmqSender,
    fetches: Arc<HashMap<ResId, PollSender>>,
    receiver: Option<PollReceiver>,
    args: TmqArgs,
    fields: Option<Vec<Field>>,
    fields_count: usize,
    affected_rows: usize,
    precision: Precision,
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        if self.receiver.is_some() {
            self.fetches.remove(&self.args.consumer_id);
            let args = self.args;
            let ws = self.ws.clone();
            tokio::spawn(async move {
                let _ = ws.send(TmqSend::Close(args).to_msg()).await;
            });
        }
    }
}

impl Debug for AsyncTmqBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsClient")
            .field("req_id", &self.req_id)
            .field("...", &"...")
            .finish()
    }
}
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Dsn(#[from] DsnError),
    #[error("{0}")]
    FetchError(#[from] oneshot::error::RecvError),
    #[error("{0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<Message>),
    #[error("{0}")]
    StdSendError(#[from] std::sync::mpsc::SendError<tokio_tungstenite::tungstenite::Message>),
    #[error("{0}")]
    RecvError(#[from] std::sync::mpsc::RecvError),
    #[error("{0}")]
    TaosError(#[from] taos_error::Error),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("{0}")]
    WsError(#[from] WsError),
    #[error("{0}")]
    InitError(#[from] crate::Error),
}

type Result<T> = std::result::Result<T, Error>;

impl Drop for AsyncTmqBuilder {
    fn drop(&mut self) {
        // send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

impl AsyncTmqBuilder {
    // pub(crate) fn new
    pub(crate) async fn from_wsinfo(info: &WsInfo) -> Result<Self> {
        let (ws, _) = connect_async(info.to_stmt_url()).await?;
        let req_id = 0;
        let (mut sender, mut reader) = ws.split();
        // let init = TmqSend::Init {
        //     req_id: 0,
        //     req: info.to_tmq_init()?,
        // };
        // sender.send(init.to_msg()).await?;

        // log::info!("");

        // let recv = reader.next().await.unwrap()?;
        // println!("received");
        // let args = match recv {
        //     Message::Text(text) => {
        //         let v: TmqRecv = serde_json::from_str(&text).unwrap();
        //         let (args, data, ok) = dbg!(v.ok());
        //         let _ = ok?;
        //         match data {
        //             TmqRecvData::Init => args,
        //             _ => unreachable!(),
        //         }
        //     }
        //     _ => unreachable!(),
        // };

        use std::collections::hash_map::RandomState;

        let queries = Arc::new(HashMap::<ReqId, tokio::sync::oneshot::Sender<_>>::new(
            100,
            RandomState::new(),
        ));

        let fetches = Arc::new(HashMap::<TmqArgs, PollSender>::new(100, RandomState::new()));

        let messages = Arc::new(HashMap::<TmqArgs, TmqMsgSender>::new(100, RandomState::new()));

        let queries_sender = queries.clone();
        let fetches_sender = fetches.clone();
        let messages_sender = messages.clone();

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel(100);
        let ws2 = ws.clone();

        // // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let mut close_listener = rx.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(10));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        //
                        // println!("10ms passed");
                    }
                    Some(msg) = msg_recv.recv() => {
                        dbg!(&msg);
                        sender.send(msg).await.unwrap();
                    }
                    _ = rx.changed() => {
                        log::info!("close sender task");
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
                                    dbg!(&text);
                                    let v: TmqRecv = serde_json::from_str(&text).unwrap();
                                    let (args, data, ok) = v.ok();
                                    match &data {
                                        TmqRecvData::Init => {
                                            if let Some((_, sender)) = queries_sender.remove(&args.req_id)
                                            {
                                                sender.send(ok.map(|_|args)).unwrap();
                                            }
                                        },
                                        TmqRecvData::Subscribe => {
                                            if let Some(_) = fetches_sender.read(&args, |_, sender| {
                                                sender.send(ok).unwrap();
                                            }) {}
                                        }
                                        TmqRecvData::Poll(poll) => {
                                            todo!()
                                        }
                                        TmqRecvData::Unsubscribe => {
                                            if let Some(_) = fetches_sender.read(&args, |_, sender| {
                                                sender.send(ok).unwrap();
                                            }) {}
                                        }
                                        _ => unreachable!()

                                        // TmqRecvData::Fetch(fetch) => {
                                        //     let id = fetch;
                                        //     if fetch.completed {
                                        //         ws2.send(
                                        //             TmqSend::Close(TmqArgs {
                                        //                 req_id,
                                        //                 consumer_id,
                                        //             })
                                        //             .to_msg(),
                                        //         )
                                        //         .await
                                        //         .unwrap();
                                        //     }
                                        //     let data = ok.map(|_|TmqMsgData::Fetch(fetch));
                                        //     if let Some(_) = fetches_sender.read(&id, |_, v| {
                                        //         log::info!("send data to fetches with id {}", id);
                                        //         v.send(data.clone()).unwrap();
                                        //     }) {}
                                        // }
                                        // // Block type is for binary.
                                        // TmqRecvData::Block(_) => unreachable!(),

                                    }
                                }
                                Message::Binary(block) => {
                                    dbg!(&block);
                                    let mut slice = block.as_slice();
                                    use taos_query::util::InlinableRead;
                                    let res_id = slice.read_u64().unwrap();

                                    if let Some(_) = messages_sender.read(&res_id, |_, v| {
                                        log::info!("send data to fetches with id {}", res_id);
                                        let raw = slice.read_inlinable::<RawBlock>().unwrap();
                                        v.send(Ok(TmqMsgData::Block(dbg!(raw)).clone())).unwrap();
                                    }) {}
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
                                    log::debug!("* frame data: {frame:?}");
                                }
                            },
                            Err(err) => {
                                dbg!(err);
                            }
                        }
                    }
                    _ = close_listener.changed() => {
                        log::info!("close reader task");
                        break
                    }
                }
            }
        });

        Ok(Self {
            req_id: Arc::new(AtomicU64::new(req_id + 1)),
            queries,
            fetches,
            ws,
            close_signal: tx,
        })
    }
    /// Build TDengine websocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041/
    /// ```
    ///
    pub async fn from_dsn(dsn: impl IntoDsn) -> Result<Self> {
        let info = WsInfo::from_dsn(dsn)?;
        Self::from_wsinfo(&info).await
    }

    fn req_id(&self) -> u64 {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn s_query(&self, sql: &str) -> Result<ResultSet> {
        todo!()
        // let req_id = self.req_id();
        // let action = TmqSend::Subscribe {
        //     req_id,
        //     sql: sql.to_string(),
        // };
        // let (tx, rx) = oneshot::channel();
        // {
        //     self.queries.insert(req_id, tx).unwrap();
        //     self.ws.send(action.to_msg()).await?;
        // }
        // let resp = rx.await??;

        // if resp.fields_count > 0 {
        //     let names = resp.fields_names.unwrap();
        //     let types = resp.fields_types.unwrap();
        //     let bytes = resp.fields_lengths.unwrap();
        //     let fields: Vec<_> = names
        //         .into_iter()
        //         .zip(types)
        //         .zip(bytes)
        //         .map(|((name, ty), bytes)| Field::new(name, ty, bytes))
        //         .collect();

        //     let (sender, receiver) = std::sync::mpsc::sync_channel(2);
        //     self.fetches.insert(resp.id, sender).unwrap();
        //     Ok(ResultSet {
        //         ws: self.ws.clone(),
        //         fetches: self.fetches.clone(),
        //         receiver: Some(receiver),
        //         fields: Some(fields),
        //         fields_count: resp.fields_count,
        //         precision: resp.precision,
        //         affected_rows: resp.affected_rows,
        //         args: TmqArgs {
        //             req_id,
        //             consumer_id: resp.id,
        //         },
        //     })
        // } else {
        //     Ok(ResultSet {
        //         affected_rows: resp.affected_rows,
        //         ws: self.ws.clone(),
        //         fetches: self.fetches.clone(),
        //         receiver: None,
        //         args: TmqArgs {
        //             req_id,
        //             consumer_id: resp.id,
        //         },
        //         fields: None,
        //         fields_count: 0,
        //         precision: resp.precision,
        //     })
        // }
    }

    pub async fn s_exec(&self, sql: &str) -> Result<usize> {
        todo!()
        // let req_id = self.req_id();
        // let action = TmqSend::Query {
        //     req_id,
        //     sql: sql.to_string(),
        // };
        // let (tx, rx) = oneshot::channel();
        // {
        //     self.queries.insert(req_id, tx).unwrap();
        //     self.ws.send(action.to_msg()).await?;
        // }
        // let resp = rx.await??;
        // Ok(resp.affected_rows)
    }
}

// Websocket tests should always use `multi_thread`

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_client() -> anyhow::Result<()> {
    // use futures::TryStreamExt;
    // std::env::set_var("RUST_LOG", "debug");
    // pretty_env_logger::init();
    // let client = WsAsyncTmq::from_dsn("ws://localhost:6041/").await?;
    // assert_eq!(client.exec("drop database if exists abc_a").await?, 0);
    // assert_eq!(client.exec("create database abc_a").await?, 0);
    // assert_eq!(
    //     client
    //         .exec("create table abc_a.tb1(ts timestamp, v int)")
    //         .await?,
    //     0
    // );
    // assert_eq!(
    //     client
    //         .exec("insert into abc_a.tb1 values(1655793421375, 1)")
    //         .await?,
    //     1
    // );

    // // let mut rs = client.s_query("select * from abc_a.tb1").unwrap().unwrap();
    // let mut rs = client.query("select * from abc_a.tb1").await?;

    // #[derive(Debug, serde::Deserialize)]
    // #[allow(dead_code)]
    // struct A {
    //     ts: String,
    //     v: i32,
    // }

    // let values: Vec<A> = rs.deserialize_stream().try_collect().await?;

    // dbg!(values);

    // assert_eq!(client.exec("drop database abc_a").await?, 0);
    Ok(())
}
