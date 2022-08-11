use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;
use taos_query::common::{Precision, Ty};

use crate::infra::ToMessage;
use crate::WsInfo;

pub type ReqId = u64;

/// Type for result ID.
pub type ResId = u64;

pub type ConsumerId = u64;

#[serde_as]
#[derive(Debug, Serialize, Default, Clone)]
pub struct TmqInit {
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
    #[serde_as(as = "NoneAsEmptyString")]
    pub(crate) db: Option<String>,
    pub group_id: String,
    pub client_id: Option<String>,
    pub offset_reset: Option<String>,
}

impl TmqInit {
    pub fn new(
        user: impl Into<String>,
        password: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            user: Some(user.into()),
            password: Some(password.into()),
            db: None,
            group_id: group_id.into(),
            client_id: None,
            offset_reset: None,
        }
    }
    pub fn with_database(mut self, db: impl Into<String>) -> Self {
        self.db = Some(db.into());
        self
    }
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }
    pub fn with_offset_reset(mut self, offset: impl Into<String>) -> Self {
        self.offset_reset = Some(offset.into());
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TmqArgs {
    pub req_id: ReqId,
    pub consumer_id: ConsumerId,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub enum TmqSend {
    Init {
        req_id: ReqId,
        #[serde(flatten)]
        req: TmqInit,
    },
    Subscribe {
        #[serde(flatten)]
        args: TmqArgs,
        topics: Vec<String>,
    },
    Poll {
        #[serde(flatten)]
        args: TmqArgs,
        blocking_time: u64,
    },
    Fetch(TmqArgs),
    FetchBlock(TmqArgs),
    Commit(TmqArgs),
    Unsubscribe(TmqArgs),
    Close(TmqArgs),
}

#[derive(Debug, Serialize)]
pub struct WsFetchArgs {
    req_id: ReqId,
    id: ResId,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub struct TmqPoll {
    #[serde(default)]
    pub database: Option<String>,
    pub have_message: bool,
    pub topic: String,
    pub vgroup_id: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TmqFetch {
    pub completed: bool,
    pub table_name: Option<String>,
    pub fields_count: usize,
    pub fields_names: Option<Vec<String>>,
    pub fields_types: Option<Vec<Ty>>,
    pub fields_lengths: Option<Vec<u32>>,
    pub precision: Precision,
    pub rows: usize,
}

#[derive(Debug, Clone)]
pub enum TmqMsgData {
    Fetch(TmqFetch),
    Block(Vec<u8>),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum TmqRecvData {
    Init,
    Subscribe,
    Unsubscribe,
    Commit,
    Poll(TmqPoll),
    Fetch(TmqFetch),
    Block(Vec<u32>),
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct TmqRecv {
    pub code: i32,
    #[serde_as(as = "NoneAsEmptyString")]
    pub message: Option<String>,
    #[serde(flatten)]
    pub args: TmqArgs,
    #[serde(flatten)]
    pub data: TmqRecvData,
}

impl TmqRecv {
    pub(crate) fn ok(self) -> (TmqArgs, TmqRecvData, Result<(), RawError>) {
        (
            self.args,
            self.data,
            if self.code == 0 {
                Ok(())
            } else {
                Err(RawError::new(
                    self.code,
                    self.message.unwrap_or_default(),
                ))
            },
        )
    }
}

#[test]
fn test_serde_recv_data() {
    let json = r#"{
        "code": 0,
        "message": "",
        "action": "conn",
        "req_id": 1
    }"#;
    let d: TmqRecv = serde_json::from_str(&json).unwrap();
    dbg!(d);
}

impl ToMessage for TmqSend {}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use crate::infra::ToMessage;
    use crate::*;

    use super::*;

    #[test]
    fn dsn_error() {
        Ws::from_dsn("").unwrap_err();
    }

    #[test]
    fn test_connect_sequential() -> anyhow::Result<()> {
        let ws_info = WsInfo::from_dsn("ws://localhost:6041/stmt?group.id=2")?;
        let mut ws = ClientBuilder::new(&ws_info.to_tmq_url())?;

        let mut client = ws.connect_insecure()?;

        let init = TmqSend::Init {
            req_id: 0,
            req: ws_info.to_tmq_init()?,
        };
        client.send_message(&init.to_message()).unwrap();

        println!("receiving");

        let recv = client.recv_message()?;
        println!("received");
        let args = match recv {
            websocket::OwnedMessage::Text(text) => {
                let v: TmqRecv = serde_json::from_str(&text).unwrap();
                let (args, data, ok) = dbg!(v.ok());
                let _ = ok?;
                args
            }
            _ => unreachable!(),
        };

        let query = TmqSend::Subscribe {
            args,
            topics: vec!["stmt".to_string()],
        };

        client.send_message(&query.to_message()).unwrap();

        println!("receiving");

        let _ = loop {
            let recv = client.recv_message()?;
            println!("received");
            match recv {
                websocket::OwnedMessage::Text(text) => {
                    let j: serde_json::Value = serde_json::from_str(&text).unwrap();
                    dbg!(j);
                    let v: TmqRecv = serde_json::from_str(&text).unwrap();
                    dbg!(&v);
                    let (args, data, ok) = v.ok();
                    let _ = ok.unwrap();
                    match data {
                        TmqRecvData::Subscribe => break,
                        _ => unreachable!(),
                    }
                }
                websocket::OwnedMessage::Binary(_) => todo!(),
                websocket::OwnedMessage::Close(_) => todo!(),
                websocket::OwnedMessage::Ping(_) => todo!(),
                websocket::OwnedMessage::Pong(_) => todo!(),
            }
        };

        let poll = TmqSend::Poll {
            args,
            blocking_time: 1000,
        };
        client.send_message(&poll.to_message()).unwrap();
        println!("polling");

        let data = loop {
            let recv = client.recv_message()?;
            println!("received");
            match recv {
                websocket::OwnedMessage::Text(text) => {
                    let j: serde_json::Value = serde_json::from_str(&text).unwrap();
                    dbg!(j);
                    let v: TmqRecv = serde_json::from_str(&text).unwrap();
                    dbg!(&v);
                    let (args, data, ok) = v.ok();
                    let _ = ok.unwrap();
                    match data {
                        TmqRecvData::Poll(poll) => break poll,
                        _ => unreachable!(),
                    }
                }
                websocket::OwnedMessage::Binary(_) => todo!(),
                websocket::OwnedMessage::Close(_) => todo!(),
                websocket::OwnedMessage::Ping(_) => todo!(),
                websocket::OwnedMessage::Pong(_) => todo!(),
            }
        };
        dbg!(&data);

        if !data.have_message {
            return Ok(());
        }

        let fetch = TmqSend::Fetch(args);
        client.send_message(&fetch.to_message()).unwrap();
        println!("fetching");
        let data = loop {
            let recv = client.recv_message()?;
            println!("received");
            match recv {
                websocket::OwnedMessage::Text(text) => {
                    let j: serde_json::Value = serde_json::from_str(&text).unwrap();
                    dbg!(j);
                    let v: TmqRecv = serde_json::from_str(&text).unwrap();
                    dbg!(&v);
                    let (args, data, ok) = v.ok();
                    let _ = ok.unwrap();
                    match data {
                        TmqRecvData::Fetch(poll) => break poll,
                        _ => unreachable!(),
                    }
                }
                websocket::OwnedMessage::Binary(_) => todo!(),
                websocket::OwnedMessage::Close(_) => todo!(),
                websocket::OwnedMessage::Ping(_) => todo!(),
                websocket::OwnedMessage::Pong(_) => todo!(),
            }
        };
        dbg!(&data);

        let send = TmqSend::FetchBlock(args);
        client.send_message(&send.to_message()).unwrap();
        println!("fetching");
        let block = loop {
            let recv = client.recv_message()?;
            println!("received");
            match recv {
                websocket::OwnedMessage::Binary(bytes) => {
                    let mut block = RawBlock::new(bytes);
                    block
                        .with_cols(data.fields_count)
                        .with_rows(data.rows)
                        .with_precision(data.precision);
                    let mut block = Block::from_raw_block(block);
                    if let Some(name) = data.table_name {
                        block.with_table_name(&name);
                    }
                    break block;
                }
                _ => todo!(),
            }
        };
        dbg!(&block);

        // let id = query_resp.id;
        // let req_id = 2;
        // let res_args = TmqArgs {
        //     req_id,
        //     consumer_id: id,
        // };

        // assert!(!query_resp.is_update);

        // loop {
        //     // Now we can fetch blocks.

        //     // 1. call fetch
        //     let fetch = TmqSend::Fetch(res_args);
        //     client.send_message(&fetch.to_message()).unwrap();
        //     let fetch_resp = match client.recv_message()? {
        //         websocket::OwnedMessage::Text(text) => {
        //             let j: serde_json::Value = serde_json::from_str(&text).unwrap();
        //             dbg!(j);
        //             let v: TmqRecv = serde_json::from_str(&text).unwrap();
        //             dbg!(&v);
        //             match v.data {
        //                 TmqRecvData::Conn => unreachable!(),
        //                 TmqRecvData::Query(_) => unreachable!(),
        //                 TmqRecvData::Fetch(resp) => resp,
        //                 TmqRecvData::Block(_) => todo!(),
        //             }
        //         }
        //         websocket::OwnedMessage::Binary(_) => todo!(),
        //         websocket::OwnedMessage::Close(_) => todo!(),
        //         websocket::OwnedMessage::Ping(_) => todo!(),
        //         websocket::OwnedMessage::Pong(_) => todo!(),
        //     };
        //     dbg!(&fetch_resp);

        //     if fetch_resp.completed {
        //         break;
        //     }

        //     let fetch_block = TmqSend::FetchBlock(res_args);
        //     client.send_message(&fetch_block.to_message()).unwrap();

        //     let mut raw = match client.recv_message()? {
        //         websocket::OwnedMessage::Binary(bytes) => {
        //             use taos_query::util::InlinableRead;

        //             dbg!(&bytes[0..16]);

        //             dbg!(bytes.len());

        //             let mut slice = bytes.as_slice();
        //             let _ = slice.read_u64().unwrap();
        //             slice.read_inlinable::<RawBlock>().unwrap()
        //         }
        //         _ => unreachable!(),
        //     };

        //     raw.with_rows(fetch_resp.rows)
        //         .with_cols(query_resp.fields_count)
        //         .with_precision(query_resp.precision);
        //     dbg!(&raw);

        //     for row in 0..raw.nrows() {
        //         for col in 0..raw.ncols() {
        //             let v = unsafe { raw.get_unchecked(row, col) };
        //             println!("({}, {}): {}", row, col, v);
        //         }
        //     }
        // }

        let close = TmqSend::Close(args);
        client.send_message(&close.to_message()).unwrap();

        Ok(())
    }
}
