use std::any::Any;
use std::ops::Deref;
use std::str::FromStr;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;

use taos_error::Error;
use taos_query::common::Precision;
use taos_query::common::Ty;

use crate::infra::ToMessage;
use crate::infra::WsConnReq;

pub type ReqId = u64;

/// Type for result ID.
pub type ResId = u64;

pub type ConsumerId = u64;

pub type MessageId = u64;

#[derive(Debug, Serialize, Default, Clone)]
pub struct MessageArgs {
    pub(crate) req_id: ReqId,
    pub(crate) message_id: MessageId,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(i32)]
pub enum MessageType {
    Invalid = 0,
    Data = 1,
    Meta,
}

impl Default for MessageType {
    fn default() -> Self {
        Self::Invalid
    }
}

#[serde_as]
#[derive(Debug, Serialize, Default, Clone)]
pub struct TmqInit {
    pub group_id: String,
    pub client_id: Option<String>,
    pub offset_reset: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TmqArgs {
    pub req_id: ReqId,
    #[serde(default)]
    pub message_id: ConsumerId,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub enum TmqSend {
    Subscribe {
        req_id: ReqId,
        #[serde(flatten)]
        conn: WsConnReq,
        #[serde(flatten)]
        req: TmqInit,
        topics: Vec<String>,
    },
    Poll {
        req_id: ReqId,
        blocking_time: i64,
    },
    FetchJsonMeta(MessageArgs),
    FetchRawMeta(MessageArgs),
    Fetch(MessageArgs),
    FetchBlock(MessageArgs),
    Commit(MessageArgs),
    Unsubscribe {
        req_id: ReqId,
    },
    Close,
}

impl TmqSend {
    pub fn req_id(&self) -> ReqId {
        match self {
            TmqSend::Subscribe {
                req_id,
                conn,
                req,
                topics,
            } => *req_id,
            TmqSend::Poll {
                req_id,
                blocking_time,
            } => *req_id,
            TmqSend::FetchJsonMeta(args) => args.req_id,
            TmqSend::FetchRawMeta(args) => args.req_id,
            TmqSend::Fetch(args) => args.req_id,
            TmqSend::FetchBlock(args) => args.req_id,
            TmqSend::Commit(args) => args.req_id,
            TmqSend::Unsubscribe { req_id } => *req_id,
            TmqSend::Close => unreachable!(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WsFetchArgs {
    req_id: ReqId,
    id: ResId,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub struct TmqPoll {
    pub message_id: MessageId,
    pub database: String,
    pub have_message: bool,
    pub topic: String,
    pub vgroup_id: u64,
    pub message_type: MessageType,
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
    JsonMeta(Value),
    RawMeta(Vec<u8>),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum TmqRecvData {
    Subscribe,
    Poll(TmqPoll),
    Fetch(TmqFetch),
    FetchJsonMeta {
        data: Value,
    },
    FetchRawMeta {
        #[serde(skip)]
        meta: Bytes,
    },
    Block(Vec<u32>),
    Commit,
    Close,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct TmqRecv {
    pub code: i32,
    #[serde_as(as = "NoneAsEmptyString")]
    pub message: Option<String>,
    pub req_id: ReqId,
    // #[serde(flatten)]
    // pub args: TmqArgs,
    #[serde(flatten)]
    pub data: TmqRecvData,
}

impl TmqRecv {
    pub(crate) fn ok(self) -> (ReqId, TmqRecvData, Result<(), taos_error::Error>) {
        (
            self.req_id,
            self.data,
            if self.code == 0 {
                Ok(())
            } else {
                Err(taos_error::Error::new(
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

/// Type for result ID.
pub type StmtId = u64;

#[derive(Debug, Serialize)]
pub struct StmtInit {
    req_id: ReqId,
}
#[derive(Debug, Serialize)]
pub struct StmtPrepare {
    req_id: ReqId,
    stmt_id: StmtId,
    sql: String,
}
#[derive(Debug, Serialize)]
pub struct StmtSetTableName {
    req_id: ReqId,
    stmt_id: StmtId,
    name: String,
}

#[derive(Debug, Serialize)]
pub struct StmtSetTags {
    req_id: ReqId,
    stmt_id: StmtId,
    tags: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct StmtBind {
    req_id: ReqId,
    stmt_id: StmtId,
    columns: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct StmtAddBatch {
    req_id: ReqId,
    stmt_id: StmtId,
}
#[derive(Debug, Serialize)]
pub struct StmtExec {
    req_id: ReqId,
    stmt_id: StmtId,
}

#[derive(Debug, Serialize)]
pub struct StmtClose {
    req_id: ReqId,
    stmt_id: StmtId,
}
// #[derive(Debug, Serialize)]
// #[serde(untagged)]
// pub enum WsSendData {
//     Conn(WsConnReq),
//     Init(StmtInit),
//     Prepare(StmtPrepare),
//     SetTableName(StmtSetTableName),
//     SetTags(StmtSetTags),
//     Bind(StmtBind),
//     AddBatch(StmtAddBatch),
//     Exec(StmtExec),
//     Close(),
// }

#[derive(Debug, Serialize, Clone, Copy)]
pub struct StmtArgs {
    pub req_id: ReqId,
    pub stmt_id: StmtId,
}
#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub enum StmtSend {
    Conn {
        req_id: ReqId,
        #[serde(flatten)]
        req: WsConnReq,
    },
    Init {
        req_id: ReqId,
    },
    Prepare {
        #[serde(flatten)]
        args: StmtArgs,
        sql: String,
    },
    SetTableName {
        #[serde(flatten)]
        args: StmtArgs,
        name: String,
    },
    SetTags {
        #[serde(flatten)]
        args: StmtArgs,
        tags: Vec<Value>,
    },
    Bind {
        #[serde(flatten)]
        args: StmtArgs,
        columns: Vec<Value>,
    },
    AddBatch(StmtArgs),
    Exec(StmtArgs),
    Close(StmtArgs),
}

impl ToMessage for StmtSend {}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum StmtRecvData {
    Conn,
    Init {
        #[serde(default)]
        stmt_id: StmtId,
    },
    Prepare {
        #[serde(default)]
        stmt_id: StmtId,
    },
    SetTableName {
        #[serde(default)]
        stmt_id: StmtId,
    },
    SetTags {
        #[serde(default)]
        stmt_id: StmtId,
    },
    Bind {
        #[serde(default)]
        stmt_id: StmtId,
    },
    AddBatch {
        #[serde(default)]
        stmt_id: StmtId,
    },
    Exec {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        affected: usize,
    },
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct StmtRecv {
    pub code: i32,
    #[serde_as(as = "NoneAsEmptyString")]
    pub message: Option<String>,
    pub req_id: ReqId,
    #[serde(flatten)]
    pub data: StmtRecvData,
}

#[derive(Debug)]
pub enum StmtOk {
    Conn(Result<(), Error>),
    Init(ReqId, Result<StmtId, Error>),
    Stmt(StmtId, Result<Option<usize>, Error>),
}

impl StmtRecv {
    pub(crate) fn ok(self) -> StmtOk {
        macro_rules! _e {
            () => {
                Err(taos_error::Error::new(
                    if self.code == 65536 { -1 } else { self.code },
                    self.message.unwrap_or_default(),
                ))
            };
        }
        match self.data {
            StmtRecvData::Conn => StmtOk::Conn({
                if self.code == 0 {
                    Ok(())
                } else {
                    _e!()
                }
            }),
            StmtRecvData::Init { stmt_id } => StmtOk::Init(self.req_id, {
                if self.code == 0 {
                    Ok(stmt_id)
                } else {
                    _e!()
                }
            }),
            StmtRecvData::Prepare { stmt_id }
            | StmtRecvData::SetTableName { stmt_id }
            | StmtRecvData::SetTags { stmt_id }
            | StmtRecvData::Bind { stmt_id }
            | StmtRecvData::AddBatch { stmt_id } => StmtOk::Stmt(stmt_id, {
                if self.code == 0 {
                    Ok(None)
                } else {
                    _e!()
                }
            }),
            StmtRecvData::Exec { stmt_id, affected } => StmtOk::Stmt(stmt_id, {
                if self.code == 0 {
                    Ok(Some(affected))
                } else {
                    _e!()
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Ok;

    use super::*;

    #[test]
    fn stmt() -> anyhow::Result<()> {
        Ok(())
    }
}
