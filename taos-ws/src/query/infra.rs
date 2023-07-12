use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;
use taos_query::common::{Precision, Ty};
use taos_query::prelude::RawError;

pub type ReqId = u64;

/// Type for result ID.
pub type ResId = u64;

#[serde_as]
#[derive(Debug, Serialize, Default, Clone)]
pub struct WsConnReq {
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
    #[serde_as(as = "NoneAsEmptyString")]
    pub(crate) db: Option<String>,
}

impl WsConnReq {
    #[cfg(test)]
    pub fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            user: Some(user.into()),
            password: Some(password.into()),
            db: None,
        }
    }
    // pub fn with_database(mut self, db: impl Into<String>) -> Self {
    //     self.db = Some(db.into());
    //     self
    // }
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct WsResArgs {
    pub req_id: ReqId,
    pub id: ResId,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub enum WsSend {
    Version,
    // Pong(Vec<u8>),
    Conn {
        req_id: ReqId,
        #[serde(flatten)]
        req: WsConnReq,
    },
    Query {
        req_id: ReqId,
        sql: String,
    },
    Fetch(WsResArgs),
    FetchBlock(WsResArgs),
    Binary(Vec<u8>),
    FreeResult(WsResArgs),
}

impl WsSend {
    pub(crate) fn req_id(&self) -> ReqId {
        match self {
            WsSend::Conn { req_id, req: _ } => *req_id,
            WsSend::Query { req_id, sql: _ } => *req_id,
            WsSend::Fetch(args) => args.req_id,
            WsSend::FetchBlock(args) => args.req_id,
            WsSend::FreeResult(args) => args.req_id,
            WsSend::Binary(bytes) => unsafe { *(bytes.as_ptr() as *const u64) as _ },
            _ => unreachable!(),
        }
    }
}

unsafe impl Send for WsSend {}
unsafe impl Sync for WsSend {}

#[test]
fn test_serde_send() {
    let s = WsSend::Conn {
        req_id: 1,
        req: WsConnReq::new("root", "taosdata"),
    };
    let v = serde_json::to_value(s).unwrap();
    let j = serde_json::json!({
        "action": "conn",
        "args": {
            "req_id": 1,
            "user": "root",
            "password": "taosdata",
            "db": ""
        }
    });
    assert_eq!(v, j);
}

#[derive(Debug, Serialize)]
pub struct WsFetchArgs {
    req_id: ReqId,
    id: ResId,
}

#[serde_as]
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub struct WsQueryResp {
    pub id: ResId,
    pub is_update: bool,
    pub affected_rows: usize,
    pub fields_count: usize,
    pub fields_names: Option<Vec<String>>,
    pub fields_types: Option<Vec<Ty>>,
    pub fields_lengths: Option<Vec<u32>>,
    pub precision: Precision,
    #[serde_as(as = "serde_with::DurationNanoSeconds")]
    pub timing: Duration,
}

#[serde_as]
#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default)]
pub struct WsFetchResp {
    pub id: ResId,
    pub completed: bool,
    pub lengths: Option<Vec<u32>>,
    pub rows: usize,
    #[serde_as(as = "serde_with::DurationNanoSeconds")]
    pub timing: Duration,
}

#[derive(Debug, Deserialize, Clone)]
#[serde_as]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum WsRecvData {
    Conn,
    Version {
        version: String,
    },
    Query(WsQueryResp),
    Fetch(WsFetchResp),
    /// Will only produced by error
    FetchBlock,
    Block {
        #[serde(default)]
        #[serde_as(as = "serde_with::DurationNanoSeconds")]
        timing: Duration,
        raw: Vec<u8>,
    },
    BlockV2 {
        #[serde(default)]
        #[serde_as(as = "serde_with::DurationNanoSeconds")]
        timing: Duration,
        raw: Vec<u8>,
    },
    WriteMeta,
    WriteRaw,
    WriteRawBlock,
    WriteRawBlockWithFields,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct WsRecv {
    pub code: i32,
    #[serde_as(as = "NoneAsEmptyString")]
    pub message: Option<String>,
    #[serde(default)]
    pub req_id: ReqId,
    #[serde(flatten)]
    pub data: WsRecvData,
}

impl WsRecv {
    pub(crate) fn ok(self) -> (ReqId, WsRecvData, Result<(), RawError>) {
        (
            self.req_id,
            self.data,
            if self.code == 0 {
                Ok(())
            } else {
                Err(RawError::new(self.code, self.message.unwrap_or_default()))
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
    let d: WsRecv = serde_json::from_str(json).unwrap();
    dbg!(d);
}

pub(crate) trait ToMessage: Serialize {
    // #[cfg(feature = "async")]
    fn to_msg(&self) -> tokio_tungstenite::tungstenite::Message {
        tokio_tungstenite::tungstenite::Message::Text(serde_json::to_string(self).unwrap())
    }
}

impl ToMessage for WsSend {}

#[cfg(test)]
mod tests {

    // use websocket::ClientBuilder;

    use crate::*;

    #[test]
    fn dsn_error() {
        let _ = TaosBuilder::from_dsn("").unwrap_err();
    }
}
