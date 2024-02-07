use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mode: Option<i32>,
}

impl WsConnReq {
    #[cfg(test)]
    pub fn new(
        user: impl Into<String>,
        password: impl Into<String>,
        db: impl Into<String>,
    ) -> Self {
        Self {
            user: Some(user.into()),
            password: Some(password.into()),
            db: Some(db.into()),
            mode: None,
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
    Insert {
        protocol: u8,
        precision: String,
        data: String,
        ttl: Option<i32>,
        req_id: Option<ReqId>,
    },
}

impl WsSend {
    pub(crate) fn req_id(&self) -> ReqId {
        match self {
            WsSend::Conn { req_id, .. } => *req_id,
            WsSend::Insert { req_id, .. } => req_id.unwrap_or(0),
            _ => unreachable!(),
        }
    }
}

unsafe impl Send for WsSend {}
unsafe impl Sync for WsSend {}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_serde_send() {
        let s = WsSend::Conn {
            req_id: 1,
            req: WsConnReq::new("root", "taosdata", "db"),
        };
        let v = serde_json::to_value(s).unwrap();
        let j = serde_json::json!({
            "action": "conn",
            "args": {
                "req_id": 1,
                "user": "root",
                "password": "taosdata",
                "db": "db"
            }
        });
        assert_eq!(v, j);
    }
}

#[serde_as]
#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default)]
pub struct InsertResp {
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
    Insert(InsertResp),
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
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct WsRecv {
    #[serde(default)]
    pub code: i32,
    #[serde_as(as = "NoneAsEmptyString")]
    #[serde(default)]
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

pub(crate) trait ToMessage: Serialize {
    // #[cfg(feature = "async")]
    fn to_tungstenite_msg(&self) -> tokio_tungstenite::tungstenite::Message {
        tokio_tungstenite::tungstenite::Message::Text(serde_json::to_string(self).unwrap())
    }
    fn to_msg(&self) -> ws_tool::Message<bytes::Bytes> {
        ws_tool::Message{
            code: ws_tool::frame::OpCode::Text,
            data: serde_json::to_vec(self).unwrap().into(),
            close_code: None,
        }
    }
}

impl ToMessage for WsSend {}
