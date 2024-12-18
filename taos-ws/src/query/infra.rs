use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, NoneAsEmptyString};
use taos_query::common::{Precision, Ty};
use taos_query::prelude::RawError;

pub(crate) type ReqId = u64;
pub(crate) type StmtId = u64;

/// Type for result ID.
pub(crate) type ResId = u64;

#[serde_as]
#[derive(Debug, Serialize, Default, Clone)]
pub(crate) struct WsConnReq {
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
    #[serde_as(as = "NoneAsEmptyString")]
    pub(crate) db: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mode: Option<u32>,
}

impl WsConnReq {
    #[cfg(test)]
    fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            user: Some(user.into()),
            password: Some(password.into()),
            db: None,
            mode: None,
        }
    }

    // pub fn with_database(mut self, db: impl Into<String>) -> Self {
    //     self.db = Some(db.into());
    //     self
    // }
}

#[derive(Debug, Serialize, Clone, Copy)]
pub(crate) struct WsResArgs {
    pub req_id: ReqId,
    pub id: ResId,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub(crate) enum WsSend {
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
    Query {
        req_id: ReqId,
        sql: String,
    },
    Fetch(WsResArgs),
    FetchBlock(WsResArgs),
    Binary(Vec<u8>),
    FreeResult(WsResArgs),
    Stmt2Init {
        req_id: ReqId,
        single_stb_insert: bool,
        single_table_bind_once: bool,
    },
    Stmt2Prepare {
        req_id: ReqId,
        stmt_id: StmtId,
        sql: String,
        get_fields: bool,
    },
    Stmt2Exec {
        req_id: ReqId,
        stmt_id: StmtId,
    },
    Stmt2Result {
        req_id: ReqId,
        stmt_id: StmtId,
    },
    Stmt2Close {
        req_id: ReqId,
        stmt_id: StmtId,
    },
}

impl WsSend {
    pub(crate) fn req_id(&self) -> ReqId {
        match self {
            WsSend::Conn { req_id, .. }
            | WsSend::Query { req_id, .. }
            | WsSend::Stmt2Init { req_id, .. }
            | WsSend::Stmt2Prepare { req_id, .. }
            | WsSend::Stmt2Exec { req_id, .. }
            | WsSend::Stmt2Result { req_id, .. }
            | WsSend::Stmt2Close { req_id, .. } => *req_id,
            WsSend::Insert { req_id, .. } => req_id.unwrap_or(0),
            WsSend::Fetch(args) | WsSend::FetchBlock(args) | WsSend::FreeResult(args) => {
                args.req_id
            }
            WsSend::Binary(bytes) => unsafe { *(bytes.as_ptr() as *const u64) as _ },
            WsSend::Version => unreachable!(),
        }
    }
}

unsafe impl Send for WsSend {}
unsafe impl Sync for WsSend {}

// #[derive(Debug, Serialize)]
// pub struct WsFetchArgs {
//     req_id: ReqId,
//     id: ResId,
// }

#[serde_as]
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub(crate) struct WsQueryResp {
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
pub(crate) struct InsertResp {
    #[serde_as(as = "serde_with::DurationNanoSeconds")]
    pub timing: Duration,
}

#[serde_as]
#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default)]
pub(crate) struct WsFetchResp {
    pub id: ResId,
    pub completed: bool,
    pub lengths: Option<Vec<u32>>,
    pub rows: usize,
    #[serde_as(as = "serde_with::DurationNanoSeconds")]
    pub timing: Duration,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub(crate) struct WsRecv {
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
                if self.message.as_deref() == Some("success") {
                    Err(RawError::from_code(self.code))
                } else {
                    Err(RawError::new(self.code, self.message.unwrap_or_default()))
                }
            },
        )
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
#[serde_as]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub(crate) enum WsRecvData {
    Conn,
    Version {
        version: String,
    },
    Insert(InsertResp),

    #[serde(alias = "binary_query")]
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
    BlockNew {
        #[allow(dead_code)]
        block_version: u16,
        #[serde(default)]
        #[serde_as(as = "serde_with::DurationNanoSeconds")]
        timing: Duration,
        #[allow(dead_code)]
        block_req_id: ReqId,
        block_code: u32,
        block_message: String,
        finished: bool,
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
    Stmt2Init {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        timing: u64,
    },
    Stmt2Prepare {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        is_insert: bool,
        #[serde(default)]
        fields: Option<Vec<Stmt2Field>>,
        #[serde(default)]
        fields_count: usize,
        #[serde(default)]
        timing: u64,
    },
    Stmt2Bind {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        timing: u64,
    },
    Stmt2Exec {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        affected: usize,
        #[serde(default)]
        timing: u64,
    },
    Stmt2Result {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        id: u64,
        #[serde(default)]
        fields_count: u64,
        #[serde(default)]
        fields_names: Vec<String>,
        #[serde(default)]
        fields_types: Vec<Ty>,
        #[serde(default)]
        fields_lengths: Vec<u64>,
        #[serde(default)]
        precision: Precision,
        #[serde(default)]
        timing: u64,
    },
    Stmt2Close {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        timing: u64,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Stmt2Field {
    pub name: String,
    pub field_type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
    pub bind_type: BindType,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BindType {
    Column,
    Tag,
    TableName,
}

impl<'de> Deserialize<'de> for BindType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BindTypeVisitor;

        impl<'de> serde::de::Visitor<'de> for BindTypeVisitor {
            type Value = BindType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a valid number for BindType")
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(match v {
                    1 => BindType::Column,
                    2 => BindType::Tag,
                    4 => BindType::TableName,
                    _ => return Err(E::custom(format!("Invalid bind type: {}", v))),
                })
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_u8(v as _)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_u8(v as _)
            }
        }

        deserializer.deserialize_any(BindTypeVisitor)
    }
}

pub trait ToMessage: Serialize {
    fn to_msg(&self) -> tokio_tungstenite::tungstenite::Message {
        tokio_tungstenite::tungstenite::Message::Text(serde_json::to_string(self).unwrap())
    }
}

impl ToMessage for WsSend {}

#[cfg(test)]
mod tests {
    use crate::{
        query::{
            infra::{WsRecv, WsSend},
            WsConnReq,
        },
        TaosBuilder,
    };

    use super::BindType;

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
                "db": "",
            }
        });
        assert_eq!(v, j);
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

    #[test]
    fn dsn_error() {
        let _ = TaosBuilder::from_dsn("").unwrap_err();
    }

    #[test]
    fn test_bind_type_deserialize() {
        let valid_cases = vec![
            (1, BindType::Column),
            (2, BindType::Tag),
            (4, BindType::TableName),
        ];
        for (val, expected) in valid_cases {
            let res: BindType = serde_json::from_value(serde_json::json!(val as i64)).unwrap();
            assert_eq!(res, expected);

            let res: BindType = serde_json::from_value(serde_json::json!(val as u64)).unwrap();
            assert_eq!(res, expected);
        }

        let invalid_cases = vec![0, 3, 255];
        for val in invalid_cases {
            let res: Result<BindType, _> = serde_json::from_value(serde_json::json!(val as i64));
            assert!(res.is_err());

            let res: Result<BindType, _> = serde_json::from_value(serde_json::json!(val as u64));
            assert!(res.is_err());
        }

        let res: Result<BindType, _> = serde_json::from_value(serde_json::json!("invalid"));
        assert!(res.is_err());
    }
}
