use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;
use taos_query::common::{Precision, Ty};
use taos_query::prelude::RawError as Error;

use crate::query::infra::{ToMessage, WsConnReq};
use crate::stmt2::{ReqId, Stmt2Args, StmtId};

#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub enum Stmt2Send {
    Conn {
        req_id: ReqId,
        #[serde(flatten)]
        req: WsConnReq,
    },
    Stmt2Init {
        req_id: ReqId,
        single_stb_insert: bool,
        single_table_bind_once: bool,
    },
    Stmt2Prepare {
        #[serde(flatten)]
        args: Stmt2Args,
        sql: String,
        get_fields: bool,
    },
    Stmt2BindParam,
    Stmt2Exec(Stmt2Args),
    Stmt2GetFields {
        #[serde(flatten)]
        args: Stmt2Args,
        field_types: Vec<i8>,
    },
    Stmt2Result(Stmt2Args),
    Stmt2Close(Stmt2Args),
}

impl ToMessage for Stmt2Send {}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct Stmt2Recv {
    pub code: i32,
    #[serde_as(as = "NoneAsEmptyString")]
    pub message: Option<String>,
    pub req_id: ReqId,
    pub stmt_id: StmtId,
    pub timing: u64,
    #[serde(flatten)]
    pub data: Stmt2RecvData,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum Stmt2RecvData {
    Conn,
    Stmt2Init,
    Stmt2Prepare {
        is_insert: bool,
        fields: Option<Vec<String>>,
        field_count: u64,
    },
    Stmt2BindParam,
    Stmt2Exec {
        #[serde(default)]
        affected: usize,
    },
    Stmt2GetFields {
        table_count: u32,
        query_count: u32,
        col_fields: Option<Vec<i8>>,
        tag_fields: Option<Vec<i8>>,
    },
    Stmt2Result {
        result_id: u64,
        fields_count: u64,
        fields_names: Option<Vec<String>>,
        fields_types: Option<Vec<Ty>>,
        fields_lengths: Option<Vec<u64>>,
        precision: Precision,
    },
    Stmt2Close,
}

#[derive(Debug)]
pub enum Stmt2Ok {
    Conn(Result<(), Error>),
    Stmt2Init(ReqId, Result<StmtId, Error>),
    Stmt2BindParam(Result<(), Error>),
    Stmt2Res(StmtId, Result<Stmt2Result, Error>),
    Stmt2NumRes(StmtId, Result<Option<usize>, Error>),
    Stmt2PrepareRes(StmtId, Result<Stmt2PrepareResult, Error>),
    Stmt2Fields(StmtId, Result<Stmt2Fields, Error>),
    Stmt2Close(Result<(), Error>),
}

#[derive(Debug, Deserialize)]
pub struct Stmt2Result {
    pub result_id: u64,
    pub fields_count: u64,
    pub fields_names: Option<Vec<String>>,
    pub fields_types: Option<Vec<Ty>>,
    pub fields_lengths: Option<Vec<u64>>,
    pub precision: Precision,
}

#[derive(Debug, Deserialize)]
pub struct Stmt2PrepareResult {
    pub timing: u64,
    pub stmt_id: StmtId,
    pub is_insert: bool,
    pub fields: Option<Vec<String>>,
    pub field_count: u64,
}

#[derive(Debug, Deserialize)]
pub struct Stmt2Fields {
    table_count: u32,
    query_count: u32,
    col_fields: Option<Vec<i8>>,
    tag_fields: Option<Vec<i8>>,
}

impl Stmt2Recv {
    pub(crate) fn ok(self) -> Stmt2Ok {
        macro_rules! _e {
            () => {
                Err(Error::new(
                    if self.code == 65536 { -1 } else { self.code },
                    self.message.unwrap_or_default(),
                ))
            };
        }
        match self.data {
            Stmt2RecvData::Conn => Stmt2Ok::Conn({
                if self.code == 0 {
                    Ok(())
                } else {
                    _e!()
                }
            }),
            Stmt2RecvData::Stmt2Init => Stmt2Ok::Stmt2Init(
                self.req_id,
                if self.code == 0 {
                    Ok(self.stmt_id)
                } else {
                    _e!()
                },
            ),
            Stmt2RecvData::Stmt2Prepare {
                is_insert,
                fields,
                field_count,
            } => Stmt2Ok::Stmt2PrepareRes(
                self.stmt_id,
                if self.code == 0 {
                    Ok(Stmt2PrepareResult {
                        timing: self.timing,
                        stmt_id: self.stmt_id,
                        is_insert,
                        fields,
                        field_count,
                    })
                } else {
                    _e!()
                },
            ),
            Stmt2RecvData::Stmt2BindParam => Stmt2Ok::Stmt2BindParam({
                if self.code == 0 {
                    Ok(())
                } else {
                    _e!()
                }
            }),
            Stmt2RecvData::Stmt2Exec { affected } => Stmt2Ok::Stmt2NumRes(
                self.stmt_id,
                if self.code == 0 {
                    Ok(Some(affected))
                } else {
                    _e!()
                },
            ),
            Stmt2RecvData::Stmt2GetFields {
                table_count,
                query_count,
                col_fields,
                tag_fields,
            } => Stmt2Ok::Stmt2Fields(
                self.stmt_id,
                if self.code == 0 {
                    Ok(Stmt2Fields {
                        table_count,
                        query_count,
                        col_fields,
                        tag_fields,
                    })
                } else {
                    _e!()
                },
            ),
            Stmt2RecvData::Stmt2Result {
                result_id,
                fields_count,
                fields_names,
                fields_types,
                fields_lengths,
                precision,
            } => Stmt2Ok::Stmt2Res(
                self.stmt_id,
                if self.code == 0 {
                    Ok(Stmt2Result {
                        result_id,
                        fields_count,
                        fields_names,
                        fields_types: fields_types
                            .map(|v| v.into_iter().map(|v| v.into()).collect()),
                        fields_lengths,
                        precision: precision.into(),
                    })
                } else {
                    _e!()
                },
            ),
            Stmt2RecvData::Stmt2Close => {
                Stmt2Ok::Stmt2Close(if self.code == 0 { Ok(()) } else { _e!() })
            }
        }
    }
}