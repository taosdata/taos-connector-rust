use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;

use crate::query::infra::{ToMessage, WsConnReq};
use taos_query::prelude::RawError as Error;

use crate::stmt::{StmtField, StmtParam, StmtUseResult};

pub type ReqId = u64;

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
    GetTagFields(StmtArgs),
    GetColFields(StmtArgs),
    UseResult(StmtArgs),
    StmtNumParams(StmtArgs),
    StmtGetParam {
        #[serde(flatten)]
        args: StmtArgs,
        index: i64,
    },
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
    GetTagFields {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        fields: Vec<StmtField>,
    },
    GetColFields {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        fields: Vec<StmtField>,
    },
    UseResult {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        result_id: u64,
        #[serde(default)]
        fields_count: i64,
        #[serde(default)]
        fields_names: Option<Vec<String>>,
        #[serde(default)]
        fields_types: Option<Vec<u8>>,
        #[serde(default)]
        fields_lengths: Option<Vec<u32>>,
        #[serde(default)]
        precision: i64,
    },
    StmtNumParams {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        num_params: usize,
    },
    StmtGetParam {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        index: i64,
        #[serde(default)]
        data_type: i64,
        #[serde(default)]
        length: i64,
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
    StmtFields(StmtId, Result<Vec<StmtField>, Error>),
    StmtParam(StmtId, Result<StmtParam, Error>),
    StmtUseResult(StmtId, Result<StmtUseResult, Error>),
}

impl StmtRecv {
    pub(crate) fn ok(self) -> StmtOk {
        macro_rules! _e {
            () => {
                Err(Error::new(
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
            StmtRecvData::GetTagFields { stmt_id, fields }
            | StmtRecvData::GetColFields { stmt_id, fields } => StmtOk::StmtFields(stmt_id, {
                if self.code == 0 {
                    Ok(fields)
                } else {
                    _e!()
                }
            }),
            StmtRecvData::UseResult {
                stmt_id,
                result_id,
                fields_count,
                fields_names,
                fields_types,
                fields_lengths,
                precision,
            } => StmtOk::StmtUseResult(stmt_id, {
                if self.code == 0 {
                    Ok(StmtUseResult {
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
                }
            }),
            StmtRecvData::StmtNumParams {
                stmt_id,
                num_params,
            } => StmtOk::Stmt(stmt_id, {
                if self.code == 0 {
                    Ok(Some(num_params))
                } else {
                    _e!()
                }
            }),
            StmtRecvData::StmtGetParam {
                stmt_id,
                index,
                data_type,
                length,
            } => StmtOk::StmtParam(stmt_id, {
                if self.code == 0 {
                    Ok(StmtParam {
                        index,
                        data_type: data_type.into(),
                        length,
                    })
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

    #[test]
    fn stmt() -> anyhow::Result<()> {
        Ok(())
    }
}
