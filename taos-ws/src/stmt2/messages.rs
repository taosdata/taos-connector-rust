use std::fmt;

use serde::de::{self, Visitor};
use serde::Deserializer;
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
    Stmt2GetFields {
        #[serde(flatten)]
        args: Stmt2Args,
        field_types: Vec<i8>,
    },
    Stmt2Exec(Stmt2Args),
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
    pub timing: u64,
    #[serde(flatten)]
    pub data: Stmt2RecvData,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum Stmt2RecvData {
    Conn,
    Stmt2Init {
        stmt_id: StmtId,
    },
    Stmt2Prepare {
        stmt_id: StmtId,
        is_insert: bool,
        fields: Option<Vec<PrepareField>>,
        fields_count: usize,
    },
    Stmt2Bind {
        stmt_id: StmtId,
    },
    Stmt2Exec {
        stmt_id: StmtId,
        #[serde(default)]
        affected: usize,
    },
    Stmt2GetFields {
        stmt_id: StmtId,
        table_count: u32,
        query_count: u32,
        col_fields: Option<Vec<Field>>,
        tag_fields: Option<Vec<Field>>,
    },
    Stmt2Result {
        stmt_id: StmtId,
        result_id: u64,
        fields_count: u64,
        fields_names: Option<Vec<String>>,
        fields_types: Option<Vec<Ty>>,
        fields_lengths: Option<Vec<u64>>,
        precision: Precision,
    },
    Stmt2Close {
        stmt_id: StmtId,
    },
}

#[derive(Debug, Deserialize)]
pub struct Field {
    name: String,
    field_type: i8,
    precision: u8,
    scale: u8,
    bytes: i32,
}

#[derive(Debug, Deserialize)]
pub struct PrepareField {
    pub name: String,
    pub field_type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
    pub bind_type: BindType,
}

#[derive(Debug, Deserialize)]
pub struct PrepareField2 {
    pub name: String,
    pub field_type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
    pub bind_type: u8,
}

#[derive(Debug)]
pub enum BindType {
    Column,
    Tag,
    TableName,
}

impl<'de> Deserialize<'de> for BindType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BindTypeVisitor;

        impl<'de> Visitor<'de> for BindTypeVisitor {
            type Value = BindType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a valid number for BindType")
            }

            fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(match v {
                    1 => BindType::Column,
                    2 => BindType::Tag,
                    4 => BindType::TableName,
                    _ => return Err(E::custom(format!("Invalid bind type: {}", v))),
                })
            }

            fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i8(v as _)
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i8(v as _)
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i8(v as _)
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i8(v as _)
            }

            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i8(v as _)
            }

            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i8(v as _)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i8(v as _)
            }
        }

        deserializer.deserialize_any(BindTypeVisitor)
    }
}

#[derive(Debug)]
pub enum Stmt2Ok {
    Conn(Result<(), Error>),
    Stmt2Init(ReqId, Result<StmtId, Error>),
    Stmt2Bind(StmtId, Result<(), Error>),
    Stmt2Res(StmtId, Result<Stmt2Result, Error>),
    Stmt2ExecRes(StmtId, Result<Option<usize>, Error>),
    Stmt2PrepareRes(StmtId, Result<Stmt2PrepareResult, Error>),
    Stmt2Fields(StmtId, Result<Stmt2Fields, Error>),
    Stmt2Close(StmtId, Result<(), Error>),
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
    pub fields: Option<Vec<PrepareField>>,
    pub fields_count: usize,
}

#[derive(Debug, Deserialize)]
pub struct Stmt2Fields {
    table_count: u32,
    query_count: u32,
    col_fields: Option<Vec<Field>>,
    tag_fields: Option<Vec<Field>>,
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
            Stmt2RecvData::Stmt2Init { stmt_id } => Stmt2Ok::Stmt2Init(
                self.req_id,
                if self.code == 0 { Ok(stmt_id) } else { _e!() },
            ),
            Stmt2RecvData::Stmt2Prepare {
                stmt_id,
                is_insert,
                fields,
                fields_count,
            } => Stmt2Ok::Stmt2PrepareRes(
                stmt_id,
                if self.code == 0 {
                    Ok(Stmt2PrepareResult {
                        timing: self.timing,
                        stmt_id,
                        is_insert,
                        fields,
                        fields_count,
                    })
                } else {
                    _e!()
                },
            ),
            Stmt2RecvData::Stmt2Bind { stmt_id } => {
                Stmt2Ok::Stmt2Bind(stmt_id, if self.code == 0 { Ok(()) } else { _e!() })
            }
            Stmt2RecvData::Stmt2Exec { stmt_id, affected } => Stmt2Ok::Stmt2ExecRes(
                stmt_id,
                if self.code == 0 {
                    Ok(Some(affected))
                } else {
                    _e!()
                },
            ),
            Stmt2RecvData::Stmt2GetFields {
                stmt_id,
                table_count,
                query_count,
                col_fields,
                tag_fields,
            } => Stmt2Ok::Stmt2Fields(
                stmt_id,
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
                stmt_id,
                result_id,
                fields_count,
                fields_names,
                fields_types,
                fields_lengths,
                precision,
            } => Stmt2Ok::Stmt2Res(
                stmt_id,
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
            Stmt2RecvData::Stmt2Close { stmt_id } => {
                Stmt2Ok::Stmt2Close(stmt_id, if self.code == 0 { Ok(()) } else { _e!() })
            }
        }
    }
}
