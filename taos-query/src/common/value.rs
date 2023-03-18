use std::{borrow::Cow, fmt::Display, str::Utf8Error};

use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use super::{Timestamp, Ty};

#[derive(Debug, Clone)]
pub enum BorrowedValue<'b> {
    Null(Ty),    // 0
    Bool(bool),  // 1
    TinyInt(i8), // 2
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    VarChar(&'b str),
    Timestamp(Timestamp),
    NChar(Cow<'b, str>),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64), // 14
    Json(Cow<'b, [u8]>),
    VarBinary(&'b [u8]),
    Decimal(Decimal),
    Blob(&'b [u8]),
    MediumBlob(&'b [u8]),
}

macro_rules! borrowed_value_to_native {
    ($v:expr) => {
        match $v {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Bool(v) => Some(if *v { 1 } else { 0 }),
            BorrowedValue::TinyInt(v) => Some(*v as _),
            BorrowedValue::SmallInt(v) => Some(*v as _),
            BorrowedValue::Int(v) => Some(*v as _),
            BorrowedValue::BigInt(v) => Some(*v as _),
            BorrowedValue::Float(v) => Some(*v as _),
            BorrowedValue::Double(v) => Some(*v as _),
            BorrowedValue::VarChar(s) => s.parse().map(Some).unwrap_or(None),
            BorrowedValue::Timestamp(v) => Some(v.as_raw_i64() as _),
            BorrowedValue::NChar(s) => s.parse().map(Some).unwrap_or(None),
            BorrowedValue::UTinyInt(v) => Some(*v as _),
            BorrowedValue::USmallInt(v) => Some(*v as _),
            BorrowedValue::UInt(v) => Some(*v as _),
            BorrowedValue::UBigInt(v) => Some(*v as _),
            BorrowedValue::Json(v) => serde_json::from_slice(&v).ok(),
            BorrowedValue::VarBinary(_) => todo!(),
            BorrowedValue::Decimal(_) => todo!(),
            BorrowedValue::Blob(_) => todo!(),
            BorrowedValue::MediumBlob(_) => todo!(),
        }
    };
}

macro_rules! borrowed_value_to_float {
    ($v:expr) => {
        match $v {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Bool(v) => Some(if *v { 1. } else { 0. }),
            BorrowedValue::TinyInt(v) => Some(*v as _),
            BorrowedValue::SmallInt(v) => Some(*v as _),
            BorrowedValue::Int(v) => Some(*v as _),
            BorrowedValue::BigInt(v) => Some(*v as _),
            BorrowedValue::Float(v) => Some(*v as _),
            BorrowedValue::Double(v) => Some(*v as _),
            BorrowedValue::VarChar(s) => s.parse().map(Some).unwrap_or(None),
            BorrowedValue::Timestamp(v) => Some(v.as_raw_i64() as _),
            BorrowedValue::NChar(s) => s.parse().map(Some).unwrap_or(None),
            BorrowedValue::UTinyInt(v) => Some(*v as _),
            BorrowedValue::USmallInt(v) => Some(*v as _),
            BorrowedValue::UInt(v) => Some(*v as _),
            BorrowedValue::UBigInt(v) => Some(*v as _),
            BorrowedValue::Json(v) => serde_json::from_slice(&v).ok(),
            BorrowedValue::VarBinary(_) => todo!(),
            BorrowedValue::Decimal(_) => todo!(),
            BorrowedValue::Blob(_) => todo!(),
            BorrowedValue::MediumBlob(_) => todo!(),
        }
    };
}

impl<'b> BorrowedValue<'b> {
    /// The data type of this value.
    pub const fn ty(&self) -> Ty {
        use BorrowedValue::*;
        match self {
            Null(ty) => *ty,
            Bool(_) => Ty::Bool,
            TinyInt(_) => Ty::TinyInt,
            SmallInt(_) => Ty::SmallInt,
            Int(_) => Ty::Int,
            BigInt(_) => Ty::BigInt,
            UTinyInt(_) => Ty::UTinyInt,
            USmallInt(_) => Ty::USmallInt,
            UInt(_) => Ty::UInt,
            UBigInt(_) => Ty::UBigInt,
            Float(_) => Ty::Float,
            Double(_) => Ty::Double,
            VarChar(_) => Ty::VarChar,
            Timestamp(_) => Ty::Timestamp,
            Json(_) => Ty::Json,
            NChar(_) => Ty::NChar,
            VarBinary(_) => Ty::VarBinary,
            Decimal(_) => Ty::Decimal,
            Blob(_) => Ty::Blob,
            MediumBlob(_) => Ty::MediumBlob,
        }
    }

    pub fn to_sql_value(&self) -> String {
        use BorrowedValue::*;
        match self {
            Null(_) => "NULL".to_string(),
            Bool(v) => format!("{v}"),
            TinyInt(v) => format!("{v}"),
            SmallInt(v) => format!("{v}"),
            Int(v) => format!("{v}"),
            BigInt(v) => format!("{v}"),
            Float(v) => format!("{v}"),
            Double(v) => format!("{v}"),
            VarChar(v) => format!("\"{}\"", v.escape_debug()),
            Timestamp(v) => format!("{}", v.as_raw_i64()),
            NChar(v) => format!("\"{}\"", v.escape_debug()),
            UTinyInt(v) => format!("{v}"),
            USmallInt(v) => format!("{v}"),
            UInt(v) => format!("{v}"),
            UBigInt(v) => format!("{v}"),
            Json(v) => format!("\"{}\"", unsafe { std::str::from_utf8_unchecked(v) }),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }

    /// Check if the value is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, BorrowedValue::Null(_))
    }
    /// Only VarChar, NChar, Json could be treated as [&str].
    fn strict_as_str(&self) -> &str {
        use BorrowedValue::*;
        match self {
            VarChar(v) => *v,
            NChar(v) => &v,
            Null(_) => panic!("expect str but value is null"),
            Timestamp(_) => panic!("expect str but value is timestamp"),
            _ => panic!("expect str but only varchar/binary/nchar is supported"),
        }
    }
    pub fn to_string(&self) -> Result<String, Utf8Error> {
        use BorrowedValue::*;
        match self {
            Null(_) => Ok(String::new()),
            Bool(v) => Ok(format!("{v}")),
            VarChar(v) => Ok(v.to_string()),
            Json(v) => Ok(unsafe { std::str::from_utf8_unchecked(v) }.to_string()),
            NChar(v) => Ok(v.to_string()),
            TinyInt(v) => Ok(format!("{v}")),
            SmallInt(v) => Ok(format!("{v}")),
            Int(v) => Ok(format!("{v}")),
            BigInt(v) => Ok(format!("{v}")),
            UTinyInt(v) => Ok(format!("{v}")),
            USmallInt(v) => Ok(format!("{v}")),
            UInt(v) => Ok(format!("{v}")),
            UBigInt(v) => Ok(format!("{v}")),
            Float(v) => Ok(format!("{v}")),
            Double(v) => Ok(format!("{v}")),
            Timestamp(v) => Ok(v.to_datetime_with_tz().to_rfc3339()),
            _ => unreachable!("un supported type to string"),
        }
    }

    pub fn to_value(&self) -> Value {
        use BorrowedValue::*;
        match self {
            Null(ty) => Value::Null(*ty),
            Bool(v) => Value::Bool(*v),
            TinyInt(v) => Value::TinyInt(*v),
            SmallInt(v) => Value::SmallInt(*v),
            Int(v) => Value::Int(*v),
            BigInt(v) => Value::BigInt(*v),
            UTinyInt(v) => Value::UTinyInt(*v),
            USmallInt(v) => Value::USmallInt(*v),
            UInt(v) => Value::UInt(*v),
            UBigInt(v) => Value::UBigInt(*v),
            Float(v) => Value::Float(*v),
            Double(v) => Value::Double(*v),
            VarChar(v) => Value::VarChar(v.to_string()),
            Timestamp(v) => Value::Timestamp(*v),
            Json(v) => {
                Value::Json(serde_json::from_slice(v).expect("json should always be deserialized"))
            }
            NChar(str) => Value::NChar(str.to_string()),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        use BorrowedValue::*;
        match self {
            Null(_) => serde_json::Value::Null,
            Bool(v) => serde_json::Value::Bool(*v),
            TinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            SmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            BigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UTinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            USmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UBigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Float(v) => serde_json::Value::Number(serde_json::Number::from_f64(*v as f64).unwrap()),
            Double(v) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*v as f64).unwrap())
            }
            VarChar(v) => serde_json::Value::String(v.to_string()),
            Timestamp(v) => serde_json::Value::Number(serde_json::Number::from(v.as_raw_i64())),
            Json(v) => serde_json::Value::Number(
                serde_json::from_slice(v).expect("json should always be deserialized"),
            ),
            NChar(str) => serde_json::Value::String(str.to_string()),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }

    #[inline]
    pub fn into_value(self) -> Value {
        use BorrowedValue::*;
        match self {
            Null(ty) => Value::Null(ty),
            Bool(v) => Value::Bool(v),
            TinyInt(v) => Value::TinyInt(v),
            SmallInt(v) => Value::SmallInt(v),
            Int(v) => Value::Int(v),
            BigInt(v) => Value::BigInt(v),
            UTinyInt(v) => Value::UTinyInt(v),
            USmallInt(v) => Value::USmallInt(v),
            UInt(v) => Value::UInt(v),
            UBigInt(v) => Value::UBigInt(v),
            Float(v) => Value::Float(v),
            Double(v) => Value::Double(v),
            VarChar(v) => Value::VarChar(v.to_string()),
            Timestamp(v) => Value::Timestamp(v),
            Json(v) => {
                Value::Json(serde_json::from_slice(&v).expect("json should always be deserialized"))
            }
            NChar(str) => Value::NChar(str.to_string()),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }

    pub(crate) fn to_bool(&self) -> Option<bool> {
        match self {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Bool(v) => Some(*v),
            BorrowedValue::TinyInt(v) => Some(*v > 0),
            BorrowedValue::SmallInt(v) => Some(*v > 0),
            BorrowedValue::Int(v) => Some(*v > 0),
            BorrowedValue::BigInt(v) => Some(*v > 0),
            BorrowedValue::Float(v) => Some(*v > 0.),
            BorrowedValue::Double(v) => Some(*v > 0.),
            BorrowedValue::VarChar(s) => match *s {
                "" => None,
                "false" | "f" | "F" | "FALSE" | "False" => Some(false),
                "true" | "t" | "T" | "TRUE" | "True" => Some(true),
                _ => Some(true),
            },
            BorrowedValue::Timestamp(_) => Some(true),
            BorrowedValue::NChar(s) => match s.as_ref() {
                "" => None,
                "false" | "f" | "F" | "FALSE" | "False" => Some(false),
                "true" | "t" | "T" | "TRUE" | "True" => Some(true),
                _ => Some(true),
            },
            BorrowedValue::UTinyInt(v) => Some(*v != 0),
            BorrowedValue::USmallInt(v) => Some(*v != 0),
            BorrowedValue::UInt(v) => Some(*v != 0),
            BorrowedValue::UBigInt(v) => Some(*v != 0),
            BorrowedValue::Json(v) => Some(true),
            BorrowedValue::VarBinary(_) => todo!(),
            BorrowedValue::Decimal(_) => todo!(),
            BorrowedValue::Blob(_) => todo!(),
            BorrowedValue::MediumBlob(_) => todo!(),
        }
    }

    pub(crate) fn to_i8(&self) -> Option<i8> {
        borrowed_value_to_native!(self)
    }
    pub(crate) fn to_i16(&self) -> Option<i16> {
        borrowed_value_to_native!(self)
    }
    pub(crate) fn to_i32(&self) -> Option<i32> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_i64(&self) -> Option<i64> {
        borrowed_value_to_native!(self)
    }
    pub(crate) fn to_u8(&self) -> Option<u8> {
        borrowed_value_to_native!(self)
    }
    pub(crate) fn to_u16(&self) -> Option<u16> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_u32(&self) -> Option<u32> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_u64(&self) -> Option<u64> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_f32(&self) -> Option<f32> {
        borrowed_value_to_float!(self)
    }
    pub(crate) fn to_f64(&self) -> Option<f64> {
        borrowed_value_to_float!(self)
    }
    pub(crate) fn to_str(&self) -> Option<Cow<str>> {
        match self {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Bool(v) => Some(v.to_string().into()),
            BorrowedValue::TinyInt(v) => Some(v.to_string().into()),
            BorrowedValue::SmallInt(v) => Some(v.to_string().into()),
            BorrowedValue::Int(v) => Some(v.to_string().into()),
            BorrowedValue::BigInt(v) => Some(v.to_string().into()),
            BorrowedValue::Float(v) => Some(v.to_string().into()),
            BorrowedValue::Double(v) => Some(v.to_string().into()),
            BorrowedValue::VarChar(s) => Some((*s).into()),
            BorrowedValue::Timestamp(v) => Some(v.to_datetime_with_tz().to_string().into()),
            BorrowedValue::NChar(s) => Some(s.as_ref().into()),
            BorrowedValue::UTinyInt(v) => Some(v.to_string().into()),
            BorrowedValue::USmallInt(v) => Some(v.to_string().into()),
            BorrowedValue::UInt(v) => Some(v.to_string().into()),
            BorrowedValue::UBigInt(v) => Some(v.to_string().into()),
            BorrowedValue::Json(v) => Some(unsafe { std::str::from_utf8_unchecked(&v) }.into()),
            BorrowedValue::VarBinary(_) => todo!(),
            BorrowedValue::Decimal(_) => todo!(),
            BorrowedValue::Blob(_) => todo!(),
            BorrowedValue::MediumBlob(_) => todo!(),
        }
    }
    pub(crate) fn to_timestamp(&self) -> Option<Timestamp> {
        match self {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Timestamp(v) => Some(*v),
            _ => panic!("Unsupported conversion from {} to timestamp", self.ty()),
        }
    }
}

impl<'b> Display for BorrowedValue<'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BorrowedValue::*;
        match self {
            Null(_) => f.write_str("NULL"),
            Bool(v) => f.write_fmt(format_args!("{v}")),
            TinyInt(v) => f.write_fmt(format_args!("{v}")),
            SmallInt(v) => f.write_fmt(format_args!("{v}")),
            Int(v) => f.write_fmt(format_args!("{v}")),
            BigInt(v) => f.write_fmt(format_args!("{v}")),
            Float(v) => f.write_fmt(format_args!("{v}")),
            Double(v) => f.write_fmt(format_args!("{v}")),
            VarChar(v) => f.write_fmt(format_args!("{v}")),
            Timestamp(v) => f.write_fmt(format_args!("{v}")),
            NChar(v) => f.write_fmt(format_args!("{v}")),
            UTinyInt(v) => f.write_fmt(format_args!("{v}")),
            USmallInt(v) => f.write_fmt(format_args!("{v}")),
            UInt(v) => f.write_fmt(format_args!("{v}")),
            UBigInt(v) => f.write_fmt(format_args!("{v}")),
            Json(v) => f.write_fmt(format_args!("{}", v.as_ref().escape_ascii())),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }
}

unsafe impl<'b> Send for BorrowedValue<'b> {}

// #[derive(Debug, Clone)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum Value {
    Null(Ty),    // 0
    Bool(bool),  // 1
    TinyInt(i8), // 2
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    VarChar(String),
    Timestamp(Timestamp),
    NChar(String),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64), // 14
    Json(serde_json::Value),
    VarBinary(Vec<u8>),
    Decimal(Decimal),
    Blob(Vec<u8>),
    MediumBlob(Vec<u8>),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Value::*;
        match self {
            Null(_) => f.write_str("NULL"),
            Bool(v) => f.write_fmt(format_args!("{v}")),
            TinyInt(v) => f.write_fmt(format_args!("{v}")),
            SmallInt(v) => f.write_fmt(format_args!("{v}")),
            Int(v) => f.write_fmt(format_args!("{v}")),
            BigInt(v) => f.write_fmt(format_args!("{v}")),
            Float(v) => f.write_fmt(format_args!("{v}")),
            Double(v) => f.write_fmt(format_args!("{v}")),
            VarChar(v) => f.write_fmt(format_args!("{v}")),
            Timestamp(v) => f.write_fmt(format_args!("{v}")),
            NChar(v) => f.write_fmt(format_args!("{v}")),
            UTinyInt(v) => f.write_fmt(format_args!("{v}")),
            USmallInt(v) => f.write_fmt(format_args!("{v}")),
            UInt(v) => f.write_fmt(format_args!("{v}")),
            UBigInt(v) => f.write_fmt(format_args!("{v}")),
            Json(v) => f.write_fmt(format_args!("{v}")),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }
}

impl Value {
    /// The data type of this value.
    pub const fn ty(&self) -> Ty {
        use Value::*;
        match self {
            Null(ty) => *ty,
            Bool(_) => Ty::Bool,
            TinyInt(_) => Ty::TinyInt,
            SmallInt(_) => Ty::SmallInt,
            Int(_) => Ty::Int,
            BigInt(_) => Ty::BigInt,
            UTinyInt(_) => Ty::UTinyInt,
            USmallInt(_) => Ty::USmallInt,
            UInt(_) => Ty::UInt,
            UBigInt(_) => Ty::UBigInt,
            Float(_) => Ty::Float,
            Double(_) => Ty::Double,
            VarChar(_) => Ty::VarChar,
            Timestamp(_) => Ty::Timestamp,
            Json(_) => Ty::Json,
            NChar(_) => Ty::NChar,
            VarBinary(_) => Ty::VarBinary,
            Decimal(_) => Ty::Decimal,
            Blob(_) => Ty::Blob,
            MediumBlob(_) => Ty::MediumBlob,
        }
    }

    pub fn to_borrowed_value(&self) -> BorrowedValue {
        use Value::*;
        match self {
            Null(ty) => BorrowedValue::Null(*ty),
            Bool(v) => BorrowedValue::Bool(*v),
            TinyInt(v) => BorrowedValue::TinyInt(*v),
            SmallInt(v) => BorrowedValue::SmallInt(*v),
            Int(v) => BorrowedValue::Int(*v),
            BigInt(v) => BorrowedValue::BigInt(*v),
            UTinyInt(v) => BorrowedValue::UTinyInt(*v),
            USmallInt(v) => BorrowedValue::USmallInt(*v),
            UInt(v) => BorrowedValue::UInt(*v),
            UBigInt(v) => BorrowedValue::UBigInt(*v),
            Float(v) => BorrowedValue::Float(*v),
            Double(v) => BorrowedValue::Double(*v),
            VarChar(v) => BorrowedValue::VarChar(v),
            Timestamp(v) => BorrowedValue::Timestamp(*v),
            Json(j) => BorrowedValue::Json(j.to_string().into_bytes().into()),
            NChar(v) => BorrowedValue::NChar(v.as_str().into()),
            VarBinary(v) => BorrowedValue::VarBinary(v),
            Decimal(v) => BorrowedValue::Decimal(*v),
            Blob(v) => BorrowedValue::Blob(v),
            MediumBlob(v) => BorrowedValue::MediumBlob(v),
        }
    }

    /// Check if the value is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, Value::Null(_))
    }
    /// Only VarChar, NChar, Json could be treated as [&str].
    pub fn strict_as_str(&self) -> &str {
        use Value::*;
        match self {
            VarChar(v) => v.as_str(),
            NChar(v) => v.as_str(),
            Json(v) => v.as_str().expect("invalid str type"),
            Null(_) => "Null",
            Timestamp(_) => panic!("expect str but value is timestamp"),
            _ => panic!("expect str but only varchar/binary/json/nchar is supported"),
        }
    }

    pub fn to_sql_value(&self) -> String {
        use Value::*;
        match self {
            Null(_) => "NULL".to_string(),
            Bool(v) => format!("{v}"),
            TinyInt(v) => format!("{v}"),
            SmallInt(v) => format!("{v}"),
            Int(v) => format!("{v}"),
            BigInt(v) => format!("{v}"),
            Float(v) => format!("{v}"),
            Double(v) => format!("{v}"),
            VarChar(v) => format!("\"{}\"", v.escape_debug()),
            Timestamp(v) => format!("{}", v.as_raw_i64()),
            NChar(v) => format!("\"{}\"", v.escape_debug()),
            UTinyInt(v) => format!("{v}"),
            USmallInt(v) => format!("{v}"),
            UInt(v) => format!("{v}"),
            UBigInt(v) => format!("{v}"),
            Json(v) => format!("\"{}\"", v),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }

    pub fn to_string(&self) -> Result<String, Utf8Error> {
        use Value::*;
        match self {
            Null(_) => Ok(String::new()),
            VarChar(v) => Ok(v.to_string()),
            Json(v) => Ok(v.to_string()),
            NChar(v) => Ok(v.to_string()),
            TinyInt(v) => Ok(format!("{v}")),
            SmallInt(v) => Ok(format!("{v}")),
            Int(v) => Ok(format!("{v}")),
            BigInt(v) => Ok(format!("{v}")),
            UTinyInt(v) => Ok(format!("{v}")),
            USmallInt(v) => Ok(format!("{v}")),
            UInt(v) => Ok(format!("{v}")),
            UBigInt(v) => Ok(format!("{v}")),
            Float(v) => Ok(format!("{v}")),
            Double(v) => Ok(format!("{v}")),
            Timestamp(v) => Ok(v.to_datetime_with_tz().to_rfc3339()),
            _ => unreachable!("un supported type to string"),
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        use Value::*;
        match self {
            Null(_) => serde_json::Value::Null,
            Bool(v) => serde_json::Value::Bool(*v),
            TinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            SmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            BigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UTinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            USmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UBigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Float(v) => serde_json::Value::Number(serde_json::Number::from_f64(*v as f64).unwrap()),
            Double(v) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*v as f64).unwrap())
            }
            VarChar(v) => serde_json::Value::String(v.to_string()),
            Timestamp(v) => serde_json::Value::Number(serde_json::Number::from(v.as_raw_i64())),
            Json(v) => v.clone(),
            NChar(str) => serde_json::Value::String(str.to_string()),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }
}

impl<'b> PartialEq<&Value> for BorrowedValue<'b> {
    fn eq(&self, other: &&Value) -> bool {
        self == *other
    }
}

impl<'b> PartialEq<Value> for BorrowedValue<'b> {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Self::Null(l0), Value::Null(r0)) => l0 == r0,
            (Self::Bool(l0), Value::Bool(r0)) => l0 == r0,
            (Self::TinyInt(l0), Value::TinyInt(r0)) => l0 == r0,
            (Self::SmallInt(l0), Value::SmallInt(r0)) => l0 == r0,
            (Self::Int(l0), Value::Int(r0)) => l0 == r0,
            (Self::BigInt(l0), Value::BigInt(r0)) => l0 == r0,
            (Self::Float(l0), Value::Float(r0)) => l0 == r0,
            (Self::Double(l0), Value::Double(r0)) => l0 == r0,
            (Self::VarChar(l0), Value::VarChar(r0)) => l0 == r0,
            (Self::Timestamp(l0), Value::Timestamp(r0)) => l0 == r0,
            (Self::NChar(l0), Value::NChar(r0)) => l0 == r0,
            (Self::UTinyInt(l0), Value::UTinyInt(r0)) => l0 == r0,
            (Self::USmallInt(l0), Value::USmallInt(r0)) => l0 == r0,
            (Self::UInt(l0), Value::UInt(r0)) => l0 == r0,
            (Self::UBigInt(l0), Value::UBigInt(r0)) => l0 == r0,
            (Self::Json(l0), Value::Json(r0)) => l0.as_ref() == &serde_json::to_vec(r0).unwrap(),
            (Self::VarBinary(l0), Value::VarBinary(r0)) => l0 == r0,
            (Self::Decimal(l0), Value::Decimal(r0)) => l0 == r0,
            (Self::Blob(l0), Value::Blob(r0)) => l0 == r0,
            (Self::MediumBlob(l0), Value::MediumBlob(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl<'b> PartialEq<BorrowedValue<'b>> for Value {
    fn eq(&self, other: &BorrowedValue<'b>) -> bool {
        match (other, self) {
            (BorrowedValue::Null(l0), Value::Null(r0)) => l0 == r0,
            (BorrowedValue::Bool(l0), Value::Bool(r0)) => l0 == r0,
            (BorrowedValue::TinyInt(l0), Value::TinyInt(r0)) => l0 == r0,
            (BorrowedValue::SmallInt(l0), Value::SmallInt(r0)) => l0 == r0,
            (BorrowedValue::Int(l0), Value::Int(r0)) => l0 == r0,
            (BorrowedValue::BigInt(l0), Value::BigInt(r0)) => l0 == r0,
            (BorrowedValue::Float(l0), Value::Float(r0)) => l0 == r0,
            (BorrowedValue::Double(l0), Value::Double(r0)) => l0 == r0,
            (BorrowedValue::VarChar(l0), Value::VarChar(r0)) => l0 == r0,
            (BorrowedValue::Timestamp(l0), Value::Timestamp(r0)) => l0 == r0,
            (BorrowedValue::NChar(l0), Value::NChar(r0)) => l0 == r0,
            (BorrowedValue::UTinyInt(l0), Value::UTinyInt(r0)) => l0 == r0,
            (BorrowedValue::USmallInt(l0), Value::USmallInt(r0)) => l0 == r0,
            (BorrowedValue::UInt(l0), Value::UInt(r0)) => l0 == r0,
            (BorrowedValue::UBigInt(l0), Value::UBigInt(r0)) => l0 == r0,
            (BorrowedValue::Json(l0), Value::Json(r0)) => {
                l0.as_ref() == &serde_json::to_vec(r0).unwrap()
            }
            (BorrowedValue::VarBinary(l0), Value::VarBinary(r0)) => l0 == r0,
            (BorrowedValue::Decimal(l0), Value::Decimal(r0)) => l0 == r0,
            (BorrowedValue::Blob(l0), Value::Blob(r0)) => l0 == r0,
            (BorrowedValue::MediumBlob(l0), Value::MediumBlob(r0)) => l0 == r0,
            _ => false,
        }
    }
}

macro_rules! _impl_primitive_from {
    ($f:ident, $t:ident) => {
        impl From<$f> for Value {
            fn from(value: $f) -> Self {
                Value::$t(value)
            }
        }
        impl From<Option<$f>> for Value {
            fn from(value: Option<$f>) -> Self {
                match value {
                    Some(value) => Value::$t(value),
                    None => Value::Null(Ty::$t),
                }
            }
        }
    };
}

_impl_primitive_from!(bool, Bool);
_impl_primitive_from!(i8, TinyInt);
_impl_primitive_from!(i16, SmallInt);
_impl_primitive_from!(i32, Int);
_impl_primitive_from!(i64, BigInt);
_impl_primitive_from!(u8, UTinyInt);
_impl_primitive_from!(u16, USmallInt);
_impl_primitive_from!(u32, UInt);
_impl_primitive_from!(u64, UBigInt);
_impl_primitive_from!(f32, Float);
_impl_primitive_from!(f64, Double);
_impl_primitive_from!(Timestamp, Timestamp);
mod de;
