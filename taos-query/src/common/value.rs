use std::{borrow::Cow, fmt::Display, str::Utf8Error};

use super::{Timestamp, Ty};
use bytes::Bytes;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

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
    VarBinary(Cow<'b, [u8]>),
    Decimal(Decimal),
    Blob(&'b [u8]),
    MediumBlob(&'b [u8]),
    Geometry(Cow<'b, [u8]>),
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
            BorrowedValue::VarBinary(_v) => todo!(),
            BorrowedValue::Decimal(_) => todo!(),
            BorrowedValue::Blob(_) => todo!(),
            BorrowedValue::MediumBlob(_) => todo!(),
            BorrowedValue::Geometry(_) => todo!(),
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
            BorrowedValue::Geometry(_) => todo!(),
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
            Geometry(_) => Ty::Geometry,
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
            Geometry(_) => todo!(),
        }
    }

    pub fn to_sql_value_with_rfc3339(&self) -> String {
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
            Timestamp(v) => format!("{}", v.to_string()),
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
            Geometry(_) => todo!(),
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
            VarChar(v) => v,
            NChar(v) => v,
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
            VarBinary(v) => Value::VarBinary(Bytes::copy_from_slice(v.as_ref())),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
            Geometry(v) => Value::Geometry(Bytes::copy_from_slice(v.as_ref())),
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
            Double(v) => serde_json::Value::Number(serde_json::Number::from_f64(*v).unwrap()),
            VarChar(v) => serde_json::Value::String(v.to_string()),
            Timestamp(v) => serde_json::Value::Number(serde_json::Number::from(v.as_raw_i64())),
            Json(v) => serde_json::from_slice(v).expect("json should always be deserialized"),
            NChar(str) => serde_json::Value::String(str.to_string()),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
            Geometry(_) => todo!(),
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
            VarBinary(v) => Value::VarBinary(Bytes::from(v.into_owned())),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
            Geometry(v) => Value::Geometry(Bytes::from(v.into_owned())),
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
            BorrowedValue::Json(_) => Some(true),
            BorrowedValue::VarBinary(_) => todo!(),
            BorrowedValue::Decimal(_) => todo!(),
            BorrowedValue::Blob(_) => todo!(),
            BorrowedValue::MediumBlob(_) => todo!(),
            BorrowedValue::Geometry(_) => todo!(),
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
            BorrowedValue::Json(v) => Some(unsafe { std::str::from_utf8_unchecked(v) }.into()),
            BorrowedValue::VarBinary(_) => todo!(),
            BorrowedValue::Decimal(_) => todo!(),
            BorrowedValue::Blob(_) => todo!(),
            BorrowedValue::MediumBlob(_) => todo!(),
            BorrowedValue::Geometry(_) => todo!(),
        }
    }
    #[allow(dead_code)]
    pub(crate) fn to_bytes(&self) -> Option<Bytes> {
        match self {
            BorrowedValue::VarBinary(v) => Some(Bytes::from(v.to_vec())),
            BorrowedValue::Geometry(v) => Some(Bytes::from(v.to_vec())),
            _ => None,
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
            VarBinary(v) => f.write_fmt(format_args!("{:?}", v.to_vec())),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
            Geometry(v) => f.write_fmt(format_args!("{:?}", v.to_vec())),
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
    VarBinary(Bytes),
    Decimal(Decimal),
    Blob(Vec<u8>),
    MediumBlob(Vec<u8>),
    Geometry(Bytes),
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
            VarBinary(v) => f.write_fmt(format_args!("{:?}", v.to_vec())),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
            Geometry(v) => f.write_fmt(format_args!("{:?}", v.to_vec())),
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
            Geometry(_) => Ty::Geometry,
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
            VarBinary(v) => BorrowedValue::VarBinary(Cow::Borrowed(v.as_ref())),
            Decimal(v) => BorrowedValue::Decimal(*v),
            Blob(v) => BorrowedValue::Blob(v),
            MediumBlob(v) => BorrowedValue::MediumBlob(v),
            Geometry(v) => BorrowedValue::Geometry(Cow::Borrowed(v.as_ref())),
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
            Geometry(_) => todo!(),
        }
    }

    pub fn to_sql_value_with_rfc3339(&self) -> String {
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
            Timestamp(v) => format!("\"{}\"", v.to_string()),
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
            Geometry(_) => todo!(),
        }
    }

    pub fn to_string(&self) -> Result<String, Utf8Error> {
        use Value::*;
        match self {
            Null(_) => Ok(String::new()),
            Bool(v) => Ok(format!("{v}")),
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
            _ => unreachable!("unsupported type to string"),
        }
    }

    #[warn(unreachable_patterns)]
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
            Double(v) => serde_json::Value::Number(serde_json::Number::from_f64(*v).unwrap()),
            VarChar(v) => serde_json::Value::String(v.to_string()),
            Timestamp(v) => serde_json::Value::Number(serde_json::Number::from(v.as_raw_i64())),
            Json(v) => v.clone(),
            NChar(str) => serde_json::Value::String(str.to_string()),
            Decimal(v) => serde_json::Value::String(format!("{:?}", v)),
            Blob(v) => serde_json::Value::String(format!("{:?}", v)),
            MediumBlob(v) => serde_json::Value::String(format!("{:?}", v)),
            VarBinary(v) => serde_json::Value::String(format!("{:?}", v.to_vec())),
            Geometry(v) => serde_json::Value::String(format!("{:?}", v.to_vec())),
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
            (Self::Json(l0), Value::Json(r0)) => l0.as_ref() == serde_json::to_vec(r0).unwrap(),
            (Self::VarBinary(l0), Value::VarBinary(r0)) => l0.as_ref() == r0.as_ref(),
            (Self::Decimal(l0), Value::Decimal(r0)) => l0 == r0,
            (Self::Blob(l0), Value::Blob(r0)) => l0 == r0,
            (Self::MediumBlob(l0), Value::MediumBlob(r0)) => l0 == r0,
            (Self::Geometry(l0), Value::Geometry(r0)) => l0.as_ref() == r0.as_ref(),
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
                l0.as_ref() == serde_json::to_vec(r0).unwrap()
            }
            (BorrowedValue::VarBinary(l0), Value::VarBinary(r0)) => l0.as_ref() == r0.as_ref(),
            (BorrowedValue::Decimal(l0), Value::Decimal(r0)) => l0 == r0,
            (BorrowedValue::Blob(l0), Value::Blob(r0)) => l0 == r0,
            (BorrowedValue::MediumBlob(l0), Value::MediumBlob(r0)) => l0 == r0,
            (BorrowedValue::Geometry(l0), Value::Geometry(r0)) => l0.as_ref() == r0.as_ref(),
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

#[cfg(test)]
mod tests {
    use crate::common::Precision;

    use super::*;

    #[test]
    fn test_borrowed_value_to_native() {
        let tiny_int_value = BorrowedValue::TinyInt(42);
        assert_eq!(tiny_int_value.to_i8(), Some(42));
        assert_eq!(tiny_int_value.to_i16(), Some(42));
        assert_eq!(tiny_int_value.to_i32(), Some(42));
        assert_eq!(tiny_int_value.to_i64(), Some(42));
        assert_eq!(tiny_int_value.to_u8(), Some(42));
        assert_eq!(tiny_int_value.to_u16(), Some(42));
        assert_eq!(tiny_int_value.to_u32(), Some(42));
        assert_eq!(tiny_int_value.to_u64(), Some(42));

        let float_value = BorrowedValue::Float(3.14);
        println!("float_value: {:?}", float_value.to_f32());
        println!("float_value: {:?}", float_value.to_f64());
    }

    #[test]
    fn test_null_value() {
        let null_value = Value::Null(Ty::Int);
        assert_eq!(null_value.ty(), Ty::Int);
        assert_eq!(null_value.to_sql_value(), "NULL".to_string());
        assert_eq!(null_value.to_string(), Ok("".to_string()));
        assert_eq!(null_value.to_json_value(), serde_json::Value::Null);
        assert_eq!(format!("{}", null_value), "NULL");
        let null_value_borrowed = null_value.to_borrowed_value();
        assert_eq!(null_value_borrowed.ty(), Ty::Int);
        assert_eq!(null_value_borrowed.to_sql_value(), "NULL".to_string());
        assert_eq!(null_value_borrowed.to_string(), Ok("".to_string()));
        assert_eq!(null_value_borrowed.to_json_value(), serde_json::Value::Null);
        assert_eq!(format!("{}", null_value_borrowed), "NULL");
        println!("{:?}", null_value_borrowed.to_str());
        assert_eq!(null_value_borrowed.to_bool(), None);
        assert_eq!(null_value_borrowed.to_value(), null_value);
        assert_eq!(null_value_borrowed.clone().into_value(), null_value);
        assert_eq!(null_value_borrowed, null_value);
        assert_eq!(null_value, null_value_borrowed);
        assert_eq!(null_value_borrowed, &null_value);
    }

    #[test]
    fn test_bool_value() {
        let bool_value = Value::Bool(true);
        assert_eq!(bool_value.ty(), Ty::Bool);
        assert_eq!(bool_value.to_sql_value(), "true".to_string());
        assert_eq!(bool_value.to_string(), Ok("true".to_string()));
        assert_eq!(bool_value.to_json_value(), serde_json::Value::Bool(true));
        assert_eq!(format!("{}", bool_value), "true");
        let bool_value_borrowed = bool_value.to_borrowed_value();
        assert_eq!(bool_value_borrowed.ty(), Ty::Bool);
        assert_eq!(bool_value_borrowed.to_sql_value(), "true".to_string());
        assert_eq!(bool_value_borrowed.to_string(), Ok("true".to_string()));
        assert_eq!(
            bool_value_borrowed.to_json_value(),
            serde_json::Value::Bool(true)
        );
        assert_eq!(format!("{}", bool_value_borrowed), "true");
        println!("{:?}", bool_value_borrowed.to_str());
        assert_eq!(bool_value_borrowed.to_bool(), Some(true));
        assert_eq!(bool_value_borrowed.to_value(), bool_value);
        assert_eq!(bool_value_borrowed.clone().into_value(), bool_value);
        assert_eq!(bool_value_borrowed, bool_value);
        assert_eq!(bool_value, bool_value_borrowed);
        assert_eq!(bool_value_borrowed, &bool_value);
    }

    #[test]
    fn test_tiny_int_value() {
        let tiny_int_value = Value::TinyInt(42);
        assert_eq!(tiny_int_value.ty(), Ty::TinyInt);
        assert_eq!(tiny_int_value.to_sql_value(), "42".to_string());
        assert_eq!(tiny_int_value.to_string(), Ok("42".to_string()));
        assert_eq!(
            tiny_int_value.to_json_value(),
            serde_json::Value::Number(42.into())
        );
        assert_eq!(format!("{}", tiny_int_value), "42");
        let tiny_int_value_borrowed = tiny_int_value.to_borrowed_value();
        assert_eq!(tiny_int_value_borrowed.ty(), Ty::TinyInt);
        assert_eq!(tiny_int_value_borrowed.to_sql_value(), "42".to_string());
        assert_eq!(tiny_int_value_borrowed.to_string(), Ok("42".to_string()));
        assert_eq!(
            tiny_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(42.into())
        );
        assert_eq!(format!("{}", tiny_int_value_borrowed), "42");
        println!("{:?}", tiny_int_value_borrowed.to_str());
        assert_eq!(tiny_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(tiny_int_value_borrowed.to_value(), tiny_int_value);
        assert_eq!(tiny_int_value_borrowed.clone().into_value(), tiny_int_value);
        assert_eq!(tiny_int_value_borrowed, tiny_int_value);
        assert_eq!(tiny_int_value, tiny_int_value_borrowed);
        assert_eq!(tiny_int_value_borrowed, &tiny_int_value);
    }

    #[test]
    fn test_small_int_value() {
        let small_int_value = Value::SmallInt(1000);
        assert_eq!(small_int_value.ty(), Ty::SmallInt);
        assert_eq!(small_int_value.to_sql_value(), "1000".to_string());
        assert_eq!(small_int_value.to_string(), Ok("1000".to_string()));
        assert_eq!(
            small_int_value.to_json_value(),
            serde_json::Value::Number(1000.into())
        );
        assert_eq!(format!("{}", small_int_value), "1000");
        let small_int_value_borrowed = small_int_value.to_borrowed_value();
        assert_eq!(small_int_value_borrowed.ty(), Ty::SmallInt);
        assert_eq!(small_int_value_borrowed.to_sql_value(), "1000".to_string());
        assert_eq!(small_int_value_borrowed.to_string(), Ok("1000".to_string()));
        assert_eq!(
            small_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(1000.into())
        );
        assert_eq!(format!("{}", small_int_value_borrowed), "1000");
        println!("{:?}", small_int_value_borrowed.to_str());
        assert_eq!(small_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(small_int_value_borrowed.to_value(), small_int_value);
        assert_eq!(
            small_int_value_borrowed.clone().into_value(),
            small_int_value
        );
        assert_eq!(small_int_value_borrowed, small_int_value);
        assert_eq!(small_int_value, small_int_value_borrowed);
        assert_eq!(small_int_value_borrowed, &small_int_value);
    }

    #[test]
    fn test_int_value() {
        let int_value = Value::Int(-500);
        assert_eq!(int_value.ty(), Ty::Int);
        assert_eq!(int_value.to_sql_value(), "-500".to_string());
        assert_eq!(int_value.to_string(), Ok("-500".to_string()));
        assert_eq!(
            int_value.to_json_value(),
            serde_json::Value::Number((-500).into())
        );
        assert_eq!(format!("{}", int_value), "-500");
        let int_value_borrowed = int_value.to_borrowed_value();
        assert_eq!(int_value_borrowed.ty(), Ty::Int);
        assert_eq!(int_value_borrowed.to_sql_value(), "-500".to_string());
        assert_eq!(int_value_borrowed.to_string(), Ok("-500".to_string()));
        assert_eq!(
            int_value_borrowed.to_json_value(),
            serde_json::Value::Number((-500).into())
        );
        assert_eq!(format!("{}", int_value_borrowed), "-500");
        println!("{:?}", int_value_borrowed.to_str());
        assert_eq!(int_value_borrowed.to_bool(), Some(false));
        assert_eq!(int_value_borrowed.to_value(), int_value);
        assert_eq!(int_value_borrowed.clone().into_value(), int_value);
        assert_eq!(int_value_borrowed, int_value);
        assert_eq!(int_value, int_value_borrowed);
        assert_eq!(int_value_borrowed, &int_value);
    }

    #[test]
    fn test_big_int_value() {
        let big_int_value = Value::BigInt(1234567890);
        assert_eq!(big_int_value.ty(), Ty::BigInt);
        assert_eq!(big_int_value.to_sql_value(), "1234567890".to_string());
        assert_eq!(big_int_value.to_string(), Ok("1234567890".to_string()));
        assert_eq!(
            big_int_value.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );
        assert_eq!(format!("{}", big_int_value), "1234567890");
        let big_int_value_borrowed = big_int_value.to_borrowed_value();
        assert_eq!(big_int_value_borrowed.ty(), Ty::BigInt);
        assert_eq!(
            big_int_value_borrowed.to_sql_value(),
            "1234567890".to_string()
        );
        assert_eq!(
            big_int_value_borrowed.to_string(),
            Ok("1234567890".to_string())
        );
        assert_eq!(
            big_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );
        assert_eq!(format!("{}", big_int_value_borrowed), "1234567890");
        println!("{:?}", big_int_value_borrowed.to_str());
        assert_eq!(big_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(big_int_value_borrowed.to_value(), big_int_value);
        assert_eq!(big_int_value_borrowed.clone().into_value(), big_int_value);
        assert_eq!(big_int_value_borrowed, big_int_value);
        assert_eq!(big_int_value, big_int_value_borrowed);
        assert_eq!(big_int_value_borrowed, &big_int_value);
    }

    #[test]
    fn test_utiny_int_value() {
        let utiny_int_value = Value::UTinyInt(42);
        assert_eq!(utiny_int_value.ty(), Ty::UTinyInt);
        assert_eq!(utiny_int_value.to_sql_value(), "42".to_string());
        assert_eq!(utiny_int_value.to_string(), Ok("42".to_string()));
        assert_eq!(
            utiny_int_value.to_json_value(),
            serde_json::Value::Number(42.into())
        );
        assert_eq!(format!("{}", utiny_int_value), "42");
        let utiny_int_value_borrowed = utiny_int_value.to_borrowed_value();
        assert_eq!(utiny_int_value_borrowed.ty(), Ty::UTinyInt);
        assert_eq!(utiny_int_value_borrowed.to_sql_value(), "42".to_string());
        assert_eq!(utiny_int_value_borrowed.to_string(), Ok("42".to_string()));
        assert_eq!(
            utiny_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(42.into())
        );
        assert_eq!(format!("{}", utiny_int_value_borrowed), "42");
        println!("{:?}", utiny_int_value_borrowed.to_str());
        assert_eq!(utiny_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(utiny_int_value_borrowed.to_value(), utiny_int_value);
        assert_eq!(
            utiny_int_value_borrowed.clone().into_value(),
            utiny_int_value
        );
        assert_eq!(utiny_int_value_borrowed, utiny_int_value);
        assert_eq!(utiny_int_value, utiny_int_value_borrowed);
        assert_eq!(utiny_int_value_borrowed, &utiny_int_value);
    }

    #[test]
    fn test_usmall_int_value() {
        let usmall_int_value = Value::USmallInt(1000);
        assert_eq!(usmall_int_value.ty(), Ty::USmallInt);
        assert_eq!(usmall_int_value.to_sql_value(), "1000".to_string());
        assert_eq!(usmall_int_value.to_string(), Ok("1000".to_string()));
        assert_eq!(
            usmall_int_value.to_json_value(),
            serde_json::Value::Number(1000.into())
        );
        assert_eq!(format!("{}", usmall_int_value), "1000");
        let usmall_int_value_borrowed = usmall_int_value.to_borrowed_value();
        assert_eq!(usmall_int_value_borrowed.ty(), Ty::USmallInt);
        assert_eq!(usmall_int_value_borrowed.to_sql_value(), "1000".to_string());
        assert_eq!(
            usmall_int_value_borrowed.to_string(),
            Ok("1000".to_string())
        );
        assert_eq!(
            usmall_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(1000.into())
        );
        assert_eq!(format!("{}", usmall_int_value_borrowed), "1000");
        println!("{:?}", usmall_int_value_borrowed.to_str());
        assert_eq!(usmall_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(usmall_int_value_borrowed.to_value(), usmall_int_value);
        assert_eq!(
            usmall_int_value_borrowed.clone().into_value(),
            usmall_int_value
        );
        assert_eq!(usmall_int_value_borrowed, usmall_int_value);
        assert_eq!(usmall_int_value, usmall_int_value_borrowed);
        assert_eq!(usmall_int_value_borrowed, &usmall_int_value);
    }

    #[test]
    fn test_uint_value() {
        let uint_value = Value::UInt(5000);
        assert_eq!(uint_value.ty(), Ty::UInt);
        assert_eq!(uint_value.to_sql_value(), "5000".to_string());
        assert_eq!(uint_value.to_string(), Ok("5000".to_string()));
        assert_eq!(
            uint_value.to_json_value(),
            serde_json::Value::Number(5000.into())
        );
        assert_eq!(format!("{}", uint_value), "5000");
        let uint_value_borrowed = uint_value.to_borrowed_value();
        assert_eq!(uint_value_borrowed.ty(), Ty::UInt);
        assert_eq!(uint_value_borrowed.to_sql_value(), "5000".to_string());
        assert_eq!(uint_value_borrowed.to_string(), Ok("5000".to_string()));
        assert_eq!(
            uint_value_borrowed.to_json_value(),
            serde_json::Value::Number(5000.into())
        );
        assert_eq!(format!("{}", uint_value_borrowed), "5000");
        println!("{:?}", uint_value_borrowed.to_str());
        assert_eq!(uint_value_borrowed.to_bool(), Some(true));
        assert_eq!(uint_value_borrowed.to_value(), uint_value);
        assert_eq!(uint_value_borrowed.clone().into_value(), uint_value);
        assert_eq!(uint_value_borrowed, uint_value);
        assert_eq!(uint_value, uint_value_borrowed);
        assert_eq!(uint_value_borrowed, &uint_value);
    }

    #[test]
    fn test_ubig_int_value() {
        let ubig_int_value = Value::UBigInt(1234567890);
        assert_eq!(ubig_int_value.ty(), Ty::UBigInt);
        assert_eq!(ubig_int_value.to_sql_value(), "1234567890".to_string());
        assert_eq!(ubig_int_value.to_string(), Ok("1234567890".to_string()));
        assert_eq!(
            ubig_int_value.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );
        assert_eq!(format!("{}", ubig_int_value), "1234567890");
        let ubig_int_value_borrowed = ubig_int_value.to_borrowed_value();
        assert_eq!(ubig_int_value_borrowed.ty(), Ty::UBigInt);
        assert_eq!(
            ubig_int_value_borrowed.to_sql_value(),
            "1234567890".to_string()
        );
        assert_eq!(
            ubig_int_value_borrowed.to_string(),
            Ok("1234567890".to_string())
        );
        assert_eq!(
            ubig_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );
        assert_eq!(format!("{}", ubig_int_value_borrowed), "1234567890");
        println!("{:?}", ubig_int_value_borrowed.to_str());
        assert_eq!(ubig_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(ubig_int_value_borrowed.to_value(), ubig_int_value);
        assert_eq!(ubig_int_value_borrowed.clone().into_value(), ubig_int_value);
        assert_eq!(ubig_int_value_borrowed, ubig_int_value);
        assert_eq!(ubig_int_value, ubig_int_value_borrowed);
        assert_eq!(ubig_int_value_borrowed, &ubig_int_value);
    }

    #[test]
    fn test_float_value() {
        let float_value = Value::Float(3.14);
        assert_eq!(float_value.ty(), Ty::Float);
        assert_eq!(float_value.to_sql_value(), "3.14".to_string());
        assert_eq!(float_value.to_string(), Ok("3.14".to_string()));
        println!("{:?}", float_value.to_json_value());
        assert_eq!(format!("{}", float_value), "3.14");
        let float_value_borrowed = float_value.to_borrowed_value();
        assert_eq!(float_value_borrowed.ty(), Ty::Float);
        assert_eq!(float_value_borrowed.to_sql_value(), "3.14".to_string());
        assert_eq!(float_value_borrowed.to_string(), Ok("3.14".to_string()));
        println!("{:?}", float_value_borrowed.to_json_value());
        assert_eq!(format!("{}", float_value_borrowed), "3.14");
        println!("{:?}", float_value_borrowed.to_str());
        assert_eq!(float_value_borrowed.to_bool(), Some(true));
        assert_eq!(float_value_borrowed.to_value(), float_value);
        assert_eq!(float_value_borrowed.clone().into_value(), float_value);
        assert_eq!(float_value_borrowed, float_value);
        assert_eq!(float_value, float_value_borrowed);
        assert_eq!(float_value_borrowed, &float_value);
    }

    #[test]
    fn test_double_value() {
        let double_value = Value::Double(2.71828);
        assert_eq!(double_value.ty(), Ty::Double);
        assert_eq!(double_value.to_sql_value(), "2.71828".to_string());
        assert_eq!(double_value.to_string(), Ok("2.71828".to_string()));
        println!("{:?}", double_value.to_json_value());
        assert_eq!(format!("{}", double_value), "2.71828");
        let double_value_borrowed = double_value.to_borrowed_value();
        assert_eq!(double_value_borrowed.ty(), Ty::Double);
        assert_eq!(double_value_borrowed.to_sql_value(), "2.71828".to_string());
        assert_eq!(double_value_borrowed.to_string(), Ok("2.71828".to_string()));
        println!("{:?}", double_value_borrowed.to_json_value());
        assert_eq!(format!("{}", double_value_borrowed), "2.71828");
        println!("{:?}", double_value_borrowed.to_str());
        assert_eq!(double_value_borrowed.to_bool(), Some(true));
        assert_eq!(double_value_borrowed.to_value(), double_value);
        assert_eq!(double_value_borrowed.clone().into_value(), double_value);
        assert_eq!(double_value_borrowed, double_value);
        assert_eq!(double_value, double_value_borrowed);
        assert_eq!(double_value_borrowed, &double_value);
    }

    #[test]
    fn test_var_char_value() {
        let varchar_value = Value::VarChar("hello".to_string());
        assert_eq!(varchar_value.ty(), Ty::VarChar);
        assert_eq!(varchar_value.to_sql_value(), "\"hello\"".to_string());
        assert_eq!(varchar_value.to_string(), Ok("hello".to_string()));
        assert_eq!(
            varchar_value.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(format!("{}", varchar_value), "hello");
        let varchar_value_borrowed = varchar_value.to_borrowed_value();
        assert_eq!(varchar_value_borrowed.ty(), Ty::VarChar);
        assert_eq!(
            varchar_value_borrowed.to_sql_value(),
            "\"hello\"".to_string()
        );
        assert_eq!(varchar_value_borrowed.to_string(), Ok("hello".to_string()));
        assert_eq!(
            varchar_value_borrowed.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(format!("{}", varchar_value_borrowed), "hello");
        println!("{:?}", varchar_value_borrowed.to_str());
        assert_eq!(varchar_value_borrowed.to_bool(), Some(true));
        assert_eq!(varchar_value_borrowed.to_value(), varchar_value);
        assert_eq!(varchar_value_borrowed.clone().into_value(), varchar_value);
        assert_eq!(varchar_value_borrowed, varchar_value);
        assert_eq!(varchar_value, varchar_value_borrowed);
        assert_eq!(varchar_value_borrowed, &varchar_value);
    }

    #[test]
    fn test_timestamp_value() {
        let timestamp_value = Value::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert_eq!(timestamp_value.ty(), Ty::Timestamp);
        assert_eq!(timestamp_value.to_sql_value(), "1".to_string());
        println!("{:?}", timestamp_value.to_string());
        assert_eq!(
            timestamp_value.to_json_value(),
            serde_json::Value::Number(1.into())
        );
        println!("{}", format!("{}", timestamp_value));
        let timestamp_value_borrowed = timestamp_value.to_borrowed_value();
        assert_eq!(timestamp_value_borrowed.ty(), Ty::Timestamp);
        assert_eq!(timestamp_value_borrowed.to_sql_value(), "1".to_string());
        println!("{:?}", timestamp_value_borrowed.to_string());
        assert_eq!(
            timestamp_value_borrowed.to_json_value(),
            serde_json::Value::Number(1.into())
        );
        println!("{}", format!("{}", timestamp_value_borrowed));
        println!("{:?}", timestamp_value_borrowed.to_str());
        assert_eq!(timestamp_value_borrowed.to_bool(), Some(true));
        assert_eq!(timestamp_value_borrowed.to_value(), timestamp_value);
        assert_eq!(
            timestamp_value_borrowed.clone().into_value(),
            timestamp_value
        );
        assert_eq!(timestamp_value_borrowed, timestamp_value);
        assert_eq!(timestamp_value, timestamp_value_borrowed);
        assert_eq!(timestamp_value_borrowed, &timestamp_value);
    }

    #[test]
    fn test_nchar_value() {
        let nchar_value = Value::NChar("hello".to_string());
        assert_eq!(nchar_value.ty(), Ty::NChar);
        assert_eq!(nchar_value.to_sql_value(), "\"hello\"".to_string());
        assert_eq!(nchar_value.to_string(), Ok("hello".to_string()));
        assert_eq!(
            nchar_value.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(format!("{}", nchar_value), "hello");
        let nchar_value_borrowed = nchar_value.to_borrowed_value();
        assert_eq!(nchar_value_borrowed.ty(), Ty::NChar);
        assert_eq!(nchar_value_borrowed.to_sql_value(), "\"hello\"".to_string());
        assert_eq!(nchar_value_borrowed.to_string(), Ok("hello".to_string()));
        assert_eq!(
            nchar_value_borrowed.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(format!("{}", nchar_value_borrowed), "hello");
        println!("{:?}", nchar_value_borrowed.to_str());
        assert_eq!(nchar_value_borrowed.to_bool(), Some(true));
        assert_eq!(nchar_value_borrowed.to_value(), nchar_value);
        assert_eq!(nchar_value_borrowed.clone().into_value(), nchar_value);
        assert_eq!(nchar_value_borrowed, nchar_value);
        assert_eq!(nchar_value, nchar_value_borrowed);
        assert_eq!(nchar_value_borrowed, &nchar_value);
    }

    #[test]
    fn test_json_value() {
        let json_value = Value::Json(serde_json::json!({"hello": "world"}));
        assert_eq!(json_value.ty(), Ty::Json);
        assert_eq!(
            json_value.to_sql_value(),
            "\"{\"hello\":\"world\"}\"".to_string()
        );
        assert_eq!(
            json_value.to_string(),
            Ok("{\"hello\":\"world\"}".to_string())
        );
        assert_eq!(
            json_value.to_json_value(),
            serde_json::json!({"hello": "world"})
        );
        assert_eq!(format!("{}", json_value), "{\"hello\":\"world\"}");
        let json_value_borrowed = json_value.to_borrowed_value();
        assert_eq!(json_value_borrowed.ty(), Ty::Json);
        assert_eq!(
            json_value_borrowed.to_sql_value(),
            "\"{\"hello\":\"world\"}\"".to_string()
        );
        assert_eq!(
            json_value_borrowed.to_string(),
            Ok("{\"hello\":\"world\"}".to_string())
        );
        assert_eq!(
            json_value_borrowed.to_json_value(),
            serde_json::json!({"hello": "world"})
        );
        assert_eq!(
            format!("{}", json_value_borrowed),
            "{\\\"hello\\\":\\\"world\\\"}"
        );
        println!("{:?}", json_value_borrowed.to_str());
        assert_eq!(json_value_borrowed.to_bool(), Some(true));
        assert_eq!(json_value_borrowed.to_value(), json_value);
        assert_eq!(json_value_borrowed.clone().into_value(), json_value);
        assert_eq!(json_value_borrowed, json_value);
        assert_eq!(json_value, json_value_borrowed);
        assert_eq!(json_value_borrowed, &json_value);
    }

    #[test]
    fn test_ty() {
        let null_value = BorrowedValue::Null(Ty::Int);
        assert_eq!(null_value.ty(), Ty::Int);

        let bool_value = BorrowedValue::Bool(true);
        assert_eq!(bool_value.ty(), Ty::Bool);

        let tiny_int_value = BorrowedValue::TinyInt(42);
        assert_eq!(tiny_int_value.ty(), Ty::TinyInt);

        let small_int_value = BorrowedValue::SmallInt(1000);
        assert_eq!(small_int_value.ty(), Ty::SmallInt);

        let int_value = BorrowedValue::Int(-500);
        assert_eq!(int_value.ty(), Ty::Int);

        let big_int_value = BorrowedValue::BigInt(1234567890);
        assert_eq!(big_int_value.ty(), Ty::BigInt);

        let utiny_int_value = BorrowedValue::UTinyInt(42);
        assert_eq!(utiny_int_value.ty(), Ty::UTinyInt);

        let usmall_int_value = BorrowedValue::USmallInt(1000);
        assert_eq!(usmall_int_value.ty(), Ty::USmallInt);

        let uint_value = BorrowedValue::UInt(5000);
        assert_eq!(uint_value.ty(), Ty::UInt);

        let ubig_int_value = BorrowedValue::UBigInt(1234567890);
        assert_eq!(ubig_int_value.ty(), Ty::UBigInt);

        let float_value = BorrowedValue::Float(3.14);
        assert_eq!(float_value.ty(), Ty::Float);

        let double_value = BorrowedValue::Double(2.71828);
        assert_eq!(double_value.ty(), Ty::Double);

        let varchar_value = BorrowedValue::VarChar("hello");
        assert_eq!(varchar_value.ty(), Ty::VarChar);

        let timestamp_value = BorrowedValue::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert_eq!(timestamp_value.ty(), Ty::Timestamp);

        let blob_value = BorrowedValue::Blob(&[1, 2, 3]);
        assert_eq!(blob_value.ty(), Ty::Blob);

        let medium_blob_value = BorrowedValue::MediumBlob(&[1, 2, 3]);
        assert_eq!(medium_blob_value.ty(), Ty::MediumBlob);
    }

    #[test]
    fn test_to_sql_value() {
        let null_value = BorrowedValue::Null(Ty::Int);
        assert_eq!(null_value.to_sql_value(), "NULL".to_string());

        let bool_value = BorrowedValue::Bool(true);
        assert_eq!(bool_value.to_sql_value(), "true".to_string());

        let tiny_int_value = BorrowedValue::TinyInt(42);
        assert_eq!(tiny_int_value.to_sql_value(), "42".to_string());

        let small_int_value = BorrowedValue::SmallInt(1000);
        assert_eq!(small_int_value.to_sql_value(), "1000".to_string());

        let int_value = BorrowedValue::Int(-500);
        assert_eq!(int_value.to_sql_value(), "-500".to_string());

        let big_int_value = BorrowedValue::BigInt(1234567890);
        assert_eq!(big_int_value.to_sql_value(), "1234567890".to_string());

        let utiny_int_value = BorrowedValue::UTinyInt(42);
        assert_eq!(utiny_int_value.to_sql_value(), "42".to_string());

        let usmall_int_value = BorrowedValue::USmallInt(1000);
        assert_eq!(usmall_int_value.to_sql_value(), "1000".to_string());

        let uint_value = BorrowedValue::UInt(5000);
        assert_eq!(uint_value.to_sql_value(), "5000".to_string());

        let ubig_int_value = BorrowedValue::UBigInt(1234567890);
        assert_eq!(ubig_int_value.to_sql_value(), "1234567890".to_string());

        let float_value = BorrowedValue::Float(3.14);
        assert_eq!(float_value.to_sql_value(), "3.14".to_string());

        let double_value = BorrowedValue::Double(2.71828);
        assert_eq!(double_value.to_sql_value(), "2.71828".to_string());

        let varchar_value = BorrowedValue::VarChar("hello");
        assert_eq!(varchar_value.to_sql_value(), "\"hello\"".to_string());

        let timestamp_value = BorrowedValue::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert_eq!(timestamp_value.to_sql_value(), "1".to_string());

        let nchar_value = Value::NChar("hello".to_string());
        let b_nchar_value = nchar_value.to_borrowed_value();
        assert_eq!(b_nchar_value.to_sql_value(), "\"hello\"".to_string());
    }

    #[test]
    fn test_to_json_value() {
        let null_value = BorrowedValue::Null(Ty::Int);
        assert_eq!(null_value.to_json_value(), serde_json::Value::Null);

        let bool_value = BorrowedValue::Bool(true);
        assert_eq!(bool_value.to_json_value(), serde_json::Value::Bool(true));

        let tiny_int_value = BorrowedValue::TinyInt(42);
        assert_eq!(
            tiny_int_value.to_json_value(),
            serde_json::Value::Number(42.into())
        );

        let small_int_value = BorrowedValue::SmallInt(1000);
        assert_eq!(
            small_int_value.to_json_value(),
            serde_json::Value::Number(1000.into())
        );

        let int_value = BorrowedValue::Int(-500);
        assert_eq!(
            int_value.to_json_value(),
            serde_json::Value::Number((-500).into())
        );

        let big_int_value = BorrowedValue::BigInt(1234567890);
        assert_eq!(
            big_int_value.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );

        let utiny_int_value = BorrowedValue::UTinyInt(42);
        assert_eq!(
            utiny_int_value.to_json_value(),
            serde_json::Value::Number(42.into())
        );

        let usmall_int_value = BorrowedValue::USmallInt(1000);
        assert_eq!(
            usmall_int_value.to_json_value(),
            serde_json::Value::Number(1000.into())
        );

        let uint_value = BorrowedValue::UInt(5000);
        assert_eq!(
            uint_value.to_json_value(),
            serde_json::Value::Number(5000.into())
        );

        let ubig_int_value = BorrowedValue::UBigInt(1234567890);
        assert_eq!(
            ubig_int_value.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );

        let float_value = BorrowedValue::Float(3.14);
        assert_eq!(
            float_value.to_json_value(),
            serde_json::json!(3.140000104904175)
        );

        let double_value = BorrowedValue::Double(2.71828);
        assert_eq!(double_value.to_json_value(), serde_json::json!(2.71828));

        let varchar_value = BorrowedValue::VarChar("hello");
        assert_eq!(
            varchar_value.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );

        let timestamp_value = BorrowedValue::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert_eq!(
            timestamp_value.to_json_value(),
            serde_json::Value::Number(1.into())
        );

        let json_value = Value::Json(serde_json::json!({"hello": "world"}));
        let b_json_value = json_value.to_borrowed_value();
        assert_eq!(
            b_json_value.to_json_value(),
            serde_json::json!({"hello": "world"})
        );

        let nchar_value = Value::NChar("hello".to_string());
        let b_nchar_value = nchar_value.to_borrowed_value();
        assert_eq!(
            b_nchar_value.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
    }
}
