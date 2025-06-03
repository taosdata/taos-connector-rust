use std::fmt::{self, Display};
use std::os::raw::c_char;
use std::str::FromStr;

use serde::de::Visitor;

/// TDengine data type enumeration.
///
/// | enum      | int | sql name          | rust type              |
/// | ----      |:---:| --- ----          |:---------:             |
/// | Null      | 0   | NULL              | None                   |
/// | Bool      | 1   | BOOL              | bool                   |
/// | TinyInt   | 2   | TINYINT           | i8                     |
/// | SmallInt  | 3   | SMALLINT          | i16                    |
/// | Int       | 4   | INT               | i32                    |
/// | BitInt    | 5   | BIGINT            | i64                    |
/// | Float     | 6   | FLOAT             | f32                    |
/// | Double    | 7   | DOUBLE            | f64                    |
/// | VarChar   | 8   | BINARY/VARCHAR    | str/String             |
/// | Timestamp | 9   | TIMESTAMP         | i64                    |
/// | NChar     | 10  | NCHAR             | str/String             |
/// | UTinyInt  | 11  | TINYINT UNSIGNED  | u8                     |
/// | USmallInt | 12  | SMALLINT UNSIGNED | u16                    |
/// | UInt      | 13  | INT UNSIGNED      | u32                    |
/// | UBigInt   | 14  | BIGINT UNSIGNED   | u64                    |
/// | Json      | 15  | JSON              | serde_json::Value      |
/// | VarBinary | 16  | VARBINARY         | Vec<u8>                |
/// | Decimal   | 17  | DECIMAL           | bigdecimal::BigDecimal |
/// | Blob      | 18  | BLOB              | Vec<u8>                |
/// | Geometry  | 20  | GEOMETRY          | Vec<u8>                |
/// | Decimal64 | 21  | DECIMAL           | bigdecimal::BigDecimal |
///
/// Notes:
/// - VarChar sql name is BINARY in TDengine 2.0, and VARCHAR in 3.0.
/// - Decimal and Blob types are not supported in TDengine 2.0.
/// - MediumBlob type is not supported in TDengine 2.0 and 3.0.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_repr::Serialize_repr)]
#[repr(u8)]
#[non_exhaustive]
#[derive(Default)]
pub enum Ty {
    /// Null is only a value, not a *real* type, a nullable data type could be represented as [`Option<T>`] in Rust.
    ///
    /// A data type should never be Null.
    #[doc(hidden)]
    #[default]
    Null = 0,
    /// The `BOOL` type in sql, will be represented as [bool] in Rust.
    Bool = 1,
    /// The `TINYINT` type in sql, will be represented as [i8] in Rust.
    TinyInt = 2,
    /// The `SMALLINT` type in sql, will be represented as [i16] in Rust.
    SmallInt = 3,
    /// The `INT` type in sql, will be represented as [i32] in Rust.
    Int = 4,
    /// The `BIGINT` type in sql, will be represented as [i64] in Rust.
    BigInt = 5,
    /// The `FLOAT` type in sql, will be represented as [f32] in Rust.
    Float = 6,
    /// The `DOUBLE` type in sql, will be represented as [f64] in Rust.
    Double = 7,
    /// The `BINARY` type in sql for TDengine 2.x, `VARCHAR` for TDengine 3.x,
    /// will be represented as [&str] or [String] in Rust. This type of data be deserialized to [`Vec<u8>`].
    VarChar = 8,
    /// The `TIMESTAMP` type in sql, will be represented as [i64] in Rust.
    /// But can be deserialized to [chrono::naive::NaiveDateTime] or [String].
    Timestamp = 9,
    /// The `NCHAR` type in sql, will be represented as [&str] or [String] in Rust.
    /// The recommended way in TDengine to store utf-8 [String].
    NChar = 10,
    /// The `TINYINT UNSIGNED` type in sql, will be represented as [u8] in Rust.
    UTinyInt = 11,
    /// The `SMALLINT UNSIGNED` type in sql, will be represented as [u16] in Rust.
    USmallInt = 12,
    /// The `INT UNSIGNED` type in sql, will be represented as [u32] in Rust.
    UInt = 13,
    /// The `BIGINT UNSIGNED` type in sql, will be represented as [u64] in Rust.
    UBigInt = 14,
    /// The `JSON` type in sql, will be represented as [serde_json::value::Value] in Rust.
    Json = 15,
    /// The `VARBINARY` type in sql, will be represented as [`Vec<u8>`] in Rust.
    VarBinary = 16,
    /// The `DECIMAL` type in sql, will be represented as [bigdecimal::BigDecimal] in Rust.
    Decimal = 17,
    /// The `BLOB` type in sql, will be represented as [`Vec<u8>`] in Rust.
    Blob = 18,
    /// Not supported now.
    #[doc(hidden)]
    MediumBlob = 19,
    /// The `GEOMETRY` type in sql, will be represented as [`Vec<u8>`] in Rust.
    Geometry = 20,
    /// The `DECIMAL` type in sql, will be represented as [bigdecimal::BigDecimal] in Rust.
    Decimal64 = 21,
}

impl<'de> serde::Deserialize<'de> for Ty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TyVisitor;

        impl<'de> Visitor<'de> for TyVisitor {
            type Value = Ty;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("invalid TDengine type")
            }
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::from_u8(v as u8))
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::from_u8(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::from_u8(v as u8))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ty::from_str(v).map_err(<E as serde::de::Error>::custom)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_any(self)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::Null)
            }
        }

        deserializer.deserialize_any(TyVisitor)
    }
}

impl FromStr for Ty {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        if let Some(s) = s.trim().strip_prefix("decimal") {
            let mut ps = s
                .split_terminator(['(', ',', ')'])
                .filter(|s| !s.trim().is_empty());
            let Some(precision) = ps.next() else {
                return Err("decimal type precision or scale not found".to_string());
            };
            let precision: u8 = precision
                .trim()
                .parse()
                .map_err(|e: std::num::ParseIntError| e.to_string())?;
            if precision <= 18 {
                return Ok(Self::Decimal64);
            }
            return Ok(Self::Decimal);
        }
        match s.as_str() {
            "timestamp" => Ok(Ty::Timestamp),
            "bool" => Ok(Ty::Bool),
            "tinyint" => Ok(Ty::TinyInt),
            "smallint" => Ok(Ty::SmallInt),
            "int" => Ok(Ty::Int),
            "bigint" => Ok(Ty::BigInt),
            "tinyint unsigned" => Ok(Ty::UTinyInt),
            "smallint unsigned" => Ok(Ty::USmallInt),
            "int unsigned" => Ok(Ty::UInt),
            "bigint unsigned" => Ok(Ty::UBigInt),
            "float" => Ok(Ty::Float),
            "double" => Ok(Ty::Double),
            "binary" | "varchar" => Ok(Ty::VarChar),
            "nchar" => Ok(Ty::NChar),
            "json" => Ok(Ty::Json),
            "varbinary" => Ok(Ty::VarBinary),
            "blob" => Ok(Ty::Blob),
            "mediumblob" => Ok(Ty::MediumBlob),
            "geometry" => Ok(Ty::Geometry),
            s => Err(format!("not a valid data type string: {s}")),
        }
    }
}

impl Ty {
    /// Check if the data type is null or not.
    pub const fn is_null(&self) -> bool {
        matches!(self, Ty::Null)
    }

    /// Var type is one of [Ty::VarChar], [Ty::VarBinary], [Ty::NChar], [Ty::Geometry].
    pub const fn is_var_type(&self) -> bool {
        use Ty::*;
        matches!(self, VarChar | VarBinary | NChar | Geometry)
    }

    // /// Check if the data type need quotes, means one of [Ty::VarChar], [Ty::NChar], [Ty::Json].
    // pub const fn is_quote(&self) -> bool {
    //     use Ty::*;
    //     matches!(self, Json)
    // }

    pub const fn is_json(&self) -> bool {
        matches!(self, Ty::Json)
    }

    /// Is one of boolean/integers/float/double/decimal
    pub const fn is_primitive(&self) -> bool {
        use Ty::*;
        matches!(
            self,
            Bool | TinyInt
                | SmallInt
                | Int
                | BigInt
                | UTinyInt
                | USmallInt
                | UInt
                | UBigInt
                | Float
                | Double
                | Timestamp
                | Decimal
                | Decimal64
        )
    }

    /// Get fixed length if the type is primitive.
    pub const fn fixed_length(&self) -> usize {
        use Ty::*;
        match self {
            Bool | TinyInt | UTinyInt => 1,
            SmallInt | USmallInt => 2,
            Int | UInt | Float => 4,
            BigInt | Double | Timestamp | UBigInt | Decimal64 => 8,
            Decimal => 16,
            _ => 0,
        }
    }

    /// The sql name of type.
    pub const fn name(&self) -> &'static str {
        use Ty::*;
        match self {
            Null => "NULL",
            Bool => "BOOL",
            TinyInt => "TINYINT",
            SmallInt => "SMALLINT",
            Int => "INT",
            BigInt => "BIGINT",
            Float => "FLOAT",
            Double => "DOUBLE",
            VarChar => "BINARY",
            Timestamp => "TIMESTAMP",
            NChar => "NCHAR",
            UTinyInt => "TINYINT UNSIGNED",
            USmallInt => "SMALLINT UNSIGNED",
            UInt => "INT UNSIGNED",
            UBigInt => "BIGINT UNSIGNED",
            Json => "JSON",
            VarBinary => "VARBINARY",
            Decimal | Decimal64 => "DECIMAL",
            Blob => "BLOB",
            MediumBlob => "MEDIUMBLOB",
            Geometry => "GEOMETRY",
        }
    }

    pub const fn lowercase_name(&self) -> &'static str {
        use Ty::*;
        match self {
            Null => "null",
            Bool => "bool",
            TinyInt => "tinyint",
            SmallInt => "smallint",
            Int => "int",
            BigInt => "bigint",
            Float => "float",
            Double => "double",
            VarChar => "binary",
            Timestamp => "timestamp",
            NChar => "nchar",
            UTinyInt => "tinyint unsigned",
            USmallInt => "smallint unsigned",
            UInt => "int unsigned",
            UBigInt => "bigint unsigned",
            Json => "json",
            VarBinary => "varbinary",
            Decimal | Decimal64 => "decimal",
            Blob => "blob",
            MediumBlob => "mediumblob",
            Geometry => "geometry",
        }
    }

    pub const fn tsdb_name(&self) -> *const c_char {
        use Ty::*;
        match self {
            Null => "TSDB_DATA_TYPE_NULL\0".as_ptr() as *const c_char,
            Bool => "TSDB_DATA_TYPE_BOOL\0".as_ptr() as *const c_char,
            TinyInt => "TSDB_DATA_TYPE_TINYINT\0".as_ptr() as *const c_char,
            SmallInt => "TSDB_DATA_TYPE_SMALLINT\0".as_ptr() as *const c_char,
            Int => "TSDB_DATA_TYPE_INT\0".as_ptr() as *const c_char,
            BigInt => "TSDB_DATA_TYPE_BIGINT\0".as_ptr() as *const c_char,
            Float => "TSDB_DATA_TYPE_FLOAT\0".as_ptr() as *const c_char,
            Double => "TSDB_DATA_TYPE_DOUBLE\0".as_ptr() as *const c_char,
            VarChar => "TSDB_DATA_TYPE_VARCHAR\0".as_ptr() as *const c_char,
            Timestamp => "TSDB_DATA_TYPE_TIMESTAMP\0".as_ptr() as *const c_char,
            NChar => "TSDB_DATA_TYPE_NCHAR\0".as_ptr() as *const c_char,
            UTinyInt => "TSDB_DATA_TYPE_UTINYINT\0".as_ptr() as *const c_char,
            USmallInt => "TSDB_DATA_TYPE_USMALLINT\0".as_ptr() as *const c_char,
            UInt => "TSDB_DATA_TYPE_UINT\0".as_ptr() as *const c_char,
            UBigInt => "TSDB_DATA_TYPE_UBIGINT\0".as_ptr() as *const c_char,
            Json => "TSDB_DATA_TYPE_JSON\0".as_ptr() as *const c_char,
            VarBinary => "TSDB_DATA_TYPE_VARBINARY\0".as_ptr() as *const c_char,
            Decimal => "TSDB_DATA_TYPE_DECIMAL\0".as_ptr() as *const c_char,
            Decimal64 => "TSDB_DATA_TYPE_DECIMAL64\0".as_ptr() as *const c_char,
            Blob => "TSDB_DATA_TYPE_BLOB\0".as_ptr() as *const c_char,
            MediumBlob => "TSDB_DATA_TYPE_MEDIUMBLOB\0".as_ptr() as *const c_char,
            Geometry => "TSDB_DATA_TYPE_GEOMETRY\0".as_ptr() as *const c_char,
        }
    }

    #[inline]
    pub const fn from_u8_option(v: u8) -> Option<Self> {
        use Ty::*;
        match v {
            0 => Some(Null),
            1 => Some(Bool),
            2 => Some(TinyInt),
            3 => Some(SmallInt),
            4 => Some(Int),
            5 => Some(BigInt),
            6 => Some(Float),
            7 => Some(Double),
            8 => Some(VarChar),
            9 => Some(Timestamp),
            10 => Some(NChar),
            11 => Some(UTinyInt),
            12 => Some(USmallInt),
            13 => Some(UInt),
            14 => Some(UBigInt),
            15 => Some(Json),
            16 => Some(VarBinary),
            17 => Some(Decimal),
            18 => Some(Blob),
            19 => Some(MediumBlob),
            20 => Some(Geometry),
            21 => Some(Decimal64),
            _ => None,
        }
    }

    /// The enum constants directly to str.
    #[inline]
    pub(crate) const fn as_variant_str(&self) -> &'static str {
        use Ty::*;
        macro_rules! _var_str {
          ($($v:ident) *) => {
              match self {
                $($v => stringify!($v),) *
              }
          }
        }
        _var_str!(
            Null Bool TinyInt SmallInt Int BigInt UTinyInt USmallInt UInt UBigInt
            Float Double VarChar NChar Timestamp Json VarBinary Decimal Decimal64 Blob MediumBlob Geometry
        )
    }

    #[inline]
    const fn from_u8(v: u8) -> Self {
        use Ty::*;
        match v {
            0 => Null,
            1 => Bool,
            2 => TinyInt,
            3 => SmallInt,
            4 => Int,
            5 => BigInt,
            6 => Float,
            7 => Double,
            8 => VarChar,
            9 => Timestamp,
            10 => NChar,
            11 => UTinyInt,
            12 => USmallInt,
            13 => UInt,
            14 => UBigInt,
            15 => Json,
            16 => VarBinary,
            17 => Decimal,
            18 => Blob,
            19 => MediumBlob,
            20 => Geometry,
            21 => Decimal64,
            _ => panic!("unknown data type"),
        }
    }
}
impl From<u8> for Ty {
    #[inline]
    fn from(v: u8) -> Self {
        unsafe { std::mem::transmute(v) }
    }
}

impl Display for Ty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
macro_rules! _impl_from_primitive {
    ($($ty:ty) *) => {
      $(
         impl From<$ty> for Ty {
            #[inline]
           fn from(v: $ty) -> Self {
             Self::from_u8(v as _)
           }
         }
      )*
    }
}

_impl_from_primitive!(i8 i16 i32 i64 u16 u32 u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_ty_test() -> anyhow::Result<()> {
        assert_eq!("DECIMAL(5,2)".parse::<Ty>().unwrap(), Ty::Decimal64);
        assert_eq!("DECIMAL(20,2)".parse::<Ty>().unwrap(), Ty::Decimal);

        assert_eq!(Ty::from_u8(17), Ty::Decimal);
        assert_eq!(Ty::from_u8(21), Ty::Decimal64);

        assert_eq!(Ty::Decimal.as_variant_str(), "Decimal");
        assert_eq!(Ty::Decimal64.as_variant_str(), "Decimal64");

        assert_eq!(Ty::Decimal.fixed_length(), 16);
        assert_eq!(Ty::Decimal64.fixed_length(), 8);

        assert!(!Ty::Decimal.is_json());
        assert!(!Ty::Decimal64.is_json());

        assert!(Ty::Decimal.is_primitive());
        assert!(Ty::Decimal64.is_primitive());

        assert_eq!(Ty::Decimal.name(), "DECIMAL");
        assert_eq!(Ty::Decimal64.name(), "DECIMAL");

        assert_eq!(Ty::Decimal.lowercase_name(), "decimal");
        assert_eq!(Ty::Decimal64.lowercase_name(), "decimal");

        assert_eq!(Ty::from_u8_option(17), Some(Ty::Decimal));
        assert_eq!(Ty::from_u8_option(21), Some(Ty::Decimal64));

        assert_eq!(Ty::from(17), Ty::Decimal);
        assert_eq!(Ty::from(21), Ty::Decimal64);

        assert_eq!(format!("{}", Ty::Decimal), "DECIMAL");
        assert_eq!(format!("{}", Ty::Decimal64), "DECIMAL");

        Ok(())
    }
}
