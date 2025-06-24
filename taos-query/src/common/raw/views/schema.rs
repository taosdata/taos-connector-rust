use std::fmt::{self, Display};
use std::num::ParseIntError;
use std::ops::Deref;
use std::{fmt::Debug, str::FromStr};

use bytes::Bytes;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::common::Ty;

#[derive(Debug, Clone, Copy, Eq, KnownLayout, IntoBytes, FromBytes, Unaligned, Immutable)]
#[repr(C)]
pub struct PrecScale {
    pub scale: u8,
    pub prec: u8,
    _empty: u8,
    pub len: u8,
}

impl PrecScale {
    /// Create a new `PrecScale` with the given length, precision, and scale.
    #[inline]
    pub const fn new(len: u8, precision: u8, scale: u8) -> Self {
        Self {
            len,
            _empty: 0,
            prec: precision,
            scale,
        }
    }
}

impl PartialEq for PrecScale {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.prec == other.prec && self.scale == other.scale
    }
}

#[derive(Clone, Copy, KnownLayout, FromBytes, Immutable)]
#[repr(C)]
pub union LenOrDec {
    pub len: u32,
    pub dec: PrecScale,
}

impl LenOrDec {
    #[inline]
    pub const fn new_len(len: u32) -> Self {
        Self { len }
    }

    #[inline]
    pub const fn new_dec(len: u32, precision: u8, scale: u8) -> Self {
        Self {
            dec: PrecScale {
                len: len as u8,
                _empty: 0,
                prec: precision,
                scale,
            },
        }
    }
}

/// Represent column basics information: type, length.
#[derive(
    Clone,
    Copy,
    KnownLayout,
    TryFromBytes,
    Immutable,
    DeserializeFromStr,
    SerializeDisplay,
    Unaligned,
)]
#[repr(C)]
#[repr(packed(1))]
pub struct ColSchema {
    pub ty: Ty,
    pub attr: LenOrDec,
}

impl PartialEq for ColSchema {
    fn eq(&self, other: &Self) -> bool {
        self.ty == other.ty
            && self.len() == other.len()
            && self.precision() == other.precision()
            && self.scale() == other.scale()
    }
}
impl Eq for ColSchema {}

impl Debug for ColSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("ColSchema");
        debug.field("ty", &self.ty);
        if self.ty.is_decimal() {
            let dec = unsafe { self.attr.dec };
            debug
                .field("len", &dec.len)
                .field("prec", &dec.prec)
                .field("scale", &dec.scale);
        } else {
            let len = unsafe { self.attr.len };
            debug.field("len", &len);
        }
        debug.finish()
    }
}

impl ColSchema {
    pub fn as_ty(&self) -> Ty {
        self.ty
    }
}

impl Display for ColSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ty.is_decimal() {
            let dec = unsafe { self.attr.dec };
            write!(f, "{}({},{})", self.ty, dec.prec, dec.scale)
        } else if self.ty.is_var_type() {
            write!(f, "{}({})", self.ty, unsafe { self.positive_var_len() })
        } else {
            write!(f, "{}", self.ty)
        }
    }
}

const MAX_DECIMAL_PRECISION: u8 = 38;
const MAX_DECIMAL64_PRECISION: u8 = 18;
const MIN_DECIMAL_SCALE: u8 = 0;

#[derive(thiserror::Error, Debug)]
pub enum ParseDataTypeError {
    #[error("Invalid data type number: {0}")]
    TypeNumberError(u8),
    #[error("Parse DECIMAL precision error: {0}")]
    ParseDecimalPrecisionError(ParseIntError),
    #[error("Parse DECIMAL scale error")]
    ParseDecimalScaleError(ParseIntError),
    #[error("Invalid DECIMAL precision: {0}")]
    InvalidDecimalPrecision(u8),
    #[error("Invalid DECIMAL scale: {0}")]
    InvalidDecimalScale(u8),
    #[error("Invalid VARCHAR length: {0}")]
    InvalidVarCharLength(String),
    #[error("Invalid VARBINARY length: {0}")]
    InvalidVarBinaryLength(String),
    #[error("Invalid NCHAR length: {0}")]
    InvalidNCharLength(String),
    #[error("Invalid GEOMETRY length: {0}")]
    InvalidGeometryLength(String),
    #[error("Invalid data type format: expect {0}, but got {1}")]
    FormatError(&'static str, String),
    #[error("Unknown or unsupported data type: {0}")]
    UnknownDataType(String),
}

impl FromStr for ColSchema {
    type Err = ParseDataTypeError;

    /// Zero-copy compatible parsing of a data type string.
    ///
    /// Requires allocation only when the input string is not a valid data type or in lower cases.
    ///
    /// This function supports parsing various data types, including fixed-length types like TINYINT, SMALLINT, INT, BIGINT,
    /// as well as variable-length types like VARCHAR, BINARY, NCHAR, VARBINARY, GEOMETRY, and JSON.
    /// It also supports decimal types with precision and scale, such as DECIMAL(p,s) or DECIMAL(p).
    /// It returns a `ColSchema` instance representing the parsed data type.
    ///
    /// # Errors
    ///
    /// Returns a `ParseDataTypeError` if the input string is not a valid data type or if the parameters are invalid.
    ///
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if let Ok(i) = u8::from_str(&s) {
            // Handle fixed-length types: TINYINT, SMALLINT, INT, BIGINT, etc.
            return Ok(Self::from_ty(
                Ty::from_u8_option(i).ok_or_else(|| ParseDataTypeError::TypeNumberError(i))?,
            ));
        }

        match s {
            "BOOL" => Ok(Self::from_ty(Ty::Bool)),
            "TINYINT" => Ok(Self::from_ty(Ty::TinyInt)),
            "SMALLINT" => Ok(Self::from_ty(Ty::SmallInt)),
            "INT" => Ok(Self::from_ty(Ty::Int)),
            "BIGINT" => Ok(Self::from_ty(Ty::BigInt)),
            "TINYINT UNSIGNED" => Ok(Self::from_ty(Ty::UTinyInt)),
            "SMALLINT UNSIGNED" => Ok(Self::from_ty(Ty::USmallInt)),
            "INT UNSIGNED" => Ok(Self::from_ty(Ty::UInt)),
            "BIGINT UNSIGNED" => Ok(Self::from_ty(Ty::UBigInt)),
            "FLOAT" => Ok(Self::from_ty(Ty::Float)),
            "DOUBLE" => Ok(Self::from_ty(Ty::Double)),
            "TIMESTAMP" => Ok(Self::from_ty(Ty::Timestamp)),
            "BINARY" | "VARCHAR" => Ok(Self::from_ty(Ty::VarChar)),
            "NCHAR" => Ok(Self::from_ty(Ty::NChar)),
            "JSON" => Ok(Self::from_ty(Ty::Json)),
            "VARBINARY" => Ok(Self::from_ty(Ty::VarBinary)),
            "GEOMETRY" => Ok(Self::from_ty(Ty::Geometry)),
            "BLOB" => Ok(Self::from_ty(Ty::Blob)),
            "MEDIUMBLOB" => Ok(Self::from_ty(Ty::MediumBlob)),
            // Handle types with length, e.g. VARCHAR(100), NCHAR(50), VARBINARY(32), GEOMETRY(128)
            _ => {
                // Handle decimal types: DECIMAL(p,s) or DECIMAL(p)
                if let Some(rest) = s.strip_prefix("DECIMAL") {
                    let params = rest.trim_start_matches('(').trim_end_matches(')').trim();
                    if params.is_empty() {
                        // If no params, fallback to default
                        return Ok(Self::new_decimal(
                            Ty::Decimal64,
                            MAX_DECIMAL64_PRECISION,
                            MIN_DECIMAL_SCALE,
                        ));
                    }
                    let mut parts = params.split(',').map(|x| x.trim());
                    match (parts.next(), parts.next()) {
                        (Some(p), Some(s)) => {
                            let precision: u8 = p
                                .parse()
                                .map_err(ParseDataTypeError::ParseDecimalPrecisionError)?;
                            let scale: u8 = s
                                .parse()
                                .map_err(ParseDataTypeError::ParseDecimalScaleError)?;
                            if scale > precision {
                                return Err(ParseDataTypeError::InvalidDecimalScale(scale));
                            }
                            match precision {
                                1..=MAX_DECIMAL64_PRECISION => {
                                    return Ok(Self::new_decimal(Ty::Decimal64, precision, scale));
                                }
                                1..=MAX_DECIMAL_PRECISION => {
                                    return Ok(Self::new_decimal(Ty::Decimal, precision, scale));
                                }
                                _ => {
                                    return Err(ParseDataTypeError::InvalidDecimalPrecision(
                                        precision,
                                    ));
                                }
                            }
                        }
                        (Some(p), None) => {
                            let precision: u8 = p
                                .parse()
                                .map_err(ParseDataTypeError::ParseDecimalPrecisionError)?;
                            match precision {
                                1..=MAX_DECIMAL64_PRECISION => {
                                    return Ok(Self::new_decimal(Ty::Decimal64, precision, 0));
                                }
                                1..=MAX_DECIMAL_PRECISION => {
                                    return Ok(Self::new_decimal(Ty::Decimal, precision, 0));
                                }
                                _ => {
                                    return Err(ParseDataTypeError::InvalidDecimalPrecision(
                                        precision,
                                    ));
                                }
                            }
                        }
                        _ => {
                            // If no params, fallback to default
                            return Ok(Self::new_decimal(
                                Ty::Decimal64,
                                MAX_DECIMAL64_PRECISION,
                                MIN_DECIMAL_SCALE,
                            ));
                        }
                    }
                }
                if let Some(rest) = s.strip_prefix("VARCHAR") {
                    let params = rest.trim_start_matches('(').trim_end_matches(')').trim();
                    if params.is_empty() {
                        // If no params, fallback to default
                        return Ok(Self::new(Ty::VarBinary, 0));
                    }
                    let mut parts = params.split(',').map(|x| x.trim());
                    match (parts.next(), parts.next()) {
                        (Some(len), None) => {
                            let len: u32 = len.parse().map_err(|_| {
                                ParseDataTypeError::InvalidVarCharLength(len.to_string())
                            })?;
                            return Ok(Self::new(Ty::VarChar, len));
                        }
                        _ => {
                            return Err(ParseDataTypeError::FormatError(
                                "VARCHAR(length)",
                                s.to_string(),
                            ));
                        }
                    }
                }
                if let Some(rest) = s.strip_prefix("BINARY") {
                    let params = rest.trim_start_matches('(').trim_end_matches(')').trim();
                    if params.is_empty() {
                        // If no params, fallback to default
                        return Ok(Self::new(Ty::VarBinary, 0));
                    }
                    let mut parts = params.split(',').map(|x| x.trim());
                    match (parts.next(), parts.next()) {
                        (Some(len), None) => {
                            let len: u32 = len.parse().map_err(|_| {
                                ParseDataTypeError::InvalidVarCharLength(len.to_string())
                            })?;
                            return Ok(Self::new(Ty::VarChar, len));
                        }
                        _ => {
                            return Err(ParseDataTypeError::FormatError(
                                "BINARY(length)",
                                s.to_string(),
                            ));
                        }
                    }
                }
                if let Some(rest) = s.strip_prefix("NCHAR(") {
                    let params = rest.trim_start_matches('(').trim_end_matches(')').trim();
                    if params.is_empty() {
                        // If no params, fallback to default
                        return Ok(Self::new(Ty::NChar, 0));
                    }
                    if let Some(len) = rest.strip_suffix(')') {
                        let len: u32 = len
                            .trim()
                            .parse()
                            .map_err(|_| ParseDataTypeError::InvalidNCharLength(len.to_string()))?;
                        return Ok(Self::new(Ty::NChar, len));
                    }
                    return Err(ParseDataTypeError::FormatError(
                        "NCHAR(length)",
                        s.to_string(),
                    ));
                }
                if let Some(rest) = s.strip_prefix("VARBINARY(") {
                    if let Some(len) = rest.strip_suffix(')') {
                        let len: u32 = len.trim().parse().map_err(|_| {
                            ParseDataTypeError::InvalidVarBinaryLength(len.to_string())
                        })?;
                        return Ok(Self::new(Ty::VarBinary, len));
                    }
                    return Err(ParseDataTypeError::FormatError(
                        "VARBINARY(length)",
                        s.to_string(),
                    ));
                }
                if let Some(rest) = s.strip_prefix("GEOMETRY(") {
                    if let Some(len) = rest.strip_suffix(')') {
                        let len: u32 = len.trim().parse().map_err(|_| {
                            ParseDataTypeError::InvalidGeometryLength(len.to_string())
                        })?;
                        return Ok(Self::new(Ty::Geometry, len));
                    }
                    return Err(ParseDataTypeError::FormatError(
                        "GEOMETRY(length)",
                        s.to_string(),
                    ));
                }
                let u = s.to_uppercase();
                if u == s {
                    return Err(ParseDataTypeError::UnknownDataType(s.to_string()));
                }
                return Self::from_str(&u);
            }
        }
    }
}

impl From<Ty> for ColSchema {
    fn from(value: Ty) -> Self {
        Self::from_ty(value)
    }
}
impl ColSchema {
    #[inline]
    pub(crate) const fn new(ty: Ty, len: u32) -> Self {
        Self {
            ty,
            attr: LenOrDec { len },
        }
    }
    #[inline]
    pub(crate) const fn new_decimal(ty: Ty, precision: u8, scale: u8) -> Self {
        assert!(ty.is_decimal(), "Type must be decimal");
        Self {
            ty,
            attr: LenOrDec {
                dec: PrecScale {
                    len: ty.fixed_length() as u8,
                    _empty: 0,
                    prec: precision,
                    scale,
                },
            },
        }
    }
    #[inline]
    pub(crate) const fn from_ty(ty: Ty) -> Self {
        let attr = if ty.is_decimal() {
            LenOrDec {
                dec: PrecScale {
                    len: ty.fixed_length() as u8,
                    _empty: 0,
                    prec: 0,
                    scale: 0,
                },
            }
        } else {
            LenOrDec::new_len(ty.fixed_length() as _)
        };
        Self { ty, attr }
    }

    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        unsafe { std::mem::transmute::<&Self, &[u8; 5]>(self) }
    }

    #[inline]
    pub fn into_bytes(self) -> [u8; 5] {
        unsafe { std::mem::transmute::<Self, [u8; 5]>(self) }
    }

    /// The raw length of the column schema.
    #[inline]
    pub fn len(&self) -> u32 {
        if self.ty.is_decimal() {
            unsafe { self.attr.dec.len as _ }
        } else {
            unsafe { self.attr.len }
        }
    }

    /// Schema length to use in SQL queries.
    ///
    /// If the length is 0, returns 64 when is variable-length type (VARBINARY, VARCHAR).
    #[inline]
    pub fn len_or_fixed(&self) -> u32 {
        if self.ty.is_var_type() {
            let len = unsafe { self.attr.len };
            if len == 0 {
                return 64; // Default variable length for other types
            } else {
                return len;
            }
        } else {
            self.ty.fixed_length() as u32
        }
    }

    /// Returns 64 when length is zero.
    unsafe fn positive_var_len(&self) -> u32 {
        let len = self.attr.len;
        if len == 0 {
            return 64; // Default variable length for other types
        } else {
            return len;
        }
    }

    /// Set the length of the column schema.
    ///
    /// If the type is decimal, it will change the type to `Decimal64` or `Decimal` based on the length.
    /// If the length is 8, it will set the type to `Decimal64`,
    /// if the length is 16, it will set the type to `Decimal`.
    ///
    /// # Panics
    ///
    /// Panics if the length is not 8 or 16 for decimal types.
    ///
    pub fn set_len(&mut self, len: u32) {
        if self.ty.is_decimal() {
            if len == 8 {
                self.ty = Ty::Decimal64;
            } else if len == 16 {
                self.ty = Ty::Decimal;
            } else {
                panic!("Invalid decimal length: {len}");
            }
            self.attr.dec.len = len as u8;
        } else {
            self.attr.len = len;
        }
    }

    #[inline]
    pub(crate) fn as_prec_scale_unchecked(&self) -> PrecScale {
        unsafe { self.attr.dec }
    }

    /// Precision of a decimal value
    #[inline]
    pub fn precision(&self) -> u8 {
        if self.ty.is_decimal() {
            unsafe { self.attr.dec.prec }
        } else {
            0
        }
    }

    /// Scale of a decimal value
    #[inline]
    pub fn scale(&self) -> u8 {
        if self.ty.is_decimal() {
            unsafe { self.attr.dec.scale }
        } else {
            0
        }
    }
}

pub struct Schemas(pub(crate) Bytes);

impl<T: Into<Bytes>> From<T> for Schemas {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl Debug for Schemas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.deref(), f)
    }
}

impl Schemas {
    /// As a [ColSchema] slice.
    pub fn as_slice(&self) -> &[ColSchema] {
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr() as *const ColSchema,
                self.0.len() / std::mem::size_of::<ColSchema>(),
            )
        }
    }
}

impl Deref for Schemas {
    type Target = [ColSchema];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn col_schema() {
        let col = ColSchema {
            ty: Ty::BigInt,
            attr: LenOrDec::new_len(4),
        };
        let bytes: [u8; 5] = unsafe { std::mem::transmute_copy(&col) };
        dbg!(&bytes);

        let bytes: [u8; 5] = [4, 1, 0, 0, 0];
        let col2: ColSchema = unsafe { std::mem::transmute_copy(&bytes) };
        dbg!(col2);

        assert_eq!(std::mem::size_of_val(&col), 5);
        assert_eq!(std::mem::align_of_val(&col), 1);
    }

    #[test]
    fn test_bin() {
        let v: [u8; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ptr = v.as_ptr();

        let v_u16 = unsafe { *std::mem::transmute::<*const u8, *const u16>(ptr) };
        println!("{v_u16:#x?}: {:?}", v_u16.to_le_bytes());
        #[derive(Debug, Clone, Copy)]
        #[repr(packed)]
        #[allow(dead_code)]
        struct A {
            a: u16,
            b: u32,
        }
        println!("A size: {}", std::mem::size_of::<A>());
        let a: &A = unsafe {
            std::mem::transmute::<*const u8, *const A>(ptr)
                .as_ref()
                .unwrap()
        };
        println!("{a:#x?}");
    }

    #[test]
    fn test_dec() {
        let schema = ColSchema::new(Ty::BigInt, 8);
        assert_eq!(schema.len(), 8);
        let dec_schema = ColSchema::new_decimal(Ty::Decimal64, 10, 2);
        assert_eq!(dec_schema.len(), 8);
        assert_eq!(dec_schema.ty, Ty::Decimal64);
        assert_eq!(unsafe { dec_schema.attr.dec.prec }, 10);
        assert_eq!(unsafe { dec_schema.attr.dec.scale }, 2);

        let bytes = dec_schema.as_bytes();
        assert_eq!(bytes, [21, 8, 0, 10, 2]);
    }

    #[test]
    fn test_col_schema_from_str() {
        let schema: ColSchema = "VARCHAR(100)".parse().unwrap();
        assert_eq!(schema.ty, Ty::VarChar);
        assert_eq!(schema.len(), 100);
        assert_eq!(schema.to_string(), "BINARY(100)");
        let schema: ColSchema = "BINARY(100)".parse().unwrap();
        assert_eq!(schema.ty, Ty::VarChar);
        assert_eq!(schema.len(), 100);
        assert_eq!(schema.to_string(), "BINARY(100)");
        let schema: ColSchema = "NCHAR(50)".parse().unwrap();
        assert_eq!(schema.ty, Ty::NChar);
        assert_eq!(schema.len(), 50);
        assert_eq!(schema.to_string(), "NCHAR(50)");
        let schema: ColSchema = "VARBINARY(32)".parse().unwrap();
        assert_eq!(schema.ty, Ty::VarBinary);
        assert_eq!(schema.len(), 32);
        assert_eq!(schema.to_string(), "VARBINARY(32)");
        let schema: ColSchema = "GEOMETRY(128)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Geometry);
        assert_eq!(schema.len(), 128);
        assert_eq!(schema.to_string(), "GEOMETRY(128)");
        let schema: ColSchema = "JSON".parse().unwrap();
        assert_eq!(schema.ty, Ty::Json);
        assert_eq!(schema.len(), 0);
        assert_eq!(schema.to_string(), "JSON");
        let schema: ColSchema = "BLOB".parse().unwrap();
        assert_eq!(schema.ty, Ty::Blob);
        assert_eq!(schema.len(), 0);
        assert_eq!(schema.to_string(), "BLOB");
        let schema: ColSchema = "MEDIUMBLOB".parse().unwrap();
        assert_eq!(schema.ty, Ty::MediumBlob);
        assert_eq!(schema.len(), 0);
        assert_eq!(schema.to_string(), "MEDIUMBLOB");
        let schema: ColSchema = "BOOL".parse().unwrap();
        assert_eq!(schema.ty, Ty::Bool);
        assert_eq!(schema.len(), 1);
        assert_eq!(schema.to_string(), "BOOL");
        let schema: ColSchema = "TINYINT".parse().unwrap();
        assert_eq!(schema.ty, Ty::TinyInt);
        assert_eq!(schema.len(), 1);
        assert_eq!(schema.to_string(), "TINYINT");
        let schema: ColSchema = "SMALLINT".parse().unwrap();
        assert_eq!(schema.ty, Ty::SmallInt);
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.to_string(), "SMALLINT");
        let schema: ColSchema = "INT".parse().unwrap();
        assert_eq!(schema.ty, Ty::Int);
        assert_eq!(schema.len(), 4);
        assert_eq!(schema.to_string(), "INT");
        let schema: ColSchema = "BIGINT".parse().unwrap();
        assert_eq!(schema.ty, Ty::BigInt);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.to_string(), "BIGINT");
        let schema: ColSchema = "TINYINT UNSIGNED".parse().unwrap();
        assert_eq!(schema.ty, Ty::UTinyInt);
        assert_eq!(schema.len(), 1);
        assert_eq!(schema.to_string(), "TINYINT UNSIGNED");
        let schema: ColSchema = "SMALLINT UNSIGNED".parse().unwrap();
        assert_eq!(schema.ty, Ty::USmallInt);
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.to_string(), "SMALLINT UNSIGNED");
        let schema: ColSchema = "INT UNSIGNED".parse().unwrap();
        assert_eq!(schema.ty, Ty::UInt);
        assert_eq!(schema.len(), 4);
        assert_eq!(schema.to_string(), "INT UNSIGNED");
        let schema: ColSchema = "BIGINT UNSIGNED".parse().unwrap();
        assert_eq!(schema.ty, Ty::UBigInt);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.to_string(), "BIGINT UNSIGNED");
        let schema: ColSchema = "FLOAT".parse().unwrap();
        assert_eq!(schema.ty, Ty::Float);
        assert_eq!(schema.len(), 4);
        assert_eq!(schema.to_string(), "FLOAT");
        let schema: ColSchema = "DOUBLE".parse().unwrap();
        assert_eq!(schema.ty, Ty::Double);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.to_string(), "DOUBLE");
        let schema: ColSchema = "TIMESTAMP".parse().unwrap();
        assert_eq!(schema.ty, Ty::Timestamp);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.to_string(), "TIMESTAMP");
        let schema = "UNKNOWN".parse::<ColSchema>();
        assert!(schema.is_err());
        let schema = "DECIMAL(40,2)".parse::<ColSchema>();
        assert!(schema.is_err());
    }

    #[test]
    fn test_schema_decimal() {
        let schema: ColSchema = "DECIMAL(10,2)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal64);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.precision(), 10);
        assert_eq!(schema.scale(), 2);
        assert_eq!(schema.to_string(), "DECIMAL(10, 2)");

        let schema: ColSchema = "DECIMAL(20,5)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal);
        assert_eq!(schema.len(), 16);
        assert_eq!(schema.precision(), 20);
        assert_eq!(schema.scale(), 5);
        assert_eq!(schema.to_string(), "DECIMAL(20, 5)");

        let schema: ColSchema = "DECIMAL(38,0)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal);
        assert_eq!(schema.len(), 16);
        assert_eq!(schema.precision(), 38);
        assert_eq!(schema.scale(), 0);
        assert_eq!(schema.to_string(), "DECIMAL(38, 0)");

        let schema: ColSchema = "DECIMAL(38)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal);
        assert_eq!(schema.len(), 16);
        assert_eq!(schema.precision(), 38);
        assert_eq!(schema.scale(), 0);
        assert_eq!(schema.to_string(), "DECIMAL(38, 0)");

        let schema: ColSchema = "DECIMAL(18,4)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal64);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.precision(), 18);
        assert_eq!(schema.scale(), 4);
        assert_eq!(schema.to_string(), "DECIMAL(18, 4)");

        let schema: ColSchema = "DECIMAL(18)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal64);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.precision(), 18);
        assert_eq!(schema.scale(), 0);
        assert_eq!(schema.to_string(), "DECIMAL(18, 0)");
        let schema: ColSchema = "DECIMAL".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal64);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.precision(), 18);
        assert_eq!(schema.scale(), 0);
    }
}
