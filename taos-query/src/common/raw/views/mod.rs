mod bool_view;

pub use bool_view::BoolView;

mod tinyint_view;

use itertools::Itertools;
pub use tinyint_view::TinyIntView;

mod smallint_view;
pub use smallint_view::SmallIntView;

mod int_view;
pub use int_view::IntView;

mod big_int_view;
pub use big_int_view::BigIntView;

mod float_view;
pub use float_view::FloatView;

mod double_view;
pub use double_view::DoubleView;

mod tinyint_unsigned_view;
pub use tinyint_unsigned_view::UTinyIntView;

mod small_int_unsigned_view;
pub use small_int_unsigned_view::USmallIntView;

mod int_unsigned_view;
pub use int_unsigned_view::UIntView;

mod big_int_unsigned_view;
pub use big_int_unsigned_view::UBigIntView;

mod timestamp_view;
pub use timestamp_view::TimestampView;

mod var_char_view;
pub use var_char_view::VarCharView;

mod n_char_view;
pub use n_char_view::NCharView;

mod json_view;
pub use json_view::JsonView;

mod schema;
pub(crate) use schema::*;

mod nulls;
pub(crate) use nulls::*;

mod offsets;
pub(crate) use offsets::*;

mod lengths;
pub(crate) use lengths::*;

mod from;

use crate::{
    common::{BorrowedValue, Ty, Value},
    Precision,
};

use std::{
    ffi::c_void,
    fmt::{Debug, Display},
    io::Write,
    iter::FusedIterator,
};

pub(crate) trait IsColumnView: Sized {
    /// View item data type.
    fn ty(&self) -> Ty;

    fn from_value_iter<'a>(iter: impl Iterator<Item = &'a Value>) -> Self {
        Self::from_borrowed_value_iter::<'a>(iter.map(|v| v.to_borrowed_value()))
    }

    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self;
}

/// Compatible version for var char.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Version {
    V2,
    V3,
}

#[derive(Clone)]
pub enum ColumnView {
    Bool(BoolView),           // 1
    TinyInt(TinyIntView),     // 2
    SmallInt(SmallIntView),   // 3
    Int(IntView),             // 4
    BigInt(BigIntView),       // 5
    Float(FloatView),         // 6
    Double(DoubleView),       // 7
    VarChar(VarCharView),     // 8
    Timestamp(TimestampView), // 9
    NChar(NCharView),         // 10
    UTinyInt(UTinyIntView),   // 11
    USmallInt(USmallIntView), // 12
    UInt(UIntView),           // 13
    UBigInt(UBigIntView),     // 14
    Json(JsonView),           // 15
}
unsafe impl Send for ColumnView {}
unsafe impl Sync for ColumnView {}

impl Debug for ColumnView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool(view) => f.debug_tuple("Bool").field(&view.to_vec()).finish(),
            Self::TinyInt(view) => f.debug_tuple("TinyInt").field(&view.to_vec()).finish(),
            Self::SmallInt(view) => f.debug_tuple("SmallInt").field(&view.to_vec()).finish(),
            Self::Int(view) => f.debug_tuple("Int").field(&view.to_vec()).finish(),
            Self::BigInt(view) => f.debug_tuple("BigInt").field(&view.to_vec()).finish(),
            Self::Float(view) => f.debug_tuple("Float").field(&view.to_vec()).finish(),
            Self::Double(view) => f.debug_tuple("Double").field(&view.to_vec()).finish(),
            Self::VarChar(view) => f.debug_tuple("VarChar").field(&view.to_vec()).finish(),
            Self::Timestamp(view) => f.debug_tuple("Timestamp").field(&view.to_vec()).finish(),
            Self::NChar(view) => f.debug_tuple("NChar").field(&view.to_vec()).finish(),
            Self::UTinyInt(view) => f.debug_tuple("UTinyInt").field(&view.to_vec()).finish(),
            Self::USmallInt(view) => f.debug_tuple("USmallInt").field(&view.to_vec()).finish(),
            Self::UInt(view) => f.debug_tuple("UInt").field(&view.to_vec()).finish(),
            Self::UBigInt(view) => f.debug_tuple("UBigInt").field(&view.to_vec()).finish(),
            Self::Json(view) => f.debug_tuple("Json").field(&view.to_vec()).finish(),
        }
    }
}

impl std::ops::Add for ColumnView {
    type Output = ColumnView;

    fn add(self, rhs: Self) -> Self::Output {
        self.concat(&rhs)
    }
}

#[derive(Debug)]
pub struct CastError {
    from: Ty,
    to: Ty,
    message: &'static str,
}

impl Display for CastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Cast from {} to {} error: {}",
            self.from, self.to, self.message
        ))
    }
}

impl std::error::Error for CastError {}

pub struct ColumnViewIter<'c> {
    view: &'c ColumnView,
    row: usize,
}

impl<'c> Iterator for ColumnViewIter<'c> {
    type Item = BorrowedValue<'c>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.view.len() {
            return None;
        }
        let row = self.row;
        self.row += 1;
        Some(unsafe { self.view.get_ref_unchecked(row) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.row, Some(self.view.len() - self.row))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let row = self.row + n;
        if row >= self.view.len() {
            return None;
        }
        self.row += n;
        Some(unsafe { self.view.get_ref_unchecked(row) })
    }

    #[inline]
    fn count(self) -> usize {
        self.view.len()
    }
}

impl<'c> ExactSizeIterator for ColumnViewIter<'c> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

impl<'a> FusedIterator for ColumnViewIter<'a> {}

impl<'a> IntoIterator for &'a ColumnView {
    type Item = BorrowedValue<'a>;

    type IntoIter = ColumnViewIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl ColumnView {
    pub fn from_millis_timestamp(values: Vec<impl Into<Option<i64>>>) -> Self {
        ColumnView::Timestamp(TimestampView::from_millis(values))
    }
    pub fn from_micros_timestamp(values: Vec<impl Into<Option<i64>>>) -> Self {
        ColumnView::Timestamp(TimestampView::from_micros(values))
    }
    pub fn from_nanos_timestamp(values: Vec<impl Into<Option<i64>>>) -> Self {
        ColumnView::Timestamp(TimestampView::from_nanos(values))
    }
    pub fn from_bools(values: Vec<impl Into<Option<bool>>>) -> Self {
        ColumnView::Bool(BoolView::from_iter(values))
    }
    pub fn from_tiny_ints(values: Vec<impl Into<Option<i8>>>) -> Self {
        ColumnView::TinyInt(TinyIntView::from_iter(values))
    }
    pub fn from_small_ints(values: Vec<impl Into<Option<i16>>>) -> Self {
        ColumnView::SmallInt(SmallIntView::from_iter(values))
    }
    pub fn from_ints(values: Vec<impl Into<Option<i32>>>) -> Self {
        ColumnView::Int(IntView::from_iter(values))
    }
    pub fn from_big_ints(values: Vec<impl Into<Option<i64>>>) -> Self {
        ColumnView::BigInt(BigIntView::from_iter(values))
    }
    pub fn from_unsigned_tiny_ints(values: Vec<impl Into<Option<u8>>>) -> Self {
        ColumnView::UTinyInt(UTinyIntView::from_iter(values))
    }
    pub fn from_unsigned_small_ints(values: Vec<impl Into<Option<u16>>>) -> Self {
        ColumnView::USmallInt(USmallIntView::from_iter(values))
    }
    pub fn from_unsigned_ints(values: Vec<impl Into<Option<u32>>>) -> Self {
        ColumnView::UInt(UIntView::from_iter(values))
    }
    pub fn from_unsigned_big_ints(values: Vec<impl Into<Option<u64>>>) -> Self {
        ColumnView::UBigInt(UBigIntView::from_iter(values))
    }
    pub fn from_floats(values: Vec<impl Into<Option<f32>>>) -> Self {
        ColumnView::Float(FloatView::from_iter(values))
    }
    pub fn from_doubles(values: Vec<impl Into<Option<f64>>>) -> Self {
        ColumnView::Double(DoubleView::from_iter(values))
    }
    pub fn from_varchar<
        S: AsRef<str>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        ColumnView::VarChar(VarCharView::from_iter(iter))
    }
    pub fn from_nchar<
        S: AsRef<str>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        ColumnView::NChar(NCharView::from_iter(iter))
    }
    pub fn from_json<
        S: AsRef<str>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        ColumnView::Json(JsonView::from_iter(iter))
    }

    #[inline]
    pub fn concat_iter<'b, 'a: 'b>(
        &'a self,
        rhs: impl Iterator<Item = BorrowedValue<'b>>,
        ty: Ty,
    ) -> ColumnView {
        match ty {
            Ty::Null => unreachable!(),
            Ty::Bool => ColumnView::Bool(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::TinyInt => ColumnView::TinyInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::SmallInt => ColumnView::SmallInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Int => ColumnView::Int(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::BigInt => ColumnView::BigInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::UTinyInt => ColumnView::UTinyInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::USmallInt => ColumnView::USmallInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::UInt => ColumnView::UInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::UBigInt => ColumnView::UBigInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Float => ColumnView::Float(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Double => ColumnView::Double(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Timestamp => ColumnView::Timestamp(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::VarChar => ColumnView::VarChar(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::NChar => ColumnView::NChar(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Json => ColumnView::Json(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::VarBinary => todo!(),
            Ty::Decimal => todo!(),
            Ty::Blob => todo!(),
            Ty::MediumBlob => todo!(),
        }
    }

    /// Concatenate another column view, output a new column view with exact type of self.
    #[inline]
    pub fn concat(&self, rhs: &ColumnView) -> ColumnView {
        self.concat_as(rhs, self.as_ty())
    }

    /// Concatenate another column view strictly, output a new column view with exact type of self.
    ///
    /// # Panics
    ///
    /// Panics if the two column views have different types.
    #[inline]
    pub fn concat_strictly(&self, rhs: &ColumnView) -> ColumnView {
        match (self, rhs) {
            (ColumnView::Timestamp(a), ColumnView::Timestamp(b)) => {
                ColumnView::Timestamp(a.concat(b))
            }
            (ColumnView::Bool(a), ColumnView::Bool(b)) => ColumnView::Bool(a.concat(b)),
            (ColumnView::TinyInt(a), ColumnView::TinyInt(b)) => ColumnView::TinyInt(a.concat(b)),
            (ColumnView::UTinyInt(a), ColumnView::UTinyInt(b)) => ColumnView::UTinyInt(a.concat(b)),
            (ColumnView::SmallInt(a), ColumnView::SmallInt(b)) => ColumnView::SmallInt(a.concat(b)),
            (ColumnView::USmallInt(a), ColumnView::USmallInt(b)) => {
                ColumnView::USmallInt(a.concat(b))
            }
            (ColumnView::Int(a), ColumnView::Int(b)) => ColumnView::Int(a.concat(b)),
            (ColumnView::UInt(a), ColumnView::UInt(b)) => ColumnView::UInt(a.concat(b)),
            (ColumnView::BigInt(a), ColumnView::BigInt(b)) => ColumnView::BigInt(a.concat(b)),
            (ColumnView::UBigInt(a), ColumnView::UBigInt(b)) => ColumnView::UBigInt(a.concat(b)),
            (ColumnView::Float(a), ColumnView::Float(b)) => ColumnView::Float(a.concat(b)),
            (ColumnView::Double(a), ColumnView::Double(b)) => ColumnView::Double(a.concat(b)),
            (ColumnView::VarChar(a), ColumnView::VarChar(b)) => ColumnView::VarChar(a.concat(b)),
            (ColumnView::NChar(a), ColumnView::NChar(b)) => ColumnView::NChar(a.concat(b)),
            (ColumnView::Json(a), ColumnView::Json(b)) => ColumnView::Json(a.concat(b)),
            _ => panic!("strict concat needs same schema: {self:?}, {rhs:?}"),
        }
    }

    /// Concatenate another column view, output a new column view with specified type `ty`.
    #[inline]
    pub fn concat_as(&self, rhs: &ColumnView, ty: Ty) -> ColumnView {
        self.concat_iter(rhs.iter(), ty)
    }

    /// Generate single element view for specified type `ty`.
    pub fn null(n: usize, ty: Ty) -> Self {
        match ty {
            Ty::Null => panic!("type should be known"),
            Ty::Bool => Self::from_bools(vec![None; n]),
            Ty::TinyInt => Self::from_tiny_ints(vec![None; n]),
            Ty::SmallInt => Self::from_small_ints(vec![None; n]),
            Ty::Int => Self::from_ints(vec![None; n]),
            Ty::BigInt => Self::from_big_ints(vec![None; n]),
            Ty::UTinyInt => Self::from_unsigned_tiny_ints(vec![None; n]),
            Ty::USmallInt => Self::from_unsigned_small_ints(vec![None; n]),
            Ty::UInt => Self::from_unsigned_ints(vec![None; n]),
            Ty::UBigInt => Self::from_unsigned_big_ints(vec![None; n]),
            Ty::Float => Self::from_floats(vec![None; n]),
            Ty::Double => Self::from_doubles(vec![None; n]),
            Ty::Timestamp => Self::from_millis_timestamp(vec![None; n]),
            Ty::VarChar => Self::from_varchar::<&'static str, _, _, _>(vec![None; n]),
            Ty::NChar => Self::from_nchar::<&'static str, _, _, _>(vec![None; n]),
            Ty::Json => todo!(),
            Ty::VarBinary => todo!(),
            Ty::Decimal => todo!(),
            Ty::Blob => todo!(),
            Ty::MediumBlob => todo!(),
        }
    }

    /// It's equal to the cols
    pub fn len(&self) -> usize {
        match self {
            ColumnView::Bool(view) => view.len(),
            ColumnView::TinyInt(view) => view.len(),
            ColumnView::SmallInt(view) => view.len(),
            ColumnView::Int(view) => view.len(),
            ColumnView::BigInt(view) => view.len(),
            ColumnView::Float(view) => view.len(),
            ColumnView::Double(view) => view.len(),
            ColumnView::VarChar(view) => view.len(),
            ColumnView::Timestamp(view) => view.len(),
            ColumnView::NChar(view) => view.len(),
            ColumnView::UTinyInt(view) => view.len(),
            ColumnView::USmallInt(view) => view.len(),
            ColumnView::UInt(view) => view.len(),
            ColumnView::UBigInt(view) => view.len(),
            ColumnView::Json(view) => view.len(),
        }
    }

    pub fn max_variable_length(&self) -> usize {
        match self {
            ColumnView::Bool(_) => 1,
            ColumnView::TinyInt(_) => 1,
            ColumnView::SmallInt(_) => 2,
            ColumnView::Int(_) => 4,
            ColumnView::BigInt(_) => 8,
            ColumnView::Float(_) => 4,
            ColumnView::Double(_) => 8,
            ColumnView::VarChar(view) => view.max_length(),
            ColumnView::Timestamp(_) => 8,
            ColumnView::NChar(view) => view.max_length(),
            ColumnView::UTinyInt(_) => 1,
            ColumnView::USmallInt(_) => 2,
            ColumnView::UInt(_) => 4,
            ColumnView::UBigInt(_) => 8,
            ColumnView::Json(view) => view.max_length(),
        }
    }

    /// Check if a value at `row` is null
    #[inline]
    pub(super) unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        match self {
            ColumnView::Bool(view) => view.is_null_unchecked(row),
            ColumnView::TinyInt(view) => view.is_null_unchecked(row),
            ColumnView::SmallInt(view) => view.is_null_unchecked(row),
            ColumnView::Int(view) => view.is_null_unchecked(row),
            ColumnView::BigInt(view) => view.is_null_unchecked(row),
            ColumnView::Float(view) => view.is_null_unchecked(row),
            ColumnView::Double(view) => view.is_null_unchecked(row),
            ColumnView::VarChar(view) => view.is_null_unchecked(row),
            ColumnView::Timestamp(view) => view.is_null_unchecked(row),
            ColumnView::NChar(view) => view.is_null_unchecked(row),
            ColumnView::UTinyInt(view) => view.is_null_unchecked(row),
            ColumnView::USmallInt(view) => view.is_null_unchecked(row),
            ColumnView::UInt(view) => view.is_null_unchecked(row),
            ColumnView::UBigInt(view) => view.is_null_unchecked(row),
            ColumnView::Json(view) => view.is_null_unchecked(row),
        }
    }

    pub fn get(&self, row: usize) -> Option<BorrowedValue> {
        if row < self.len() {
            Some(unsafe { self.get_ref_unchecked(row) })
        } else {
            None
        }
    }

    /// Get one value at `row` index of the column view.
    #[inline]
    pub(super) unsafe fn get_ref_unchecked(&self, row: usize) -> BorrowedValue {
        match self {
            ColumnView::Bool(view) => view.get_value_unchecked(row),
            ColumnView::TinyInt(view) => view.get_value_unchecked(row),
            ColumnView::SmallInt(view) => view.get_value_unchecked(row),
            ColumnView::Int(view) => view.get_value_unchecked(row),
            ColumnView::BigInt(view) => view.get_value_unchecked(row),
            ColumnView::Float(view) => view.get_value_unchecked(row),
            ColumnView::Double(view) => view.get_value_unchecked(row),
            ColumnView::VarChar(view) => view.get_value_unchecked(row),
            ColumnView::Timestamp(view) => view.get_value_unchecked(row),
            ColumnView::NChar(view) => view.get_value_unchecked(row),
            ColumnView::UTinyInt(view) => view.get_value_unchecked(row),
            ColumnView::USmallInt(view) => view.get_value_unchecked(row),
            ColumnView::UInt(view) => view.get_value_unchecked(row),
            ColumnView::UBigInt(view) => view.get_value_unchecked(row),
            ColumnView::Json(view) => view.get_value_unchecked(row),
        }
    }

    /// Get pointer to value.
    #[inline]
    pub(super) unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        match self {
            ColumnView::Bool(view) => view.get_raw_value_unchecked(row),
            ColumnView::TinyInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::SmallInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::Int(view) => view.get_raw_value_unchecked(row),
            ColumnView::BigInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::Float(view) => view.get_raw_value_unchecked(row),
            ColumnView::Double(view) => view.get_raw_value_unchecked(row),
            ColumnView::VarChar(view) => view.get_raw_value_unchecked(row),
            ColumnView::Timestamp(view) => view.get_raw_value_unchecked(row),
            ColumnView::NChar(view) => view.get_raw_value_unchecked(row),
            ColumnView::UTinyInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::USmallInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::UInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::UBigInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::Json(view) => view.get_raw_value_unchecked(row),
        }
    }

    pub fn iter(&self) -> ColumnViewIter {
        ColumnViewIter { view: self, row: 0 }
    }

    pub fn slice(&self, range: std::ops::Range<usize>) -> Option<Self> {
        match self {
            ColumnView::Bool(view) => view.slice(range).map(ColumnView::Bool),
            ColumnView::TinyInt(view) => view.slice(range).map(ColumnView::TinyInt),
            ColumnView::SmallInt(view) => view.slice(range).map(ColumnView::SmallInt),
            ColumnView::Int(view) => view.slice(range).map(ColumnView::Int),
            ColumnView::BigInt(view) => view.slice(range).map(ColumnView::BigInt),
            ColumnView::Float(view) => view.slice(range).map(ColumnView::Float),
            ColumnView::Double(view) => view.slice(range).map(ColumnView::Double),
            ColumnView::VarChar(view) => view.slice(range).map(ColumnView::VarChar),
            ColumnView::Timestamp(view) => view.slice(range).map(ColumnView::Timestamp),
            ColumnView::NChar(view) => view.slice(range).map(ColumnView::NChar),
            ColumnView::UTinyInt(view) => view.slice(range).map(ColumnView::UTinyInt),
            ColumnView::USmallInt(view) => view.slice(range).map(ColumnView::USmallInt),
            ColumnView::UInt(view) => view.slice(range).map(ColumnView::UInt),
            ColumnView::UBigInt(view) => view.slice(range).map(ColumnView::UBigInt),
            ColumnView::Json(view) => view.slice(range).map(ColumnView::Json),
        }
    }

    pub(super) fn write_raw_into<W: Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        match self {
            ColumnView::Bool(view) => view.write_raw_into(wtr),
            ColumnView::TinyInt(view) => view.write_raw_into(wtr),
            ColumnView::SmallInt(view) => view.write_raw_into(wtr),
            ColumnView::Int(view) => view.write_raw_into(wtr),
            ColumnView::BigInt(view) => view.write_raw_into(wtr),
            ColumnView::Float(view) => view.write_raw_into(wtr),
            ColumnView::Double(view) => view.write_raw_into(wtr),
            ColumnView::VarChar(view) => view.write_raw_into(wtr),
            ColumnView::Timestamp(view) => view.write_raw_into(wtr),
            ColumnView::NChar(view) => view.write_raw_into(wtr),
            ColumnView::UTinyInt(view) => view.write_raw_into(wtr),
            ColumnView::USmallInt(view) => view.write_raw_into(wtr),
            ColumnView::UInt(view) => view.write_raw_into(wtr),
            ColumnView::UBigInt(view) => view.write_raw_into(wtr),
            ColumnView::Json(view) => view.write_raw_into(wtr),
        }
    }

    pub fn as_ty(&self) -> Ty {
        match self {
            ColumnView::Bool(_) => Ty::Bool,
            ColumnView::TinyInt(_) => Ty::TinyInt,
            ColumnView::SmallInt(_) => Ty::SmallInt,
            ColumnView::Int(_) => Ty::Int,
            ColumnView::BigInt(_) => Ty::BigInt,
            ColumnView::Float(_) => Ty::Float,
            ColumnView::Double(_) => Ty::Double,
            ColumnView::VarChar(_) => Ty::VarChar,
            ColumnView::Timestamp(_) => Ty::Timestamp,
            ColumnView::NChar(_) => Ty::NChar,
            ColumnView::UTinyInt(_) => Ty::UTinyInt,
            ColumnView::USmallInt(_) => Ty::USmallInt,
            ColumnView::UInt(_) => Ty::UInt,
            ColumnView::UBigInt(_) => Ty::UBigInt,
            ColumnView::Json(_) => Ty::Json,
        }
    }

    /// Cast behaviors:
    ///
    /// - BOOL to VARCHAR/NCHAR: true => "true", false => "false"
    /// - numeric(integers/float/double) to string(varchar/nchar): like print or to_string.
    /// - string to primitive: can be parsed => primitive, others => null.
    /// - timestamp to string: RFC3339 with localized timezone.
    ///
    /// Not supported:
    /// - any to timestamp
    pub fn cast(&self, ty: Ty) -> Result<ColumnView, CastError> {
        let l_ty = self.as_ty();
        if l_ty == ty {
            return Ok(self.clone());
        }
        use Ty::*;
        match self {
            // (Bool, UBigInt) => ColumnView::from_big_ints(self.)
            ColumnView::Bool(booleans) => {
                macro_rules! _cast_bool_to {
                    ($ty:ty) => {
                        booleans
                            .iter()
                            .map(|v| v.map(|b| if b { 1 as $ty } else { 0 as $ty }))
                            .collect_vec()
                    };
                }

                let view = match ty {
                    TinyInt => Self::from_tiny_ints(_cast_bool_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_bool_to!(i16)),
                    Int => Self::from_ints(_cast_bool_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_bool_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_bool_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_bool_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_bool_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_bool_to!(u64)),
                    Float => Self::from_floats(_cast_bool_to!(f32)),
                    Double => Self::from_doubles(_cast_bool_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::TinyInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::SmallInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::Int(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::BigInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }

            ColumnView::UTinyInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }

            ColumnView::USmallInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }

            ColumnView::UInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::UBigInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }

            ColumnView::Float(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(view.iter().map(|v| v.map(|b| b > 0.0)).collect_vec()),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::Double(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(view.iter().map(|v| v.map(|b| b > 0.0)).collect_vec()),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::VarChar(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter()
                            .map(|v| v.and_then(|b| b.as_str().parse::<$ty>().ok()))
                            .collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(_cast_to!(bool)),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::NChar(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter()
                            .map(|v| v.and_then(|b| b.parse::<$ty>().ok()))
                            .collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(_cast_to!(bool)),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::Timestamp(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter()
                            .map(|v| v.map(|b| b.as_raw_i64() as $ty))
                            .collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(
                        view.iter()
                            .map(|v| v.map(|b| b.as_raw_i64() > 0))
                            .collect_vec(),
                    ),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter()
                            .map(|v| v.map(|b| b.to_datetime_with_tz().to_rfc3339())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter()
                            .map(|v| v.map(|b| b.to_datetime_with_tz().to_rfc3339())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,

                            message: "",
                        })
                    }
                };
                Ok(view)
            }
            _ => todo!(),
        }
    }

    pub fn cast_precision(&self, precision: Precision) -> ColumnView {
        match self {
            ColumnView::Timestamp(view) => ColumnView::Timestamp(view.cast_precision(precision)),
            _ => self.clone(),
        }
    }
    pub unsafe fn as_timestamp_view(&self) -> &TimestampView {
        match self {
            ColumnView::Timestamp(view) => view,
            _ => unreachable!(),
        }
    }

    pub(crate) fn _to_nulls_vec(&self) -> Vec<bool> {
        (0..self.len())
            .map(|i| unsafe { self.is_null_unchecked(i) })
            .collect()
    }
}

pub fn views_to_raw_block(views: &[ColumnView]) -> Vec<u8> {
    let header = super::Header {
        nrows: views.first().map(|v| v.len()).unwrap_or(0) as _,
        ncols: views.len() as _,
        ..Default::default()
    };

    let ncols = views.len();

    let mut bytes = Vec::new();
    bytes.extend(header.as_bytes());

    let schemas = views
        .iter()
        .map(|view| {
            let ty = view.as_ty();
            ColSchema {
                ty,
                len: ty.fixed_length() as _,
            }
        })
        .collect_vec();
    let schema_bytes = unsafe {
        std::slice::from_raw_parts(
            schemas.as_ptr() as *const u8,
            ncols * std::mem::size_of::<ColSchema>(),
        )
    };
    bytes.write_all(schema_bytes).unwrap();

    let length_offset = bytes.len();
    bytes.resize(bytes.len() + ncols * std::mem::size_of::<u32>(), 0);

    let mut lengths = vec![0; ncols];
    for (i, view) in views.iter().enumerate() {
        let cur = bytes.len();
        let n = view.write_raw_into(&mut bytes).unwrap();
        let len = bytes.len();
        debug_assert!(cur + n == len);
        if !view.as_ty().is_primitive() {
            lengths[i] = (n - header.nrows() * 4) as _;
        } else {
            lengths[i] = (header.nrows() * view.as_ty().fixed_length()) as _;
        }
    }
    unsafe {
        (*(bytes.as_mut_ptr() as *mut super::Header)).length = bytes.len() as _;
        std::ptr::copy(
            lengths.as_ptr(),
            bytes.as_mut_ptr().add(length_offset) as *mut u32,
            lengths.len(),
        );
    }
    bytes
}

impl From<Value> for ColumnView {
    fn from(value: Value) -> Self {
        match value {
            Value::Null(ty) => ColumnView::null(1, ty),
            Value::Bool(v) => vec![v].into(),
            Value::TinyInt(v) => vec![v].into(),
            Value::SmallInt(v) => vec![v].into(),
            Value::Int(v) => vec![v].into(),
            Value::BigInt(v) => vec![v].into(),
            Value::Float(v) => vec![v].into(),
            Value::Double(v) => vec![v].into(),
            Value::VarChar(v) => ColumnView::from_varchar::<String, _, _, _>(vec![v]),
            Value::Timestamp(v) => ColumnView::Timestamp(TimestampView::from_timestamp(vec![v])),
            Value::NChar(v) => ColumnView::from_nchar::<String, _, _, _>(vec![v]),
            Value::UTinyInt(v) => vec![v].into(),
            Value::USmallInt(v) => vec![v].into(),
            Value::UInt(v) => vec![v].into(),
            Value::UBigInt(v) => vec![v].into(),
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_view_null() {
        let null_value = Value::Null(Ty::Int);
        let null_column_view = ColumnView::from(null_value);
        assert_eq!(null_column_view.len(), 1);
        assert_eq!(null_column_view.as_ty(), Ty::Int);
    }

    #[test]
    fn test_column_view_bool() {
        let bool_value = Value::Bool(true);
        let bool_column_view = ColumnView::from(bool_value);
        assert_eq!(bool_column_view.len(), 1);
        assert_eq!(bool_column_view.as_ty(), Ty::Bool);
        assert_eq!(ColumnView::null(1, Ty::Bool).len(), 1);
        assert_eq!(bool_column_view.max_variable_length(), 1);
        println!("{:?}", bool_column_view.slice(0..1));
        println!("{:?}", bool_column_view.cast(Ty::TinyInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::SmallInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::Int).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::BigInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::UTinyInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::USmallInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::UInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::UBigInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::Float).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::Double).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::VarChar).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::NChar).unwrap());
    }

    #[test]
    fn test_concat_iter() {
        let column_view_int = ColumnView::from(vec![1, 2, 3]);

        let iterator_values = vec![
            BorrowedValue::Int(7),
            BorrowedValue::UInt(8),
            BorrowedValue::Int(9),
        ];

        let result_column_int =
            column_view_int.concat_iter(iterator_values.iter().cloned(), Ty::Int);
        assert_eq!(result_column_int.len(), 6);

        let result_column_uint =
            column_view_int.concat_iter(iterator_values.iter().cloned(), Ty::UInt);
        assert_eq!(result_column_uint.len(), 6);

        let result_column_bigint =
            column_view_int.concat_iter(iterator_values.iter().cloned(), Ty::BigInt);
        assert_eq!(result_column_bigint.len(), 6);

        let result_column_ubigint =
            column_view_int.concat_iter(iterator_values.iter().cloned(), Ty::UBigInt);
        assert_eq!(result_column_ubigint.len(), 6);

        let result_column_float =
            column_view_int.concat_iter(iterator_values.iter().cloned(), Ty::Float);
        assert_eq!(result_column_float.len(), 6);

        let result_column_double =
            column_view_int.concat_iter(iterator_values.iter().cloned(), Ty::Double);
        assert_eq!(result_column_double.len(), 6);

        let result_column_varchar =
            column_view_int.concat_iter(iterator_values.iter().cloned(), Ty::VarChar);
        assert_eq!(result_column_varchar.len(), 6);
    }
}
