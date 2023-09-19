use std::ffi::c_void;

use crate::common::{BorrowedValue, Precision, Timestamp, Ty};

use super::{IsColumnView, NullBits, NullsIter};

use bytes::Bytes;
use itertools::Itertools;

type Item = i64;
type View = TimestampView;
const ITEM_SIZE: usize = std::mem::size_of::<Item>();

#[derive(Debug, Clone)]
pub struct TimestampView {
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
    pub(crate) precision: Precision,
}

impl IsColumnView for View {
    fn ty(&self) -> Ty {
        Ty::USmallInt
    }
    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self {
        Self::from_nullable_timestamp(iter.map(|v| v.to_timestamp()).collect_vec())
    }
}
impl TimestampView {
    pub fn from_millis(values: Vec<impl Into<Option<i64>>>) -> Self {
        TimestampMillisecondView::from_iter(values).into_inner()
    }

    pub fn from_micros(values: Vec<impl Into<Option<i64>>>) -> Self {
        TimestampMicrosecondView::from_iter(values).into_inner()
    }

    pub fn from_nanos(values: Vec<impl Into<Option<i64>>>) -> Self {
        TimestampNanosecondView::from_iter(values).into_inner()
    }

    pub fn from_timestamp(values: Vec<Timestamp>) -> Self {
        let precision = values.first().map(|ts| ts.precision()).unwrap_or_default();
        let values = values.into_iter().map(|ts| ts.as_raw_i64()).collect_vec();
        match precision {
            Precision::Millisecond => Self::from_millis(values),
            Precision::Microsecond => Self::from_micros(values),
            Precision::Nanosecond => Self::from_nanos(values),
        }
    }
    pub fn from_nullable_timestamp(values: Vec<Option<Timestamp>>) -> Self {
        let precision = values
            .iter()
            .find(|ts| ts.is_some())
            .map(|v| v.as_ref().unwrap().precision());
        if let Some(precision) = precision {
            let values = values
                .into_iter()
                .map(|ts| ts.map(|v| v.as_raw_i64()))
                .collect_vec();
            match precision {
                Precision::Millisecond => Self::from_millis(values),
                Precision::Microsecond => Self::from_micros(values),
                Precision::Nanosecond => Self::from_nanos(values),
            }
        } else {
            Self::from_millis(vec![None; values.len()])
        }
    }

    /// Precision for current view
    pub fn precision(&self) -> Precision {
        self.precision
    }

    /// Rows
    pub fn len(&self) -> usize {
        self.data.len() / std::mem::size_of::<Item>()
    }

    /// Raw slice of target type.
    pub fn as_raw_slice(&self) -> &[Item] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const Item, self.len()) }
    }

    /// Raw pointer of the slice.
    pub fn as_raw_ptr(&self) -> *const Item {
        self.data.as_ptr() as *const Item
    }

    /// Build a nulls vector.
    pub fn to_nulls_vec(&self) -> Vec<bool> {
        self.is_null_iter().collect()
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    pub fn is_null_iter(&self) -> NullsIter {
        NullsIter {
            nulls: &self.nulls,
            row: 0,
            len: self.len(),
        }
    }

    /// Check if the value at `row` index is NULL or not.
    pub fn is_null(&self, row: usize) -> bool {
        if row < self.len() {
            unsafe { self.is_null_unchecked(row) }
        } else {
            false
        }
    }

    /// Unsafe version for [methods.is_null]
    pub unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        self.nulls.is_null_unchecked(row)
    }

    /// Get nullable value at `row` index.
    pub fn get(&self, row: usize) -> Option<Option<Timestamp>> {
        if row < self.len() {
            Some(unsafe { self.get_unchecked(row) })
        } else {
            None
        }
    }

    #[inline(always)]
    unsafe fn get_raw_at(&self, index: usize) -> *const Item {
        self.data.as_ptr().add(index * ITEM_SIZE) as _
    }

    /// Get nullable value at `row` index.
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<Timestamp> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(Timestamp::new(
                std::ptr::read_unaligned(
                    self.data.as_ptr().add(row * std::mem::size_of::<Item>()) as _
                ),
                // *self.get_raw_at(row),
                self.precision,
            ))
        }
    }

    pub unsafe fn get_ref_unchecked(&self, row: usize) -> Option<*const Item> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(self.get_raw_at(row))
        }
    }

    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map(BorrowedValue::Timestamp)
            .unwrap_or(BorrowedValue::Null(Ty::Timestamp))
    }

    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        if self.nulls.is_null_unchecked(row) {
            (
                Ty::Timestamp,
                std::mem::size_of::<Item>() as _,
                std::ptr::null(),
            )
        } else {
            (
                Ty::Timestamp,
                std::mem::size_of::<Item>() as _,
                self.get_raw_at(row) as *const Item as _,
            )
        }
    }

    /// Create a slice of view.
    pub fn slice(&self, mut range: std::ops::Range<usize>) -> Option<Self> {
        if range.start >= self.len() {
            return None;
        }
        if range.end > self.len() {
            range.end = self.len();
        }
        if range.is_empty() {
            return None;
        }

        let nulls = unsafe { self.nulls.slice(range.clone()) };
        let data = self
            .data
            .slice(range.start * ITEM_SIZE..range.end * ITEM_SIZE);
        Some(Self {
            nulls,
            data,
            precision: self.precision,
        })
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> TimestampViewIter {
        TimestampViewIter { view: self, row: 0 }
    }

    /// Convert data to a vector of all nullable values.
    pub fn to_vec(&self) -> Vec<Option<Timestamp>> {
        self.iter().collect()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let nulls = self.nulls.0.as_ref();
        wtr.write_all(nulls)?;
        wtr.write_all(&self.data)?;
        Ok(nulls.len() + self.data.len())
    }

    pub fn concat(&self, rhs: &View) -> View {
        let nulls = NullBits::from_iter(
            self.nulls
                .iter()
                .take(self.len())
                .chain(rhs.nulls.iter().take(rhs.len())),
        );
        let data: Bytes = self
            .data
            .as_ref()
            .iter()
            .chain(rhs.data.as_ref().iter())
            .copied()
            .collect();

        View {
            nulls,
            data,
            precision: self.precision,
        }
    }
}

pub struct TimestampViewIter<'a> {
    view: &'a TimestampView,
    row: usize,
}

impl<'a> Iterator for TimestampViewIter<'a> {
    type Item = Option<Timestamp>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row < self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.get_unchecked(row) })
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.row < self.view.len() {
            let len = self.view.len() - self.row;
            (len, Some(len))
        } else {
            (0, Some(0))
        }
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.view.get(self.view.len() - 1)
    }
}

impl<'a> ExactSizeIterator for TimestampViewIter<'a> {}

pub struct TimestampMillisecondView(View);

impl TimestampMillisecondView {
    pub fn into_inner(self) -> View {
        self.0
    }
}

impl<A: Into<Option<Item>>> FromIterator<A> for TimestampMillisecondView {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let (nulls, mut values): (Vec<bool>, Vec<_>) = iter
            .into_iter()
            .map(|v| match v.into() {
                Some(v) => (false, v),
                None => (true, Item::default()),
            })
            .unzip();
        Self(View {
            nulls: NullBits::from_iter(nulls),
            data: Bytes::from({
                let (ptr, len, cap) = (values.as_mut_ptr(), values.len(), values.capacity());
                std::mem::forget(values);
                unsafe { Vec::from_raw_parts(ptr as *mut u8, len * ITEM_SIZE, cap * ITEM_SIZE) }
            }),
            precision: Precision::Millisecond,
        })
    }
}
pub struct TimestampMicrosecondView(View);
impl TimestampMicrosecondView {
    pub fn into_inner(self) -> View {
        self.0
    }
}

impl<A: Into<Option<Item>>> FromIterator<A> for TimestampMicrosecondView {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let (nulls, mut values): (Vec<bool>, Vec<_>) = iter
            .into_iter()
            .map(|v| match v.into() {
                Some(v) => (false, v),
                None => (true, Item::default()),
            })
            .unzip();
        Self(View {
            nulls: NullBits::from_iter(nulls),
            data: Bytes::from({
                let (ptr, len, cap) = (values.as_mut_ptr(), values.len(), values.capacity());
                std::mem::forget(values);
                unsafe { Vec::from_raw_parts(ptr as *mut u8, len * ITEM_SIZE, cap * ITEM_SIZE) }
            }),
            precision: Precision::Microsecond,
        })
    }
}
pub struct TimestampNanosecondView(View);
impl TimestampNanosecondView {
    pub fn into_inner(self) -> View {
        self.0
    }
}

impl<A: Into<Option<Item>>> FromIterator<A> for TimestampNanosecondView {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let (nulls, mut values): (Vec<bool>, Vec<_>) = iter
            .into_iter()
            .map(|v| match v.into() {
                Some(v) => (false, v),
                None => (true, Item::default()),
            })
            .unzip();
        Self(View {
            nulls: NullBits::from_iter(nulls),
            data: Bytes::from({
                let (ptr, len, cap) = (values.as_mut_ptr(), values.len(), values.capacity());
                std::mem::forget(values);
                unsafe { Vec::from_raw_parts(ptr as *mut u8, len * ITEM_SIZE, cap * ITEM_SIZE) }
            }),
            precision: Precision::Nanosecond,
        })
    }
}

#[test]
fn test_slice() {
    let data = [0, 1, Item::MIN, Item::MAX];
    let view = TimestampMillisecondView::from_iter(data).into_inner();
    dbg!(&view);
    let slice = view.slice(1..3);
    dbg!(&slice);

    let ts_min = chrono::NaiveDateTime::MIN.timestamp_millis();
    let ts_max = chrono::NaiveDateTime::MAX.timestamp_millis();
    let data = [None, Some(ts_min), Some(ts_max), Some(0)];
    let view = TimestampMillisecondView::from_iter(data).into_inner();
    dbg!(&view);
    let range = 1..4;
    let slice = view.slice(range.clone()).unwrap();
    for (v, i) in slice.iter().zip(range) {
        assert_eq!(v.map(|ts| ts.as_raw_i64()), data[i]);
        let inner = v.unwrap();
        dbg!(inner.to_naive_datetime());
    }
}
