use std::ffi::{c_char, c_void, CString};
use std::marker::PhantomData;
use std::ptr;

use bytes::Bytes;

use super::{NullBits, NullsIter};
use crate::common::{BorrowedValue, Ty};
use crate::decimal::{Decimal, DecimalAllowedTy};

type View<T> = DecimalView<T>;

#[derive(Debug, Clone)]
pub struct DecimalView<T>
where
    T: DecimalAllowedTy,
{
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
    pub(crate) _p: PhantomData<T>,
    buf: Option<Vec<Option<*mut c_char>>>,
}

impl<T> DecimalView<T>
where
    T: DecimalAllowedTy,
{
    const ITEM_SIZE: usize = std::mem::size_of::<T>();

    /// Create a new decimal view.
    pub fn new(nulls: NullBits, data: Bytes, precision: u8, scale: u8) -> Self {
        Self {
            nulls,
            data,
            precision,
            scale,
            _p: PhantomData,
            buf: None,
        }
    }

    pub fn precision_and_scale(&self) -> (u8, u8) {
        (self.precision, self.scale)
    }

    /// Rows
    pub fn len(&self) -> usize {
        self.data.len() / std::mem::size_of::<T>()
    }

    pub fn as_raw_ptr(&self) -> *const u8 {
        self.data.as_ptr() as _
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

    #[inline(always)]
    unsafe fn get_raw_data_at(&self, index: usize) -> *const T {
        self.data.as_ptr().add(index * Self::ITEM_SIZE) as _
    }

    /// Get nullable value at `row` index.
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<Decimal<T>> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(Decimal {
                data: self.get_raw_data_at(row).read_unaligned(),
                precision: self.precision,
                scale: self.scale,
            })
        }
    }

    /// Create a slice of view.
    pub fn slice(&self, mut range: std::ops::Range<usize>) -> Option<Self> {
        if range.start >= self.len() {
            return None;
        }
        if range.end >= self.len() {
            range.end = self.len();
        }
        if range.is_empty() {
            return None;
        }

        let item_size = std::mem::size_of::<T>();
        let nulls = unsafe { self.nulls.slice(range.clone()) };
        let data = self
            .data
            .slice(range.start * item_size..range.end * item_size);
        Some(Self::new(nulls, data, self.precision, self.scale))
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> DecimalViewIter<T> {
        DecimalViewIter { view: self, row: 0 }
    }

    /// Convert data to a vector of all nullable values.
    pub fn to_vec(&self) -> Vec<Option<Decimal<T>>> {
        self.iter().collect()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let nulls = self.nulls.0.as_ref();
        debug_assert_eq!(nulls.len(), (self.len() + 7) / 8);
        wtr.write_all(nulls)?;
        wtr.write_all(&self.data)?;
        Ok(nulls.len() + self.data.len())
    }

    pub fn concat(&self, rhs: &View<T>) -> View<T> {
        assert!(
            !(self.precision != rhs.precision || self.scale != rhs.scale),
            "decimal strict concat needs same schema"
        );

        let nulls = self
            .nulls
            .iter()
            .take(self.len())
            .chain(rhs.nulls.iter().take(rhs.len()))
            .collect();
        let data: Bytes = self
            .data
            .as_ref()
            .iter()
            .chain(rhs.data.as_ref().iter())
            .copied()
            .collect();

        Self::new(nulls, data, self.precision, self.scale)
    }

    /// Get raw value at `row` index.
    pub unsafe fn get_raw_value_unchecked(&mut self, row: usize) -> (Ty, u32, *const c_void) {
        let ptr = if self.nulls.is_null_unchecked(row) {
            ptr::null()
        } else {
            if self.buf.is_none() {
                self.buf = Some(vec![None; self.len()]);
            }

            let buf = self.buf.as_mut().unwrap();
            if buf[row].is_none() {
                let data =
                    (self.data.as_ptr().add(row * Self::ITEM_SIZE) as *const T).read_unaligned();
                let dec = Decimal::new(data, self.precision, self.scale);
                let str = dec.as_bigdecimal().to_string();
                let cstr = CString::new(str).unwrap();
                buf[row] = Some(cstr.into_raw());
            }

            buf[row].unwrap() as *const c_void
        };

        (T::ty(), Self::ITEM_SIZE as _, ptr)
    }
}

impl<T> Drop for DecimalView<T>
where
    T: DecimalAllowedTy,
{
    fn drop(&mut self) {
        if let Some(buf) = self.buf.as_mut() {
            for ptr in buf.iter().flatten() {
                let _ = unsafe { CString::from_raw(*ptr as *mut c_char) };
            }
        }
    }
}

macro_rules! impl_from_iter {
    ($ty: ty) => {
        pub fn from_values<I, T>(values: I, precision: u8, scale: u8) -> Self
        where
            T: Into<Option<$ty>>,
            I: IntoIterator<Item = T>,
        {
            let (nulls, mut values): (Vec<bool>, Vec<$ty>) = values
                .into_iter()
                .map(|v| match v.into() {
                    Some(v) => (false, v),
                    None => (true, 0),
                })
                .unzip();
            Self {
                nulls: NullBits::from_iter(nulls),
                data: bytes::Bytes::from({
                    let (ptr, len, cap) = (values.as_mut_ptr(), values.len(), values.capacity());
                    std::mem::forget(values);

                    let item_size = std::mem::size_of::<$ty>();

                    #[cfg(target_endian = "little")]
                    unsafe {
                        Vec::from_raw_parts(ptr as *mut u8, len * item_size, cap * item_size)
                    }

                    #[cfg(target_endian = "big")]
                    {
                        let mut bytes = unsafe {
                            Vec::from_raw_parts(ptr as *mut u8, len * item_size, cap * item_size)
                        };
                        for i in (0..bytes.len()).step_by(item_size) {
                            let j = i + item_size;
                            let val = <$ty>::from_ne_bytes(
                                &bytes[i..j].try_into().expect("slice with incorrect length"),
                            );
                            bytes[i..j].copy_from_slice(&val.to_le_bytes());
                        }
                        bytes
                    }
                }),
                precision,
                scale,
                _p: PhantomData,
                buf: None,
            }
        }
    };
}

impl DecimalView<i128> {
    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::Decimal), BorrowedValue::Decimal)
    }

    impl_from_iter!(i128);
}

impl DecimalView<i64> {
    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::Decimal64), BorrowedValue::Decimal64)
    }

    impl_from_iter!(i64);
}

pub struct DecimalViewIter<'a, T>
where
    T: DecimalAllowedTy,
{
    view: &'a DecimalView<T>,
    row: usize,
}

impl<T> Iterator for DecimalViewIter<'_, T>
where
    T: DecimalAllowedTy,
{
    type Item = Option<Decimal<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row < self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.get_unchecked(row) })
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.row < self.view.len() {
            let len = self.view.len() - self.row;
            (len, Some(len))
        } else {
            (0, Some(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::decimal::Decimal;

    #[test]
    fn from_iter_64_test() -> anyhow::Result<()> {
        let view = DecimalView::<i64>::from_values([Some(12345), None, Some(22), Some(5)], 10, 2);

        let (precision, scale) = view.precision_and_scale();
        assert_eq!(precision, 10);
        assert_eq!(scale, 2);

        assert_eq!(view.len(), 4);

        assert_eq!(
            unsafe { view.get_value_unchecked(0) },
            BorrowedValue::Decimal64(Decimal::new(12345, precision, scale))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(1) },
            BorrowedValue::Null(Ty::Decimal64)
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(2) },
            BorrowedValue::Decimal64(Decimal::new(22, precision, scale))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(3) },
            BorrowedValue::Decimal64(Decimal::new(5, precision, scale))
        );

        assert!(!view.is_null(0));
        assert!(view.is_null(1));
        assert!(!view.is_null(2));
        assert!(!view.is_null(3));
        assert!(!view.is_null(4));

        let mut nulls = view.is_null_iter();
        assert!(!nulls.next().unwrap());
        assert!(nulls.next().unwrap());
        assert!(!nulls.next().unwrap());
        assert!(!nulls.next().unwrap());

        let mut iter = view.iter();
        assert_eq!(
            iter.next(),
            Some(Some(Decimal::new(12345, precision, scale)))
        );
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(Decimal::new(22, precision, scale))));
        assert_eq!(iter.next(), Some(Some(Decimal::new(5, precision, scale))));
        assert_eq!(iter.next(), None);

        let view = view.slice(1..3).unwrap();
        let mut iter = view.iter();
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(Decimal::new(22, precision, scale))));
        assert_eq!(iter.next(), None);
        Ok(())
    }

    #[test]
    fn from_iter_128_test() -> anyhow::Result<()> {
        let view = DecimalView::<i128>::from_values([Some(12345), None, Some(22), Some(5)], 10, 2);

        let (precision, scale) = view.precision_and_scale();
        assert_eq!(precision, 10);
        assert_eq!(scale, 2);

        assert_eq!(view.len(), 4);

        assert_eq!(
            unsafe { view.get_value_unchecked(0) },
            BorrowedValue::Decimal(Decimal::new(12345, precision, scale))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(1) },
            BorrowedValue::Null(Ty::Decimal)
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(2) },
            BorrowedValue::Decimal(Decimal::new(22, precision, scale))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(3) },
            BorrowedValue::Decimal(Decimal::new(5, precision, scale))
        );

        assert!(!view.is_null(0));
        assert!(view.is_null(1));
        assert!(!view.is_null(2));
        assert!(!view.is_null(3));
        assert!(!view.is_null(4));

        let mut nulls = view.is_null_iter();
        assert!(!nulls.next().unwrap());
        assert!(nulls.next().unwrap());
        assert!(!nulls.next().unwrap());
        assert!(!nulls.next().unwrap());

        let mut iter = view.iter();
        assert_eq!(
            iter.next(),
            Some(Some(Decimal::new(12345, precision, scale)))
        );
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(Decimal::new(22, precision, scale))));
        assert_eq!(iter.next(), Some(Some(Decimal::new(5, precision, scale))));
        assert_eq!(iter.next(), None);

        let view = view.slice(1..3).unwrap();
        let mut iter = view.iter();
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(Decimal::new(22, precision, scale))));
        assert_eq!(iter.next(), None);
        Ok(())
    }
}
