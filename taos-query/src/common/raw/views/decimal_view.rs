use std::marker::PhantomData;

use bytes::Bytes;

use crate::common::{self, BorrowedValue, Ty};

use super::{IsColumnView, NullBits, NullsIter};

type Item<T> = common::decimal::Decimal<T>;
type View<T> = DecimalView<T>;

#[derive(Debug, Clone)]
pub struct DecimalView<T> {
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
    pub(crate) _p: PhantomData<T>,
}

macro_rules! impl_is_column_view {
    ($ty: expr, $prim: ty) => {
        impl IsColumnView for View<$prim> {
            fn ty(&self) -> Ty {
                $ty
            }

            fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self {
                iter.map(|v| {
                    v.to_decimal().and_then(|v| {
                        use bigdecimal::ToPrimitive;
                        let (data, scale) = v.as_bigint_and_exponent();
                        paste::paste! {
                            data.[<to_$prim>]().map(|data| Item {
                                data,
                                precision: v.digits() as _,
                                scale: scale as _,
                            })
                        }
                    })
                })
                .collect()
            }
        }
    };
}

impl_is_column_view!(Ty::Decimal, i128);
impl_is_column_view!(Ty::Decimal64, i64);

impl<T> DecimalView<T> {
    const ITEM_SIZE: usize = std::mem::size_of::<T>();

    /// Rows
    pub fn len(&self) -> usize {
        self.data.len() / std::mem::size_of::<T>()
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
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<Item<T>> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(Item {
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
        Some(Self {
            nulls,
            data,
            precision: self.precision,
            scale: self.scale,
            _p: PhantomData,
        })
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> DecimalViewIter<T> {
        DecimalViewIter { view: self, row: 0 }
    }

    /// Convert data to a vector of all nullable values.
    pub fn to_vec(&self) -> Vec<Option<Item<T>>> {
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
        if self.precision != rhs.precision || self.scale != rhs.scale {
            panic!("decimal strict concat needs same schema")
        }

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

        View {
            nulls,
            data,
            precision: self.precision,
            scale: self.scale,
            _p: PhantomData,
        }
    }
}

impl DecimalView<i128> {
    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::Decimal), BorrowedValue::Decimal)
    }
}

impl DecimalView<i64> {
    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::Decimal), BorrowedValue::Decimal64)
    }
}

macro_rules! impl_from_iter {
    ($prim: ty) => {
        impl<A: Into<Option<Item<$prim>>>> FromIterator<A> for DecimalView<$prim> {
            fn from_iter<I: IntoIterator<Item = A>>(iter: I) -> Self {
                let mut first_precision: Option<u8> = None;
                let mut first_scale: Option<u8> = None;
                let (nulls, mut values): (Vec<bool>, Vec<_>) = iter
                    .into_iter()
                    .map(|v| match v.into() {
                        Some(v) => {
                            let (num, scale) = (v.data, v.scale);
                            if let Some(first_scale) = first_scale {
                                if first_scale != scale {
                                    (false, 0)
                                } else {
                                    (true, num)
                                }
                            } else {
                                let _ = first_precision.insert(v.precision);
                                let _ = first_scale.insert(scale);
                                (true, num)
                            }
                        }
                        None => (false, 0),
                    })
                    .unzip();
                Self {
                    nulls: NullBits::from_iter(nulls),
                    data: Bytes::from({
                        let (ptr, len, cap) =
                            (values.as_mut_ptr(), values.len(), values.capacity());
                        std::mem::forget(values);

                        let item_size = std::mem::size_of::<i64>();

                        #[cfg(target_endian = "little")]
                        unsafe {
                            Vec::from_raw_parts(ptr as *mut u8, len * item_size, cap * item_size)
                        }

                        #[cfg(target_endian = "big")]
                        {
                            let mut bytes = unsafe {
                                Vec::from_raw_parts(
                                    ptr as *mut u8,
                                    len * item_size,
                                    cap * item_size,
                                )
                            };
                            for i in (0..bytes.len()).step_by(item_size) {
                                let j = i + item_size;
                                let val = i64::from_ne_bytes(
                                    &bytes[i..j].try_into().expect("slice with incorrect length"),
                                );
                                bytes[i..j].copy_from_slice(&val.to_le_bytes());
                            }
                            bytes
                        }
                    }),
                    precision: first_precision.unwrap_or_default(),
                    scale: first_scale.unwrap_or_default(),
                    _p: PhantomData,
                }
            }
        }
    };
}

impl_from_iter!(i128);
impl_from_iter!(i64);

pub struct DecimalViewIter<'a, T> {
    view: &'a DecimalView<T>,
    row: usize,
}

impl<T> Iterator for DecimalViewIter<'_, T> {
    type Item = Option<Item<T>>;

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
