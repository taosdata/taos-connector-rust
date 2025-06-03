use std::borrow::Cow;
use std::ffi::c_void;

use bytes::Bytes;
use itertools::Itertools;

use super::{IsColumnView, Offsets};
use crate::common::{BorrowedValue, Ty};
use crate::prelude::InlinableWrite;
use crate::util::InlineBytes;

#[derive(Debug, Clone)]
pub struct BlobView {
    pub(crate) offsets: Offsets,
    pub(crate) data: Bytes,
}

impl IsColumnView for BlobView {
    fn ty(&self) -> Ty {
        Ty::Blob
    }

    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self {
        Self::from_iter::<Bytes, _, _, _>(iter.map(|v| v.to_bytes()).collect_vec())
    }
}

impl BlobView {
    /// Get the length of the `offsets`.
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Get the raw pointer to the underlying data.
    #[inline]
    pub fn as_raw_ptr(&self) -> *const u8 {
        self.data.as_ptr() as _
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    #[inline]
    pub fn is_null_iter(&self) -> BlobNullsIter {
        BlobNullsIter { view: self, row: 0 }
    }

    /// Build a nulls vector.
    #[inline]
    pub fn to_nulls_vec(&self) -> Vec<bool> {
        self.is_null_iter().collect()
    }

    /// Check if the value at `row` index is NULL or not.
    ///
    /// Returns null when `row` index out of bound.
    #[inline]
    pub fn is_null(&self, row: usize) -> bool {
        if row < self.len() {
            unsafe { self.is_null_unchecked(row) }
        } else {
            false
        }
    }

    /// Unsafe version for [is_null](#method.is_null)
    #[inline]
    pub(crate) unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        self.offsets.get_unchecked(row) < 0
    }

    /// Get the `InlineBytes<u32>` at `row` index.
    #[inline]
    pub(crate) unsafe fn get_unchecked(&self, row: usize) -> Option<&InlineBytes<u32>> {
        let offset = self.offsets.get_unchecked(row);
        if offset >= 0 {
            Some(InlineBytes::<u32>::from_ptr(
                self.data.as_ptr().offset(offset as isize),
            ))
        } else {
            None
        }
    }

    /// Get the value at `row` index.
    #[inline]
    pub(crate) unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::Blob), |b| {
                BorrowedValue::Blob(Cow::Borrowed(b.as_bytes()))
            })
    }

    /// Get the raw value at `row` index.
    #[inline]
    pub(crate) unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        match self.get_unchecked(row) {
            Some(b) => (Ty::Blob, b.len() as _, b.as_ptr() as _),
            None => (Ty::Blob, 0, std::ptr::null()),
        }
    }

    /// Get the length of the value at `row` index.
    #[inline]
    pub unsafe fn get_length_unchecked(&self, row: usize) -> Option<usize> {
        let offset = self.offsets.get_unchecked(row);
        if offset >= 0 {
            Some(InlineBytes::<u32>::from_ptr(self.data.as_ptr().offset(offset as isize)).len())
        } else {
            None
        }
    }

    /// Get the lengths of all values.
    #[inline]
    pub fn lengths(&self) -> Vec<Option<usize>> {
        (0..self.len())
            .map(|i| unsafe { self.get_length_unchecked(i) })
            .collect()
    }

    /// Get the maximum length of all values.
    #[inline]
    pub fn max_length(&self) -> usize {
        (0..self.len())
            .filter_map(|i| unsafe { self.get_length_unchecked(i) })
            .max()
            .unwrap_or(0)
    }

    /// Get an iterator for the `BlobView`.
    #[inline]
    pub fn iter(&self) -> BlobIter {
        BlobIter { view: self, row: 0 }
    }

    /// Convert the `BlobView` to a vector of optional byte vectors.
    #[inline]
    pub fn to_vec(&self) -> Vec<Option<Vec<u8>>> {
        (0..self.len())
            .map(|row| unsafe { self.get_unchecked(row) }.map(|b| b.as_bytes().to_vec()))
            .collect()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let mut offsets = Vec::with_capacity(self.len());
        let mut bytes: Vec<u8> = Vec::new();
        for v in self.iter() {
            if let Some(v) = v {
                offsets.push(bytes.len() as i32);
                bytes.write_inlined_bytes::<4>(v.as_bytes()).unwrap();
            } else {
                offsets.push(-1);
            }
        }

        unsafe {
            let offsets_bytes = std::slice::from_raw_parts(
                offsets.as_ptr() as *const u8,
                offsets.len() * std::mem::size_of::<i32>(),
            );
            wtr.write_all(offsets_bytes)?;
            wtr.write_all(&bytes)?;
            Ok(offsets_bytes.len() + bytes.len())
        }
    }

    /// Create a new `BlobView` from an iterator of optional byte slices.
    pub fn from_iter<
        S: AsRef<[u8]>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        let iter = iter.into_iter();
        let mut offsets = Vec::with_capacity(iter.len());
        let mut data = Vec::new();

        for i in iter.map(|v| v.into()) {
            if let Some(s) = i {
                let s: &[u8] = s.as_ref();
                offsets.push(data.len() as i32);
                data.write_inlined_bytes::<4>(s).unwrap();
            } else {
                offsets.push(-1);
            }
        }

        let offsets_bytes = unsafe {
            Vec::from_raw_parts(
                offsets.as_mut_ptr() as *mut u8,
                offsets.len() * 4,
                offsets.capacity() * 4,
            )
        };

        std::mem::forget(offsets);

        Self {
            offsets: Offsets(offsets_bytes.into()),
            data: data.into(),
        }
    }
}

pub struct BlobIter<'a> {
    view: &'a BlobView,
    row: usize,
}

impl<'a> Iterator for BlobIter<'a> {
    type Item = Option<&'a InlineBytes<u32>>;

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
        let len = self.view.len() - self.row;
        (len, Some(len))
    }
}

impl ExactSizeIterator for BlobIter<'_> {}

pub struct BlobNullsIter<'a> {
    view: &'a BlobView,
    row: usize,
}

impl Iterator for BlobNullsIter<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row < self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.is_null_unchecked(row) })
        } else {
            None
        }
    }
}

impl ExactSizeIterator for BlobNullsIter<'_> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}
