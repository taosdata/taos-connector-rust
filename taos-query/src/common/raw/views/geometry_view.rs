use std::{borrow::Cow, ffi::c_void, fmt::Debug};

use super::{IsColumnView, Offsets};
use crate::{
    common::{BorrowedValue, Ty},
    prelude::InlinableWrite,
    util::InlineBytes,
};

use bytes::Bytes;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct GeometryView {
    // version: Version,
    pub(crate) offsets: Offsets,
    pub(crate) data: Bytes,
}

impl IsColumnView for GeometryView {
    fn ty(&self) -> Ty {
        Ty::Geometry
    }
    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self {
        Self::from_iter::<Bytes, _, _, _>(iter.map(|v| v.to_bytes()).collect_vec())
    }
}

impl GeometryView {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    pub fn is_null_iter(&self) -> GeometryNullsIter {
        GeometryNullsIter { view: self, row: 0 }
    }

    /// Build a nulls vector.
    pub fn to_nulls_vec(&self) -> Vec<bool> {
        self.is_null_iter().collect()
    }

    /// Check if the value at `row` index is NULL or not.
    ///
    /// Returns null when `row` index out of bound.
    pub fn is_null(&self, row: usize) -> bool {
        if row < self.len() {
            unsafe { self.is_null_unchecked(row) }
        } else {
            false
        }
    }

    /// Unsafe version for [is_null](#method.is_null)
    pub(crate) unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        self.offsets.get_unchecked(row) < 0
    }

    pub(crate) unsafe fn get_unchecked(&self, row: usize) -> Option<&InlineBytes> {
        let offset = self.offsets.get_unchecked(row);
        if offset >= 0 {
            Some(InlineBytes::<u16>::from_ptr(
                self.data.as_ptr().offset(offset as isize),
            ))
        } else {
            None
        }
    }

    pub(crate) unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map(|s| BorrowedValue::Geometry(Cow::Borrowed(s.as_bytes())))
            .unwrap_or(BorrowedValue::Null(Ty::Geometry))
    }

    pub(crate) unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        match self.get_unchecked(row) {
            Some(s) => (Ty::Geometry, s.len() as _, s.as_ptr() as _),
            None => (Ty::Geometry, 0, std::ptr::null()),
        }
    }

    #[inline]
    pub unsafe fn get_length_unchecked(&self, row: usize) -> Option<usize> {
        let offset = self.offsets.get_unchecked(row);
        if offset >= 0 {
            Some(InlineBytes::<u16>::from_ptr(self.data.as_ptr().offset(offset as isize)).len())
        } else {
            None
        }
    }

    #[inline]
    pub fn lengths(&self) -> Vec<Option<usize>> {
        (0..self.len())
            .map(|i| unsafe { self.get_length_unchecked(i) })
            .collect()
    }

    #[inline]
    pub fn max_length(&self) -> usize {
        (0..self.len())
            .filter_map(|i| unsafe { self.get_length_unchecked(i) })
            .min()
            .unwrap_or(0)
    }

    pub fn iter(&self) -> GeometryIter {
        GeometryIter { view: self, row: 0 }
    }

    pub fn to_vec(&self) -> Vec<Option<Vec<u8>>> {
        (0..self.len())
            .map(|row| {
                unsafe { self.get_unchecked(row) }
                    .map(|s| s.as_bytes())
                    .map(|s| s.to_vec())
            })
            .collect()
    }
    // pub fn iter_as_bytes(&self) -> impl Iterator<Item = Option<&[u8]>> {
    //     (0..self.len()).map(|row| unsafe { self.get_unchecked(row) }.map(|s| s.as_bytes()))
    // }

    // pub fn to_bytes_vec(&self) -> Vec<Option<&[u8]>> {
    //     self.iter_as_bytes().collect_vec()
    // }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let mut offsets = Vec::with_capacity(self.len());
        let mut bytes: Vec<u8> = Vec::new();
        for v in self.iter() {
            if let Some(v) = v {
                offsets.push(bytes.len() as i32);
                bytes.write_inlined_bytes::<2>(v.as_bytes()).unwrap();
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
        // let offsets = self.offsets.as_bytes();
        // dbg!(self, offsets);
        // wtr.write_all(offsets)?;
        // wtr.write_all(&self.data)?;
        // Ok(offsets.len() + self.data.len())
    }

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
                data.write_inlined_bytes::<2>(s).unwrap();
            } else {
                offsets.push(-1);
            }
        }
        // dbg!(&offsets);
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

    // pub fn concat(&self, rhs: &Self) -> Self {
    //     Self::from_iter::<&InlineJson, _, _, _>(self.iter().chain(rhs.iter()).collect_vec())
    // }
}

pub struct GeometryIter<'a> {
    view: &'a GeometryView,
    row: usize,
}

impl<'a> Iterator for GeometryIter<'a> {
    type Item = Option<&'a InlineBytes>;

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

impl<'a> ExactSizeIterator for GeometryIter<'a> {}

pub struct GeometryNullsIter<'a> {
    view: &'a GeometryView,
    row: usize,
}

impl<'a> Iterator for GeometryNullsIter<'a> {
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

impl<'a> ExactSizeIterator for GeometryNullsIter<'a> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}
