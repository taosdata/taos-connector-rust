use std::ffi::c_void;

use bytes::Bytes;

use super::{IsColumnView, NullBits, NullsIter};
use crate::common::{BorrowedValue, Ty};

type Item = u8;
type View = UTinyIntView;
const ITEM_SIZE: usize = std::mem::size_of::<Item>();

#[derive(Debug, Clone)]
pub struct UTinyIntView {
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
}

impl IsColumnView for View {
    fn ty(&self) -> Ty {
        Ty::UTinyInt
    }

    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self {
        iter.map(|v| v.to_u8()).collect()
    }
}
impl UTinyIntView {
    /// Rows
    pub fn len(&self) -> usize {
        self.data.len()
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
    pub fn get(&self, row: usize) -> Option<Item> {
        if row < self.len() {
            unsafe { self.get_unchecked(row) }
        } else {
            None
        }
    }

    #[inline(always)]
    unsafe fn get_raw_at(&self, index: usize) -> *const Item {
        self.data.as_ptr().add(index * ITEM_SIZE) as _
    }

    /// Get nullable value at `row` index.
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<Item> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(self.get_raw_at(row).read_unaligned())
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
            .map_or(BorrowedValue::Null(Ty::UTinyInt), BorrowedValue::UTinyInt)
    }

    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        if self.nulls.is_null_unchecked(row) {
            (
                Ty::UTinyInt,
                std::mem::size_of::<Item>() as _,
                std::ptr::null(),
            )
        } else {
            (
                Ty::UTinyInt,
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
        Some(Self { nulls, data })
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> UTinyIntViewIter {
        UTinyIntViewIter { view: self, row: 0 }
    }

    /// Convert data to a vector of all nullable values.
    pub fn to_vec(&self) -> Vec<Option<Item>> {
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

        View { nulls, data }
    }
}

pub struct UTinyIntViewIter<'a> {
    view: &'a UTinyIntView,
    row: usize,
}

impl Iterator for UTinyIntViewIter<'_> {
    type Item = Option<Item>;

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

impl ExactSizeIterator for UTinyIntViewIter<'_> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

#[test]
fn test_slice() {
    let data = [0, 1, Item::MIN, Item::MAX];
    let view = View::from_iter(data);
    dbg!(&view);
    let slice = view.slice(1..3);
    dbg!(&slice);

    let data = [None, Some(Item::MIN), Some(Item::MAX), None];
    let view = View::from_iter(data);
    dbg!(&view);
    let range = 1..4;
    let slice = view.slice(range.clone()).unwrap();
    for (v, i) in slice.iter().zip(range) {
        assert_eq!(v, data[i]);
    }
}
