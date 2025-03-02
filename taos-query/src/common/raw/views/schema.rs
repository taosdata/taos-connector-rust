use std::fmt::Debug;
use std::ops::Deref;

use bytes::Bytes;

use crate::common::Ty;

/// Represent column basics information: type, length.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
#[repr(packed(1))]
pub struct ColSchema {
    pub(crate) ty: Ty,
    pub(crate) len: u32,
}

impl ColSchema {
    #[inline]
    pub(crate) const fn new(ty: Ty, len: u32) -> Self {
        Self { ty, len }
    }

    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        unsafe { std::mem::transmute::<&Self, &[u8; 5]>(self) }
    }

    #[inline]
    pub fn into_bytes(self) -> [u8; 5] {
        unsafe { std::mem::transmute::<Self, [u8; 5]>(self) }
    }

    #[inline]
    pub fn len(&self) -> u32 {
        self.len
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
            len: 1,
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
}
