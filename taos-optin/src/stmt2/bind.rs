use std::{ffi::CString, ptr};

use derive_more::Deref;
use taos_query::{
    common::{itypes::IsValue, ColumnView, Ty},
    stmt2::Stmt2BindParam,
    RawError, RawResult,
};

use crate::types::{BindFrom, TaosStmt2Bind, TaosStmt2Bindv};

#[derive(Debug, Deref)]
#[repr(transparent)]
pub struct TaosStmt2BindTag(TaosStmt2Bind);

impl TaosStmt2BindTag {
    fn new(ty: Ty) -> Self {
        Self(TaosStmt2Bind {
            buffer_type: ty as _,
            buffer: ptr::null_mut(),
            length: ptr::null_mut(),
            is_null: ptr::null_mut(),
            num: 1,
        })
    }
}

impl BindFrom for TaosStmt2BindTag {
    fn null() -> Self {
        let mut bind = Self::new(Ty::Null);
        bind.0.is_null = Box::into_raw(Box::new(1i8)) as _;
        bind
    }

    fn from_primitive<T: IsValue + Clone>(value: &T) -> Self {
        let mut bind = Self::new(T::TY);
        bind.0.buffer = Box::into_raw(Box::new(value.clone())) as _;
        bind.0.length = Box::into_raw(Box::new(value.fixed_length() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_timestamp(value: i64) -> Self {
        let mut bind = Self::new(Ty::Timestamp);
        bind.0.buffer = Box::into_raw(Box::new(value)) as _;
        bind.0.length = Box::into_raw(Box::new(std::mem::size_of::<i64>() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_varchar(value: &str) -> Self {
        let mut bind = Self::new(Ty::VarChar);
        bind.0.buffer = value.as_ptr() as _;
        bind.0.length = Box::into_raw(Box::new(value.len() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_nchar(value: &str) -> Self {
        let mut bind = Self::new(Ty::NChar);
        bind.0.buffer = value.as_ptr() as _;
        bind.0.length = Box::into_raw(Box::new(value.len() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }

    fn from_json(value: &str) -> Self {
        let mut bind = Self::new(Ty::Json);
        bind.0.buffer = value.as_ptr() as _;
        bind.0.length = Box::into_raw(Box::new(value.len() as i32));
        bind.0.is_null = Box::into_raw(Box::new(0i8)) as _;
        bind
    }
}

impl Drop for TaosStmt2BindTag {
    fn drop(&mut self) {
        if !self.is_null.is_null() {
            let _ = unsafe { Box::from_raw(self.is_null as *mut i8) };
        }
        if !self.length.is_null() {
            let _ = unsafe { Box::from_raw(self.length) };
        }
        if !self.buffer.is_null() {
            let ty = Ty::from(self.buffer_type as u8);
            match ty {
                Ty::Bool => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut bool) };
                }
                Ty::TinyInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i8) };
                }
                Ty::SmallInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i16) };
                }
                Ty::Int => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i32) };
                }
                Ty::BigInt | Ty::Timestamp => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i64) };
                }
                Ty::UTinyInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u8) };
                }
                Ty::USmallInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u16) };
                }
                Ty::UInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u32) };
                }
                Ty::UBigInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u64) };
                }
                Ty::Float => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut f32) };
                }
                Ty::Double => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut f64) };
                }
                _ => (),
            }
        }
    }
}

#[derive(Debug, Deref)]
#[repr(transparent)]
pub struct TaosStmt2BindColumn(TaosStmt2Bind);

impl<'a> From<&'a ColumnView> for TaosStmt2BindColumn {
    fn from(value: &'a ColumnView) -> Self {
        todo!()
    }
}

pub struct Stmt2BindvGuard {
    pub bindv: TaosStmt2Bindv,
    tag_lens: Vec<usize>,
    col_lens: Vec<usize>,
}

impl Drop for Stmt2BindvGuard {
    fn drop(&mut self) {
        unsafe {
            let count = self.bindv.count as usize;
            let tbnames = self.bindv.tbnames;
            let tags = self.bindv.tags;
            let cols = self.bindv.bind_cols;

            for i in 0..count as isize {
                let tbname = *tbnames.offset(i);
                if !tbname.is_null() {
                    let _ = CString::from_raw(tbname);
                }

                let tag = *tags.offset(i) as *mut TaosStmt2BindTag;
                if !tag.is_null() {
                    let len = self.tag_lens[i as usize];
                    let _ = Vec::from_raw_parts(tag, len, len);
                }

                let col = *cols.offset(i) as *mut TaosStmt2BindColumn;
                if !col.is_null() {
                    let len = self.col_lens[i as usize];
                    let _ = Vec::from_raw_parts(col, len, len);
                }
            }

            let _ = Vec::from_raw_parts(tbnames, count, count);
            let _ = Vec::from_raw_parts(tags, count, count);
            let _ = Vec::from_raw_parts(cols, count, count);
        }
    }
}

pub fn build_bindv(params: &[Stmt2BindParam]) -> RawResult<Stmt2BindvGuard> {
    let count = params.len();
    let mut tbnames = Vec::with_capacity(count);
    let mut tag_ptrs = Vec::with_capacity(count);
    let mut tag_lens = Vec::with_capacity(count);
    let mut col_ptrs = Vec::with_capacity(count);
    let mut col_lens = Vec::with_capacity(count);

    for param in params {
        if let Some(tbname) = param.table_name() {
            let tbname = CString::new(tbname.as_str()).map_err(RawError::from_any)?;
            tbnames.push(tbname.into_raw());
        } else {
            tbnames.push(ptr::null_mut());
        }

        if let Some(tags) = param.tags() {
            let mut ts = Vec::with_capacity(tags.len());
            for tag in tags {
                ts.push(TaosStmt2BindTag::from_value(tag));
            }
            let ptr = if ts.is_empty() {
                ptr::null_mut()
            } else {
                ts.as_mut_ptr() as *mut TaosStmt2Bind
            };
            tag_ptrs.push(ptr);
            tag_lens.push(ts.len());
            std::mem::forget(ts);
        } else {
            tag_ptrs.push(ptr::null_mut());
            tag_lens.push(0);
        }

        if let Some(cols) = param.columns() {
            let mut cs = Vec::with_capacity(cols.len());
            for col in cols {
                cs.push(TaosStmt2BindColumn::from(col));
            }
            let ptr = if cs.is_empty() {
                ptr::null_mut()
            } else {
                cs.as_mut_ptr() as *mut TaosStmt2Bind
            };
            col_ptrs.push(ptr);
            col_lens.push(cs.len());
            std::mem::forget(cs);
        } else {
            col_ptrs.push(ptr::null_mut());
            col_lens.push(0);
        }
    }

    let bindv = TaosStmt2Bindv {
        count: count as _,
        tbnames: tbnames.as_mut_ptr() as _,
        tags: tag_ptrs.as_mut_ptr() as _,
        bind_cols: col_ptrs.as_mut_ptr() as _,
    };

    std::mem::forget(tbnames);
    std::mem::forget(tag_ptrs);
    std::mem::forget(col_ptrs);

    Ok(Stmt2BindvGuard {
        bindv,
        tag_lens,
        col_lens,
    })
}
