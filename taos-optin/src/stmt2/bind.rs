use std::{ffi::CString, ptr};

use taos_query::{stmt2::Stmt2BindParam, RawError, RawResult};

use crate::types::{
    BindFrom, TaosStmt2Bind, TaosStmt2BindColumn, TaosStmt2BindTag, TaosStmt2Bindv,
};

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
