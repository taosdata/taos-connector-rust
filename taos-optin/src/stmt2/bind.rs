use std::{ffi::CString, ptr};

use taos_query::{stmt2::Stmt2BindParam, RawError, RawResult};

use crate::types::{
    BindFrom, TaosStmt2Bind, TaosStmt2BindColumn, TaosStmt2BindTag, TaosStmt2Bindv,
};

pub struct TaosStmt2BindvOwned {
    pub bindv: TaosStmt2Bindv,
    _tag_lens: Vec<usize>,
    _col_lens: Vec<usize>,
}

pub fn build_bindv_owned(params: &[Stmt2BindParam]) -> RawResult<TaosStmt2BindvOwned> {
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
            let tags = tags
                .iter()
                .map(TaosStmt2BindTag::from_value)
                .collect::<Vec<_>>();
            let ptr = if tags.is_empty() {
                ptr::null_mut()
            } else {
                tags.as_ptr() as *mut TaosStmt2Bind
            };
            tag_ptrs.push(ptr);
            tag_lens.push(tags.len());
            std::mem::forget(tags);
        } else {
            tag_ptrs.push(ptr::null_mut());
            tag_lens.push(0);
        }

        if let Some(cols) = param.columns() {
            let cols = cols
                .iter()
                .map(TaosStmt2BindColumn::from)
                .collect::<Vec<_>>();
            let ptr = if cols.is_empty() {
                ptr::null_mut()
            } else {
                cols.as_ptr() as *mut TaosStmt2Bind
            };
            col_ptrs.push(ptr);
            col_lens.push(cols.len());
            std::mem::forget(cols);
        } else {
            col_ptrs.push(ptr::null_mut());
            col_lens.push(0);
        }
    }

    let bindv = TaosStmt2Bindv {
        count: count as _,
        tbnames: tbnames.as_ptr() as _,
        tags: tag_ptrs.as_ptr() as _,
        bind_cols: col_ptrs.as_ptr() as _,
    };

    std::mem::forget(tbnames);
    std::mem::forget(tag_ptrs);
    std::mem::forget(col_ptrs);

    Ok(TaosStmt2BindvOwned {
        bindv,
        _tag_lens: tag_lens,
        _col_lens: col_lens,
    })
}
