use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::{ptr, slice, str};

use taos_error::Code;
use taos_query::common::{ColumnView, Timestamp, Ty, Value};
use taos_query::stmt2::{Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_query::{block_in_place_or_global, global_tokio_runtime};
use taos_ws::query::{BindType, Stmt2Field};
use taos_ws::{Stmt2, Taos};
use tracing::{debug, error, Instrument};

use crate::taos::stmt2::{
    TAOS_FIELD_ALL, TAOS_STMT2, TAOS_STMT2_BIND, TAOS_STMT2_BINDV, TAOS_STMT2_OPTION,
};
use crate::taos::{__taos_async_fn_t, TAOS_RES};
use crate::ws::error::*;
use crate::ws::query::{taos_free_result, QueryResultSet};
use crate::ws::{ResultSet, SafePtr, TaosResult, TAOS};

#[derive(Debug)]
struct TaosStmt2 {
    stmt2: Stmt2,
    async_exec_fn: Option<__taos_async_fn_t>,
    userdata: *mut c_void,
    fields_len: Option<usize>,
    res: Option<*mut TAOS_RES>,
}

unsafe impl Send for TaosStmt2 {}
unsafe impl Sync for TaosStmt2 {}

impl TaosStmt2 {
    fn new(stmt2: Stmt2, async_exec_fn: Option<__taos_async_fn_t>, userdata: *mut c_void) -> Self {
        Self {
            stmt2,
            async_exec_fn,
            userdata,
            fields_len: None,
            res: None,
        }
    }
}

impl Drop for TaosStmt2 {
    fn drop(&mut self) {
        if let Some(res) = self.res.take() {
            unsafe { taos_free_result(res) };
        }
    }
}

impl TAOS_STMT2_BIND {
    fn to_value(&self) -> Value {
        debug!("to_value, bind: {self:?}");

        if !self.is_null.is_null() && unsafe { self.is_null.read() != 0 } {
            let val = Value::Null(self.ty());
            debug!("to_value, value: {val:?}");
            return val;
        }

        assert!(!self.buffer.is_null());
        assert!(!self.length.is_null());

        let val = match self.ty() {
            Ty::Null => Value::Null(self.ty()),
            Ty::Bool => unsafe { Value::Bool(*(self.buffer as *const _)) },
            Ty::TinyInt => unsafe { Value::TinyInt(*(self.buffer as *const _)) },
            Ty::SmallInt => unsafe { Value::SmallInt(*(self.buffer as *const _)) },
            Ty::Int => unsafe { Value::Int(*(self.buffer as *const _)) },
            Ty::BigInt => unsafe { Value::BigInt(*(self.buffer as *const _)) },
            Ty::UTinyInt => unsafe { Value::UTinyInt(*(self.buffer as *const _)) },
            Ty::USmallInt => unsafe { Value::USmallInt(*(self.buffer as *const _)) },
            Ty::UInt => unsafe { Value::UInt(*(self.buffer as *const _)) },
            Ty::UBigInt => unsafe { Value::UBigInt(*(self.buffer as *const _)) },
            Ty::Float => unsafe { Value::Float(*(self.buffer as *const _)) },
            Ty::Double => unsafe { Value::Double(*(self.buffer as *const _)) },
            Ty::Timestamp => unsafe {
                Value::Timestamp(Timestamp::Milliseconds(*(self.buffer as *const _)))
            },
            Ty::VarChar => unsafe {
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                let val = str::from_utf8_unchecked(slice).to_owned();
                Value::VarChar(val)
            },
            Ty::NChar => unsafe {
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                let val = std::str::from_utf8_unchecked(slice).to_owned();
                Value::NChar(val)
            },
            Ty::Json => unsafe {
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                let val = serde_json::from_slice(slice).unwrap();
                Value::Json(val)
            },
            Ty::VarBinary => unsafe {
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                Value::VarBinary(slice.into())
            },
            Ty::Geometry => unsafe {
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                Value::Geometry(slice.into())
            },
            _ => todo!(),
        };

        debug!("to_value, value: {val:?}");

        val
    }

    fn to_column_view(&self) -> ColumnView {
        debug!("to_column_view, bind: {self:?}");

        let ty = self.ty();
        let num = self.num as usize;
        let lens = unsafe { slice::from_raw_parts(self.length, num) };
        let len = lens.iter().sum::<i32>() as usize;

        let mut is_nulls = None;
        if !self.is_null.is_null() {
            is_nulls = Some(unsafe { slice::from_raw_parts(self.is_null, num) });
        }

        debug!("to_column_view, ty: {ty}, num: {num}, is_nulls: {is_nulls:?}, lens: {lens:?}, total_len: {len}");

        macro_rules! view {
            ($from:expr) => {{
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const _, num) };
                let mut vals = vec![None; num];
                if let Some(is_nulls) = is_nulls {
                    for i in 0..num {
                        if is_nulls[i] == 0 {
                            vals[i] = Some(slice[i]);
                        }
                    }
                } else {
                    for i in 0..num {
                        vals[i] = Some(slice[i]);
                    }
                }
                $from(vals)
            }};
        }

        let view = match ty {
            Ty::Bool => view!(ColumnView::from_bools),
            Ty::TinyInt => view!(ColumnView::from_tiny_ints),
            Ty::SmallInt => view!(ColumnView::from_small_ints),
            Ty::Int => view!(ColumnView::from_ints),
            Ty::BigInt => view!(ColumnView::from_big_ints),
            Ty::UTinyInt => view!(ColumnView::from_unsigned_tiny_ints),
            Ty::USmallInt => view!(ColumnView::from_unsigned_small_ints),
            Ty::UInt => view!(ColumnView::from_unsigned_ints),
            Ty::UBigInt => view!(ColumnView::from_unsigned_big_ints),
            Ty::Float => view!(ColumnView::from_floats),
            Ty::Double => view!(ColumnView::from_doubles),
            Ty::Timestamp => view!(ColumnView::from_millis_timestamp),
            Ty::VarChar => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const _, len) };
                let mut vals = vec![None; num];
                let mut idx = 0;

                if let Some(is_nulls) = is_nulls {
                    for i in 0..num {
                        if is_nulls[i] == 0 {
                            let bytes = &slice[idx..idx + lens[i] as usize];
                            vals[i] = unsafe { Some(str::from_utf8_unchecked(bytes)) };
                            idx += lens[i] as usize;
                        }
                    }
                } else {
                    for i in 0..num {
                        let bytes = &slice[idx..idx + lens[i] as usize];
                        vals[i] = unsafe { Some(str::from_utf8_unchecked(bytes)) };
                        idx += lens[i] as usize;
                    }
                }

                ColumnView::from_varchar::<&str, _, _, _>(vals)
            }
            Ty::NChar => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const _, len) };
                let mut vals = vec![None; num];
                let mut idx = 0;

                if let Some(is_nulls) = is_nulls {
                    for i in 0..num {
                        if is_nulls[i] == 0 {
                            let bytes = &slice[idx..idx + lens[i] as usize];
                            vals[i] = unsafe { Some(str::from_utf8_unchecked(bytes)) };
                            idx += lens[i] as usize;
                        }
                    }
                } else {
                    for i in 0..num {
                        let bytes = &slice[idx..idx + lens[i] as usize];
                        vals[i] = unsafe { Some(str::from_utf8_unchecked(bytes)) };
                        idx += lens[i] as usize;
                    }
                }

                ColumnView::from_nchar::<&str, _, _, _>(vals)
            }
            Ty::VarBinary => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len) };
                let mut vals = vec![None; num];
                let mut idx = 0;

                if let Some(is_nulls) = is_nulls {
                    for i in 0..num {
                        if is_nulls[i] == 0 {
                            let val = &slice[idx..idx + lens[i] as usize];
                            vals[i] = Some(val);
                            idx += lens[i] as usize;
                        }
                    }
                } else {
                    for i in 0..num {
                        let val = &slice[idx..idx + lens[i] as usize];
                        vals[i] = Some(val);
                        idx += lens[i] as usize;
                    }
                }

                ColumnView::from_bytes::<&[u8], _, _, _>(vals)
            }
            Ty::Geometry => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len) };
                let mut vals = vec![None; num];
                let mut idx = 0;

                if let Some(is_nulls) = is_nulls {
                    for i in 0..num {
                        if is_nulls[i] == 0 {
                            let val = &slice[idx..idx + lens[i] as usize];
                            vals[i] = Some(val);
                            idx += lens[i] as usize;
                        }
                    }
                } else {
                    for i in 0..num {
                        let val = &slice[idx..idx + lens[i] as usize];
                        vals[i] = Some(val);
                        idx += lens[i] as usize;
                    }
                }

                ColumnView::from_geobytes::<&[u8], _, _, _>(vals)
            }
            _ => todo!(),
        };

        debug!("to_column_view, view: {view:?}");

        view
    }

    fn ty(&self) -> Ty {
        self.buffer_type.into()
    }
}

impl TAOS_STMT2_BINDV {
    fn to_bind_params(&self, stmt2: &Stmt2) -> TaosResult<Vec<Stmt2BindParam>> {
        if stmt2.is_insert().is_none() {
            return Err(TaosError::new(
                Code::FAILED,
                "taos_stmt2_prepare is not called",
            ));
        }

        let cnt = self.count as usize;
        let mut tag_cnt = 0;
        let mut col_cnt = 0;

        if stmt2.is_insert().unwrap() {
            for field in stmt2.fields().unwrap() {
                match field.bind_type {
                    BindType::Tag => tag_cnt += 1,
                    BindType::Column => col_cnt += 1,
                    BindType::TableName => {}
                }
            }
        } else {
            col_cnt = stmt2.fields_count().unwrap();
        }

        let mut tbnames = vec![None; cnt];
        if !self.tbnames.is_null() {
            let slice = unsafe { slice::from_raw_parts(self.tbnames, cnt) };
            for i in 0..cnt {
                if !slice[i].is_null() {
                    let tbname = unsafe {
                        match CStr::from_ptr(slice[i]).to_str() {
                            Ok(tbname) => tbname.to_owned(),
                            Err(_) => {
                                return Err(TaosError::new(
                                    Code::INVALID_PARA,
                                    "tbname is invalid utf-8",
                                ));
                            }
                        }
                    };
                    tbnames[i] = Some(tbname);
                }
            }
        }

        let mut tags = vec![None; cnt];
        if !self.tags.is_null() && tag_cnt > 0 {
            let slice = unsafe { slice::from_raw_parts(self.tags, cnt) };
            for i in 0..cnt {
                let mut vals = Vec::with_capacity(tag_cnt);
                let binds = unsafe { slice::from_raw_parts(slice[i], tag_cnt) };
                for bind in binds {
                    vals.push(bind.to_value());
                }
                tags[i] = Some(vals);
            }
        }

        let mut cols = vec![None; cnt];
        if !self.bind_cols.is_null() && col_cnt > 0 {
            let slice = unsafe { slice::from_raw_parts(self.bind_cols, cnt) };
            for i in 0..cnt {
                let mut views = Vec::with_capacity(col_cnt);
                let binds = unsafe { slice::from_raw_parts(slice[i], col_cnt) };
                for bind in binds {
                    views.push(bind.to_column_view());
                }
                cols[i] = Some(views);
            }
        }

        let mut params = Vec::with_capacity(cnt);
        for i in 0..cnt {
            let param = Stmt2BindParam::new(tbnames[i].take(), tags[i].take(), cols[i].take());
            params.push(param);
        }

        Ok(params)
    }
}

impl From<&Stmt2Field> for TAOS_FIELD_ALL {
    fn from(field: &Stmt2Field) -> Self {
        let field_name = field.name.as_str();
        let mut name = [0 as c_char; 65];
        unsafe {
            ptr::copy_nonoverlapping(
                field_name.as_ptr(),
                name.as_mut_ptr() as _,
                field_name.len(),
            );
        };

        Self {
            name,
            r#type: field.field_type,
            precision: field.precision,
            scale: field.scale,
            bytes: field.bytes,
            field_type: field.bind_type as _,
        }
    }
}

pub unsafe fn taos_stmt2_init(taos: *mut TAOS, option: *mut TAOS_STMT2_OPTION) -> *mut TAOS_STMT2 {
    debug!("taos_stmt2_init start, taos: {taos:?}, option: {option:?}");
    let taos_stmt2: TaosMaybeError<TaosStmt2> = match stmt2_init(taos, option) {
        Ok(taos_stmt2) => taos_stmt2.into(),
        Err(err) => {
            error!("taos_stmt2_init failed, err: {err:?}");
            set_err_and_get_code(err);
            return ptr::null_mut();
        }
    };
    debug!("taos_stmt2_init, taos_stmt2: {taos_stmt2:?}");
    let res = Box::into_raw(Box::new(taos_stmt2)) as _;
    debug!("taos_stmt2_init succ, res: {res:?}");
    res
}

unsafe fn stmt2_init(taos: *mut TAOS, option: *mut TAOS_STMT2_OPTION) -> TaosResult<TaosStmt2> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;

    let mut stmt2 = Stmt2::new(taos.client());

    let (req_id, single_stb_insert, single_table_bind_once, async_exec_fn, userdata) =
        match option.as_ref() {
            Some(option) => {
                let async_exec_fn = option.asyncExecFn as *const ();
                let async_exec_fn = if !async_exec_fn.is_null() {
                    Some(option.asyncExecFn)
                } else {
                    None
                };

                (
                    option.reqid as _,
                    option.singleStbInsert,
                    option.singleTableBindOnce,
                    async_exec_fn,
                    option.userdata,
                )
            }
            None => (generate_req_id(), false, false, None, ptr::null_mut()),
        };

    debug!("stmt2_init, req_id: {req_id}, single_stb_insert: {single_stb_insert}, single_table_bind_once: {single_table_bind_once}, async_exec_fn: {async_exec_fn:?}, userdata: {userdata:?}");

    block_in_place_or_global(stmt2.init_with_options(
        req_id,
        single_stb_insert,
        single_table_bind_once,
    ))?;

    Ok(TaosStmt2::new(stmt2, async_exec_fn, userdata))
}

pub unsafe fn taos_stmt2_prepare(
    stmt: *mut TAOS_STMT2,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    debug!("taos_stmt2_prepare start, stmt: {stmt:?}, sql: {sql:?}, length: {length}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => &mut taos_stmt2.stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if sql.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "sql is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let sql = if length > 0 {
        // TODO: check utf-8
        str::from_utf8_unchecked(slice::from_raw_parts(sql as _, length as _))
    } else {
        match CStr::from_ptr(sql).to_str() {
            Ok(sql) => sql,
            Err(_) => {
                maybe_err.with_err(Some(TaosError::new(
                    Code::INVALID_PARA,
                    "sql is invalid utf-8",
                )));
                return format_errno(Code::INVALID_PARA.into());
            }
        }
    };

    debug!("taos_stmt2_prepare, sql: {sql}");

    match stmt2.prepare(sql) {
        Ok(_) => {
            debug!("taos_stmt2_prepare succ");
            maybe_err.clear_err();
            clear_err_and_ret_succ()
        }
        Err(err) => {
            error!("taos_stmt2_prepare failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            format_errno(err.code().into())
        }
    }
}

pub unsafe fn taos_stmt2_bind_param(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
) -> c_int {
    debug!("taos_stmt2_bind_param start, stmt: {stmt:?}, bindv: {bindv:?}, col_idx: {col_idx}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => &mut taos_stmt2.stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let bindv = match bindv.as_ref() {
        Some(bindv) => bindv,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "bindv is null")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let params = match bindv.to_bind_params(stmt2) {
        Ok(params) => params,
        Err(err) => {
            error!("taos_stmt2_bind_param failed, err: {err:?}");
            let code = err.code();
            maybe_err.with_err(Some(err));
            return format_errno(code.into());
        }
    };

    debug!("taos_stmt2_bind_param, params: {params:?}");

    match stmt2.bind(&params) {
        Ok(_) => {
            debug!("taos_stmt2_bind_param succ");
            maybe_err.clear_err();
            0
        }
        Err(err) => {
            error!("taos_stmt2_bind_param failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            format_errno(err.code().into())
        }
    }
}

pub unsafe fn taos_stmt2_bind_param_a(
    _stmt: *mut TAOS_STMT2,
    _bindv: *mut TAOS_STMT2_BINDV,
    _col_idx: i32,
    _fp: __taos_async_fn_t,
    _param: *mut c_void,
) -> c_int {
    0
}

pub unsafe fn taos_stmt2_exec(stmt: *mut TAOS_STMT2, affected_rows: *mut c_int) -> c_int {
    debug!("taos_stmt2_exec start, stmt: {stmt:?}, affected_rows: {affected_rows:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => taos_stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if taos_stmt2.async_exec_fn.is_none() {
        if affected_rows.is_null() {
            maybe_err.with_err(Some(TaosError::new(
                Code::INVALID_PARA,
                "affected_rows is null",
            )));
            return format_errno(Code::INVALID_PARA.into());
        }

        match taos_stmt2.stmt2.exec() {
            Ok(rows) => {
                *affected_rows = rows as _;
                debug!("taos_stmt2_exec succ, affected_rows: {rows}");
                maybe_err.clear_err();
                return clear_err_and_ret_succ();
            }
            Err(err) => {
                error!("taos_stmt2_exec failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                return format_errno(err.code().into());
            }
        }
    }

    global_tokio_runtime().spawn(
        async move {
            use taos_query::stmt2::Stmt2AsyncBindable;

            let taos_stmt2 = maybe_err.deref_mut().unwrap();
            let stmt2 = &mut taos_stmt2.stmt2;
            let async_exec_fn = taos_stmt2.async_exec_fn.unwrap();
            let userdata = SafePtr(taos_stmt2.userdata);

            // TODO: add lock
            if let Err(err) = Stmt2AsyncBindable::exec(stmt2).await {
                error!("async taos_stmt2_exec exec failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                let code = format_errno(err.code().into());
                async_exec_fn(userdata.0, ptr::null_mut(), code);
                return;
            }

            match Stmt2AsyncBindable::result_set(stmt2).await {
                Ok(rs) => {
                    debug!("async taos_stmt2_exec callback, result_set: {rs:?}");
                    let rs: TaosMaybeError<ResultSet> =
                        ResultSet::Query(QueryResultSet::new(rs)).into();
                    let res = Box::into_raw(Box::new(rs));
                    maybe_err.clear_err();
                    taos_stmt2.res = Some(res as _);
                    async_exec_fn(userdata.0, res as _, 0);
                }
                Err(err) => {
                    error!("async taos_stmt2_exec result failed, err: {err:?}");
                    maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                    let code = format_errno(err.code().into());
                    async_exec_fn(userdata.0, ptr::null_mut(), code as _);
                }
            }
        }
        .in_current_span(),
    );

    debug!("async taos_stmt2_exec succ");
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt2_close(stmt: *mut TAOS_STMT2) -> c_int {
    debug!("taos_stmt2_close, stmt: {stmt:?}");
    if stmt.is_null() {
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
    }
    let _ = Box::from_raw(stmt as *mut TaosMaybeError<TaosStmt2>);
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt2_is_insert(stmt: *mut TAOS_STMT2, insert: *mut c_int) -> c_int {
    debug!("taos_stmt2_is_insert start, stmt: {stmt:?}, insert: {insert:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => &mut taos_stmt2.stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if insert.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "insert is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    match stmt2.is_insert() {
        Some(is_insert) => {
            *insert = is_insert as _;
            debug!("taos_stmt2_is_insert succ, is_insert: {is_insert}");
            maybe_err.clear_err();
            clear_err_and_ret_succ()
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt2_prepare is not called",
            )));
            format_errno(Code::FAILED.into())
        }
    }
}

pub unsafe fn taos_stmt2_get_fields(
    stmt: *mut TAOS_STMT2,
    count: *mut c_int,
    fields: *mut *mut TAOS_FIELD_ALL,
) -> c_int {
    debug!("taos_stmt2_get_fields start, stmt: {stmt:?}, count: {count:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => taos_stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if count.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "count is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt2.stmt2;
    if stmt2.is_insert().is_none() {
        maybe_err.with_err(Some(TaosError::new(
            Code::FAILED,
            "taos_stmt2_prepare is not called",
        )));
        return format_errno(Code::FAILED.into());
    }

    if stmt2.is_insert().unwrap() {
        let stmt2_fields = stmt2.fields().unwrap();
        let field_all: Vec<TAOS_FIELD_ALL> = stmt2_fields.iter().map(|f| f.into()).collect();
        if !field_all.is_empty() && !fields.is_null() {
            *fields = Box::into_raw(field_all.into_boxed_slice()) as _;
            taos_stmt2.fields_len = Some(stmt2_fields.len());
        }
        *count = stmt2_fields.len() as _;
    } else {
        *count = stmt2.fields_count().unwrap() as _;
    }

    debug!("taos_stmt2_get_fields succ, fields: {fields:?}, count: {count:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt2_free_fields(stmt: *mut TAOS_STMT2, fields: *mut TAOS_FIELD_ALL) {
    debug!("taos_stmt2_free_fields start, stmt: {stmt:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return;
        }
    };

    let taos_stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => taos_stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return;
        }
    };

    if taos_stmt2.fields_len.is_none() {
        maybe_err.with_err(Some(TaosError::new(
            Code::FAILED,
            "taos_stmt2_get_fields is not called",
        )));
        return;
    }

    let len = taos_stmt2.fields_len.unwrap();
    let fields = Vec::from_raw_parts(fields, len, len);

    debug!("taos_stmt2_free_fields succ, fields: {fields:?}");

    maybe_err.clear_err();
    clear_error_info();
}

pub unsafe fn taos_stmt2_result(stmt: *mut TAOS_STMT2) -> *mut TAOS_RES {
    debug!("taos_stmt2_result start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return ptr::null_mut();
        }
    };

    let taos_stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => taos_stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return ptr::null_mut();
        }
    };

    let stmt2 = &mut taos_stmt2.stmt2;
    match stmt2.result_set() {
        Ok(rs) => {
            let rs: TaosMaybeError<ResultSet> = ResultSet::Query(QueryResultSet::new(rs)).into();
            debug!("taos_stmt2_result succ, result_set: {rs:?}");
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            let res = Box::into_raw(Box::new(rs)) as _;
            taos_stmt2.res = Some(res);
            res
        }
        Err(err) => {
            error!("taos_stmt2_result failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            ptr::null_mut()
        }
    }
}

pub unsafe fn taos_stmt2_error(stmt: *mut TAOS_STMT2) -> *mut c_char {
    taos_errstr(stmt) as _
}
