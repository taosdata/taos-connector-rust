use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::{ptr, slice, str};

use taos_error::Code;
use taos_query::common::{ColumnView, Timestamp, Ty, Value};
use taos_query::stmt2::{Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_query::{block_in_place_or_global, global_tokio_runtime};
use taos_ws::query::{BindType, Stmt2Field};
use taos_ws::{Stmt2, Taos};
use tracing::{error, trace, Instrument};

use crate::ws::error::*;
use crate::ws::query::QueryResultSet;
use crate::ws::{ResultSet, SafePtr, TaosResult, __taos_async_fn_t, TAOS, TAOS_RES};

#[allow(non_camel_case_types)]
pub type TAOS_STMT2 = c_void;

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub struct TAOS_STMT2_OPTION {
    pub reqid: i64,
    pub singleStbInsert: bool,
    pub singleTableBindOnce: bool,
    pub asyncExecFn: __taos_async_fn_t,
    pub userdata: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: c_int,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BINDV {
    pub count: c_int,
    pub tbnames: *mut *mut c_char,
    pub tags: *mut *mut TAOS_STMT2_BIND,
    pub bind_cols: *mut *mut TAOS_STMT2_BIND,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD_ALL {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
    pub field_type: u8,
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_init(
    taos: *mut TAOS,
    option: *mut TAOS_STMT2_OPTION,
) -> *mut TAOS_STMT2 {
    trace!("taos_stmt2_init start, taos: {taos:?}, option: {option:?}");
    let taos_stmt2: TaosMaybeError<TaosStmt2> = match stmt2_init(taos, option) {
        Ok(taos_stmt2) => taos_stmt2.into(),
        Err(err) => {
            error!("taos_stmt2_init failed, err: {err:?}");
            set_err_and_get_code(err);
            return ptr::null_mut();
        }
    };
    trace!("taos_stmt2_init, taos_stmt2: {taos_stmt2:?}");
    let res = Box::into_raw(Box::new(taos_stmt2)) as _;
    trace!("taos_stmt2_init succ, res: {res:?}");
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

    trace!("stmt2_init, req_id: {req_id}, single_stb_insert: {single_stb_insert}, single_table_bind_once: {single_table_bind_once}, async_exec_fn: {async_exec_fn:?}, userdata: {userdata:?}");

    block_in_place_or_global(stmt2.init_with_options(
        req_id,
        single_stb_insert,
        single_table_bind_once,
    ))?;

    Ok(TaosStmt2::new(stmt2, async_exec_fn, userdata))
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_prepare(
    stmt: *mut TAOS_STMT2,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    trace!("taos_stmt2_prepare start, stmt: {stmt:?}, sql: {sql:?}, length: {length}");

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

    trace!("taos_stmt2_prepare, sql: {sql}");

    match stmt2.prepare(sql) {
        Ok(_) => {
            trace!("taos_stmt2_prepare succ");
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

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_bind_param(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
) -> c_int {
    trace!("taos_stmt2_bind_param start, stmt: {stmt:?}, bindv: {bindv:?}, col_idx: {col_idx}");

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

    trace!("taos_stmt2_bind_param, params: {params:?}");

    match stmt2.bind(&params) {
        Ok(_) => {
            trace!("taos_stmt2_bind_param succ");
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

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_bind_param_a(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) -> c_int {
    0
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_exec(
    stmt: *mut TAOS_STMT2,
    affected_rows: *mut c_int,
) -> c_int {
    trace!("taos_stmt2_exec start, stmt: {stmt:?}, affected_rows: {affected_rows:?}");

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
                trace!("taos_stmt2_exec succ, affected_rows: {rows}");
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
                    trace!("async taos_stmt2_exec callback, result_set: {rs:?}");
                    let rs: TaosMaybeError<ResultSet> =
                        ResultSet::Query(QueryResultSet::new(rs)).into();
                    let res = Box::into_raw(Box::new(rs));
                    maybe_err.clear_err();
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

    trace!("async taos_stmt2_exec succ");
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_close(stmt: *mut TAOS_STMT2) -> c_int {
    trace!("taos_stmt2_close, stmt: {stmt:?}");
    if stmt.is_null() {
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
    }
    let _ = Box::from_raw(stmt as *mut TaosMaybeError<TaosStmt2>);
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_is_insert(stmt: *mut TAOS_STMT2, insert: *mut c_int) -> c_int {
    trace!("taos_stmt2_is_insert start, stmt: {stmt:?}, insert: {insert:?}");

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
            trace!("taos_stmt2_is_insert succ, is_insert: {is_insert}");
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

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_get_fields(
    stmt: *mut TAOS_STMT2,
    count: *mut c_int,
    fields: *mut *mut TAOS_FIELD_ALL,
) -> c_int {
    trace!("taos_stmt2_get_fields start, stmt: {stmt:?}, count: {count:?}, fields: {fields:?}");

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

    trace!("taos_stmt2_get_fields succ, fields: {fields:?}, count: {count:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_free_fields(
    stmt: *mut TAOS_STMT2,
    fields: *mut TAOS_FIELD_ALL,
) {
    trace!("taos_stmt2_free_fields start, stmt: {stmt:?}, fields: {fields:?}");

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

    trace!("taos_stmt2_free_fields succ, fields: {fields:?}");

    maybe_err.clear_err();
    clear_error_info();
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_result(stmt: *mut TAOS_STMT2) -> *mut TAOS_RES {
    trace!("taos_stmt2_result start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return ptr::null_mut();
        }
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => &mut taos_stmt2.stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return ptr::null_mut();
        }
    };

    match stmt2.result_set() {
        Ok(rs) => {
            let rs: TaosMaybeError<ResultSet> = ResultSet::Query(QueryResultSet::new(rs)).into();
            trace!("taos_stmt2_result succ, result_set: {rs:?}");
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            Box::into_raw(Box::new(rs)) as _
        }
        Err(err) => {
            error!("taos_stmt2_result failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_error(stmt: *mut TAOS_STMT2) -> *mut c_char {
    taos_errstr(stmt) as _
}

#[derive(Debug)]
struct TaosStmt2 {
    stmt2: Stmt2,
    async_exec_fn: Option<__taos_async_fn_t>,
    userdata: *mut c_void,
    fields_len: Option<usize>,
}

impl TaosStmt2 {
    fn new(stmt2: Stmt2, async_exec_fn: Option<__taos_async_fn_t>, userdata: *mut c_void) -> Self {
        Self {
            stmt2,
            async_exec_fn,
            userdata,
            fields_len: None,
        }
    }
}

impl TAOS_STMT2_BIND {
    fn to_value(&self) -> Value {
        trace!("to_value, bind: {self:?}");

        if !self.is_null.is_null() && unsafe { self.is_null.read() != 0 } {
            let val = Value::Null(self.ty());
            trace!("to_value, value: {val:?}");
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

        trace!("to_value, value: {val:?}");

        val
    }

    fn to_column_view(&self) -> ColumnView {
        trace!("to_column_view, bind: {self:?}");

        let ty = self.ty();
        let num = self.num as usize;
        let lens = unsafe { slice::from_raw_parts(self.length, num) };
        let len = lens.iter().sum::<i32>() as usize;

        let mut is_nulls = None;
        if !self.is_null.is_null() {
            is_nulls = Some(unsafe { slice::from_raw_parts(self.is_null, num) });
        }

        trace!("to_column_view, ty: {ty}, num: {num}, is_nulls: {is_nulls:?}, lens: {lens:?}, total_len: {len}");

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

        trace!("to_column_view, view: {view:?}");

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

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ws::query::*;
    use crate::ws::{taos_close, test_connect, test_exec, test_exec_many};

    macro_rules! new_bind {
        ($ty:expr, $buffer:ident, $length:ident, $is_null:ident) => {
            TAOS_STMT2_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                length: $length.as_mut_ptr(),
                is_null: $is_null.as_mut_ptr(),
                num: $is_null.len() as _,
            }
        };
    }

    macro_rules! new_bind_without_is_null {
        ($ty:expr, $buffer:ident, $length:ident) => {
            TAOS_STMT2_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                length: $length.as_mut_ptr(),
                is_null: ptr::null_mut(),
                num: $length.len() as _,
            }
        };
    }

    #[test]
    fn test_stmt2() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739274502",
                    "create database test_1739274502",
                    "use test_1739274502",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let len = sql.to_bytes().len();
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), len as _);
            assert_eq!(code, 0);

            let mut buffer = vec![1739763276172i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![2];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 1);

            let mut insert = 0;
            let code = taos_stmt2_is_insert(stmt2, &mut insert);
            assert_eq!(code, 0);
            assert_eq!(insert, 1);

            let mut count = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt2_get_fields(stmt2, &mut count, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(count, 2);
            assert!(!fields.is_null());

            taos_stmt2_free_fields(stmt2, fields);

            let error = taos_stmt2_error(stmt2);
            assert_eq!(CStr::from_ptr(error), c"");

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739274502");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_bind_param() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739502440",
                    "create database test_1739502440",
                    "use test_1739502440",
                    "create table s0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                    c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                    c9 bigint unsigned, c10 float, c11 double, c12 varchar(10), c13 nchar(10), \
                    c14 varbinary(10), c15 geometry(50)) \
                    tags (t1 timestamp, t2 bool, t3 tinyint, t4 smallint, t5 int, t6 bigint, \
                    t7 tinyint unsigned, t8 smallint unsigned, t9 int unsigned, \
                    t10 bigint unsigned, t11 float, t12 double, t13 varchar(10), t14 nchar(10), \
                    t15 varbinary(10), t16 geometry(50), t17 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql =
                c"insert into ? using s0 tags(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname1 = c"d0";
            let tbname2 = c"d1";
            let mut tbnames = vec![tbname1.as_ptr() as _, tbname2.as_ptr() as _];

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t1 = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![1i8];
            let mut length = vec![1];
            let mut is_null = vec![0];
            let t2 = new_bind!(Ty::Bool, buffer, length, is_null);

            let mut buffer = vec![10i8];
            let mut length = vec![1];
            let mut is_null = vec![0];
            let t3 = new_bind!(Ty::TinyInt, buffer, length, is_null);

            let mut buffer = vec![23i16];
            let mut length = vec![2];
            let mut is_null = vec![0];
            let t4 = new_bind!(Ty::SmallInt, buffer, length, is_null);

            let mut buffer = vec![479i32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t5 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut buffer = vec![1999i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t6 = new_bind!(Ty::BigInt, buffer, length, is_null);

            let mut buffer = vec![27u8];
            let mut length = vec![1];
            let mut is_null = vec![0];
            let t7 = new_bind!(Ty::UTinyInt, buffer, length, is_null);

            let mut buffer = vec![89u16];
            let mut length = vec![2];
            let mut is_null = vec![0];
            let t8 = new_bind!(Ty::USmallInt, buffer, length, is_null);

            let mut buffer = vec![234578u32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t9 = new_bind!(Ty::UInt, buffer, length, is_null);

            let mut buffer = vec![234578u64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t10 = new_bind!(Ty::UBigInt, buffer, length, is_null);

            let mut buffer = vec![1.23f32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t11 = new_bind!(Ty::Float, buffer, length, is_null);

            let mut buffer = vec![2345345.99f64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t12 = new_bind!(Ty::Double, buffer, length, is_null);

            let mut buffer = vec![104u8, 101, 108, 108, 111];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t13 = new_bind!(Ty::VarChar, buffer, length, is_null);

            let mut buffer = vec![104u8, 101, 108, 108, 111];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t14 = new_bind!(Ty::NChar, buffer, length, is_null);

            let mut buffer = vec![118u8, 97, 114, 98, 105, 110, 97, 114, 121];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t15 = new_bind!(Ty::VarBinary, buffer, length, is_null);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
            ];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t16 = new_bind!(Ty::Geometry, buffer, length, is_null);

            let mut is_null = vec![1];
            let t17 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: ptr::null_mut(),
                length: ptr::null_mut(),
                is_null: is_null.as_mut_ptr(),
                num: is_null.len() as _,
            };

            let mut tag1 = vec![
                t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17,
            ];
            let mut tag2 = tag1.clone();
            let mut tags = vec![tag1.as_mut_ptr(), tag2.as_mut_ptr()];

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![1i8, 1, 0, 0];
            let mut length = vec![1, 1, 1, 1];
            let mut is_null = vec![0, 0, 0, 1];
            let c1 = new_bind!(Ty::Bool, buffer, length, is_null);

            let mut buffer = vec![23i8, 0, -23, 0];
            let mut length = vec![1, 1, 1, 1];
            let mut is_null = vec![0, 0, 0, 1];
            let c2 = new_bind!(Ty::TinyInt, buffer, length, is_null);

            let mut buffer = vec![34i16, 0, -34, 0];
            let mut length = vec![2, 2, 2, 2];
            let mut is_null = vec![0, 0, 0, 1];
            let c3 = new_bind!(Ty::SmallInt, buffer, length, is_null);

            let mut buffer = vec![45i32, 46, -45, 0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c4 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut buffer = vec![56i64, 57, -56, 0];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 1];
            let c5 = new_bind!(Ty::BigInt, buffer, length, is_null);

            let mut buffer = vec![67u8, 68, 67, 0];
            let mut length = vec![1, 1, 1, 1];
            let mut is_null = vec![0, 0, 0, 1];
            let c6 = new_bind!(Ty::UTinyInt, buffer, length, is_null);

            let mut buffer = vec![78u16, 79, 78, 0];
            let mut length = vec![2, 2, 2, 2];
            let mut is_null = vec![0, 0, 0, 1];
            let c7 = new_bind!(Ty::USmallInt, buffer, length, is_null);

            let mut buffer = vec![89u32, 90, 89, 0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c8 = new_bind!(Ty::UInt, buffer, length, is_null);

            let mut buffer = vec![100u64, 101, 100, 0];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 1];
            let c9 = new_bind!(Ty::UBigInt, buffer, length, is_null);

            let mut buffer = vec![1.23f32, 1.24, -1.23, 0.0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c10 = new_bind!(Ty::Float, buffer, length, is_null);

            let mut buffer = vec![2345.67f64, 2345.68, -2345.67, 0.0];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 1];
            let c11 = new_bind!(Ty::Double, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c12 = new_bind!(Ty::VarChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c13 = new_bind!(Ty::NChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c14 = new_bind!(Ty::VarBinary, buffer, length, is_null);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
            ];
            let mut length = vec![21, 0, 21, 0];
            let mut is_null = vec![0, 1, 0, 1];
            let c15 = new_bind!(Ty::Geometry, buffer, length, is_null);

            let mut col1 = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let ts = new_bind_without_is_null!(Ty::Timestamp, buffer, length);

            let mut buffer = vec![1i8, 1, 0, 0];
            let mut length = vec![1, 1, 1, 1];
            let c1 = new_bind_without_is_null!(Ty::Bool, buffer, length);

            let mut buffer = vec![23i8, 0, -23, 0];
            let mut length = vec![1, 1, 1, 1];
            let c2 = new_bind_without_is_null!(Ty::TinyInt, buffer, length);

            let mut buffer = vec![34i16, 0, -34, 0];
            let mut length = vec![2, 2, 2, 2];
            let c3 = new_bind_without_is_null!(Ty::SmallInt, buffer, length);

            let mut buffer = vec![45i32, 46, -45, 0];
            let mut length = vec![4, 4, 4, 4];
            let c4 = new_bind_without_is_null!(Ty::Int, buffer, length);

            let mut buffer = vec![56i64, 57, -56, 0];
            let mut length = vec![8, 8, 8, 8];
            let c5 = new_bind_without_is_null!(Ty::BigInt, buffer, length);

            let mut buffer = vec![67u8, 68, 67, 0];
            let mut length = vec![1, 1, 1, 1];
            let c6 = new_bind_without_is_null!(Ty::UTinyInt, buffer, length);

            let mut buffer = vec![78u16, 79, 78, 0];
            let mut length = vec![2, 2, 2, 2];
            let c7 = new_bind_without_is_null!(Ty::USmallInt, buffer, length);

            let mut buffer = vec![89u32, 90, 89, 0];
            let mut length = vec![4, 4, 4, 4];
            let c8 = new_bind_without_is_null!(Ty::UInt, buffer, length);

            let mut buffer = vec![100u64, 101, 100, 0];
            let mut length = vec![8, 8, 8, 8];
            let c9 = new_bind_without_is_null!(Ty::UBigInt, buffer, length);

            let mut buffer = vec![1.23f32, 1.24, -1.23, 0.0];
            let mut length = vec![4, 4, 4, 4];
            let c10 = new_bind_without_is_null!(Ty::Float, buffer, length);

            let mut buffer = vec![2345.67f64, 2345.68, -2345.67, 0.0];
            let mut length = vec![8, 8, 8, 8];
            let c11 = new_bind_without_is_null!(Ty::Double, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c12 = new_bind_without_is_null!(Ty::VarChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c13 = new_bind_without_is_null!(Ty::NChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c14 = new_bind_without_is_null!(Ty::VarBinary, buffer, length);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
                0, 0, 0, 0, 0, 0, 240, 63,
            ];
            let mut length = vec![21, 21, 21, 21];
            let c15 = new_bind_without_is_null!(Ty::Geometry, buffer, length);

            let mut col2 = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let mut cols = vec![col1.as_mut_ptr(), col2.as_mut_ptr()];

            let mut bindv = TAOS_STMT2_BINDV {
                count: tbnames.len() as _,
                tbnames: tbnames.as_mut_ptr(),
                tags: tags.as_mut_ptr(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 8);

            let sql = c"select * from s0 where c4 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c4 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c4];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 0);

            let res = taos_stmt2_result(stmt2);
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 33);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            taos_free_result(res);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739502440");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739966021",
                    "create database test_1739966021",
                    "use test_1739966021",
                    "create table s0 (ts timestamp, c1 int) tags (t1 json)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname = c"d0";
            let mut tbnames = vec![tbname.as_ptr() as _];

            let mut buffer = vec![
                123u8, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125,
            ];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t1 = new_bind!(Ty::Json, buffer, length, is_null);

            let mut tag = vec![t1];
            let mut tags = vec![tag.as_mut_ptr()];

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];

            let mut bindv = TAOS_STMT2_BINDV {
                count: tbnames.len() as _,
                tbnames: tbnames.as_mut_ptr(),
                tags: tags.as_mut_ptr(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 1);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739966021");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_exec_async() {
        unsafe {
            extern "C" fn fp(userdata: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert_eq!(code, 0);
                    assert!(!res.is_null());

                    let userdata = CStr::from_ptr(userdata as _);
                    assert_eq!(userdata, c"hello, world");

                    let row = taos_fetch_row(res);
                    assert!(!row.is_null());

                    let fields = taos_fetch_fields(res);
                    assert!(!fields.is_null());

                    let num_fields = taos_num_fields(res);
                    assert_eq!(num_fields, 2);

                    let mut str = vec![0 as c_char; 1024];
                    let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                    assert!(len > 0);
                    println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

                    taos_free_result(res);
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739864837",
                    "create database test_1739864837",
                    "use test_1739864837",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 2)",
                ],
            );

            let userdata = c"hello, world";
            let mut option = TAOS_STMT2_OPTION {
                reqid: 1001,
                singleStbInsert: true,
                singleTableBindOnce: false,
                asyncExecFn: fp,
                userdata: userdata.as_ptr() as _,
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![1];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let code = taos_stmt2_exec(stmt2, ptr::null_mut());
            assert_eq!(code, 0);

            std::thread::sleep(std::time::Duration::from_secs(1));

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739864837");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_result() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739876374",
                    "create database test_1739876374",
                    "use test_1739876374",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 1)",
                    "insert into t0 values(1739762261438, 99)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 0);

            let res = taos_stmt2_result(stmt2);
            assert!(!res.is_null());

            let affected_rows = taos_affected_rows(res);
            assert_eq!(affected_rows, 0);

            let precision = taos_result_precision(res);
            assert_eq!(precision, 0);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 2);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            taos_stop_query(res);

            taos_free_result(res);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739876374");
            taos_close(taos);
        }
    }
}
