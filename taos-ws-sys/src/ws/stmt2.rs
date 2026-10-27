// Copyright (c) 2025 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::sync::{Arc, Condvar, Mutex};
use std::{ptr, slice, str};

use byteorder::{ByteOrder, LittleEndian};
use taos_error::Code;
use taos_query::common::Ty;
use taos_query::stmt2::Stmt2Bindable;
use taos_query::util::generate_req_id;
use taos_query::{block_in_place_or_global, global_tokio_runtime};
use taos_ws::query::{BindType, Stmt2Field};
use taos_ws::{Stmt2, Taos};
use tracing::{debug, error, trace, Instrument};

use crate::ws::error::*;
use crate::ws::query::QueryResultSet;
use crate::ws::{
    ResultSet, ResultSetOperations, SafePtr, TaosResult, __taos_async_fn_t, TAOS, TAOS_RES,
};

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
#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_COLUMN_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub length: *mut i32,
    pub is_null: *mut c_char,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_COLUMN_BINDV {
    pub num_columns: c_int,
    pub num_rows: c_int,
    pub num_tables: c_int,
    pub columns: *mut TAOS_STMT2_COLUMN_BIND,
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

    let stmt2 = Stmt2::new(taos.client_cloned());

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

    debug!("stmt2_init, req_id: 0x{req_id:x}, single_stb_insert: {single_stb_insert}, single_table_bind_once: {single_table_bind_once}, async_exec_fn: {async_exec_fn:?}, userdata: {userdata:?}");

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
    debug!("taos_stmt2_prepare start, stmt: {stmt:?}, sql: {sql:?}, length: {length}");

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
    let lifecycle = taos_stmt2.lifecycle.clone();
    lifecycle.wait_async_done_and_clear_result();
    let generation = lifecycle.begin_operation();
    let stmt2 = &mut taos_stmt2.stmt2;

    if sql.is_null() {
        lifecycle.publish_error(
            generation,
            maybe_err,
            Some(TaosError::new(Code::INVALID_PARA, "sql is null")),
        );
        return format_errno(Code::INVALID_PARA.into());
    }

    let sql = if length > 0 {
        // TODO: check utf-8
        str::from_utf8_unchecked(slice::from_raw_parts(sql as _, length as _))
    } else {
        match CStr::from_ptr(sql).to_str() {
            Ok(sql) => sql,
            Err(_) => {
                lifecycle.publish_error(
                    generation,
                    maybe_err,
                    Some(TaosError::new(Code::INVALID_PARA, "sql is invalid utf-8")),
                );
                return format_errno(Code::INVALID_PARA.into());
            }
        }
    };

    debug!("taos_stmt2_prepare, sql: {sql}");

    match stmt2.prepare(sql) {
        Ok(_) => {
            if stmt2.is_insert() == Some(true)
                && stmt2.fields().is_none()
                && stmt2.fields_count().unwrap_or(0) > 0
            {
                let msg = "stmt2 does not support insert statement with a table name placeholder, \
                    e.g. `insert into ? ...` or `insert into db.? ...`";
                error!("taos_stmt2_prepare failed, err: {msg}");
                stmt2.mark_unprepared();
                lifecycle.publish_error(
                    generation,
                    maybe_err,
                    Some(TaosError::new(Code::INVALID_PARA, msg)),
                );
                return format_errno(Code::INVALID_PARA.into());
            }
            debug!("taos_stmt2_prepare succ");
            lifecycle.publish_error(generation, maybe_err, None);
            clear_err_and_ret_succ()
        }
        Err(err) => {
            error!("taos_stmt2_prepare failed, err: {err:?}");
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(err.code(), &err.to_string())),
            );
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
    debug!("taos_stmt2_bind_param start, stmt: {stmt:?}, bindv: {bindv:?}, col_idx: {col_idx}");

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
    let lifecycle = taos_stmt2.lifecycle.clone();
    lifecycle.wait_async_done_and_clear_result();
    let generation = lifecycle.begin_operation();
    let stmt2 = &mut taos_stmt2.stmt2;

    let bindv = match bindv.as_ref() {
        Some(bindv) => bindv,
        None => {
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(Code::INVALID_PARA, "bindv is null")),
            );
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let is_insert = match stmt2.is_insert() {
        Some(is_insert) => is_insert,
        None => {
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(
                    Code::FAILED,
                    "taos_stmt2_prepare is not called",
                )),
            );
            return format_errno(Code::FAILED.into());
        }
    };

    let mut tag_cnt = 0;
    let mut col_cnt = 0;
    if is_insert {
        if let Some(fields) = stmt2.fields() {
            for field in fields {
                match field.bind_type {
                    BindType::Tag => tag_cnt += 1,
                    BindType::Column => col_cnt += 1,
                    BindType::TableName | BindType::Unknown(_) => {}
                }
            }
        }
    } else {
        col_cnt = stmt2.fields_count().unwrap_or(0);
    }

    let req_id = generate_req_id();
    let bytes = match bindv.to_bytes(req_id, stmt2.stmt_id(), tag_cnt, col_cnt) {
        Ok(bytes) => bytes,
        Err(err) => {
            error!("taos_stmt2_bind_param failed, err: {err:?}");
            let code = err.code();
            lifecycle.publish_error(generation, maybe_err, Some(err));
            return format_errno(code.into());
        }
    };

    trace!("taos_stmt2_bind_param, bind bytes: {bytes:?}");

    match block_in_place_or_global(stmt2.bind_bytes(req_id, bytes)) {
        Ok(_) => {
            debug!("taos_stmt2_bind_param succ");
            lifecycle.publish_error(generation, maybe_err, None);
            0
        }
        Err(err) => {
            error!("taos_stmt2_bind_param failed, err: {err:?}");
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(err.code(), &err.to_string())),
            );
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
pub unsafe extern "C" fn taos_stmt2_bind_param_column(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_COLUMN_BINDV,
) -> c_int {
    stmt2_column_bind_not_implemented(stmt, "taos_stmt2_bind_param_column")
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_bind_param_column_a(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_COLUMN_BINDV,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) -> c_int {
    let code = stmt2_column_bind_not_implemented(stmt, "taos_stmt2_bind_param_column_a");
    fp(param, ptr::null_mut(), code);
    code
}

fn stmt2_column_bind_not_implemented(stmt: *mut TAOS_STMT2, api: &str) -> c_int {
    let msg = format!("{api} is not implemented");
    error!("{msg}");
    let error = TaosError::new(Code::FAILED, &msg);
    let code = set_err_and_get_code(TaosError::from(&error));

    if let Some(maybe_err) = unsafe { (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() } {
        let lifecycle = maybe_err
            .deref()
            .map(|taos_stmt2| taos_stmt2.lifecycle.clone());
        if let Some(lifecycle) = lifecycle {
            let generation = lifecycle.begin_operation();
            lifecycle.publish_error(generation, maybe_err, Some(error));
            return code;
        }
    }

    code
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_exec(
    stmt: *mut TAOS_STMT2,
    affected_rows: *mut c_int,
) -> c_int {
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
    let lifecycle = taos_stmt2.lifecycle.clone();

    if taos_stmt2.async_exec_fn.is_none() {
        let generation = lifecycle.begin_operation();
        if affected_rows.is_null() {
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(Code::INVALID_PARA, "affected_rows is null")),
            );
            return format_errno(Code::INVALID_PARA.into());
        }

        match taos_stmt2.stmt2.exec() {
            Ok(rows) => {
                *affected_rows = rows as _;
                debug!("taos_stmt2_exec succ, affected_rows: {rows}");
                lifecycle.publish_error(generation, maybe_err, None);
                return clear_err_and_ret_succ();
            }
            Err(err) => {
                error!("taos_stmt2_exec failed, err: {err:?}");
                lifecycle.publish_error(
                    generation,
                    maybe_err,
                    Some(TaosError::new(err.code(), &err.to_string())),
                );
                return format_errno(err.code().into());
            }
        }
    }

    let is_insert = match taos_stmt2.stmt2.is_insert() {
        Some(is_insert) => is_insert,
        None => {
            let generation = lifecycle.begin_operation();
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(
                    Code::FAILED,
                    "taos_stmt2_prepare is not called",
                )),
            );
            return format_errno(Code::FAILED.into());
        }
    };

    let (generation, started) = lifecycle.begin_async();
    if !started {
        lifecycle.publish_error(
            generation,
            maybe_err,
            Some(TaosError::new(
                Code::FAILED,
                "another async stmt2 exec is still in flight",
            )),
        );
        return format_errno(Code::FAILED.into());
    }

    let stmt2 = taos_stmt2.stmt2.clone();
    let async_exec_fn = taos_stmt2.async_exec_fn.unwrap();
    let userdata = SafePtr(taos_stmt2.userdata);

    lifecycle.publish_error(generation, maybe_err, None);

    let guard = AsyncInFlightGuard {
        lifecycle: lifecycle.clone(),
    };

    global_tokio_runtime().spawn(
        async move {
            use taos_query::stmt2::Stmt2AsyncBindable;

            let mut stmt2 = stmt2;
            let userdata = userdata;
            let _guard = guard;

            let affected = match Stmt2AsyncBindable::exec(&mut stmt2).await {
                Ok(rows) => rows,
                Err(err) => {
                    error!("async taos_stmt2_exec exec failed, err: {err:?}");
                    lifecycle.complete_async_error(
                        generation,
                        TaosError::new(err.code(), &err.to_string()),
                    );
                    let code = format_errno(err.code().into());
                    async_exec_fn(userdata.0, ptr::null_mut(), code);
                    return;
                }
            };

            if is_insert {
                debug!("async taos_stmt2_exec insert succ, affected_rows: {affected}");
                let rs: TaosMaybeError<ResultSet> =
                    ResultSet::Stmt2(Stmt2ResultSet::new(affected as _)).into();
                let res =
                    lifecycle.complete_async_success(generation, CachedStmt2ResultKind::Insert, rs);
                async_exec_fn(userdata.0, res, 0);
                return;
            }

            match Stmt2AsyncBindable::result_set(&stmt2).await {
                Ok(rs) => {
                    debug!("async taos_stmt2_exec callback, result_set: {rs:?}");
                    let rs: TaosMaybeError<ResultSet> =
                        ResultSet::Query(QueryResultSet::new(rs)).into();
                    let res = lifecycle.complete_async_success(
                        generation,
                        CachedStmt2ResultKind::Query,
                        rs,
                    );
                    async_exec_fn(userdata.0, res, 0);
                }
                Err(err) => {
                    error!("async taos_stmt2_exec result failed, err: {err:?}");
                    lifecycle.complete_async_error(
                        generation,
                        TaosError::new(err.code(), &err.to_string()),
                    );
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

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_close(stmt: *mut TAOS_STMT2) -> c_int {
    debug!("taos_stmt2_close, stmt: {stmt:?}");
    if stmt.is_null() {
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
    }

    let maybe_err = Box::from_raw(stmt as *mut TaosMaybeError<TaosStmt2>);
    if let Some(taos_stmt2) = maybe_err.deref() {
        let lifecycle = taos_stmt2.lifecycle.clone();
        lifecycle.wait_async_done();
    }

    drop(maybe_err);
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_is_insert(stmt: *mut TAOS_STMT2, insert: *mut c_int) -> c_int {
    debug!("taos_stmt2_is_insert start, stmt: {stmt:?}, insert: {insert:?}");

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
    let lifecycle = taos_stmt2.lifecycle.clone();
    let generation = lifecycle.begin_operation();

    if insert.is_null() {
        lifecycle.publish_error(
            generation,
            maybe_err,
            Some(TaosError::new(Code::INVALID_PARA, "insert is null")),
        );
        return format_errno(Code::INVALID_PARA.into());
    }

    match taos_stmt2.stmt2.is_insert() {
        Some(is_insert) => {
            *insert = is_insert as _;
            debug!("taos_stmt2_is_insert succ, is_insert: {is_insert}");
            lifecycle.publish_error(generation, maybe_err, None);
            clear_err_and_ret_succ()
        }
        None => {
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(
                    Code::FAILED,
                    "taos_stmt2_prepare is not called",
                )),
            );
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

    let lifecycle = taos_stmt2.lifecycle.clone();
    lifecycle.wait_async_done_and_clear_result();
    let generation = lifecycle.begin_operation();

    if count.is_null() {
        lifecycle.publish_error(
            generation,
            maybe_err,
            Some(TaosError::new(Code::INVALID_PARA, "count is null")),
        );
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt2.stmt2;
    if stmt2.is_insert().is_none() {
        lifecycle.publish_error(
            generation,
            maybe_err,
            Some(TaosError::new(
                Code::FAILED,
                "taos_stmt2_prepare is not called",
            )),
        );
        return format_errno(Code::FAILED.into());
    }

    if stmt2.is_insert().unwrap() {
        let stmt2_fields = stmt2.fields().unwrap_or_default();
        if !fields.is_null() {
            if stmt2_fields.is_empty() {
                *fields = ptr::null_mut();
            } else {
                let field_all: Vec<TAOS_FIELD_ALL> =
                    stmt2_fields.iter().map(|f| f.into()).collect();
                *fields = Box::into_raw(field_all.into_boxed_slice()) as _;
                taos_stmt2.fields_len = Some(stmt2_fields.len());
            }
        }
        *count = stmt2_fields.len() as _;
    } else {
        *count = stmt2.fields_count().unwrap() as _;
    }

    debug!("taos_stmt2_get_fields succ, fields: {fields:?}, count: {count:?}");

    lifecycle.publish_error(generation, maybe_err, None);
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_free_fields(
    stmt: *mut TAOS_STMT2,
    fields: *mut TAOS_FIELD_ALL,
) {
    debug!("taos_stmt2_free_fields start, stmt: {stmt:?}, fields: {fields:?}");

    if fields.is_null() {
        return;
    }

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
    let lifecycle = taos_stmt2.lifecycle.clone();
    let generation = lifecycle.begin_operation();

    if taos_stmt2.fields_len.is_none() {
        lifecycle.publish_error(
            generation,
            maybe_err,
            Some(TaosError::new(
                Code::FAILED,
                "taos_stmt2_get_fields is not called",
            )),
        );
        return;
    }

    let len = taos_stmt2.fields_len.unwrap();
    let fields = Vec::from_raw_parts(fields, len, len);

    debug!("taos_stmt2_free_fields succ, fields: {fields:?}");

    lifecycle.publish_error(generation, maybe_err, None);
    clear_error_info();
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_result(stmt: *mut TAOS_STMT2) -> *mut TAOS_RES {
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

    let lifecycle = taos_stmt2.lifecycle.clone();
    let is_async = taos_stmt2.async_exec_fn.is_some();
    let generation = lifecycle.begin_operation();

    match taos_stmt2.stmt2.is_insert() {
        Some(false) => {}
        Some(true) => {
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(
                    Code::FAILED,
                    "taos_stmt2_result is only valid for query statement",
                )),
            );
            return ptr::null_mut();
        }
        None => {
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(
                    Code::FAILED,
                    "taos_stmt2_prepare is not called",
                )),
            );
            return ptr::null_mut();
        }
    }

    if is_async {
        match lifecycle.async_query_result(generation, maybe_err) {
            AsyncQueryResultState::Ready(result) => {
                debug!("taos_stmt2_result succ, cached result: {result:?}");
                clear_error_info();
                return result;
            }
            AsyncQueryResultState::Pending | AsyncQueryResultState::Failed => {
                return ptr::null_mut();
            }
        }
    }

    if let Some(ptr) = lifecycle.cached_query_result_ptr() {
        debug!("taos_stmt2_result succ, cached result: {ptr:?}");
        lifecycle.publish_error(generation, maybe_err, None);
        clear_error_info();
        return ptr;
    }

    match taos_stmt2.stmt2.result_set() {
        Ok(rs) => {
            let rs: TaosMaybeError<ResultSet> = ResultSet::Query(QueryResultSet::new(rs)).into();
            debug!("taos_stmt2_result succ, result_set: {rs:?}");
            let ptr = lifecycle.set_cached_result(CachedStmt2ResultKind::Query, rs);
            lifecycle.publish_error(generation, maybe_err, None);
            clear_error_info();
            ptr
        }
        Err(err) => {
            error!("taos_stmt2_result failed, err: {err:?}");
            lifecycle.publish_error(
                generation,
                maybe_err,
                Some(TaosError::new(err.code(), &err.to_string())),
            );
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_error(stmt: *mut TAOS_STMT2) -> *mut c_char {
    if let Some(maybe_err) = (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        let lifecycle = maybe_err
            .deref()
            .map(|taos_stmt2| taos_stmt2.lifecycle.clone());
        if let Some(lifecycle) = lifecycle {
            lifecycle.sync_outer_error(maybe_err);
        }
    }
    taos_errstr(stmt) as _
}

#[derive(Debug)]
struct TaosStmt2 {
    stmt2: Stmt2,
    async_exec_fn: Option<__taos_async_fn_t>,
    userdata: *mut c_void,
    fields_len: Option<usize>,
    lifecycle: Arc<Stmt2Lifecycle>,
}

impl TaosStmt2 {
    fn new(stmt2: Stmt2, async_exec_fn: Option<__taos_async_fn_t>, userdata: *mut c_void) -> Self {
        Self {
            stmt2,
            async_exec_fn,
            userdata,
            fields_len: None,
            lifecycle: Arc::new(Stmt2Lifecycle::default()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CachedStmt2ResultKind {
    Query,
    Insert,
}

struct CachedStmt2Result {
    kind: CachedStmt2ResultKind,
    value: Box<TaosMaybeError<ResultSet>>,
}

impl CachedStmt2Result {
    fn as_ptr(&self) -> *mut TAOS_RES {
        self.value.as_ref() as *const TaosMaybeError<ResultSet> as *mut TAOS_RES
    }
}

#[derive(Clone)]
struct Stmt2ErrorSnapshot {
    code: Code,
    message: String,
}

impl Stmt2ErrorSnapshot {
    fn to_error(&self) -> TaosError {
        TaosError::new(self.code, &self.message)
    }
}

impl From<&TaosError> for Stmt2ErrorSnapshot {
    fn from(error: &TaosError) -> Self {
        Self {
            code: error.code(),
            message: error.message().to_string(),
        }
    }
}

enum AsyncCompletion {
    Idle,
    Pending {
        generation: u64,
    },
    Success {
        generation: u64,
    },
    Failed {
        generation: u64,
        error: Stmt2ErrorSnapshot,
    },
}

impl Default for AsyncCompletion {
    fn default() -> Self {
        Self::Idle
    }
}

#[derive(Default)]
struct Stmt2VisibleError {
    generation: u64,
    provisional_async_generation: Option<u64>,
    error: Option<Stmt2ErrorSnapshot>,
}

#[derive(Default)]
struct Stmt2LifecycleState {
    async_in_flight: bool,
    cached_result: Option<CachedStmt2Result>,
    completion: AsyncCompletion,
    next_generation: u64,
    latest_operation_generation: u64,
    visible_error: Stmt2VisibleError,
}

#[derive(Default)]
struct Stmt2Lifecycle {
    state: Mutex<Stmt2LifecycleState>,
    done: Condvar,
}

enum AsyncQueryResultState {
    Ready(*mut TAOS_RES),
    Pending,
    Failed,
}

impl std::fmt::Debug for Stmt2Lifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stmt2Lifecycle").finish_non_exhaustive()
    }
}

impl Stmt2Lifecycle {
    fn reserve_operation(state: &mut Stmt2LifecycleState) -> u64 {
        state.next_generation = state.next_generation.wrapping_add(1);
        if state.next_generation == 0 {
            state.next_generation = 1;
        }
        state.latest_operation_generation = state.next_generation;
        state.next_generation
    }

    fn begin_operation(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        Self::reserve_operation(&mut state)
    }

    fn publish_error<T>(
        &self,
        generation: u64,
        outer: &mut TaosMaybeError<T>,
        error: Option<TaosError>,
    ) {
        let mut state = self.state.lock().unwrap();
        if state.latest_operation_generation != generation {
            return;
        }

        state.visible_error = Stmt2VisibleError {
            generation,
            provisional_async_generation: None,
            error: error.as_ref().map(Stmt2ErrorSnapshot::from),
        };

        match error {
            Some(error) => outer.with_err(Some(error)),
            None => outer.clear_err(),
        }
    }

    fn sync_outer_error<T>(&self, outer: &mut TaosMaybeError<T>) {
        let state = self.state.lock().unwrap();
        match state.visible_error.error.as_ref() {
            Some(error) => outer.with_err(Some(error.to_error())),
            None => outer.clear_err(),
        }
    }

    fn wait_async_done(&self) {
        let mut state = self.state.lock().unwrap();
        while state.async_in_flight {
            state = self.done.wait(state).unwrap();
        }
    }

    fn wait_async_done_and_clear_result(&self) {
        let mut state = self.state.lock().unwrap();
        while state.async_in_flight {
            state = self.done.wait(state).unwrap();
        }
        state.cached_result = None;
        state.completion = AsyncCompletion::Idle;
    }

    fn cached_query_result_ptr(&self) -> Option<*mut TAOS_RES> {
        let state = self.state.lock().unwrap();
        state
            .cached_result
            .as_ref()
            .filter(|r| r.kind == CachedStmt2ResultKind::Query)
            .map(|r| r.as_ptr())
    }

    fn set_cached_result(
        &self,
        kind: CachedStmt2ResultKind,
        result: TaosMaybeError<ResultSet>,
    ) -> *mut TAOS_RES {
        let mut state = self.state.lock().unwrap();
        state.cached_result = Some(CachedStmt2Result {
            kind,
            value: Box::new(result),
        });
        state.cached_result.as_ref().unwrap().as_ptr()
    }

    fn begin_async(&self) -> (u64, bool) {
        let mut state = self.state.lock().unwrap();
        let generation = Self::reserve_operation(&mut state);
        if state.async_in_flight {
            return (generation, false);
        }
        state.async_in_flight = true;
        state.cached_result = None;
        state.completion = AsyncCompletion::Pending { generation };
        (generation, true)
    }

    fn should_publish_async_completion(state: &Stmt2LifecycleState, async_generation: u64) -> bool {
        state.latest_operation_generation == async_generation
            || (state.visible_error.generation == state.latest_operation_generation
                && state.visible_error.provisional_async_generation == Some(async_generation))
    }

    fn complete_async_success(
        &self,
        generation: u64,
        kind: CachedStmt2ResultKind,
        result: TaosMaybeError<ResultSet>,
    ) -> *mut TAOS_RES {
        let mut state = self.state.lock().unwrap();
        state.cached_result = Some(CachedStmt2Result {
            kind,
            value: Box::new(result),
        });
        let result = state.cached_result.as_ref().unwrap().as_ptr();
        state.completion = AsyncCompletion::Success { generation };

        if Self::should_publish_async_completion(&state, generation) {
            let visible_generation = state.latest_operation_generation;
            state.visible_error = Stmt2VisibleError {
                generation: visible_generation,
                provisional_async_generation: None,
                error: None,
            };
        }

        result
    }

    fn complete_async_error(&self, generation: u64, error: TaosError) {
        let error = Stmt2ErrorSnapshot::from(&error);
        let mut state = self.state.lock().unwrap();
        state.cached_result = None;
        state.completion = AsyncCompletion::Failed {
            generation,
            error: error.clone(),
        };

        if Self::should_publish_async_completion(&state, generation) {
            let visible_generation = state.latest_operation_generation;
            state.visible_error = Stmt2VisibleError {
                generation: visible_generation,
                provisional_async_generation: None,
                error: Some(error),
            };
        }
    }

    fn async_query_result<T>(
        &self,
        operation_generation: u64,
        outer: &mut TaosMaybeError<T>,
    ) -> AsyncQueryResultState {
        let mut state = self.state.lock().unwrap();
        let should_publish = state.latest_operation_generation == operation_generation;

        enum Snapshot {
            Ready(*mut TAOS_RES),
            Pending(Option<u64>),
            Failed(Stmt2ErrorSnapshot),
        }

        let snapshot = match &state.completion {
            AsyncCompletion::Idle => Snapshot::Pending(None),
            AsyncCompletion::Pending { generation } => Snapshot::Pending(Some(*generation)),
            AsyncCompletion::Success { generation } => {
                debug_assert_ne!(*generation, 0);
                match state
                    .cached_result
                    .as_ref()
                    .filter(|result| result.kind == CachedStmt2ResultKind::Query)
                {
                    Some(result) => Snapshot::Ready(result.as_ptr()),
                    None => Snapshot::Pending(None),
                }
            }
            AsyncCompletion::Failed { generation, error } => {
                debug_assert_ne!(*generation, 0);
                Snapshot::Failed(error.clone())
            }
        };

        match snapshot {
            Snapshot::Ready(result) => {
                if should_publish {
                    state.visible_error = Stmt2VisibleError {
                        generation: operation_generation,
                        provisional_async_generation: None,
                        error: None,
                    };
                    outer.clear_err();
                }
                AsyncQueryResultState::Ready(result)
            }
            Snapshot::Pending(async_generation) => {
                if should_publish {
                    let error =
                        TaosError::new(Code::FAILED, "async stmt2 result is not available yet");
                    state.visible_error = Stmt2VisibleError {
                        generation: operation_generation,
                        provisional_async_generation: async_generation,
                        error: Some(Stmt2ErrorSnapshot::from(&error)),
                    };
                    outer.with_err(Some(error));
                }
                AsyncQueryResultState::Pending
            }
            Snapshot::Failed(error) => {
                if should_publish {
                    state.visible_error = Stmt2VisibleError {
                        generation: operation_generation,
                        provisional_async_generation: None,
                        error: Some(error.clone()),
                    };
                    outer.with_err(Some(error.to_error()));
                }
                AsyncQueryResultState::Failed
            }
        }
    }

    fn finish_async(&self) {
        let mut state = self.state.lock().unwrap();
        state.async_in_flight = false;
        drop(state);
        self.done.notify_all();
    }
}

struct AsyncInFlightGuard {
    lifecycle: Arc<Stmt2Lifecycle>,
}

impl Drop for AsyncInFlightGuard {
    fn drop(&mut self) {
        self.lifecycle.finish_async();
    }
}

#[derive(Debug)]
pub struct Stmt2ResultSet {
    affected_rows: i64,
}

impl Stmt2ResultSet {
    fn new(affected_rows: i64) -> Self {
        Self { affected_rows }
    }
}

impl ResultSetOperations for Stmt2ResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        0
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        0
    }

    fn tmq_get_offset(&self) -> taos_ws::Offset {
        panic!("tmq_get_offset is not supported for Stmt2ResultSet")
    }

    fn precision(&self) -> taos_query::common::Precision {
        taos_query::common::Precision::Millisecond
    }

    fn affected_rows(&self) -> i32 {
        self.affected_rows as _
    }

    fn affected_rows64(&self) -> i64 {
        self.affected_rows
    }

    fn num_of_fields(&self) -> i32 {
        0
    }

    fn get_fields(&mut self) -> *mut crate::ws::TAOS_FIELD {
        ptr::null_mut()
    }

    fn get_fields_e(&mut self) -> *mut crate::ws::TAOS_FIELD_E {
        ptr::null_mut()
    }

    unsafe fn fetch_raw_block(
        &mut self,
        _ptr: *mut *mut c_void,
        _rows: *mut i32,
    ) -> Result<(), taos_ws::query::Error> {
        Err(taos_ws::query::Error::CommonError(
            "stmt2 result set does not support fetch raw block".to_string(),
        ))
    }

    unsafe fn fetch_block(
        &mut self,
        _rows: *mut crate::ws::TAOS_ROW,
        _num: *mut c_int,
    ) -> Result<(), taos_ws::query::Error> {
        Err(taos_ws::query::Error::CommonError(
            "stmt2 result set does not support fetch block".to_string(),
        ))
    }

    unsafe fn fetch_row(&mut self) -> Result<crate::ws::TAOS_ROW, taos_ws::query::Error> {
        Err(taos_ws::query::Error::CommonError(
            "stmt2 result set does not support fetch row".to_string(),
        ))
    }

    unsafe fn get_raw_value(&mut self, _row: usize, _col: usize) -> (Ty, u32, *const c_void) {
        (Ty::Null, 0, ptr::null())
    }

    fn take_timing(&mut self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    fn stop_query(&mut self) {}

    unsafe fn is_null_by_column(
        &mut self,
        _column_index: usize,
        _result: *mut bool,
        _rows: *mut c_int,
    ) -> Result<(), TaosError> {
        Err(TaosError::new(
            Code::FAILED,
            "stmt2 result set does not support is_null_by_column",
        ))
    }

    fn get_column_data_offset(&mut self, _column_index: usize) -> Result<*mut c_int, TaosError> {
        Err(TaosError::new(
            Code::FAILED,
            "stmt2 result set does not support get_column_data_offset",
        ))
    }
}

impl TAOS_STMT2_BIND {
    fn ty(&self) -> Ty {
        self.buffer_type.into()
    }
}

const REQ_ID_POS: usize = 0;
const STMT_ID_POS: usize = REQ_ID_POS + 8;
const ACTION_POS: usize = STMT_ID_POS + 8;
const VERSION_POS: usize = ACTION_POS + 8;
const COL_IDX_POS: usize = VERSION_POS + 2;
const HEADER_LEN: usize = COL_IDX_POS + 4;

const TOTAL_LENGTH_POS: usize = 0;
const TABLE_COUNT_POS: usize = TOTAL_LENGTH_POS + 4;
const TAG_COUNT_POS: usize = TABLE_COUNT_POS + 4;
const COL_COUNT_POS: usize = TAG_COUNT_POS + 4;
const TABLE_NAMES_OFFSET_POS: usize = COL_COUNT_POS + 4;
const TAGS_OFFSET_POS: usize = TABLE_NAMES_OFFSET_POS + 4;
const COLS_OFFSET_POS: usize = TAGS_OFFSET_POS + 4;
const DATA_POS: usize = COLS_OFFSET_POS + 4;

const ACTION: u64 = 9;
const VERSION: u16 = 1;
const COL_IDX: i32 = -1;

impl TAOS_STMT2_BINDV {
    fn to_bytes(
        &self,
        req_id: u64,
        stmt_id: u64,
        tag_cnt: usize,
        col_cnt: usize,
    ) -> TaosResult<Vec<u8>> {
        debug!("to_bytes, req_id: 0x{req_id:x}, stmt_id: {stmt_id}, tag_cnt: {tag_cnt}, col_cnt: {col_cnt}");

        let (tbname_buf_len, tbname_lens) = if !self.tbnames.is_null() {
            self.calc_tbname_lens()?
        } else {
            (0, vec![])
        };

        let (tag_buf_len, tag_lens, tag_total_lens, tag_buf_lens) = if !self.tags.is_null() {
            self.calc_tag_or_col_lens(self.tags, tag_cnt)?
        } else {
            (0, vec![], vec![], vec![])
        };

        let (col_buf_len, col_lens, col_total_lens, col_buf_lens) = if !self.bind_cols.is_null() {
            self.calc_tag_or_col_lens(self.bind_cols, col_cnt)?
        } else {
            (0, vec![], vec![], vec![])
        };

        let have_tbname = tbname_buf_len > 0;
        let have_tag = tag_buf_len > 0;
        let have_col = col_buf_len > 0;

        let tbname_total_len = tbname_lens.len() * 2 + tbname_buf_len;
        let tag_total_len = tag_lens.len() * 4 + tag_buf_len as usize;
        let col_total_len = col_lens.len() * 4 + col_buf_len as usize;
        let total_len = 28 + tbname_total_len + tag_total_len + col_total_len;

        trace!("to_bytes, tbname_buf_len: {tbname_buf_len}, tbname_lens: {tbname_lens:?}");
        trace!("to_bytes, tag_buf_len: {tag_buf_len}, tag_lens: {tag_lens:?}, tag_total_lens: {tag_total_lens:?}, tag_buf_lens: {tag_buf_lens:?}");
        trace!("to bytes, col_buf_len: {col_buf_len}, col_lens: {col_lens:?}, col_total_lens: {col_total_lens:?}, col_buf_lens: {col_buf_lens:?}");
        trace!("to bytes, tbname_total_len: {tbname_total_len}, tag_total_len: {tag_total_len}, col_total_len: {col_total_len}, total_len: {total_len}");

        let mut data = vec![0u8; HEADER_LEN + total_len];
        self.write_headers(&mut data, req_id, stmt_id);

        let bytes = &mut data[HEADER_LEN..];
        LittleEndian::write_u32(&mut bytes[TOTAL_LENGTH_POS..], total_len as _);
        LittleEndian::write_i32(&mut bytes[TABLE_COUNT_POS..], self.count);
        LittleEndian::write_i32(&mut bytes[TAG_COUNT_POS..], tag_cnt as _);
        LittleEndian::write_i32(&mut bytes[COL_COUNT_POS..], col_cnt as _);

        if have_tbname {
            LittleEndian::write_u32(&mut bytes[TABLE_NAMES_OFFSET_POS..], DATA_POS as _);
            self.write_tbnames(&mut bytes[DATA_POS..], &tbname_lens);
        }

        if have_tag {
            let tags_offset = DATA_POS + tbname_total_len;
            LittleEndian::write_u32(&mut bytes[TAGS_OFFSET_POS..], tags_offset as _);
            self.write_tags_or_cols(
                &mut bytes[tags_offset..],
                self.tags,
                tag_cnt,
                &tag_lens,
                &tag_total_lens,
                &tag_buf_lens,
            );
        }

        if have_col {
            let cols_offset = DATA_POS + tbname_total_len + tag_total_len;
            LittleEndian::write_u32(&mut bytes[COLS_OFFSET_POS..], cols_offset as _);
            self.write_tags_or_cols(
                &mut bytes[cols_offset..],
                self.bind_cols,
                col_cnt,
                &col_lens,
                &col_total_lens,
                &col_buf_lens,
            );
        }

        Ok(data)
    }

    fn calc_tbname_lens(&self) -> TaosResult<(usize, Vec<u16>)> {
        let mut len = 0;
        let mut lens = vec![0u16; self.count as usize];
        for i in 0..self.count as usize {
            let tbname_ptr = unsafe { self.tbnames.add(i).read() };
            if tbname_ptr.is_null() {
                return Err(TaosError::new(
                    Code::INVALID_PARA,
                    "stmt2 bind tbname is null",
                ));
            }
            lens[i] = unsafe { CStr::from_ptr(tbname_ptr).to_bytes_with_nul().len() } as _;
            len += lens[i] as usize;
        }
        Ok((len, lens))
    }

    #[allow(clippy::type_complexity)]
    fn calc_tag_or_col_lens(
        &self,
        binds_ptr: *mut *mut TAOS_STMT2_BIND,
        tc_cnt: usize,
    ) -> TaosResult<(u32, Vec<u32>, Vec<u32>, Vec<u32>)> {
        let cnt = self.count as usize;
        let mut len = 0;
        let mut data_lens = vec![0; cnt];
        let mut total_lens = vec![0; cnt * tc_cnt];
        let mut buf_lens = vec![0; cnt * tc_cnt];

        for i in 0..cnt {
            let bind_ptr = unsafe { binds_ptr.add(i).read() };
            if bind_ptr.is_null() {
                return Err(TaosError::new(
                    Code::INVALID_PARA,
                    "stmt2 bind tag or column is null",
                ));
            }

            for j in 0..tc_cnt {
                let bind = unsafe { bind_ptr.add(j).read() };
                let have_len = bind.ty().fixed_length() == 0 || bind.ty().is_decimal();
                let buf_len = if self.check_tag_or_col_is_null(bind.is_null, bind.num as _) {
                    0
                } else if have_len {
                    if bind.length.is_null() {
                        return Err(TaosError::new(
                            Code::INVALID_PARA,
                            "stmt2 bind tag or column length is null",
                        ));
                    }

                    let lens = unsafe { slice::from_raw_parts(bind.length, bind.num as _) };
                    let is_null = bind.is_null.is_null();
                    let mut buf_len = 0;
                    for (k, len) in lens.iter().enumerate() {
                        if is_null || unsafe { bind.is_null.add(k).read() } == 0 {
                            buf_len += lens[k] as u32;
                        }
                    }
                    buf_len
                } else {
                    bind.num as u32 * bind.ty().fixed_length() as u32
                };

                let k = i * tc_cnt + j;
                buf_lens[k] = buf_len;
                total_lens[k] = self.calc_tag_or_col_header_len(bind.num as _, have_len) + buf_len;
                data_lens[i] += total_lens[k];
                len += total_lens[k];
            }
        }

        Ok((len, data_lens, total_lens, buf_lens))
    }

    fn check_tag_or_col_is_null(&self, is_null: *const c_char, len: usize) -> bool {
        if is_null.is_null() {
            return false;
        }
        let slice = unsafe { slice::from_raw_parts(is_null, len) };
        slice.iter().all(|&v| v == 1)
    }

    fn calc_tag_or_col_header_len(&self, num: u32, have_len: bool) -> u32 {
        let mut len = 17 + num;
        if have_len {
            len += num * 4;
        }
        len
    }

    fn write_headers(&self, bytes: &mut [u8], req_id: u64, stmt_id: u64) {
        LittleEndian::write_u64(&mut bytes[REQ_ID_POS..], req_id);
        LittleEndian::write_u64(&mut bytes[STMT_ID_POS..], stmt_id);
        LittleEndian::write_u64(&mut bytes[ACTION_POS..], ACTION);
        LittleEndian::write_u16(&mut bytes[VERSION_POS..], VERSION);
        LittleEndian::write_i32(&mut bytes[COL_IDX_POS..], COL_IDX);
    }

    fn write_tbnames(&self, bytes: &mut [u8], tbname_lens: &[u16]) {
        let mut offset = 0;
        for len in tbname_lens {
            LittleEndian::write_u16(&mut bytes[offset..], *len);
            offset += 2;
        }
        for i in 0..self.count as usize {
            let tbname_ptr = unsafe { self.tbnames.add(i).read() };
            let tbname = unsafe { CStr::from_ptr(tbname_ptr).to_bytes_with_nul() };
            bytes[offset..offset + tbname.len()].copy_from_slice(tbname);
            offset += tbname.len();
        }
    }

    fn write_tags_or_cols(
        &self,
        bytes: &mut [u8],
        binds_ptr: *mut *mut TAOS_STMT2_BIND,
        tc_cnt: usize,
        tc_lens: &[u32],
        tc_total_lens: &[u32],
        tc_buf_lens: &[u32],
    ) {
        let mut offset = 0;
        for len in tc_lens {
            LittleEndian::write_u32(&mut bytes[offset..], *len);
            offset += 4;
        }

        for i in 0..self.count as usize {
            let bind_ptr = unsafe { binds_ptr.add(i).read() };
            for j in 0..tc_cnt {
                let bind = unsafe { bind_ptr.add(j).read() };
                LittleEndian::write_u32(&mut bytes[offset..], tc_total_lens[i * tc_cnt + j]);
                offset += 4;
                LittleEndian::write_i32(&mut bytes[offset..], bind.ty() as _);
                offset += 4;
                LittleEndian::write_i32(&mut bytes[offset..], bind.num);
                offset += 4;

                if !bind.is_null.is_null() {
                    unsafe {
                        ptr::copy_nonoverlapping(
                            bind.is_null as *const u8,
                            bytes.as_mut_ptr().add(offset),
                            bind.num as _,
                        );
                    }
                }
                offset += bind.num as usize;

                let have_len = bind.ty().fixed_length() == 0 || bind.ty().is_decimal();
                if have_len {
                    bytes[offset] = 1;
                }
                offset += 1;

                if have_len {
                    let cnt = bind.num as usize * 4;
                    if !self.check_tag_or_col_is_null(bind.is_null, bind.num as _) {
                        unsafe {
                            ptr::copy_nonoverlapping(
                                bind.length as *const u8,
                                bytes.as_mut_ptr().add(offset),
                                cnt,
                            );
                        }
                    }
                    offset += cnt;
                }

                let buf_len = tc_buf_lens[i * tc_cnt + j];
                LittleEndian::write_u32(&mut bytes[offset..], buf_len);
                offset += 4;

                if !bind.buffer.is_null() {
                    unsafe {
                        ptr::copy_nonoverlapping(
                            bind.buffer as *const u8,
                            bytes.as_mut_ptr().add(offset),
                            buf_len as _,
                        );
                    }
                    offset += buf_len as usize;
                }
            }
        }
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
            field_type: field.bind_type.as_u8(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicPtr, Ordering};
    use std::sync::{Arc, Barrier};

    use futures::future::try_join_all;
    use rand::Rng;
    use taos_query::util::ws_proxy::{InterceptFn, ProxyAction, WsProxy};
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::ws::{query::*, taos_connect};
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
    fn test_stmt2_column_bind_c_abi_shape() {
        let bindv = TAOS_STMT2_COLUMN_BINDV {
            num_columns: 0,
            num_rows: 0,
            num_tables: 0,
            columns: ptr::null_mut(),
        };

        let _ = format!("{bindv:?}");

        let _bind: unsafe extern "C" fn(*mut TAOS_STMT2, *mut TAOS_STMT2_COLUMN_BINDV) -> c_int =
            taos_stmt2_bind_param_column;
        let _bind_a: unsafe extern "C" fn(
            *mut TAOS_STMT2,
            *mut TAOS_STMT2_COLUMN_BINDV,
            __taos_async_fn_t,
            *mut c_void,
        ) -> c_int = taos_stmt2_bind_param_column_a;
    }

    #[test]
    fn test_stmt2_lifecycle_async_success_supersedes_pending_result_error() {
        let lifecycle = Arc::new(Stmt2Lifecycle::default());
        let mut outer: TaosMaybeError<()> = ().into();

        let (async_generation, started) = lifecycle.begin_async();
        assert!(started);
        lifecycle.publish_error(async_generation, &mut outer, None);

        let poll_generation = lifecycle.begin_operation();
        assert!(matches!(
            lifecycle.async_query_result(poll_generation, &mut outer),
            AsyncQueryResultState::Pending
        ));
        let err = unsafe { CStr::from_ptr(outer.errstr().unwrap()) }
            .to_str()
            .unwrap();
        assert!(err.contains("not available yet"), "{err}");

        let lifecycle2 = lifecycle.clone();
        std::thread::spawn(move || {
            let result: TaosMaybeError<ResultSet> = ResultSet::Stmt2(Stmt2ResultSet::new(0)).into();
            lifecycle2.complete_async_success(
                async_generation,
                CachedStmt2ResultKind::Query,
                result,
            );
        })
        .join()
        .unwrap();

        lifecycle.sync_outer_error(&mut outer);
        assert!(outer.errno().is_none());

        let result_generation = lifecycle.begin_operation();
        assert!(matches!(
            lifecycle.async_query_result(result_generation, &mut outer),
            AsyncQueryResultState::Ready(_)
        ));
    }

    #[test]
    fn test_stmt2_lifecycle_older_async_completion_preserves_newer_error() {
        let lifecycle = Arc::new(Stmt2Lifecycle::default());
        let mut outer: TaosMaybeError<()> = ().into();

        let (async_generation, started) = lifecycle.begin_async();
        assert!(started);
        lifecycle.publish_error(async_generation, &mut outer, None);

        let sync_generation = lifecycle.begin_operation();
        lifecycle.publish_error(
            sync_generation,
            &mut outer,
            Some(TaosError::new(Code::INVALID_PARA, "newer sync error")),
        );

        let lifecycle2 = lifecycle.clone();
        std::thread::spawn(move || {
            lifecycle2.complete_async_error(
                async_generation,
                TaosError::new(Code::FAILED, "older async error"),
            );
        })
        .join()
        .unwrap();

        lifecycle.sync_outer_error(&mut outer);
        let err = unsafe { CStr::from_ptr(outer.errstr().unwrap()) }
            .to_str()
            .unwrap();
        assert_eq!(err, "newer sync error");
    }

    #[test]
    fn test_stmt2_lifecycle_stale_result_operation_still_reads_ready_completion() {
        let lifecycle = Stmt2Lifecycle::default();
        let (async_generation, started) = lifecycle.begin_async();
        assert!(started);
        let result: TaosMaybeError<ResultSet> = ResultSet::Stmt2(Stmt2ResultSet::new(0)).into();
        lifecycle.complete_async_success(async_generation, CachedStmt2ResultKind::Query, result);

        let result_generation = lifecycle.begin_operation();
        let newer_generation = lifecycle.begin_operation();
        let mut newer_outer: TaosMaybeError<()> = ().into();
        lifecycle.publish_error(
            newer_generation,
            &mut newer_outer,
            Some(TaosError::new(Code::INVALID_PARA, "newer sync error")),
        );

        let mut result_outer: TaosMaybeError<()> = ().into();
        assert!(matches!(
            lifecycle.async_query_result(result_generation, &mut result_outer),
            AsyncQueryResultState::Ready(_)
        ));

        lifecycle.sync_outer_error(&mut newer_outer);
        let err = unsafe { CStr::from_ptr(newer_outer.errstr().unwrap()) }
            .to_str()
            .unwrap();
        assert_eq!(err, "newer sync error");
    }

    static STMT2_COLUMN_BIND_CALLBACK_CODE: std::sync::atomic::AtomicI32 =
        std::sync::atomic::AtomicI32::new(0);

    extern "C" fn capture_column_bind_code(_param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
        assert!(res.is_null());
        STMT2_COLUMN_BIND_CALLBACK_CODE.store(code, std::sync::atomic::Ordering::SeqCst);
    }

    #[test]
    fn test_stmt2_column_bind_stubs_report_unsupported() {
        use crate::ws::error::{format_errno, taos_errstr};

        let expected = format_errno(Code::FAILED.into());
        unsafe {
            assert_eq!(
                taos_stmt2_bind_param_column(ptr::null_mut(), ptr::null_mut()),
                expected
            );
            let err = CStr::from_ptr(taos_errstr(ptr::null_mut()))
                .to_str()
                .unwrap();
            assert!(err.contains("not implemented"), "{err}");

            STMT2_COLUMN_BIND_CALLBACK_CODE.store(0, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(
                taos_stmt2_bind_param_column_a(
                    ptr::null_mut(),
                    ptr::null_mut(),
                    capture_column_bind_code,
                    ptr::null_mut(),
                ),
                expected
            );
            assert_eq!(
                STMT2_COLUMN_BIND_CALLBACK_CODE.load(std::sync::atomic::Ordering::SeqCst),
                expected
            );
        }
    }

    #[test]
    fn test_stmt2_calc_col_lens_decimal64_use_variable_length() {
        let mut buffer = b"99.98761.0234".to_vec();
        let mut length = vec![7, 6];
        let mut is_null = vec![0, 0];
        let c1 = new_bind!(Ty::Decimal64, buffer, length, is_null);

        let mut col = vec![c1];
        let mut cols = vec![col.as_mut_ptr()];
        let bindv = TAOS_STMT2_BINDV {
            count: 1,
            tbnames: ptr::null_mut(),
            tags: ptr::null_mut(),
            bind_cols: cols.as_mut_ptr(),
        };

        let (len, data_lens, total_lens, buf_lens) =
            bindv.calc_tag_or_col_lens(bindv.bind_cols, 1).unwrap();
        assert_eq!(buf_lens, vec![13]);
        assert_eq!(total_lens, vec![40]);
        assert_eq!(data_lens, vec![40]);
        assert_eq!(len, 40);
    }

    #[test]
    fn test_stmt2_calc_col_lens_decimal_use_variable_length_with_null_and_scientific() {
        let mut buffer = b"1234567890.12345678901231.23e+5".to_vec();
        let mut length = vec![24, 0, 7];
        let mut is_null = vec![0, 1, 0];
        let c1 = new_bind!(Ty::Decimal, buffer, length, is_null);

        let mut col = vec![c1];
        let mut cols = vec![col.as_mut_ptr()];
        let bindv = TAOS_STMT2_BINDV {
            count: 1,
            tbnames: ptr::null_mut(),
            tags: ptr::null_mut(),
            bind_cols: cols.as_mut_ptr(),
        };

        let (len, data_lens, total_lens, buf_lens) =
            bindv.calc_tag_or_col_lens(bindv.bind_cols, 1).unwrap();
        assert_eq!(buf_lens, vec![31]);
        assert_eq!(total_lens, vec![63]);
        assert_eq!(data_lens, vec![63]);
        assert_eq!(len, 63);
    }

    #[test]
    fn test_stmt2_write_cols_all_null_varbinary_keeps_length_slot() {
        let mut ts_buffer = vec![1739521477831i64, 1739521477832];
        let mut ts_length = vec![8, 8];
        let mut ts_is_null = vec![0, 0];
        let ts = new_bind!(Ty::Timestamp, ts_buffer, ts_length, ts_is_null);

        let mut c1_is_null = vec![1, 1];
        let c1 = TAOS_STMT2_BIND {
            buffer_type: Ty::VarBinary as _,
            buffer: ptr::null_mut(),
            length: ptr::null_mut(),
            is_null: c1_is_null.as_mut_ptr(),
            num: c1_is_null.len() as _,
        };

        let mut c2_buffer = vec![10i32, 11];
        let mut c2_length = vec![4, 4];
        let mut c2_is_null = vec![0, 0];
        let c2 = new_bind!(Ty::Int, c2_buffer, c2_length, c2_is_null);

        let mut col = vec![ts, c1, c2];
        let mut cols = vec![col.as_mut_ptr()];
        let bindv = TAOS_STMT2_BINDV {
            count: 1,
            tbnames: ptr::null_mut(),
            tags: ptr::null_mut(),
            bind_cols: cols.as_mut_ptr(),
        };

        let bytes = bindv.to_bytes(1, 1, 0, 3).unwrap();
        let payload = &bytes[HEADER_LEN..];
        let cols_offset = LittleEndian::read_u32(&payload[COLS_OFFSET_POS..]) as usize;
        let cols_data = &payload[cols_offset..];

        let table_len = LittleEndian::read_u32(&cols_data[0..4]) as usize;
        let ts_total = LittleEndian::read_u32(&cols_data[4..8]) as usize;
        let c1_pos = 4 + ts_total;
        let c1_total = LittleEndian::read_u32(&cols_data[c1_pos..c1_pos + 4]) as usize;
        let c2_pos = c1_pos + c1_total;
        let c2_total = LittleEndian::read_u32(&cols_data[c2_pos..c2_pos + 4]) as usize;

        assert_eq!(ts_total, 35);
        assert_eq!(c1_total, 27);
        assert_eq!(c2_total, 27);
        assert_eq!(table_len, ts_total + c1_total + c2_total);
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

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1754448810",
                    "create database test_1754448810",
                    "use test_1754448810",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname = c"d0";
            let mut tbnames = vec![tbname.as_ptr() as _];

            let mut buffer = vec![101];
            let t1 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: buffer.as_mut_ptr() as _,
                length: ptr::null_mut(),
                is_null: ptr::null_mut(),
                num: buffer.len() as _,
            };

            let mut tag = vec![t1];
            let mut tags = vec![tag.as_mut_ptr()];

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let ts = TAOS_STMT2_BIND {
                buffer_type: Ty::Timestamp as _,
                buffer: buffer.as_mut_ptr() as _,
                length: ptr::null_mut(),
                is_null: ptr::null_mut(),
                num: buffer.len() as _,
            };

            let mut buffer = vec![1, 2, 3, 4];
            let c1 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: buffer.as_mut_ptr() as _,
                length: ptr::null_mut(),
                is_null: ptr::null_mut(),
                num: buffer.len() as _,
            };

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
            assert_eq!(affected_rows, 4);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1754448810");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_all_null_varbinary_not_last_column() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1778045960",
                    "create database test_1778045960",
                    "use test_1778045960",
                    "create table t0 (ts timestamp, c1 varbinary(32), c2 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut ts_buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut ts_length = vec![8, 8, 8, 8];
            let mut ts_is_null = vec![0, 0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, ts_buffer, ts_length, ts_is_null);

            let mut c1_is_null = vec![1, 1, 1, 1];
            let c1 = TAOS_STMT2_BIND {
                buffer_type: Ty::VarBinary as _,
                buffer: ptr::null_mut(),
                length: ptr::null_mut(),
                is_null: c1_is_null.as_mut_ptr(),
                num: c1_is_null.len() as _,
            };

            let mut c2_buffer = vec![10i32, 11, 12, 13];
            let mut c2_length = vec![4, 4, 4, 4];
            let mut c2_is_null = vec![0, 0, 0, 0];
            let c2 = new_bind!(Ty::Int, c2_buffer, c2_length, c2_is_null);

            let mut col = vec![ts, c1, c2];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 4);

            let sql = c"select * from t0 where c2 >= ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut query_buffer = vec![0i32];
            let mut query_length = vec![4];
            let mut query_is_null = vec![0];
            let c2 = new_bind!(Ty::Int, query_buffer, query_length, query_is_null);

            let mut col = vec![c2];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
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

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 3);

            for _ in 0..4 {
                let row = taos_fetch_row(res);
                assert!(!row.is_null());

                let mut str = vec![0 as c_char; 1024];
                let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                assert!(len > 0);
                let row_text = CStr::from_ptr(str.as_ptr()).to_str().unwrap();
                assert!(row_text.contains("NULL"));
            }

            assert_eq!(taos_stmt2_close(stmt2), 0);

            test_exec(taos, "drop database if exists test_1778045960");
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
    fn test_taos_stmt2_async_insert_success() {
        static ASYNC_INSERT_AFFECTED: AtomicI64 = AtomicI64::new(-1);
        static ASYNC_INSERT_CODE: AtomicI64 = AtomicI64::new(-1);
        static ASYNC_INSERT_RES_NULL: AtomicI64 = AtomicI64::new(-1);

        unsafe {
            extern "C" fn fp(_userdata: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    ASYNC_INSERT_CODE.store(code as i64, Ordering::SeqCst);
                    ASYNC_INSERT_RES_NULL.store(res.is_null() as i64, Ordering::SeqCst);
                    if !res.is_null() {
                        let affected = taos_affected_rows(res);
                        ASYNC_INSERT_AFFECTED.store(affected as i64, Ordering::SeqCst);
                    }
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783575980",
                    "create database test_1783575980",
                    "use test_1783575980",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let mut option = TAOS_STMT2_OPTION {
                reqid: 2001,
                singleStbInsert: false,
                singleTableBindOnce: false,
                asyncExecFn: fp,
                userdata: ptr::null_mut(),
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![1739521477831i64, 1739521477832, 1739521477833];
            let mut length = vec![8, 8, 8];
            let mut is_null = vec![0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![10i32, 20, 30];
            let mut length = vec![4, 4, 4];
            let mut is_null = vec![0, 0, 0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let code = taos_stmt2_exec(stmt2, ptr::null_mut());
            assert_eq!(code, 0);

            std::thread::sleep(std::time::Duration::from_secs(1));

            assert_eq!(ASYNC_INSERT_CODE.load(Ordering::SeqCst), 0);
            assert_eq!(ASYNC_INSERT_RES_NULL.load(Ordering::SeqCst), 0);
            assert_eq!(ASYNC_INSERT_AFFECTED.load(Ordering::SeqCst), 3);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database if exists test_1783575980");
            taos_close(taos);
        }
    }

    struct SyncPoints {
        entered: Barrier,
        release: Barrier,
        res_ptr: AtomicPtr<c_void>,
        stmt_ptr: AtomicPtr<c_void>,
        result_from_callback: AtomicPtr<c_void>,
        code: AtomicI64,
        cb_done: AtomicI64,
    }

    fn new_sync_points() -> Box<SyncPoints> {
        Box::new(SyncPoints {
            entered: Barrier::new(2),
            release: Barrier::new(2),
            res_ptr: AtomicPtr::new(ptr::null_mut()),
            stmt_ptr: AtomicPtr::new(ptr::null_mut()),
            result_from_callback: AtomicPtr::new(ptr::null_mut()),
            code: AtomicI64::new(-1),
            cb_done: AtomicI64::new(0),
        })
    }

    extern "C" fn blocking_cb(userdata: *mut c_void, res: *mut TAOS_RES, code: c_int) {
        unsafe {
            let sp = &*(userdata as *const SyncPoints);
            sp.res_ptr.store(res, Ordering::SeqCst);
            sp.code.store(code as i64, Ordering::SeqCst);
            sp.entered.wait();
            sp.release.wait();
            let stmt = sp.stmt_ptr.load(Ordering::SeqCst);
            if !stmt.is_null() {
                sp.result_from_callback
                    .store(taos_stmt2_result(stmt), Ordering::SeqCst);
            }
            sp.cb_done.store(1, Ordering::SeqCst);
        }
    }

    unsafe fn prep_async_query_stmt(taos: *mut TAOS, sp: &SyncPoints) -> *mut TAOS_STMT2 {
        let mut option = TAOS_STMT2_OPTION {
            reqid: 3001,
            singleStbInsert: false,
            singleTableBindOnce: false,
            asyncExecFn: blocking_cb,
            userdata: sp as *const SyncPoints as *mut c_void,
        };
        let stmt2 = taos_stmt2_init(taos, &mut option);
        assert!(!stmt2.is_null());

        let sql = c"select * from t0 where c1 > ?";
        assert_eq!(taos_stmt2_prepare(stmt2, sql.as_ptr(), 0), 0);

        let mut buffer = vec![0];
        let mut length = vec![4];
        let mut is_null = vec![0];
        let c1 = new_bind!(Ty::Int, buffer, length, is_null);
        let mut col = vec![c1];
        let mut cols = vec![col.as_mut_ptr()];
        let mut bindv = TAOS_STMT2_BINDV {
            count: 1,
            tbnames: ptr::null_mut(),
            tags: ptr::null_mut(),
            bind_cols: cols.as_mut_ptr(),
        };
        assert_eq!(taos_stmt2_bind_param(stmt2, &mut bindv, -1), 0);
        stmt2
    }

    #[test]
    fn test_taos_stmt2_async_result_available_while_callback_runs() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783576629",
                    "create database test_1783576629",
                    "use test_1783576629",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 5)",
                ],
            );

            let sp = new_sync_points();
            let stmt2 = prep_async_query_stmt(taos, &sp);

            assert_eq!(taos_stmt2_exec(stmt2, ptr::null_mut()), 0);

            sp.entered.wait();

            let res_during_callback = taos_stmt2_result(stmt2);
            assert!(
                !res_during_callback.is_null(),
                "result must be available while the async callback runs"
            );
            assert_eq!(res_during_callback, sp.res_ptr.load(Ordering::SeqCst));

            sp.release.wait();
            while sp.cb_done.load(Ordering::SeqCst) == 0 {
                std::thread::yield_now();
            }

            let mut res_after = ptr::null_mut();
            for _ in 0..500 {
                res_after = taos_stmt2_result(stmt2);
                if !res_after.is_null() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert!(!res_after.is_null());
            assert_eq!(res_after, sp.res_ptr.load(Ordering::SeqCst));

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1783576629");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_async_result_available_in_callback() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1784000078",
                    "create database test_1784000078",
                    "use test_1784000078",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 5)",
                ],
            );

            let sp = new_sync_points();
            let stmt2 = prep_async_query_stmt(taos, &sp);
            sp.stmt_ptr.store(stmt2 as _, Ordering::SeqCst);

            assert_eq!(taos_stmt2_exec(stmt2, ptr::null_mut()), 0);

            sp.entered.wait();
            sp.release.wait();
            while sp.cb_done.load(Ordering::SeqCst) == 0 {
                std::thread::yield_now();
            }

            let callback_result = sp.result_from_callback.load(Ordering::SeqCst);
            assert!(
                !callback_result.is_null(),
                "query result must be available while the async callback runs"
            );
            assert_eq!(callback_result, sp.res_ptr.load(Ordering::SeqCst));

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1784000078");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_async_reject_concurrent_exec() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783576905",
                    "create database test_1783576905",
                    "use test_1783576905",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 7)",
                ],
            );

            let sp = new_sync_points();
            let stmt2 = prep_async_query_stmt(taos, &sp);

            assert_eq!(taos_stmt2_exec(stmt2, ptr::null_mut()), 0);

            sp.entered.wait();

            let code = taos_stmt2_exec(stmt2, ptr::null_mut());
            assert_ne!(code, 0, "second async exec while in flight must fail");

            sp.release.wait();
            while sp.cb_done.load(Ordering::SeqCst) == 0 {
                std::thread::yield_now();
            }

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1783576905");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_async_exec_without_prepare_sync_error() {
        static ASYNC_NO_PREPARE_CB_CALLED: AtomicI64 = AtomicI64::new(0);

        unsafe {
            extern "C" fn fp(_userdata: *mut c_void, _res: *mut TAOS_RES, _code: c_int) {
                ASYNC_NO_PREPARE_CB_CALLED.store(1, Ordering::SeqCst);
            }

            let taos = test_connect();
            let mut option = TAOS_STMT2_OPTION {
                reqid: 6001,
                singleStbInsert: false,
                singleTableBindOnce: false,
                asyncExecFn: fp,
                userdata: ptr::null_mut(),
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());

            let code = taos_stmt2_exec(stmt2, ptr::null_mut());
            assert_ne!(
                code, 0,
                "async exec without prepare must fail synchronously"
            );

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert!(err.contains("taos_stmt2_prepare is not called"), "{err}");

            std::thread::sleep(std::time::Duration::from_millis(200));
            assert_eq!(
                ASYNC_NO_PREPARE_CB_CALLED.load(Ordering::SeqCst),
                0,
                "no async task/callback should be spawned on sync error"
            );

            assert_eq!(taos_stmt2_close(stmt2), 0);
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_result_on_insert_returns_null() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783578433",
                    "create database test_1783578433",
                    "use test_1783578433",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            assert_eq!(taos_stmt2_prepare(stmt2, sql.as_ptr(), 0), 0);

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);
            let mut buffer = vec![10i32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);
            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };
            assert_eq!(taos_stmt2_bind_param(stmt2, &mut bindv, -1), 0);

            let mut affected_rows = 0;
            assert_eq!(taos_stmt2_exec(stmt2, &mut affected_rows), 0);

            let res = taos_stmt2_result(stmt2);
            assert!(res.is_null(), "result on insert stmt must be NULL");
            let errstr = CStr::from_ptr(taos_stmt2_error(stmt2));
            assert!(
                !errstr.to_bytes().is_empty(),
                "insert result should set an error message"
            );

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1783578433");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_async_exec_error_path() {
        static ASYNC_ERR_CODE: AtomicI64 = AtomicI64::new(0);
        static ASYNC_CB_CALLED: AtomicBool = AtomicBool::new(false);

        unsafe {
            extern "C" fn fp(_userdata: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                ASYNC_ERR_CODE.store(code as i64, Ordering::SeqCst);
                assert!(res.is_null());
                ASYNC_CB_CALLED.store(true, Ordering::SeqCst);
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783578589",
                    "create database test_1783578589",
                    "use test_1783578589",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 5)",
                ],
            );

            let mut option = TAOS_STMT2_OPTION {
                reqid: 5001,
                singleStbInsert: false,
                singleTableBindOnce: false,
                asyncExecFn: fp,
                userdata: ptr::null_mut(),
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 > ?";
            assert_eq!(taos_stmt2_prepare(stmt2, sql.as_ptr(), 0), 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);
            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };
            assert_eq!(taos_stmt2_bind_param(stmt2, &mut bindv, -1), 0);

            test_exec(taos, "drop table t0");

            assert_eq!(taos_stmt2_exec(stmt2, ptr::null_mut()), 0);

            let mut waited = 0;
            while !ASYNC_CB_CALLED.load(Ordering::SeqCst) && waited < 60_000 {
                std::thread::sleep(std::time::Duration::from_millis(50));
                waited += 50;
            }
            assert!(
                ASYNC_CB_CALLED.load(Ordering::SeqCst),
                "async callback not invoked within 60s"
            );

            assert_ne!(ASYNC_ERR_CODE.load(Ordering::SeqCst), 0);

            let res = taos_stmt2_result(stmt2);
            assert!(res.is_null());

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1783578589");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_close_waits_async_callback() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783581062",
                    "create database test_1783581062",
                    "use test_1783581062",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 9)",
                ],
            );

            let sp = new_sync_points();
            let stmt2 = prep_async_query_stmt(taos, &sp);

            assert_eq!(taos_stmt2_exec(stmt2, ptr::null_mut()), 0);

            sp.entered.wait();

            let stmt2_ptr = SafePtr(stmt2);
            let close_returned = Arc::new(AtomicI64::new(0));
            let close_returned2 = close_returned.clone();
            let handle = std::thread::spawn(move || {
                let stmt2 = stmt2_ptr;
                let code = taos_stmt2_close(stmt2.0);
                assert_eq!(code, 0);
                close_returned2.store(1, Ordering::SeqCst);
            });

            std::thread::sleep(std::time::Duration::from_millis(300));
            assert_eq!(
                close_returned.load(Ordering::SeqCst),
                0,
                "close must not return while callback is blocked"
            );

            sp.release.wait();
            handle.join().unwrap();
            assert_eq!(close_returned.load(Ordering::SeqCst), 1);
            assert_eq!(sp.cb_done.load(Ordering::SeqCst), 1);

            test_exec(taos, "drop database if exists test_1783581062");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_async_query_result_owned_by_stmt() {
        static OWNED_FETCH_OK: AtomicI64 = AtomicI64::new(0);
        static OWNED_CODE: AtomicI64 = AtomicI64::new(-1);

        unsafe {
            extern "C" fn fp(_userdata: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    OWNED_CODE.store(code as i64, Ordering::SeqCst);
                    if !res.is_null() {
                        let row = taos_fetch_row(res);
                        if !row.is_null() {
                            OWNED_FETCH_OK.store(1, Ordering::SeqCst);
                        }
                    }
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783581251",
                    "create database test_1783581251",
                    "use test_1783581251",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 11)",
                    "insert into t0 values(1739762261438, 22)",
                ],
            );

            let mut option = TAOS_STMT2_OPTION {
                reqid: 4001,
                singleStbInsert: false,
                singleTableBindOnce: false,
                asyncExecFn: fp,
                userdata: ptr::null_mut(),
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 > ?";
            assert_eq!(taos_stmt2_prepare(stmt2, sql.as_ptr(), 0), 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);
            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };
            assert_eq!(taos_stmt2_bind_param(stmt2, &mut bindv, -1), 0);

            assert_eq!(taos_stmt2_exec(stmt2, ptr::null_mut()), 0);

            std::thread::sleep(std::time::Duration::from_secs(1));

            assert_eq!(OWNED_CODE.load(Ordering::SeqCst), 0);
            assert_eq!(OWNED_FETCH_OK.load(Ordering::SeqCst), 1);

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1783581251");
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

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739876374");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_result_same_pointer() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783581418",
                    "create database test_1783581418",
                    "use test_1783581418",
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

            let res1 = taos_stmt2_result(stmt2);
            assert!(!res1.is_null());
            let res2 = taos_stmt2_result(stmt2);
            assert!(!res2.is_null());
            assert_eq!(res1, res2);

            let row = taos_fetch_row(res1);
            assert!(!row.is_null());

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database if exists test_1783581418");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_close_frees_result() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783581482",
                    "create database test_1783581482",
                    "use test_1783581482",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 1)",
                    "insert into t0 values(1739762261438, 2)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 >= ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);
            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };
            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);

            let res = taos_stmt2_result(stmt2);
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database if exists test_1783581482");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_rebind_clears_prev_result() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783581523",
                    "create database test_1783581523",
                    "use test_1783581523",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 10)",
                    "insert into t0 values(1739762261438, 20)",
                    "insert into t0 values(1739762261439, 30)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![25];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);
            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };
            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);

            let res1 = taos_stmt2_result(stmt2);
            assert!(!res1.is_null());
            let row = taos_fetch_row(res1);
            assert!(!row.is_null());
            let fields = taos_fetch_fields(res1);
            let num_fields = taos_num_fields(res1);
            let mut buf = vec![0 as c_char; 128];
            taos_print_row(buf.as_mut_ptr(), row, fields, num_fields);
            let text = CStr::from_ptr(buf.as_ptr()).to_str().unwrap().to_string();
            assert!(text.contains("30"), "round1 row: {text}");
            let row = taos_fetch_row(res1);
            assert!(row.is_null());

            let mut buffer = vec![5];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);
            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };
            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);

            let res2 = taos_stmt2_result(stmt2);
            assert!(!res2.is_null());
            let mut cnt = 0;
            loop {
                let row = taos_fetch_row(res2);
                if row.is_null() {
                    break;
                }
                cnt += 1;
            }
            assert_eq!(cnt, 3, "round2 should match 3 rows");

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database if exists test_1783581523");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_insert_and_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1757667641",
                    "create database test_1757667641",
                    "use test_1757667641",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![45i32, 46, -45, 0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 4);

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![45];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
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
            assert_eq!(num_fields, 2);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1757667641");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_stmt2_blob() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1753168041",
                    "create database test_1753168041",
                    "use test_1753168041",
                    "create table t0 (ts timestamp, c1 int, c2 blob)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![1, 2, 3, 4];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut buffer = "hello".as_bytes().to_vec();
            buffer.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]);
            let mut length = vec![0, 0, 5, 4];
            let mut is_null = vec![1, 0, 0, 0];
            let c2 = new_bind!(Ty::Blob, buffer, length, is_null);

            let mut col = vec![ts, c1, c2];
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
            assert_eq!(affected_rows, 4);

            let sql = c"select * from t0 where ts > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![1739521477830i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut col = vec![ts];
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

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 3);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1739521477831 1 NULL",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1739521477832 2 \\x",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1739521477833 3 \\x68656C6C6F",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1739521477834 4 \\x12345678",
            );

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database test_1753168041");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_stmt2_decimal_bind_strings() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1776764597",
                    "create database test_1776764597",
                    "use test_1776764597",
                    "create table t0 (ts timestamp, c1 decimal(10, 2), c2 decimal(20, 10), c3 decimal(10, 2))",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?, ?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut ts_buffer = vec![1726803356466i64, 1726803356467, 1726803356468];
            let mut ts_length = vec![8, 8, 8];
            let mut ts_is_null = vec![0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, ts_buffer, ts_length, ts_is_null);

            let mut c1_buffer = b"99.98761.0234".to_vec();
            let mut c1_length = vec![7, 6, 0];
            let mut c1_is_null = vec![0, 0, 1];
            let c1 = new_bind!(Ty::Decimal64, c1_buffer, c1_length, c1_is_null);

            let mut c2_buffer = b"1234567890.12345678901231.23e+5".to_vec();
            let mut c2_length = vec![24, 7, 0];
            let mut c2_is_null = vec![0, 0, 1];
            let c2 = new_bind!(Ty::Decimal, c2_buffer, c2_length, c2_is_null);

            let mut c3_is_null = vec![1, 1, 1];
            let c3 = TAOS_STMT2_BIND {
                buffer_type: Ty::Decimal64 as _,
                buffer: ptr::null_mut(),
                length: ptr::null_mut(),
                is_null: c3_is_null.as_mut_ptr(),
                num: c3_is_null.len() as _,
            };

            let mut col = vec![ts, c1, c2, c3];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 3);

            let sql = c"select * from t0 where ts >= ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut ts_buffer = vec![1726803356466i64];
            let mut ts_length = vec![8];
            let mut ts_is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, ts_buffer, ts_length, ts_is_null);

            let mut col = vec![ts];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
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

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 4);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1726803356466 99.99 1234567890.1234567890 NULL",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1726803356467 1.02 123000.0000000000 NULL",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1726803356468 NULL NULL NULL",
            );

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1776764597");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_stmt2_decimal_integer_overflow_error_passthrough() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1776765040",
                    "create database test_1776765040",
                    "use test_1776765040",
                    "create table t0 (ts timestamp, c1 decimal(4, 2))",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut ts_buffer = vec![1726803356466i64];
            let mut ts_length = vec![8];
            let mut ts_is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, ts_buffer, ts_length, ts_is_null);

            let mut c1_buffer = b"100.1".to_vec();
            let mut c1_length = vec![5];
            let mut c1_is_null = vec![0];
            let c1 = new_bind!(Ty::Decimal64, c1_buffer, c1_length, c1_is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_ne!(code, 0);

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert!(err.contains("Decimal value overflow"));

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1776765040");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_stmt2_decimal_rounding_overflow_error_passthrough() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1776765391",
                    "create database test_1776765391",
                    "use test_1776765391",
                    "create table t0 (ts timestamp, c1 decimal(3, 1))",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut ts_buffer = vec![1726803356466i64];
            let mut ts_length = vec![8];
            let mut ts_is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, ts_buffer, ts_length, ts_is_null);

            let mut c1_buffer = b"99.99".to_vec();
            let mut c1_length = vec![5];
            let mut c1_is_null = vec![0];
            let c1 = new_bind!(Ty::Decimal64, c1_buffer, c1_length, c1_is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_ne!(code, 0);

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert!(err.contains("Decimal value overflow"));

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1776765391");
            taos_close(taos);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_conn() -> anyhow::Result<()> {
        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, _ctx| {
                if let Message::Text(text) = msg {
                    if text.contains("stmt") {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                } else if let Message::Binary(bytes) = msg {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    if action == 9 {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8818", "ws://localhost:6041/ws", intercept_fn).await;

        let n = 10;
        let mut tasks = Vec::with_capacity(n);
        for i in 0..n {
            tasks.push(tokio::spawn(async move {
                unsafe {
                    let taos = taos_connect(
                        c"localhost".as_ptr(),
                        ptr::null(),
                        ptr::null(),
                        ptr::null(),
                        8818,
                    );
                    assert!(!taos.is_null());

                    let db = format!("test_{}", 1760161589 + i);

                    test_exec_many(
                        taos,
                        &[
                            &format!("drop database if exists {db}"),
                            &format!("create database {db}"),
                            &format!("use {db}"),
                            "create table t0 (ts timestamp, c1 int)",
                        ],
                    );

                    let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
                    assert!(!stmt2.is_null());

                    let sql = CString::new(format!("insert into {db}.t0 values(?, ?)")).unwrap();
                    let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
                    assert_eq!(code, 0);

                    let mut buffer = vec![1739521477831i64];
                    let mut length = vec![8];
                    let mut is_null = vec![0];
                    let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

                    let mut buffer = vec![1i32];
                    let mut length = vec![4];
                    let mut is_null = vec![0];
                    let c1 = new_bind!(Ty::Int, buffer, length, is_null);

                    let mut col = vec![ts, c1];
                    let mut cols = vec![col.as_mut_ptr()];
                    let mut bindv = TAOS_STMT2_BINDV {
                        count: 1,
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

                    let sql = CString::new(format!("select * from {db}.t0 where c1 = ?")).unwrap();
                    let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
                    assert_eq!(code, 0);

                    let mut buffer = vec![1];
                    let mut length = vec![4];
                    let mut is_null = vec![0];
                    let c1 = new_bind!(Ty::Int, buffer, length, is_null);

                    let mut col = vec![c1];
                    let mut cols = vec![col.as_mut_ptr()];
                    let mut bindv = TAOS_STMT2_BINDV {
                        count: 1,
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

                    let code = taos_stmt2_close(stmt2);
                    assert_eq!(code, 0);

                    test_exec(taos, format!("drop database if exists {db}"));
                    taos_close(taos);
                }

                Ok::<_, anyhow::Error>(())
            }));
        }

        let results = try_join_all(tasks).await.unwrap();
        for res in results {
            res?;
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_stmt2() -> anyhow::Result<()> {
        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, _ctx| {
                if let Message::Text(text) = msg {
                    if text.contains("stmt") {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                } else if let Message::Binary(bytes) = msg {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    if action == 9 {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8819", "ws://localhost:6041/ws", intercept_fn).await;

        let taos = unsafe {
            taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                8819,
            )
        };
        assert!(!taos.is_null());

        test_exec_many(
            taos,
            &[
                "drop database if exists test_1760164460",
                "create database test_1760164460",
                "use test_1760164460",
                "create table t0 (ts timestamp, c1 int)",
            ],
        );

        let taos_ptr = SafePtr(taos);

        let n = 10;
        let mut tasks = Vec::with_capacity(n);
        for i in 0..n {
            let taos_ptr = taos_ptr.clone();
            tasks.push(tokio::spawn(async move {
                unsafe {
                    let taos_ptr = taos_ptr;
                    let taos = taos_ptr.0;
                    let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
                    assert!(!stmt2.is_null());

                    let sql = c"insert into test_1760164460.t0 values(?, ?)";
                    let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
                    assert_eq!(code, 0);

                    let mut buffer = vec![1739521477831i64];
                    let mut length = vec![8];
                    let mut is_null = vec![0];
                    let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

                    let mut buffer = vec![1i32];
                    let mut length = vec![4];
                    let mut is_null = vec![0];
                    let c1 = new_bind!(Ty::Int, buffer, length, is_null);

                    let mut col = vec![ts, c1];
                    let mut cols = vec![col.as_mut_ptr()];
                    let mut bindv = TAOS_STMT2_BINDV {
                        count: 1,
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

                    let sql = c"select * from test_1760164460.t0 where c1 = ?";
                    let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
                    assert_eq!(code, 0);

                    let mut buffer = vec![1];
                    let mut length = vec![4];
                    let mut is_null = vec![0];
                    let c1 = new_bind!(Ty::Int, buffer, length, is_null);

                    let mut col = vec![c1];
                    let mut cols = vec![col.as_mut_ptr()];
                    let mut bindv = TAOS_STMT2_BINDV {
                        count: 1,
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

                    let code = taos_stmt2_close(stmt2);
                    assert_eq!(code, 0);
                }

                Ok::<_, anyhow::Error>(())
            }));
        }

        let results = try_join_all(tasks).await.unwrap();
        for res in results {
            res?;
        }

        test_exec(taos, "drop database if exists test_1760164460");
        unsafe { taos_close(taos) };

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_stmt2() -> anyhow::Result<()> {
        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, _ctx| {
                if let Message::Text(text) = msg {
                    if text.contains("stmt") {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                } else if let Message::Binary(bytes) = msg {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    if action == 9 {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8821", "ws://localhost:6041/ws", intercept_fn).await;

        unsafe {
            let taos = taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                8821,
            );
            assert!(!taos.is_null());

            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1760167825",
                    "create database test_1760167825",
                    "use test_1760167825",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into test_1760167825.t0 values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let n = 10;
            let mut ts_buffer = Vec::with_capacity(n);
            let mut c1_buffer = Vec::with_capacity(n);
            for i in 0..n {
                ts_buffer.push(1726803356466 + i as i64);
                c1_buffer.push(100 + i as i32);
            }

            let mut length = vec![8; n];
            let mut is_null = vec![0; n];
            let ts = new_bind!(Ty::Timestamp, ts_buffer, length, is_null);

            let mut length = vec![4; n];
            let mut is_null = vec![0; n];
            let c1 = new_bind!(Ty::Int, c1_buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            for _ in 0..1000 {
                let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
                assert_eq!(code, 0);

                let mut affected_rows = 0;
                let code = taos_stmt2_exec(stmt2, &mut affected_rows);
                assert_eq!(code, 0);
                assert_eq!(affected_rows, 10);
            }

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1760167825");
            taos_close(taos);
        }

        Ok(())
    }

    #[test]
    fn test_taos_stmt2_get_fields_insert() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1764847750",
                    "create database test_1764847750",
                    "use test_1764847750",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut count = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt2_get_fields(stmt2, &mut count, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(count, 4);
            assert!(!fields.is_null());

            let field_slice = slice::from_raw_parts(fields, count as usize);
            let field_types = field_slice.iter().map(|f| f.field_type).collect::<Vec<_>>();
            assert_eq!(field_types, vec![4, 2, 1, 1]);

            taos_stmt2_free_fields(stmt2, fields);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database if exists test_1764847750");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_get_fields_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1765161954",
                    "create database test_1765161954",
                    "use test_1765161954",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"select * from s0 where tbname = ? and t1 = ? and ts > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut count = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt2_get_fields(stmt2, &mut count, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(count, 3);
            assert!(fields.is_null());

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database if exists test_1765161954");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_taos_stmt2_prepare_placeholder_tbname_rejected() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783597162",
                    "create database test_1783597162",
                    "use test_1783597162",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into test_1783597162.? (ts, c1) values (?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_ne!(code, 0, "prepare with table name placeholder must fail");

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert!(err.contains("table name placeholder"), "{err}");

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1783597162");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_taos_stmt2_prepare_literal_sql() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783739807",
                    "create database test_1783739807",
                    "use test_1783739807",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values (now, 1)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 1);

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1783739807");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_taos_stmt2_get_fields_literal_insert_free_null_is_noop() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1784000254",
                    "create database test_1784000254",
                    "use test_1784000254",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values (now, 1)";
            assert_eq!(taos_stmt2_prepare(stmt2, sql.as_ptr(), 0), 0);

            let mut count = -1;
            let mut fields = ptr::null_mut();
            let code = taos_stmt2_get_fields(stmt2, &mut count, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(count, 0);
            assert!(fields.is_null());

            taos_stmt2_free_fields(stmt2, fields);

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1784000254");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_taos_stmt2_bind_after_rejected_prepare_fails() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1784007841",
                    "create database test_1784007841",
                    "use test_1784007841",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into test_1784007841.? (ts, c1) values (?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_ne!(code, 0);

            let mut insert = -1;
            let code = taos_stmt2_is_insert(stmt2, &mut insert);
            assert_ne!(code, 0);

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);
            let mut col = vec![ts];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };
            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_ne!(code, 0);

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1784007841");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_bind_without_prepare_returns_error() {
        unsafe {
            let taos = test_connect();
            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let mut buffer = vec![1739763276172i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);
            let mut col = vec![ts];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_ne!(code, 0);

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert!(err.contains("taos_stmt2_prepare is not called"), "{err}");

            assert_eq!(taos_stmt2_close(stmt2), 0);
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_async_error_readable_via_error() {
        static ERR_CB_DONE: AtomicBool = AtomicBool::new(false);
        static ERR_CB_CODE: AtomicI64 = AtomicI64::new(0);
        static ERR_STMT2: AtomicPtr<c_void> = AtomicPtr::new(ptr::null_mut());

        unsafe {
            extern "C" fn fp(_userdata: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert!(res.is_null());
                    ERR_CB_CODE.store(code as i64, Ordering::SeqCst);
                    let stmt = ERR_STMT2.load(Ordering::SeqCst);
                    let err = CStr::from_ptr(taos_stmt2_error(stmt)).to_str().unwrap();
                    assert!(!err.is_empty());
                    ERR_CB_DONE.store(true, Ordering::SeqCst);
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1783749398",
                    "create database test_1783749398",
                    "use test_1783749398",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 5)",
                ],
            );

            let mut option = TAOS_STMT2_OPTION {
                reqid: 7001,
                singleStbInsert: false,
                singleTableBindOnce: false,
                asyncExecFn: fp,
                userdata: ptr::null_mut(),
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());
            ERR_STMT2.store(stmt2 as _, Ordering::SeqCst);

            let sql = c"select * from t0 where c1 > ?";
            assert_eq!(taos_stmt2_prepare(stmt2, sql.as_ptr(), 0), 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);
            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };
            assert_eq!(taos_stmt2_bind_param(stmt2, &mut bindv, -1), 0);

            test_exec(taos, "drop table t0");

            assert_eq!(taos_stmt2_exec(stmt2, ptr::null_mut()), 0);

            let mut waited = 0;
            while !ERR_CB_DONE.load(Ordering::SeqCst) && waited < 60_000 {
                std::thread::sleep(std::time::Duration::from_millis(50));
                waited += 50;
            }
            assert!(ERR_CB_DONE.load(Ordering::SeqCst));
            assert_ne!(ERR_CB_CODE.load(Ordering::SeqCst), 0);

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert!(!err.is_empty());

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1783749398");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_error_prefers_async_error_over_stale_outer_error() {
        unsafe {
            let taos = test_connect();
            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let maybe_err = &mut *(stmt2 as *mut TaosMaybeError<TaosStmt2>);
            let lifecycle = maybe_err.deref().unwrap().lifecycle.clone();
            let (generation, started) = lifecycle.begin_async();
            assert!(started);
            lifecycle.publish_error(generation, maybe_err, None);
            maybe_err.with_err(Some(TaosError::new(Code::FAILED, "stale outer error")));
            lifecycle.complete_async_error(
                generation,
                TaosError::new(Code::FAILED, "current async error"),
            );
            lifecycle.finish_async();

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert_eq!(err, "current async error");

            assert_eq!(taos_stmt2_close(stmt2), 0);
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_error_preserves_newer_sync_error() {
        unsafe {
            let taos = test_connect();
            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let maybe_err = &mut *(stmt2 as *mut TaosMaybeError<TaosStmt2>);
            let lifecycle = maybe_err.deref().unwrap().lifecycle.clone();
            let (generation, started) = lifecycle.begin_async();
            assert!(started);
            lifecycle.publish_error(generation, maybe_err, None);
            lifecycle.complete_async_error(
                generation,
                TaosError::new(Code::FAILED, "stale async error"),
            );
            lifecycle.finish_async();

            assert_ne!(taos_stmt2_is_insert(stmt2, ptr::null_mut()), 0);

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert_eq!(err, "insert is null");

            assert_eq!(taos_stmt2_close(stmt2), 0);
            taos_close(taos);
        }
    }

    unsafe fn seed_completed_async_error(stmt: *mut TAOS_STMT2) {
        let maybe_err = &mut *(stmt as *mut TaosMaybeError<TaosStmt2>);
        let lifecycle = maybe_err.deref().unwrap().lifecycle.clone();
        let (generation, started) = lifecycle.begin_async();
        assert!(started);
        lifecycle.publish_error(generation, maybe_err, None);
        lifecycle.complete_async_error(
            generation,
            TaosError::new(Code::FAILED, "stale async error"),
        );
        lifecycle.finish_async();
    }

    extern "C" fn ignore_stmt2_callback(_param: *mut c_void, _res: *mut TAOS_RES, _code: c_int) {}

    #[test]
    fn test_taos_stmt2_bind_param_column_error_supersedes_async_error() {
        unsafe {
            let taos = test_connect();
            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());
            seed_completed_async_error(stmt2);

            assert_ne!(taos_stmt2_bind_param_column(stmt2, ptr::null_mut()), 0);

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert_eq!(err, "taos_stmt2_bind_param_column is not implemented");

            assert_eq!(taos_stmt2_close(stmt2), 0);
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_bind_param_column_with_stmt_preserves_thread_local_error() {
        unsafe {
            let taos = test_connect();
            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            assert_ne!(taos_stmt2_bind_param_column(stmt2, ptr::null_mut()), 0);

            let err = CStr::from_ptr(taos_errstr(ptr::null_mut()))
                .to_str()
                .unwrap();
            assert_eq!(err, "taos_stmt2_bind_param_column is not implemented");

            assert_eq!(taos_stmt2_close(stmt2), 0);
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_bind_param_column_a_error_supersedes_async_error() {
        unsafe {
            let taos = test_connect();
            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());
            seed_completed_async_error(stmt2);

            assert_ne!(
                taos_stmt2_bind_param_column_a(
                    stmt2,
                    ptr::null_mut(),
                    ignore_stmt2_callback,
                    ptr::null_mut(),
                ),
                0
            );

            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert_eq!(err, "taos_stmt2_bind_param_column_a is not implemented");

            assert_eq!(taos_stmt2_close(stmt2), 0);
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_taos_stmt2_failed_prepare_invalidates_previous_state() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1784040734",
                    "create database test_1784040734",
                    "use test_1784040734",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            assert_eq!(
                taos_stmt2_prepare(stmt2, c"select * from t0 where c1 > ?".as_ptr(), 0),
                0
            );

            let mut count = 0;
            assert_eq!(taos_stmt2_get_fields(stmt2, &mut count, ptr::null_mut()), 0);
            assert_eq!(count, 1);

            assert_ne!(
                taos_stmt2_prepare(stmt2, c"select from".as_ptr(), 0),
                0,
                "the second prepare must fail"
            );

            let mut is_insert = -1;
            assert_ne!(taos_stmt2_is_insert(stmt2, &mut is_insert), 0);

            count = -1;
            assert_ne!(taos_stmt2_get_fields(stmt2, &mut count, ptr::null_mut()), 0);

            let mut value = vec![1_i32];
            let mut length = vec![std::mem::size_of::<i32>() as i32];
            let mut is_null = vec![0];
            let value_bind = new_bind!(Ty::Int, value, length, is_null);
            let mut columns = vec![value_bind];
            let mut column_rows = vec![columns.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: column_rows.as_mut_ptr(),
            };
            assert_ne!(taos_stmt2_bind_param(stmt2, &mut bindv, -1), 0);

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1784040734");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_taos_stmt2_async_success_clears_not_ready_error() {
        static CALLBACK_DONE: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        static CALLBACK_CODE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

        unsafe {
            extern "C" fn callback(_userdata: *mut c_void, _res: *mut TAOS_RES, code: c_int) {
                CALLBACK_CODE.store(code, std::sync::atomic::Ordering::SeqCst);
                CALLBACK_DONE.store(true, std::sync::atomic::Ordering::SeqCst);
            }

            CALLBACK_DONE.store(false, std::sync::atomic::Ordering::SeqCst);
            CALLBACK_CODE.store(-1, std::sync::atomic::Ordering::SeqCst);

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1784040791",
                    "create database test_1784040791",
                    "use test_1784040791",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(now, 1)",
                ],
            );

            let mut option = TAOS_STMT2_OPTION {
                reqid: 1784040791,
                singleStbInsert: false,
                singleTableBindOnce: false,
                asyncExecFn: callback,
                userdata: ptr::null_mut(),
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());
            assert_eq!(
                taos_stmt2_prepare(stmt2, c"select * from t0 where c1 > ?".as_ptr(), 0),
                0
            );

            let mut value = vec![0_i32];
            let mut length = vec![std::mem::size_of::<i32>() as i32];
            let mut is_null = vec![0];
            let value_bind = new_bind!(Ty::Int, value, length, is_null);
            let mut columns = vec![value_bind];
            let mut column_rows = vec![columns.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: column_rows.as_mut_ptr(),
            };
            assert_eq!(taos_stmt2_bind_param(stmt2, &mut bindv, -1), 0);

            assert_eq!(taos_stmt2_exec(stmt2, ptr::null_mut()), 0);
            if taos_stmt2_result(stmt2).is_null() {
                let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
                assert!(err.contains("not available yet"), "{err}");
            }

            for _ in 0..1_000 {
                if CALLBACK_DONE.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            assert_eq!(CALLBACK_CODE.load(std::sync::atomic::Ordering::SeqCst), 0);
            let err = CStr::from_ptr(taos_stmt2_error(stmt2)).to_str().unwrap();
            assert!(
                !err.contains("not available yet"),
                "successful async execution must clear stale outer error: {err}"
            );

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1784040791");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_rejected_placeholder_prepare_does_not_recover() -> anyhow::Result<()> {
        let restart_next_request = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let intercept_fn: InterceptFn = {
            let restart_next_request = restart_next_request.clone();
            Arc::new(move |_msg, _ctx| {
                if restart_next_request.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    ProxyAction::Restart
                } else {
                    ProxyAction::Forward
                }
            })
        };
        let _proxy = WsProxy::start("127.0.0.1:8822", "ws://localhost:6041/ws", intercept_fn).await;

        unsafe {
            let taos = taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                8822,
            );
            assert!(!taos.is_null());

            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1784040918",
                    "create database test_1784040918",
                    "use test_1784040918",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());
            assert_ne!(
                taos_stmt2_prepare(
                    stmt2,
                    c"insert into test_1784040918.? (ts, c1) values (?, ?)".as_ptr(),
                    0,
                ),
                0
            );

            restart_next_request.store(true, std::sync::atomic::Ordering::SeqCst);
            let result = taos_query(taos, c"select 1".as_ptr());
            let mut reconnected = !result.is_null();
            if reconnected {
                taos_free_result(result);
            }
            for _ in 0..100 {
                if reconnected {
                    break;
                }
                let result = taos_query(taos, c"select 1".as_ptr());
                if !result.is_null() {
                    taos_free_result(result);
                    reconnected = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            assert!(
                !restart_next_request.load(std::sync::atomic::Ordering::SeqCst),
                "proxy restart was not triggered"
            );
            assert!(
                reconnected,
                "connection did not recover after proxy restart"
            );

            let mut is_insert = -1;
            assert_ne!(taos_stmt2_is_insert(stmt2, &mut is_insert), 0);

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1784040918");
            taos_close(taos);
        }

        Ok(())
    }
}
