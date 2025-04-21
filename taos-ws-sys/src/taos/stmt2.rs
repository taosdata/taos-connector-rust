use std::ffi::{c_char, c_int, c_ulong, c_void};

use crate::taos::{__taos_async_fn_t, driver, CAPI, TAOS, TAOS_RES};
use crate::ws::stmt2;

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
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_init(
    taos: *mut TAOS,
    option: *mut TAOS_STMT2_OPTION,
) -> *mut TAOS_STMT2 {
    if driver() {
        stmt2::taos_stmt2_init(taos, option)
    } else {
        (CAPI.stmt2_api.taos_stmt2_init)(taos, option)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_prepare(
    stmt: *mut TAOS_STMT2,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_prepare(stmt, sql, length)
    } else {
        (CAPI.stmt2_api.taos_stmt2_prepare)(stmt, sql, length)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_bind_param(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_bind_param(stmt, bindv, col_idx)
    } else {
        (CAPI.stmt2_api.taos_stmt2_bind_param)(stmt, bindv, col_idx)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_bind_param_a(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_bind_param_a(stmt, bindv, col_idx, fp, param)
    } else {
        (CAPI.stmt2_api.taos_stmt2_bind_param_a)(stmt, bindv, col_idx, fp, param)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_exec(
    stmt: *mut TAOS_STMT2,
    affected_rows: *mut c_int,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_exec(stmt, affected_rows)
    } else {
        (CAPI.stmt2_api.taos_stmt2_exec)(stmt, affected_rows)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_close(stmt: *mut TAOS_STMT2) -> c_int {
    if driver() {
        stmt2::taos_stmt2_close(stmt)
    } else {
        (CAPI.stmt2_api.taos_stmt2_close)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_is_insert(stmt: *mut TAOS_STMT2, insert: *mut c_int) -> c_int {
    if driver() {
        stmt2::taos_stmt2_is_insert(stmt, insert)
    } else {
        (CAPI.stmt2_api.taos_stmt2_is_insert)(stmt, insert)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_get_fields(
    stmt: *mut TAOS_STMT2,
    count: *mut c_int,
    fields: *mut *mut TAOS_FIELD_ALL,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_get_fields(stmt, count, fields)
    } else {
        (CAPI.stmt2_api.taos_stmt2_get_fields)(stmt, count, fields)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_free_fields(
    stmt: *mut TAOS_STMT2,
    fields: *mut TAOS_FIELD_ALL,
) {
    if driver() {
        stmt2::taos_stmt2_free_fields(stmt, fields);
    } else {
        (CAPI.stmt2_api.taos_stmt2_free_fields)(stmt, fields);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_result(stmt: *mut TAOS_STMT2) -> *mut TAOS_RES {
    if driver() {
        stmt2::taos_stmt2_result(stmt)
    } else {
        (CAPI.stmt2_api.taos_stmt2_result)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_error(stmt: *mut TAOS_STMT2) -> *mut c_char {
    if driver() {
        stmt2::taos_stmt2_error(stmt)
    } else {
        (CAPI.stmt2_api.taos_stmt2_error)(stmt)
    }
}
