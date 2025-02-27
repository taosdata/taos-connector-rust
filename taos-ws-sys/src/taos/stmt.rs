use std::ffi::{c_char, c_int, c_ulong, c_void};
use std::sync::atomic::Ordering;

use tracing::instrument;

use crate::taos::{CAPI, DRIVER, TAOS, TAOS_RES};
use crate::ws::stmt;

#[allow(non_camel_case_types)]
pub type TAOS_STMT = c_void;

#[repr(C)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub struct TAOS_STMT_OPTIONS {
    pub reqId: i64,
    pub singleStbInsert: bool,
    pub singleTableBindOnce: bool,
}

#[repr(C)]
#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub struct TAOS_MULTI_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub buffer_length: usize,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: c_int,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD_E {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_init(taos: *mut TAOS) -> *mut TAOS_STMT {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_init(taos)
    } else {
        (CAPI.stmt_api.taos_stmt_init)(taos)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_init_with_reqid(taos: *mut TAOS, reqid: i64) -> *mut TAOS_STMT {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_init_with_reqid(taos, reqid)
    } else {
        (CAPI.stmt_api.taos_stmt_init_with_reqid)(taos, reqid)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_init_with_options(
    taos: *mut TAOS,
    options: *mut TAOS_STMT_OPTIONS,
) -> *mut TAOS_STMT {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_init_with_options(taos, options)
    } else {
        (CAPI.stmt_api.taos_stmt_init_with_options)(taos, options)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_prepare(
    stmt: *mut TAOS_STMT,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_prepare(stmt, sql, length)
    } else {
        (CAPI.stmt_api.taos_stmt_prepare)(stmt, sql, length)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_tbname_tags(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_set_tbname_tags(stmt, name, tags)
    } else {
        (CAPI.stmt_api.taos_stmt_set_tbname_tags)(stmt, name, tags)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_set_tbname(stmt, name)
    } else {
        (CAPI.stmt_api.taos_stmt_set_tbname)(stmt, name)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_tags(
    stmt: *mut TAOS_STMT,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_set_tags(stmt, tags)
    } else {
        (CAPI.stmt_api.taos_stmt_set_tags)(stmt, tags)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_sub_tbname(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_set_sub_tbname(stmt, name)
    } else {
        (CAPI.stmt_api.taos_stmt_set_sub_tbname)(stmt, name)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_get_tag_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_get_tag_fields(stmt, fieldNum, fields)
    } else {
        (CAPI.stmt_api.taos_stmt_get_tag_fields)(stmt, fieldNum, fields)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_get_col_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_get_col_fields(stmt, fieldNum, fields)
    } else {
        (CAPI.stmt_api.taos_stmt_get_col_fields)(stmt, fieldNum, fields)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_reclaim_fields(stmt: *mut TAOS_STMT, fields: *mut TAOS_FIELD_E) {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_reclaim_fields(stmt, fields);
    } else {
        (CAPI.stmt_api.taos_stmt_reclaim_fields)(stmt, fields);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_is_insert(stmt, insert)
    } else {
        (CAPI.stmt_api.taos_stmt_is_insert)(stmt, insert)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_num_params(stmt, nums)
    } else {
        (CAPI.stmt_api.taos_stmt_num_params)(stmt, nums)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_get_param(
    stmt: *mut TAOS_STMT,
    idx: c_int,
    r#type: *mut c_int,
    bytes: *mut c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_get_param(stmt, idx, r#type, bytes)
    } else {
        (CAPI.stmt_api.taos_stmt_get_param)(stmt, idx, r#type, bytes)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_bind_param(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_bind_param(stmt, bind)
    } else {
        (CAPI.stmt_api.taos_stmt_bind_param)(stmt, bind)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_bind_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_bind_param_batch(stmt, bind)
    } else {
        (CAPI.stmt_api.taos_stmt_bind_param_batch)(stmt, bind)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_bind_single_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
    colIdx: c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_bind_single_param_batch(stmt, bind, colIdx)
    } else {
        (CAPI.stmt_api.taos_stmt_bind_single_param_batch)(stmt, bind, colIdx)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_add_batch(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_add_batch)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_execute(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_execute)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_use_result(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_use_result)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_close(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_close)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_errstr(stmt: *mut TAOS_STMT) -> *mut c_char {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_errstr(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_errstr)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_affected_rows(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_affected_rows)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_affected_rows_once(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stmt::taos_stmt_affected_rows_once(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_affected_rows_once)(stmt)
    }
}
