#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void};
use std::ptr;

use crate::ws::query::{__taos_async_whitelist_fn_t, __taos_notify_fn_t, TAOS_DB_ROUTE_INFO};
use crate::ws::tmq::{tmq_raw_data, tmq_t};
use crate::ws::{TAOS, TAOS_FIELD, TAOS_RES, TSDB_OPTION_CONNECTION};

#[no_mangle]
pub extern "C" fn taos_init() -> c_int {
    0
}

#[no_mangle]
pub extern "C" fn taos_cleanup() {}

#[no_mangle]
pub extern "C" fn taos_connect_auth(
    ip: *const c_char,
    user: *const c_char,
    auth: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    ptr::null_mut()
}

#[no_mangle]
pub extern "C" fn taos_kill_query(taos: *mut TAOS) {}

#[no_mangle]
pub extern "C" fn taos_reset_current_db(taos: *mut TAOS) {}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_get_db_route_info(
    taos: *mut TAOS,
    db: *const c_char,
    dbInfo: *mut TAOS_DB_ROUTE_INFO,
) -> c_int {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_get_table_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *const c_char,
    vgId: *mut c_int,
) -> c_int {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_get_tables_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *mut *const c_char,
    tableNum: c_int,
    vgId: *mut c_int,
) -> c_int {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_load_table_info(taos: *mut TAOS, tableNameList: *const c_char) -> c_int {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_set_hb_quit(quitByKill: i8) {}

#[no_mangle]
pub extern "C" fn taos_set_notify_cb(
    taos: *mut TAOS,
    fp: __taos_notify_fn_t,
    param: *mut c_void,
    r#type: c_int,
) -> c_int {
    0
}

#[no_mangle]
pub extern "C" fn taos_set_conn_mode(taos: *mut TAOS, mode: c_int, value: c_int) -> c_int {
    0
}

#[no_mangle]
pub extern "C" fn tmq_get_connect(tmq: *mut tmq_t) -> *mut TAOS {
    ptr::null_mut()
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum SET_CONF_RET_CODE {
    SET_CONF_RET_SUCC = 0,
    SET_CONF_RET_ERR_PART = -1,
    SET_CONF_RET_ERR_INNER = -2,
    SET_CONF_RET_ERR_JSON_INVALID = -3,
    SET_CONF_RET_ERR_JSON_PARSE = -4,
    SET_CONF_RET_ERR_ONLY_ONCE = -5,
    SET_CONF_RET_ERR_TOO_LONG = -6,
}

pub const RET_MSG_LENGTH: usize = 1024;

#[repr(C)]
#[derive(Debug)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct setConfRet {
    pub retCode: SET_CONF_RET_CODE,
    pub retMsg: [c_char; RET_MSG_LENGTH],
}

#[no_mangle]
pub extern "C" fn taos_set_config(config: *const c_char) -> setConfRet {
    setConfRet {
        retCode: SET_CONF_RET_CODE::SET_CONF_RET_SUCC,
        retMsg: [0; RET_MSG_LENGTH],
    }
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TAOS_FIELD_T {
    TAOS_FIELD_COL = 1,
    TAOS_FIELD_TAG = 2,
    TAOS_FIELD_QUERY = 3,
    TAOS_FIELD_TBNAME = 4,
}

#[no_mangle]
pub extern "C" fn taos_fetch_whitelist_a(
    taos: *mut TAOS,
    fp: __taos_async_whitelist_fn_t,
    param: *mut c_void,
) {
}

#[no_mangle]
pub extern "C" fn tmq_get_raw(res: *mut TAOS_RES, raw: *mut tmq_raw_data) -> i32 {
    0
}

#[no_mangle]
pub extern "C" fn tmq_write_raw(taos: *mut TAOS, raw: tmq_raw_data) -> i32 {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_write_raw_block(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
) -> i32 {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_write_raw_block_with_reqid(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    reqid: i64,
) -> i32 {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_write_raw_block_with_fields(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
) -> i32 {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_write_raw_block_with_fields_with_reqid(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
    reqid: i64,
) -> i32 {
    0
}

#[no_mangle]
pub extern "C" fn tmq_free_raw(raw: tmq_raw_data) {}

#[no_mangle]
pub extern "C" fn tmq_get_json_meta(res: *mut TAOS_RES) -> *const c_char {
    ptr::null()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn tmq_free_json_meta(jsonMeta: *mut c_char) {}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn getBuildInfo() -> *const c_char {
    ptr::null()
}

#[no_mangle]
pub extern "C" fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
) -> c_int {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_write_crashinfo(signum: c_int, sigInfo: *mut c_void, context: *mut c_void) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_taos_set_config() {
        let res = taos_set_config(ptr::null());
        assert_eq!(res.retCode, SET_CONF_RET_CODE::SET_CONF_RET_SUCC);
        assert_eq!(res.retMsg, [0; RET_MSG_LENGTH]);
    }
}
