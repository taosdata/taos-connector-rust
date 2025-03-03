#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void};

use tracing::instrument;

use crate::ws::query::{
    __taos_async_whitelist_fn_t, __taos_notify_fn_t, setConfRet, TAOS_DB_ROUTE_INFO,
    TSDB_SERVER_STATUS,
};
use crate::ws::tmq::{tmq_raw_data, tmq_t};
use crate::ws::{TAOS, TAOS_FIELD, TAOS_RES, TSDB_OPTION_CONNECTION};

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_init() -> c_int {
    0
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_cleanup() {}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_connect_auth(
    ip: *const c_char,
    user: *const c_char,
    auth: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    todo!("taos_connect_auth");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_kill_query(taos: *mut TAOS) {}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_reset_current_db(taos: *mut TAOS) {
    todo!("taos_reset_current_db")
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_get_db_route_info(
    taos: *mut TAOS,
    db: *const c_char,
    dbInfo: *mut TAOS_DB_ROUTE_INFO,
) -> c_int {
    todo!("taos_get_db_route_info");
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_get_table_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *const c_char,
    vgId: *mut c_int,
) -> c_int {
    todo!("taos_get_table_vgId");
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_get_tables_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *mut *const c_char,
    tableNum: c_int,
    vgId: *mut c_int,
) -> c_int {
    todo!("taos_get_tables_vgId");
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_load_table_info(taos: *mut TAOS, tableNameList: *const c_char) -> c_int {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_set_hb_quit(quitByKill: i8) {}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_set_notify_cb(
    taos: *mut TAOS,
    fp: __taos_notify_fn_t,
    param: *mut c_void,
    r#type: c_int,
) -> c_int {
    0
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_set_conn_mode(taos: *mut TAOS, mode: c_int, value: c_int) -> c_int {
    0
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_get_connect(tmq: *mut tmq_t) -> *mut TAOS {
    std::ptr::null_mut()
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_set_config(config: *const c_char) -> setConfRet {
    todo!("taos_set_config");
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
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_fetch_whitelist_a(
    taos: *mut TAOS,
    fp: __taos_async_whitelist_fn_t,
    param: *mut c_void,
) {
    todo!("taos_fetch_whitelist_a");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_get_raw(res: *mut TAOS_RES, raw: *mut tmq_raw_data) -> i32 {
    todo!("tmq_get_raw");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_write_raw(taos: *mut TAOS, raw: tmq_raw_data) -> i32 {
    todo!("tmq_write_raw");
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_write_raw_block(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
) -> i32 {
    todo!("taos_write_raw_block");
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_write_raw_block_with_reqid(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    reqid: i64,
) -> i32 {
    todo!("taos_write_raw_block_with_reqid");
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_write_raw_block_with_fields(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
) -> i32 {
    todo!("taos_write_raw_block_with_fields");
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_write_raw_block_with_fields_with_reqid(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
    reqid: i64,
) -> i32 {
    todo!("taos_write_raw_block_with_fields_with_reqid");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_free_raw(raw: tmq_raw_data) {
    todo!("tmq_free_raw");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_get_json_meta(res: *mut TAOS_RES) -> *const c_char {
    todo!("tmq_get_json_meta");
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_free_json_meta(jsonMeta: *mut c_char) {
    todo!("tmq_free_json_meta");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_check_server_status(
    fqdn: *const c_char,
    port: i32,
    details: *mut c_char,
    maxlen: i32,
) -> TSDB_SERVER_STATUS {
    todo!("taos_check_server_status");
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn getBuildInfo() -> *const c_char {
    todo!("getBuildInfo");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
) -> c_int {
    todo!("taos_options_connection")
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_write_crashinfo(
    signum: c_int,
    sigInfo: *mut c_void,
    context: *mut c_void,
) -> c_void {
    todo!("taos_write_crashinfo")
}
