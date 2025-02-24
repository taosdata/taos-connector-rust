#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void};

use crate::taos::query::{
    __taos_async_whitelist_fn_t, __taos_notify_fn_t, setConfRet, TAOS_DB_ROUTE_INFO,
    TSDB_SERVER_STATUS,
};
use crate::taos::tmq::{tmq_raw_data, tmq_t};
use crate::taos::{TAOS, TAOS_RES, TSDB_OPTION_CONNECTION};
use crate::ws::TAOS_FIELD;

pub fn taos_init() -> c_int {
    0
}

pub fn taos_cleanup() {}

pub fn taos_connect_auth(
    ip: *const c_char,
    user: *const c_char,
    auth: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    todo!("taos_connect_auth");
}

pub fn taos_connect_dsn(
    dsn: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
) -> *mut TAOS {
    todo!("taos_connect_dsn");
}

pub fn taos_connect_dsn_auth(
    dsn: *const c_char,
    user: *const c_char,
    auth: *const c_char,
    db: *const c_char,
) -> *mut TAOS {
    todo!("taos_connect_dsn_auth");
}

pub fn taos_kill_query(taos: *mut TAOS) {}

pub fn taos_reset_current_db(taos: *mut TAOS) {
    todo!("taos_reset_current_db")
}

#[allow(non_snake_case)]
pub fn taos_get_db_route_info(
    taos: *mut TAOS,
    db: *const c_char,
    dbInfo: *mut TAOS_DB_ROUTE_INFO,
) -> c_int {
    todo!("taos_get_db_route_info");
}

#[allow(non_snake_case)]
pub fn taos_get_table_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *const c_char,
    vgId: *mut c_int,
) -> c_int {
    todo!("taos_get_table_vgId");
}

#[allow(non_snake_case)]
pub fn taos_get_tables_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *mut *const c_char,
    tableNum: c_int,
    vgId: *mut c_int,
) -> c_int {
    todo!("taos_get_tables_vgId");
}

#[allow(non_snake_case)]
pub fn taos_load_table_info(taos: *mut TAOS, tableNameList: *const c_char) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_set_hb_quit(quitByKill: i8) {}

pub fn taos_set_notify_cb(
    taos: *mut TAOS,
    fp: __taos_notify_fn_t,
    param: *mut c_void,
    r#type: c_int,
) -> c_int {
    0
}

pub fn taos_set_conn_mode(taos: *mut TAOS, mode: c_int, value: c_int) -> c_int {
    0
}

pub fn tmq_get_connect(tmq: *mut tmq_t) -> *mut TAOS {
    std::ptr::null_mut()
}

pub fn taos_set_config(config: *const c_char) -> setConfRet {
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

pub fn taos_fetch_whitelist_a(
    taos: *mut TAOS,
    fp: __taos_async_whitelist_fn_t,
    param: *mut c_void,
) {
    todo!("taos_fetch_whitelist_a");
}

pub fn tmq_get_raw(res: *mut TAOS_RES, raw: *mut tmq_raw_data) -> i32 {
    todo!("tmq_get_raw");
}

pub fn tmq_write_raw(taos: *mut TAOS, raw: tmq_raw_data) -> i32 {
    todo!("tmq_write_raw");
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
) -> i32 {
    todo!("taos_write_raw_block");
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block_with_reqid(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    reqid: i64,
) -> i32 {
    todo!("taos_write_raw_block_with_reqid");
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block_with_fields(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
) -> i32 {
    todo!("taos_write_raw_block_with_fields");
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block_with_fields_with_reqid(
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

pub fn tmq_free_raw(raw: tmq_raw_data) {
    todo!("tmq_free_raw");
}

pub fn tmq_get_json_meta(res: *mut TAOS_RES) -> *const c_char {
    todo!("tmq_get_json_meta");
}

#[allow(non_snake_case)]
pub fn tmq_free_json_meta(jsonMeta: *mut c_char) {
    todo!("tmq_free_json_meta");
}

pub fn taos_check_server_status(
    fqdn: *const c_char,
    port: i32,
    details: *mut c_char,
    maxlen: i32,
) -> TSDB_SERVER_STATUS {
    todo!("taos_check_server_status");
}

#[allow(non_snake_case)]
pub fn getBuildInfo() -> *const c_char {
    todo!("getBuildInfo");
}

pub fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
) -> c_int {
    todo!("taos_options_connection")
}
