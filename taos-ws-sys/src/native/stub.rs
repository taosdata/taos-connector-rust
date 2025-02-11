#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void};

use crate::native::tmq::tmq_t;
use crate::native::{TAOS, TAOS_FIELD, TAOS_RES};

#[no_mangle]
pub extern "C" fn taos_init() -> c_int {
    0
}

#[no_mangle]
pub extern "C" fn taos_cleanup() {}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_OPTION_CONNECTION {
    TSDB_OPTION_CONNECTION_CLEAR = -1,
    TSDB_OPTION_CONNECTION_CHARSET = 0,
    TSDB_OPTION_CONNECTION_TIMEZONE = 1,
    TSDB_OPTION_CONNECTION_USER_IP = 2,
    TSDB_OPTION_CONNECTION_USER_APP = 3,
    TSDB_MAX_OPTIONS_CONNECTION = 4,
}

#[no_mangle]
pub unsafe extern "C" fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
    varargs: ...
) -> c_int {
    todo!()
}

#[no_mangle]
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
pub extern "C" fn taos_connect_dsn(
    dsn: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
) -> *mut TAOS {
    todo!("taos_connect_dsn");
}

#[no_mangle]
pub extern "C" fn taos_connect_dsn_auth(
    dsn: *const c_char,
    user: *const c_char,
    auth: *const c_char,
    db: *const c_char,
) -> *mut TAOS {
    todo!("taos_connect_dsn_auth");
}

#[no_mangle]
pub extern "C" fn taos_kill_query(taos: *mut TAOS) {}

#[no_mangle]
pub extern "C" fn taos_reset_current_db(taos: *mut TAOS) {
    todo!("taos_reset_current_db")
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct TAOS_VGROUP_HASH_INFO {
    pub vgId: i32,
    pub hashBegin: u32,
    pub hashEnd: u32,
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct TAOS_DB_ROUTE_INFO {
    pub routeVersion: i32,
    pub hashPrefix: i16,
    pub hashSuffix: i16,
    pub hashMethod: i8,
    pub vgNum: i32,
    pub vgHash: *mut TAOS_VGROUP_HASH_INFO,
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_get_db_route_info(
    taos: *mut TAOS,
    db: *const c_char,
    dbInfo: *mut TAOS_DB_ROUTE_INFO,
) -> c_int {
    todo!("taos_get_db_route_info");
}

#[no_mangle]
#[allow(non_snake_case)]
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
pub extern "C" fn taos_load_table_info(taos: *mut TAOS, tableNameList: *const c_char) -> c_int {
    0
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_set_hb_quit(quitByKill: i8) {}

#[allow(non_camel_case_types)]
pub type __taos_notify_fn_t = extern "C" fn(param: *mut c_void, ext: *mut c_void, r#type: c_int);

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
    std::ptr::null_mut()
}

#[repr(C)]
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
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct setConfRet {
    pub retCode: SET_CONF_RET_CODE,
    pub retMsg: [c_char; RET_MSG_LENGTH],
}

#[no_mangle]
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

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD_STB {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
    pub field_type: u8,
}

#[allow(non_camel_case_types)]
pub type __taos_async_whitelist_fn_t = extern "C" fn(
    param: *mut c_void,
    code: i32,
    taos: *mut TAOS,
    numOfWhiteLists: i32,
    pWhiteLists: *mut u64,
);

#[no_mangle]
pub extern "C" fn taos_fetch_whitelist_a(
    taos: *mut TAOS,
    fp: __taos_async_whitelist_fn_t,
    param: *mut c_void,
) {
    todo!("taos_fetch_whitelist_a");
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct tmq_raw_data {
    pub raw: *mut c_void,
    pub raw_len: u32,
    pub raw_type: u16,
}

#[no_mangle]
pub extern "C" fn tmq_get_raw(res: *mut TAOS_RES, raw: *mut tmq_raw_data) -> i32 {
    todo!("tmq_get_raw");
}

#[no_mangle]
pub extern "C" fn tmq_write_raw(taos: *mut TAOS, raw: tmq_raw_data) -> i32 {
    todo!("tmq_write_raw");
}

#[no_mangle]
#[allow(non_snake_case)]
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
pub extern "C" fn tmq_free_raw(raw: tmq_raw_data) {
    todo!("tmq_free_raw");
}

#[no_mangle]
pub extern "C" fn tmq_get_json_meta(res: *mut TAOS_RES) -> *const c_char {
    todo!("tmq_get_json_meta");
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn tmq_free_json_meta(jsonMeta: *mut c_char) {
    todo!("tmq_free_json_meta");
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SERVER_STATUS {
    TSDB_SRV_STATUS_UNAVAILABLE = 0,
    TSDB_SRV_STATUS_NETWORK_OK = 1,
    TSDB_SRV_STATUS_SERVICE_OK = 2,
    TSDB_SRV_STATUS_SERVICE_DEGRADED = 3,
    TSDB_SRV_STATUS_EXTING = 4,
}

#[no_mangle]
pub extern "C" fn taos_check_server_status(
    fqdn: *const c_char,
    port: i32,
    details: *mut c_char,
    maxlen: i32,
) -> TSDB_SERVER_STATUS {
    todo!("taos_check_server_status");
}

#[no_mangle]
pub extern "C" fn getBuildInfo() -> *const c_char {
    todo!("getBuildInfo");
}
