use std::ffi::{c_char, c_void};

use crate::{TAOS, TAOS_RES};

#[allow(non_camel_case_types)]
pub type TAOS_ROW = *mut *mut c_void;

#[allow(non_camel_case_types)]
pub type __taos_async_fn_t = extern "C" fn(param: *mut c_void, res: *mut TAOS_RES, code: i32);

#[allow(non_camel_case_types)]
pub type __taos_notify_fn_t = extern "C" fn(param: *mut c_void, ext: *mut c_void, r#type: i32);

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub bytes: i32,
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct TAOS_VGROUP_HASH_INFO {
    pub vgId: u32,
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
pub extern "C" fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_query_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    reqId: i64,
) -> *mut TAOS_RES {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_row(res: *mut TAOS_RES) -> *mut TAOS_ROW {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_result_precision(res: *mut TAOS_RES) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_free_result(res: *mut TAOS_RES) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_kill_query(taos: *mut TAOS) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_field_count(res: *mut TAOS_RES) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_num_fields(res: *mut TAOS_RES) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_affected_rows(res: *mut TAOS_RES) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_affected_rows64(res: *mut TAOS_RES) -> i64 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_fields(res: *mut TAOS_RES) -> *mut TAOS_FIELD {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_select_db(taos: *mut TAOS, db: *const c_char) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_print_row(
    str: *mut c_char,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: i32,
) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_print_row_with_size(
    str: *mut c_char,
    size: u32,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: i32,
) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stop_query(res: *mut TAOS_RES) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_is_null(res: *mut TAOS_RES, row: i32, col: i32) -> bool {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_is_null_by_column(
    res: *mut TAOS_RES,
    columnIndex: i32,
    result: *mut bool, // bool result[]
    rows: *mut i32,
) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_is_update_query(res: *mut TAOS_RES) -> bool {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_block(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> i32 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_fetch_block_s(
    res: *mut TAOS_RES,
    numOfRows: *mut i32,
    rows: *mut TAOS_ROW,
) -> i32 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_fetch_raw_block(
    res: *mut TAOS_RES,
    numOfRows: *mut i32,
    pData: *mut *mut c_void,
) -> i32 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_get_column_data_offset(res: *mut TAOS_RES, columnIndex: i32) -> *mut i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_reset_current_db(taos: *mut TAOS) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_lengths(res: *mut TAOS_RES) -> *mut i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_get_server_info(taos: *mut TAOS) -> *const c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_get_client_info() -> *const c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_get_current_db(
    taos: *mut TAOS,
    database: *mut c_char,
    len: i32,
    required: *mut i32,
) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_query_a(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_query_a_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
    reqid: i64,
) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_rows_a(res: *mut TAOS_RES, fp: __taos_async_fn_t, param: *mut c_void) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_raw_block_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_get_raw_block(res: *mut TAOS_RES) -> *const c_void {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_get_db_route_info(
    taos: *mut TAOS,
    db: *const c_char,
    dbInfo: *mut TAOS_DB_ROUTE_INFO,
) -> i32 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_get_table_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *const c_char,
    vgId: *mut i32,
) -> i32 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_get_tables_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *const *const c_char, // const char *table[]
    tableNum: i32,
    vgId: *mut i32,
) -> i32 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_load_table_info(taos: *mut TAOS, tableNameList: *const c_char) -> i32 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_set_hb_quit(quitByKill: i8) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_set_notify_cb(
    taos: *mut TAOS,
    fp: __taos_notify_fn_t,
    param: *mut c_void,
    r#type: i32,
) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_set_conn_mode(taos: *mut TAOS, mode: i32, value: i32) -> i32 {
    todo!()
}
