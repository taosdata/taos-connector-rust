use std::ffi::{c_char, c_int, c_void};

use crate::taos::{
    __taos_async_fn_t, driver, CAPI, TAOS, TAOS_FIELD, TAOS_FIELD_E, TAOS_RES, TAOS_ROW,
};
use crate::ws::{error, query, stub};

pub const TSDB_DATA_TYPE_NULL: usize = 0;
pub const TSDB_DATA_TYPE_BOOL: usize = 1;
pub const TSDB_DATA_TYPE_TINYINT: usize = 2;
pub const TSDB_DATA_TYPE_SMALLINT: usize = 3;
pub const TSDB_DATA_TYPE_INT: usize = 4;
pub const TSDB_DATA_TYPE_BIGINT: usize = 5;
pub const TSDB_DATA_TYPE_FLOAT: usize = 6;
pub const TSDB_DATA_TYPE_DOUBLE: usize = 7;
pub const TSDB_DATA_TYPE_VARCHAR: usize = 8;
pub const TSDB_DATA_TYPE_TIMESTAMP: usize = 9;
pub const TSDB_DATA_TYPE_NCHAR: usize = 10;
pub const TSDB_DATA_TYPE_UTINYINT: usize = 11;
pub const TSDB_DATA_TYPE_USMALLINT: usize = 12;
pub const TSDB_DATA_TYPE_UINT: usize = 13;
pub const TSDB_DATA_TYPE_UBIGINT: usize = 14;
pub const TSDB_DATA_TYPE_JSON: usize = 15;
pub const TSDB_DATA_TYPE_VARBINARY: usize = 16;
pub const TSDB_DATA_TYPE_DECIMAL: usize = 17;
pub const TSDB_DATA_TYPE_BLOB: usize = 18;
pub const TSDB_DATA_TYPE_MEDIUMBLOB: usize = 19;
pub const TSDB_DATA_TYPE_BINARY: usize = TSDB_DATA_TYPE_VARCHAR;
pub const TSDB_DATA_TYPE_GEOMETRY: usize = 20;
pub const TSDB_DATA_TYPE_DECIMAL64: usize = 21;
pub const TSDB_DATA_TYPE_MAX: usize = 22;

#[allow(non_camel_case_types)]
pub type __taos_notify_fn_t = extern "C" fn(param: *mut c_void, ext: *mut c_void, r#type: c_int);

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

#[allow(non_camel_case_types)]
pub type __taos_async_whitelist_fn_t = extern "C" fn(
    param: *mut c_void,
    code: i32,
    taos: *mut TAOS,
    numOfWhiteLists: i32,
    pWhiteLists: *mut u64,
);

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum TSDB_SERVER_STATUS {
    TSDB_SRV_STATUS_UNAVAILABLE = 0,
    TSDB_SRV_STATUS_NETWORK_OK = 1,
    TSDB_SRV_STATUS_SERVICE_OK = 2,
    TSDB_SRV_STATUS_SERVICE_DEGRADED = 3,
    TSDB_SRV_STATUS_EXTING = 4,
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_data_type(r#type: c_int) -> *const c_char {
    if driver() {
        query::taos_data_type(r#type)
    } else {
        (CAPI.query_api.taos_data_type)(r#type)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES {
    if driver() {
        query::taos_query(taos, sql)
    } else {
        (CAPI.query_api.taos_query)(taos, sql)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_query_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    reqId: i64,
) -> *mut TAOS_RES {
    if driver() {
        query::taos_query_with_reqid(taos, sql, reqId)
    } else {
        (CAPI.query_api.taos_query_with_reqid)(taos, sql, reqId)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_row(res: *mut TAOS_RES) -> TAOS_ROW {
    if driver() {
        query::taos_fetch_row(res)
    } else {
        (CAPI.query_api.taos_fetch_row)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_result_precision(res: *mut TAOS_RES) -> c_int {
    if driver() {
        query::taos_result_precision(res)
    } else {
        (CAPI.query_api.taos_result_precision)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_free_result(res: *mut TAOS_RES) {
    if driver() {
        query::taos_free_result(res);
    } else {
        (CAPI.query_api.taos_free_result)(res);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_kill_query(taos: *mut TAOS) {
    if driver() {
        stub::taos_kill_query(taos);
    } else {
        (CAPI.query_api.taos_kill_query)(taos);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_field_count(res: *mut TAOS_RES) -> c_int {
    if driver() {
        query::taos_field_count(res)
    } else {
        (CAPI.query_api.taos_field_count)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_num_fields(res: *mut TAOS_RES) -> c_int {
    if driver() {
        query::taos_num_fields(res)
    } else {
        (CAPI.query_api.taos_num_fields)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_affected_rows(res: *mut TAOS_RES) -> c_int {
    if driver() {
        query::taos_affected_rows(res)
    } else {
        (CAPI.query_api.taos_affected_rows)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_affected_rows64(res: *mut TAOS_RES) -> i64 {
    if driver() {
        query::taos_affected_rows64(res)
    } else {
        (CAPI.query_api.taos_affected_rows64)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_fields(res: *mut TAOS_RES) -> *mut TAOS_FIELD {
    if driver() {
        query::taos_fetch_fields(res)
    } else {
        (CAPI.query_api.taos_fetch_fields)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_fields_e(res: *mut TAOS_RES) -> *mut TAOS_FIELD_E {
    if driver() {
        query::taos_fetch_fields_e(res)
    } else {
        (CAPI.query_api.taos_fetch_fields_e)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_select_db(taos: *mut TAOS, db: *const c_char) -> c_int {
    if driver() {
        query::taos_select_db(taos, db)
    } else {
        (CAPI.query_api.taos_select_db)(taos, db)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_print_row(
    str: *mut c_char,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    if driver() {
        query::taos_print_row(str, row, fields, num_fields)
    } else {
        (CAPI.query_api.taos_print_row)(str, row, fields, num_fields)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_print_row_with_size(
    str: *mut c_char,
    size: u32,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    if driver() {
        query::taos_print_row_with_size(str, size, row, fields, num_fields)
    } else {
        (CAPI.query_api.taos_print_row_with_size)(str, size, row, fields, num_fields)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stop_query(res: *mut TAOS_RES) {
    if driver() {
        query::taos_stop_query(res);
    } else {
        (CAPI.query_api.taos_stop_query)(res);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_is_null(res: *mut TAOS_RES, row: i32, col: i32) -> bool {
    if driver() {
        query::taos_is_null(res, row, col)
    } else {
        (CAPI.query_api.taos_is_null)(res, row, col)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_is_null_by_column(
    res: *mut TAOS_RES,
    columnIndex: c_int,
    result: *mut bool,
    rows: *mut c_int,
) -> c_int {
    if driver() {
        query::taos_is_null_by_column(res, columnIndex, result, rows)
    } else {
        (CAPI.query_api.taos_is_null_by_column)(res, columnIndex, result, rows)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_is_update_query(res: *mut TAOS_RES) -> bool {
    if driver() {
        query::taos_is_update_query(res)
    } else {
        (CAPI.query_api.taos_is_update_query)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_block(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int {
    if driver() {
        query::taos_fetch_block(res, rows)
    } else {
        (CAPI.query_api.taos_fetch_block)(res, rows)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_block_s(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    rows: *mut TAOS_ROW,
) -> c_int {
    if driver() {
        query::taos_fetch_block_s(res, numOfRows, rows)
    } else {
        (CAPI.query_api.taos_fetch_block_s)(res, numOfRows, rows)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_raw_block(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    pData: *mut *mut c_void,
) -> c_int {
    if driver() {
        query::taos_fetch_raw_block(res, numOfRows, pData)
    } else {
        (CAPI.query_api.taos_fetch_raw_block)(res, numOfRows, pData)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_get_column_data_offset(
    res: *mut TAOS_RES,
    columnIndex: c_int,
) -> *mut c_int {
    if driver() {
        query::taos_get_column_data_offset(res, columnIndex)
    } else {
        (CAPI.query_api.taos_get_column_data_offset)(res, columnIndex)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> c_int {
    if driver() {
        query::taos_validate_sql(taos, sql)
    } else {
        (CAPI.query_api.taos_validate_sql)(taos, sql)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_reset_current_db(taos: *mut TAOS) {
    if driver() {
        stub::taos_reset_current_db(taos);
    } else {
        (CAPI.query_api.taos_reset_current_db)(taos);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_lengths(res: *mut TAOS_RES) -> *mut c_int {
    if driver() {
        query::taos_fetch_lengths(res)
    } else {
        (CAPI.query_api.taos_fetch_lengths)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW {
    if driver() {
        query::taos_result_block(res)
    } else {
        (CAPI.query_api.taos_result_block)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_get_server_info(taos: *mut TAOS) -> *const c_char {
    if driver() {
        query::taos_get_server_info(taos)
    } else {
        (CAPI.query_api.taos_get_server_info)(taos)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_get_client_info() -> *const c_char {
    if driver() {
        query::taos_get_client_info()
    } else {
        (CAPI.query_api.taos_get_client_info)()
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_get_current_db(
    taos: *mut TAOS,
    database: *mut c_char,
    len: c_int,
    required: *mut c_int,
) -> c_int {
    if driver() {
        query::taos_get_current_db(taos, database, len, required)
    } else {
        (CAPI.query_api.taos_get_current_db)(taos, database, len, required)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_query_a(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    if driver() {
        query::taos_query_a(taos, sql, fp, param);
    } else {
        (CAPI.query_api.taos_query_a)(taos, sql, fp, param);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_query_a_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
    reqid: i64,
) {
    if driver() {
        query::taos_query_a_with_reqid(taos, sql, fp, param, reqid);
    } else {
        (CAPI.query_api.taos_query_a_with_reqid)(taos, sql, fp, param, reqid);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_rows_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    if driver() {
        query::taos_fetch_rows_a(res, fp, param);
    } else {
        (CAPI.query_api.taos_fetch_rows_a)(res, fp, param);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_raw_block_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    if driver() {
        query::taos_fetch_raw_block_a(res, fp, param);
    } else {
        (CAPI.query_api.taos_fetch_raw_block_a)(res, fp, param);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_get_raw_block(res: *mut TAOS_RES) -> *const c_void {
    if driver() {
        query::taos_get_raw_block(res)
    } else {
        (CAPI.query_api.taos_get_raw_block)(res)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_get_db_route_info(
    taos: *mut TAOS,
    db: *const c_char,
    dbInfo: *mut TAOS_DB_ROUTE_INFO,
) -> c_int {
    if driver() {
        stub::taos_get_db_route_info(taos, db, dbInfo)
    } else {
        (CAPI.query_api.taos_get_db_route_info)(taos, db, dbInfo)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_get_table_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *const c_char,
    vgId: *mut c_int,
) -> c_int {
    if driver() {
        stub::taos_get_table_vgId(taos, db, table, vgId)
    } else {
        (CAPI.query_api.taos_get_table_vgId)(taos, db, table, vgId)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_get_tables_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *mut *const c_char,
    tableNum: c_int,
    vgId: *mut c_int,
) -> c_int {
    if driver() {
        stub::taos_get_tables_vgId(taos, db, table, tableNum, vgId)
    } else {
        (CAPI.query_api.taos_get_tables_vgId)(taos, db, table, tableNum, vgId)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_load_table_info(
    taos: *mut TAOS,
    tableNameList: *const c_char,
) -> c_int {
    if driver() {
        stub::taos_load_table_info(taos, tableNameList)
    } else {
        (CAPI.query_api.taos_load_table_info)(taos, tableNameList)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_set_hb_quit(quitByKill: i8) {
    if driver() {
        stub::taos_set_hb_quit(quitByKill);
    } else {
        (CAPI.query_api.taos_set_hb_quit)(quitByKill);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_set_notify_cb(
    taos: *mut TAOS,
    fp: __taos_notify_fn_t,
    param: *mut c_void,
    r#type: c_int,
) -> c_int {
    if driver() {
        stub::taos_set_notify_cb(taos, fp, param, r#type)
    } else {
        (CAPI.query_api.taos_set_notify_cb)(taos, fp, param, r#type)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_fetch_whitelist_a(
    taos: *mut TAOS,
    fp: __taos_async_whitelist_fn_t,
    param: *mut c_void,
) {
    if driver() {
        stub::taos_fetch_whitelist_a(taos, fp, param);
    } else {
        (CAPI.query_api.taos_fetch_whitelist_a)(taos, fp, param);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_set_conn_mode(taos: *mut TAOS, mode: c_int, value: c_int) -> c_int {
    if driver() {
        stub::taos_set_conn_mode(taos, mode, value)
    } else {
        (CAPI.query_api.taos_set_conn_mode)(taos, mode, value)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_set_config(config: *const c_char) -> setConfRet {
    if driver() {
        stub::taos_set_config(config)
    } else {
        (CAPI.query_api.taos_set_config)(config)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_check_server_status(
    fqdn: *const c_char,
    port: i32,
    details: *mut c_char,
    maxlen: i32,
) -> TSDB_SERVER_STATUS {
    if driver() {
        query::taos_check_server_status(fqdn, port, details, maxlen)
    } else {
        (CAPI.query_api.taos_check_server_status)(fqdn, port, details, maxlen)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_write_crashinfo(
    signum: c_int,
    sigInfo: *mut c_void,
    context: *mut c_void,
) {
    if driver() {
        stub::taos_write_crashinfo(signum, sigInfo, context);
    } else {
        (CAPI.query_api.taos_write_crashinfo)(signum, sigInfo, context);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn getBuildInfo() -> *const c_char {
    if driver() {
        stub::getBuildInfo()
    } else {
        (CAPI.query_api.getBuildInfo)()
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_errno(res: *mut TAOS_RES) -> c_int {
    if driver() {
        error::taos_errno(res)
    } else {
        (CAPI.query_api.taos_errno)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_errstr(res: *mut TAOS_RES) -> *const c_char {
    if driver() {
        error::taos_errstr(res)
    } else {
        (CAPI.query_api.taos_errstr)(res)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::sync::mpsc;
    use std::thread::sleep;
    use std::time::Duration;
    use std::{ptr, slice, vec};

    use taos_query::common::Precision;

    use super::*;
    use crate::taos::{
        taos_close, taos_connect, taos_init, test_connect, test_exec, test_exec_many,
    };

    #[test]
    fn test_taos_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102397",
                    "create database test_1737102397",
                    "use test_1737102397",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "select * from t0",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                    "create table d0 using s0 tags(1)",
                    "insert into d0 values (now, 1)",
                    "select * from d0",
                    "insert into d1 using s0 tags(2) values(now, 1)",
                    "select * from d1",
                    "select * from s0",
                    "drop database test_1737102397",
                ],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_row() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102398",
                    "create database test_1737102398",
                    "use test_1737102398",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102398");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_result_precision() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102399",
                    "create database test_1737102399",
                    "use test_1737102399",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let precision: Precision = taos_result_precision(res).into();
            assert_eq!(precision, Precision::Millisecond);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102399");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_field_count() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102400",
                    "create database test_1737102400",
                    "use test_1737102400",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let count = taos_field_count(res);
            assert_eq!(count, 2);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102400");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_num_fields() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102401",
                    "create database test_1737102401",
                    "use test_1737102401",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let num = taos_num_fields(res);
            assert_eq!(num, 2);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102401");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_affected_rows() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102402",
                    "create database test_1737102402",
                    "use test_1737102402",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 0);

            taos_free_result(res);

            let res = taos_query(taos, c"insert into t0 values (now, 2)".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 1);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102402");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_affected_rows64() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102403",
                    "create database test_1737102403",
                    "use test_1737102403",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows64(res);
            assert_eq!(rows, 0);

            taos_free_result(res);

            let res = taos_query(taos, c"insert into t0 values (now, 2)".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 1);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102403");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_fields() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102404",
                    "create database test_1737102404",
                    "use test_1737102404",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102404");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_select_db() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102405",
                    "create database test_1737102405",
                ],
            );

            let res = taos_select_db(taos, c"test_1737102405".as_ptr());
            assert_eq!(res, 0);

            test_exec_many(
                taos,
                &[
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "drop database test_1737102405",
                ],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_print_row() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102406",
                    "create database test_1737102406",
                    "use test_1737102406",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (1741660079228, 1)",
                    "insert into t0 values (1741660080229, 0)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 2);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(len, 15);
            assert_eq!(
                "1741660079228 1",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(len, 15);
            assert_eq!(
                "1741660080229 0",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102406");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741657731",
                    "create database test_1741657731",
                    "use test_1741657731",
                    "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                    c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                    c9 bigint unsigned, c10 float, c11 double, c12 varchar(20), c13 nchar(10), \
                    c14 varbinary(10), c15 geometry(50), c16 decimal(10, 3), c17 decimal(38, 10))",
                    "insert into t0 values (1743557474107, true, 1, 1, 1, 1, 1, 1, 1, 1, 1.1, 1.1, \
                    'hello world', 'hello', 'hello', 'POINT(1 1)', 12345.123, \
                    12345678901234567890.123456789)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 18);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(len, 152);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()),
                c"1743557474107 1 1 1 1 1 1 1 1 1 1.1 1.1 hello world \xf3\x86\x95\xa8 68656c6c6f 0101000000000000000000f03f000000000000f03f 12345.123 12345678901234567890.1234567890",
            );

            taos_free_result(res);
            test_exec(taos, "drop database test_1741657731");
            taos_close(taos);
        }
    }

    #[test]
    fn test_write_to_cstr() {
        unsafe fn write_to_cstr(size: &mut usize, str: *mut c_char, content: &str) -> i32 {
            if content.len() > *size {
                return -1;
            }
            let cstr = content.as_ptr() as *const c_char;
            ptr::copy_nonoverlapping(cstr, str, content.len());
            *size -= content.len();
            content.len() as _
        }

        unsafe {
            let mut len = 0;
            let mut size = 1024;
            let mut str = vec![0 as c_char; size];
            let length = write_to_cstr(&mut size, str.as_mut_ptr().add(len), "hello world");
            assert_eq!(length, 11);
            assert_eq!(
                "hello world",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );

            len += length as usize;
            let length = write_to_cstr(&mut size, str.as_mut_ptr().add(len), "hello\0world");
            assert_eq!(length, 11);
            assert_eq!(
                "hello worldhello",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );
        }
    }

    #[test]
    fn test_taos_stop_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102407",
                    "create database test_1737102407",
                    "use test_1737102407",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            taos_stop_query(res);
            let errno = taos_errno(ptr::null_mut());
            assert_eq!(errno, 0);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102407");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_is_null() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102408",
                    "create database test_1737102408",
                    "use test_1737102408",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let is_null = taos_is_null(res, 0, 0);
            assert!(is_null);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let is_null = taos_is_null(res, 0, 0);
            assert!(!is_null);

            let is_null = taos_is_null(ptr::null_mut(), 0, 0);
            assert!(is_null);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102408");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_is_update_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102409",
                    "create database test_1737102409",
                    "use test_1737102409",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"insert into t0 values (now, 2)".as_ptr());
            assert!(!res.is_null());

            let is_update = taos_is_update_query(res);
            assert!(is_update);

            taos_free_result(res);

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let is_update = taos_is_update_query(res);
            assert!(!is_update);

            taos_free_result(res);

            let is_update = taos_is_update_query(ptr::null_mut());
            assert!(is_update);

            test_exec(taos, "drop database test_1737102409");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_raw_block() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102410",
                    "create database test_1737102410",
                    "use test_1737102410",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 1);
            assert!(!data.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102410");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_server_info() {
        unsafe {
            let taos = test_connect();
            let server_info = taos_get_server_info(taos);
            assert!(!server_info.is_null());
            let server_info = CStr::from_ptr(server_info).to_str().unwrap();
            println!("server_info: {server_info}");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_client_info() {
        unsafe {
            let client_info = taos_get_client_info();
            assert!(!client_info.is_null());
            println!("client_info: {:?}", CStr::from_ptr(client_info));
            if driver() {
                let client_info = CStr::from_ptr(client_info).to_str().unwrap();
                assert_eq!(client_info, "0.2.1");
            }
        }
    }

    #[test]
    fn test_taos_get_current_db() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102411",
                    "create database test_1737102411",
                ],
            );
            taos_close(taos);

            let taos = taos_connect(
                c"localhost".as_ptr(),
                c"root".as_ptr(),
                c"taosdata".as_ptr(),
                c"test_1737102411".as_ptr(),
                0,
            );
            assert!(!taos.is_null());

            let mut db = vec![0 as c_char; 1024];
            let mut required = 0;
            let code = taos_get_current_db(taos, db.as_mut_ptr(), db.len() as _, &mut required);
            assert_eq!(code, 0);
            assert_eq!(CStr::from_ptr(db.as_ptr()), c"test_1737102411");

            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_current_db_without_db() {
        unsafe {
            let taos = test_connect();
            let mut db = vec![0 as c_char; 1024];
            let mut required = 0;
            let code = taos_get_current_db(taos, db.as_mut_ptr(), db.len() as _, &mut required);
            assert!(code != 0);
            let errstr = taos_errstr(ptr::null_mut());
            println!("errstr: {:?}", CStr::from_ptr(errstr));
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_data_type() {
        unsafe {
            let code = taos_init();
            assert_eq!(code, 0);

            let type_null_ptr = taos_data_type(0);
            let type_null = CStr::from_ptr(type_null_ptr);
            assert_eq!(type_null, c"TSDB_DATA_TYPE_NULL");

            let type_geo_ptr = taos_data_type(20);
            let type_geo = CStr::from_ptr(type_geo_ptr);
            assert_eq!(type_geo, c"TSDB_DATA_TYPE_GEOMETRY");

            let type_invalid = taos_data_type(100);
            assert_eq!(CStr::from_ptr(type_invalid), c"UNKNOWN");
        }
    }

    #[test]
    fn test_taos_is_null_by_column() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740644681",
                    "create database test_1740644681",
                    "use test_1740644681",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now+1s, 1)",
                    "insert into t0 values (now+2s, null)",
                    "insert into t0 values (now+3s, 2)",
                    "insert into t0 values (now+4s, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 4);
            assert!(!data.is_null());

            let mut rows = 100;
            let mut result = vec![false; rows as _];
            let code = taos_is_null_by_column(res, 1, result.as_mut_ptr(), &mut rows);
            assert_eq!(code, 0);
            assert_eq!(rows, 4);

            assert!(!result[0]);
            assert!(result[1]);
            assert!(!result[2]);
            assert!(result[3]);

            taos_free_result(res);
            test_exec(taos, "drop database test_1740644681");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_query_a() {
        unsafe {
            extern "C" fn cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert_eq!(code, 0);
                    assert_eq!(CStr::from_ptr(param as _), c"hello, world");
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

                    taos_free_result(res);
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740664844",
                    "create database test_1740664844",
                    "use test_1740664844",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let sql = c"select * from t0";
            let param = c"hello, world";
            taos_query_a(taos, sql.as_ptr(), cb, param.as_ptr() as _);

            test_exec(taos, "drop database test_1740664844");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_rows_a() {
        unsafe {
            extern "C" fn fetch_rows_cb(taos: *mut c_void, res: *mut TAOS_RES, num_of_row: c_int) {
                unsafe {
                    println!("fetch_rows_cb, num_of_row: {}", num_of_row);
                    let num_fields = taos_num_fields(res);
                    let fields = taos_fetch_fields(res);
                    if num_of_row > 0 {
                        assert_eq!(num_of_row, 4);
                        for _ in 0..num_of_row {
                            let row = taos_fetch_row(res);
                            let mut str = vec![0 as c_char; 1024];
                            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);
                        }
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        println!("fetch_rows_cb, no more data");
                        taos_free_result(res);
                    }
                }
            }

            extern "C" fn query_cb(taos: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    println!("query_cb");
                    if code == 0 && !res.is_null() {
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        taos_free_result(res);
                    }
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740731669",
                    "create database test_1740731669",
                    "use test_1740731669",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "insert into t0 values (now+1s, 2)",
                    "insert into t0 values (now+2s, 3)",
                    "insert into t0 values (now+3s, 4)",
                ],
            );

            let sql = c"select * from t0";
            taos_query_a(taos, sql.as_ptr(), query_cb, taos);

            sleep(Duration::from_secs(1));

            test_exec(taos, "drop database test_1740731669");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_rows_a_() {
        unsafe {
            extern "C" fn fetch_rows_cb(taos: *mut c_void, res: *mut TAOS_RES, num_of_row: c_int) {
                unsafe {
                    println!("fetch_rows_cb, num_of_row: {}", num_of_row);
                    let num_fields = taos_num_fields(res);
                    let fields = taos_fetch_fields(res);
                    if num_of_row > 0 {
                        for _ in 0..num_of_row {
                            let row = taos_fetch_row(res);
                            let mut str = vec![0 as c_char; 1024];
                            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);
                        }
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        println!("fetch_rows_cb, no more data");
                        taos_free_result(res);
                    }
                }
            }

            extern "C" fn query_cb(taos: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    println!("query_cb");
                    if code == 0 && !res.is_null() {
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        taos_free_result(res);
                    }
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740732937",
                    "create database test_1740732937",
                    "use test_1740732937",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            let num = 7000;
            for i in 0..num {
                let sql = format!("insert into t0 values ({}, {})", ts + i, i);
                test_exec(taos, &sql);
            }

            let sql = c"select * from t0";
            taos_query_a(taos, sql.as_ptr(), query_cb, taos);

            sleep(Duration::from_secs(5));

            test_exec(taos, "drop database test_1740732937");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_column_data_offset() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740785939",
                    "create database test_1740785939",
                    "use test_1740785939",
                    "create table t0 (ts timestamp, c1 varchar(20), c2 nchar(20), c3 varbinary(20), c4 geometry(50))",
                    "insert into t0 values (now+1s, 'hello', 'hello', 'hello', 'POINT(1.0 1.0)')",
                    "insert into t0 values (now+2s, 'world', 'world', 'world', 'POINT(2.0 2.0)')",
                    "insert into t0 values (now+3s, null, null, null, null)",
                    "insert into t0 values (now+4s, 'hello, world', 'hello, world', 'hello, world', 'POINT(3.0 3.0)')",
                    "insert into t0 values (now+5s, null, null, null, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 5);
            assert!(!data.is_null());

            let offset_ptr = taos_get_column_data_offset(res, 0);
            assert!(offset_ptr.is_null());

            let offset_ptr = taos_get_column_data_offset(res, 1);
            assert!(!offset_ptr.is_null());

            let mut offsets = Vec::with_capacity(rows as _);
            for i in 0..rows {
                let offset = ptr::read_unaligned(offset_ptr.offset(i as _));
                offsets.push(offset);
            }
            assert_eq!(offsets, [0, 7, -1, 14, -1]);

            let offset_ptr = taos_get_column_data_offset(res, 2);
            assert!(!offset_ptr.is_null());

            let mut offsets = Vec::with_capacity(rows as _);
            for i in 0..rows {
                let offset = ptr::read_unaligned(offset_ptr.offset(i as _));
                offsets.push(offset);
            }
            assert_eq!(offsets, [0, 22, -1, 44, -1]);

            let offset_ptr = taos_get_column_data_offset(res, 3);
            assert!(!offset_ptr.is_null());

            let mut offsets = Vec::with_capacity(rows as _);
            for i in 0..rows {
                let offset = ptr::read_unaligned(offset_ptr.offset(i as _));
                offsets.push(offset);
            }
            assert_eq!(offsets, [0, 7, -1, 14, -1]);

            let offset_ptr = taos_get_column_data_offset(res, 4);
            assert!(!offset_ptr.is_null());

            let mut offsets = Vec::with_capacity(rows as _);
            for i in 0..rows {
                let offset = ptr::read_unaligned(offset_ptr.offset(i as _));
                offsets.push(offset);
            }
            assert_eq!(offsets, [0, 23, -1, 46, -1]);

            taos_free_result(res);
            test_exec(taos, "drop database test_1740785939");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_lengths() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740838806",
                    "create database test_1740838806",
                    "use test_1740838806",
                    "create table t0 (ts timestamp, c1 bool, c2 int, c3 varchar(10), c4 nchar(15))",
                    "insert into t0 values (now, 1, 2025, 'hello', 'helloworld')",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 1);
            assert!(!data.is_null());

            let lengths = taos_fetch_lengths(res);
            assert!(!lengths.is_null());

            let lengths = slice::from_raw_parts(lengths, 5);
            assert_eq!(lengths, [8, 1, 4, 12, 62]);

            taos_free_result(res);
            test_exec(taos, "drop database test_1740838806");
            taos_close(taos);
        }

        // FIXME
        // unsafe {
        //     let taos = test_connect();
        //     test_exec_many(
        //         taos,
        //         &[
        //             "drop database if exists test_1740841972",
        //             "create database test_1740841972",
        //             "use test_1740841972",
        //             "create table t0 (ts timestamp, c1 bool, c2 int, c3 varchar(10), c4 nchar(15))",
        //             "insert into t0 values (now, 1, 2025, 'hello', 'helloworld')",
        //         ],
        //     );

        //     let res = taos_query(taos, c"select * from t0".as_ptr());
        //     assert!(!res.is_null());

        //     let mut rows = 0;
        //     let mut data = ptr::null_mut();
        //     let code = taos_fetch_raw_block(res, &mut rows, &mut data);
        //     assert_eq!(code, 0);
        //     assert_eq!(rows, 1);
        //     assert!(!data.is_null());

        //     let row = taos_fetch_row(res);
        //     assert!(!row.is_null());

        //     let lengths = taos_fetch_lengths(res);
        //     assert!(!lengths.is_null());

        //     let lengths = slice::from_raw_parts(lengths, 5);
        //     assert_eq!(lengths, [8, 1, 4, 5, 10]);

        //     taos_free_result(res);
        //     test_exec(taos, "drop database test_1740841972");
        //     taos_close(taos);
        // }
    }

    #[test]
    fn test_taos_validate_sql() {
        unsafe {
            let taos = test_connect();
            let sql = c"create database if not exists test_1741339814";
            let code = taos_validate_sql(taos, sql.as_ptr());
            assert_eq!(code, 0);
            taos_close(taos);
        }
    }

    #[test]
    #[ignore]
    fn test_taos_fetch_raw_block_a() {
        unsafe {
            extern "C" fn query_cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert_eq!(code, 0);
                    let tx = param as *mut mpsc::Sender<*mut TAOS_RES>;
                    let _ = (*tx).send(res).unwrap();
                }
            }

            extern "C" fn fetch_raw_block_cb(param: *mut c_void, res: *mut TAOS_RES, rows: c_int) {
                unsafe {
                    assert_eq!(rows, 1);
                    let tx = param as *mut mpsc::Sender<*mut TAOS_RES>;
                    let _ = (*tx).send(res).unwrap();
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1745219459",
                    "create database test_1745219459",
                    "use test_1745219459",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let sql = c"select * from t0";
            let (mut query_tx, query_rx) = mpsc::channel();
            taos_query_a(
                taos,
                sql.as_ptr(),
                query_cb,
                &mut query_tx as *mut _ as *mut _,
            );
            let res = query_rx.recv().unwrap();

            let (mut fetch_raw_block_tx, fetch_raw_block_rx) = mpsc::channel();
            taos_fetch_raw_block_a(
                res,
                fetch_raw_block_cb,
                &mut fetch_raw_block_tx as *mut _ as *mut _,
            );
            let res = fetch_raw_block_rx.recv().unwrap();

            let row = taos_result_block(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 2);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), *row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            taos_free_result(res);
            test_exec(taos, "drop database test_1745219459");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_raw_block() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741489408",
                    "create database test_1741489408",
                    "use test_1741489408",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 2025)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let block = taos_get_raw_block(res);
            assert!(!block.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1741489408");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_block() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1745215341",
                    "create database test_1745215341",
                    "use test_1745215341",
                    "create table t0 (ts timestamp, c1 int, c2 varchar(20))",
                    "insert into t0 values (1741780784749, 2025, 'hello')",
                    "insert into t0 values (1741780784750, 999, null)",
                    "insert into t0 values (1741780784751, null, 'world')",
                    "insert into t0 values (1741780784752, null, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 3);

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let fields = slice::from_raw_parts(fields, num_fields as _);

            loop {
                let mut rows = ptr::null_mut();
                let num_of_rows = taos_fetch_block(res, &mut rows);
                println!("num_of_rows: {}", num_of_rows);

                if num_of_rows == 0 {
                    break;
                }

                let rows = slice::from_raw_parts(rows, num_fields as _);

                for (c, field) in fields.iter().enumerate() {
                    for r in 0..num_of_rows as usize {
                        match field.r#type as usize {
                            TSDB_DATA_TYPE_TIMESTAMP => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 8) as isize) as *mut i64;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_INT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 4) as isize) as *mut i32;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_VARCHAR => {
                                let offsets_ptr = taos_get_column_data_offset(res, c as _);
                                let offset = ptr::read_unaligned(offsets_ptr.offset(r as _));
                                if offset == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((offset + 2) as isize) as *mut c_char;
                                    let val = CStr::from_ptr(val).to_str().unwrap();
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            taos_free_result(res);
            test_exec(taos, "drop database test_1745215341");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_block_s() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741779708",
                    "create database test_1741779708",
                    "use test_1741779708",
                    "create table t0 (ts timestamp, c1 int, c2 varchar(20))",
                    "insert into t0 values (1741780784749, 2025, 'hello')",
                    "insert into t0 values (1741780784750, 999, null)",
                    "insert into t0 values (1741780784751, null, 'world')",
                    "insert into t0 values (1741780784752, null, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 3);

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let fields = slice::from_raw_parts(fields, num_fields as _);

            loop {
                let mut num_of_rows = 0;
                let mut rows = ptr::null_mut();
                let code = taos_fetch_block_s(res, &mut num_of_rows, &mut rows);
                assert_eq!(code, 0);
                println!("num_of_rows: {}", num_of_rows);

                if num_of_rows == 0 {
                    break;
                }

                let rows = slice::from_raw_parts(rows, num_fields as _);

                for (c, field) in fields.iter().enumerate() {
                    for r in 0..num_of_rows as usize {
                        match field.r#type as usize {
                            TSDB_DATA_TYPE_TIMESTAMP => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 8) as isize) as *mut i64;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_INT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 4) as isize) as *mut i32;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_VARCHAR => {
                                let offsets_ptr = taos_get_column_data_offset(res, c as _);
                                let offset = ptr::read_unaligned(offsets_ptr.offset(r as _));
                                if offset == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((offset + 2) as isize) as *mut c_char;
                                    let val = CStr::from_ptr(val).to_str().unwrap();
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            taos_free_result(res);
            test_exec(taos, "drop database test_1741779708");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741782821",
                    "create database test_1741782821",
                    "use test_1741782821",
                    "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                    c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                    c9 bigint unsigned, c10 float, c11 double, c12 varchar(10), c13 nchar(10), \
                    c14 varbinary(10), c15 geometry(50))",
                    "insert into t0 values (1741780784752, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.1, 1.1, \
                    'hello', 'hello', 'hello', 'POINT(1 1)')",
                    "insert into t0 values (1741780784753, null, null, null, null, null, null, \
                    null, null, null, null, null, null, null, null, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 16);

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let fields = slice::from_raw_parts(fields, num_fields as _);

            loop {
                let mut num_of_rows = 0;
                let mut rows = ptr::null_mut();
                let code = taos_fetch_block_s(res, &mut num_of_rows, &mut rows);
                assert_eq!(code, 0);
                println!("num_of_rows: {}", num_of_rows);

                if num_of_rows == 0 {
                    break;
                }

                let rows = slice::from_raw_parts(rows, num_fields as _);

                for (c, field) in fields.iter().enumerate() {
                    for r in 0..num_of_rows as usize {
                        match field.r#type as usize {
                            TSDB_DATA_TYPE_TIMESTAMP => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 8) as isize) as *mut i64;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_BOOL => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset(r as isize) as *mut bool;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_TINYINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset(r as isize) as *mut i8;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_SMALLINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset(r as isize) as *mut i16;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_INT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 4) as isize) as *mut i32;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_BIGINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 4) as isize) as *mut i64;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_UTINYINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset(r as isize) as *mut u8;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_USMALLINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset(r as isize) as *mut u16;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_UINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 4) as isize) as *mut u32;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_UBIGINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 4) as isize) as *mut u64;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_FLOAT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 4) as isize) as *mut f32;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_DOUBLE => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((r * 8) as isize) as *mut f64;
                                    println!("col: {}, row: {}, val: {}", c, r, *val);
                                }
                            }
                            TSDB_DATA_TYPE_VARCHAR => {
                                let offsets_ptr = taos_get_column_data_offset(res, c as _);
                                let offset = ptr::read_unaligned(offsets_ptr.offset(r as _));
                                if offset == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let val = rows[c].offset((offset + 2) as isize) as *mut c_char;
                                    let val = CStr::from_ptr(val).to_str().unwrap();
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_NCHAR => {
                                let offsets_ptr = taos_get_column_data_offset(res, c as _);
                                let offset = ptr::read_unaligned(offsets_ptr.offset(r as _));
                                if offset == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(offset as isize) as *const i16;
                                    let len = ptr::read_unaligned(ptr);
                                    let mut bytes = Vec::with_capacity(len as _);
                                    for i in 0..(len / 4) as i32 {
                                        let val = rows[c].offset((offset + 2 + i * 4) as isize)
                                            as *mut u32;
                                        let val = ptr::read_unaligned(val);
                                        bytes.push(val);
                                    }
                                    println!("col: {}, row: {}, val: {:?}", c, r, bytes);
                                }
                            }
                            TSDB_DATA_TYPE_VARBINARY => {
                                let offsets_ptr = taos_get_column_data_offset(res, c as _);
                                let offset = ptr::read_unaligned(offsets_ptr.offset(r as _));
                                if offset == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(offset as isize) as *const i16;
                                    let len = ptr::read_unaligned(ptr);
                                    let val = rows[c].offset((offset + 2) as isize) as *mut u8;
                                    let val = slice::from_raw_parts(val, len as usize);
                                    println!("col: {}, row: {}, val: {:?}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_GEOMETRY => {
                                let offsets_ptr = taos_get_column_data_offset(res, c as _);
                                let offset = ptr::read_unaligned(offsets_ptr.offset(r as _));
                                if offset == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(offset as isize) as *const i16;
                                    let len = ptr::read_unaligned(ptr);
                                    let val = rows[c].offset((offset + 2) as isize) as *mut u8;
                                    let val = slice::from_raw_parts(val, len as usize);
                                    println!("col: {}, row: {}, val: {:?}", c, r, val);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            taos_free_result(res);
            test_exec(taos, "drop database test_1741782821");
            taos_close(taos);
        }
    }

    #[test]
    #[ignore]
    fn test_taos_check_server_status() {
        unsafe { taos_init() };

        unsafe {
            let max_len = 20;
            let mut details = vec![0 as c_char; max_len];
            let fqdn = c"localhost";
            let status =
                taos_check_server_status(fqdn.as_ptr(), 6030, details.as_mut_ptr(), max_len as _);
            assert_eq!(status, TSDB_SERVER_STATUS::TSDB_SRV_STATUS_SERVICE_OK);
            let details = CStr::from_ptr(details.as_ptr());
            println!("status: {status:?}, details: {details:?}");
        }

        unsafe {
            let max_len = 20;
            let mut details = vec![0 as c_char; max_len];
            let status =
                taos_check_server_status(ptr::null(), 0, details.as_mut_ptr(), max_len as _);
            assert_eq!(status, TSDB_SERVER_STATUS::TSDB_SRV_STATUS_SERVICE_OK);
            let details = CStr::from_ptr(details.as_ptr());
            println!("status: {status:?}, details: {details:?}");
        }
    }

    #[test]
    fn test_taos_fetch_fields_e() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1743154970",
                    "create database test_1743154970",
                    "use test_1743154970",
                    "create table t0 (ts timestamp, c1 int, c2 decimal(10, 2), c3 decimal(38, 20))",
                    "insert into t0 values (now, 1, 1234.56, 1234567890.123456789)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields_e(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 4);

            let fields = slice::from_raw_parts(fields, num_fields as _);

            assert_eq!(fields[0].r#type, TSDB_DATA_TYPE_TIMESTAMP as i8);
            assert_eq!(fields[0].bytes, 8);

            assert_eq!(fields[1].r#type, TSDB_DATA_TYPE_INT as i8);
            assert_eq!(fields[1].bytes, 4);

            assert_eq!(fields[2].r#type, TSDB_DATA_TYPE_DECIMAL64 as i8);
            assert_eq!(fields[2].bytes, 8);
            assert_eq!(fields[2].precision, 10);
            assert_eq!(fields[2].scale, 2);

            assert_eq!(fields[3].r#type, TSDB_DATA_TYPE_DECIMAL as i8);
            assert_eq!(fields[3].bytes, 16);
            assert_eq!(fields[3].precision, 38);
            assert_eq!(fields[3].scale, 20);

            taos_free_result(res);
            test_exec(taos, "drop database test_1743154970");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_kill_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1745206722",
                    "create database test_1745206722",
                    "use test_1745206722",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (1741660079228, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            taos_kill_query(taos);
            taos_free_result(res);
            test_exec(taos, "drop database test_1745206722");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_reset_current_db() {
        unsafe {
            let taos = test_connect();
            taos_reset_current_db(taos);
            taos_close(taos);
        }
    }
}
