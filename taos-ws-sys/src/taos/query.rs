use std::ffi::{c_char, c_int, c_void};
use std::sync::atomic::Ordering;

use tracing::instrument;

use crate::taos::{__taos_async_fn_t, CAPI, DRIVER, TAOS, TAOS_RES, TAOS_ROW};
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
pub const TSDB_DATA_TYPE_MAX: usize = 21;

#[allow(non_camel_case_types)]
pub type __taos_notify_fn_t = extern "C" fn(param: *mut c_void, ext: *mut c_void, r#type: c_int);

#[repr(C)]
#[derive(Debug)]
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
#[derive(Debug)]
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
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub enum TSDB_SERVER_STATUS {
    TSDB_SRV_STATUS_UNAVAILABLE = 0,
    TSDB_SRV_STATUS_NETWORK_OK = 1,
    TSDB_SRV_STATUS_SERVICE_OK = 2,
    TSDB_SRV_STATUS_SERVICE_DEGRADED = 3,
    TSDB_SRV_STATUS_EXTING = 4,
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_query(taos, sql)
    } else {
        (CAPI.query_api.taos_query)(taos, sql)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_query_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    reqId: i64,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_query_with_reqid(taos, sql, reqId)
    } else {
        (CAPI.query_api.taos_query_with_reqid)(taos, sql, reqId)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_row(res: *mut TAOS_RES) -> TAOS_ROW {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_fetch_row(res)
    } else {
        (CAPI.query_api.taos_fetch_row)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_result_precision(res: *mut TAOS_RES) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_result_precision(res)
    } else {
        (CAPI.query_api.taos_result_precision)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_free_result(res: *mut TAOS_RES) {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_free_result(res);
    } else {
        (CAPI.query_api.taos_free_result)(res);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_field_count(res: *mut TAOS_RES) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_field_count(res)
    } else {
        (CAPI.query_api.taos_field_count)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_num_fields(res: *mut TAOS_RES) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_num_fields(res)
    } else {
        (CAPI.query_api.taos_num_fields)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_affected_rows(res: *mut TAOS_RES) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_affected_rows(res)
    } else {
        (CAPI.query_api.taos_affected_rows)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_affected_rows64(res: *mut TAOS_RES) -> i64 {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_affected_rows64(res)
    } else {
        (CAPI.query_api.taos_affected_rows64)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_fields(res: *mut TAOS_RES) -> *mut TAOS_FIELD {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_fetch_fields(res)
    } else {
        (CAPI.query_api.taos_fetch_fields)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_select_db(taos: *mut TAOS, db: *const c_char) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_select_db(taos, db)
    } else {
        (CAPI.query_api.taos_select_db)(taos, db)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_print_row(
    str: *mut c_char,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_print_row(str, row, fields, num_fields)
    } else {
        (CAPI.query_api.taos_print_row)(str, row, fields, num_fields)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_print_row_with_size(
    str: *mut c_char,
    size: u32,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_print_row_with_size(str, size, row, fields, num_fields)
    } else {
        (CAPI.query_api.taos_print_row_with_size)(str, size, row, fields, num_fields)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stop_query(res: *mut TAOS_RES) {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_stop_query(res);
    } else {
        (CAPI.query_api.taos_stop_query)(res);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_is_null(res: *mut TAOS_RES, row: i32, col: i32) -> bool {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_is_null(res, row, col)
    } else {
        (CAPI.query_api.taos_is_null)(res, row, col)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_is_null_by_column(
    res: *mut TAOS_RES,
    columnIndex: c_int,
    result: *mut bool,
    rows: *mut c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_is_null_by_column(res, columnIndex, result, rows)
    } else {
        (CAPI.query_api.taos_is_null_by_column)(res, columnIndex, result, rows)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_is_update_query(res: *mut TAOS_RES) -> bool {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_is_update_query(res)
    } else {
        (CAPI.query_api.taos_is_update_query)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_block(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_fetch_block(res, rows)
    } else {
        (CAPI.query_api.taos_fetch_block)(res, rows)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_block_s(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    rows: *mut TAOS_ROW,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_fetch_block_s(res, numOfRows, rows)
    } else {
        (CAPI.query_api.taos_fetch_block_s)(res, numOfRows, rows)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_raw_block(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    pData: *mut *mut c_void,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_fetch_raw_block(res, numOfRows, pData)
    } else {
        (CAPI.query_api.taos_fetch_raw_block)(res, numOfRows, pData)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_column_data_offset(
    res: *mut TAOS_RES,
    columnIndex: c_int,
) -> *mut c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_get_column_data_offset(res, columnIndex)
    } else {
        (CAPI.query_api.taos_get_column_data_offset)(res, columnIndex)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_validate_sql(taos, sql)
    } else {
        (CAPI.query_api.taos_validate_sql)(taos, sql)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_lengths(res: *mut TAOS_RES) -> *mut c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_fetch_lengths(res)
    } else {
        (CAPI.query_api.taos_fetch_lengths)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_result_block(res)
    } else {
        (CAPI.query_api.taos_result_block)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_server_info(taos: *mut TAOS) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_get_server_info(taos)
    } else {
        (CAPI.query_api.taos_get_server_info)(taos)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_client_info() -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_get_client_info()
    } else {
        (CAPI.query_api.taos_get_client_info)()
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_current_db(
    taos: *mut TAOS,
    database: *mut c_char,
    len: c_int,
    required: *mut c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_get_current_db(taos, database, len, required)
    } else {
        (CAPI.query_api.taos_get_current_db)(taos, database, len, required)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_query_a(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_query_a(taos, sql, fp, param);
    } else {
        (CAPI.query_api.taos_query_a)(taos, sql, fp, param);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_query_a_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
    reqid: i64,
) {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_query_a_with_reqid(taos, sql, fp, param, reqid);
    } else {
        (CAPI.query_api.taos_query_a_with_reqid)(taos, sql, fp, param, reqid);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_rows_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_fetch_rows_a(res, fp, param);
    } else {
        (CAPI.query_api.taos_fetch_rows_a)(res, fp, param);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_raw_block_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_fetch_raw_block_a(res, fp, param);
    } else {
        (CAPI.query_api.taos_fetch_raw_block_a)(res, fp, param);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_raw_block(res: *mut TAOS_RES) -> *const c_void {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_get_raw_block(res)
    } else {
        (CAPI.query_api.taos_get_raw_block)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_kill_query(taos: *mut TAOS) {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_kill_query(taos);
    } else {
        (CAPI.query_api.taos_kill_query)(taos);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_reset_current_db(taos: *mut TAOS) {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_reset_current_db(taos);
    } else {
        (CAPI.query_api.taos_reset_current_db)(taos);
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_db_route_info(
    taos: *mut TAOS,
    db: *const c_char,
    dbInfo: *mut TAOS_DB_ROUTE_INFO,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_get_db_route_info(taos, db, dbInfo)
    } else {
        (CAPI.query_api.taos_get_db_route_info)(taos, db, dbInfo)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_table_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *const c_char,
    vgId: *mut c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_get_table_vgId(taos, db, table, vgId)
    } else {
        (CAPI.query_api.taos_get_table_vgId)(taos, db, table, vgId)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_tables_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *mut *const c_char,
    tableNum: c_int,
    vgId: *mut c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_get_tables_vgId(taos, db, table, tableNum, vgId)
    } else {
        (CAPI.query_api.taos_get_tables_vgId)(taos, db, table, tableNum, vgId)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_load_table_info(
    taos: *mut TAOS,
    tableNameList: *const c_char,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_load_table_info(taos, tableNameList)
    } else {
        (CAPI.query_api.taos_load_table_info)(taos, tableNameList)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_set_hb_quit(quitByKill: i8) {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_set_hb_quit(quitByKill);
    } else {
        (CAPI.query_api.taos_set_hb_quit)(quitByKill);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_set_notify_cb(
    taos: *mut TAOS,
    fp: __taos_notify_fn_t,
    param: *mut c_void,
    r#type: c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_set_notify_cb(taos, fp, param, r#type)
    } else {
        (CAPI.query_api.taos_set_notify_cb)(taos, fp, param, r#type)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_set_conn_mode(taos: *mut TAOS, mode: c_int, value: c_int) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_set_conn_mode(taos, mode, value)
    } else {
        (CAPI.query_api.taos_set_conn_mode)(taos, mode, value)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_set_config(config: *const c_char) -> setConfRet {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_set_config(config)
    } else {
        (CAPI.query_api.taos_set_config)(config)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_data_type(r#type: c_int) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        query::taos_data_type(r#type)
    } else {
        (CAPI.query_api.taos_data_type)(r#type)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_errno(res: *mut TAOS_RES) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        error::taos_errno(res)
    } else {
        (CAPI.query_api.taos_errno)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_errstr(res: *mut TAOS_RES) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        error::taos_errstr(res)
    } else {
        (CAPI.query_api.taos_errstr)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_whitelist_a(
    taos: *mut TAOS,
    fp: __taos_async_whitelist_fn_t,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_fetch_whitelist_a(taos, fp, param);
    } else {
        (CAPI.query_api.taos_fetch_whitelist_a)(taos, fp, param);
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_write_raw_block(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_write_raw_block(taos, numOfRows, pData, tbname)
    } else {
        (CAPI.query_api.taos_write_raw_block)(taos, numOfRows, pData, tbname)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_write_raw_block_with_reqid(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    reqid: i64,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_write_raw_block_with_reqid(taos, numOfRows, pData, tbname, reqid)
    } else {
        (CAPI.query_api.taos_write_raw_block_with_reqid)(taos, numOfRows, pData, tbname, reqid)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_write_raw_block_with_fields(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_write_raw_block_with_fields(taos, rows, pData, tbname, fields, numFields)
    } else {
        (CAPI.query_api.taos_write_raw_block_with_fields)(
            taos, rows, pData, tbname, fields, numFields,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_write_raw_block_with_fields_with_reqid(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
    reqid: i64,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_write_raw_block_with_fields_with_reqid(
            taos, rows, pData, tbname, fields, numFields, reqid,
        )
    } else {
        (CAPI.query_api.taos_write_raw_block_with_fields_with_reqid)(
            taos, rows, pData, tbname, fields, numFields, reqid,
        )
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_check_server_status(
    fqdn: *const c_char,
    port: i32,
    details: *mut c_char,
    maxlen: i32,
) -> TSDB_SERVER_STATUS {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_check_server_status(fqdn, port, details, maxlen)
    } else {
        (CAPI.query_api.taos_check_server_status)(fqdn, port, details, maxlen)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn getBuildInfo() -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        stub::getBuildInfo()
    } else {
        (CAPI.query_api.getBuildInfo)()
    }
}
