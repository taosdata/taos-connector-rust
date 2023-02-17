use std::os::raw::*;
use taos_macros::c_cfg;

use crate::types::*;

pub type TAOS = c_void;
pub type TaosStmt = c_void;
pub type TaosRes = c_void;
pub type TaosStream = c_void;
pub type TaosSub = c_void;
pub type TaosRow = *mut *mut c_void;

pub type TaosSubscribeCb =
    unsafe extern "C" fn(sub: *mut TaosSub, res: *mut TaosRes, param: *mut c_void, code: c_int);

pub type TaosStreamCb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut TaosRes, row: TaosRow);

pub type TaosStreamCloseCb = unsafe extern "C" fn(param: *mut c_void);

extern "C" {
    pub fn taos_cleanup();

    pub fn taos_options(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int;

    pub fn taos_get_client_info() -> *const c_char;

    pub fn taos_data_type(type_: c_int) -> *const c_char;
}

#[c_cfg(taos_parse_time)]
extern "C" {
    pub fn taos_parse_time(
        time_str: *const c_char,
        time: *mut i64,
        len: i32,
        time_precision: Precision,
        daylight: i8, // if in daylight saving time (DST) { 1 } else { 0 }
    ) -> i32;
}

extern "C" {
    pub fn taos_connect(
        ip: *const c_char,
        user: *const c_char,
        pass: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> *mut TAOS;

    pub fn taos_connect_auth(
        ip: *const c_char,
        user: *const c_char,
        auth: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> *mut TAOS;

    pub fn taos_close(taos: *mut TAOS);

}

pub type TaosAsyncFetchCb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut c_void, rows: c_int);

pub type TaosAsyncQueryCb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut c_void, code: c_int);

extern "C" {
    pub fn taos_fetch_rows_a(res: *mut TaosRes, fp: TaosAsyncFetchCb, param: *mut c_void);

    pub fn taos_query_a(
        taos: *mut TAOS,
        sql: *const c_char,
        fp: TaosAsyncQueryCb,
        param: *mut c_void,
    );
}

extern "C" {
    pub fn taos_load_table_info(taos: *mut TAOS, tableNameList: *const c_char) -> c_int;

    pub fn taos_stmt_init(taos: *mut TAOS) -> *mut TaosStmt;

    pub fn taos_stmt_prepare(stmt: *mut TaosStmt, sql: *const c_char, length: c_ulong) -> c_int;

    pub fn taos_stmt_set_tbname_tags(
        stmt: *mut TaosStmt,
        name: *const c_char,
        tags: *mut TaosBind,
    ) -> c_int;

    pub fn taos_stmt_set_tbname(stmt: *mut TaosStmt, name: *const c_char) -> c_int;

    pub fn taos_stmt_set_tags(stmt: *mut TaosStmt, tags: *mut TaosBind) -> c_int;

    pub fn taos_stmt_set_sub_tbname(stmt: *mut TaosStmt, name: *const c_char) -> c_int;

    pub fn taos_stmt_is_insert(stmt: *mut TaosStmt, insert: *mut c_int) -> c_int;

    pub fn taos_stmt_num_params(stmt: *mut TaosStmt, nums: *mut c_int) -> c_int;

    pub fn taos_stmt_get_param(
        stmt: *mut TaosStmt,
        idx: c_int,
        type_: *mut c_int,
        bytes: *mut c_int,
    ) -> c_int;

    pub fn taos_stmt_bind_param(stmt: *mut TaosStmt, bind: *const TaosBind) -> c_int;

    pub fn taos_stmt_bind_param_batch(stmt: *mut TaosStmt, bind: *const TaosMultiBind) -> c_int;

    pub fn taos_stmt_bind_single_param_batch(
        stmt: *mut TaosStmt,
        bind: *const TaosMultiBind,
        colIdx: c_int,
    ) -> c_int;

    pub fn taos_stmt_add_batch(stmt: *mut TaosStmt) -> c_int;

    pub fn taos_stmt_execute(stmt: *mut TaosStmt) -> c_int;

    pub fn taos_stmt_affected_rows(stmt: *mut TaosStmt) -> c_int;

    pub fn taos_stmt_use_result(stmt: *mut TaosStmt) -> *mut TaosRes;

    pub fn taos_stmt_close(stmt: *mut TaosStmt) -> c_int;

    pub fn taos_stmt_errstr(stmt: *mut TaosStmt) -> *const c_char;
}

extern "C" {
    pub fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TaosRes;

    pub fn taos_fetch_row(res: *mut TaosRes) -> TaosRow;

    pub fn taos_result_precision(res: *mut TaosRes) -> c_int;

    pub fn taos_free_result(res: *mut TaosRes);

    pub fn taos_field_count(res: *mut TaosRes) -> c_int;

    pub fn taos_affected_rows(res: *mut TaosRes) -> c_int;

    pub fn taos_fetch_fields(res: *mut TaosRes) -> *mut TAOS_FIELD;

    pub fn taos_select_db(taos: *mut TAOS, db: *const c_char) -> c_int;

    pub fn taos_print_row(
        str_: *mut c_char,
        row: TaosRow,
        fields: *mut TAOS_FIELD,
        num_fields: c_int,
    ) -> c_int;

    pub fn taos_stop_query(res: *mut TaosRes);

    pub fn taos_is_null(res: *mut TaosRes, row: i32, col: i32) -> bool;

    pub fn taos_is_update_query(res: *mut TaosRes) -> bool;

    pub fn taos_fetch_block(res: *mut TaosRes, rows: *mut TaosRow) -> c_int;

    pub fn taos_fetch_lengths(res: *mut TaosRes) -> *mut c_int;

    pub fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> c_int;

    pub fn taos_reset_current_db(taos: *mut TAOS);

    pub fn taos_get_server_info(taos: *mut TAOS) -> *mut c_char;

    pub fn taos_errstr(tres: *mut TaosRes) -> *mut c_char;

    pub fn taos_errno(tres: *mut TaosRes) -> c_int;

}

#[c_cfg(taos_v3)]
extern "C" {
    pub fn taos_get_column_data_offset(res: *mut TaosRes, col: i32) -> *mut i32;

    pub fn taos_fetch_raw_block(res: *mut TaosRes, num: *mut i32, data: *mut *mut c_void)
        -> c_int;

    pub fn taos_fetch_raw_block_a(res: *mut TaosRes, fp: TaosAsyncFetchCb, param: *mut c_void);

    pub fn taos_get_raw_block(taos: *mut TaosRes) -> *mut c_void;
}

#[c_cfg(taos_result_block)]
extern "C" {
    pub fn taos_result_block(res: *mut TaosRes) -> *mut TaosRow;
}

#[cfg(taos_fetch_block_s)]
extern "C" {
    pub fn taos_fetch_block_s(
        res: *mut TaosRes,
        num_of_rows: *mut c_int,
        rows: *mut TaosRow,
    ) -> c_int;
}

#[cfg(not(taos_fetch_block_s))]
#[no_mangle]
pub unsafe extern "C" fn taos_fetch_block_s(
    res: *mut TaosRes,
    num_of_rows: *mut c_int,
    rows: *mut TaosRow,
) -> c_int {
    *num_of_rows = taos_fetch_block(res, rows);
    return 0;
}

extern "C" {
    pub fn taos_subscribe(
        taos: *mut TAOS,
        restart: c_int,
        topic: *const c_char,
        sql: *const c_char,
        fp: Option<TaosSubscribeCb>,
        param: *mut c_void,
        interval: c_int,
    ) -> *mut TaosSub;

    pub fn taos_consume(tsub: *mut TaosSub) -> *mut TaosRes;

    pub fn taos_unsubscribe(tsub: *mut TaosSub, keep_progress: c_int);
}

extern "C" {
    pub fn taos_open_stream(
        taos: *mut TAOS,
        sql: *const c_char,
        fp: Option<TaosStreamCb>,
        stime: i64,
        param: *mut c_void,
        callback: Option<TaosStreamCloseCb>,
    ) -> *mut TaosStream;

    pub fn taos_close_stream(stream: *mut TaosStream);
}
