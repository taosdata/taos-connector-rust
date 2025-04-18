#![allow(non_snake_case)]

use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_ulong, c_void};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use dlopen2::raw::Library;
use query::{
    __taos_async_whitelist_fn_t, __taos_notify_fn_t, setConfRet, TAOS_DB_ROUTE_INFO,
    TSDB_SERVER_STATUS,
};
use stmt::{TAOS_MULTI_BIND, TAOS_STMT, TAOS_STMT_OPTIONS};
use stmt2::{TAOS_FIELD_ALL, TAOS_STMT2, TAOS_STMT2_BINDV, TAOS_STMT2_OPTION};
use tmq::{
    tmq_commit_cb, tmq_conf_res_t, tmq_conf_t, tmq_list_t, tmq_raw_data, tmq_res_t, tmq_t,
    tmq_topic_assignment,
};

use crate::taos::*;

lazy_static::lazy_static! {
    static ref RAW_LIBRARIES: Mutex<HashMap<PathBuf, Arc<Library>>> = Mutex::new(HashMap::new());
}

pub struct ApiEntry {
    #[allow(dead_code)]
    lib: Arc<Library>,
    pub(crate) basic_api: BasicApi,
    pub(crate) query_api: QueryApi,
    pub(crate) tmq_api: TmqApi,
    pub(crate) sml_api: SmlApi,
    pub(crate) stmt_api: StmtApi,
    pub(crate) stmt2_api: Stmt2Api,
}

pub struct BasicApi {
    pub(crate) taos_init: unsafe extern "C" fn() -> c_int,

    pub(crate) taos_cleanup: unsafe extern "C" fn(),

    pub(crate) taos_options:
        unsafe extern "C" fn(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int,

    #[allow(dead_code)]
    pub(crate) taos_options_connection: unsafe extern "C" fn(
        taos: *mut TAOS,
        option: TSDB_OPTION_CONNECTION,
        arg: *const c_void,
        varargs: ...
    ) -> c_int,

    pub(crate) taos_connect: unsafe extern "C" fn(
        ip: *const c_char,
        user: *const c_char,
        pass: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> *mut TAOS,

    pub(crate) taos_connect_auth: unsafe extern "C" fn(
        ip: *const c_char,
        user: *const c_char,
        auth: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> *mut TAOS,

    pub(crate) taos_close: unsafe extern "C" fn(taos: *mut TAOS),
}

pub struct QueryApi {
    pub(crate) taos_data_type: unsafe extern "C" fn(r#type: c_int) -> *const c_char,

    pub(crate) taos_query:
        unsafe extern "C" fn(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES,

    pub(crate) taos_query_with_reqid:
        unsafe extern "C" fn(taos: *mut TAOS, sql: *const c_char, reqId: i64) -> *mut TAOS_RES,

    pub(crate) taos_fetch_row: unsafe extern "C" fn(res: *mut TAOS_RES) -> TAOS_ROW,

    pub(crate) taos_result_precision: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,

    pub(crate) taos_free_result: unsafe extern "C" fn(res: *mut TAOS_RES),

    pub(crate) taos_kill_query: unsafe extern "C" fn(taos: *mut TAOS),

    pub(crate) taos_field_count: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,

    pub(crate) taos_num_fields: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,

    pub(crate) taos_affected_rows: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,

    pub(crate) taos_affected_rows64: unsafe extern "C" fn(res: *mut TAOS_RES) -> i64,

    pub(crate) taos_fetch_fields: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut TAOS_FIELD,

    pub(crate) taos_fetch_fields_e: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut TAOS_FIELD_E,

    pub(crate) taos_select_db: unsafe extern "C" fn(taos: *mut TAOS, db: *const c_char) -> c_int,

    pub(crate) taos_print_row: unsafe extern "C" fn(
        str: *mut c_char,
        row: TAOS_ROW,
        fields: *mut TAOS_FIELD,
        num_fields: c_int,
    ) -> c_int,

    pub(crate) taos_print_row_with_size: unsafe extern "C" fn(
        str: *mut c_char,
        size: u32,
        row: TAOS_ROW,
        fields: *mut TAOS_FIELD,
        num_fields: c_int,
    ) -> c_int,

    pub(crate) taos_stop_query: unsafe extern "C" fn(res: *mut TAOS_RES),

    pub(crate) taos_is_null: unsafe extern "C" fn(res: *mut TAOS_RES, row: i32, col: i32) -> bool,

    pub(crate) taos_is_null_by_column: unsafe extern "C" fn(
        res: *mut TAOS_RES,
        columnIndex: c_int,
        result: *mut bool,
        rows: *mut c_int,
    ) -> c_int,

    pub(crate) taos_is_update_query: unsafe extern "C" fn(res: *mut TAOS_RES) -> bool,

    pub(crate) taos_fetch_block:
        unsafe extern "C" fn(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int,

    pub(crate) taos_fetch_block_s: unsafe extern "C" fn(
        res: *mut TAOS_RES,
        numOfRows: *mut c_int,
        rows: *mut TAOS_ROW,
    ) -> c_int,

    pub(crate) taos_fetch_raw_block: unsafe extern "C" fn(
        res: *mut TAOS_RES,
        numOfRows: *mut c_int,
        pData: *mut *mut c_void,
    ) -> c_int,

    pub(crate) taos_get_column_data_offset:
        unsafe extern "C" fn(res: *mut TAOS_RES, columnIndex: c_int) -> *mut c_int,

    pub(crate) taos_validate_sql:
        unsafe extern "C" fn(taos: *mut TAOS, sql: *const c_char) -> c_int,

    pub(crate) taos_reset_current_db: unsafe extern "C" fn(taos: *mut TAOS),

    pub(crate) taos_fetch_lengths: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut c_int,

    pub(crate) taos_result_block: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut TAOS_ROW,

    pub(crate) taos_get_server_info: unsafe extern "C" fn(taos: *mut TAOS) -> *const c_char,

    pub(crate) taos_get_client_info: unsafe extern "C" fn() -> *const c_char,

    pub(crate) taos_get_current_db: unsafe extern "C" fn(
        taos: *mut TAOS,
        database: *mut c_char,
        len: c_int,
        required: *mut c_int,
    ) -> c_int,

    pub(crate) taos_query_a: unsafe extern "C" fn(
        taos: *mut TAOS,
        sql: *const c_char,
        fp: __taos_async_fn_t,
        param: *mut c_void,
    ),

    pub(crate) taos_query_a_with_reqid: unsafe extern "C" fn(
        taos: *mut TAOS,
        sql: *const c_char,
        fp: __taos_async_fn_t,
        param: *mut c_void,
        reqid: i64,
    ),

    pub(crate) taos_fetch_rows_a:
        unsafe extern "C" fn(res: *mut TAOS_RES, fp: __taos_async_fn_t, param: *mut c_void),

    pub(crate) taos_fetch_raw_block_a:
        unsafe extern "C" fn(res: *mut TAOS_RES, fp: __taos_async_fn_t, param: *mut c_void),

    pub(crate) taos_get_raw_block: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_void,

    pub(crate) taos_get_db_route_info: unsafe extern "C" fn(
        taos: *mut TAOS,
        db: *const c_char,
        dbInfo: *mut TAOS_DB_ROUTE_INFO,
    ) -> c_int,

    pub(crate) taos_get_table_vgId: unsafe extern "C" fn(
        taos: *mut TAOS,
        db: *const c_char,
        table: *const c_char,
        vgId: *mut c_int,
    ) -> c_int,

    pub(crate) taos_get_tables_vgId: unsafe extern "C" fn(
        taos: *mut TAOS,
        db: *const c_char,
        table: *mut *const c_char,
        tableNum: c_int,
        vgId: *mut c_int,
    ) -> c_int,

    pub(crate) taos_load_table_info:
        unsafe extern "C" fn(taos: *mut TAOS, tableNameList: *const c_char) -> c_int,

    pub(crate) taos_set_hb_quit: unsafe extern "C" fn(quitByKill: i8),

    pub(crate) taos_set_notify_cb: unsafe extern "C" fn(
        taos: *mut TAOS,
        fp: __taos_notify_fn_t,
        param: *mut c_void,
        r#type: c_int,
    ) -> c_int,

    pub(crate) taos_fetch_whitelist_a:
        unsafe extern "C" fn(taos: *mut TAOS, fp: __taos_async_whitelist_fn_t, param: *mut c_void),

    pub(crate) taos_set_conn_mode:
        unsafe extern "C" fn(taos: *mut TAOS, mode: c_int, value: c_int) -> c_int,

    pub(crate) taos_set_config: unsafe extern "C" fn(config: *const c_char) -> setConfRet,

    pub(crate) taos_write_crashinfo:
        unsafe extern "C" fn(signum: c_int, sigInfo: *mut c_void, context: *mut c_void),

    pub(crate) taos_check_server_status: unsafe extern "C" fn(
        fqdn: *const c_char,
        port: i32,
        details: *mut c_char,
        maxlen: i32,
    ) -> TSDB_SERVER_STATUS,

    pub(crate) getBuildInfo: unsafe extern "C" fn() -> *const c_char,

    pub(crate) taos_errstr: unsafe extern "C" fn(taos: *const TAOS) -> *const c_char,

    pub(crate) taos_errno: unsafe extern "C" fn(taos: *const TAOS) -> c_int,
}

pub struct TmqApi {
    pub(crate) tmq_conf_new: unsafe extern "C" fn() -> *mut tmq_conf_t,

    pub(crate) tmq_conf_set: unsafe extern "C" fn(
        conf: *mut tmq_conf_t,
        key: *const c_char,
        value: *const c_char,
    ) -> tmq_conf_res_t,

    pub(crate) tmq_conf_destroy: unsafe extern "C" fn(conf: *mut tmq_conf_t),

    pub(crate) tmq_conf_set_auto_commit_cb:
        unsafe extern "C" fn(conf: *mut tmq_conf_t, cb: tmq_commit_cb, param: *mut c_void),

    pub(crate) tmq_list_new: unsafe extern "C" fn() -> *mut tmq_list_t,

    pub(crate) tmq_list_append:
        unsafe extern "C" fn(list: *mut tmq_list_t, value: *const c_char) -> i32,

    pub(crate) tmq_list_destroy: unsafe extern "C" fn(list: *mut tmq_list_t),

    pub(crate) tmq_list_get_size: unsafe extern "C" fn(list: *const tmq_list_t) -> i32,

    pub(crate) tmq_list_to_c_array:
        unsafe extern "C" fn(list: *const tmq_list_t) -> *mut *mut c_char,

    pub(crate) tmq_consumer_new: unsafe extern "C" fn(
        conf: *mut tmq_conf_t,
        errstr: *mut c_char,
        errstr_len: i32,
    ) -> *mut tmq_t,

    pub(crate) tmq_subscribe:
        unsafe extern "C" fn(tmq: *mut tmq_t, topic_list: *const tmq_list_t) -> i32,

    pub(crate) tmq_unsubscribe: unsafe extern "C" fn(tmq: *mut tmq_t) -> i32,

    pub(crate) tmq_subscription:
        unsafe extern "C" fn(tmq: *mut tmq_t, topics: *mut *mut tmq_list_t) -> i32,

    pub(crate) tmq_consumer_poll:
        unsafe extern "C" fn(tmq: *mut tmq_t, timeout: i64) -> *mut TAOS_RES,

    pub(crate) tmq_consumer_close: unsafe extern "C" fn(tmq: *mut tmq_t) -> i32,

    pub(crate) tmq_commit_sync: unsafe extern "C" fn(tmq: *mut tmq_t, msg: *const TAOS_RES) -> i32,

    pub(crate) tmq_commit_async: unsafe extern "C" fn(
        tmq: *mut tmq_t,
        msg: *const TAOS_RES,
        cb: tmq_commit_cb,
        param: *mut c_void,
    ),

    pub(crate) tmq_commit_offset_sync: unsafe extern "C" fn(
        tmq: *mut tmq_t,
        topic_name: *const c_char,
        vgroup_id: i32,
        offset: i64,
    ) -> i32,

    pub(crate) tmq_commit_offset_async: unsafe extern "C" fn(
        tmq: *mut tmq_t,
        topic_name: *const c_char,
        vgroup_id: i32,
        offset: i64,
        cb: tmq_commit_cb,
        param: *mut c_void,
    ),

    pub(crate) tmq_get_topic_assignment: unsafe extern "C" fn(
        tmq: *mut tmq_t,
        topic_name: *const c_char,
        tmq_topic_assignment: *mut *mut tmq_topic_assignment,
        num_of_assignment: *mut i32,
    ) -> i32,

    pub(crate) tmq_free_assignment: unsafe extern "C" fn(assignment: *mut tmq_topic_assignment),

    pub(crate) tmq_offset_seek: unsafe extern "C" fn(
        tmq: *mut tmq_t,
        topic_name: *const c_char,
        vgroup_id: i32,
        offset: i64,
    ) -> i32,

    pub(crate) tmq_position:
        unsafe extern "C" fn(tmq: *mut tmq_t, topic_name: *const c_char, vgroup_id: i32) -> i64,

    pub(crate) tmq_committed:
        unsafe extern "C" fn(tmq: *mut tmq_t, topic_name: *const c_char, vgroup_id: i32) -> i64,

    pub(crate) tmq_get_connect: unsafe extern "C" fn(tmq: *mut tmq_t) -> *mut TAOS,

    pub(crate) tmq_get_table_name: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,

    pub(crate) tmq_get_res_type: unsafe extern "C" fn(res: *mut TAOS_RES) -> tmq_res_t,

    pub(crate) tmq_get_topic_name: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,

    pub(crate) tmq_get_db_name: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,

    pub(crate) tmq_get_vgroup_id: unsafe extern "C" fn(res: *mut TAOS_RES) -> i32,

    pub(crate) tmq_get_vgroup_offset: unsafe extern "C" fn(res: *mut TAOS_RES) -> i64,

    pub(crate) tmq_err2str: unsafe extern "C" fn(code: i32) -> *const c_char,

    pub(crate) tmq_get_raw: unsafe extern "C" fn(res: *mut TAOS_RES, raw: *mut tmq_raw_data) -> i32,

    pub(crate) tmq_write_raw: unsafe extern "C" fn(taos: *mut TAOS, raw: tmq_raw_data) -> i32,

    pub(crate) taos_write_raw_block: unsafe extern "C" fn(
        taos: *mut TAOS,
        numOfRows: i32,
        pData: *mut c_char,
        tbname: *const c_char,
    ) -> i32,

    pub(crate) taos_write_raw_block_with_reqid: unsafe extern "C" fn(
        taos: *mut TAOS,
        numOfRows: i32,
        pData: *mut c_char,
        tbname: *const c_char,
        reqid: i64,
    ) -> i32,

    pub(crate) taos_write_raw_block_with_fields: unsafe extern "C" fn(
        taos: *mut TAOS,
        rows: i32,
        pData: *mut c_char,
        tbname: *const c_char,
        fields: *mut TAOS_FIELD,
        numFields: i32,
    ) -> i32,

    pub(crate) taos_write_raw_block_with_fields_with_reqid: unsafe extern "C" fn(
        taos: *mut TAOS,
        rows: i32,
        pData: *mut c_char,
        tbname: *const c_char,
        fields: *mut TAOS_FIELD,
        numFields: i32,
        reqid: i64,
    ) -> i32,

    pub(crate) tmq_free_raw: unsafe extern "C" fn(raw: tmq_raw_data),

    pub(crate) tmq_get_json_meta: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,

    pub(crate) tmq_free_json_meta: unsafe extern "C" fn(jsonMeta: *mut c_char),
}

pub struct SmlApi {
    pub(crate) taos_schemaless_insert: unsafe extern "C" fn(
        taos: *mut TAOS,
        lines: *mut *mut c_char,
        numLines: c_int,
        protocol: c_int,
        precision: c_int,
    ) -> *mut TAOS_RES,

    pub(crate) taos_schemaless_insert_with_reqid: unsafe extern "C" fn(
        taos: *mut TAOS,
        lines: *mut *mut c_char,
        numLines: c_int,
        protocol: c_int,
        precision: c_int,
        reqid: i64,
    ) -> *mut TAOS_RES,

    pub(crate) taos_schemaless_insert_raw: unsafe extern "C" fn(
        taos: *mut TAOS,
        lines: *const c_char,
        len: c_int,
        totalRows: *mut i32,
        protocol: c_int,
        precision: c_int,
    ) -> *mut TAOS_RES,

    pub(crate) taos_schemaless_insert_raw_with_reqid: unsafe extern "C" fn(
        taos: *mut TAOS,
        lines: *const c_char,
        len: c_int,
        totalRows: *mut i32,
        protocol: c_int,
        precision: c_int,
        req_id: i64,
    ) -> *mut TAOS_RES,

    pub(crate) taos_schemaless_insert_ttl: unsafe extern "C" fn(
        taos: *mut TAOS,
        lines: *mut *mut c_char,
        numLines: c_int,
        protocol: c_int,
        precision: c_int,
        ttl: i32,
    ) -> *mut TAOS_RES,

    pub(crate) taos_schemaless_insert_ttl_with_reqid: unsafe extern "C" fn(
        taos: *mut TAOS,
        lines: *mut *mut c_char,
        numLines: c_int,
        protocol: c_int,
        precision: c_int,
        ttl: i32,
        reqid: i64,
    ) -> *mut TAOS_RES,

    pub(crate) taos_schemaless_insert_raw_ttl: unsafe extern "C" fn(
        taos: *mut TAOS,
        lines: *const c_char,
        len: c_int,
        totalRows: *mut i32,
        protocol: c_int,
        precision: c_int,
        ttl: i32,
    ) -> *mut TAOS_RES,

    pub(crate) taos_schemaless_insert_raw_ttl_with_reqid: unsafe extern "C" fn(
        taos: *mut TAOS,
        lines: *const c_char,
        len: c_int,
        totalRows: *mut i32,
        protocol: c_int,
        precision: c_int,
        ttl: i32,
        req_id: i64,
    )
        -> *mut TAOS_RES,

    pub(crate) taos_schemaless_insert_raw_ttl_with_reqid_tbname_key:
        unsafe extern "C" fn(
            taos: *mut TAOS,
            lines: *mut c_char,
            len: c_int,
            totalRows: *mut i32,
            protocol: c_int,
            precision: c_int,
            ttl: i32,
            reqid: i64,
            tbnameKey: *mut c_char,
        ) -> *mut TAOS_RES,

    pub(crate) taos_schemaless_insert_ttl_with_reqid_tbname_key:
        unsafe extern "C" fn(
            taos: *mut TAOS,
            lines: *mut *mut c_char,
            numLines: c_int,
            protocol: c_int,
            precision: c_int,
            ttl: i32,
            reqid: i64,
            tbnameKey: *mut c_char,
        ) -> *mut TAOS_RES,
}

pub struct StmtApi {
    pub(crate) taos_stmt_init: unsafe extern "C" fn(taos: *mut TAOS) -> *mut TAOS_STMT,

    pub(crate) taos_stmt_init_with_reqid:
        unsafe extern "C" fn(taos: *mut TAOS, req_id: i64) -> *mut TAOS_STMT,

    pub(crate) taos_stmt_init_with_options:
        unsafe extern "C" fn(taos: *mut TAOS, options: *mut TAOS_STMT_OPTIONS) -> *mut TAOS_STMT,

    pub(crate) taos_stmt_prepare:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, sql: *const c_char, length: c_ulong) -> c_int,

    pub(crate) taos_stmt_set_tbname_tags: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT,
        name: *const c_char,
        tags: *mut TAOS_MULTI_BIND,
    ) -> c_int,

    pub(crate) taos_stmt_set_tbname:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int,

    pub(crate) taos_stmt_set_tags:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, tags: *mut TAOS_MULTI_BIND) -> c_int,

    pub(crate) taos_stmt_set_sub_tbname:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int,

    pub(crate) taos_stmt_get_tag_fields: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT,
        field_num: *mut c_int,
        fields: *mut *mut TAOS_FIELD_E,
    ) -> c_int,

    pub(crate) taos_stmt_get_col_fields: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT,
        field_num: *mut c_int,
        fields: *mut *mut TAOS_FIELD_E,
    ) -> c_int,

    pub(crate) taos_stmt_reclaim_fields:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, fields: *mut TAOS_FIELD_E),

    pub(crate) taos_stmt_is_insert:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int,

    pub(crate) taos_stmt_num_params:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int,

    pub(crate) taos_stmt_get_param: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT,
        idx: c_int,
        r#type: *mut c_int,
        bytes: *mut c_int,
    ) -> c_int,

    pub(crate) taos_stmt_bind_param:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, bind: *const TAOS_MULTI_BIND) -> c_int,

    pub(crate) taos_stmt_bind_param_batch:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, bind: *mut TAOS_MULTI_BIND) -> c_int,

    pub(crate) taos_stmt_bind_single_param_batch: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT,
        bind: *mut TAOS_MULTI_BIND,
        colIdx: c_int,
    ) -> c_int,

    pub(crate) taos_stmt_add_batch: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_execute: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_use_result: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> *mut TAOS_RES,

    pub(crate) taos_stmt_close: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_errstr: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> *mut c_char,

    pub(crate) taos_stmt_affected_rows: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_affected_rows_once: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,
}

pub struct Stmt2Api {
    pub(crate) taos_stmt2_init:
        unsafe extern "C" fn(taos: *mut TAOS, option: *mut TAOS_STMT2_OPTION) -> *mut TAOS_STMT2,

    pub(crate) taos_stmt2_prepare:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT2, sql: *const c_char, length: c_ulong) -> c_int,

    pub(crate) taos_stmt2_bind_param: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT2,
        bindv: *mut TAOS_STMT2_BINDV,
        col_idx: i32,
    ) -> c_int,

    pub(crate) taos_stmt2_bind_param_a: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT2,
        bindv: *mut TAOS_STMT2_BINDV,
        col_idx: i32,
        fp: __taos_async_fn_t,
        param: *mut c_void,
    ) -> c_int,

    pub(crate) taos_stmt2_exec:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT2, affected_rows: *mut c_int) -> c_int,

    pub(crate) taos_stmt2_close: unsafe extern "C" fn(stmt: *mut TAOS_STMT2) -> c_int,

    pub(crate) taos_stmt2_is_insert:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT2, insert: *mut c_int) -> c_int,

    pub(crate) taos_stmt2_get_fields: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT2,
        count: *mut c_int,
        fields: *mut *mut TAOS_FIELD_ALL,
    ) -> c_int,

    pub(crate) taos_stmt2_free_fields:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT2, fields: *mut TAOS_FIELD_ALL),

    pub(crate) taos_stmt2_result: unsafe extern "C" fn(stmt: *mut TAOS_STMT2) -> *mut TAOS_RES,

    pub(crate) taos_stmt2_error: unsafe extern "C" fn(stmt: *mut TAOS_STMT2) -> *mut c_char,
}

pub const fn default_lib_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "taosnative.dll"
    } else if cfg!(target_os = "linux") {
        "libtaosnative.so"
    } else if cfg!(target_os = "macos") {
        "libtaosnative.dylib"
    } else {
        panic!("current os is not supported")
    }
}

impl ApiEntry {
    pub fn open_default() -> Result<Self, dlopen2::Error> {
        let lib_env = "TAOS_LIBRARY_PATH";
        let path = if let Some(path) = std::env::var_os(lib_env) {
            PathBuf::from(path)
        } else {
            PathBuf::from(default_lib_name())
        };
        Self::dlopen(path)
    }

    pub fn dlopen<S: AsRef<Path>>(path: S) -> Result<Self, dlopen2::Error> {
        let path = path.as_ref();
        let path = if path.is_file() {
            path.to_owned()
        } else if path.is_dir() {
            path.join(default_lib_name())
        } else if path.as_os_str().is_empty() {
            PathBuf::from(default_lib_name())
        } else {
            path.to_path_buf()
        };

        let mut guard = RAW_LIBRARIES.lock().unwrap();
        let lib = if let Some(lib) = guard.get(&path) {
            lib.clone()
        } else {
            let lib = Library::open(path.as_os_str())?;
            let lib = Arc::new(lib);
            guard.insert(path, lib.clone());
            lib
        };

        macro_rules! symbol {
            ($($name:ident),*) => {
                $(let $name = lib.symbol(stringify!($name))?;)*
            };
        }

        let basic_api = unsafe {
            symbol!(
                taos_init,
                taos_cleanup,
                taos_options,
                taos_options_connection,
                taos_connect,
                taos_connect_auth,
                taos_close
            );

            BasicApi {
                taos_init,
                taos_cleanup,
                taos_options,
                taos_options_connection,
                taos_connect,
                taos_connect_auth,
                taos_close,
            }
        };

        let query_api = unsafe {
            symbol!(
                taos_data_type,
                taos_query,
                taos_query_with_reqid,
                taos_fetch_row,
                taos_result_precision,
                taos_free_result,
                taos_kill_query,
                taos_field_count,
                taos_num_fields,
                taos_affected_rows,
                taos_affected_rows64,
                taos_fetch_fields,
                taos_fetch_fields_e,
                taos_select_db,
                taos_print_row,
                taos_print_row_with_size,
                taos_stop_query,
                taos_is_null,
                taos_is_null_by_column,
                taos_is_update_query,
                taos_fetch_block,
                taos_fetch_block_s,
                taos_fetch_raw_block,
                taos_get_column_data_offset,
                taos_validate_sql,
                taos_reset_current_db,
                taos_fetch_lengths,
                taos_result_block,
                taos_get_server_info,
                taos_get_client_info,
                taos_get_current_db,
                taos_query_a,
                taos_query_a_with_reqid,
                taos_fetch_rows_a,
                taos_fetch_raw_block_a,
                taos_get_raw_block,
                taos_get_db_route_info,
                taos_get_table_vgId,
                taos_get_tables_vgId,
                taos_load_table_info,
                taos_set_hb_quit,
                taos_set_notify_cb,
                taos_fetch_whitelist_a,
                taos_set_conn_mode,
                taos_set_config,
                taos_check_server_status,
                taos_write_crashinfo,
                getBuildInfo,
                taos_errstr,
                taos_errno
            );

            QueryApi {
                taos_data_type,
                taos_query,
                taos_query_with_reqid,
                taos_fetch_row,
                taos_result_precision,
                taos_free_result,
                taos_kill_query,
                taos_field_count,
                taos_num_fields,
                taos_affected_rows,
                taos_affected_rows64,
                taos_fetch_fields,
                taos_fetch_fields_e,
                taos_select_db,
                taos_print_row,
                taos_print_row_with_size,
                taos_stop_query,
                taos_is_null,
                taos_is_null_by_column,
                taos_is_update_query,
                taos_fetch_block,
                taos_fetch_block_s,
                taos_fetch_raw_block,
                taos_get_column_data_offset,
                taos_validate_sql,
                taos_reset_current_db,
                taos_fetch_lengths,
                taos_result_block,
                taos_get_server_info,
                taos_get_client_info,
                taos_get_current_db,
                taos_query_a,
                taos_query_a_with_reqid,
                taos_fetch_rows_a,
                taos_fetch_raw_block_a,
                taos_get_raw_block,
                taos_get_db_route_info,
                taos_get_table_vgId,
                taos_get_tables_vgId,
                taos_load_table_info,
                taos_set_hb_quit,
                taos_set_notify_cb,
                taos_fetch_whitelist_a,
                taos_set_conn_mode,
                taos_set_config,
                taos_check_server_status,
                taos_write_crashinfo,
                getBuildInfo,
                taos_errstr,
                taos_errno,
            }
        };

        let tmq_api = unsafe {
            symbol!(
                tmq_conf_new,
                tmq_conf_set,
                tmq_conf_destroy,
                tmq_conf_set_auto_commit_cb,
                tmq_list_new,
                tmq_list_append,
                tmq_list_destroy,
                tmq_list_get_size,
                tmq_list_to_c_array,
                tmq_consumer_new,
                tmq_subscribe,
                tmq_unsubscribe,
                tmq_subscription,
                tmq_consumer_poll,
                tmq_consumer_close,
                tmq_commit_sync,
                tmq_commit_async,
                tmq_commit_offset_sync,
                tmq_commit_offset_async,
                tmq_get_topic_assignment,
                tmq_free_assignment,
                tmq_offset_seek,
                tmq_position,
                tmq_committed,
                tmq_get_connect,
                tmq_get_table_name,
                tmq_get_res_type,
                tmq_get_topic_name,
                tmq_get_db_name,
                tmq_get_vgroup_id,
                tmq_get_vgroup_offset,
                tmq_err2str,
                tmq_get_raw,
                tmq_write_raw,
                taos_write_raw_block,
                taos_write_raw_block_with_reqid,
                taos_write_raw_block_with_fields,
                taos_write_raw_block_with_fields_with_reqid,
                tmq_free_raw,
                tmq_get_json_meta,
                tmq_free_json_meta
            );

            TmqApi {
                tmq_conf_new,
                tmq_conf_set,
                tmq_conf_destroy,
                tmq_conf_set_auto_commit_cb,
                tmq_list_new,
                tmq_list_append,
                tmq_list_destroy,
                tmq_list_get_size,
                tmq_list_to_c_array,
                tmq_consumer_new,
                tmq_subscribe,
                tmq_unsubscribe,
                tmq_subscription,
                tmq_consumer_poll,
                tmq_consumer_close,
                tmq_commit_sync,
                tmq_commit_async,
                tmq_commit_offset_sync,
                tmq_commit_offset_async,
                tmq_get_topic_assignment,
                tmq_free_assignment,
                tmq_offset_seek,
                tmq_position,
                tmq_committed,
                tmq_get_connect,
                tmq_get_table_name,
                tmq_get_res_type,
                tmq_get_topic_name,
                tmq_get_db_name,
                tmq_get_vgroup_id,
                tmq_get_vgroup_offset,
                tmq_err2str,
                tmq_get_raw,
                tmq_write_raw,
                taos_write_raw_block,
                taos_write_raw_block_with_reqid,
                taos_write_raw_block_with_fields,
                taos_write_raw_block_with_fields_with_reqid,
                tmq_free_raw,
                tmq_get_json_meta,
                tmq_free_json_meta,
            }
        };

        let sml_api = unsafe {
            symbol!(
                taos_schemaless_insert,
                taos_schemaless_insert_with_reqid,
                taos_schemaless_insert_raw,
                taos_schemaless_insert_raw_with_reqid,
                taos_schemaless_insert_ttl,
                taos_schemaless_insert_ttl_with_reqid,
                taos_schemaless_insert_raw_ttl,
                taos_schemaless_insert_raw_ttl_with_reqid,
                taos_schemaless_insert_raw_ttl_with_reqid_tbname_key,
                taos_schemaless_insert_ttl_with_reqid_tbname_key
            );

            SmlApi {
                taos_schemaless_insert,
                taos_schemaless_insert_with_reqid,
                taos_schemaless_insert_raw,
                taos_schemaless_insert_raw_with_reqid,
                taos_schemaless_insert_ttl,
                taos_schemaless_insert_ttl_with_reqid,
                taos_schemaless_insert_raw_ttl,
                taos_schemaless_insert_raw_ttl_with_reqid,
                taos_schemaless_insert_raw_ttl_with_reqid_tbname_key,
                taos_schemaless_insert_ttl_with_reqid_tbname_key,
            }
        };

        let stmt_api = unsafe {
            symbol!(
                taos_stmt_init,
                taos_stmt_init_with_reqid,
                taos_stmt_init_with_options,
                taos_stmt_prepare,
                taos_stmt_set_tbname_tags,
                taos_stmt_set_tbname,
                taos_stmt_set_tags,
                taos_stmt_set_sub_tbname,
                taos_stmt_get_tag_fields,
                taos_stmt_get_col_fields,
                taos_stmt_reclaim_fields,
                taos_stmt_is_insert,
                taos_stmt_num_params,
                taos_stmt_get_param,
                taos_stmt_bind_param,
                taos_stmt_bind_param_batch,
                taos_stmt_bind_single_param_batch,
                taos_stmt_add_batch,
                taos_stmt_execute,
                taos_stmt_use_result,
                taos_stmt_close,
                taos_stmt_errstr,
                taos_stmt_affected_rows,
                taos_stmt_affected_rows_once
            );

            StmtApi {
                taos_stmt_init,
                taos_stmt_init_with_reqid,
                taos_stmt_init_with_options,
                taos_stmt_prepare,
                taos_stmt_set_tbname_tags,
                taos_stmt_set_tbname,
                taos_stmt_set_tags,
                taos_stmt_set_sub_tbname,
                taos_stmt_get_tag_fields,
                taos_stmt_get_col_fields,
                taos_stmt_reclaim_fields,
                taos_stmt_is_insert,
                taos_stmt_num_params,
                taos_stmt_get_param,
                taos_stmt_bind_param,
                taos_stmt_bind_param_batch,
                taos_stmt_bind_single_param_batch,
                taos_stmt_add_batch,
                taos_stmt_execute,
                taos_stmt_use_result,
                taos_stmt_close,
                taos_stmt_errstr,
                taos_stmt_affected_rows,
                taos_stmt_affected_rows_once,
            }
        };

        let stmt2_api = unsafe {
            symbol!(
                taos_stmt2_init,
                taos_stmt2_prepare,
                taos_stmt2_bind_param,
                taos_stmt2_bind_param_a,
                taos_stmt2_exec,
                taos_stmt2_close,
                taos_stmt2_is_insert,
                taos_stmt2_get_fields,
                taos_stmt2_free_fields,
                taos_stmt2_result,
                taos_stmt2_error
            );

            Stmt2Api {
                taos_stmt2_init,
                taos_stmt2_prepare,
                taos_stmt2_bind_param,
                taos_stmt2_bind_param_a,
                taos_stmt2_exec,
                taos_stmt2_close,
                taos_stmt2_is_insert,
                taos_stmt2_get_fields,
                taos_stmt2_free_fields,
                taos_stmt2_result,
                taos_stmt2_error,
            }
        };

        Ok(Self {
            lib,
            basic_api,
            query_api,
            tmq_api,
            sml_api,
            stmt_api,
            stmt2_api,
        })
    }
}
