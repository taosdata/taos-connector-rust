#![feature(c_variadic)]

use std::ffi::{c_char, c_int, c_ulong, c_void};
use std::sync::atomic::{AtomicBool, Ordering};

use native::__taos_async_fn_t;
use native_::{default_lib_name, ApiEntry};
use once_cell::sync::Lazy;
use tracing::instrument;

pub mod native;
pub mod native_;
pub mod ws;

static DRIVER: AtomicBool = AtomicBool::new(true);
static CAPI: Lazy<ApiEntry> = Lazy::new(|| match ApiEntry::open_default() {
    Ok(api) => api,
    Err(err) => panic!("Can't open {} library: {:?}", default_lib_name(), err),
});

pub type TAOS = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_ROW = *mut *mut c_void;

#[allow(non_camel_case_types)]
pub type TAOS_RES = c_void;

// TMQ API

#[allow(non_camel_case_types)]
pub type tmq_t = c_void;

#[allow(non_camel_case_types)]
pub type tmq_conf_t = c_void;

#[allow(non_camel_case_types)]
pub type tmq_list_t = c_void;

#[allow(non_camel_case_types)]
pub type tmq_commit_cb = extern "C" fn(tmq: *mut tmq_t, code: i32, param: *mut c_void);

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq, Eq)]
pub enum tmq_conf_res_t {
    TMQ_CONF_UNKNOWN = -2,
    TMQ_CONF_INVALID = -1,
    TMQ_CONF_OK = 0,
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug)]
pub struct tmq_topic_assignment {
    pub vgId: i32,
    pub currentOffset: i64,
    pub begin: i64,
    pub end: i64,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq, Eq)]
pub enum tmq_res_t {
    TMQ_RES_INVALID = -1,
    TMQ_RES_DATA = 1,
    TMQ_RES_TABLE_META = 2,
    TMQ_RES_METADATA = 3,
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_conf_new() -> *mut tmq_conf_t {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_conf_new()
    } else {
        (CAPI.tmq_api.tmq_conf_new)()
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> tmq_conf_res_t {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_conf_set(conf, key, value)
    } else {
        (CAPI.tmq_api.tmq_conf_set)(conf, key, value)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_conf_destroy(conf: *mut tmq_conf_t) {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_conf_destroy(conf)
    } else {
        (CAPI.tmq_api.tmq_conf_destroy)(conf)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_conf_set_auto_commit_cb(
    conf: *mut tmq_conf_t,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_conf_set_auto_commit_cb(conf, cb, param)
    } else {
        (CAPI.tmq_api.tmq_conf_set_auto_commit_cb)(conf, cb, param)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_list_new() -> *mut tmq_list_t {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_list_new()
    } else {
        (CAPI.tmq_api.tmq_list_new)()
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_list_append(list: *mut tmq_list_t, value: *const c_char) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_list_append(list, value)
    } else {
        (CAPI.tmq_api.tmq_list_append)(list, value)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_list_destroy(list: *mut tmq_list_t) {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_list_destroy(list)
    } else {
        (CAPI.tmq_api.tmq_list_destroy)(list)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_list_get_size(list: *const tmq_list_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_list_get_size(list)
    } else {
        (CAPI.tmq_api.tmq_list_get_size)(list)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_list_to_c_array(list: *const tmq_list_t) -> *mut *mut c_char {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_list_to_c_array(list)
    } else {
        (CAPI.tmq_api.tmq_list_to_c_array)(list)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_consumer_new(
    conf: *mut tmq_conf_t,
    errstr: *mut c_char,
    errstrLen: i32,
) -> *mut tmq_t {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_consumer_new(conf, errstr, errstrLen)
    } else {
        (CAPI.tmq_api.tmq_consumer_new)(conf, errstr, errstrLen)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_subscribe(tmq: *mut tmq_t, topic_list: *const tmq_list_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_subscribe(tmq, topic_list)
    } else {
        (CAPI.tmq_api.tmq_subscribe)(tmq, topic_list)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_unsubscribe(tmq: *mut tmq_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_unsubscribe(tmq)
    } else {
        (CAPI.tmq_api.tmq_unsubscribe)(tmq)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_subscription(tmq: *mut tmq_t, topics: *mut *mut tmq_list_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_subscription(tmq, topics)
    } else {
        (CAPI.tmq_api.tmq_subscription)(tmq, topics)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_consumer_poll(tmq: *mut tmq_t, timeout: i64) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_consumer_poll(tmq, timeout)
    } else {
        (CAPI.tmq_api.tmq_consumer_poll)(tmq, timeout)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_consumer_close(tmq: *mut tmq_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_consumer_close(tmq)
    } else {
        (CAPI.tmq_api.tmq_consumer_close)(tmq)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_commit_sync(tmq: *mut tmq_t, msg: *const TAOS_RES) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_commit_sync(tmq, msg)
    } else {
        (CAPI.tmq_api.tmq_commit_sync)(tmq, msg)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_commit_async(
    tmq: *mut tmq_t,
    msg: *const TAOS_RES,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_commit_async(tmq, msg, cb, param)
    } else {
        (CAPI.tmq_api.tmq_commit_async)(tmq, msg, cb, param)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_commit_offset_sync(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_commit_offset_sync(tmq, pTopicName, vgId, offset)
    } else {
        (CAPI.tmq_api.tmq_commit_offset_sync)(tmq, pTopicName, vgId, offset)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_commit_offset_async(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_commit_offset_async(tmq, pTopicName, vgId, offset, cb, param)
    } else {
        (CAPI.tmq_api.tmq_commit_offset_async)(tmq, pTopicName, vgId, offset, cb, param)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_topic_assignment(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    assignment: *mut *mut tmq_topic_assignment,
    numOfAssignment: *mut i32,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_get_topic_assignment(tmq, pTopicName, assignment, numOfAssignment)
    } else {
        (CAPI.tmq_api.tmq_get_topic_assignment)(tmq, pTopicName, assignment, numOfAssignment)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_free_assignment(pAssignment: *mut tmq_topic_assignment) {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_free_assignment(pAssignment)
    } else {
        (CAPI.tmq_api.tmq_free_assignment)(pAssignment)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_offset_seek(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_offset_seek(tmq, pTopicName, vgId, offset)
    } else {
        (CAPI.tmq_api.tmq_offset_seek)(tmq, pTopicName, vgId, offset)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_position(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
) -> i64 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_position(tmq, pTopicName, vgId)
    } else {
        (CAPI.tmq_api.tmq_position)(tmq, pTopicName, vgId)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_committed(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
) -> i64 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_committed(tmq, pTopicName, vgId)
    } else {
        (CAPI.tmq_api.tmq_committed)(tmq, pTopicName, vgId)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_table_name(res: *mut TAOS_RES) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_get_table_name(res)
    } else {
        (CAPI.tmq_api.tmq_get_table_name)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_get_res_type(res: *mut TAOS_RES) -> tmq_res_t {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_get_res_type(res)
    } else {
        (CAPI.tmq_api.tmq_get_res_type)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_topic_name(res: *mut TAOS_RES) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_get_topic_name(res)
    } else {
        (CAPI.tmq_api.tmq_get_topic_name)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_db_name(res: *mut TAOS_RES) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_get_db_name(res)
    } else {
        (CAPI.tmq_api.tmq_get_db_name)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_vgroup_id(res: *mut TAOS_RES) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_get_vgroup_id(res)
    } else {
        (CAPI.tmq_api.tmq_get_vgroup_id)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_vgroup_offset(res: *mut TAOS_RES) -> i64 {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_get_vgroup_offset(res)
    } else {
        (CAPI.tmq_api.tmq_get_vgroup_offset)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_err2str(code: i32) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        native::tmq::tmq_err2str(code)
    } else {
        (CAPI.tmq_api.tmq_err2str)(code)
    }
}

// STMT API

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
        native::stmt::taos_stmt_init(taos)
    } else {
        (CAPI.stmt_api.taos_stmt_init)(taos)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_init_with_reqid(taos: *mut TAOS, reqid: i64) -> *mut TAOS_STMT {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_init_with_reqid(taos, reqid)
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
        native::stmt::taos_stmt_init_with_options(taos, options)
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
        native::stmt::taos_stmt_prepare(stmt, sql, length)
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
        native::stmt::taos_stmt_set_tbname_tags(stmt, name, tags)
    } else {
        (CAPI.stmt_api.taos_stmt_set_tbname_tags)(stmt, name, tags)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_set_tbname(stmt, name)
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
        native::stmt::taos_stmt_set_tags(stmt, tags)
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
        native::stmt::taos_stmt_set_sub_tbname(stmt, name)
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
        native::stmt::taos_stmt_get_tag_fields(stmt, fieldNum, fields)
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
        native::stmt::taos_stmt_get_col_fields(stmt, fieldNum, fields)
    } else {
        (CAPI.stmt_api.taos_stmt_get_col_fields)(stmt, fieldNum, fields)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_reclaim_fields(stmt: *mut TAOS_STMT, fields: *mut TAOS_FIELD_E) {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_reclaim_fields(stmt, fields)
    } else {
        (CAPI.stmt_api.taos_stmt_reclaim_fields)(stmt, fields)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_is_insert(stmt, insert)
    } else {
        (CAPI.stmt_api.taos_stmt_is_insert)(stmt, insert)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_num_params(stmt, nums)
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
        native::stmt::taos_stmt_get_param(stmt, idx, r#type, bytes)
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
        native::stmt::taos_stmt_bind_param(stmt, bind)
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
        native::stmt::taos_stmt_bind_param_batch(stmt, bind)
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
        native::stmt::taos_stmt_bind_single_param_batch(stmt, bind, colIdx)
    } else {
        (CAPI.stmt_api.taos_stmt_bind_single_param_batch)(stmt, bind, colIdx)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_add_batch(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_add_batch)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_execute(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_execute)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_use_result(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_use_result)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_close(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_close)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_errstr(stmt: *mut TAOS_STMT) -> *mut c_char {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_errstr(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_errstr)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_affected_rows(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_affected_rows)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_affected_rows_once(stmt: *mut TAOS_STMT) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt::taos_stmt_affected_rows_once(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_affected_rows_once)(stmt)
    }
}

// STMT2 API

#[allow(non_camel_case_types)]
pub type TAOS_STMT2 = c_void;

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub struct TAOS_STMT2_OPTION {
    pub reqid: i64,
    pub singleStbInsert: bool,
    pub singleTableBindOnce: bool,
    pub asyncExecFn: __taos_async_fn_t,
    pub userdata: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: c_int,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BINDV {
    pub count: c_int,
    pub tbnames: *mut *mut c_char,
    pub tags: *mut *mut TAOS_STMT2_BIND,
    pub bind_cols: *mut *mut TAOS_STMT2_BIND,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD_ALL {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
    pub field_type: u8,
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_init(
    taos: *mut TAOS,
    option: *mut TAOS_STMT2_OPTION,
) -> *mut TAOS_STMT2 {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_init(taos, option)
    } else {
        (CAPI.stmt2_api.taos_stmt2_init)(taos, option)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_prepare(
    stmt: *mut TAOS_STMT2,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_prepare(stmt, sql, length)
    } else {
        (CAPI.stmt2_api.taos_stmt2_prepare)(stmt, sql, length)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_bind_param(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_bind_param(stmt, bindv, col_idx)
    } else {
        (CAPI.stmt2_api.taos_stmt2_bind_param)(stmt, bindv, col_idx)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_exec(
    stmt: *mut TAOS_STMT2,
    affected_rows: *mut c_int,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_exec(stmt, affected_rows)
    } else {
        (CAPI.stmt2_api.taos_stmt2_exec)(stmt, affected_rows)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_close(stmt: *mut TAOS_STMT2) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_close(stmt)
    } else {
        (CAPI.stmt2_api.taos_stmt2_close)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_is_insert(stmt: *mut TAOS_STMT2, insert: *mut c_int) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_is_insert(stmt, insert)
    } else {
        (CAPI.stmt2_api.taos_stmt2_is_insert)(stmt, insert)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_get_fields(
    stmt: *mut TAOS_STMT2,
    count: *mut c_int,
    fields: *mut *mut TAOS_FIELD_ALL,
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_get_fields(stmt, count, fields)
    } else {
        (CAPI.stmt2_api.taos_stmt2_get_fields)(stmt, count, fields)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_free_fields(
    stmt: *mut TAOS_STMT2,
    fields: *mut TAOS_FIELD_ALL,
) {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_free_fields(stmt, fields)
    } else {
        (CAPI.stmt2_api.taos_stmt2_free_fields)(stmt, fields)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_result(stmt: *mut TAOS_STMT2) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_result(stmt)
    } else {
        (CAPI.stmt2_api.taos_stmt2_result)(stmt)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_error(stmt: *mut TAOS_STMT2) -> *mut c_char {
    if DRIVER.load(Ordering::Relaxed) {
        native::stmt2::taos_stmt2_error(stmt)
    } else {
        (CAPI.stmt2_api.taos_stmt2_error)(stmt)
    }
}

// SML API

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_PROTOCOL_TYPE {
    TSDB_SML_UNKNOWN_PROTOCOL = 0,
    TSDB_SML_LINE_PROTOCOL,
    TSDB_SML_TELNET_PROTOCOL,
    TSDB_SML_JSON_PROTOCOL,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_TIMESTAMP_TYPE {
    TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,
    TSDB_SML_TIMESTAMP_HOURS,
    TSDB_SML_TIMESTAMP_MINUTES,
    TSDB_SML_TIMESTAMP_SECONDS,
    TSDB_SML_TIMESTAMP_MILLI_SECONDS,
    TSDB_SML_TIMESTAMP_MICRO_SECONDS,
    TSDB_SML_TIMESTAMP_NANO_SECONDS,
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert(taos, lines, numLines, protocol, precision)
    } else {
        (CAPI.sml_api.taos_schemaless_insert)(taos, lines, numLines, protocol, precision)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert_with_reqid(
            taos, lines, numLines, protocol, precision, reqid,
        )
    } else {
        (CAPI.sml_api.taos_schemaless_insert_with_reqid)(
            taos, lines, numLines, protocol, precision, reqid,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert_raw(taos, lines, len, totalRows, protocol, precision)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw)(taos, lines, len, totalRows, protocol, precision)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert_raw_with_reqid(
            taos, lines, len, totalRows, protocol, precision, reqid,
        )
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw_with_reqid)(
            taos, lines, len, totalRows, protocol, precision, reqid,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_schemaless_insert_ttl(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert_ttl(taos, lines, numLines, protocol, precision, ttl)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_ttl)(taos, lines, numLines, protocol, precision, ttl)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_schemaless_insert_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert_ttl_with_reqid(
            taos, lines, numLines, protocol, precision, ttl, reqid,
        )
    } else {
        (CAPI.sml_api.taos_schemaless_insert_ttl_with_reqid)(
            taos, lines, numLines, protocol, precision, ttl, reqid,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert_raw_ttl(
            taos, lines, len, totalRows, protocol, precision, ttl,
        )
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw_ttl)(
            taos, lines, len, totalRows, protocol, precision, ttl,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert_raw_ttl_with_reqid(
            taos, lines, len, totalRows, protocol, precision, ttl, reqid,
        )
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw_ttl_with_reqid)(
            taos, lines, len, totalRows, protocol, precision, ttl, reqid,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
            taos, lines, len, totalRows, protocol, precision, ttl, reqid, tbnameKey,
        )
    } else {
        (CAPI
            .sml_api
            .taos_schemaless_insert_raw_ttl_with_reqid_tbname_key)(
            taos, lines, len, totalRows, protocol, precision, ttl, reqid, tbnameKey,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_schemaless_insert_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        native::sml::taos_schemaless_insert_ttl_with_reqid_tbname_key(
            taos, lines, numLines, protocol, precision, ttl, reqid, tbnameKey,
        )
    } else {
        (CAPI
            .sml_api
            .taos_schemaless_insert_ttl_with_reqid_tbname_key)(
            taos, lines, numLines, protocol, precision, ttl, reqid, tbnameKey,
        )
    }
}

// ERROR API

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_errno(res: *mut TAOS_RES) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        native::error::taos_errno(res)
    } else {
        (CAPI.error_api.taos_errno)(res)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_errstr(res: *mut TAOS_RES) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        native::error::taos_errstr(res)
    } else {
        (CAPI.error_api.taos_errstr)(res)
    }
}
