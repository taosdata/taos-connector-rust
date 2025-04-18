use std::ffi::{c_char, c_void};
use std::sync::atomic::Ordering;

use tracing::instrument;

use crate::taos::{CAPI, DRIVER, TAOS, TAOS_FIELD, TAOS_RES};
use crate::ws::{stub, tmq};

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

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct tmq_raw_data {
    pub raw: *mut c_void,
    pub raw_len: u32,
    pub raw_type: u16,
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_conf_new() -> *mut tmq_conf_t {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_conf_new()
    } else {
        (CAPI.tmq_api.tmq_conf_new)()
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> tmq_conf_res_t {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_conf_set(conf, key, value)
    } else {
        (CAPI.tmq_api.tmq_conf_set)(conf, key, value)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_conf_destroy(conf: *mut tmq_conf_t) {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_conf_destroy(conf);
    } else {
        (CAPI.tmq_api.tmq_conf_destroy)(conf);
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_conf_set_auto_commit_cb(
    conf: *mut tmq_conf_t,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_conf_set_auto_commit_cb(conf, cb, param);
    } else {
        (CAPI.tmq_api.tmq_conf_set_auto_commit_cb)(conf, cb, param);
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_list_new() -> *mut tmq_list_t {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_list_new()
    } else {
        (CAPI.tmq_api.tmq_list_new)()
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_list_append(list: *mut tmq_list_t, value: *const c_char) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_list_append(list, value)
    } else {
        (CAPI.tmq_api.tmq_list_append)(list, value)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_list_destroy(list: *mut tmq_list_t) {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_list_destroy(list);
    } else {
        (CAPI.tmq_api.tmq_list_destroy)(list);
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_list_get_size(list: *const tmq_list_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_list_get_size(list)
    } else {
        (CAPI.tmq_api.tmq_list_get_size)(list)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_list_to_c_array(list: *const tmq_list_t) -> *mut *mut c_char {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_list_to_c_array(list)
    } else {
        (CAPI.tmq_api.tmq_list_to_c_array)(list)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_consumer_new(
    conf: *mut tmq_conf_t,
    errstr: *mut c_char,
    errstrLen: i32,
) -> *mut tmq_t {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_consumer_new(conf, errstr, errstrLen)
    } else {
        (CAPI.tmq_api.tmq_consumer_new)(conf, errstr, errstrLen)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_subscribe(tmq: *mut tmq_t, topic_list: *const tmq_list_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_subscribe(tmq, topic_list)
    } else {
        (CAPI.tmq_api.tmq_subscribe)(tmq, topic_list)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_unsubscribe(tmq: *mut tmq_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_unsubscribe(tmq)
    } else {
        (CAPI.tmq_api.tmq_unsubscribe)(tmq)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_subscription(tmq: *mut tmq_t, topics: *mut *mut tmq_list_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_subscription(tmq, topics)
    } else {
        (CAPI.tmq_api.tmq_subscription)(tmq, topics)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_consumer_poll(tmq: *mut tmq_t, timeout: i64) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_consumer_poll(tmq, timeout)
    } else {
        (CAPI.tmq_api.tmq_consumer_poll)(tmq, timeout)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_consumer_close(tmq: *mut tmq_t) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_consumer_close(tmq)
    } else {
        (CAPI.tmq_api.tmq_consumer_close)(tmq)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_commit_sync(tmq: *mut tmq_t, msg: *const TAOS_RES) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_commit_sync(tmq, msg)
    } else {
        (CAPI.tmq_api.tmq_commit_sync)(tmq, msg)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_commit_async(
    tmq: *mut tmq_t,
    msg: *const TAOS_RES,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_commit_async(tmq, msg, cb, param);
    } else {
        (CAPI.tmq_api.tmq_commit_async)(tmq, msg, cb, param);
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_commit_offset_sync(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_commit_offset_sync(tmq, pTopicName, vgId, offset)
    } else {
        (CAPI.tmq_api.tmq_commit_offset_sync)(tmq, pTopicName, vgId, offset)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_commit_offset_async(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_commit_offset_async(tmq, pTopicName, vgId, offset, cb, param);
    } else {
        (CAPI.tmq_api.tmq_commit_offset_async)(tmq, pTopicName, vgId, offset, cb, param);
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_topic_assignment(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    assignment: *mut *mut tmq_topic_assignment,
    numOfAssignment: *mut i32,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_get_topic_assignment(tmq, pTopicName, assignment, numOfAssignment)
    } else {
        (CAPI.tmq_api.tmq_get_topic_assignment)(tmq, pTopicName, assignment, numOfAssignment)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_free_assignment(pAssignment: *mut tmq_topic_assignment) {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_free_assignment(pAssignment);
    } else {
        (CAPI.tmq_api.tmq_free_assignment)(pAssignment);
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_offset_seek(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_offset_seek(tmq, pTopicName, vgId, offset)
    } else {
        (CAPI.tmq_api.tmq_offset_seek)(tmq, pTopicName, vgId, offset)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_position(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
) -> i64 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_position(tmq, pTopicName, vgId)
    } else {
        (CAPI.tmq_api.tmq_position)(tmq, pTopicName, vgId)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_committed(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
) -> i64 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_committed(tmq, pTopicName, vgId)
    } else {
        (CAPI.tmq_api.tmq_committed)(tmq, pTopicName, vgId)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_connect(tmq: *mut tmq_t) -> *mut TAOS {
    if DRIVER.load(Ordering::Relaxed) {
        stub::tmq_get_connect(tmq)
    } else {
        (CAPI.tmq_api.tmq_get_connect)(tmq)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_table_name(res: *mut TAOS_RES) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_get_table_name(res)
    } else {
        (CAPI.tmq_api.tmq_get_table_name)(res)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_res_type(res: *mut TAOS_RES) -> tmq_res_t {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_get_res_type(res)
    } else {
        (CAPI.tmq_api.tmq_get_res_type)(res)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_topic_name(res: *mut TAOS_RES) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_get_topic_name(res)
    } else {
        (CAPI.tmq_api.tmq_get_topic_name)(res)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_db_name(res: *mut TAOS_RES) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_get_db_name(res)
    } else {
        (CAPI.tmq_api.tmq_get_db_name)(res)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_vgroup_id(res: *mut TAOS_RES) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_get_vgroup_id(res)
    } else {
        (CAPI.tmq_api.tmq_get_vgroup_id)(res)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_vgroup_offset(res: *mut TAOS_RES) -> i64 {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_get_vgroup_offset(res)
    } else {
        (CAPI.tmq_api.tmq_get_vgroup_offset)(res)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_err2str(code: i32) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        tmq::tmq_err2str(code)
    } else {
        (CAPI.tmq_api.tmq_err2str)(code)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_raw(res: *mut TAOS_RES, raw: *mut tmq_raw_data) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        stub::tmq_get_raw(res, raw)
    } else {
        (CAPI.tmq_api.tmq_get_raw)(res, raw)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_write_raw(taos: *mut TAOS, raw: tmq_raw_data) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        stub::tmq_write_raw(taos, raw)
    } else {
        (CAPI.tmq_api.tmq_write_raw)(taos, raw)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_write_raw_block(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
) -> i32 {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_write_raw_block(taos, numOfRows, pData, tbname)
    } else {
        (CAPI.tmq_api.taos_write_raw_block)(taos, numOfRows, pData, tbname)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
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
        (CAPI.tmq_api.taos_write_raw_block_with_reqid)(taos, numOfRows, pData, tbname, reqid)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
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
        (CAPI.tmq_api.taos_write_raw_block_with_fields)(
            taos, rows, pData, tbname, fields, numFields,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
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
        (CAPI.tmq_api.taos_write_raw_block_with_fields_with_reqid)(
            taos, rows, pData, tbname, fields, numFields, reqid,
        )
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_free_raw(raw: tmq_raw_data) {
    if DRIVER.load(Ordering::Relaxed) {
        stub::tmq_free_raw(raw);
    } else {
        (CAPI.tmq_api.tmq_free_raw)(raw);
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_get_json_meta(res: *mut TAOS_RES) -> *const c_char {
    if DRIVER.load(Ordering::Relaxed) {
        stub::tmq_get_json_meta(res)
    } else {
        (CAPI.tmq_api.tmq_get_json_meta)(res)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn tmq_free_json_meta(jsonMeta: *mut c_char) {
    if DRIVER.load(Ordering::Relaxed) {
        stub::tmq_free_json_meta(jsonMeta);
    } else {
        (CAPI.tmq_api.tmq_free_json_meta)(jsonMeta);
    }
}
