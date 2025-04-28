use std::ffi::{c_char, c_void};

use crate::taos::{driver, CAPI, TAOS, TAOS_FIELD, TAOS_RES};
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
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_conf_new() -> *mut tmq_conf_t {
    if driver() {
        tmq::tmq_conf_new()
    } else {
        (CAPI.tmq_api.tmq_conf_new)()
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> tmq_conf_res_t {
    if driver() {
        tmq::tmq_conf_set(conf, key, value)
    } else {
        (CAPI.tmq_api.tmq_conf_set)(conf, key, value)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_conf_destroy(conf: *mut tmq_conf_t) {
    if driver() {
        tmq::tmq_conf_destroy(conf);
    } else {
        (CAPI.tmq_api.tmq_conf_destroy)(conf);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_conf_set_auto_commit_cb(
    conf: *mut tmq_conf_t,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    if driver() {
        tmq::tmq_conf_set_auto_commit_cb(conf, cb, param);
    } else {
        (CAPI.tmq_api.tmq_conf_set_auto_commit_cb)(conf, cb, param);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_list_new() -> *mut tmq_list_t {
    if driver() {
        tmq::tmq_list_new()
    } else {
        (CAPI.tmq_api.tmq_list_new)()
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_list_append(list: *mut tmq_list_t, value: *const c_char) -> i32 {
    if driver() {
        tmq::tmq_list_append(list, value)
    } else {
        (CAPI.tmq_api.tmq_list_append)(list, value)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_list_destroy(list: *mut tmq_list_t) {
    if driver() {
        tmq::tmq_list_destroy(list);
    } else {
        (CAPI.tmq_api.tmq_list_destroy)(list);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_list_get_size(list: *const tmq_list_t) -> i32 {
    if driver() {
        tmq::tmq_list_get_size(list)
    } else {
        (CAPI.tmq_api.tmq_list_get_size)(list)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_list_to_c_array(list: *const tmq_list_t) -> *mut *mut c_char {
    if driver() {
        tmq::tmq_list_to_c_array(list)
    } else {
        (CAPI.tmq_api.tmq_list_to_c_array)(list)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_consumer_new(
    conf: *mut tmq_conf_t,
    errstr: *mut c_char,
    errstrLen: i32,
) -> *mut tmq_t {
    if driver() {
        tmq::tmq_consumer_new(conf, errstr, errstrLen)
    } else {
        (CAPI.tmq_api.tmq_consumer_new)(conf, errstr, errstrLen)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_subscribe(tmq: *mut tmq_t, topic_list: *const tmq_list_t) -> i32 {
    if driver() {
        tmq::tmq_subscribe(tmq, topic_list)
    } else {
        (CAPI.tmq_api.tmq_subscribe)(tmq, topic_list)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_unsubscribe(tmq: *mut tmq_t) -> i32 {
    if driver() {
        tmq::tmq_unsubscribe(tmq)
    } else {
        (CAPI.tmq_api.tmq_unsubscribe)(tmq)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_subscription(tmq: *mut tmq_t, topics: *mut *mut tmq_list_t) -> i32 {
    if driver() {
        tmq::tmq_subscription(tmq, topics)
    } else {
        (CAPI.tmq_api.tmq_subscription)(tmq, topics)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_consumer_poll(tmq: *mut tmq_t, timeout: i64) -> *mut TAOS_RES {
    if driver() {
        tmq::tmq_consumer_poll(tmq, timeout)
    } else {
        (CAPI.tmq_api.tmq_consumer_poll)(tmq, timeout)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_consumer_close(tmq: *mut tmq_t) -> i32 {
    if driver() {
        tmq::tmq_consumer_close(tmq)
    } else {
        (CAPI.tmq_api.tmq_consumer_close)(tmq)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_commit_sync(tmq: *mut tmq_t, msg: *const TAOS_RES) -> i32 {
    if driver() {
        tmq::tmq_commit_sync(tmq, msg)
    } else {
        (CAPI.tmq_api.tmq_commit_sync)(tmq, msg)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_commit_async(
    tmq: *mut tmq_t,
    msg: *const TAOS_RES,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    if driver() {
        tmq::tmq_commit_async(tmq, msg, cb, param);
    } else {
        (CAPI.tmq_api.tmq_commit_async)(tmq, msg, cb, param);
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_commit_offset_sync(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    if driver() {
        tmq::tmq_commit_offset_sync(tmq, pTopicName, vgId, offset)
    } else {
        (CAPI.tmq_api.tmq_commit_offset_sync)(tmq, pTopicName, vgId, offset)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_commit_offset_async(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    if driver() {
        tmq::tmq_commit_offset_async(tmq, pTopicName, vgId, offset, cb, param);
    } else {
        (CAPI.tmq_api.tmq_commit_offset_async)(tmq, pTopicName, vgId, offset, cb, param);
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_topic_assignment(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    assignment: *mut *mut tmq_topic_assignment,
    numOfAssignment: *mut i32,
) -> i32 {
    if driver() {
        tmq::tmq_get_topic_assignment(tmq, pTopicName, assignment, numOfAssignment)
    } else {
        (CAPI.tmq_api.tmq_get_topic_assignment)(tmq, pTopicName, assignment, numOfAssignment)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_free_assignment(pAssignment: *mut tmq_topic_assignment) {
    if driver() {
        tmq::tmq_free_assignment(pAssignment);
    } else {
        (CAPI.tmq_api.tmq_free_assignment)(pAssignment);
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_offset_seek(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    if driver() {
        tmq::tmq_offset_seek(tmq, pTopicName, vgId, offset)
    } else {
        (CAPI.tmq_api.tmq_offset_seek)(tmq, pTopicName, vgId, offset)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_position(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
) -> i64 {
    if driver() {
        tmq::tmq_position(tmq, pTopicName, vgId)
    } else {
        (CAPI.tmq_api.tmq_position)(tmq, pTopicName, vgId)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_committed(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
) -> i64 {
    if driver() {
        tmq::tmq_committed(tmq, pTopicName, vgId)
    } else {
        (CAPI.tmq_api.tmq_committed)(tmq, pTopicName, vgId)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_connect(tmq: *mut tmq_t) -> *mut TAOS {
    if driver() {
        stub::tmq_get_connect(tmq)
    } else {
        (CAPI.tmq_api.tmq_get_connect)(tmq)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_table_name(res: *mut TAOS_RES) -> *const c_char {
    if driver() {
        tmq::tmq_get_table_name(res)
    } else {
        (CAPI.tmq_api.tmq_get_table_name)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_res_type(res: *mut TAOS_RES) -> tmq_res_t {
    if driver() {
        tmq::tmq_get_res_type(res)
    } else {
        (CAPI.tmq_api.tmq_get_res_type)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_topic_name(res: *mut TAOS_RES) -> *const c_char {
    if driver() {
        tmq::tmq_get_topic_name(res)
    } else {
        (CAPI.tmq_api.tmq_get_topic_name)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_db_name(res: *mut TAOS_RES) -> *const c_char {
    if driver() {
        tmq::tmq_get_db_name(res)
    } else {
        (CAPI.tmq_api.tmq_get_db_name)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_vgroup_id(res: *mut TAOS_RES) -> i32 {
    if driver() {
        tmq::tmq_get_vgroup_id(res)
    } else {
        (CAPI.tmq_api.tmq_get_vgroup_id)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_vgroup_offset(res: *mut TAOS_RES) -> i64 {
    if driver() {
        tmq::tmq_get_vgroup_offset(res)
    } else {
        (CAPI.tmq_api.tmq_get_vgroup_offset)(res)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_err2str(code: i32) -> *const c_char {
    if driver() {
        tmq::tmq_err2str(code)
    } else {
        (CAPI.tmq_api.tmq_err2str)(code)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_raw(res: *mut TAOS_RES, raw: *mut tmq_raw_data) -> i32 {
    if driver() {
        stub::tmq_get_raw(res, raw)
    } else {
        (CAPI.tmq_api.tmq_get_raw)(res, raw)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_write_raw(taos: *mut TAOS, raw: tmq_raw_data) -> i32 {
    if driver() {
        stub::tmq_write_raw(taos, raw)
    } else {
        (CAPI.tmq_api.tmq_write_raw)(taos, raw)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_write_raw_block(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
) -> i32 {
    if driver() {
        stub::taos_write_raw_block(taos, numOfRows, pData, tbname)
    } else {
        (CAPI.tmq_api.taos_write_raw_block)(taos, numOfRows, pData, tbname)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_write_raw_block_with_reqid(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    reqid: i64,
) -> i32 {
    if driver() {
        stub::taos_write_raw_block_with_reqid(taos, numOfRows, pData, tbname, reqid)
    } else {
        (CAPI.tmq_api.taos_write_raw_block_with_reqid)(taos, numOfRows, pData, tbname, reqid)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_write_raw_block_with_fields(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
) -> i32 {
    if driver() {
        stub::taos_write_raw_block_with_fields(taos, rows, pData, tbname, fields, numFields)
    } else {
        (CAPI.tmq_api.taos_write_raw_block_with_fields)(
            taos, rows, pData, tbname, fields, numFields,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_write_raw_block_with_fields_with_reqid(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
    reqid: i64,
) -> i32 {
    if driver() {
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
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_free_raw(raw: tmq_raw_data) {
    if driver() {
        stub::tmq_free_raw(raw);
    } else {
        (CAPI.tmq_api.tmq_free_raw)(raw);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_get_json_meta(res: *mut TAOS_RES) -> *const c_char {
    if driver() {
        stub::tmq_get_json_meta(res)
    } else {
        (CAPI.tmq_api.tmq_get_json_meta)(res)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn tmq_free_json_meta(jsonMeta: *mut c_char) {
    if driver() {
        stub::tmq_free_json_meta(jsonMeta);
    } else {
        (CAPI.tmq_api.tmq_free_json_meta)(jsonMeta);
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::ptr;
    use std::str::FromStr;
    use std::thread::sleep;
    use std::time::Duration;

    use super::*;
    use crate::taos::query::taos_free_result;
    use crate::taos::{taos_close, test_connect, test_exec_many};

    #[test]
    fn test_tmq_conf() {
        unsafe {
            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"td.connect.ip";
            let val = c"localhost";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.user";
            let val = c"root";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.pass";
            let val = c"taosdata";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"group.id";
            let val = c"1";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"client.id";
            let val = c"1";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"enable.auto.commit";
            let val = c"true";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"msg.with.table.name";
            let val = c"true";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"enable.replay";
            let val = c"true";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.port";
            let val = c"6041";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.commit.interval.ms";
            let val = c"5000";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"session.timeout.ms";
            let val = c"10000";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"max.poll.interval.ms";
            let val = c"10000";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset";
            let val = c"earliest";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            tmq_consumer_close(tmq);

            tmq_conf_destroy(conf);
        }

        unsafe {
            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id";
            let val = c"10";
            let res = tmq_conf_set(conf, key.as_ptr() as _, val.as_ptr() as _);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            tmq_consumer_close(tmq);

            tmq_conf_destroy(conf);
        }
    }

    #[test]
    fn test_tmq_list() {
        unsafe {
            let list = tmq_list_new();
            assert!(!list.is_null());

            let val = c"topic".as_ptr();
            let res = tmq_list_append(list, val);
            assert_eq!(res, 0);

            let size = tmq_list_get_size(list);
            assert_eq!(size, 1);

            let arr = tmq_list_to_c_array(list);
            assert!(!arr.is_null());

            let arr = tmq_list_to_c_array(list);
            assert!(!arr.is_null());

            tmq_list_destroy(list);
        }
    }

    #[test]
    #[ignore]
    fn test_tmq_subscribe() {
        unsafe {
            let db = "test_1737357513";
            let topic = "topic_1737357513";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    &format!("create topic {topic} as select * from t0"),
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, value.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let res = tmq_consumer_poll(consumer, 1000);
            let errno = tmq_commit_sync(consumer, res);
            assert_eq!(errno, 0);

            taos_free_result(res);

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );

            taos_close(taos);
        }
    }

    #[test]
    #[ignore]
    fn test_tmq_get_topic_assignment() {
        unsafe {
            let db = "test_1737423043";
            let topic = "topic_1737423043";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    &format!("create topic {topic} as select * from t0"),
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, value.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let topic_name = CString::from_str(topic).unwrap();
            let mut assignment = ptr::null_mut();
            let mut num_of_assignment = 0;

            let errno = tmq_get_topic_assignment(
                consumer,
                topic_name.as_ptr(),
                &mut assignment,
                &mut num_of_assignment,
            );
            assert_eq!(errno, 0);
            println!("assignment: {assignment:?}, num_of_assignment: {num_of_assignment}");

            tmq_free_assignment(assignment);

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );

            taos_close(taos);
        }
    }

    // FIXME
    // #[test]
    // fn test_tmq_offset_seek() {
    //     unsafe {
    //         let db = "test_1737440249";
    //         let topic = "topic_1737440249";

    //         let taos = test_connect();
    //         test_exec_many(
    //             taos,
    //             &[
    //                 &format!("drop topic if exists {topic}"),
    //                 &format!("drop database if exists {db}"),
    //                 &format!("create database {db}"),
    //                 &format!("use {db}"),
    //                 "create table t0 (ts timestamp, c1 int)",
    //                 "insert into t0 values (now, 1)",
    //                 &format!("create topic {topic} as select * from t0"),
    //             ],
    //         );

    //         let conf = tmq_conf_new();
    //         assert!(!conf.is_null());

    //         let key = c"group.id".as_ptr();
    //         let value = c"1".as_ptr();
    //         let res = tmq_conf_set(conf, key, value);
    //         assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

    //         let mut errstr = [0; 256];
    //         let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
    //         assert!(!consumer.is_null());

    //         let list = tmq_list_new();
    //         assert!(!list.is_null());

    //         let value = CString::from_str(topic).unwrap();
    //         let errno = tmq_list_append(list, value.as_ptr());
    //         assert_eq!(errno, 0);

    //         let errno = tmq_subscribe(consumer, list);
    //         assert_eq!(errno, 0);

    //         tmq_conf_destroy(conf);
    //         tmq_list_destroy(list);

    //         let topic_name = CString::from_str(topic).unwrap();
    //         let mut assignment = ptr::null_mut();
    //         let mut num_of_assignment = 0;

    //         let errno = tmq_get_topic_assignment(
    //             consumer,
    //             topic_name.as_ptr(),
    //             &mut assignment,
    //             &mut num_of_assignment,
    //         );
    //         assert_eq!(errno, 0);

    //         let (_, len) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
    //         let assigns = Vec::from_raw_parts(assignment, len, len);

    //         for assign in assigns {
    //             let offset = tmq_position(consumer, topic_name.as_ptr(), assign.vgId);
    //             assert!(offset >= 0);

    //             let errno =
    //                 tmq_offset_seek(consumer, topic_name.as_ptr(), assign.vgId, assign.begin);
    //             assert_eq!(errno, 0);
    //         }

    //         loop {
    //             let res = tmq_consumer_poll(consumer, 1000);
    //             if res.is_null() {
    //                 break;
    //             }

    //             let row = taos_fetch_row(res);
    //             assert!(!row.is_null());

    //             let fields = taos_fetch_fields(res);
    //             assert!(!fields.is_null());

    //             let num_fields = taos_num_fields(res);
    //             assert_eq!(num_fields, 2);

    //             let mut str = vec![0 as c_char; 1024];
    //             let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
    //             println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

    //             let errno = tmq_commit_sync(consumer, res);
    //             assert_eq!(errno, 0);

    //             taos_free_result(res);
    //         }

    //         let errno = tmq_unsubscribe(consumer);
    //         assert_eq!(errno, 0);

    //         let errno = tmq_consumer_close(consumer);
    //         assert_eq!(errno, 0);

    //         test_exec_many(
    //             taos,
    //             &[format!("drop topic {topic}"), format!("drop database {db}")],
    //         );

    //         taos_close(taos);
    //     }
    // }

    // FIXME
    // #[test]
    // fn test_tmq_commit_offset_sync() {
    //     unsafe {
    //         let db = "test_1737444552";
    //         let topic = "topic_1737444552";
    //         let table = "t0";

    //         let taos = test_connect();
    //         test_exec_many(
    //             taos,
    //             &[
    //                 &format!("drop topic if exists {topic}"),
    //                 &format!("drop database if exists {db}"),
    //                 &format!("create database {db}"),
    //                 &format!("use {db}"),
    //                 &format!("create table {table} (ts timestamp, c1 int)"),
    //                 &format!("insert into {table} values (now, 1)"),
    //                 &format!("insert into {table} values (now, 2)"),
    //                 &format!("create topic {topic} as database {db}"),
    //             ],
    //         );

    //         let conf = tmq_conf_new();
    //         assert!(!conf.is_null());

    //         let key = c"group.id".as_ptr();
    //         let value = c"1".as_ptr();
    //         let res = tmq_conf_set(conf, key, value);
    //         assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

    //         let key = c"auto.offset.reset".as_ptr();
    //         let value = c"earliest".as_ptr();
    //         let res = tmq_conf_set(conf, key, value);
    //         assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

    //         let mut errstr = [0; 256];
    //         let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
    //         assert!(!consumer.is_null());

    //         let list = tmq_list_new();
    //         assert!(!list.is_null());

    //         let value = CString::from_str(topic).unwrap();
    //         let errno = tmq_list_append(list, value.as_ptr());
    //         assert_eq!(errno, 0);

    //         let errno = tmq_subscribe(consumer, list);
    //         assert_eq!(errno, 0);

    //         tmq_conf_destroy(conf);
    //         tmq_list_destroy(list);

    //         let topic_name = CString::from_str(topic).unwrap();
    //         let mut assignment = ptr::null_mut();
    //         let mut num_of_assignment = 0;

    //         let errno = tmq_get_topic_assignment(
    //             consumer,
    //             topic_name.as_ptr(),
    //             &mut assignment,
    //             &mut num_of_assignment,
    //         );
    //         assert_eq!(errno, 0);

    //         let (_, len) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
    //         let assigns = Vec::from_raw_parts(assignment, len, len);

    //         let mut vg_ids = Vec::new();
    //         for assign in &assigns {
    //             vg_ids.push(assign.vgId);
    //         }

    //         loop {
    //             let res = tmq_consumer_poll(consumer, 1000);
    //             if res.is_null() {
    //                 break;
    //             }

    //             let table_name = tmq_get_table_name(res);
    //             assert!(!table_name.is_null());
    //             assert_eq!(
    //                 CStr::from_ptr(table_name),
    //                 CString::new(table).unwrap().as_c_str()
    //             );

    //             let db_name = tmq_get_db_name(res);
    //             assert!(!db_name.is_null());
    //             assert_eq!(
    //                 CStr::from_ptr(db_name),
    //                 CString::new(db).unwrap().as_c_str()
    //             );

    //             let res_type = tmq_get_res_type(res);
    //             assert_eq!(res_type, tmq_res_t::TMQ_RES_DATA);

    //             let topic_name = tmq_get_topic_name(res);
    //             assert!(!topic_name.is_null());
    //             assert_eq!(
    //                 CStr::from_ptr(topic_name),
    //                 CString::new(topic).unwrap().as_c_str()
    //             );

    //             let vg_id = tmq_get_vgroup_id(res);
    //             assert!(vg_ids.contains(&vg_id));

    //             let offset = tmq_get_vgroup_offset(res);
    //             assert_eq!(offset, 0);

    //             let mut current_offset = 0;
    //             for assign in &assigns {
    //                 if assign.vgId == vg_id {
    //                     current_offset = assign.currentOffset;
    //                     println!("current_offset: {current_offset}");
    //                     break;
    //                 }
    //             }

    //             let errno = tmq_commit_offset_sync(consumer, topic_name, vg_id, current_offset);
    //             assert_eq!(errno, 0);

    //             let committed_offset = tmq_committed(consumer, topic_name, vg_id);
    //             assert_eq!(committed_offset, current_offset);

    //             taos_free_result(res);
    //         }

    //         let errno = tmq_unsubscribe(consumer);
    //         assert_eq!(errno, 0);

    //         let errno = tmq_consumer_close(consumer);
    //         assert_eq!(errno, 0);

    //         test_exec_many(
    //             taos,
    //             &[format!("drop topic {topic}"), format!("drop database {db}")],
    //         );

    //         taos_close(taos);
    //     }
    // }

    #[test]
    fn test_tmq_subscription() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1741260674",
                    "drop database if exists test_1741260674",
                    "create database test_1741260674",
                    "use test_1741260674",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "insert into t0 values (now+1s, 2)",
                    "insert into t0 values (now+2s, 3)",
                    "insert into t0 values (now+3s, 4)",
                    "create topic topic_1741260674 as database test_1741260674",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let val = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let val = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = "topic_1741260674";
            let val = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, val.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(tmq, list);
            assert_eq!(errno, 0);

            let mut topics = ptr::null_mut();
            let code = tmq_subscription(tmq, &mut topics);
            assert_eq!(code, 0);

            let size = tmq_list_get_size(topics);
            assert_eq!(size, 1);

            let arr = tmq_list_to_c_array(topics);
            assert!(!arr.is_null());

            tmq_list_destroy(topics);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            sleep(Duration::from_secs(3));

            let errno = tmq_unsubscribe(tmq);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(tmq);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic topic_1741260674",
                    "drop database test_1741260674",
                ],
            );

            taos_close(taos);
        }

        // unsafe {
        //     let taos = test_connect();
        //     test_exec_many(
        //         taos,
        //         &[
        //             "drop topic if exists topic_1742281773",
        //             "drop database if exists test_1742281773",
        //             "create database test_1742281773",
        //             "use test_1742281773",
        //             "create table t0 (ts timestamp, c1 int)",
        //             "insert into t0 values (now, 1)",
        //             "insert into t0 values (now+1s, 2)",
        //             "insert into t0 values (now+2s, 3)",
        //             "insert into t0 values (now+3s, 4)",
        //             "create topic topic_1742281773 as database test_1742281773",
        //         ],
        //     );

        //     let conf = tmq_conf_new();
        //     assert!(!conf.is_null());

        //     let key = c"group.id".as_ptr();
        //     let val = c"10".as_ptr();
        //     let res = tmq_conf_set(conf, key, val);
        //     assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

        //     let key = c"auto.offset.reset".as_ptr();
        //     let val = c"earliest".as_ptr();
        //     let res = tmq_conf_set(conf, key, val);
        //     assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

        //     let mut errstr = [0; 256];
        //     let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
        //     assert!(!tmq.is_null());

        //     let list = tmq_list_new();
        //     assert!(!list.is_null());

        //     let topic = c"topic_1742281773";
        //     let errno = tmq_list_append(list, topic.as_ptr());
        //     assert_eq!(errno, 0);

        //     let errno = tmq_subscribe(tmq, list);
        //     assert_eq!(errno, 0);

        //     let mut topics = ptr::null_mut();
        //     let code = tmq_subscription(tmq, &mut topics);
        //     assert_eq!(code, 0);
        //     assert!(!topics.is_null());

        //     let size = tmq_list_get_size(topics);
        //     assert_eq!(size, 1);

        //     let maybe_err = Box::from_raw(topics as *mut TaosMaybeError<TmqList>);
        //     let topics = maybe_err.deref_mut().unwrap();
        //     assert_eq!(topics.topics, vec!["topic_1742281773"]);

        //     tmq_conf_destroy(conf);
        //     tmq_list_destroy(list);

        //     sleep(Duration::from_secs(3));

        //     let errno = tmq_unsubscribe(tmq);
        //     assert_eq!(errno, 0);

        //     let errno = tmq_consumer_close(tmq);
        //     assert_eq!(errno, 0);

        //     test_exec_many(
        //         taos,
        //         &[
        //             "drop topic topic_1742281773",
        //             "drop database test_1742281773",
        //         ],
        //     );

        //     taos_close(taos);
        // }
    }

    #[test]
    fn test_tmq_commit_async() {
        unsafe {
            extern "C" fn cb(tmq: *mut tmq_t, code: i32, param: *mut c_void) {
                unsafe {
                    assert!(!tmq.is_null());
                    assert_eq!(code, 0);

                    assert!(!param.is_null());
                    assert_eq!("hello", CStr::from_ptr(param as *const _).to_str().unwrap());
                }
            }

            let db = "test_1741264926";
            let topic = "topic_1741264926";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    &format!("create topic {topic} as select * from t0"),
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, value.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let res = tmq_consumer_poll(consumer, 1000);

            let param = c"hello";
            tmq_commit_async(consumer, res, cb, param.as_ptr() as _);

            sleep(Duration::from_secs(1));

            taos_free_result(res);

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );

            taos_close(taos);
        }
    }

    // FIXME
    // #[test]
    // fn test_tmq_commit_offset_async() {
    //     unsafe {
    //         extern "C" fn cb(tmq: *mut tmq_t, code: i32, param: *mut c_void) {
    //             unsafe {
    //                 assert!(!tmq.is_null());
    //                 assert_eq!(code, 0);

    //                 assert!(!param.is_null());
    //                 assert_eq!("hello", CStr::from_ptr(param as *const _).to_str().unwrap());
    //             }
    //         }

    //         let db = "test_1741271535";
    //         let topic = "topic_1741271535";
    //         let table = "t0";

    //         let taos = test_connect();
    //         test_exec_many(
    //             taos,
    //             &[
    //                 &format!("drop topic if exists {topic}"),
    //                 &format!("drop database if exists {db}"),
    //                 &format!("create database {db}"),
    //                 &format!("use {db}"),
    //                 &format!("create table {table} (ts timestamp, c1 int)"),
    //                 &format!("insert into {table} values (now, 1)"),
    //                 &format!("insert into {table} values (now, 2)"),
    //                 &format!("create topic {topic} as database {db}"),
    //             ],
    //         );

    //         let conf = tmq_conf_new();
    //         assert!(!conf.is_null());

    //         let key = c"group.id".as_ptr();
    //         let value = c"1".as_ptr();
    //         let res = tmq_conf_set(conf, key, value);
    //         assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

    //         let key = c"auto.offset.reset".as_ptr();
    //         let value = c"earliest".as_ptr();
    //         let res = tmq_conf_set(conf, key, value);
    //         assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

    //         let mut errstr = [0; 256];
    //         let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
    //         assert!(!consumer.is_null());

    //         let list = tmq_list_new();
    //         assert!(!list.is_null());

    //         let value = CString::from_str(topic).unwrap();
    //         let errno = tmq_list_append(list, value.as_ptr());
    //         assert_eq!(errno, 0);

    //         let errno = tmq_subscribe(consumer, list);
    //         assert_eq!(errno, 0);

    //         tmq_conf_destroy(conf);
    //         tmq_list_destroy(list);

    //         let topic_name = CString::from_str(topic).unwrap();
    //         let mut assignment = ptr::null_mut();
    //         let mut num_of_assignment = 0;

    //         let errno = tmq_get_topic_assignment(
    //             consumer,
    //             topic_name.as_ptr(),
    //             &mut assignment,
    //             &mut num_of_assignment,
    //         );
    //         assert_eq!(errno, 0);

    //         let (_, len) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
    //         let assigns = Vec::from_raw_parts(assignment, len, len);

    //         let mut vg_ids = Vec::new();
    //         for assign in &assigns {
    //             vg_ids.push(assign.vgId);
    //         }

    //         loop {
    //             let res = tmq_consumer_poll(consumer, 1000);
    //             if res.is_null() {
    //                 break;
    //             }

    //             let topic_name = tmq_get_topic_name(res);
    //             assert!(!topic_name.is_null());
    //             assert_eq!(
    //                 CStr::from_ptr(topic_name),
    //                 CString::new(topic).unwrap().as_c_str()
    //             );

    //             let vg_id = tmq_get_vgroup_id(res);
    //             assert!(vg_ids.contains(&vg_id));

    //             let mut current_offset = 0;
    //             for assign in &assigns {
    //                 if assign.vgId == vg_id {
    //                     current_offset = assign.currentOffset;
    //                     println!("current_offset: {current_offset}");
    //                     break;
    //                 }
    //             }

    //             let param = c"hello";
    //             tmq_commit_offset_async(
    //                 consumer,
    //                 topic_name,
    //                 vg_id,
    //                 current_offset,
    //                 cb,
    //                 param.as_ptr() as _,
    //             );

    //             sleep(Duration::from_secs(1));

    //             let committed_offset = tmq_committed(consumer, topic_name, vg_id);
    //             assert_eq!(committed_offset, current_offset);

    //             taos_free_result(res);
    //         }

    //         let errno = tmq_unsubscribe(consumer);
    //         assert_eq!(errno, 0);

    //         let errno = tmq_consumer_close(consumer);
    //         assert_eq!(errno, 0);

    //         test_exec_many(
    //             taos,
    //             &[format!("drop topic {topic}"), format!("drop database {db}")],
    //         );

    //         taos_close(taos);
    //     }
    // }

    // FIXME
    // #[test]
    // fn test_tmq_err2str() {
    //     unsafe {
    //         let errstr = tmq_err2str(0);
    //         assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "success");
    //     }

    //     unsafe {
    //         let errstr = tmq_err2str(-1);
    //         assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "fail");
    //     }

    //     unsafe {
    //         set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid para"));
    //         let errstr = tmq_err2str(format_errno(Code::INVALID_PARA.into()));
    //         assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "invalid para");
    //     }

    //     unsafe {
    //         set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid para"));
    //         let errstr = tmq_err2str(format_errno(Code::COLUMN_EXISTS.into()));
    //         assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "");
    //     }
    // }

    // #[test]
    // fn test_poll_auto_commit() {
    //     unsafe {
    //         extern "C" fn cb(tmq: *mut tmq_t, code: i32, param: *mut c_void) {
    //             unsafe {
    //                 println!("call auto commit callback");

    //                 assert!(!tmq.is_null());
    //                 assert_eq!(code, 0);

    //                 assert!(!param.is_null());
    //                 assert_eq!("hello", CStr::from_ptr(param as *const _).to_str().unwrap());
    //             }
    //         }

    //         let taos = test_connect();
    //         test_exec_many(
    //             taos,
    //             [
    //                 "drop topic if exists topic_1741333066",
    //                 "drop database if exists test_1741333066",
    //                 "create database test_1741333066 wal_retention_period 3600",
    //                 "use test_1741333066",
    //                 "create table t0 (ts timestamp, c1 int)",
    //                 "create topic topic_1741333066 as select * from t0",
    //             ],
    //         );

    //         let num = 9000;
    //         let ts = 1741336467000i64;
    //         for i in 0..num {
    //             test_exec(taos, format!("insert into t0 values ({}, 1)", ts + i));
    //         }

    //         test_exec_many(
    //             taos,
    //             [
    //                 "drop database if exists test_1741333142",
    //                 "create database test_1741333142 wal_retention_period 3600",
    //                 "use test_1741333142",
    //             ],
    //         );

    //         let conf = tmq_conf_new();
    //         assert!(!conf.is_null());

    //         let key = c"group.id".as_ptr();
    //         let val = c"10".as_ptr();
    //         let res = tmq_conf_set(conf, key, val);
    //         assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

    //         let key = c"auto.offset.reset".as_ptr();
    //         let val = c"earliest".as_ptr();
    //         let res = tmq_conf_set(conf, key, val);
    //         assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

    //         let key = c"enable.auto.commit".as_ptr();
    //         let val = c"true".as_ptr();
    //         let res = tmq_conf_set(conf, key, val);
    //         assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

    //         let key = c"auto.commit.interval.ms".as_ptr();
    //         let val = c"1".as_ptr();
    //         let res = tmq_conf_set(conf, key, val);
    //         assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

    //         let param = c"hello";
    //         tmq_conf_set_auto_commit_cb(conf, cb, param.as_ptr() as _);

    //         let mut errstr = [0; 256];
    //         let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
    //         assert!(!tmq.is_null());

    //         let list = tmq_list_new();
    //         assert!(!list.is_null());

    //         let value = CString::from_str("topic_1741333066").unwrap();
    //         let code = tmq_list_append(list, value.as_ptr());
    //         assert_eq!(code, 0);

    //         let code = tmq_subscribe(tmq, list);
    //         assert_eq!(code, 0);

    //         tmq_conf_destroy(conf);
    //         tmq_list_destroy(list);

    //         let topic = CString::from_str("topic_1741333066").unwrap();
    //         let mut assignment = ptr::null_mut();
    //         let mut num_of_assignment = 0;

    //         let code = tmq_get_topic_assignment(
    //             tmq,
    //             topic.as_ptr(),
    //             &mut assignment,
    //             &mut num_of_assignment,
    //         );
    //         assert_eq!(code, 0);

    //         let (_, len) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
    //         let assigns = Vec::from_raw_parts(assignment, len, len);

    //         let mut vg_ids = Vec::new();
    //         for assign in &assigns {
    //             vg_ids.push(assign.vgId);
    //         }

    //         let topic = topic.as_ptr();
    //         let mut pre_vg_id = None;
    //         let mut pre_offset = 0;

    //         loop {
    //             let res = tmq_consumer_poll(tmq, 1000);
    //             println!("poll res: {res:?}");
    //             if res.is_null() {
    //                 break;
    //             }

    //             let vg_id = tmq_get_vgroup_id(res);
    //             assert!(vg_ids.contains(&vg_id));

    //             if let Some(pre_vg_id) = pre_vg_id {
    //                 let offset = tmq_committed(tmq, topic, pre_vg_id);
    //                 assert!(offset >= pre_offset);
    //             }

    //             pre_vg_id = Some(vg_id);
    //             pre_offset = tmq_get_vgroup_offset(res);

    //             taos_free_result(res);
    //         }

    //         let code = tmq_unsubscribe(tmq);
    //         assert_eq!(code, 0);

    //         let code = tmq_consumer_close(tmq);
    //         assert_eq!(code, 0);

    //         sleep(Duration::from_secs(3));

    //         test_exec_many(
    //             taos,
    //             [
    //                 "drop topic topic_1741333066",
    //                 "drop database test_1741333066",
    //             ],
    //         );

    //         taos_close(taos);
    //     }
    // }

    #[test]
    #[ignore]
    fn test_show_consumers() {
        unsafe {
            let _ = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_line_number(true)
                .with_file(true)
                .try_init();

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1744006630",
                    "drop topic if exists topic_1744008820",
                    "drop database if exists test_1744006630",
                    "create database test_1744006630 wal_retention_period 3600",
                    "create topic topic_1744006630 with meta as database test_1744006630",
                    "create topic topic_1744008820 with meta as database test_1744006630",
                    "use test_1744006630",
                    "create table t0(ts timestamp, c1 int)",
                    "insert into t0 values(now, 1)",
                    "insert into t0 values(now+1s, 1)",
                    "insert into t0 values(now+2s, 1)",
                    "insert into t0 values(now+3s, 1)",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"topic_1744006630";
            let errno = tmq_list_append(list, topic.as_ptr());
            assert_eq!(errno, 0);

            let topic = c"topic_1744008820";
            let errno = tmq_list_append(list, topic.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                tracing::debug!("poll res: {res:?}");
                if res.is_null() {
                    break;
                }

                if !res.is_null() {
                    let errno = tmq_commit_sync(consumer, res);
                    assert_eq!(errno, 0);

                    taos_free_result(res);
                }
            }

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic topic_1744006630",
                    "drop topic topic_1744008820",
                    "drop database test_1744006630",
                ],
            );

            taos_close(taos);
        }
    }
}
