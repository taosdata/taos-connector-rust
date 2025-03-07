use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::str::FromStr;
use std::time::Duration;
use std::{mem, ptr};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use taos_error::Code;
use taos_query::common::{Precision, RawBlock as Block, Ty};
use taos_query::tmq::{self, AsConsumer, IsData, IsOffset};
use taos_query::{global_tokio_runtime, Dsn, TBuilder};
use taos_ws::consumer::Data;
use taos_ws::query::Error;
use taos_ws::{Consumer, Offset, TmqBuilder};
use tracing::{error, instrument, trace, warn, Instrument};

use crate::ws::error::{format_errno, set_err_and_get_code, TaosError, TaosMaybeError};
use crate::ws::{
    ResultSet, ResultSetOperations, Row, SafePtr, TaosResult, TAOS_FIELD, TAOS_RES, TAOS_ROW,
};

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

pub const TSDB_CLIENT_ID_LEN: usize = 256;
pub const TSDB_CGROUP_LEN: usize = 193;
pub const TSDB_USER_LEN: usize = 24;
pub const TSDB_FQDN_LEN: usize = 128;
pub const TSDB_PASSWORD_LEN: usize = 32;
pub const TSDB_VERSION_LEN: usize = 32;

pub const TSDB_ACCT_ID_LEN: usize = 11;
pub const TSDB_DB_NAME_LEN: usize = 65;
pub const TSDB_NAME_DELIMITER_LEN: usize = 1;
pub const TSDB_DB_FNAME_LEN: usize = TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN;

pub const TSDB_MAX_REPLICA: usize = 5;

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_conf_new() -> *mut tmq_conf_t {
    trace!("tmq_conf_new");
    let tmq_conf: TaosMaybeError<TmqConf> = TmqConf::new().into();
    trace!(tmq_conf=?tmq_conf, "tmq_conf_new done");
    Box::into_raw(Box::new(tmq_conf)) as _
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> tmq_conf_res_t {
    trace!(conf=?conf, key=?key, value=?value, "tmq_conf_set");

    if conf.is_null() || key.is_null() || value.is_null() {
        return tmq_conf_res_t::TMQ_CONF_INVALID;
    }

    match _tmq_conf_set(conf, key, value) {
        Ok(_) => tmq_conf_res_t::TMQ_CONF_OK,
        Err(err) => {
            trace!(err=?err, "tmq_conf_set failed");
            match err.code() {
                Code::INVALID_PARA => tmq_conf_res_t::TMQ_CONF_INVALID,
                _ => tmq_conf_res_t::TMQ_CONF_UNKNOWN,
            }
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_conf_destroy(conf: *mut tmq_conf_t) {
    trace!(conf=?conf, "tmq_conf_destroy");
    if !conf.is_null() {
        let _ = unsafe { Box::from_raw(conf as *mut TaosMaybeError<TmqConf>) };
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_conf_set_auto_commit_cb(
    conf: *mut tmq_conf_t,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    todo!()
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_list_new() -> *mut tmq_list_t {
    trace!("tmq_list_new");
    let tmq_list: TaosMaybeError<TmqList> = TmqList::new().into();
    trace!(tmq_list=?tmq_list, "tmq_list_new done");
    Box::into_raw(Box::new(tmq_list)) as _
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_list_append(list: *mut tmq_list_t, value: *const c_char) -> i32 {
    trace!(list=?list, value=?value, "tmq_list_append");

    if list.is_null() || value.is_null() {
        return format_errno(Code::OBJECT_IS_NULL.into());
    }

    trace!("tmq_list_append value={:?}", CStr::from_ptr(value));

    match _tmq_list_append(list, value) {
        Ok(_) => Code::SUCCESS.into(),
        Err(err) => set_err_and_get_code(err),
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_list_destroy(list: *mut tmq_list_t) {
    trace!("tmq_list_destroy start, list: {list:?}");
    if !list.is_null() {
        let list = unsafe { Box::from_raw(list as *mut TaosMaybeError<TmqList>) };
        trace!("tmq_list_destroy succ, list: {list:?}");
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_list_get_size(list: *const tmq_list_t) -> i32 {
    trace!(list=?list, "tmq_list_get_size");
    match (list as *mut TaosMaybeError<TmqList>)
        .as_mut()
        .and_then(|list| list.deref_mut())
    {
        Some(list) => {
            trace!(list=?list, "tmq_list_get_size done");
            list.topics.len() as i32
        }
        None => set_err_and_get_code(TaosError::new(Code::FAILED, "list is null")),
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_list_to_c_array(list: *const tmq_list_t) -> *mut *mut c_char {
    trace!(list=?list, "tmq_list_to_c_array");

    if list.is_null() {
        return ptr::null_mut();
    }

    match (list as *const TaosMaybeError<TmqList>)
        .as_ref()
        .and_then(|list| list.deref())
    {
        Some(list) => {
            if !list.topics.is_empty() {
                let mut array: Vec<*mut c_char> = list
                    .topics
                    .iter()
                    .map(|s| CString::new(&**s).unwrap().into_raw())
                    .collect();

                let ptr = array.as_mut_ptr();
                mem::forget(array);
                ptr
            } else {
                ptr::null_mut()
            }
        }
        None => ptr::null_mut(),
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
    trace!(conf=?conf, errstr=?errstr, errstr_len=errstrLen, "tmq_consumer_new");
    match _tmq_consumer_new(conf) {
        Ok(tmq) => {
            let tmq: TaosMaybeError<Tmq> = tmq.into();
            Box::into_raw(Box::new(tmq)) as _
        }
        Err(err) => {
            trace!(err=?err, "tmq_consumer_new failed");
            if errstrLen > 0 && !errstr.is_null() {
                let message = CString::new(err.to_string()).unwrap();
                let count = message.to_bytes().len().min(errstrLen as usize - 1);
                ptr::copy(message.as_ptr(), errstr, count);
                *errstr.add(count) = 0;
            }
            set_err_and_get_code(err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_subscribe(tmq: *mut tmq_t, topic_list: *const tmq_list_t) -> i32 {
    trace!("tmq_subscribe start, tmq: {tmq:?}, topic_list: {topic_list:?}");

    if tmq.is_null() || topic_list.is_null() {
        error!("tmq_subscribe failed, err: tmq or topic_list is null");
        return format_errno(Code::INVALID_PARA.into());
    }

    let tmq_list = match (topic_list as *const TaosMaybeError<TmqList>)
        .as_ref()
        .and_then(|list| list.deref())
    {
        Some(tmq_list) => tmq_list,
        None => {
            error!("tmq_subscribe failed, err: topic_list as *const TaosMaybeError<TmqList>");
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => {
            if let Some(consumer) = &mut tmq.consumer {
                match consumer.subscribe(&tmq_list.topics) {
                    Ok(_) => {
                        trace!("tmq_subscribe done");
                        Code::SUCCESS.into()
                    }
                    Err(err) => {
                        error!("tmq_subscribe failed, err: {:?}", err.message());
                        set_err_and_get_code(TaosError::new(Code::FAILED, &err.message()))
                    }
                }
            } else {
                error!("tmq_subscribe failed, err: invalid consumer");
                set_err_and_get_code(TaosError::new(Code::FAILED, "invalid consumer"))
            }
        }
        None => {
            error!("tmq_subscribe failed, err: tmq as *mut TaosMaybeError<Tmq>");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_unsubscribe(tmq: *mut tmq_t) -> i32 {
    trace!("tmq_unsubscribe start, tmq: {tmq:?}");

    if tmq.is_null() {
        error!("tmq_unsubscribe failed, err: tmq is null");
        return format_errno(Code::INVALID_PARA.into());
    }

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => {
            if let Some(consumer) = tmq.consumer.take() {
                consumer.unsubscribe();
            }
            trace!("tmq_unsubscribe done");
            Code::SUCCESS.into()
        }
        None => {
            error!("tmq_unsubscribe failed, err: tmq as *mut TaosMaybeError<Tmq>");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_subscription(tmq: *mut tmq_t, topics: *mut *mut tmq_list_t) -> i32 {
    trace!("tmq_subscription start, tmq: {tmq:?}, topics: {topics:?}");

    if tmq.is_null() {
        error!("tmq_subscription failed, err: tmq is null");
        return format_errno(Code::INVALID_PARA.into());
    }

    if topics.is_null() {
        error!("tmq_subscription failed, err: topics is null");
        return format_errno(Code::INVALID_PARA.into());
    }

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => {
            if let Some(consumer) = tmq.consumer.take() {
                let topic_list = consumer.list_topics().unwrap();
                *topics = tmq_list_new();
                for t in topic_list {
                    let val = CString::new(t).unwrap();
                    let code = tmq_list_append(*topics, val.as_ptr());
                    if code != 0 {
                        error!(
                            "tmq_subscription failed, err: tmq_list_append failed, code: {code}"
                        );
                        return code;
                    }
                }
            }
            trace!("tmq_subscription succ");
            Code::SUCCESS.into()
        }
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is invalid")),
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_consumer_poll(tmq: *mut tmq_t, timeout: i64) -> *mut TAOS_RES {
    trace!("tmq_consumer_poll start, tmq: {tmq:?}, timeout: {timeout}");

    if tmq.is_null() {
        error!("tmq_consumer_poll failed, err: tmq is null");
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
        return ptr::null_mut();
    }

    match _tmq_consumer_poll(tmq, timeout) {
        Ok(Some(rs)) => {
            let rs: TaosMaybeError<ResultSet> = rs.into();
            trace!("tmq_consumer_poll done, rs: {rs:?}");
            Box::into_raw(Box::new(rs)) as _
        }
        Ok(None) => {
            trace!("tmq_consumer_poll done, no ResultSet");
            ptr::null_mut()
        }
        Err(err) => {
            error!("tmq_consumer_poll failed, err: {err:?}");
            set_err_and_get_code(err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_consumer_close(tmq: *mut tmq_t) -> i32 {
    trace!("tmq_consumer_close start, tmq: {tmq:?}");

    if tmq.is_null() {
        error!("tmq_consumer_close failed, err: tmq is null");
        return format_errno(Code::INVALID_PARA.into());
    }

    let _ = unsafe { Box::from_raw(tmq as *mut TaosMaybeError<Tmq>) };
    trace!("tmq_consumer_close done");
    Code::SUCCESS.into()
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_commit_sync(tmq: *mut tmq_t, msg: *const TAOS_RES) -> i32 {
    trace!("tmq_commit_sync start, tmq: {tmq:?}, msg: {msg:?}");

    if tmq.is_null() {
        error!("tmq_commit_sync failed, err: tmq is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
    }

    match _tmq_commit_sync(tmq, msg) {
        Ok(_) => {
            trace!("tmq_commit_sync done");
            Code::SUCCESS.into()
        }
        Err(err) => {
            error!("tmq_commit_sync failed, err: {err:?}");
            set_err_and_get_code(err)
        }
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
    trace!("tmq_commit_async start, tmq: {tmq:?}, msg: {msg:?}, cb: {cb:?}, param: {param:?}");

    if tmq.is_null() {
        error!("tmq_commit_async failed, err: tmq is null");
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
        return;
    }

    let tmq = SafePtr(tmq);
    let res = SafePtr(msg);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let tmq = tmq;
            let res = res;
            let param = param;

            let offset = (res.0 as *const TaosMaybeError<ResultSet>)
                .as_ref()
                .and_then(|rs| rs.deref())
                .map(ResultSetOperations::tmq_get_offset);

            trace!("tmq_commit_async, offset: {offset:?}");

            let maybe_err = (tmq.0 as *mut TaosMaybeError<Tmq>).as_mut().unwrap();

            let tmq1 = match maybe_err.deref_mut() {
                Some(tmq) => tmq,
                None => {
                    error!("tmq_commit_async failed, err: tmq is invalid");
                    maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "tmq is invalid")));
                    let code = format_errno(Code::INVALID_PARA.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            let consumer = match &mut tmq1.consumer {
                Some(consumer) => consumer,
                None => {
                    error!("tmq_commit_async failed, err: consumer is none");
                    maybe_err.with_err(Some(TaosError::new(Code::FAILED, "consumer is none")));
                    let code = format_errno(Code::FAILED.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            match offset {
                Some(offset) => {
                    match taos_query::tmq::AsAsyncConsumer::commit(consumer, offset).await {
                        Ok(_) => {
                            trace!("tmq_commit_async commit callback");
                            cb(tmq.0, 0, param.0);
                        }
                        Err(err) => {
                            error!("tmq_commit_async commit failed, err: {err:?}");
                            let code = format_errno(err.code().into());
                            cb(tmq.0, code, param.0);
                        }
                    }
                }
                None => match taos_query::tmq::AsAsyncConsumer::commit_all(consumer).await {
                    Ok(_) => {
                        trace!("tmq_commit_async commit all callback, commit all succ");
                        cb(tmq.0, 0, param.0);
                    }
                    Err(err) => {
                        error!("tmq_commit_async commit all failed, err: {err:?}");
                        let code = format_errno(err.code().into());
                        cb(tmq.0, code, param.0);
                    }
                },
            }
        }
        .in_current_span(),
    );

    trace!("tmq_commit_async succ");
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
    trace!("tmq_commit_offset_sync start, tmq: {tmq:?}, p_topic_name: {pTopicName:?}, vg_id: {vgId}, offset: {offset}");

    if tmq.is_null() {
        error!("tmq_commit_offset_sync failed, err: tmq is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
    }

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_commit_offset_sync failed, err: invalid tmq");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid tmq"));
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_commit_offset_sync failed, err: invalid data");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid data"));
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_commit_offset_sync failed, err: invalid consumer");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid consumer"));
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_commit_offset_sync failed, err: invalid topic name");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid topic name"));
        }
    };

    match consumer.commit_offset(topic_name, vgId, offset) {
        Ok(_) => {
            trace!("tmq_commit_offset_sync done");
            Code::SUCCESS.into()
        }
        Err(err) => {
            error!("tmq_commit_offset_sync failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message()))
        }
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
    trace!(
        "tmq_commit_offset_async start, tmq: {tmq:?}, p_topic_name: {pTopicName:?}, vg_id: {vgId}, offset: {offset}, cb: {cb:?}, param: {param:?}"
    );

    if tmq.is_null() {
        error!("tmq_commit_offset_async failed, err: tmq is null");
        return;
    }

    if pTopicName.is_null() {
        error!("tmq_commit_offset_async failed, err: p_topic_name is null");
        return;
    }

    let tmq = SafePtr(tmq);
    let p_topic_name = SafePtr(pTopicName);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let tmq = tmq;
            let p_topic_name = p_topic_name;
            let param = param;

            let maybe_err = (tmq.0 as *mut TaosMaybeError<Tmq>).as_mut().unwrap();

            let tmq1 = match maybe_err.deref_mut() {
                Some(tmq) => tmq,
                None => {
                    error!("tmq_commit_offset_async failed, err: tmq is invalid");
                    maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "tmq is invalid")));
                    let code = format_errno(Code::INVALID_PARA.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            let consumer = match &mut tmq1.consumer {
                Some(consumer) => consumer,
                None => {
                    error!("tmq_commit_offset_async failed, err: consumer is none");
                    maybe_err.with_err(Some(TaosError::new(Code::FAILED, "consumer is none")));
                    let code = format_errno(Code::FAILED.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            let topic = match CStr::from_ptr(p_topic_name.0).to_str() {
                Ok(topic) => topic,
                Err(_) => {
                    error!("tmq_commit_offset_async failed, err: p_topic_name is invalid utf-8");
                    maybe_err.with_err(Some(TaosError::new(
                        Code::INVALID_PARA,
                        "pTopicName is invalid utf-8",
                    )));
                    let code = format_errno(Code::INVALID_PARA.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            match taos_query::tmq::AsAsyncConsumer::commit_offset(consumer, topic, vgId, offset)
                .await
            {
                Ok(_) => {
                    trace!("tmq_commit_offset_async callback");
                    cb(tmq.0, 0, param.0);
                }
                Err(err) => {
                    error!("tmq_commit_offset_async failed, err: {err:?}");
                    maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                    let code = format_errno(err.code().into());
                    cb(tmq.0, code, param.0);
                }
            }
        }
        .in_current_span(),
    );

    trace!("tmq_commit_offset_async succ");
}

static TOPIC_ASSIGNMETN_MAP: Lazy<DashMap<usize, (usize, usize)>> = Lazy::new(DashMap::new);

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_topic_assignment(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    assignment: *mut *mut tmq_topic_assignment,
    numOfAssignment: *mut i32,
) -> i32 {
    trace!(
        "tmq_get_topic_assignment start, tmq: {:?}, p_topic_name: {:?}, assignment: {:?}, num_of_assignment: {}",
        tmq, CStr::from_ptr(pTopicName), assignment, *numOfAssignment
    );

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => match &mut tmq.consumer {
            Some(consumer) => match consumer.assignments() {
                Some(assigns) => {
                    if assigns.is_empty() {
                        *assignment = ptr::null_mut();
                        *numOfAssignment = 0;
                    } else {
                        let (_, assigns) = assigns.first().unwrap().clone();
                        let len = assigns.len();
                        let cap = assigns.capacity();

                        trace!("tmq_get_topic_assignment, assigns: {assigns:?}, len: {len}, cap: {cap}");

                        *numOfAssignment = len as _;
                        *assignment = Box::into_raw(assigns.into_boxed_slice()) as _;

                        TOPIC_ASSIGNMETN_MAP.insert(*assignment as usize, (len, cap));
                    }

                    trace!(
                        "tmq_get_topic_assignment done, assignment: {:?}, num_of_assignment: {}",
                        *assignment,
                        *numOfAssignment
                    );

                    Code::SUCCESS.into()
                }
                None => {
                    *assignment = ptr::null_mut();
                    *numOfAssignment = 0;
                    trace!("tmq_get_topic_assignment done, no assignment");
                    Code::SUCCESS.into()
                }
            },
            None => {
                error!("tmq_get_topic_assignment failed, err: invalid consumer");
                set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid consumer"))
            }
        },
        None => {
            error!("tmq_get_topic_assignment failed, err: invalid tmq");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid tmq"))
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_free_assignment(pAssignment: *mut tmq_topic_assignment) {
    trace!("tmq_free_assignment start, p_assignment: {pAssignment:?}");
    if pAssignment.is_null() {
        trace!("tmq_free_assignment done, p_assignment is null");
        return;
    }

    let (_, (len, cap)) = TOPIC_ASSIGNMETN_MAP
        .remove(&(pAssignment as usize))
        .unwrap();
    let assigns = Vec::from_raw_parts(pAssignment, len, cap);
    trace!("tmq_free_assignment done, assigns: {assigns:?}, len: {len}, cap: {cap}");
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
    trace!(
        "tmq_offset_seek start, tmq: {:?}, p_topic_name: {:?}, vg_id: {}, offset: {}",
        tmq,
        CStr::from_ptr(pTopicName),
        vgId,
        offset
    );

    if tmq.is_null() {
        error!("tmq_offset_seek failed, err: tmq is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
    }

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_offset_seek failed, err: invalid tmq");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid tmq"));
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_offset_seek failed, err: invalid data");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid data"));
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_offset_seek failed, err: invalid consumer");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid consumer"));
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_offset_seek failed, err: invalid topic name");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid topic name"));
        }
    };

    match consumer.offset_seek(topic_name, vgId, offset) {
        Ok(_) => {
            trace!("tmq_offset_seek done");
            Code::SUCCESS.into()
        }
        Err(err) => {
            error!("tmq_offset_seek failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message()))
        }
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
    trace!(
        "tmq_position start, tmq: {:?}, p_topic_name: {:?}, vg_id: {}",
        tmq,
        CStr::from_ptr(pTopicName),
        vgId
    );

    if tmq.is_null() {
        error!("tmq_position failed, err: tmq is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null")) as _;
    }

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_position failed, err: invalid tmq");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid tmq")) as _;
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_position failed, err: invalid data");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid data")) as _;
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_position failed, err: invalid consumer");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid consumer"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_position failed, err: invalid topic name");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid topic name"))
                as _;
        }
    };

    match consumer.position(topic_name, vgId) {
        Ok(offset) => {
            trace!("tmq_position done, offset: {offset}");
            offset
        }
        Err(err) => {
            error!("tmq_position failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message())) as _
        }
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
    trace!(
        "tmq_committed start, tmq: {:?}, p_topic_name: {:?}, vg_id: {}",
        tmq,
        CStr::from_ptr(pTopicName),
        vgId
    );

    if tmq.is_null() {
        error!("tmq_committed failed, err: tmq is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null")) as _;
    }

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_committed failed, err: invalid tmq");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid tmq")) as _;
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_committed failed, err: invalid data");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid data")) as _;
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_committed failed, err: invalid consumer");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid consumer"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_committed failed, err: invalid topic name");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid topic name"))
                as _;
        }
    };

    match consumer.committed(topic_name, vgId) {
        Ok(offset) => {
            trace!("tmq_committed done, offset: {offset}");
            offset
        }
        Err(err) => {
            error!("tmq_committed failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message())) as _
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_table_name(res: *mut TAOS_RES) -> *const c_char {
    trace!("tmq_get_table_name start, res: {res:?}");

    if res.is_null() {
        trace!("tmq_get_table_name done, res is null");
        return ptr::null();
    }

    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            trace!("tmq_get_table_name done, rs: {rs:?}");
            rs.tmq_get_table_name()
        }
        None => {
            warn!("tmq_get_table_name failed, err: invalid res");
            ptr::null()
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_get_res_type(res: *mut TAOS_RES) -> tmq_res_t {
    trace!("tmq_get_res_type start, res: {res:?}");
    if res.is_null() {
        trace!("tmq_get_res_type done, res is null");
        return tmq_res_t::TMQ_RES_INVALID;
    }
    trace!("tmq_get_res_type done, res type: TMQ_RES_DATA");
    tmq_res_t::TMQ_RES_DATA
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_topic_name(res: *mut TAOS_RES) -> *const c_char {
    trace!("tmq_get_topic_name start, res: {res:?}");

    if res.is_null() {
        trace!("tmq_get_topic_name done, res is null");
        return ptr::null();
    }

    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            trace!("tmq_get_topic_name done, rs: {rs:?}");
            rs.tmq_get_topic_name()
        }
        None => {
            warn!("tmq_get_topic_name failed, err: invalid res");
            ptr::null()
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_db_name(res: *mut TAOS_RES) -> *const c_char {
    trace!("tmq_get_db_name start, res: {res:?}");

    if res.is_null() {
        trace!("tmq_get_db_name done, res is null");
        return ptr::null();
    }

    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            trace!("tmq_get_db_name done, rs: {rs:?}");
            rs.tmq_get_db_name()
        }
        None => {
            warn!("tmq_get_db_name failed, err: invalid res");
            ptr::null()
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_vgroup_id(res: *mut TAOS_RES) -> i32 {
    trace!("tmq_get_vgroup_id start, res: {res:?}");

    if res.is_null() {
        trace!("tmq_get_vgroup_id done, res is null");
        return format_errno(Code::INVALID_PARA.into());
    }

    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            trace!("tmq_get_vgroup_id done, rs: {rs:?}");
            rs.tmq_get_vgroup_id()
        }
        None => {
            error!("tmq_get_vgroup_id failed, err: invalid res");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid res"))
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn tmq_get_vgroup_offset(res: *mut TAOS_RES) -> i64 {
    trace!("tmq_get_vgroup_offset start, res: {res:?}");

    if res.is_null() {
        trace!("tmq_get_vgroup_offset done, res is null");
        return format_errno(Code::INVALID_PARA.into()) as _;
    }

    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            trace!("tmq_get_vgroup_offset done, rs: {rs:?}");
            rs.tmq_get_vgroup_offset()
        }
        None => {
            error!("tmq_get_vgroup_offset failed, err: invalid res");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid res")) as _
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn tmq_err2str(code: i32) -> *const c_char {
    todo!()
}

#[derive(Debug)]
pub struct TmqResultSet {
    block: Option<Block>,
    fields: Vec<TAOS_FIELD>,
    num_of_fields: i32,
    precision: Precision,
    offset: Offset,
    row: Row,
    data: Data,
    table_name: Option<CString>,
    topic_name: Option<CString>,
    db_name: Option<CString>,
}

impl TmqResultSet {
    fn new(block: Block, offset: Offset, data: Data) -> Self {
        let mut fields = Vec::new();
        fields.extend(block.fields().iter().map(TAOS_FIELD::from));

        let num_of_fields = block.ncols();
        let row_data = vec![ptr::null(); num_of_fields];
        let precision = block.precision();
        let table_name = block.table_name().and_then(|name| CString::new(name).ok());
        let topic_name = CString::new(offset.topic()).ok();
        let db_name = CString::new(offset.database()).ok();

        Self {
            block: Some(block),
            fields,
            num_of_fields: num_of_fields as i32,
            precision,
            offset,
            row: Row::new(row_data, 0),
            data,
            table_name,
            topic_name,
            db_name,
        }
    }
}

impl ResultSetOperations for TmqResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        match self.topic_name {
            Some(ref name) => name.as_ptr(),
            None => ptr::null(),
        }
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        match self.db_name {
            Some(ref name) => name.as_ptr(),
            None => ptr::null(),
        }
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        match self.table_name {
            Some(ref name) => name.as_ptr(),
            None => ptr::null(),
        }
    }

    fn tmq_get_offset(&self) -> Offset {
        self.offset.clone()
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        self.offset.offset()
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        self.offset.vgroup_id()
    }

    fn precision(&self) -> Precision {
        self.precision
    }

    fn affected_rows(&self) -> i32 {
        0
    }

    fn affected_rows64(&self) -> i64 {
        0
    }

    fn num_of_fields(&self) -> i32 {
        self.num_of_fields
    }

    fn get_fields(&mut self) -> *mut TAOS_FIELD {
        self.fields.as_mut_ptr()
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *mut c_void, rows: *mut i32) -> Result<(), Error> {
        self.block = self.data.fetch_raw_block()?;
        if let Some(block) = self.block.as_ref() {
            *ptr = block.as_raw_bytes().as_ptr() as _;
            *rows = block.nrows() as _;
        } else {
            *rows = 0;
        }
        Ok(())
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        if self.block.is_none() || self.row.current_row >= self.block.as_ref().unwrap().nrows() {
            self.block = self.data.fetch_raw_block()?;
            self.row.current_row = 0;
        }

        match self.block.as_ref() {
            Some(block) => {
                if block.nrows() == 0 {
                    return Ok(ptr::null_mut());
                }

                for col in 0..block.ncols() {
                    let value = block.get_raw_value_unchecked(self.row.current_row, col);
                    self.row.data[col] = value.2;
                }

                self.row.current_row += 1;
                Ok(self.row.data.as_ptr() as _)
            }
            None => Ok(ptr::null_mut()),
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        if let Some(block) = &self.block {
            if row < block.nrows() && col < block.ncols() {
                return block.get_raw_value_unchecked(row, col);
            }
        }
        (Ty::Null, 0, ptr::null())
    }

    fn take_timing(&mut self) -> Duration {
        Duration::from_millis(self.offset.timing() as u64)
    }

    fn stop_query(&mut self) {}
}

#[derive(Debug)]
struct TmqConf {
    map: HashMap<String, String>,
}

impl TmqConf {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}

unsafe fn _tmq_conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> TaosResult<()> {
    let key = CStr::from_ptr(key).to_str()?.to_string();
    let val = CStr::from_ptr(value).to_str()?.to_string();

    trace!("tmq_conf_set start, key: {key}, val: {val}");

    match (conf as *mut TaosMaybeError<TmqConf>)
        .as_mut()
        .and_then(|maybe_err| maybe_err.deref_mut())
    {
        Some(conf) => {
            match key.to_lowercase().as_str() {
                "td.connect.ip" | "td.connect.user" | "td.connect.pass" | "td.connect.db"
                | "group.id" | "client.id" => {}
                "enable.auto.commit" | "msg.with.table.name" | "enable.replay" => {
                    match val.to_lowercase().as_str() {
                        "true" | "false" => {}
                        _ => {
                            error!("tmq_conf_set failed, err: invalid bool value");
                            return Err(TaosError::new(Code::INVALID_PARA, "invalid bool value"));
                        }
                    }
                }
                "td.connect.port" | "auto.commit.interval.ms" => match u32::from_str(&val) {
                    Ok(_) => {}
                    Err(_) => {
                        error!("tmq_conf_set failed, err: invalid integer value");
                        return Err(TaosError::new(Code::INVALID_PARA, "invalid integer value"));
                    }
                },
                "session.timeout.ms" => match u32::from_str(&val) {
                    Ok(val) => {
                        if !(6000..=1800000).contains(&val) {
                            error!("tmq_conf_set failed, err: session.timeout.ms out of range");
                            return Err(TaosError::new(
                                Code::INVALID_PARA,
                                "session.timeout.ms out of range",
                            ));
                        }
                    }
                    Err(_) => {
                        error!("tmq_conf_set failed, err: session.timeout.ms is invalid");
                        return Err(TaosError::new(
                            Code::INVALID_PARA,
                            "session.timeout.ms is invalid",
                        ));
                    }
                },
                "max.poll.interval.ms" => match i32::from_str(&val) {
                    Ok(val) => {
                        if val < 1000 {
                            error!("tmq_conf_set failed, err: max.poll.interval.ms out of range");
                            return Err(TaosError::new(
                                Code::INVALID_PARA,
                                "max.poll.interval.ms out of range",
                            ));
                        }
                    }
                    Err(_) => {
                        error!("tmq_conf_set failed, err: max.poll.interval.ms is invalid");
                        return Err(TaosError::new(
                            Code::INVALID_PARA,
                            "max.poll.interval.ms is invalid",
                        ));
                    }
                },
                "auto.offset.reset" => match val.to_lowercase().as_str() {
                    "none" | "earliest" | "latest" => {}
                    _ => {
                        error!("tmq_conf_set failed, err: auto.offset.reset is invalid");
                        return Err(TaosError::new(
                            Code::INVALID_PARA,
                            "auto.offset.reset is invalid",
                        ));
                    }
                },
                _ => {
                    error!("tmq_conf_set failed, err: unknow key: {key}");
                    return Err(TaosError::new(Code::FAILED, &format!("unknow key: {key}")));
                }
            }
            conf.map.insert(key, val);
            trace!("tmq_conf_set succ, conf: {conf:?}");
            Ok(())
        }
        None => Err(TaosError::new(Code::INVALID_PARA, "conf is null")),
    }
}

#[derive(Debug)]
struct TmqList {
    topics: Vec<String>,
}

impl TmqList {
    fn new() -> Self {
        Self { topics: Vec::new() }
    }
}

unsafe fn _tmq_list_append(list: *mut tmq_list_t, value: *const c_char) -> TaosResult<()> {
    let topic = CStr::from_ptr(value).to_str()?.to_string();
    match (list as *mut TaosMaybeError<TmqList>)
        .as_mut()
        .and_then(|list| list.deref_mut())
    {
        Some(list) => {
            if !list.topics.is_empty() {
                return Err(TaosError::new(
                    Code::TMQ_TOPIC_APPEND_ERR,
                    "only one topic is supported",
                ));
            }
            list.topics.push(topic);
            Ok(())
        }
        None => Err(TaosError::new(Code::FAILED, "conf is null")),
    }
}

struct Tmq {
    consumer: Option<Consumer>,
}

impl Tmq {
    fn new(consumer: Consumer) -> Self {
        Self {
            consumer: Some(consumer),
        }
    }
}

impl Drop for Tmq {
    fn drop(&mut self) {
        if self.consumer.is_some() {
            let _ = self.consumer.take();
        }
    }
}

unsafe fn _tmq_consumer_new(conf: *mut tmq_conf_t) -> TaosResult<Tmq> {
    let mut dsn = Dsn::from_str("taos://localhost:6041")?;

    match (conf as *mut TaosMaybeError<TmqConf>)
        .as_mut()
        .and_then(|conf| conf.deref_mut())
    {
        Some(conf) => {
            for (key, value) in conf.map.iter() {
                dsn.params.insert(key.clone(), value.clone());
            }
        }
        None => return Err(TaosError::new(Code::FAILED, "conf is null")),
    }

    let consumer = TmqBuilder::from_dsn(&dsn)?.build()?;
    Ok(Tmq::new(consumer))
}

unsafe fn _tmq_consumer_poll(tmq: *mut tmq_t, timeout: i64) -> TaosResult<Option<ResultSet>> {
    let timeout = match timeout {
        0 => tmq::Timeout::Never,
        n if n < 0 => tmq::Timeout::from_millis(1000),
        _ => tmq::Timeout::from_millis(timeout as u64),
    };

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => match &tmq.consumer {
            Some(consumer) => {
                let res = consumer.recv_timeout(timeout)?;
                match res {
                    Some((offset, message_set)) => {
                        if message_set.has_meta() {
                            return Err(TaosError::new(
                                Code::FAILED,
                                "message has meta, only support topic created with select sql",
                            ));
                        }
                        let data = message_set.into_data().unwrap();
                        match data.fetch_raw_block()? {
                            Some(block) => {
                                Ok(Some(ResultSet::Tmq(TmqResultSet::new(block, offset, data))))
                            }
                            None => Ok(None),
                        }
                    }
                    None => Ok(None),
                }
            }
            None => Err(TaosError::new(Code::FAILED, "invalid consumer")),
        },
        None => Err(TaosError::new(Code::INVALID_PARA, "tmq is null")),
    }
}

unsafe fn _tmq_commit_sync(tmq: *mut tmq_t, res: *const TAOS_RES) -> TaosResult<()> {
    let offset = (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
        .map(ResultSetOperations::tmq_get_offset);

    trace!("_tmq_commit_sync, offset: {offset:?}");

    let tmq = match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => tmq,
        None => return Err(TaosError::new(Code::INVALID_PARA, "tmq is null")),
    };

    if tmq.consumer.is_none() {
        return Err(TaosError::new(Code::FAILED, "invalid consumer"));
    }

    let consumer = tmq.consumer.as_mut().unwrap();

    match offset {
        Some(offset) => match consumer.commit(offset) {
            Ok(_) => Ok(()),
            Err(err) => Err(TaosError::new(err.code(), &err.message())),
        },
        None => match consumer.commit_all() {
            Ok(_) => Ok(()),
            Err(err) => Err(TaosError::new(err.code(), &err.message())),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use super::*;
    use crate::ws::query::{taos_fetch_fields, taos_fetch_row, taos_num_fields, taos_print_row};
    use crate::ws::{test_connect, test_exec_many};

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
    }

    #[test]
    fn test_tmq_list() {
        unsafe {
            let tmq_list = tmq_list_new();
            assert!(!tmq_list.is_null());

            let value = c"topic".as_ptr();
            let res = tmq_list_append(tmq_list, value);
            assert_eq!(res, 0);

            let size = tmq_list_get_size(tmq_list);
            assert_eq!(size, 1);

            let array = tmq_list_to_c_array(tmq_list);
            assert!(!array.is_null());

            tmq_list_destroy(tmq_list);
        }
    }

    #[test]
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

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );
        }
    }

    #[test]
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
        }
    }

    #[test]
    fn test_tmq_offset_seek() {
        unsafe {
            let db = "test_1737440249";
            let topic = "topic_1737440249";

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

            let (_, (len, cap)) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
            let assigns = Vec::from_raw_parts(assignment, len, cap);

            for assign in assigns {
                let offset = tmq_position(consumer, topic_name.as_ptr(), assign.vgId);
                assert!(offset >= 0);

                let errno =
                    tmq_offset_seek(consumer, topic_name.as_ptr(), assign.vgId, assign.begin);
                assert_eq!(errno, 0);
            }

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                let row = taos_fetch_row(res);
                assert!(!row.is_null());

                let fields = taos_fetch_fields(res);
                assert!(!fields.is_null());

                let num_fields = taos_num_fields(res);
                assert_eq!(num_fields, 2);

                let mut str = vec![0 as c_char; 1024];
                let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

                let errno = tmq_commit_sync(consumer, res);
                assert_eq!(errno, 0);
            }

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );
        }
    }

    #[test]
    fn test_tmq_commit_offset_sync() {
        unsafe {
            let db = "test_1737444552";
            let topic = "topic_1737444552";
            let table = "t0";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    &format!("create table {table} (ts timestamp, c1 int)"),
                    &format!("insert into {table} values (now, 1)"),
                    &format!("insert into {table} values (now, 2)"),
                    &format!("create topic {topic} as database {db}"),
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

            let (_, (len, cap)) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
            let assigns = Vec::from_raw_parts(assignment, len, cap);

            let mut vg_ids = Vec::new();
            for assign in &assigns {
                vg_ids.push(assign.vgId);
            }

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                let table_name = tmq_get_table_name(res);
                assert!(!table_name.is_null());
                assert_eq!(
                    CStr::from_ptr(table_name),
                    CString::new(table).unwrap().as_c_str()
                );

                let db_name = tmq_get_db_name(res);
                assert!(!db_name.is_null());
                assert_eq!(
                    CStr::from_ptr(db_name),
                    CString::new(db).unwrap().as_c_str()
                );

                let res_type = tmq_get_res_type(res);
                assert_eq!(res_type, tmq_res_t::TMQ_RES_DATA);

                let topic_name = tmq_get_topic_name(res);
                assert!(!topic_name.is_null());
                assert_eq!(
                    CStr::from_ptr(topic_name),
                    CString::new(topic).unwrap().as_c_str()
                );

                let vg_id = tmq_get_vgroup_id(res);
                assert!(vg_ids.contains(&vg_id));

                let offset = tmq_get_vgroup_offset(res);
                assert_eq!(offset, 0);

                let mut current_offset = 0;
                for assign in &assigns {
                    if assign.vgId == vg_id {
                        current_offset = assign.currentOffset;
                        println!("current_offset: {current_offset}");
                        break;
                    }
                }

                let errno = tmq_commit_offset_sync(consumer, topic_name, vg_id, current_offset);
                assert_eq!(errno, 0);

                let committed_offset = tmq_committed(consumer, topic_name, vg_id);
                assert_eq!(committed_offset, current_offset);
            }

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );
        }
    }

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
        }
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

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );
        }
    }

    #[test]
    fn test_tmq_commit_offset_async() {
        unsafe {
            extern "C" fn cb(tmq: *mut tmq_t, code: i32, param: *mut c_void) {
                unsafe {
                    assert!(!tmq.is_null());
                    assert_eq!(code, 0);

                    assert!(!param.is_null());
                    assert_eq!("hello", CStr::from_ptr(param as *const _).to_str().unwrap());
                }
            }

            let db = "test_1741271535";
            let topic = "topic_1741271535";
            let table = "t0";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    &format!("create table {table} (ts timestamp, c1 int)"),
                    &format!("insert into {table} values (now, 1)"),
                    &format!("insert into {table} values (now, 2)"),
                    &format!("create topic {topic} as database {db}"),
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

            let (_, (len, cap)) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
            let assigns = Vec::from_raw_parts(assignment, len, cap);

            let mut vg_ids = Vec::new();
            for assign in &assigns {
                vg_ids.push(assign.vgId);
            }

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                let topic_name = tmq_get_topic_name(res);
                assert!(!topic_name.is_null());
                assert_eq!(
                    CStr::from_ptr(topic_name),
                    CString::new(topic).unwrap().as_c_str()
                );

                let vg_id = tmq_get_vgroup_id(res);
                assert!(vg_ids.contains(&vg_id));

                let mut current_offset = 0;
                for assign in &assigns {
                    if assign.vgId == vg_id {
                        current_offset = assign.currentOffset;
                        println!("current_offset: {current_offset}");
                        break;
                    }
                }

                let param = c"hello";
                tmq_commit_offset_async(
                    consumer,
                    topic_name,
                    vg_id,
                    current_offset,
                    cb,
                    param.as_ptr() as _,
                );

                sleep(Duration::from_secs(1));

                let committed_offset = tmq_committed(consumer, topic_name, vg_id);
                assert_eq!(committed_offset, current_offset);
            }

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );
        }
    }
}
