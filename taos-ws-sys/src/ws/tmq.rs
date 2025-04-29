use std::alloc::{alloc, dealloc, Layout};
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;
use std::str::FromStr;
use std::time::{Duration, Instant};

use taos_error::Code;
use taos_query::common::{Precision, RawBlock as Block, Ty};
use taos_query::tmq::{self, AsConsumer, IsData, IsOffset};
use taos_query::{global_tokio_runtime, Dsn, TBuilder};
use taos_ws::consumer::Data;
use taos_ws::query::Error;
use taos_ws::{Consumer, Offset, TmqBuilder};
use tracing::{debug, error, trace, warn, Instrument};

use crate::taos::tmq::{
    tmq_commit_cb, tmq_conf_res_t, tmq_conf_t, tmq_list_t, tmq_res_t, tmq_t, tmq_topic_assignment,
};
use crate::taos::{TAOS_FIELD_E, TAOS_RES};
use crate::ws::error::{errstr, format_errno, set_err_and_get_code, TaosError, TaosMaybeError};
use crate::ws::{ResultSet, ResultSetOperations, Row, SafePtr, TaosResult, TAOS_FIELD, TAOS_ROW};

pub fn tmq_conf_new() -> *mut tmq_conf_t {
    let tmq_conf: TaosMaybeError<TmqConf> = TmqConf::new().into();
    let tmq_conf = Box::into_raw(Box::new(tmq_conf)) as _;
    debug!("tmq_conf_new, tmq_conf: {tmq_conf:?}");
    tmq_conf
}

pub unsafe fn tmq_conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> tmq_conf_res_t {
    debug!("tmq_conf_set start, conf: {conf:?}, key: {key:?}, value: {value:?}");

    if conf.is_null() || key.is_null() || value.is_null() {
        error!("tmq_conf_set failed, err: invalid params");
        return tmq_conf_res_t::TMQ_CONF_INVALID;
    }

    match conf_set(conf, key, value) {
        Ok(_) => {
            debug!("tmq_conf_set succ");
            tmq_conf_res_t::TMQ_CONF_OK
        }
        Err(err) => {
            error!("tmq_conf_set failed, err: {err:?}");
            match err.code() {
                Code::INVALID_PARA => tmq_conf_res_t::TMQ_CONF_INVALID,
                _ => tmq_conf_res_t::TMQ_CONF_UNKNOWN,
            }
        }
    }
}

unsafe fn conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> TaosResult<()> {
    let key = CStr::from_ptr(key).to_str()?.to_string();
    let val = CStr::from_ptr(value).to_str()?.to_string();

    debug!("conf_set, key: {key}, val: {val}");

    match (conf as *mut TaosMaybeError<TmqConf>)
        .as_mut()
        .and_then(|maybe_err| maybe_err.deref_mut())
    {
        Some(conf) => {
            match key.to_lowercase().as_str() {
                // TODO: TmqInit add user and pass
                // "td.connect.ip" | "td.connect.user" | "td.connect.pass" | "td.connect.db"
                // | "group.id" | "client.id" => {}
                "enable.auto.commit" | "msg.with.table.name" | "enable.replay" => {
                    match val.to_lowercase().as_str() {
                        "true" | "false" => {}
                        _ => {
                            error!("tmq_conf_set failed, err: invalid bool value");
                            return Err(TaosError::new(Code::INVALID_PARA, "invalid bool value"));
                        }
                    }
                }
                "td.connect.port" | "auto.commit.interval.ms" | "session.timeout.ms" => {
                    match u32::from_str(&val) {
                        Ok(_) => {}
                        Err(_) => {
                            error!("tmq_conf_set failed, err: invalid integer value");
                            return Err(TaosError::new(
                                Code::INVALID_PARA,
                                "invalid integer value",
                            ));
                        }
                    }
                }
                // "session.timeout.ms" => match u32::from_str(&val) {
                //     Ok(val) => {
                //         if !(6000..=1800000).contains(&val) {
                //             error!("tmq_conf_set failed, err: session.timeout.ms out of range");
                //             return Err(TaosError::new(
                //                 Code::INVALID_PARA,
                //                 "session.timeout.ms out of range",
                //             ));
                //         }
                //     }
                //     Err(_) => {
                //         error!("tmq_conf_set failed, err: session.timeout.ms is invalid");
                //         return Err(TaosError::new(
                //             Code::INVALID_PARA,
                //             "session.timeout.ms is invalid",
                //         ));
                //     }
                // },
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
                _ => {}
            }
            conf.map.insert(key, val);
            Ok(())
        }
        None => Err(TaosError::new(Code::INVALID_PARA, "conf is null")),
    }
}

pub fn tmq_conf_destroy(conf: *mut tmq_conf_t) {
    debug!("tmq_conf_destroy, conf: {conf:?}");
    if !conf.is_null() {
        let _ = unsafe { Box::from_raw(conf as *mut TaosMaybeError<TmqConf>) };
    }
}

pub unsafe fn tmq_conf_set_auto_commit_cb(
    conf: *mut tmq_conf_t,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    debug!("tmq_conf_set_auto_commit_cb start, conf: {conf:?}, cb: {cb:?}, param: {param:?}");

    let fp = cb as *const ();
    if fp.is_null() {
        debug!("tmq_conf_set_auto_commit_cb succ, cb is null");
        return;
    }

    match (conf as *mut TaosMaybeError<TmqConf>)
        .as_mut()
        .and_then(|maybe_err| maybe_err.deref_mut())
    {
        Some(conf) => {
            conf.auto_commit_cb = Some((cb, param));
            debug!("tmq_conf_set_auto_commit_cb succ");
        }
        None => {
            error!("tmq_conf_set_auto_commit_cb failed, err: conf is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "conf is null"));
        }
    }
}

pub fn tmq_list_new() -> *mut tmq_list_t {
    let tmq_list: TaosMaybeError<TmqList> = TmqList::new().into();
    debug!("tmq_list_new, tmq_list: {tmq_list:?}");
    Box::into_raw(Box::new(tmq_list)) as _
}

pub unsafe fn tmq_list_append(list: *mut tmq_list_t, value: *const c_char) -> i32 {
    debug!("tmq_list_append start, list: {list:?}, value: {value:?}");

    if list.is_null() || value.is_null() {
        return format_errno(Code::OBJECT_IS_NULL.into());
    }

    debug!("tmq_list_append, value: {:?}", CStr::from_ptr(value));

    match list_append(list, value) {
        Ok(_) => {
            debug!("tmq_list_append succ");
            0
        }
        Err(err) => {
            error!("tmq_list_append failed, err: {err:?}");
            set_err_and_get_code(err)
        }
    }
}

unsafe fn list_append(list: *mut tmq_list_t, value: *const c_char) -> TaosResult<()> {
    let topic = CStr::from_ptr(value).to_str()?.to_string();
    match (list as *mut TaosMaybeError<TmqList>)
        .as_mut()
        .and_then(|list| list.deref_mut())
    {
        Some(list) => {
            list.topics.push(topic);
            Ok(())
        }
        None => Err(TaosError::new(Code::FAILED, "conf is null")),
    }
}

pub fn tmq_list_destroy(list: *mut tmq_list_t) {
    debug!("tmq_list_destroy start, list: {list:?}");
    if !list.is_null() {
        let list = unsafe { Box::from_raw(list as *mut TaosMaybeError<TmqList>) };
        debug!("tmq_list_destroy succ, list: {list:?}");
    }
}

pub unsafe fn tmq_list_get_size(list: *const tmq_list_t) -> i32 {
    debug!("tmq_list_get_size start, list: {list:?}");
    match (list as *mut TaosMaybeError<TmqList>)
        .as_mut()
        .and_then(|list| list.deref_mut())
    {
        Some(list) => {
            debug!("tmq_list_get_size succ, list: {list:?}");
            list.topics.len() as i32
        }
        None => {
            error!("tmq_list_get_size failed, err: list is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "list is null"))
        }
    }
}

pub unsafe fn tmq_list_to_c_array(list: *const tmq_list_t) -> *mut *mut c_char {
    debug!("tmq_list_to_c_array start, list: {list:?}");

    match (list as *const TaosMaybeError<TmqList>)
        .as_ref()
        .and_then(|list| list.deref_mut())
    {
        Some(list) => {
            if !list.topics.is_empty() {
                let arr = list
                    .topics
                    .iter()
                    .map(|s| CString::new(&**s).unwrap().into_raw())
                    .collect::<Vec<_>>();
                debug!("tmq_list_to_c_array succ, arr: {arr:?}, list: {list:?}");
                list.set_c_array(arr);
                list.c_array.as_mut().unwrap().as_mut_ptr()
            } else {
                debug!("tmq_list_to_c_array succ, topics is empty");
                ptr::null_mut()
            }
        }
        None => {
            error!("tmq_list_to_c_array failed, err: list is null");
            ptr::null_mut()
        }
    }
}

#[allow(non_snake_case)]
pub unsafe fn tmq_consumer_new(
    conf: *mut tmq_conf_t,
    errstr: *mut c_char,
    errstrLen: i32,
) -> *mut tmq_t {
    debug!("tmq_consumer_new start, conf: {conf:?}, errstr: {errstr:?}, errstr_len: {errstrLen}");
    match consumer_new(conf) {
        Ok(tmq) => {
            let tmq: TaosMaybeError<Tmq> = tmq.into();
            let tmq = Box::into_raw(Box::new(tmq)) as _;
            debug!("tmq_consumer_new succ, tmq: {tmq:?}");
            tmq
        }
        Err(err) => {
            error!("tmq_consumer_new failed, err: {err:?}");
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

unsafe fn consumer_new(conf: *mut tmq_conf_t) -> TaosResult<Tmq> {
    match (conf as *mut TaosMaybeError<TmqConf>)
        .as_mut()
        .and_then(|conf| conf.deref_mut())
    {
        Some(conf) => {
            debug!("consumer_new, conf: {conf:?}");

            let host = conf
                .map
                .get("td.connect.ip")
                .map_or("localhost", |val| val.as_str());

            let user = conf
                .map
                .get("td.connect.user")
                .map_or("root", |val| val.as_str());

            let pass = conf
                .map
                .get("td.connect.pass")
                .map_or("taosdata", |val| val.as_str());

            let dsn = if (host.contains("cloud.tdengine") || host.contains("cloud.taosdata"))
                && user == "token"
            {
                let mut port = conf
                    .map
                    .get("td.connect.port")
                    .map_or("443", |val| val.as_str());

                if port == "0" {
                    port = "443";
                }

                format!("wss://{host}:{port}/?token={pass}")
            } else {
                let mut port = conf
                    .map
                    .get("td.connect.port")
                    .map_or("6041", |val| val.as_str());

                if port == "0" {
                    port = "6041";
                }

                format!("ws://{user}:{pass}@{host}:{port}")
            };

            let mut dsn = Dsn::from_str(&dsn)?;

            let mut auto_commit = false;
            let mut auto_commit_interval_ms = 5000;

            for (key, val) in conf.map.iter() {
                match key.as_str() {
                    "enable.auto.commit" => {
                        auto_commit = val.to_lowercase().parse().unwrap_or(false);
                        dsn.params.insert(key.clone(), "false".to_string());
                    }
                    "auto.commit.interval.ms" => {
                        auto_commit_interval_ms = val.parse().unwrap_or(5000);
                    }
                    _ => {
                        dsn.params.insert(key.clone(), val.clone());
                    }
                }
            }

            debug!("consumer_new, dsn: {dsn:?}");

            let consumer = TmqBuilder::from_dsn(&dsn)?.build()?;

            Ok(Tmq::new(
                consumer,
                auto_commit,
                auto_commit_interval_ms,
                conf.auto_commit_cb,
            ))
        }
        None => Err(TaosError::new(Code::INVALID_PARA, "conf is null")),
    }
}

pub unsafe fn tmq_subscribe(tmq: *mut tmq_t, topic_list: *const tmq_list_t) -> i32 {
    debug!("tmq_subscribe start, tmq: {tmq:?}, topic_list: {topic_list:?}");

    let tmq_list = match (topic_list as *const TaosMaybeError<TmqList>)
        .as_ref()
        .and_then(|list| list.deref())
    {
        Some(tmq_list) => tmq_list,
        None => {
            error!("tmq_subscribe failed, err: topic_list is null");
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
                        debug!("tmq_subscribe succ");
                        0
                    }
                    Err(err) => {
                        error!("tmq_subscribe failed, err: {err:?}");
                        set_err_and_get_code(TaosError::new(Code::FAILED, &err.message()))
                    }
                }
            } else {
                error!("tmq_subscribe failed, err: consumer is none");
                set_err_and_get_code(TaosError::new(Code::FAILED, "consumer is none"))
            }
        }
        None => {
            error!("tmq_subscribe failed, err: tmq is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

pub unsafe fn tmq_unsubscribe(tmq: *mut tmq_t) -> i32 {
    debug!("tmq_unsubscribe start, tmq: {tmq:?}");

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => {
            if let Some(consumer) = tmq.consumer.take() {
                consumer.unsubscribe();
            }
            debug!("tmq_unsubscribe succ");
            0
        }
        None => {
            error!("tmq_unsubscribe failed, err: tmq is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

pub unsafe fn tmq_subscription(tmq: *mut tmq_t, topics: *mut *mut tmq_list_t) -> i32 {
    debug!("tmq_subscription start, tmq: {tmq:?}, topics: {topics:?}");

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
                for topic in topic_list {
                    let value = CString::new(topic).unwrap();
                    let code = tmq_list_append(*topics, value.as_ptr());
                    if code != 0 {
                        error!(
                            "tmq_subscription failed, err: tmq_list_append failed, code: {code}"
                        );
                        return code;
                    }
                }
            }
            debug!("tmq_subscription succ");
            0
        }
        None => {
            error!("tmq_subscription failed, err: tmq is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

pub unsafe fn tmq_consumer_poll(tmq: *mut tmq_t, timeout: i64) -> *mut TAOS_RES {
    debug!("tmq_consumer_poll start, tmq: {tmq:?}, timeout: {timeout}");

    if tmq.is_null() {
        error!("tmq_consumer_poll failed, err: tmq is null");
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
        return ptr::null_mut();
    }

    match consumer_poll(tmq, timeout) {
        Ok(Some(rs)) => {
            let rs: TaosMaybeError<ResultSet> = rs.into();
            debug!("tmq_consumer_poll succ, rs: {rs:?}");
            Box::into_raw(Box::new(rs)) as _
        }
        Ok(None) => {
            debug!("tmq_consumer_poll succ, rs is none");
            ptr::null_mut()
        }
        Err(err) => {
            error!("tmq_consumer_poll failed, err: {err:?}");
            set_err_and_get_code(err);
            ptr::null_mut()
        }
    }
}

unsafe fn consumer_poll(tmq_ptr: *mut tmq_t, timeout: i64) -> TaosResult<Option<ResultSet>> {
    let timeout = match timeout {
        0 => tmq::Timeout::Never,
        n if n < 0 => tmq::Timeout::from_millis(1000),
        _ => tmq::Timeout::from_millis(timeout as u64),
    };

    match (tmq_ptr as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => match &tmq.consumer {
            Some(consumer) => {
                if tmq.auto_commit {
                    if let Some(offset) = tmq.auto_commit_offset.0.take() {
                        let now = Instant::now();
                        if now - tmq.auto_commit_offset.1
                            > Duration::from_millis(tmq.auto_commit_interval_ms)
                        {
                            tmq.auto_commit_offset.1 = now;
                            debug!("poll auto commit, commit offset: {offset:?}");
                            if let Err(err) = consumer.commit(offset) {
                                error!("poll auto commit failed, err: {err:?}");

                                if let Some((cb, param)) = tmq.auto_commit_cb {
                                    debug!("poll auto commit failed, callback");
                                    cb(tmq_ptr, format_errno(err.code().into()), param);
                                }
                            } else if let Some((cb, param)) = tmq.auto_commit_cb {
                                debug!("poll auto commit succ, callback");
                                cb(tmq_ptr, 0, param);
                            }
                        }
                    }
                }

                let res = consumer.recv_timeout(timeout)?;
                match res {
                    Some((offset, message_set)) => {
                        if tmq.auto_commit {
                            debug!("poll auto commit, set offset: {offset:?}");
                            tmq.auto_commit_offset.0 = Some(offset.clone());
                        }

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
            None => Err(TaosError::new(Code::FAILED, "consumer is none")),
        },
        None => Err(TaosError::new(Code::INVALID_PARA, "tmq is null")),
    }
}

pub fn tmq_consumer_close(tmq: *mut tmq_t) -> i32 {
    debug!("tmq_consumer_close, tmq: {tmq:?}");
    if tmq.is_null() {
        error!("tmq_consumer_close failed, err: tmq is null");
        return format_errno(Code::INVALID_PARA.into());
    }
    let _ = unsafe { Box::from_raw(tmq as *mut TaosMaybeError<Tmq>) };
    0
}

pub unsafe fn tmq_commit_sync(tmq: *mut tmq_t, msg: *const TAOS_RES) -> i32 {
    debug!("tmq_commit_sync start, tmq: {tmq:?}, msg: {msg:?}");

    if tmq.is_null() {
        error!("tmq_commit_sync failed, err: tmq is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
    }

    match commit_sync(tmq, msg) {
        Ok(_) => {
            debug!("tmq_commit_sync succ");
            0
        }
        Err(err) => {
            error!("tmq_commit_sync failed, err: {err:?}");
            set_err_and_get_code(err)
        }
    }
}

unsafe fn commit_sync(tmq: *mut tmq_t, res: *const TAOS_RES) -> TaosResult<()> {
    let offset = (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
        .map(ResultSetOperations::tmq_get_offset);

    debug!("commit_sync, offset: {offset:?}");

    let tmq = match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => tmq,
        None => return Err(TaosError::new(Code::INVALID_PARA, "tmq is null")),
    };

    if tmq.consumer.is_none() {
        return Err(TaosError::new(Code::FAILED, "consumer is none"));
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

pub unsafe fn tmq_commit_async(
    tmq: *mut tmq_t,
    msg: *const TAOS_RES,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    debug!("tmq_commit_async start, tmq: {tmq:?}, msg: {msg:?}, cb: {cb:?}, param: {param:?}");

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

            debug!("tmq_commit_async, offset: {offset:?}");

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
                            debug!("tmq_commit_async commit callback");
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
                        debug!("tmq_commit_async commit all callback, commit all succ");
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

    debug!("tmq_commit_async succ");
}

#[allow(non_snake_case)]
pub unsafe fn tmq_commit_offset_sync(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    debug!("tmq_commit_offset_sync start, tmq: {tmq:?}, p_topic_name: {pTopicName:?}, vg_id: {vgId}, offset: {offset}");

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_commit_offset_sync failed, err: tmq is null");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_commit_offset_sync failed, err: tmq is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is invalid"));
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_commit_offset_sync failed, err: consumer is none");
            return set_err_and_get_code(TaosError::new(Code::FAILED, "consumer is none"));
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_commit_offset_sync failed, err: topic name is invalid");
            return set_err_and_get_code(TaosError::new(
                Code::INVALID_PARA,
                "topic name is invalid",
            ));
        }
    };

    match consumer.commit_offset(topic_name, vgId, offset) {
        Ok(_) => {
            debug!("tmq_commit_offset_sync succ");
            0
        }
        Err(err) => {
            error!("tmq_commit_offset_sync failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message()))
        }
    }
}

#[allow(non_snake_case)]
pub unsafe fn tmq_commit_offset_async(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    debug!(
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
                    debug!("tmq_commit_offset_async callback");
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

    debug!("tmq_commit_offset_async succ");
}

#[allow(non_snake_case)]
pub unsafe fn tmq_get_topic_assignment(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    assignment: *mut *mut tmq_topic_assignment,
    numOfAssignment: *mut i32,
) -> i32 {
    debug!("tmq_get_topic_assignment start, tmq: {tmq:?}, p_topic_name: {pTopicName:?}, assignment: {assignment:?}, num_of_assignment: {numOfAssignment:?}");

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
                        let (_, assigns) = assigns.first().unwrap();
                        let len = assigns.len();
                        trace!("tmq_get_topic_assignment, assigns: {assigns:?}, len: {len}");

                        let total_size =
                            size_of::<usize>() + len * size_of::<tmq_topic_assignment>();
                        let layout =
                            Layout::from_size_align_unchecked(total_size, align_of::<usize>());
                        let ptr = alloc(layout);
                        if ptr.is_null() {
                            error!("tmq_get_topic_assignment failed, alloc null");
                            return set_err_and_get_code(TaosError::new(
                                Code::FAILED,
                                "alloc failed",
                            ));
                        }

                        *(ptr as *mut usize) = len;
                        let assigns_ptr = ptr.add(size_of::<usize>()) as *mut tmq_topic_assignment;
                        ptr::copy_nonoverlapping(assigns.as_ptr() as _, assigns_ptr, len);

                        *assignment = assigns_ptr;
                        *numOfAssignment = len as _;
                    }
                    debug!("tmq_get_topic_assignment succ",);
                    0
                }
                None => {
                    *assignment = ptr::null_mut();
                    *numOfAssignment = 0;
                    debug!("tmq_get_topic_assignment succ, no assignment");
                    0
                }
            },
            None => {
                error!("tmq_get_topic_assignment failed, err: consumer is none");
                set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "consumer is none"))
            }
        },
        None => {
            error!("tmq_get_topic_assignment failed, err: tmq is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

#[allow(non_snake_case)]
pub unsafe fn tmq_free_assignment(pAssignment: *mut tmq_topic_assignment) {
    debug!("tmq_free_assignment start, p_assignment: {pAssignment:?}");
    if pAssignment.is_null() {
        error!("tmq_free_assignment failed, p_assignment is null");
        return;
    }

    let ptr = (pAssignment as *mut u8).sub(size_of::<usize>());
    let len = *(ptr as *const usize);
    let total_size = size_of::<usize>() + len * size_of::<tmq_topic_assignment>();
    let layout = Layout::from_size_align_unchecked(total_size, align_of::<usize>());
    dealloc(ptr, layout);

    debug!("tmq_free_assignment succ");
}

#[allow(non_snake_case)]
pub unsafe fn tmq_offset_seek(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    debug!(
        "tmq_offset_seek start, tmq: {:?}, p_topic_name: {:?}, vg_id: {}, offset: {}",
        tmq,
        CStr::from_ptr(pTopicName),
        vgId,
        offset
    );

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_offset_seek failed, err: tmq is null");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_offset_seek failed, err: tmq is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is invalid"));
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_offset_seek failed, err: consumer is none");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "consumer is none"));
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_offset_seek failed, err: topic name is invalid");
            return set_err_and_get_code(TaosError::new(
                Code::INVALID_PARA,
                "topic name is invalid",
            ));
        }
    };

    match consumer.offset_seek(topic_name, vgId, offset) {
        Ok(_) => {
            debug!("tmq_offset_seek succ");
            0
        }
        Err(err) => {
            error!("tmq_offset_seek failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message()))
        }
    }
}

#[allow(non_snake_case)]
pub unsafe fn tmq_position(tmq: *mut tmq_t, pTopicName: *const c_char, vgId: i32) -> i64 {
    debug!(
        "tmq_position start, tmq: {:?}, p_topic_name: {:?}, vg_id: {}",
        tmq,
        CStr::from_ptr(pTopicName),
        vgId
    );

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_position failed, err: tmq is null");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null")) as _;
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_position failed, err: tmq is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is invalid")) as _;
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_position failed, err: consumer is none");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "consumer is none"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_position failed, err: topic name is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "topic name is invalid"))
                as _;
        }
    };

    match consumer.position(topic_name, vgId) {
        Ok(offset) => {
            debug!("tmq_position succ, offset: {offset}");
            offset
        }
        Err(err) => {
            error!("tmq_position failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message())) as _
        }
    }
}

#[allow(non_snake_case)]
pub unsafe fn tmq_committed(tmq: *mut tmq_t, pTopicName: *const c_char, vgId: i32) -> i64 {
    debug!(
        "tmq_committed start, tmq: {:?}, p_topic_name: {:?}, vg_id: {}",
        tmq,
        CStr::from_ptr(pTopicName),
        vgId
    );

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_committed failed, err: tmq is null");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null")) as _;
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_committed failed, err: tmq is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is invalid")) as _;
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_committed failed, err: consumer is none");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "consumer is none"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_committed failed, err: topic name is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "topic name is invalid"))
                as _;
        }
    };

    match consumer.committed(topic_name, vgId) {
        Ok(offset) => {
            debug!("tmq_committed succ, offset: {offset}");
            offset
        }
        Err(err) => {
            error!("tmq_committed failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message())) as _
        }
    }
}

// FIXME: inconsistent with native behavior
pub unsafe fn tmq_get_table_name(res: *mut TAOS_RES) -> *const c_char {
    debug!("tmq_get_table_name start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_table_name succ, rs: {rs:?}");
            rs.tmq_get_table_name()
        }
        None => {
            warn!("tmq_get_table_name failed, err: res is null");
            ptr::null()
        }
    }
}

// FIXME: inconsistent with native behavior
pub fn tmq_get_res_type(res: *mut TAOS_RES) -> tmq_res_t {
    debug!("tmq_get_res_type start, res: {res:?}");
    if res.is_null() {
        debug!("tmq_get_res_type succ, res is null");
        return tmq_res_t::TMQ_RES_INVALID;
    }
    debug!("tmq_get_res_type succ");
    tmq_res_t::TMQ_RES_DATA
}

pub unsafe fn tmq_get_topic_name(res: *mut TAOS_RES) -> *const c_char {
    debug!("tmq_get_topic_name start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_topic_name succ, rs: {rs:?}");
            rs.tmq_get_topic_name()
        }
        None => {
            error!("tmq_get_topic_name failed, err: res is null");
            ptr::null()
        }
    }
}

pub unsafe fn tmq_get_db_name(res: *mut TAOS_RES) -> *const c_char {
    debug!("tmq_get_db_name start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_db_name succ, rs: {rs:?}");
            rs.tmq_get_db_name()
        }
        None => {
            error!("tmq_get_db_name failed, err: res is null");
            ptr::null()
        }
    }
}

pub unsafe fn tmq_get_vgroup_id(res: *mut TAOS_RES) -> i32 {
    debug!("tmq_get_vgroup_id start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_vgroup_id succ, rs: {rs:?}");
            rs.tmq_get_vgroup_id()
        }
        None => {
            error!("tmq_get_vgroup_id failed, err: res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

pub unsafe fn tmq_get_vgroup_offset(res: *mut TAOS_RES) -> i64 {
    debug!("tmq_get_vgroup_offset start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_vgroup_offset succ, rs: {rs:?}");
            rs.tmq_get_vgroup_offset()
        }
        None => {
            error!("tmq_get_vgroup_offset failed, err: res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null")) as _
        }
    }
}

pub unsafe fn tmq_err2str(code: i32) -> *const c_char {
    debug!("tmq_err2str, code: {code}");
    let err = match code {
        0 => TaosError::new(Code::SUCCESS, "success"),
        -1 => TaosError::new(Code::FAILED, "fail"),
        _ => return errstr(),
    };
    set_err_and_get_code(err);
    errstr()
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

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E {
        ptr::null_mut()
    }

    unsafe fn fetch_raw_block(
        &mut self,
        ptr: *mut *mut c_void,
        rows: *mut i32,
    ) -> Result<(), Error> {
        self.block = self.data.fetch_raw_block()?;
        if let Some(block) = self.block.as_ref() {
            *ptr = block.as_raw_bytes().as_ptr() as _;
            *rows = block.nrows() as _;
        } else {
            *rows = 0;
        }
        Ok(())
    }

    unsafe fn fetch_block(&mut self, _rows: *mut TAOS_ROW, _num: *mut c_int) -> Result<(), Error> {
        todo!()
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
    auto_commit_cb: Option<(tmq_commit_cb, *mut c_void)>,
}

impl TmqConf {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            auto_commit_cb: None,
        }
    }
}

#[derive(Debug)]
struct TmqList {
    topics: Vec<String>,
    c_array: Option<Vec<*mut c_char>>,
}

impl Drop for TmqList {
    fn drop(&mut self) {
        self.free_c_array();
    }
}

impl TmqList {
    fn new() -> Self {
        Self {
            topics: Vec::new(),
            c_array: None,
        }
    }

    fn free_c_array(&mut self) {
        if let Some(arr) = self.c_array.take() {
            for ch in arr {
                let _ = unsafe { CString::from_raw(ch) };
            }
        }
    }

    fn set_c_array(&mut self, arr: Vec<*mut c_char>) {
        self.free_c_array();
        self.c_array = Some(arr);
    }
}

struct Tmq {
    consumer: Option<Consumer>,
    auto_commit: bool,
    auto_commit_interval_ms: u64,
    auto_commit_offset: (Option<Offset>, Instant),
    auto_commit_cb: Option<(tmq_commit_cb, *mut c_void)>,
}

impl Tmq {
    fn new(
        consumer: Consumer,
        auto_commit: bool,
        auto_commit_interval_ms: u64,
        auto_commit_cb: Option<(tmq_commit_cb, *mut c_void)>,
    ) -> Self {
        Self {
            consumer: Some(consumer),
            auto_commit,
            auto_commit_interval_ms,
            auto_commit_offset: (None, Instant::now()),
            auto_commit_cb,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tmq_err2str() {
        unsafe {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid para"));
            let errstr = tmq_err2str(format_errno(Code::INVALID_PARA.into()));
            assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "invalid para");
        }

        unsafe {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid para"));
            let errstr = tmq_err2str(format_errno(Code::COLUMN_EXISTS.into()));
            assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "invalid para");
        }
    }
}
