use std::{
    collections::HashMap,
    ffi::{c_void, CStr, CString},
    fmt::{Debug, Display},
    os::raw::{c_char, c_int},
    ptr,
    str::{FromStr, Utf8Error},
    time::Duration,
};

use futures::poll;
use taos_error::Code;

use taos_query::{
    block_in_place_or_global,
    common::{Field, Precision, RawBlock as Block, Timestamp, Ty},
    prelude::RawError,
    tmq,
    tmq::AsConsumer,
    Dsn, DsnError, Fetchable, Queryable, TBuilder,
};

use taos_ws::{
    query::{asyn::WS_ERROR_NO, Error, ResultSet, Taos},
    Consumer, TaosBuilder, TmqBuilder,
};

use crate::*;
use cargo_metadata::MetadataCommand;

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum ws_tmq_conf_res_t {
    Unknown = -2,
    Invalid = -1,
    Ok = 0,
}
impl From<Code> for ws_tmq_conf_res_t {
    fn from(value: Code) -> Self {
        match value {
            Code::SUCCESS => ws_tmq_conf_res_t::Ok,
            _ => ws_tmq_conf_res_t::Invalid,
        }
    }
}

struct TmqConf {
    hsmap: HashMap<String, String>,
}

struct WsTmqList {
    topics: Vec<String>,
}

#[allow(non_camel_case_types)]
pub type ws_tmq_list_t = c_void;
#[allow(non_camel_case_types)]
pub type ws_tmq_conf_t = c_void;

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_conf_new() -> *mut ws_tmq_conf_t {
    let tmq_conf: WsMaybeError<TmqConf> = TmqConf {
        hsmap: HashMap::new(),
    }
    .into();
    Box::into_raw(Box::new(tmq_conf)) as _
}

unsafe fn tmq_conf_set(
    conf: *mut ws_tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> WsResult<()> {
    let key = CStr::from_ptr(key).to_str()?.to_string();
    let value = CStr::from_ptr(value).to_str()?.to_string();

    match (conf as *mut WsMaybeError<TmqConf>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(tmq_conf) => {
            match key.to_lowercase().as_str() {
                "group.id" | "client.id" | "td.connect.db" => {}
                // "td.connect.protocol" => match value.to_lowercase().as_str() {
                //     "ws" | "wss" | "http" | "https" => {}
                //     _ => {
                //         log::trace!("set tmq conf failed, key: {}, value: {}", &key, &value);
                //         return Err(WsError::new(Code::FAILED, "invalid value"));
                //     }
                // },
                "enable.auto.commit" | "msg.with.table.name" => match value.to_lowercase().as_str()
                {
                    "true" | "false" => {}
                    _ => {
                        log::trace!("set tmq conf failed, key: {}, value: {}", &key, &value);
                        return Err(WsError::new(Code::FAILED, "invalid value"));
                    }
                },
                "auto.commit.interval.ms" => match i32::from_str(&value) {
                    Ok(_) => {}
                    Err(_) => {
                        log::trace!("set tmq conf failed, key: {}, value: {}", &key, &value);
                        return Err(WsError::new(Code::FAILED, "invalid value"));
                    }
                },
                "auto.offset.reset" => match value.to_lowercase().as_str() {
                    "none" | "earliest" | "latest" => {}
                    _ => {
                        log::trace!("set tmq conf failed, key: {}, value: {}", &key, &value);
                        return Err(WsError::new(Code::FAILED, "invalid value"));
                    }
                },
                _ => {
                    log::trace!("set tmq conf failed, unknow key: {}", &key);
                    return Err(WsError::new(Code::FAILED, "unknow key"));
                }
            }
            log::trace!("set tmq conf sucess, key: {}, value: {}", &key, &value);
            tmq_conf.hsmap.insert(key, value);
            Ok(())
        }
        _ => return Err(WsError::new(Code::FAILED, "invalid tmq conf Object")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_conf_set(
    conf: *mut ws_tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> ws_tmq_conf_res_t {
    if conf.is_null() || key.is_null() || value.is_null() {
        return ws_tmq_conf_res_t::Invalid;
    }

    match tmq_conf_set(conf, key, value) {
        Ok(_) => ws_tmq_conf_res_t::Ok,
        Err(e) => e.code.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_conf_destroy(conf: *mut ws_tmq_conf_t) -> i32 {
    if !conf.is_null() {
        let _boxed_conf = Box::from_raw(conf as *mut WsMaybeError<TmqConf>);
        return Code::SUCCESS.into();
    }
    return Code::FAILED.into();
}

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_list_new() -> *mut ws_tmq_list_t {
    let tmq_list: WsMaybeError<WsTmqList> = WsTmqList { topics: Vec::new() }.into();
    Box::into_raw(Box::new(tmq_list)) as _
}

unsafe fn tmq_list_append(list: *mut ws_tmq_list_t, src: *const c_char) -> WsResult<()> {
    let src = CStr::from_ptr(src).to_str()?.to_string();

    match (list as *mut WsMaybeError<WsTmqList>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(list) => {
            if list.topics.len() >= 1 {
                log::trace!("only support one topic in this websocket library");
                return Err(WsError::new(
                    Code::TMQ_TOPIC_APPEND_ERR,
                    "only support one topic",
                ));
            }

            list.topics.push(src.clone());
            Ok(())
        }
        _ => return Err(WsError::new(Code::FAILED, "invalid tmq list Object")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_list_append(list: *mut ws_tmq_list_t, src: *const c_char) -> i32 {
    if list.is_null() || src.is_null() {
        return Code::OBJECT_IS_NULL.into();
    }

    match tmq_list_append(list, src) {
        Ok(_) => Code::SUCCESS.into(),
        Err(e) => e.code.into(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_list_destroy(list: *mut ws_tmq_list_t) -> i32 {
    if !list.is_null() {
        let _boxed_conf = Box::from_raw(list as *mut WsMaybeError<WsTmqList>);
        return Code::SUCCESS.into();
    }
    return Code::INVALID_PARA.into();
}

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_list_to_c_array(
    list: *const ws_tmq_list_t,
    topic_num: *mut u32,
) -> *mut *mut c_char {
    if list.is_null() || topic_num.is_null() {
        return std::ptr::null_mut();
    }

    match (list as *const WsMaybeError<WsTmqList>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(list) => {
            if list.topics.len() >= 1 {
                *topic_num = list.topics.len() as u32;
                let c_strings: Vec<CString> = list
                    .topics
                    .iter()
                    .map(|s| CString::new(&**s).expect("CString::new failed"))
                    .collect();

                let mut raw_ptrs: Vec<*mut c_char> =
                    c_strings.into_iter().map(|cs| cs.into_raw()).collect();

                let ptr = raw_ptrs.as_mut_ptr();
                std::mem::forget(raw_ptrs); // 防止Rust清理raw_ptrs向量

                return ptr;
            } else {
                return std::ptr::null_mut();
            }
        }
        _ => return std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_list_free_c_array(
    c_str_arry: *mut *mut c_char,
    topic_num: u32,
) -> i32 {
    if c_str_arry.is_null() {
        return Code::INVALID_PARA.into();
    }

    for i in 0..topic_num {
        let _ = CString::from_raw(*c_str_arry.offset(i as isize));
    }
    let _ = Vec::from_raw_parts(c_str_arry, 0, topic_num as usize);

    return Code::SUCCESS.into();
}

#[allow(non_camel_case_types)]
pub type ws_tmq_t = c_void;

struct WsTmq {
    consumer: Consumer,
}

unsafe fn tmq_consumer_new(conf: *mut ws_tmq_conf_t, dsn: *const c_char) -> WsResult<WsTmq> {
    let dsn = if dsn.is_null() {
        CStr::from_bytes_with_nul(b"taos://localhost:6041\0").unwrap()
    } else {
        CStr::from_ptr(dsn)
    };
    let dsn = dsn.to_str()?;

    log::trace!("ws_tmq_consumer_new dsn: {}", &dsn);
    let mut dsn = Dsn::from_str(&dsn)?;

    if !conf.is_null() {
        match (conf as *mut WsMaybeError<TmqConf>)
            .as_mut()
            .and_then(|s| s.safe_deref_mut())
        {
            Some(tmq_conf) => {
                let map = &(*tmq_conf).hsmap;

                for (key, value) in map.iter() {
                    dsn.params.insert(key.clone(), value.clone());
                }
            }
            _ => return Err(WsError::new(Code::FAILED, "invalid tmq conf Object")),
        }
    }

    let builder = TmqBuilder::from_dsn(&dsn)?;
    let consumer = builder.build()?;
    let ws_tmq = WsTmq { consumer };
    return Ok(ws_tmq);
}

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_consumer_new(
    conf: *mut ws_tmq_conf_t,
    dsn: *const c_char,
    errstr: *mut c_char,
    errstr_len: c_int,
) -> *mut ws_tmq_t {
    match tmq_consumer_new(conf, dsn) {
        Ok(ws_tmq) => {
            let ws_tmq: WsMaybeError<WsTmq> = ws_tmq.into();
            return Box::into_raw(Box::new(ws_tmq)) as _;
        }
        Err(e) => {
            if errstr_len > 0 && !errstr.is_null() {
                let error_message = CString::new(e.to_string()).expect("CString::new failed");
                let bytes_to_copy = error_message.to_bytes().len().min(errstr_len as usize - 1);
                std::ptr::copy(error_message.as_ptr(), errstr as *mut c_char, bytes_to_copy);
                // 在拷贝的最后加上null终止符
                *errstr.add(bytes_to_copy) = 0;
            }
            set_error_info(e);
            return std::ptr::null_mut();
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_tmq_subscribe(
    tmq: *mut ws_tmq_t,
    topic_list: *const ws_tmq_list_t,
) -> i32 {
    if tmq.is_null() || topic_list.is_null() {
        return Code::INVALID_PARA.into();
    }

    let topic_list = match (topic_list as *const WsMaybeError<WsTmqList>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(topic_list) => topic_list,
        _ => return Code::INVALID_PARA.into(),
    };

    match (tmq as *mut WsMaybeError<WsTmq>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(ws_tmq) => match ws_tmq.consumer.subscribe(topic_list.topics.as_slice()) {
            Ok(_) => return Code::SUCCESS.into(),
            Err(_e) => {
                // set_error_info(e);
                return Code::FAILED.into();
            }
        },
        _ => {
            set_error_info(WsError::new(Code::INVALID_PARA, "invalid tmq Object"));
            return Code::INVALID_PARA.into();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tmq_conf() {
        use crate::*;
        init_env();
        unsafe {
            let conf = ws_tmq_conf_new();
            let r = ws_tmq_conf_set(
                conf,
                b"group.id\0" as *const u8 as _,
                b"abc\0" as *const u8 as _,
            );
            assert_eq!(r as i32, ws_tmq_conf_res_t::Ok as i32);

            let list = ws_tmq_list_new();
            let r = ws_tmq_list_append(list, b"topic\0" as *const u8 as *const c_char);
            assert_eq!(r, 0);

            let mut topic_num = 0;
            let c_str_arry = ws_tmq_list_to_c_array(list, &mut topic_num as *mut u32);
            assert!(!c_str_arry.is_null());

            let r = ws_tmq_list_free_c_array(c_str_arry, topic_num);
            assert_eq!(r, 0);

            let r = ws_tmq_list_destroy(list);
            assert_eq!(r, 0);

            let consumer = ws_tmq_consumer_new(
                conf,
                b"taos://localhost:6041\0" as *const u8 as *const c_char,
                std::ptr::null_mut(),
                0,
            );
            assert!(!consumer.is_null());

            let r = ws_tmq_conf_destroy(conf);
            assert_eq!(r, 0);
        }
    }
}
