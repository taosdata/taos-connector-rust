use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::str::FromStr;
use std::time::Duration;
use std::{mem, ptr};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use taos_error::Code;
use taos_query::common::{Precision, RawBlock as Block, Ty};
use taos_query::tmq::{self, AsConsumer, IsData, IsOffset};
use taos_query::{Dsn, TBuilder};
use taos_ws::consumer::Data;
use taos_ws::query::Error;
use taos_ws::{Consumer, Offset, TmqBuilder};
use tracing::{error, trace};

use crate::native::error::{format_errno, set_err_and_get_code, TaosError, TaosMaybeError};
use crate::native::{
    ResultSet, ResultSetOperations, Row, TaosResult, TAOS_FIELD, TAOS_RES, TAOS_ROW,
};

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

#[allow(non_camel_case_types)]
pub type tmq_commit_cb = extern "C" fn(tmq: *mut tmq_t, code: i32, param: *mut c_void);

// todo
pub type SRWLatch = i32;

#[allow(non_camel_case_types)]
pub type tmr_h = *mut c_void;

#[allow(non_camel_case_types)]
pub type sem_t = c_int;

#[allow(non_camel_case_types)]
pub type _hash_fn_t = extern "C" fn(*const c_char, u32) -> u32;

#[allow(non_camel_case_types)]
pub type _equal_fn_t = extern "C" fn(*const c_void, *const c_void, usize) -> i32;

#[allow(non_camel_case_types)]
pub type _hash_before_fn_t = extern "C" fn(*mut c_void);

#[allow(non_camel_case_types)]
pub type _hash_free_fn_t = extern "C" fn(*mut c_void);

#[allow(non_camel_case_types)]
pub type __taos_notify_fn_t = extern "C" fn(param: *mut c_void, ext: *mut c_void, r#type: c_int);

pub type FItem = extern "C" fn(pInfo: *mut SQueueInfo, pItem: *mut c_void);

pub type FItems = extern "C" fn(pInfo: *mut SQueueInfo, qall: *mut STaosQall, numOfItems: i32);

#[repr(C)]
pub struct SEp {
    pub fqdn: [c_char; TSDB_FQDN_LEN],
    pub port: u16,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SEpSet {
    pub inUse: i8,
    pub numOfEps: i8,
    pub eps: [SEp; TSDB_MAX_REPLICA],
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SCorEpSet {
    pub version: i32,
    pub epSet: SEpSet,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SAppClusterSummary {
    pub numOfInsertsReq: u64,
    pub numOfInsertRows: u64,
    pub insertElapsedTime: u64,
    pub insertBytes: u64,
    pub fetchBytes: u64,
    pub numOfQueryReq: u64,
    pub queryElapsedTime: u64,
    pub numOfSlowQueries: u64,
    pub totalRequests: u64,
    pub currentRequests: u64,
}

#[repr(C)]
pub struct DListNode {
    pub dl_prev: *mut SListNode,
    pub dl_next: *mut SListNode,
}

#[repr(C)]
pub struct SListNode {
    pub node: DListNode,
    pub data: [c_char; 0],
}

#[repr(C)]
pub struct DList {
    pub dl_head: *mut SListNode,
    pub dl_tail: *mut SListNode,
    pub dl_neles: i32,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SList {
    pub list: DList,
    pub eleSize: i32,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SHashNode {
    pub next: *mut SHashNode,
    pub hashVal: u32,  // the hash value of key
    pub dataLen: u32,  // length of data
    pub keyLen: u32,   // length of the key
    pub refCount: u16, // reference count
    pub removed: i8,   // flag to indicate removed
    pub data: [c_char; 0],
}

#[repr(C)]
pub struct SHashEntry {
    pub num: i32,        // number of elements in current entry
    pub latch: SRWLatch, // entry latch
    pub next: *mut SHashNode,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum SHashLockTypeE {
    HASH_NO_LOCK = 0,
    HASH_ENTRY_LOCK = 1,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SHashObj {
    pub hashList: *mut *mut SHashEntry,
    pub capacity: usize,               // number of slots
    pub size: i64,                     // number of elements in hash table
    pub hashFp: _hash_fn_t,            // hash function
    pub equalFp: _equal_fn_t,          // equal function
    pub freeFp: _hash_free_fn_t,       // hash node free callback function
    pub lock: SRWLatch,                // read-write spin lock
    pub r#type: SHashLockTypeE,        // hash type
    pub enableUpdate: bool,            // enable update
    pub pMemBlock: *mut SArray,        // memory block allocated for SHashEntry
    pub callbackFp: _hash_before_fn_t, // function invoked before return the value to caller
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SAppHbMgr {
    pub key: *mut c_char,
    pub idx: i32,
    pub reportCnt: i32,
    pub connKeyCnt: i32,
    pub connHbFlag: i8,   // 0 init, 1 send req, 2 get resp
    pub reportBytes: i64, // not implemented
    pub startTime: i64,
    pub lock: SRWLatch, // lock is used in serialization
    pub pAppInstInfo: *mut SAppInstInfo,
    pub activeInfo: *mut SHashObj, // hash<SClientHbKey, SClientHbReq>
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SMonitorParas {
    pub tsEnableMonitor: bool,
    pub tsMonitorInterval: i32,
    pub tsSlowLogThreshold: i32,
    pub tsSlowLogMaxLen: i32,
    pub tsSlowLogScope: i32,
    pub tsSlowLogThresholdTest: i32, // obsolete
    pub tsSlowLogExceptDb: [c_char; TSDB_DB_NAME_LEN],
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SAppInstServerCFG {
    pub monitorParas: SMonitorParas,
    pub enableAuditDelete: i8,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SAppInstInfo {
    pub numOfConns: i64,
    pub mgmtEp: SCorEpSet,
    pub totalDnodes: i32,
    pub onlineDnodes: i32,
    // pub qnodeMutex: TdThreadMutex,
    pub pQnodeList: *mut SArray,
    pub summary: SAppClusterSummary,
    pub pConnList: *mut SList,
    pub clusterId: i64,
    pub pTransporter: *mut c_void,
    pub pAppHbMgr: *mut SAppHbMgr,
    pub instKey: *mut c_char,
    pub serverCfg: SAppInstServerCFG,
}

#[repr(C)]
pub struct SWhiteListInfo {
    pub ver: i64,
    pub param: *mut c_void,
    pub fp: __taos_notify_fn_t,
}

#[repr(C)]
pub struct STscNotifyInfo {
    pub ver: i32,
    pub param: *mut c_void,
    pub fp: __taos_notify_fn_t,
}

pub type SPassInfo = STscNotifyInfo;

#[repr(C)]
#[allow(non_snake_case)]
pub struct STscObj {
    pub user: [c_char; TSDB_USER_LEN],
    pub pass: [c_char; TSDB_PASSWORD_LEN],
    pub db: [c_char; TSDB_DB_FNAME_LEN],
    pub sVer: [c_char; TSDB_VERSION_LEN],
    pub sDetailVer: [c_char; 128],
    pub sysInfo: i8,
    pub connType: i8,
    pub dropped: i8,
    pub biMode: i8,
    pub acctId: i32,
    pub connId: u32,
    pub appHbMgrIdx: i32,
    pub id: i64,
    // pub mutex: TdThreadMutex,
    pub numOfReqs: i32,
    pub authVer: i32,
    pub pAppInfo: *mut SAppInstInfo,
    pub pRequests: SHashObj,
    pub passInfo: SPassInfo,
    pub whiteListInfo: SWhiteListInfo,
    pub userDroppedInfo: STscNotifyInfo,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SArray {
    pub size: usize,
    pub capacity: u32,
    pub elemSize: u32,
    pub pData: *mut c_void,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct STaosQnode {
    pub next: *mut STaosQnode,
    pub queue: *mut STaosQueue,
    pub timestamp: i64,
    pub dataSize: i64,
    pub size: i32,
    pub itype: i8,
    pub reserved: [i8; 3],
    pub item: [c_char; 0],
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct STaosQset {
    pub head: *mut STaosQueue,
    pub current: *mut STaosQueue,
    // pub mutex: TdThreadMutex,
    pub sem: sem_t,
    pub numOfQueues: i32,
    pub numOfItems: i32,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SQueueInfo {
    pub ahandle: *mut c_void,
    pub fp: *mut c_void,
    pub queue: *mut c_void,
    pub workerId: i32,
    pub threadNum: i32,
    pub timestamp: i64,
    pub workerCb: *mut c_void,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct STaosQall {
    pub current: *mut STaosQnode,
    pub start: *mut STaosQnode,
    pub numOfItems: i32,
    pub memOfItems: i64,
    pub unAccessedNumOfItems: i32,
    pub unAccessMemOfItems: i64,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct STaosQueue {
    pub head: *mut STaosQnode,
    pub tail: *mut STaosQnode,
    pub next: *mut STaosQueue,
    pub qset: *mut STaosQset,
    pub ahandle: *mut c_void,
    pub itemFp: FItem,
    pub itemsFp: FItems,
    // pub mutex: TdThreadMutex,
    pub memOfItems: i64,
    pub numOfItems: i32,
    pub threadId: i64,
    pub memLimit: i64,
    pub itemLimit: i64,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct tsem2_t {
    // pub mutex: TdThreadMutex,
    // pub cond TdThreadCond,
    // pub attr: TdThreadCondAttr,
    pub count: c_int,
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct tmq_t {
    pub refId: i64,
    pub groupId: [c_char; TSDB_CGROUP_LEN],
    pub clientId: [c_char; TSDB_CLIENT_ID_LEN],
    pub user: [c_char; TSDB_USER_LEN],
    pub fqdn: [c_char; TSDB_FQDN_LEN],
    pub withTbName: i8,
    pub useSnapshot: i8,
    pub autoCommit: i8,
    pub autoCommitInterval: i32,
    pub sessionTimeoutMs: i32,
    pub heartBeatIntervalMs: i32,
    pub maxPollIntervalMs: i32,
    pub resetOffsetCfg: i8,
    pub replayEnable: i8,
    pub sourceExcluded: i8,
    pub consumerId: i64,
    pub commitCb: tmq_commit_cb,
    pub commitCbUserParam: *mut c_void,
    pub enableBatchMeta: i8,

    // status
    pub lock: SRWLatch, // todo
    pub status: i8,
    pub epoch: i32,

    // poll info
    pub pollCnt: i64,
    pub totalRows: i64,
    pub pollFlag: i8,

    // timer
    pub hbLiveTimer: tmr_h,
    pub epTimer: tmr_h,
    pub commitTimer: tmr_h,
    pub pTscObj: *mut STscObj,
    pub clientTopics: *mut SArray,
    pub mqueue: *mut STaosQueue,
    pub qall: *mut STaosQall,
    pub delayedTask: *mut STaosQueue,
    pub rspSem: tsem2_t,
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct tmq_conf_t {
    pub clientId: [c_char; TSDB_CLIENT_ID_LEN],
    pub groupId: [c_char; TSDB_CGROUP_LEN],
    pub autoCommit: i8,
    pub resetOffset: i8,
    pub withTbName: i8,
    pub snapEnable: i8,
    pub replayEnable: i8,
    pub sourceExcluded: i8,
    pub port: u16,
    pub autoCommitInterval: i32,
    pub sessionTimeoutMs: i32,
    pub heartBeatIntervalMs: i32,
    pub maxPollIntervalMs: i32,
    pub ip: *mut c_char,
    pub user: *mut c_char,
    pub pass: *mut c_char,
    pub commitCb: tmq_commit_cb,
    pub commitCbUserParam: *mut c_void,
    pub enableBatchMeta: i8,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq, Eq)]
pub enum tmq_conf_res_t {
    TMQ_CONF_UNKNOWN = -2,
    TMQ_CONF_INVALID = -1,
    TMQ_CONF_OK = 0,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct tmq_list_t {
    pub container: SArray,
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
pub enum tmq_res_t {
    TMQ_RES_INVALID = -1,
    TMQ_RES_DATA = 1,
    TMQ_RES_TABLE_META = 2,
    TMQ_RES_METADATA = 3,
}

#[allow(non_camel_case_types)]
pub type _tmq_conf_t = c_void;

#[allow(non_camel_case_types)]
pub type _tmq_list_t = c_void;

#[allow(non_camel_case_types)]
pub type _tmq_t = c_void;

#[no_mangle]
pub extern "C" fn tmq_conf_new() -> *mut _tmq_conf_t {
    trace!("tmq_conf_new");
    let tmq_conf: TaosMaybeError<TmqConf> = TmqConf::new().into();
    trace!(tmq_conf=?tmq_conf, "tmq_conf_new done");
    Box::into_raw(Box::new(tmq_conf)) as _
}

#[no_mangle]
pub unsafe extern "C" fn tmq_conf_set(
    conf: *mut _tmq_conf_t,
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
pub extern "C" fn tmq_conf_destroy(conf: *mut _tmq_conf_t) {
    trace!(conf=?conf, "tmq_conf_destroy");
    if !conf.is_null() {
        let _ = unsafe { Box::from_raw(conf as *mut TaosMaybeError<TmqConf>) };
    }
}

#[no_mangle]
pub extern "C" fn tmq_conf_set_auto_commit_cb(
    conf: *mut tmq_conf_t,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_list_new() -> *mut _tmq_list_t {
    trace!("tmq_list_new");
    let tmq_list: TaosMaybeError<TmqList> = TmqList::new().into();
    trace!(tmq_list=?tmq_list, "tmq_list_new done");
    Box::into_raw(Box::new(tmq_list)) as _
}

#[no_mangle]
pub unsafe extern "C" fn tmq_list_append(list: *mut _tmq_list_t, value: *const c_char) -> i32 {
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
pub extern "C" fn tmq_list_destroy(list: *mut _tmq_list_t) {
    trace!(list=?list, "tmq_list_destroy");
    if !list.is_null() {
        let _ = unsafe { Box::from_raw(list as *mut TaosMaybeError<TmqList>) };
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_list_get_size(list: *const _tmq_list_t) -> i32 {
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
pub unsafe extern "C" fn tmq_list_to_c_array(list: *const _tmq_list_t) -> *mut *mut c_char {
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
pub unsafe extern "C" fn tmq_consumer_new(
    conf: *mut _tmq_conf_t,
    errstr: *mut c_char,
    errstrLen: i32,
) -> *mut _tmq_t {
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
pub unsafe extern "C" fn tmq_subscribe(tmq: *mut _tmq_t, topic_list: *const _tmq_list_t) -> i32 {
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
pub unsafe extern "C" fn tmq_unsubscribe(tmq: *mut _tmq_t) -> i32 {
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
pub extern "C" fn tmq_subscription(tmq: *mut tmq_t, topics: *mut *mut tmq_list_t) -> i32 {
    todo!()
}

#[no_mangle]
pub unsafe extern "C" fn tmq_consumer_poll(tmq: *mut _tmq_t, timeout: i64) -> *mut TAOS_RES {
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
pub extern "C" fn tmq_consumer_close(tmq: *mut _tmq_t) -> i32 {
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
pub unsafe extern "C" fn tmq_commit_sync(tmq: *mut _tmq_t, msg: *const TAOS_RES) -> i32 {
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
pub extern "C" fn tmq_commit_async(
    tmq: *mut tmq_t,
    msg: *const TAOS_RES,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    todo!()
}

// TODO: test case
#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_commit_offset_sync(
    tmq: *mut _tmq_t,
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
pub extern "C" fn tmq_commit_offset_async(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    todo!()
}

static TOPIC_ASSIGNMETN_MAP: Lazy<DashMap<usize, (usize, usize)>> = Lazy::new(DashMap::new);

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_get_topic_assignment(
    tmq: *mut _tmq_t,
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
pub extern "C" fn tmq_free_assignment(pAssignment: *mut tmq_topic_assignment) {
    trace!("tmq_free_assignment start, p_assignment: {pAssignment:?}");
    if pAssignment.is_null() {
        trace!("tmq_free_assignment done, p_assignment is null");
        return;
    }

    let (_, (len, cap)) = TOPIC_ASSIGNMETN_MAP
        .remove(&(pAssignment as usize))
        .unwrap();
    let assigns = unsafe { Vec::from_raw_parts(pAssignment, len, cap) };
    trace!("tmq_free_assignment done, assigns: {assigns:?}, len: {len}, cap: {cap}");
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_offset_seek(
    tmq: *mut _tmq_t,
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
pub unsafe extern "C" fn tmq_position(
    tmq: *mut _tmq_t,
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

// TODO: test case
#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_committed(
    tmq: *mut _tmq_t,
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

// TODO: test case
#[no_mangle]
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
            error!("tmq_get_table_name failed, err: invalid res");
            ptr::null()
        }
    }
}

// TODO: test case
#[no_mangle]
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
pub extern "C" fn tmq_get_topic_name(res: *mut TAOS_RES) -> *const c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_get_db_name(res: *mut TAOS_RES) -> *const c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_get_vgroup_id(res: *mut TAOS_RES) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_get_vgroup_offset(res: *mut TAOS_RES) -> i64 {
    todo!()
}

#[no_mangle]
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
    conf: *mut _tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> TaosResult<()> {
    let key = CStr::from_ptr(key).to_str()?.to_string();
    let value = CStr::from_ptr(value).to_str()?.to_string();

    match (conf as *mut TaosMaybeError<TmqConf>)
        .as_mut()
        .and_then(|conf| conf.deref_mut())
    {
        Some(tmq_conf) => {
            match key.to_lowercase().as_str() {
                "group.id" | "client.id" | "td.connect.db" => {}
                "enable.auto.commit" | "msg.with.table.name" => match value.to_lowercase().as_str()
                {
                    "true" | "false" => {}
                    _ => {
                        trace!(key, value, "tmq_conf_set failed");
                        return Err(TaosError::new(Code::INVALID_PARA, "invalid value"));
                    }
                },
                "auto.commit.interval.ms" => match i32::from_str(&value) {
                    Ok(_) => {}
                    Err(_) => {
                        trace!(key, value, "tmq_conf_set failed");
                        return Err(TaosError::new(Code::INVALID_PARA, "invalid value"));
                    }
                },
                "auto.offset.reset" => match value.to_lowercase().as_str() {
                    "none" | "earliest" | "latest" => {}
                    _ => {
                        trace!(key, value, "tmq_conf_set failed");
                        return Err(TaosError::new(Code::INVALID_PARA, "invalid value"));
                    }
                },
                _ => {
                    trace!("tmq_conf_set failed, unknow key: {key}");
                    return Err(TaosError::new(Code::FAILED, "unknow key"));
                }
            }
            trace!(key, value, "tmq_conf_set done");
            tmq_conf.map.insert(key, value);
            Ok(())
        }
        None => Err(TaosError::new(Code::OBJECT_IS_NULL, "conf is null")),
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

unsafe fn _tmq_list_append(list: *mut _tmq_list_t, value: *const c_char) -> TaosResult<()> {
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

unsafe fn _tmq_consumer_new(conf: *mut _tmq_conf_t) -> TaosResult<Tmq> {
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

unsafe fn _tmq_consumer_poll(tmq: *mut _tmq_t, timeout: i64) -> TaosResult<Option<ResultSet>> {
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

unsafe fn _tmq_commit_sync(tmq: *mut _tmq_t, res: *const TAOS_RES) -> TaosResult<()> {
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
    use super::*;
    use crate::native::query::{
        taos_fetch_fields, taos_fetch_row, taos_num_fields, taos_print_row,
    };
    use crate::native::{test_connect, test_exec_many};

    #[test]
    fn test_tmq_conf() {
        unsafe {
            let tmq_conf = tmq_conf_new();
            assert!(!tmq_conf.is_null());

            let key = c"group.id".as_ptr() as *const c_char;
            let value = c"test".as_ptr() as *const c_char;
            let res = tmq_conf_set(tmq_conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            tmq_conf_destroy(tmq_conf);
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
            let db = "test_1737357704";
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
            let db = "test_1737423087";
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
}
