use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::str::FromStr;
use std::{mem, ptr};

use taos_error::Code;
use tracing::trace;

use crate::native::error::{format_errno, set_err_and_get_code, TaosError, TaosMaybeError};
use crate::native::{TaosResult, TAOS_RES};

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
pub extern "C" fn tmq_consumer_new(
    conf: *mut tmq_conf_t,
    errstr: *mut c_char,
    errstrLen: i32,
) -> *mut tmq_t {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_subscribe(tmq: *mut tmq_t, topic_list: *const tmq_list_t) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_unsubscribe(tmq: *mut tmq_t) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_subscription(tmq: *mut tmq_t, topics: *mut *mut tmq_list_t) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_consumer_poll(tmq: *mut tmq_t, timeout: i64) -> *mut TAOS_RES {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_consumer_close(tmq: *mut tmq_t) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_commit_sync(tmq: *mut tmq_t, msg: *const TAOS_RES) -> i32 {
    todo!()
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

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn tmq_commit_offset_sync(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    todo!()
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

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn tmq_get_topic_assignment(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    assignment: *mut *mut tmq_topic_assignment,
    numOfAssignment: *mut i32,
) -> i32 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn tmq_free_assignment(pAssignment: *mut tmq_topic_assignment) {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn tmq_offset_seek(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn tmq_position(tmq: *mut tmq_t, pTopicName: *const c_char, vgId: i32) -> i64 {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn tmq_committed(tmq: *mut tmq_t, pTopicName: *const c_char, vgId: i32) -> i64 {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_get_table_name(res: *mut TAOS_RES) -> *const c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_get_res_type(res: *mut TAOS_RES) -> tmq_res_t {
    todo!()
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
struct TmqConf {
    conf: HashMap<String, String>,
}

impl TmqConf {
    fn new() -> Self {
        Self {
            conf: HashMap::new(),
        }
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
            tmq_conf.conf.insert(key, value);
            Ok(())
        }
        None => Err(TaosError::new(Code::OBJECT_IS_NULL, "conf is null")),
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
