use std::ffi::{c_char, c_void};

use crate::{TAOS, TAOS_RES};

pub const TSDB_CLIENT_ID_LEN: usize = 256;
pub const TSDB_CGROUP_LEN: usize = 193;
pub const TSDB_USER_LEN: usize = 24;
pub const TSDB_FQDN_LEN: usize = 128;
pub const TSDB_PASSWORD_LEN: usize = 32;

pub const TSDB_ACCT_ID_LEN: usize = 11;
pub const TSDB_DB_NAME_LEN: usize = 65;
pub const TSDB_NAME_DELIMITER_LEN: usize = 1;
pub const TSDB_DB_FNAME_LEN: usize = TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN;

pub const TSDB_VERSION_LEN: usize = 32;

#[allow(non_camel_case_types)]
pub type tmq_commit_cb = extern "C" fn(tmq: *mut tmq_t, code: i32, param: *mut c_void);

// todo
pub type SRWLatch = i32;

#[allow(non_camel_case_types)]
pub type tmr_h = *mut c_void;

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
    // pub pAppInfo: SAppInstInfo,
    // pub pRequests: SHashObj,
    // pub passInfo: SPassInfo,
    // pub whiteListInfo: SWhiteListInfo,
    // pub userDroppedInfo: STscNotifyInfo,
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct SArray {
    pub size: usize,
    pub capacity: u32,
    pub elemSize: u32,
    pub pData: *mut c_void,
}

// #[repr(C)]
// pub struct STaosQueue {
//     STaosQnode   *head;
//     STaosQnode   *tail;
//     STaosQueue   *next;     // for queue set
//     STaosQset    *qset;     // for queue set
//     void         *ahandle;  // for queue set
//     FItem         itemFp;
//     FItems        itemsFp;
//     TdThreadMutex mutex;
//     int64_t       memOfItems;
//     int32_t       numOfItems;
//     int64_t       threadId;
//     int64_t       memLimit;
//     int64_t       itemLimit;
//   };

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
    pub commitCb: *mut tmq_commit_cb,
    pub commitCbUserParam: *mut c_void,
    pub enableBatchMeta: i8,

    // status
    pub lock: SRWLatch,
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
    // pub mqueue: *mut STaosQueue,
    // pub qall: *mut STaosQall,
    // pub delayedTask: *mut STaosQueue,
    // pub rspSem: tsem2_t,
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
    pub commitCb: *mut tmq_commit_cb,
    pub commitCbUserParam: *mut c_void,
    pub enableBatchMeta: i8,
}

#[repr(C)]
#[allow(non_camel_case_types)]
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

#[no_mangle]
pub extern "C" fn tmq_conf_new() -> *mut tmq_conf_t {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> tmq_conf_res_t {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_conf_destroy(conf: *mut tmq_conf_t) {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_conf_set_auto_commit_cb(
    conf: *mut tmq_conf_t,
    cb: *mut tmq_commit_cb,
    param: *mut c_void,
) {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_list_new() -> *mut tmq_list_t {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_list_append(list: *mut tmq_list_t, value: *const c_char) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_list_destroy(list: *mut tmq_list_t) {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_list_get_size(list: *const tmq_list_t) -> i32 {
    todo!()
}

#[no_mangle]
pub extern "C" fn tmq_list_to_c_array(list: *const tmq_list_t) -> *mut *mut c_char {
    todo!()
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
    cb: *mut tmq_commit_cb,
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
    cb: *mut tmq_commit_cb,
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
pub extern "C" fn tmq_get_connect(tmq: *mut tmq_t) -> *mut TAOS {
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
