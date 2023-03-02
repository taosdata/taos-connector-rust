// use dlopen2::wrapper::{Container, WrapperApi};
// use dlopen2::symbor::{Library, PtrOrNull, Ref, SymBorApi, Symbol};
use dlopen2::raw::Library;
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    ffi::{c_char, c_int, c_ulong, c_void, CStr, CString},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use taos_query::{
    common::{c_field_t, raw_data_t},
    prelude::{Code, Field, Precision, RawError},
    RawBlock,
};

use crate::{
    err_or,
    into_c_str::IntoCStr,
    types::{
        from_raw_fields, taos_async_fetch_cb, taos_async_query_cb, tmq_commit_cb, tmq_conf_res_t,
        tmq_conf_t, tmq_list_t, tmq_res_t, tmq_resp_err_t, tmq_t, TaosMultiBind, TAOS, TAOS_RES,
        TAOS_ROW, TAOS_STMT, TSDB_OPTION,
    },
    Auth,
};

use self::query_future::QueryFuture;

mod query_future;

lazy_static::lazy_static! {
    static ref RAW_LIBRARIES: Mutex<HashMap<PathBuf, Arc<Library>>> = Mutex::new(HashMap::new());
}

#[derive(Debug)]
pub struct ApiEntry {
    lib: Arc<Library>,
    version: String,
    taos_cleanup: unsafe extern "C" fn(),
    taos_get_client_info: unsafe extern "C" fn() -> *const c_char,
    taos_options: unsafe extern "C" fn(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int,
    taos_connect: unsafe extern "C" fn(
        ip: *const c_char,
        user: *const c_char,
        pass: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> *mut TAOS,
    taos_close: unsafe extern "C" fn(taos: *mut TAOS),

    // error handler
    taos_errno: unsafe extern "C" fn(taos: *const TAOS) -> c_int,
    taos_errstr: unsafe extern "C" fn(taos: *const TAOS) -> *const c_char,

    // async query
    taos_fetch_rows_a:
        unsafe extern "C" fn(res: *mut TAOS_RES, fp: taos_async_fetch_cb, param: *mut c_void),
    taos_query_a: unsafe extern "C" fn(
        taos: *mut TAOS,
        sql: *const c_char,
        fp: taos_async_query_cb,
        param: *mut c_void,
    ),
    taos_result_block: Option<unsafe extern "C" fn(taos: *mut TAOS_RES) -> *mut *mut c_void>,
    taos_get_raw_block: Option<unsafe extern "C" fn(taos: *mut TAOS_RES) -> *mut c_void>,
    taos_fetch_raw_block_a: Option<
        unsafe extern "C" fn(res: *mut TAOS_RES, fp: taos_async_fetch_cb, param: *mut c_void),
    >,
    // taos_result_block: Option<unsafe extern "C" fn(taos: *mut TAOS_RES) -> *mut c_void>,
    tmq_write_raw: Option<unsafe extern "C" fn(taos: *mut TAOS, meta: raw_data_t) -> i32>,
    taos_write_raw_block: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            nrows: i32,
            ptr: *const c_char,
            tbname: *const c_char,
        ) -> i32,
    >,
    taos_write_raw_block_with_fields: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            nrows: i32,
            ptr: *const c_char,
            tbname: *const c_char,
            fields: *const c_field_t,
            fields_count: i32,
        ) -> i32,
    >,

    // query
    taos_query: unsafe extern "C" fn(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES,
    taos_free_result: unsafe extern "C" fn(res: *mut TAOS_RES),
    taos_result_precision: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,
    taos_field_count: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,
    taos_affected_rows: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,
    taos_fetch_fields: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut c_void,
    taos_fetch_lengths: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut c_int,
    taos_fetch_block: unsafe extern "C" fn(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int,
    taos_fetch_block_s: Option<
        unsafe extern "C" fn(
            res: *mut TAOS_RES,
            num_of_rows: *mut c_int,
            rows: *mut TAOS_ROW,
        ) -> c_int,
    >,
    taos_fetch_raw_block: Option<
        unsafe extern "C" fn(res: *mut TAOS_RES, num: *mut i32, data: *mut *mut c_void) -> c_int,
    >,

    // stmt
    pub(crate) stmt: StmtApi,
    //  tmq
    pub(crate) tmq: Option<TmqApi>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TmqListApi {
    tmq_list_new: unsafe extern "C" fn() -> *mut tmq_list_t,
    tmq_list_append: unsafe extern "C" fn(arg1: *mut tmq_list_t, arg2: *const c_char) -> i32,
    tmq_list_destroy: unsafe extern "C" fn(list: *mut tmq_list_t),
    tmq_list_get_size: unsafe extern "C" fn(list: *const tmq_list_t) -> i32,
    tmq_list_to_c_array: unsafe extern "C" fn(list: *const tmq_list_t) -> *const *mut c_char,
}

impl TmqListApi {
    pub(crate) unsafe fn new(&self) -> *mut tmq_list_t {
        (self.tmq_list_new)()
    }
    pub(crate) unsafe fn destroy(&self, list: *mut tmq_list_t) {
        (self.tmq_list_destroy)(list)
    }

    pub(crate) unsafe fn from_c_str_iter<'a, T: IntoCStr<'a>>(
        &self,
        iter: impl IntoIterator<Item = T>,
    ) -> Result<*mut tmq_list_t, RawError> {
        let list = self.new();
        for item in iter {
            self.append(list, item)?;
        }
        Ok(list)
    }

    pub(crate) fn append<'a>(
        &self,
        list: *mut tmq_list_t,
        item: impl IntoCStr<'a>,
    ) -> Result<(), RawError> {
        let code = unsafe { (self.tmq_list_append)(list, item.into_c_str().as_ptr()) };
        if code == 0 {
            Ok(())
        } else {
            Err(RawError::new(Code::Failed, "append tmq list error"))
        }
    }

    pub(crate) fn len(&self, list: *mut tmq_list_t) -> i32 {
        unsafe { (self.tmq_list_get_size)(list) }
    }
    pub(crate) fn as_c_str_slice(&self, list: *mut tmq_list_t) -> &[*mut c_char] {
        let len = self.len(list) as usize;
        let data = unsafe { (self.tmq_list_to_c_array)(list) };
        unsafe { std::slice::from_raw_parts(data, len) }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TmqConfApi {
    tmq_conf_new: unsafe extern "C" fn() -> *mut tmq_conf_t,

    tmq_conf_destroy: unsafe extern "C" fn(conf: *mut tmq_conf_t),

    tmq_conf_set: unsafe extern "C" fn(
        conf: *mut tmq_conf_t,
        key: *const c_char,
        value: *const c_char,
    ) -> tmq_conf_res_t,

    tmq_conf_set_auto_commit_cb:
        unsafe extern "C" fn(conf: *mut tmq_conf_t, cb: tmq_commit_cb, param: *mut c_void),

    tmq_consumer_new: unsafe extern "C" fn(
        conf: *mut tmq_conf_t,
        errstr: *mut c_char,
        errstr_len: i32,
    ) -> *mut tmq_t,
}

impl TmqConfApi {
    pub(crate) unsafe fn new(&self) -> *mut tmq_conf_t {
        (self.tmq_conf_new)()
    }
    pub(crate) unsafe fn destroy(&self, conf: *mut tmq_conf_t) {
        (self.tmq_conf_destroy)(conf)
    }
    pub(crate) unsafe fn set(
        &self,
        conf: *mut tmq_conf_t,
        k: &str,
        v: &str,
    ) -> Result<(), RawError> {
        let key = k.into_c_str();
        let value = v.into_c_str();
        (self.tmq_conf_set)(conf, key.as_ptr(), value.as_ptr()).ok(k, v)
    }

    pub(crate) unsafe fn auto_commit_cb(
        &self,
        conf: *mut tmq_conf_t,
        cb: tmq_commit_cb,
        param: *mut c_void,
    ) {
        (self.tmq_conf_set_auto_commit_cb)(conf, cb, param)
    }

    pub(crate) unsafe fn consumer(&self, conf: *mut tmq_conf_t) -> Result<*mut tmq_t, RawError> {
        let mut err = [0; 256];
        let tmq = (self.tmq_consumer_new)(conf, err.as_mut_ptr() as _, 255);
        if err[0] != 0 {
            Err(RawError::from_string(
                String::from_utf8_lossy(&err).to_string(),
            ))
        } else if tmq.is_null() {
            Err(RawError::from_string("[optin] create new consumer failed"))
        } else {
            Ok(tmq)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TmqApi {
    tmq_get_res_type: unsafe extern "C" fn(res: *mut TAOS_RES) -> tmq_res_t,
    tmq_get_table_name: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,
    tmq_get_db_name: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,
    tmq_get_json_meta: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut c_char,
    tmq_get_topic_name: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,
    tmq_get_vgroup_id: unsafe extern "C" fn(res: *mut TAOS_RES) -> i32,
    tmq_get_raw: unsafe extern "C" fn(res: *mut TAOS_RES, raw: *mut raw_data_t) -> i32,

    pub(crate) tmq_subscribe:
        unsafe extern "C" fn(tmq: *mut tmq_t, topics: *mut tmq_list_t) -> tmq_resp_err_t,
    pub(crate) tmq_unsubscribe: unsafe extern "C" fn(tmq: *mut tmq_t) -> tmq_resp_err_t,
    pub(crate) tmq_subscription:
        unsafe extern "C" fn(tmq: *mut tmq_t, topic_list: *mut *mut tmq_list_t) -> tmq_resp_err_t,
    pub(crate) tmq_consumer_poll:
        unsafe extern "C" fn(tmq: *mut tmq_t, blocking_time: i64) -> *mut TAOS_RES,
    pub(crate) tmq_consumer_close: unsafe extern "C" fn(tmq: *mut tmq_t) -> tmq_resp_err_t,
    pub(crate) tmq_commit_sync:
        unsafe extern "C" fn(tmq: *mut tmq_t, msg: *const TAOS_RES) -> tmq_resp_err_t,
    pub(crate) tmq_commit_async: unsafe extern "C" fn(
        tmq: *mut tmq_t,
        msg: *const TAOS_RES,
        cb: tmq_commit_cb,
        param: *mut c_void,
    ),

    pub(crate) conf_api: TmqConfApi,
    pub(crate) list_api: TmqListApi,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StmtApi {
    pub(crate) taos_stmt_init: unsafe extern "C" fn(taos: *mut TAOS) -> *mut TAOS_STMT,

    pub(crate) taos_stmt_prepare:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, sql: *const c_char, length: c_ulong) -> c_int,

    pub(crate) taos_stmt_set_tbname_tags:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, name: *const c_char, tags: *mut c_void) -> c_int,

    pub(crate) taos_stmt_set_tbname:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int,

    pub(crate) taos_stmt_set_tags:
        Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT, tags: *mut c_void) -> c_int>,

    pub(crate) taos_stmt_set_sub_tbname:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int,

    pub(crate) taos_stmt_is_insert:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int,

    pub(crate) taos_stmt_num_params:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int,

    pub(crate) taos_stmt_get_param: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT,
        idx: c_int,
        type_: *mut c_int,
        bytes: *mut c_int,
    ) -> c_int,

    pub(crate) taos_stmt_bind_param:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, bind: *const c_void) -> c_int,

    pub(crate) taos_stmt_bind_param_batch:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, bind: *const TaosMultiBind) -> c_int,

    pub(crate) taos_stmt_bind_single_param_batch: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT,
        bind: *const TaosMultiBind,
        colIdx: c_int,
    ) -> c_int,

    pub(crate) taos_stmt_add_batch: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_execute: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_affected_rows: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_use_result: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> *mut TAOS_RES,

    pub(crate) taos_stmt_close: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_errstr: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> *const c_char,
}
const fn default_lib_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "taos.dll"
    } else if cfg!(target_os = "linux") {
        "libtaos.so"
    } else if cfg!(target_os = "macos") {
        "libtaos.dylib"
    } else {
        panic!("current os is not supported")
    }
}

impl Default for ApiEntry {
    fn default() -> Self {
        let lib_env = "TAOS_LIBRARY_PATH";
        let path = if let Some(path) = std::env::var_os(lib_env) {
            PathBuf::from(path)
        } else {
            PathBuf::from(default_lib_name())
        };
        Self::dlopen(path).unwrap()
    }
}

impl ApiEntry {
    pub fn dlopen<S>(path: S) -> Result<Self, dlopen2::Error>
    where
        S: AsRef<Path>,
    {
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
        // let path =
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
        macro_rules! optional_symbol {
            ($($name:ident),*) => {
                $(let $name = lib.symbol(stringify!($name)).ok();)*
            };
        }
        unsafe {
            let taos_get_client_info: extern "C" fn() -> *const c_char =
                lib.symbol("taos_get_client_info")?;
            let version = CStr::from_ptr(taos_get_client_info()).to_str().unwrap();
            symbol!(
                taos_cleanup,
                taos_options,
                taos_connect,
                taos_close,
                taos_errno,
                taos_errstr,
                taos_fetch_rows_a,
                taos_query_a,
                taos_query,
                taos_free_result,
                taos_result_precision,
                taos_field_count,
                taos_affected_rows,
                taos_fetch_fields,
                taos_fetch_lengths,
                taos_fetch_block
            );
            optional_symbol!(
                taos_fetch_block_s,
                taos_fetch_raw_block,
                taos_fetch_raw_block_a,
                tmq_write_raw,
                taos_write_raw_block,
                taos_write_raw_block_with_fields,
                taos_get_raw_block,
                taos_result_block
            );

            // stmt
            symbol!(
                taos_stmt_init,
                taos_stmt_prepare,
                taos_stmt_set_tbname_tags,
                taos_stmt_set_tbname,
                taos_stmt_set_sub_tbname,
                taos_stmt_is_insert,
                taos_stmt_num_params,
                taos_stmt_get_param,
                taos_stmt_bind_param,
                taos_stmt_bind_param_batch,
                taos_stmt_bind_single_param_batch,
                taos_stmt_add_batch,
                taos_stmt_execute,
                taos_stmt_affected_rows,
                taos_stmt_use_result,
                taos_stmt_close,
                taos_stmt_errstr
            );
            optional_symbol!(taos_stmt_set_tags);

            let stmt = StmtApi {
                taos_stmt_init,
                taos_stmt_prepare,
                taos_stmt_set_tbname_tags,
                taos_stmt_set_tbname,
                taos_stmt_set_tags,
                taos_stmt_set_sub_tbname,
                taos_stmt_is_insert,
                taos_stmt_num_params,
                taos_stmt_get_param,
                taos_stmt_bind_param,
                taos_stmt_bind_param_batch,
                taos_stmt_bind_single_param_batch,
                taos_stmt_add_batch,
                taos_stmt_execute,
                taos_stmt_affected_rows,
                taos_stmt_use_result,
                taos_stmt_close,
                taos_stmt_errstr,
            };

            let tmq = if version.starts_with('3') {
                symbol!(
                    tmq_get_res_type,
                    tmq_get_table_name,
                    tmq_get_db_name,
                    tmq_get_json_meta,
                    tmq_get_topic_name,
                    tmq_get_vgroup_id,
                    tmq_get_raw,
                    tmq_conf_new,
                    tmq_conf_destroy,
                    tmq_conf_set,
                    tmq_conf_set_auto_commit_cb,
                    tmq_list_new,
                    tmq_list_append,
                    tmq_list_destroy,
                    tmq_list_get_size,
                    tmq_list_to_c_array,
                    tmq_subscribe,
                    tmq_unsubscribe,
                    tmq_subscription,
                    tmq_consumer_poll,
                    tmq_consumer_close,
                    tmq_commit_sync,
                    tmq_commit_async,
                    tmq_consumer_new
                );

                let conf_api = TmqConfApi {
                    tmq_conf_new,
                    tmq_conf_destroy,
                    tmq_conf_set,
                    tmq_conf_set_auto_commit_cb,
                    tmq_consumer_new,
                };

                let list_api = TmqListApi {
                    tmq_list_new,
                    tmq_list_append,
                    tmq_list_destroy,
                    tmq_list_get_size,
                    tmq_list_to_c_array,
                };
                Some(TmqApi {
                    tmq_get_res_type,
                    tmq_get_table_name,
                    tmq_get_db_name,
                    tmq_get_json_meta,
                    tmq_get_topic_name,
                    tmq_get_vgroup_id,
                    tmq_get_raw,
                    tmq_subscribe,
                    tmq_unsubscribe,
                    tmq_subscription,
                    tmq_consumer_poll,
                    tmq_consumer_close,
                    tmq_commit_sync,
                    tmq_commit_async,

                    conf_api,
                    list_api,
                })
            } else {
                None
            };

            Ok(Self {
                lib,
                version: version.to_string(),
                taos_cleanup,
                taos_get_client_info,
                taos_options,
                taos_connect,
                taos_close,

                taos_errno,
                taos_errstr,

                taos_fetch_rows_a,
                taos_query_a,
                taos_query,
                tmq_write_raw,
                taos_write_raw_block,
                taos_write_raw_block_with_fields,
                taos_result_block,

                taos_free_result,
                taos_result_precision,
                taos_field_count,
                taos_affected_rows,
                taos_fetch_fields,
                taos_fetch_lengths,
                taos_fetch_block,
                taos_fetch_block_s,
                taos_fetch_raw_block,
                taos_fetch_raw_block_a,
                taos_get_raw_block,

                stmt,
                tmq,
            })
        }
    }
    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn is_v3(&self) -> bool {
        self.version.starts_with('3')
    }

    pub(super) fn options(&self, opt: TSDB_OPTION, val: &str) -> &Self {
        unsafe {
            let val = CString::new(val.as_bytes()).unwrap();
            (self.taos_options)(opt, val.as_ptr() as _);
        }
        self
    }

    pub(super) fn connect(&self, auth: &Auth) -> *mut TAOS {
        unsafe {
            (self.taos_connect)(
                auth.host_as_ptr(),
                auth.user_as_ptr(),
                auth.password_as_ptr(),
                auth.database_as_ptr(),
                auth.port(),
            )
        }
    }

    pub(super) fn check(&self, ptr: *const TAOS_RES) -> Result<(), RawError> {
        let code: Code = unsafe { (self.taos_errno)(ptr as _) & 0xffff }.into();
        if code.success() {
            Ok(())
        } else {
            let message = unsafe {
                std::str::from_utf8_unchecked(CStr::from_ptr((self.taos_errstr)(ptr)).to_bytes())
            };
            Err(RawError::new(code, message))
        }
    }
}

#[derive(Debug, Clone)]
pub struct RawTaos {
    pub(crate) c: Arc<ApiEntry>,
    ptr: *mut TAOS,
}

unsafe impl Send for RawTaos {}
unsafe impl Sync for RawTaos {}

impl RawTaos {
    pub(crate) fn new(c: Arc<ApiEntry>, ptr: *mut TAOS) -> Result<Self, RawError> {
        if ptr.is_null() {
            Err(c.check(ptr).unwrap_err())
        } else {
            Ok(Self { c, ptr })
        }
    }
    //     #[inline]
    //     pub fn connect(
    //         host: *const c_char,
    //         user: *const c_char,
    //         pass: *const c_char,
    //         db: *const c_char,
    //         port: u16,
    //     ) -> Result<Self, RawError> {
    //         let ptr = unsafe { taos_connect(host, user, pass, db, port) };
    //         let null = std::ptr::null_mut();
    //         let code = unsafe { taos_errno(null) };
    //         if code != 0 {
    //             let err = unsafe { CStr::from_ptr(taos_errstr(null)) }
    //                 .to_string_lossy()
    //                 .to_string();
    //             let err = Error::new(code, err);
    //         }

    //         if ptr.is_null() {
    //             let null = std::ptr::null_mut();
    //             let code = unsafe { taos_errno(null) };
    //             let err = unsafe { CStr::from_ptr(taos_errstr(null)) }
    //                 .to_string_lossy()
    //                 .to_string();
    //             log::trace!("error: {err}");

    //             Err(Error::new(code, err))
    //         } else {
    //             Ok(RawTaos(ptr))
    //         }
    //     }
    //     #[inline]
    //     pub fn connect_auth(
    //         host: *const c_char,
    //         user: *const c_char,
    //         auth: *const c_char,
    //         db: *const c_char,
    //         port: u16,
    //     ) -> Result<Self, RawError> {
    //         let ptr = unsafe { taos_connect_auth(host, user, auth, db, port) };
    //         if ptr.is_null() {
    //             let null = std::ptr::null_mut();
    //             let code = unsafe { taos_errno(null) };
    //             let err = unsafe { CStr::from_ptr(taos_errstr(null)) }
    //                 .to_string_lossy()
    //                 .to_string();
    //             Err(Error::new(code, err))
    //         } else {
    //             Ok(RawTaos(ptr))
    //         }
    //     }

    #[inline]
    pub fn as_ptr(&self) -> *mut TAOS {
        self.ptr
    }

    #[inline]
    pub fn query<'a, S: IntoCStr<'a>>(&self, sql: S) -> Result<RawRes, RawError> {
        let sql = sql.into_c_str();
        log::debug!("query with sql: {:?}", sql);
        Ok(RawRes {
            c: self.c.clone(),
            ptr: unsafe { (self.c.taos_query)(self.as_ptr(), sql.as_ptr()) },
        })
    }

    #[inline]
    pub fn query_async<'a, S: IntoCStr<'a>>(&self, sql: S) -> QueryFuture<'a> {
        QueryFuture::new(self.clone(), sql)
    }

    #[inline]
    pub fn query_a<'a, S: IntoCStr<'a>>(
        &self,
        sql: S,
        fp: taos_async_query_cb,
        param: *mut c_void,
    ) {
        unsafe { (self.c.taos_query_a)(self.as_ptr(), sql.into_c_str().as_ptr(), fp, param) }
    }

    //     #[inline]
    //     pub fn validate_sql(self, sql: *const c_char) -> Result<(), RawError> {
    //         let code: Code = unsafe { taos_validate_sql(self.as_ptr(), sql) }.into();
    //         if code.success() {
    //             Ok(())
    //         } else {
    //             let err = unsafe { taos_errstr(std::ptr::null_mut()) };
    //             let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
    //             Err(Error::new(code, err))
    //         }
    //     }

    //     #[inline]
    //     pub fn reset_current_db(&self) {
    //         unsafe { taos_reset_current_db(self.as_ptr()) }
    //     }

    //     #[inline]
    //     pub fn server_version(&self) -> &CStr {
    //         unsafe { CStr::from_ptr(taos_get_server_info(self.as_ptr())) }
    //     }

    //     #[inline]
    //     pub fn load_table_info(&self, list: *const c_char) -> Result<(), RawError> {
    //         err_or!(taos_load_table_info(self.as_ptr(), list))
    //     }

    #[inline]
    pub fn write_raw_meta(&self, meta: raw_data_t) -> Result<(), RawError> {
        // try 5 times if write_raw_meta fails with 0x2603 error.
        let tmq_write_raw = self
            .c
            .tmq_write_raw
            .ok_or_else(|| RawError::from_string("2.x does not support write raw meta"))?;
        let taos_errstr = self.c.taos_errstr;
        let mut retries = 5;
        loop {
            let code = unsafe { tmq_write_raw(self.as_ptr(), meta) };
            let code = Code::from(code);
            if code.success() {
                return Ok(());
            }
            if code != Code::from(0x2603) {
                let err = unsafe { taos_errstr(std::ptr::null_mut()) };
                let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
                return Err(taos_query::prelude::RawError::new(code, err));
            }
            retries -= 1;
            if retries == 0 {
                let err = unsafe { taos_errstr(std::ptr::null_mut()) };
                let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
                return Err(taos_query::prelude::RawError::new(code, err));
            }
        }
    }

    #[inline]
    pub fn write_raw_block(&self, block: &RawBlock) -> Result<(), RawError> {
        let nrows = block.nrows();
        let name = block
            .table_name()
            .ok_or_else(|| RawError::new(Code::Failed, "raw block should have table name"))?;
        let ptr = block.as_raw_bytes().as_ptr();
        if let Some(f) = self.c.taos_write_raw_block_with_fields {
            let fields: Vec<_> = block.fields().into_iter().map(|f| f.to_c_field()).collect();
            err_or!(f(
                self.as_ptr(),
                nrows as _,
                ptr as _,
                name.into_c_str().as_ptr(),
                fields.as_ptr() as _,
                fields.len() as _,
            ))
        } else if let Some(f) = self.c.taos_write_raw_block {
            err_or!(f(
                self.as_ptr(),
                nrows as _,
                ptr as _,
                name.into_c_str().as_ptr()
            ))
        } else {
            unimplemented!("2.x does not support write raw block")
        }
    }

    #[inline]
    pub fn close(&mut self) {
        unsafe { (self.c.as_ref().taos_close)(self.as_ptr()) }
    }
}

#[derive(Debug, Clone)]
pub struct RawRes {
    c: Arc<ApiEntry>,
    ptr: *mut TAOS_RES,
}

unsafe impl Send for RawRes {}
unsafe impl Sync for RawRes {}

impl RawRes {
    #[inline]
    pub fn as_ptr(&self) -> *mut TAOS_RES {
        self.ptr
    }

    #[inline]
    fn errno(&self) -> Code {
        unsafe { (self.c.taos_errno)(self.as_ptr()) & 0xffff }.into()
    }
    // #[inline]
    // fn errstr(&self) -> &CStr {
    //     unsafe { CStr::from_ptr((self.c.taos_errstr)(self.as_ptr())) }
    // }
    #[inline]
    pub fn err_as_str(&self) -> &'static str {
        unsafe {
            std::str::from_utf8_unchecked(
                CStr::from_ptr((self.c.taos_errstr)(self.as_ptr())).to_bytes(),
            )
        }
    }
    #[inline]
    pub fn with_code(self, code: Code) -> Result<Self, RawError> {
        if code.success() {
            Ok(self)
        } else {
            Err(RawError::new(code, self.err_as_str()))
        }
    }
    #[inline]
    pub fn from_ptr(c: Arc<ApiEntry>, ptr: *mut TAOS_RES) -> Result<Self, RawError> {
        let raw = unsafe { Self::from_ptr_unchecked(c, ptr) };
        let code = raw.errno();
        raw.with_code(code)
    }
    #[inline]
    pub unsafe fn from_ptr_unchecked(c: Arc<ApiEntry>, ptr: *mut TAOS_RES) -> RawRes {
        Self { c, ptr }
    }
    #[inline]
    pub fn from_ptr_with_code(
        c: Arc<ApiEntry>,
        ptr: *mut TAOS_RES,
        code: Code,
    ) -> Result<RawRes, RawError> {
        unsafe { RawRes::from_ptr_unchecked(c, ptr) }.with_code(code)
    }

    #[inline]
    pub fn affected_rows(&self) -> i32 {
        unsafe { (self.c.taos_affected_rows)(self.as_ptr()) }
    }

    #[inline]
    pub fn free_result(&mut self) {
        unsafe { (self.c.taos_free_result)(self.as_ptr()) }
    }

    #[inline]
    pub fn precision(&self) -> Precision {
        unsafe { (self.c.taos_result_precision)(self.as_ptr()) }.into()
    }
    #[inline]
    pub fn field_count(&self) -> usize {
        unsafe { (self.c.taos_field_count)(self.as_ptr()) as _ }
    }
    pub fn fetch_fields(&self) -> Vec<Field> {
        let len = unsafe { (self.c.taos_field_count)(self.as_ptr()) };
        from_raw_fields(
            self.c.version(),
            unsafe { (self.c.taos_fetch_fields)(self.as_ptr()) },
            len as usize,
        )
    }
    #[inline]
    pub fn fetch_lengths(&self) -> &[u32] {
        unsafe {
            std::slice::from_raw_parts(
                (self.c.taos_fetch_lengths)(self.as_ptr()) as *const u32,
                self.field_count(),
            )
        }
    }
    #[inline]
    unsafe fn fetch_lengths_raw(&self) -> *const i32 {
        (self.c.taos_fetch_lengths)(self.as_ptr())
    }

    #[inline]
    pub fn fetch_block(&self) -> Result<Option<(TAOS_ROW, i32, *const i32)>, RawError> {
        let block = Box::into_raw(Box::new(std::ptr::null_mut()));
        // let mut num = 0;
        let num = unsafe { (self.c.taos_fetch_block)(self.as_ptr(), block) };
        // taos_fetch_block(res, rows)
        if num > 0 {
            Ok(Some(unsafe { (*block, num, self.fetch_lengths_raw()) }))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn fetch_raw_block(&self, fields: &[Field]) -> Result<Option<RawBlock>, RawError> {
        if self.c.is_v3() {
            self.fetch_raw_block_v3(fields)
        } else {
            self.fetch_raw_block_v2(fields)
        }
    }
    #[inline]
    fn fetch_raw_block_v2(&self, fields: &[Field]) -> Result<Option<RawBlock>, RawError> {
        let mut block: *mut *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        if let Some(taos_fetch_block_s) = self.c.taos_fetch_block_s {
            let fetch =
                unsafe { (taos_fetch_block_s)(self.as_ptr(), &mut num as _, &mut block as _) };
            let lengths = self.fetch_lengths();
            dbg!(lengths, fields);
            crate::err_or!(
                self,
                fetch,
                if num > 0 {
                    let raw = RawBlock::parse_from_ptr_v2(
                        block as _,
                        fields,
                        lengths,
                        num as usize,
                        self.precision(),
                    );
                    Some(raw)
                } else {
                    None
                }
            )
        } else {
            num = unsafe { (self.c.taos_fetch_block)(self.as_ptr(), &mut block as _) };
            let lengths = self.fetch_lengths();
            if num > 0 {
                let raw = RawBlock::parse_from_ptr_v2(
                    block as _,
                    fields,
                    lengths,
                    num as usize,
                    self.precision(),
                );
                Ok(Some(raw))
            } else {
                Ok(None)
            }
        }
    }
    #[inline]
    fn fetch_raw_block_v3(&self, fields: &[Field]) -> Result<Option<RawBlock>, RawError> {
        let mut block: *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        crate::err_or!(
            self,
            (self.c.taos_fetch_raw_block.unwrap())(self.as_ptr(), &mut num as _, &mut block as _),
            if num > 0 {
                match self.tmq_message_type() {
                    tmq_res_t::TMQ_RES_INVALID => {
                        let mut raw = RawBlock::parse_from_ptr(block as _, self.precision());
                        raw.with_field_names(fields.iter().map(Field::name));
                        Some(raw)
                    }
                    tmq_res_t::TMQ_RES_DATA | tmq_res_t::TMQ_RES_METADATA => {
                        let fields = self.fetch_fields();

                        let mut raw = RawBlock::parse_from_ptr(block as _, self.precision());

                        raw.with_field_names(fields.iter().map(Field::name));

                        if let Some(name) = self.tmq_db_name() {
                            raw.with_database_name(name);
                        }

                        if let Some(name) = self.tmq_table_name() {
                            raw.with_table_name(name);
                        }

                        Some(raw)
                    }
                    tmq_res_t::TMQ_RES_TABLE_META => {
                        todo!()
                    }
                }
            } else {
                None
            }
        )
    }

    pub fn fetch_raw_block_async_v2(
        &self,
        fields: &[Field],
        precision: Precision,
        state: &UnsafeCell<SharedState>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, RawError>> {
        let current = unsafe { &mut *state.get() };

        if current.done {
            // handle errors
            if current.code != 0 {
                let err = RawError::new(current.code, self.err_as_str());

                return Poll::Ready(Err(err));
            }

            if current.num > 0 {
                // has next block.
                let mut raw = RawBlock::parse_from_ptr_v2(
                    current.block as _,
                    fields,
                    self.fetch_lengths(),
                    current.num as usize,
                    precision,
                );
                // let mut raw = unsafe { RawBlock::parse_from_ptr(current.block as _, precision) };
                raw.with_field_names(fields.iter().map(|f| f.name()));
                if current.num == 0 {
                    // finish fetch loop.
                    current.done = true;
                    current.num = 0;
                } else {
                    current.done = false;
                }
                Poll::Ready(Ok(Some(raw)))
            } else {
                // no data todo, stop stream.
                Poll::Ready(Ok(None))
            }
        } else {
            let c = self.c.clone();
            let param = Box::new((state, c, cx.waker().clone()));
            unsafe extern "C" fn async_fetch_callback(
                param: *mut c_void,
                res: *mut TAOS_RES,
                num_of_rows: c_int,
            ) {
                let param = param as *mut (&UnsafeCell<SharedState>, Arc<ApiEntry>, Waker);
                let param = Box::from_raw(param);
                let state = &mut *param.0.get();
                state.done = true;
                if num_of_rows < 0 {
                    state.code = num_of_rows;
                } else {
                    state.num = num_of_rows as _;
                    if num_of_rows > 0 {
                        state.block = (param.1.taos_result_block.unwrap())(res).read() as _;
                    }
                }
                param.2.wake()
            }
            unsafe {
                (self.c.taos_fetch_rows_a)(
                    self.as_ptr(),
                    async_fetch_callback as _,
                    Box::into_raw(param) as *mut _ as _,
                )
            };
            Poll::Pending
        }
    }

    pub fn fetch_raw_block_async(
        &self,
        fields: &[Field],
        precision: Precision,
        state: &UnsafeCell<SharedState>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, RawError>> {
        if self.c.is_v3() {
            self.fetch_raw_block_async_v3(fields, precision, state, cx)
        } else {
            self.fetch_raw_block_async_v2(fields, precision, state, cx)
        }
    }
    pub fn fetch_raw_block_async_v3(
        &self,
        fields: &[Field],
        precision: Precision,
        state: &UnsafeCell<SharedState>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, RawError>> {
        let current = unsafe { &mut *state.get() };

        if current.done {
            // handle errors
            if current.code != 0 {
                let err = RawError::new(current.code, self.err_as_str());

                return Poll::Ready(Err(err));
            }

            if current.num > 0 {
                // has next block.
                let mut raw = unsafe { RawBlock::parse_from_ptr(current.block as _, precision) };
                raw.with_field_names(fields.iter().map(|f| f.name()));
                if current.num == 0 {
                    // finish fetch loop.
                    current.done = true;
                    current.num = 0;
                } else {
                    current.done = false;
                }
                Poll::Ready(Ok(Some(raw)))
            } else {
                // no data todo, stop stream.
                Poll::Ready(Ok(None))
            }
        } else {
            let c = self.c.clone();
            let param = Box::new((state, c, cx.waker().clone()));
            unsafe extern "C" fn async_fetch_callback(
                param: *mut c_void,
                res: *mut TAOS_RES,
                num_of_rows: c_int,
            ) {
                let param = param as *mut (&UnsafeCell<SharedState>, Arc<ApiEntry>, Waker);
                let param = Box::from_raw(param);
                let state = &mut *param.0.get();
                state.done = true;
                state.block = (param.1.taos_get_raw_block.unwrap())(res);
                if num_of_rows < 0 {
                    state.code = num_of_rows;
                } else {
                    state.num = num_of_rows as _;
                }
                param.2.wake()
            }
            unsafe {
                (self.c.taos_fetch_raw_block_a.unwrap())(
                    self.as_ptr(),
                    async_fetch_callback as _,
                    Box::into_raw(param) as *mut _ as _,
                )
            };
            Poll::Pending
        }
    }

    /// Only for tmq.
    pub(crate) fn fetch_raw_message(&self) -> Option<RawBlock> {
        let mut block: *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        unsafe {
            (self.c.taos_fetch_raw_block.unwrap())(self.as_ptr(), &mut num as _, &mut block as _)
        };
        let fields = self.fetch_fields();

        if num == 0 || block.is_null() {
            return None;
        }
        let mut raw = unsafe { RawBlock::parse_from_ptr(block as _, self.precision()) };

        raw.with_field_names(fields.iter().map(Field::name));

        if let Some(name) = self.tmq_table_name() {
            raw.with_table_name(name);
        }

        Some(raw)
    }

    #[inline]
    pub(crate) fn tmq_message_type(&self) -> tmq_res_t {
        unsafe { (self.c.tmq.as_ref().unwrap().tmq_get_res_type)(self.as_ptr()) }
    }
    #[inline]
    pub fn tmq_table_name(&self) -> Option<&str> {
        unsafe {
            let c = (self.c.tmq.as_ref().unwrap().tmq_get_table_name)(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }
    #[inline]
    pub(crate) fn tmq_db_name(&self) -> Option<&str> {
        unsafe {
            let c = (self.c.tmq.as_ref().unwrap().tmq_get_db_name)(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }
    #[inline]
    pub fn tmq_topic_name(&self) -> Option<&str> {
        unsafe {
            let c = (self.c.tmq.as_ref().unwrap().tmq_get_topic_name)(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }
    #[inline]
    pub fn tmq_vgroup_id(&self) -> Option<i32> {
        unsafe {
            let c = (self.c.tmq.as_ref().unwrap().tmq_get_vgroup_id)(self.as_ptr());
            if c == -1 {
                None
            } else {
                Some(c)
            }
        }
    }
    #[inline]
    pub(crate) fn tmq_get_json_meta(&self) -> CString {
        unsafe {
            let meta = (self.c.tmq.as_ref().unwrap().tmq_get_json_meta)(self.as_ptr());
            CString::from_raw(meta)
        }
    }

    #[inline]
    pub(crate) fn tmq_get_raw(&self) -> raw_data_t {
        let mut meta = raw_data_t {
            raw: std::ptr::null_mut(),
            raw_len: 0,
            raw_type: 0,
        };
        unsafe {
            let _code = (self.c.tmq.as_ref().unwrap().tmq_get_raw)(self.as_ptr(), &mut meta as _);
        }
        meta
    }
}

// type ResultSet = RawRes;

pub struct SharedState {
    pub block: *mut c_void,
    pub done: bool,
    pub num: usize,
    pub code: i32,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            block: std::ptr::null_mut(),
            done: Default::default(),
            num: Default::default(),
            code: Default::default(),
        }
    }
}
