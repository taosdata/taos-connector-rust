#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};

use once_cell::sync::Lazy;
use tracing::instrument;

use crate::native::{default_lib_name, ApiEntry};
use crate::ws;
use crate::ws::stub;

pub mod query;
pub mod sml;
pub mod stmt;
pub mod stmt2;
pub mod tmq;

static DRIVER: AtomicBool = AtomicBool::new(true);
static CAPI: Lazy<ApiEntry> = Lazy::new(|| match ApiEntry::open_default() {
    Ok(api) => api,
    Err(err) => panic!("Can't open {} library: {:?}", default_lib_name(), err),
});

pub type TAOS = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_RES = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_ROW = *mut *mut c_void;

#[allow(non_camel_case_types)]
pub type __taos_async_fn_t = extern "C" fn(param: *mut c_void, res: *mut TAOS_RES, code: c_int);

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub enum TSDB_OPTION {
    TSDB_OPTION_LOCALE = 0,
    TSDB_OPTION_CHARSET = 1,
    TSDB_OPTION_TIMEZONE = 2,
    TSDB_OPTION_CONFIGDIR = 3,
    TSDB_OPTION_SHELL_ACTIVITY_TIMER = 4,
    TSDB_OPTION_USE_ADAPTER = 5,
    TSDB_OPTION_DRIVER = 6,
    TSDB_MAX_OPTIONS = 7,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub enum TSDB_OPTION_CONNECTION {
    TSDB_OPTION_CONNECTION_CLEAR = -1,
    TSDB_OPTION_CONNECTION_CHARSET = 0,
    TSDB_OPTION_CONNECTION_TIMEZONE = 1,
    TSDB_OPTION_CONNECTION_USER_IP = 2,
    TSDB_OPTION_CONNECTION_USER_APP = 3,
    TSDB_MAX_OPTIONS_CONNECTION = 4,
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_init() -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_init()
    } else {
        (CAPI.basic_api.taos_init)()
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_cleanup() {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_cleanup()
    } else {
        (CAPI.basic_api.taos_cleanup)()
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_connect(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    if DRIVER.load(Ordering::Relaxed) {
        ws::taos_connect(ip, user, pass, db, port)
    } else {
        (CAPI.basic_api.taos_connect)(ip, user, pass, db, port)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_connect_auth(
    ip: *const c_char,
    user: *const c_char,
    auth: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_connect_auth(ip, user, auth, db, port)
    } else {
        (CAPI.basic_api.taos_connect_auth)(ip, user, auth, db, port)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_connect_dsn(
    dsn: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
) -> *mut TAOS {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_connect_dsn(dsn, user, pass, db)
    } else {
        (CAPI.basic_api.taos_connect_dsn)(dsn, user, pass, db)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_connect_dsn_auth(
    dsn: *const c_char,
    user: *const c_char,
    auth: *const c_char,
    db: *const c_char,
) -> *mut TAOS {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_connect_dsn_auth(dsn, user, auth, db)
    } else {
        (CAPI.basic_api.taos_connect_dsn_auth)(dsn, user, auth, db)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_close(taos: *mut TAOS) {
    if DRIVER.load(Ordering::Relaxed) {
        ws::taos_close(taos)
    } else {
        (CAPI.basic_api.taos_close)(taos)
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
#[allow(clippy::just_underscores_and_digits)]
pub unsafe extern "C" fn taos_options(
    option: TSDB_OPTION,
    arg: *const c_void,
    varargs: ...
) -> c_int {
    // if DRIVER.load(Ordering::Relaxed) {
    //     native::taos_options(option, arg, varargs)
    // } else {
    //     (CAPI.basic_api.taos_options)(option, arg, varargs)
    // }

    todo!()
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
    varargs: ...
) -> c_int {
    // if DRIVER.load(Ordering::Relaxed) {
    //     todo!()
    //     native::taos_options_connection(taos, option, arg, varargs)
    // } else {
    //     (CAPI.basic_api.taos_options_connection)(taos, option, arg, varargs)
    // }

    todo!()
}
