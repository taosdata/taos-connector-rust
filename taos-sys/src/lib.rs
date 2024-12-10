#![allow(unused_variables)]
#![feature(c_variadic)]

use std::ffi::{c_char, c_int, c_void};

pub mod query;
pub mod sml;
pub mod stmt;
pub mod stub;
pub mod tmq;

pub type TAOS = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_RES = c_void;

#[repr(C)]
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

#[no_mangle]
pub extern "C" fn taos_init() -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_cleanup() {
    todo!()
}

#[no_mangle]
pub unsafe extern "C" fn taos_options(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_connect(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_connect_dsn(
    dsn: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
) -> *mut TAOS {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_close(taos: *mut TAOS) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_data_type(r#type: c_int) -> *const c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_errstr(res: *mut TAOS_RES) -> *const c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_errno(res: *mut TAOS_RES) -> c_int {
    todo!()
}
