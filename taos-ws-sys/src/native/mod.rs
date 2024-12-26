#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr::null_mut;

use error::set_err_and_get_code;
use taos_error::{Code, Error};
use taos_query::TBuilder;
use taos_ws::{Taos, TaosBuilder};

mod error;
pub mod query;
pub mod sml;
pub mod stmt;
pub mod stub;
pub mod tmq;

pub type TAOS = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_RES = c_void;

type Result<T> = std::result::Result<T, Error>;

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

#[allow(clippy::just_underscores_and_digits)]
#[no_mangle]
pub unsafe extern "C" fn taos_options(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int {
    todo!()
}

#[repr(C)]
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
pub unsafe extern "C" fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
    ...
) -> c_int {
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
    match connect(ip, user, pass, db, port) {
        Ok(taos) => Box::into_raw(Box::new(taos)) as _,
        Err(err) => {
            set_err_and_get_code(err);
            null_mut()
        }
    }
}

fn connect(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
    mut port: u16,
) -> Result<Taos> {
    const DEFAULT_IP: &str = "localhost";
    const DEFAULT_PORT: u16 = 6041;
    const DEFAULT_USER: &str = "root";
    const DEFAULT_PASS: &str = "taosdata";
    const DEFAULT_DB: &str = "";

    let ip = if ip.is_null() {
        DEFAULT_IP
    } else {
        let ip = unsafe { CStr::from_ptr(ip) };
        match ip.to_str() {
            Ok(ip) => ip,
            Err(_) => return Err(Error::new(Code::INVALID_PARA, "Invalid ip")),
        }
    };

    let user = if user.is_null() {
        DEFAULT_USER
    } else {
        let user = unsafe { CStr::from_ptr(user) };
        match user.to_str() {
            Ok(user) => user,
            Err(_) => return Err(Error::new(Code::INVALID_PARA, "Invalid user")),
        }
    };

    let pass = if pass.is_null() {
        DEFAULT_PASS
    } else {
        let pass = unsafe { CStr::from_ptr(pass) };
        match pass.to_str() {
            Ok(pass) => pass,
            Err(_) => return Err(Error::new(Code::INVALID_PARA, "Invalid pass")),
        }
    };

    let db = if db.is_null() {
        DEFAULT_DB
    } else {
        let db = unsafe { CStr::from_ptr(db) };
        match db.to_str() {
            Ok(db) => db,
            Err(_) => return Err(Error::new(Code::INVALID_PARA, "Invalid db")),
        }
    };

    if port == 0 {
        port = DEFAULT_PORT;
    }

    let dsn = format!("ws://{user}:{pass}@{ip}:{port}/{db}");
    let builder = TaosBuilder::from_dsn(dsn)?;
    let mut taos = builder.build()?;
    builder.ping(&mut taos)?;
    Ok(taos)
}

#[no_mangle]
pub extern "C" fn taos_close(taos: *mut TAOS) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_data_type(r#type: c_int) -> *const c_char {
    todo!()
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr::null;

    use super::*;

    #[test]
    fn test_taos_connect() {
        let taos = taos_connect(
            c"localhost".as_ptr(),
            c"root".as_ptr(),
            c"taosdata".as_ptr(),
            null(),
            6041,
        );
        assert!(!taos.is_null());

        let taos = taos_connect(null(), null(), null(), null(), 0);
        assert!(!taos.is_null());

        let invalid_utf8 = CString::new([0xff, 0xfe, 0xfd]).unwrap();
        let invalid_utf8_ptr = invalid_utf8.as_ptr();

        let taos = taos_connect(invalid_utf8_ptr, null(), null(), null(), 0);
        assert!(taos.is_null());

        let taos = taos_connect(null(), invalid_utf8_ptr, null(), null(), 0);
        assert!(taos.is_null());

        let taos = taos_connect(null(), null(), invalid_utf8_ptr, null(), 0);
        assert!(taos.is_null());

        let taos = taos_connect(null(), null(), null(), invalid_utf8_ptr, 0);
        assert!(taos.is_null());
    }
}
