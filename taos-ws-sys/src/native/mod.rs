#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void, CStr, VaList};
use std::ptr::{null, null_mut};
use std::sync::RwLock;

use error::set_err_and_get_code;
use once_cell::sync::Lazy;
use taos_error::{Code, Error};
use taos_query::common::Ty;
use taos_query::TBuilder;
use taos_ws::{Taos, TaosBuilder};
use tracing::trace;

pub mod error;
pub mod query;
pub mod sml;
pub mod stmt;
pub mod stub;
pub mod tmq;

pub type TAOS = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_RES = c_void;

type Result<T> = std::result::Result<T, Error>;

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
    if taos.is_null() {
        set_err_and_get_code(Error::new(Code::INVALID_PARA, "taos is null"));
        return;
    }

    unsafe {
        let _ = Box::from_raw(taos as *mut Taos);
    }
}

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

static PARAMS: Lazy<RwLock<String>> = Lazy::new(|| RwLock::new(String::new()));

// todo
#[allow(clippy::just_underscores_and_digits)]
#[no_mangle]
pub unsafe extern "C" fn taos_options(
    option: TSDB_OPTION,
    arg: *const c_void,
    mut varargs: VaList,
) -> c_int {
    let mut params = Vec::new();

    loop {
        let key_ptr: *const c_char = varargs.arg();
        if key_ptr.is_null() {
            break;
        }

        let key = match CStr::from_ptr(key_ptr).to_str() {
            Ok(key) => key,
            Err(_) => return set_err_and_get_code(Error::new(Code::INVALID_PARA, "Invalid key")),
        };

        let value_ptr: *const c_char = varargs.arg();
        if value_ptr.is_null() {
            break;
        }

        let value = match CStr::from_ptr(value_ptr).to_str() {
            Ok(value) => value,
            Err(_) => return set_err_and_get_code(Error::new(Code::INVALID_PARA, "Invalid value")),
        };

        params.push(format!("{key}={value}"));
    }

    trace!("dsn params: {:?}", params);

    *PARAMS.write().unwrap() = params.join("&");

    Code::SUCCESS.into()
}

#[no_mangle]
pub extern "C" fn taos_data_type(r#type: c_int) -> *const c_char {
    match Ty::from_u8_option(r#type as _) {
        Some(ty) => ty.tsdb_name(),
        None => null(),
    }
}

#[no_mangle]
pub extern "C" fn init_log() {
    use std::sync::Once;

    static ONCE_INIT: Once = Once::new();
    ONCE_INIT.call_once(|| {
        let mut builder = pretty_env_logger::formatted_timed_builder();
        builder.format_timestamp_nanos();
        builder.parse_filters("trace");
        builder.init();
    });

    trace!("init log");
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
        taos_close(taos);

        let taos = taos_connect(null(), null(), null(), null(), 0);
        assert!(!taos.is_null());
        taos_close(taos);

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

    #[test]
    fn test_taos_data_type() {
        unsafe {
            let type_null_ptr = taos_data_type(0);
            let type_null = CStr::from_ptr(type_null_ptr);
            assert_eq!(type_null, c"TSDB_DATA_TYPE_NULL");

            let type_geo_ptr = taos_data_type(20);
            let type_geo = CStr::from_ptr(type_geo_ptr);
            assert_eq!(type_geo, c"TSDB_DATA_TYPE_GEOMETRY");

            let type_invalid = taos_data_type(100);
            assert_eq!(type_invalid, null(),);
        }
    }
}
