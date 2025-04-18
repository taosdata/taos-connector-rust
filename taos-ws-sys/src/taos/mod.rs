use std::ffi::{c_char, c_int, c_void, CStr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;

use taos_error::Code;
use tracing::instrument;
use ws::error::{set_err_and_get_code, TaosError};

use crate::native::{default_lib_name, ApiEntry};
use crate::ws;
use crate::ws::stub;

pub mod query;
pub mod sml;
pub mod stmt;
pub mod stmt2;
pub mod tmq;

/// The default driver is native.
static DRIVER: AtomicBool = AtomicBool::new(false);

static CAPI: LazyLock<ApiEntry> = LazyLock::new(|| match ApiEntry::open_default() {
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
#[derive(Debug, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum TSDB_OPTION {
    TSDB_OPTION_LOCALE,
    TSDB_OPTION_CHARSET,
    TSDB_OPTION_TIMEZONE,
    TSDB_OPTION_CONFIGDIR,
    TSDB_OPTION_SHELL_ACTIVITY_TIMER,
    TSDB_OPTION_USE_ADAPTER,
    TSDB_OPTION_DRIVER,
    TSDB_MAX_OPTIONS,
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

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub bytes: i32,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD_E {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_init() -> c_int {
    init_driver_from_env();

    if DRIVER.load(Ordering::Relaxed) {
        ws::taos_init()
    } else {
        (CAPI.basic_api.taos_init)()
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_cleanup() {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_cleanup();
    } else {
        (CAPI.basic_api.taos_cleanup)();
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_connect(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    init_driver_from_env();

    if DRIVER.load(Ordering::Relaxed) {
        ws::taos_connect(ip, user, pass, db, port)
    } else {
        (CAPI.basic_api.taos_connect)(ip, user, pass, db, port)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_connect_auth(
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
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_close(taos: *mut TAOS) {
    if DRIVER.load(Ordering::Relaxed) {
        ws::taos_close(taos);
    } else {
        (CAPI.basic_api.taos_close)(taos);
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_options(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int {
    if arg.is_null() {
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "arg is null"));
    }

    if option == TSDB_OPTION::TSDB_OPTION_DRIVER {
        if let Ok(driver) = CStr::from_ptr(arg as _).to_str() {
            match driver {
                "native" => DRIVER.store(false, Ordering::Relaxed),
                "websocket" => DRIVER.store(true, Ordering::Relaxed),
                _ => {
                    return set_err_and_get_code(TaosError::new(
                        Code::INVALID_PARA,
                        "arg is invalid driver",
                    ));
                }
            }
        } else {
            return set_err_and_get_code(TaosError::new(
                Code::INVALID_PARA,
                "arg is invalid utf-8",
            ));
        }
    }

    if DRIVER.load(Ordering::Relaxed) {
        ws::taos_options(option, arg)
    } else {
        (CAPI.basic_api.taos_options)(option, arg)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
    varargs: ...
) -> c_int {
    if DRIVER.load(Ordering::Relaxed) {
        stub::taos_options_connection(taos, option, arg)
    } else {
        (CAPI.basic_api.taos_options_connection)(taos, option, arg)
    }
}

fn init_driver_from_env() {
    let driver = match std::env::var("TAOS_DRIVER") {
        Ok(value) => match value.as_str() {
            "native" => false,
            "websocket" => true,
            _ => {
                eprintln!("Invalid TAOS_DRIVER value: {value}. Defaulting to native.");
                false
            }
        },
        Err(_) => false,
    };

    DRIVER.store(driver, Ordering::Relaxed);
}
