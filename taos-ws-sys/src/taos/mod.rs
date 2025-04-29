use std::ffi::{c_char, c_int, c_void, CStr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Once};

use taos_error::Code;
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
static DRIVER: AtomicBool = AtomicBool::new(true);

#[inline]
fn set_driver(driver: bool) {
    DRIVER.store(driver, Ordering::Relaxed);
}

#[inline]
fn driver() -> bool {
    DRIVER.load(Ordering::Relaxed)
}

fn init_driver_from_env() {
    static DRIVER_INIT: Once = Once::new();
    DRIVER_INIT.call_once(|| {
        if let Ok(driver) = std::env::var("TAOS_DRIVER") {
            match driver.as_str() {
                "websocket" => set_driver(true),
                "native" => set_driver(false),
                _ => {
                    tracing::warn!("Invalid TAOS_DRIVER value: {driver}");
                    set_driver(true);
                }
            }
        }
    });
}

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
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_init() -> c_int {
    init_driver_from_env();

    if driver() {
        ws::taos_init()
    } else {
        (CAPI.basic_api.taos_init)()
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_cleanup() {
    if driver() {
        stub::taos_cleanup();
    } else {
        (CAPI.basic_api.taos_cleanup)();
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_connect(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    init_driver_from_env();

    if driver() {
        ws::taos_connect(ip, user, pass, db, port)
    } else {
        (CAPI.basic_api.taos_connect)(ip, user, pass, db, port)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_connect_auth(
    ip: *const c_char,
    user: *const c_char,
    auth: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    if driver() {
        stub::taos_connect_auth(ip, user, auth, db, port)
    } else {
        (CAPI.basic_api.taos_connect_auth)(ip, user, auth, db, port)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_close(taos: *mut TAOS) {
    if driver() {
        ws::taos_close(taos);
    } else {
        (CAPI.basic_api.taos_close)(taos);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_options(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int {
    if arg.is_null() {
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "arg is null"));
    }

    if option == TSDB_OPTION::TSDB_OPTION_DRIVER {
        init_driver_from_env();

        let driver = match CStr::from_ptr(arg as _).to_str() {
            Ok("native") => false,
            Ok("websocket") => true,
            _ => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "arg is invalid")),
        };

        static DRIVER_INIT: Once = Once::new();
        DRIVER_INIT.call_once(|| {
            set_driver(driver);
        });
    }

    if driver() {
        ws::taos_options(option, arg)
    } else {
        (CAPI.basic_api.taos_options)(option, arg)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
    _varargs: ...
) -> c_int {
    if driver() {
        stub::taos_options_connection(taos, option, arg)
    } else {
        (CAPI.basic_api.taos_options_connection)(taos, option, arg)
    }
}

#[cfg(test)]
pub(crate) unsafe fn test_connect() -> *mut TAOS {
    let taos = taos_connect(
        c"localhost".as_ptr(),
        c"root".as_ptr(),
        c"taosdata".as_ptr(),
        std::ptr::null(),
        0,
    );
    assert!(!taos.is_null());
    taos
}

#[cfg(test)]
pub(crate) unsafe fn test_exec<S: AsRef<str>>(taos: *mut TAOS, sql: S) {
    let sql = std::ffi::CString::new(sql.as_ref()).unwrap();
    let res = query::taos_query(taos, sql.as_ptr());
    if res.is_null() {
        let code = query::taos_errno(res);
        let err = query::taos_errstr(res);
        println!("code: {}, err: {:?}", code, CStr::from_ptr(err));
    }
    assert!(!res.is_null());
    query::taos_free_result(res);
}

#[cfg(test)]
pub(crate) unsafe fn test_exec_many<T, S>(taos: *mut TAOS, sqls: S)
where
    T: AsRef<str>,
    S: IntoIterator<Item = T>,
{
    for sql in sqls {
        test_exec(taos, sql);
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::taos::query::{taos_errno, taos_errstr};

    #[test]
    fn test_api() {
        unsafe {
            let code = taos_init();
            assert_eq!(code, 0);

            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_TIMEZONE,
                c"Asia/Shanghai".as_ptr() as *const _,
            );
            assert_eq!(code, 0);

            let taos = taos_connect(
                c"localhost".as_ptr(),
                c"root".as_ptr(),
                c"taosdata".as_ptr(),
                ptr::null(),
                0,
            );
            assert!(!taos.is_null());

            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_TIMEZONE,
                c"Asia/Shanghai".as_ptr() as *const _,
            );
            assert_eq!(code, 0);

            taos_close(taos);

            let taos = taos_connect_auth(
                c"localhost".as_ptr(),
                c"root".as_ptr(),
                c"dcc5bed04851fec854c035b2e40263b6".as_ptr(),
                ptr::null(),
                0,
            );
            if driver() {
                assert_eq!(taos, ptr::null_mut());
            } else {
                assert!(!taos.is_null());
                taos_close(taos);
            }

            taos_cleanup();
        }
    }

    #[test]
    fn test_taos_connect_unable_to_establish_connection() {
        unsafe {
            let taos = taos_connect(
                c"invalid_host".as_ptr(),
                c"root".as_ptr(),
                c"taosdata".as_ptr(),
                ptr::null(),
                0,
            );
            assert!(taos.is_null());

            let code = taos_errno(ptr::null_mut());
            let errstr = taos_errstr(ptr::null_mut());
            assert_eq!(Code::from(code), Code::new(0x000B));
            assert_eq!(
                CStr::from_ptr(errstr).to_str().unwrap(),
                "Unable to establish connection"
            );
        }
    }
}
