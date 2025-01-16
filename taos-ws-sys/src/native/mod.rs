#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;
use std::sync::RwLock;
use std::time::Duration;

use error::{set_err_and_get_code, TaosError};
use once_cell::sync::Lazy;
use taos_error::Code;
use taos_query::common::{Precision, Ty};
use taos_query::TBuilder;
use taos_ws::query::Error;
use taos_ws::{Offset, Taos, TaosBuilder};
use tracing::trace;

use crate::native::query::{QueryResultSet, TAOS_FIELD, TAOS_ROW};

pub mod error;
pub mod query;
pub mod sml;
pub mod stmt;
pub mod stub;
pub mod tmq;

pub type TAOS = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_RES = c_void;

type TaosResult<T> = Result<T, TaosError>;

#[no_mangle]
pub unsafe extern "C" fn taos_connect(
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
            ptr::null_mut()
        }
    }
}

unsafe fn connect(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
    mut port: u16,
) -> TaosResult<Taos> {
    const DEFAULT_IP: &str = "localhost";
    const DEFAULT_PORT: u16 = 6041;
    const DEFAULT_USER: &str = "root";
    const DEFAULT_PASS: &str = "taosdata";
    const DEFAULT_DB: &str = "";

    let ip = if ip.is_null() {
        DEFAULT_IP
    } else {
        CStr::from_ptr(ip).to_str()?
    };

    let user = if user.is_null() {
        DEFAULT_USER
    } else {
        CStr::from_ptr(user).to_str()?
    };

    let pass = if pass.is_null() {
        DEFAULT_PASS
    } else {
        CStr::from_ptr(pass).to_str()?
    };

    let db = if db.is_null() {
        DEFAULT_DB
    } else {
        CStr::from_ptr(db).to_str()?
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
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
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

#[allow(clippy::just_underscores_and_digits)]
#[no_mangle]
pub unsafe extern "C" fn taos_options(
    option: TSDB_OPTION,
    arg: *const c_void,
    mut varargs: ...
) -> c_int {
    let mut params = Vec::new();

    loop {
        let key_ptr: *const c_char = varargs.arg();
        if key_ptr.is_null() {
            break;
        }

        let key = match CStr::from_ptr(key_ptr).to_str() {
            Ok(key) => key,
            Err(_) => {
                return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "Invalid key"))
            }
        };

        let value_ptr: *const c_char = varargs.arg();
        if value_ptr.is_null() {
            break;
        }

        let value = match CStr::from_ptr(value_ptr).to_str() {
            Ok(value) => value,
            Err(_) => {
                return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "Invalid value"))
            }
        };

        params.push(format!("{key}={value}"));
    }

    trace!(params=?params, "dsn");

    *PARAMS.write().unwrap() = params.join("&");

    Code::SUCCESS.into()
}

#[no_mangle]
pub extern "C" fn taos_data_type(r#type: c_int) -> *const c_char {
    match Ty::from_u8_option(r#type as _) {
        Some(ty) => ty.tsdb_name(),
        None => ptr::null(),
    }
}

#[ctor::ctor]
fn init_logger() {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .init();
}

pub trait ResultSetOperations {
    fn tmq_get_topic_name(&self) -> *const c_char;

    fn tmq_get_db_name(&self) -> *const c_char;

    fn tmq_get_table_name(&self) -> *const c_char;

    fn tmq_get_vgroup_offset(&self) -> i64;

    fn tmq_get_vgroup_id(&self) -> i32;

    fn tmq_get_offset(&self) -> Offset;

    fn precision(&self) -> Precision;

    fn affected_rows(&self) -> i32;

    fn affected_rows64(&self) -> i64;

    fn num_of_fields(&self) -> i32;

    fn get_fields(&mut self) -> *mut TAOS_FIELD;

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error>;

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error>;

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void);

    fn take_timing(&mut self) -> Duration;

    fn stop_query(&mut self);
}

#[derive(Debug)]
pub enum ResultSet {
    Query(QueryResultSet),
}

impl ResultSetOperations for ResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_topic_name(),
        }
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_db_name(),
        }
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_table_name(),
        }
    }

    fn tmq_get_offset(&self) -> Offset {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_offset(),
        }
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_vgroup_offset(),
        }
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_vgroup_id(),
        }
    }

    fn precision(&self) -> Precision {
        match self {
            ResultSet::Query(rs) => rs.precision(),
        }
    }

    fn affected_rows(&self) -> i32 {
        match self {
            ResultSet::Query(rs) => rs.affected_rows(),
        }
    }

    fn affected_rows64(&self) -> i64 {
        match self {
            ResultSet::Query(rs) => rs.affected_rows64(),
        }
    }

    fn num_of_fields(&self) -> i32 {
        match self {
            ResultSet::Query(rs) => rs.num_of_fields(),
        }
    }

    fn get_fields(&mut self) -> *mut TAOS_FIELD {
        match self {
            ResultSet::Query(rs) => rs.get_fields(),
        }
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error> {
        match self {
            ResultSet::Query(rs) => rs.fetch_block(ptr, rows),
        }
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        match self {
            ResultSet::Query(rs) => rs.fetch_row(),
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        match self {
            ResultSet::Query(rs) => rs.get_raw_value(row, col),
        }
    }

    fn take_timing(&mut self) -> Duration {
        match self {
            ResultSet::Query(rs) => rs.take_timing(),
        }
    }

    fn stop_query(&mut self) {
        match self {
            ResultSet::Query(rs) => rs.stop_query(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;

    use super::*;

    #[test]
    fn test_taos_connect() {
        unsafe {
            let taos = taos_connect(
                c"localhost".as_ptr(),
                c"root".as_ptr(),
                c"taosdata".as_ptr(),
                ptr::null(),
                6041,
            );
            assert!(!taos.is_null());
            taos_close(taos);

            let taos = taos_connect(ptr::null(), ptr::null(), ptr::null(), ptr::null(), 0);
            assert!(!taos.is_null());
            taos_close(taos);

            let invalid_utf8 = CString::new([0xff, 0xfe, 0xfd]).unwrap();
            let invalid_utf8_ptr = invalid_utf8.as_ptr();

            let taos = taos_connect(invalid_utf8_ptr, ptr::null(), ptr::null(), ptr::null(), 0);
            assert!(taos.is_null());

            let taos = taos_connect(ptr::null(), invalid_utf8_ptr, ptr::null(), ptr::null(), 0);
            assert!(taos.is_null());

            let taos = taos_connect(ptr::null(), ptr::null(), invalid_utf8_ptr, ptr::null(), 0);
            assert!(taos.is_null());

            let taos = taos_connect(ptr::null(), ptr::null(), ptr::null(), invalid_utf8_ptr, 0);
            assert!(taos.is_null());
        }
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
            assert_eq!(type_invalid, ptr::null(),);
        }
    }
}
