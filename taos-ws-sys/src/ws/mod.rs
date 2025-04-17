#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;
use std::sync::OnceLock;
use std::time::Duration;

use error::{set_err_and_get_code, TaosError};
use query::QueryResultSet;
use sml::SchemalessResultSet;
use taos_error::Code;
use taos_log::QidManager;
use taos_query::common::{Field, Precision, Ty};
use taos_query::util::generate_req_id;
use taos_query::TBuilder;
use taos_ws::query::asyn::WS_ERROR_NO;
use taos_ws::query::Error;
use taos_ws::{Offset, Taos, TaosBuilder};
use tmq::TmqResultSet;
use tracing::{debug, error, instrument};

mod config;
pub mod error;
pub mod query;
pub mod sml;
pub mod stmt;
pub mod stmt2;
pub mod stub;
pub mod tmq;
pub mod util;

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

type TaosResult<T> = Result<T, TaosError>;

pub struct SafePtr<T>(pub T);

unsafe impl<T> Send for SafePtr<T> {}
unsafe impl<T> Sync for SafePtr<T> {}

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

impl TAOS_FIELD_E {
    pub fn new(field: &Field, precision: u8, scale: u8) -> Self {
        let mut name = [0 as c_char; 65];
        let field_name = field.name();
        unsafe {
            ptr::copy_nonoverlapping(
                field_name.as_ptr(),
                name.as_mut_ptr() as _,
                field_name.len(),
            );
        };

        Self {
            name,
            r#type: field.ty() as _,
            precision,
            scale,
            bytes: field.bytes() as _,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub bytes: i32,
}

impl From<&Field> for TAOS_FIELD {
    fn from(field: &Field) -> Self {
        let mut name = [0 as c_char; 65];
        let field_name = field.name();
        unsafe {
            ptr::copy_nonoverlapping(
                field_name.as_ptr(),
                name.as_mut_ptr() as _,
                field_name.len(),
            );
        };

        Self {
            name,
            r#type: field.ty() as i8,
            bytes: field.bytes() as i32,
        }
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
    match connect(ip, user, pass, db, port) {
        Ok(taos) => Box::into_raw(Box::new(taos)) as _,
        Err(mut err) => {
            error!("taos_connect failed, err: {err:?}");
            if err.code() == WS_ERROR_NO::WEBSOCKET_ERROR.as_code() {
                err = TaosError::new(Code::new(0x000B), "Unable to establish connection");
            }
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
    const DEFAULT_HOST: &str = "localhost";
    const DEFAULT_PORT: u16 = 6041;
    const DEFAULT_PORT_CLOUD: u16 = 443;
    const DEFAULT_USER: &str = "root";
    const DEFAULT_PASS: &str = "taosdata";
    const DEFAULT_DB: &str = "";

    let host = if ip.is_null() {
        DEFAULT_HOST
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

    let compression = config::get_global_compression();

    let dsn = if (host.contains("cloud.tdengine") || host.contains("cloud.taosdata"))
        && user == "token"
    {
        if port == 0 {
            port = DEFAULT_PORT_CLOUD;
        }
        format!("wss://{host}:{port}/{db}?token={pass}&compression={compression}")
    } else {
        if port == 0 {
            port = DEFAULT_PORT;
        }
        format!("ws://{user}:{pass}@{host}:{port}/{db}?compression={compression}")
    };

    debug!("taos_connect, dsn: {:?}", dsn);

    let builder = TaosBuilder::from_dsn(dsn)?;
    let mut taos = builder.build()?;
    builder.ping(&mut taos)?;
    Ok(taos)
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_close(taos: *mut TAOS) {
    debug!("taos_close, taos: {taos:?}");
    if taos.is_null() {
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
        return;
    }
    let _ = Box::from_raw(taos as *mut Taos);
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_options(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int {
    let mut c = config::CONFIG.write().unwrap();
    match option {
        TSDB_OPTION::TSDB_OPTION_CONFIGDIR => {
            if arg.is_null() {
                return set_err_and_get_code(TaosError::new(
                    Code::INVALID_PARA,
                    "taos cfg dir is null",
                ));
            }
            let dir = CStr::from_ptr(arg as _);
            if let Ok(dir) = dir.to_str() {
                c.set_config_dir(faststr::FastStr::new(dir));
                0
            } else {
                return set_err_and_get_code(TaosError::new(
                    Code::INVALID_PARA,
                    "taos cfg dir is invalid CStr",
                ));
            }
        }
        TSDB_OPTION::TSDB_OPTION_TIMEZONE => {
            if arg.is_null() {
                return set_err_and_get_code(TaosError::new(
                    Code::INVALID_PARA,
                    "taos timezone is null",
                ));
            }
            let tz = CStr::from_ptr(arg as _);
            if let Ok(tz) = tz.to_str() {
                c.set_timezone(tz);
                0
            } else {
                return set_err_and_get_code(TaosError::new(
                    Code::INVALID_PARA,
                    "taos timezone is invalid CStr",
                ));
            }
        }
        _ => 0,
    }
}

#[derive(Clone)]
struct Qid(u64);

impl QidManager for Qid {
    fn init() -> Self {
        Self(generate_req_id())
    }

    fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Qid {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Run once.
#[no_mangle]
pub extern "C" fn taos_init() -> c_int {
    static ONCE: OnceLock<c_int> = OnceLock::new();
    *ONCE.get_or_init(|| {
        if let Err(e) = taos_init_impl() {
            error!("taos_init failed, err: {e:?}");
        }
        0
    })
}

fn taos_init_impl() -> Result<(), Box<dyn std::error::Error>> {
    use taos_log::layer::TaosLayer;
    use taos_log::writer::RollingFileAppender;
    use tracing_log::LogTracer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer;

    unsafe {
        let locale = util::get_system_locale();
        let locale = std::ffi::CString::new(locale).unwrap();
        let locale_ptr = libc::setlocale(libc::LC_CTYPE, locale.as_ptr());
        if locale_ptr.is_null() {
            return Err(TaosError::new(Code::FAILED, "setlocale failed").into());
        }
    }

    if let Err(err) = config::init() {
        return Err(TaosError::new(Code::FAILED, &err).into());
    }

    let cfg = config::CONFIG.read().unwrap();
    if let Some(timezone) = cfg.timezone() {
        unsafe { std::env::set_var("TZ", timezone.as_str()) };
    }

    let mut layers = Vec::new();
    let config_dir = cfg.log_dir();

    let appender = RollingFileAppender::builder(config_dir.as_str(), "taos", 16)
        .compress(true)
        .reserved_disk_size("1GB")
        .rotation_count(3)
        .rotation_size("1GB")
        .build()?;

    layers.push(
        TaosLayer::<Qid>::new(appender)
            .with_location()
            .with_filter(cfg.log_level())
            .boxed(),
    );

    if cfg.log_output_to_screen() {
        layers.push(
            TaosLayer::<Qid, _, _>::new(std::io::stdout)
                .with_location()
                .with_filter(cfg.log_level())
                .boxed(),
        );
    }

    tracing_subscriber::registry().with(layers).init();

    LogTracer::init()?;

    debug!("taos_init, config: {cfg:?}");

    Ok(())
}

#[derive(Debug)]
struct Row {
    data: Vec<*const c_void>,
    current_row: usize,
}

impl Row {
    fn new(data: Vec<*const c_void>, current_row: usize) -> Self {
        Self { data, current_row }
    }
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

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E;

    unsafe fn fetch_raw_block(
        &mut self,
        ptr: *mut *mut c_void,
        rows: *mut i32,
    ) -> Result<(), Error>;

    unsafe fn fetch_block(&mut self, rows: *mut TAOS_ROW, num: *mut c_int) -> Result<(), Error>;

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error>;

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void);

    fn take_timing(&mut self) -> Duration;

    fn stop_query(&mut self);
}

#[derive(Debug)]
pub enum ResultSet {
    Query(QueryResultSet),
    Schemaless(SchemalessResultSet),
    Tmq(TmqResultSet),
}

impl ResultSetOperations for ResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_topic_name(),
            ResultSet::Schemaless(rs) => rs.tmq_get_topic_name(),
            ResultSet::Tmq(rs) => rs.tmq_get_topic_name(),
        }
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_db_name(),
            ResultSet::Schemaless(rs) => rs.tmq_get_db_name(),
            ResultSet::Tmq(rs) => rs.tmq_get_db_name(),
        }
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_table_name(),
            ResultSet::Schemaless(rs) => rs.tmq_get_table_name(),
            ResultSet::Tmq(rs) => rs.tmq_get_table_name(),
        }
    }

    fn tmq_get_offset(&self) -> Offset {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_offset(),
            ResultSet::Schemaless(rs) => rs.tmq_get_offset(),
            ResultSet::Tmq(rs) => rs.tmq_get_offset(),
        }
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_vgroup_offset(),
            ResultSet::Schemaless(rs) => rs.tmq_get_vgroup_offset(),
            ResultSet::Tmq(rs) => rs.tmq_get_vgroup_offset(),
        }
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_vgroup_id(),
            ResultSet::Schemaless(rs) => rs.tmq_get_vgroup_id(),
            ResultSet::Tmq(rs) => rs.tmq_get_vgroup_id(),
        }
    }

    fn precision(&self) -> Precision {
        match self {
            ResultSet::Query(rs) => rs.precision(),
            ResultSet::Schemaless(rs) => rs.precision(),
            ResultSet::Tmq(rs) => rs.precision(),
        }
    }

    fn affected_rows(&self) -> i32 {
        match self {
            ResultSet::Query(rs) => rs.affected_rows(),
            ResultSet::Schemaless(rs) => rs.affected_rows(),
            ResultSet::Tmq(rs) => rs.affected_rows(),
        }
    }

    fn affected_rows64(&self) -> i64 {
        match self {
            ResultSet::Query(rs) => rs.affected_rows64(),
            ResultSet::Schemaless(rs) => rs.affected_rows64(),
            ResultSet::Tmq(rs) => rs.affected_rows64(),
        }
    }

    fn num_of_fields(&self) -> i32 {
        match self {
            ResultSet::Query(rs) => rs.num_of_fields(),
            ResultSet::Schemaless(rs) => rs.num_of_fields(),
            ResultSet::Tmq(rs) => rs.num_of_fields(),
        }
    }

    fn get_fields(&mut self) -> *mut TAOS_FIELD {
        match self {
            ResultSet::Query(rs) => rs.get_fields(),
            ResultSet::Schemaless(rs) => rs.get_fields(),
            ResultSet::Tmq(rs) => rs.get_fields(),
        }
    }

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E {
        match self {
            ResultSet::Query(rs) => rs.get_fields_e(),
            ResultSet::Schemaless(rs) => rs.get_fields_e(),
            ResultSet::Tmq(rs) => rs.get_fields_e(),
        }
    }

    unsafe fn fetch_raw_block(
        &mut self,
        ptr: *mut *mut c_void,
        rows: *mut i32,
    ) -> Result<(), Error> {
        match self {
            ResultSet::Query(rs) => rs.fetch_raw_block(ptr, rows),
            ResultSet::Schemaless(rs) => rs.fetch_raw_block(ptr, rows),
            ResultSet::Tmq(rs) => rs.fetch_raw_block(ptr, rows),
        }
    }

    unsafe fn fetch_block(&mut self, rows: *mut TAOS_ROW, num: *mut c_int) -> Result<(), Error> {
        match self {
            ResultSet::Query(rs) => rs.fetch_block(rows, num),
            ResultSet::Schemaless(rs) => rs.fetch_block(rows, num),
            ResultSet::Tmq(rs) => rs.fetch_block(rows, num),
        }
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        match self {
            ResultSet::Query(rs) => rs.fetch_row(),
            ResultSet::Schemaless(rs) => rs.fetch_row(),
            ResultSet::Tmq(rs) => rs.fetch_row(),
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        match self {
            ResultSet::Query(rs) => rs.get_raw_value(row, col),
            ResultSet::Schemaless(rs) => rs.get_raw_value(row, col),
            ResultSet::Tmq(rs) => rs.get_raw_value(row, col),
        }
    }

    fn take_timing(&mut self) -> Duration {
        match self {
            ResultSet::Query(rs) => rs.take_timing(),
            ResultSet::Schemaless(rs) => rs.take_timing(),
            ResultSet::Tmq(rs) => rs.take_timing(),
        }
    }

    fn stop_query(&mut self) {
        match self {
            ResultSet::Query(rs) => rs.stop_query(),
            ResultSet::Schemaless(rs) => rs.stop_query(),
            ResultSet::Tmq(rs) => rs.stop_query(),
        }
    }
}

#[cfg(test)]
pub fn test_connect() -> *mut TAOS {
    unsafe {
        let taos = taos_connect(
            c"localhost".as_ptr(),
            c"root".as_ptr(),
            c"taosdata".as_ptr(),
            ptr::null(),
            6041,
        );
        assert!(!taos.is_null());
        taos
    }
}

#[cfg(test)]
pub fn test_exec<S: AsRef<str>>(taos: *mut TAOS, sql: S) {
    unsafe {
        let sql = std::ffi::CString::new(sql.as_ref()).unwrap();
        let res = query::taos_query(taos, sql.as_ptr());
        if res.is_null() {
            let code = error::taos_errno(res);
            let err = error::taos_errstr(res);
            println!("code: {}, err: {:?}", code, CStr::from_ptr(err));
        }
        assert!(!res.is_null());
        query::taos_free_result(res);
    }
}

#[cfg(test)]
pub fn test_exec_many<T, S>(taos: *mut TAOS, sqls: S)
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
    use std::ffi::CString;
    use std::ptr;

    use super::*;
    use crate::ws::error::{taos_errno, taos_errstr};

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
    fn test_taos_connect_unable_to_establish_connection() {
        unsafe {
            let taos = taos_connect(
                c"invalid_host".as_ptr(),
                c"root".as_ptr(),
                c"taosdata".as_ptr(),
                ptr::null(),
                6041,
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

    #[test]
    fn test_taos_options() {
        unsafe {
            let p = taos_options(TSDB_OPTION::TSDB_OPTION_CONFIGDIR, ptr::null());
            assert!(p != 0);
            let code = taos_errno(ptr::null_mut());
            let str = taos_errstr(ptr::null_mut());
            assert_eq!(Code::from(code), Code::INVALID_PARA);
            assert_eq!(
                CStr::from_ptr(str).to_str().unwrap(),
                "taos cfg dir is null"
            );

            let p = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                CString::new("invalid_path").unwrap().as_ptr() as *const c_void,
            );
            assert_eq!(p, 0);

            let p = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                CString::new("tests/").unwrap().as_ptr() as *const c_void,
            );
            assert_eq!(p, 0);
            taos_options(TSDB_OPTION::TSDB_OPTION_TIMEZONE, ptr::null());
            taos_options(
                TSDB_OPTION::TSDB_OPTION_TIMEZONE,
                CString::new("invalid_timezone").unwrap().as_ptr() as *const c_void,
            );
            taos_options(
                TSDB_OPTION::TSDB_OPTION_TIMEZONE,
                CString::new("UTC").unwrap().as_ptr() as *const c_void,
            );
            taos_options(
                TSDB_OPTION::TSDB_OPTION_TIMEZONE,
                CString::new("invalid_timezone").unwrap().as_ptr() as *const c_void,
            );
        }
    }

    #[test]
    fn test_taos_options_release_arg_memory() {
        unsafe {
            let arg = CString::new("/etc/taos").unwrap();
            let code = taos_options(TSDB_OPTION::TSDB_OPTION_CONFIGDIR, arg.as_ptr() as *const _);
            assert_eq!(code, 0);
            drop(arg);
            let config = config::CONFIG.read().unwrap();
            let cfg_dir = config.config_dir.as_ref();
            assert_eq!(cfg_dir, Some(&faststr::FastStr::from("/etc/taos")));
        }
    }
}
