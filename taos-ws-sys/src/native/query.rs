use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::OnceLock;
use std::time::Duration;
use std::{ptr, slice};

use bytes::Bytes;
use cargo_metadata::MetadataCommand;
use taos_error::Code;
use taos_query::common::{Precision, Ty};
use taos_query::util::{generate_req_id, hex, InlineBytes, InlineNChar, InlineStr};
use taos_query::{Fetchable, Queryable, RawBlock as Block};
use taos_ws::query::Error;
use taos_ws::{Offset, Taos};
use tracing::trace;

use crate::native::error::{set_err_and_get_code, TaosError, TaosMaybeError};
use crate::native::{
    ResultSet, ResultSetOperations, Row, TaosResult, __taos_async_fn_t, TAOS, TAOS_FIELD, TAOS_RES,
    TAOS_ROW,
};

#[no_mangle]
pub unsafe extern "C" fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES {
    taos_query_with_reqid(taos, sql, generate_req_id() as _)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_query_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    reqId: i64,
) -> *mut TAOS_RES {
    trace!(taos=?taos, req_id=reqId,"taos_query_with_reqid sql={:?}", CStr::from_ptr(sql));
    let res: TaosMaybeError<ResultSet> = query(taos, sql, reqId as u64).into();
    trace!(res=?res, "taos_query_with_reqid done");
    Box::into_raw(Box::new(res)) as _
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_row(res: *mut TAOS_RES) -> TAOS_ROW {
    fn handle_error(code: Code, msg: &str) -> TAOS_ROW {
        set_err_and_get_code(TaosError::new(code, msg));
        ptr::null_mut()
    }

    trace!(res=?res, "taos_fetch_row");

    let rs = match (res as *mut TaosMaybeError<ResultSet>).as_mut() {
        Some(rs) => rs,
        None => return handle_error(Code::INVALID_PARA, "res is null"),
    };

    let rs = match rs.deref_mut() {
        Some(rs) => rs,
        None => return handle_error(Code::INVALID_PARA, "data is null"),
    };

    match rs.fetch_row() {
        Ok(row) => {
            trace!(row=?row, "taos_fetch_row done");
            row
        }
        Err(err) => handle_error(err.errno(), &err.errstr()),
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_result_precision(res: *mut TAOS_RES) -> c_int {
    trace!(res=?res, "taos_result_precision");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            trace!(rs=?rs, "taos_result_precision done");
            rs.precision() as _
        }
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_free_result(res: *mut TAOS_RES) {
    trace!(res=?res, "taos_free_result");
    if !res.is_null() {
        let _ = Box::from_raw(res as *mut TaosMaybeError<ResultSet>);
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_field_count(res: *mut TAOS_RES) -> c_int {
    trace!(res=?res, "taos_field_count");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            trace!(rs=?rs, "taos_field_count done");
            rs.num_of_fields()
        }
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_num_fields(res: *mut TAOS_RES) -> c_int {
    taos_field_count(res)
}

#[no_mangle]
pub unsafe extern "C" fn taos_affected_rows(res: *mut TAOS_RES) -> c_int {
    trace!(res=?res, "taos_affected_rows");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            trace!(rs=?rs, "taos_affected_rows done");
            rs.affected_rows() as _
        }
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_affected_rows64(res: *mut TAOS_RES) -> i64 {
    trace!(res=?res, "taos_affected_rows64");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            trace!(rs=?rs, "taos_affected_rows64 done");
            rs.affected_rows64() as _
        }
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null")) as _,
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_fields(res: *mut TAOS_RES) -> *mut TAOS_FIELD {
    trace!(res=?res, "taos_fetch_fields");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            trace!(rs=?rs, "taos_fetch_fields done");
            rs.get_fields()
        }
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_select_db(taos: *mut TAOS, db: *const c_char) -> c_int {
    trace!(taos=?taos, "taos_select_db db={:?}", CStr::from_ptr(db));

    let taos = match (taos as *mut Taos).as_mut() {
        Some(taos) => taos,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null")),
    };

    let db = match CStr::from_ptr(db).to_str() {
        Ok(db) => db,
        Err(_) => {
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "db is null"));
        }
    };

    match taos.query(format!("use {db}")) {
        Ok(_) => Code::SUCCESS.into(),
        Err(err) => set_err_and_get_code(TaosError::from(err)),
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_print_row(
    str: *mut c_char,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    taos_print_row_with_size(str, i32::MAX as _, row, fields, num_fields)
}

#[no_mangle]
pub unsafe extern "C" fn taos_print_row_with_size(
    str: *mut c_char,
    size: u32,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    unsafe fn write_to_cstr(size: &mut usize, str: *mut c_char, content: &str) -> i32 {
        if content.len() > *size {
            return -1;
        }
        let cstr = CString::new(content).unwrap();
        ptr::copy_nonoverlapping(cstr.as_ptr(), str, content.len());
        *size -= content.len();
        content.len() as _
    }

    trace!(str=?str, size, row=?row, fields=?fields, num_fields, "taos_print_row_with_size");

    if str.is_null() || row.is_null() || fields.is_null() || num_fields <= 0 {
        return Code::SUCCESS.into();
    }

    let row = slice::from_raw_parts(row, num_fields as usize);
    let fields = slice::from_raw_parts(fields, num_fields as usize);

    let mut len: usize = 0;
    let mut size = (size - 1) as usize;

    for i in 0..num_fields as usize {
        if i > 0 && size > 0 {
            *str.add(len) = ' ' as c_char;
            len += 1;
            size -= 1;
        }

        let write_len = if row[i].is_null() {
            write_to_cstr(&mut size, str.add(len), "NULL")
        } else {
            macro_rules! read_and_write {
                ($ty:ty) => {{
                    let value = ptr::read_unaligned(row[i] as *const $ty);
                    write_to_cstr(
                        &mut size,
                        str.add(len as usize),
                        format!("{value}").as_str(),
                    )
                }};
            }

            match Ty::from(fields[i].r#type) {
                Ty::TinyInt => read_and_write!(i8),
                Ty::UTinyInt => read_and_write!(u8),
                Ty::SmallInt => read_and_write!(i16),
                Ty::USmallInt => read_and_write!(u16),
                Ty::Int => read_and_write!(i32),
                Ty::UInt => read_and_write!(u32),
                Ty::BigInt | Ty::Timestamp => read_and_write!(i64),
                Ty::UBigInt => read_and_write!(u64),
                Ty::Float => read_and_write!(f32),
                Ty::Double => read_and_write!(f64),
                Ty::Bool => {
                    let value = ptr::read_unaligned(row[i] as *const bool);
                    write_to_cstr(
                        &mut size,
                        str.add(len),
                        format!("{}", value as i32).as_str(),
                    )
                }
                Ty::VarBinary | Ty::Geometry => {
                    let data = row[i].offset(-2) as *const InlineBytes;
                    let data = Bytes::from((*data).as_bytes());
                    write_to_cstr(&mut size, str.add(len), &hex::bytes_to_hex_string(data))
                }
                Ty::VarChar => {
                    let data = row[i].offset(-2) as *const InlineStr;
                    write_to_cstr(&mut size, str.add(len), (*data).as_str())
                }
                Ty::NChar => {
                    let data = row[i].offset(-2) as *const InlineNChar;
                    write_to_cstr(&mut size, str.add(len), &(*data).to_string())
                }
                _ => 0,
            }
        };

        if write_len == -1 {
            break;
        }
        len += write_len as usize;
    }

    *str.add(len) = 0;

    trace!(
        len,
        "taos_print_row_with_size done str={:?}",
        CStr::from_ptr(str)
    );

    len as _
}

#[no_mangle]
pub unsafe extern "C" fn taos_stop_query(res: *mut TAOS_RES) {
    trace!(res=?res, "taos_stop_query");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            trace!(rs=?rs, "taos_stop_query done");
            rs.stop_query();
        }
        None => {
            let _ = set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"));
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_is_null(res: *mut TAOS_RES, row: i32, col: i32) -> bool {
    trace!(res=?res, row, col, "taos_is_null");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(ResultSet::Query(rs)) => match &rs.block {
            Some(block) => block.is_null(row as _, col as _),
            None => true,
        },
        _ => true,
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_is_null_by_column(
    res: *mut TAOS_RES,
    columnIndex: c_int,
    result: *mut bool,
    rows: *mut c_int,
) -> c_int {
    todo!()
}

#[no_mangle]
pub unsafe extern "C" fn taos_is_update_query(res: *mut TAOS_RES) -> bool {
    trace!(res=?res, "taos_is_update_query");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => rs.num_of_fields() == 0,
        None => true,
    }
}

#[no_mangle]
pub extern "C" fn taos_fetch_block(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_fetch_block_s(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    rows: *mut TAOS_ROW,
) -> c_int {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_fetch_raw_block(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    pData: *mut *mut c_void,
) -> c_int {
    unsafe fn handle_error(message: &str, rows: *mut i32) -> i32 {
        *rows = 0;
        set_err_and_get_code(TaosError::new(Code::FAILED, message))
    }

    trace!(res=?res, num_of_rows=?numOfRows, p_data=?pData, "taos_fetch_raw_block");

    match (res as *mut TaosMaybeError<ResultSet>).as_mut() {
        Some(maybe_err) => match maybe_err.deref_mut() {
            Some(rs) => match rs.fetch_block(pData, numOfRows) {
                Ok(()) => {
                    trace!(rs=?rs, "taos_fetch_raw_block done");
                    Code::SUCCESS.into()
                }
                Err(err) => {
                    set_err_and_get_code(TaosError::new(err.errno(), &err.errstr()));
                    let code = err.errno();
                    maybe_err.with_err(Some(err.into()));
                    code.into()
                }
            },
            None => handle_error("data is null", numOfRows),
        },
        None => handle_error("res is null", numOfRows),
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_get_column_data_offset(
    res: *mut TAOS_RES,
    columnIndex: c_int,
) -> *mut c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_lengths(res: *mut TAOS_RES) -> *mut c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW {
    todo!()
}

#[no_mangle]
pub unsafe extern "C" fn taos_get_server_info(taos: *mut TAOS) -> *const c_char {
    trace!(taos=?taos, "taos_get_server_info");

    static SERVER_INFO: OnceLock<CString> = OnceLock::new();

    if taos.is_null() {
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
        return ptr::null();
    }

    let server_info = SERVER_INFO.get_or_init(|| {
        if let Some(taos) = (taos as *mut Taos).as_mut() {
            CString::new(taos.version()).unwrap()
        } else {
            CString::new("").unwrap()
        }
    });

    trace!(server_info=?server_info, "taos_get_server_info done");

    server_info.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn taos_get_client_info() -> *const c_char {
    trace!("taos_get_client_info");

    static CLIENT_INFO: OnceLock<CString> = OnceLock::new();

    let metadata = match MetadataCommand::new().no_deps().exec().ok() {
        Some(md) => md,
        None => {
            set_err_and_get_code(TaosError::new(Code::FAILED, "MetadataCommand error"));
            return ptr::null();
        }
    };

    let package = match metadata
        .packages
        .into_iter()
        .find(|pkg| pkg.name == "taos-ws-sys")
    {
        Some(pkg) => pkg,
        None => {
            set_err_and_get_code(TaosError::new(Code::FAILED, "find package error"));
            return ptr::null();
        }
    };

    let version = package.version.to_string();
    let client_info = CString::new(version).unwrap();

    trace!(client_info=?client_info, "taos_get_client_info done");

    CLIENT_INFO.set(client_info).unwrap();
    CLIENT_INFO.get().unwrap().as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn taos_get_current_db(
    taos: *mut TAOS,
    database: *mut c_char,
    len: c_int,
    required: *mut c_int,
) -> c_int {
    trace!(taos=?taos, database=?database, len, required=?required, "taos_get_current_db");

    if taos.is_null() {
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
    }

    let res = taos_query(taos, c"SELECT DATABASE()".as_ptr());
    if res.is_null() {
        return set_err_and_get_code(TaosError::new(Code::FAILED, "query failed"));
    }

    let mut rows = 0;
    let mut data = ptr::null_mut();
    let mut code = taos_fetch_raw_block(res, &mut rows, &mut data);
    if code != 0 {
        return code;
    }

    let mut len_actual = 0;
    let mut db = ptr::null();
    if let Some(rs) = (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        (_, len_actual, db) = rs.get_raw_value(0, 0);
    }

    if db.is_null() {
        return set_err_and_get_code(TaosError::new(Code::FAILED, "get value failed"));
    }

    if len_actual < len as u32 {
        ptr::copy_nonoverlapping(db as _, database, len_actual as _);
    } else {
        ptr::copy_nonoverlapping(db as _, database, len as _);
        *required = len_actual as _;
        code = -1;
    }

    trace!(
        "taos_get_current_db done database={:?}",
        CStr::from_ptr(database)
    );

    code
}

#[no_mangle]
pub extern "C" fn taos_query_a(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_query_a_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
    reqid: i64,
) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_rows_a(res: *mut TAOS_RES, fp: __taos_async_fn_t, param: *mut c_void) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_raw_block_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_get_raw_block(res: *mut TAOS_RES) -> *const c_void {
    todo!()
}

#[derive(Debug)]
pub struct QueryResultSet {
    rs: taos_ws::ResultSet,
    block: Option<Block>,
    fields: Vec<TAOS_FIELD>,
    row: Row,
}

impl QueryResultSet {
    pub fn new(rs: taos_ws::ResultSet) -> Self {
        let num_of_fields = rs.num_of_fields();
        Self {
            rs,
            block: None,
            fields: Vec::new(),
            row: Row {
                data: vec![ptr::null(); num_of_fields],
                current_row: 0,
            },
        }
    }
}

impl ResultSetOperations for QueryResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_offset(&self) -> Offset {
        todo!()
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        0
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        0
    }

    fn precision(&self) -> Precision {
        self.rs.precision()
    }

    fn affected_rows(&self) -> i32 {
        self.rs.affected_rows() as _
    }

    fn affected_rows64(&self) -> i64 {
        self.rs.affected_rows64() as _
    }

    fn num_of_fields(&self) -> i32 {
        self.rs.num_of_fields() as _
    }

    fn get_fields(&mut self) -> *mut TAOS_FIELD {
        if self.fields.len() != self.rs.num_of_fields() {
            self.fields.clear();
            self.fields
                .extend(self.rs.fields().iter().map(TAOS_FIELD::from));
        }
        self.fields.as_mut_ptr()
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *mut c_void, rows: *mut i32) -> Result<(), Error> {
        self.block = self.rs.fetch_raw_block()?;
        if let Some(block) = self.block.as_ref() {
            *ptr = block.as_raw_bytes().as_ptr() as _;
            *rows = block.nrows() as _;
        } else {
            *rows = 0;
        }
        Ok(())
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        if self.block.is_none() || self.row.current_row >= self.block.as_ref().unwrap().nrows() {
            self.block = self.rs.fetch_raw_block()?;
            self.row.current_row = 0;
        }

        if let Some(block) = self.block.as_ref() {
            if block.nrows() == 0 {
                return Ok(ptr::null_mut());
            }

            for col in 0..block.ncols() {
                let res = block.get_raw_value_unchecked(self.row.current_row, col);
                self.row.data[col] = res.2;
            }
            self.row.current_row += 1;
            Ok(self.row.data.as_ptr() as _)
        } else {
            Ok(ptr::null_mut())
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        if let Some(block) = &self.block {
            if row < block.nrows() && col < block.ncols() {
                return block.get_raw_value_unchecked(row, col);
            }
        }
        (Ty::Null, 0, ptr::null())
    }

    fn take_timing(&mut self) -> Duration {
        self.rs.take_timing()
    }

    fn stop_query(&mut self) {
        taos_query::block_in_place_or_global(self.rs.stop());
    }
}

unsafe fn query(taos: *mut TAOS, sql: *const c_char, req_id: u64) -> TaosResult<ResultSet> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;
    let sql = CStr::from_ptr(sql).to_str()?;
    let rs = taos.query_with_req_id(sql, req_id)?;
    Ok(ResultSet::Query(QueryResultSet::new(rs)))
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use taos_query::common::Precision;

    use super::*;
    use crate::native::error::{taos_errno, taos_errstr};
    use crate::native::{taos_connect, test_connect, test_exec, test_exec_many};

    #[test]
    fn test_taos_query() {
        let taos = test_connect();
        test_exec_many(
            taos,
            &[
                "drop database if exists test_1737102397",
                "create database test_1737102397",
                "use test_1737102397",
                "create table t0 (ts timestamp, c1 int)",
                "insert into t0 values (now, 1)",
                "select * from t0",
                "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                "create table d0 using s0 tags(1)",
                "insert into d0 values (now, 1)",
                "select * from d0",
                "insert into d1 using s0 tags(2) values(now, 1)",
                "select * from d1",
                "select * from s0",
                "drop database test_1737102397",
            ],
        );
    }

    #[test]
    fn test_taos_fetch_row() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102398",
                    "create database test_1737102398",
                    "use test_1737102398",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102398");
        }
    }

    #[test]
    fn test_taos_result_precision() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102399",
                    "create database test_1737102399",
                    "use test_1737102399",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let precision: Precision = taos_result_precision(res).into();
            assert_eq!(precision, Precision::Millisecond);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102399");
        }
    }

    #[test]
    fn test_taos_field_count() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102400",
                    "create database test_1737102400",
                    "use test_1737102400",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let count = taos_field_count(res);
            assert_eq!(count, 2);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102400");
        }
    }

    #[test]
    fn test_taos_num_fields() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102401",
                    "create database test_1737102401",
                    "use test_1737102401",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let num = taos_num_fields(res);
            assert_eq!(num, 2);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102401");
        }
    }

    #[test]
    fn test_taos_affected_rows() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102402",
                    "create database test_1737102402",
                    "use test_1737102402",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 0);

            taos_free_result(res);

            let res = taos_query(taos, c"insert into t0 values (now, 2)".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 1);

            taos_free_result(res);

            test_exec(taos, "drop database test_1737102402");
        }
    }

    #[test]
    fn test_taos_affected_rows64() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102403",
                    "create database test_1737102403",
                    "use test_1737102403",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows64(res);
            assert_eq!(rows, 0);

            taos_free_result(res);

            let res = taos_query(taos, c"insert into t0 values (now, 2)".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 1);

            taos_free_result(res);

            test_exec(taos, "drop database test_1737102403");
        }
    }

    #[test]
    fn test_taos_fetch_fields() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102404",
                    "create database test_1737102404",
                    "use test_1737102404",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102404");
        }
    }

    #[test]
    fn test_taos_select_db() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102405",
                    "create database test_1737102405",
                ],
            );

            let res = taos_select_db(taos, c"test_1737102405".as_ptr());
            assert_eq!(res, 0);

            test_exec_many(
                taos,
                &[
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "drop database test_1737102405",
                ],
            );
        }
    }

    #[test]
    fn test_taos_print_row() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102406",
                    "create database test_1737102406",
                    "use test_1737102406",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 2);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102406");
        }
    }

    #[test]
    fn test_taos_stop_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102407",
                    "create database test_1737102407",
                    "use test_1737102407",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            taos_stop_query(res);
            let errno = taos_errno(ptr::null_mut());
            assert_eq!(errno, 0);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102407");
        }
    }

    #[test]
    fn test_taos_is_null() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102408",
                    "create database test_1737102408",
                    "use test_1737102408",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let is_null = taos_is_null(res, 0, 0);
            assert!(is_null);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let is_null = taos_is_null(res, 0, 0);
            assert!(!is_null);

            let is_null = taos_is_null(ptr::null_mut(), 0, 0);
            assert!(is_null);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102408");
        }
    }

    #[test]
    fn test_taos_is_update_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102409",
                    "create database test_1737102409",
                    "use test_1737102409",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"insert into t0 values (now, 2)".as_ptr());
            assert!(!res.is_null());

            let is_update = taos_is_update_query(res);
            assert!(is_update);

            taos_free_result(res);

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let is_update = taos_is_update_query(res);
            assert!(!is_update);

            taos_free_result(res);

            let is_update = taos_is_update_query(ptr::null_mut());
            assert!(is_update);

            test_exec(taos, "drop database test_1737102409");
        }
    }

    #[test]
    fn test_taos_fetch_raw_block() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102410",
                    "create database test_1737102410",
                    "use test_1737102410",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 1);
            assert!(!data.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102410");
        }
    }

    #[test]
    fn test_taos_get_server_info() {
        unsafe {
            let taos = test_connect();
            let server_info = taos_get_server_info(taos);
            assert!(!server_info.is_null());
            let server_info = CStr::from_ptr(server_info).to_str().unwrap();
            println!("server_info: {server_info}");
        }
    }

    #[test]
    fn test_taos_get_client_info() {
        unsafe {
            let client_info = taos_get_client_info();
            assert!(!client_info.is_null());
            let client_info = CStr::from_ptr(client_info).to_str().unwrap();
            println!("client_info: {client_info}");
        }
    }

    #[test]
    fn test_taos_get_current_db() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102411",
                    "create database test_1737102411",
                ],
            );

            let taos = taos_connect(
                c"localhost".as_ptr(),
                c"root".as_ptr(),
                c"taosdata".as_ptr(),
                c"test_1737102411".as_ptr(),
                6041,
            );
            assert!(!taos.is_null());

            let mut db = vec![0 as c_char; 1024];
            let mut required = 0;
            let code = taos_get_current_db(taos, db.as_mut_ptr(), db.len() as _, &mut required);
            assert_eq!(code, 0);
            println!("db: {:?}", CStr::from_ptr(db.as_ptr()));
        }
    }

    #[test]
    fn test_taos_get_current_db_without_db() {
        unsafe {
            let taos = test_connect();
            let mut db = vec![0 as c_char; 1024];
            let mut required = 0;
            let code = taos_get_current_db(taos, db.as_mut_ptr(), db.len() as _, &mut required);
            assert!(code != 0);
            let errstr = taos_errstr(ptr::null_mut());
            println!("errstr: {:?}", CStr::from_ptr(errstr));
        }
    }
}
