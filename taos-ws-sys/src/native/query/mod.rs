use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;

use taos_error::Code;
use taos_query::common::Field;
use taos_query::util::generate_req_id;
use tracing::trace;

use crate::native::error::{set_err_and_get_code, TaosError, TaosMaybeError};
use crate::native::{ResultSet, ResultSetOperations, TAOS, TAOS_RES};

mod inner;

pub use inner::QueryResultSet;

#[allow(non_camel_case_types)]
pub type TAOS_ROW = *mut *mut c_void;

#[allow(non_camel_case_types)]
pub type __taos_async_fn_t = extern "C" fn(param: *mut c_void, res: *mut TAOS_RES, code: c_int);

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
    let res: TaosMaybeError<ResultSet> = inner::query(taos, sql, reqId as u64).into();
    trace!(res=?res, "taos_query_with_reqid done");
    Box::into_raw(Box::new(res)) as _
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_row(res: *mut TAOS_RES) -> TAOS_ROW {
    trace!(res=?res, "taos_fetch_row");

    fn handle_error(code: Code, msg: &str) -> TAOS_ROW {
        set_err_and_get_code(TaosError::new(code, msg));
        ptr::null_mut()
    }

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
pub extern "C" fn taos_num_fields(res: *mut TAOS_RES) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_affected_rows(res: *mut TAOS_RES) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_affected_rows64(res: *mut TAOS_RES) -> i64 {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_fetch_fields(res: *mut TAOS_RES) -> *mut TAOS_FIELD {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_select_db(taos: *mut TAOS, db: *const c_char) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_print_row(
    str: *mut c_char,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_print_row_with_size(
    str: *mut c_char,
    size: u32,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stop_query(res: *mut TAOS_RES) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_is_null(res: *mut TAOS_RES, row: i32, col: i32) -> bool {
    todo!()
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
pub extern "C" fn taos_is_update_query(res: *mut TAOS_RES) -> bool {
    todo!()
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
pub extern "C" fn taos_fetch_raw_block(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    pData: *mut *mut c_void,
) -> c_int {
    todo!()
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
pub extern "C" fn taos_get_server_info(taos: *mut TAOS) -> *const c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_get_client_info() -> *const c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_get_current_db(
    taos: *mut TAOS,
    database: *mut c_char,
    len: c_int,
    required: *mut c_int,
) -> c_int {
    todo!()
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

#[cfg(test)]
mod tests {
    use std::ptr;

    use taos_query::common::Precision;

    use super::*;
    use crate::native::taos_connect;

    fn connect() -> *mut TAOS {
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

    #[test]
    fn test_taos_query() {
        unsafe {
            let taos = connect();
            let res = taos_query(taos, c"select * from test.t0".as_ptr());
            assert!(!res.is_null());
        }
    }

    #[test]
    fn test_taos_fetch_row() {
        unsafe {
            let taos = connect();
            let res = taos_query(taos, c"select * from test.t0".as_ptr());
            assert!(!res.is_null());
            let row = taos_fetch_row(res);
            assert!(!row.is_null());
        }
    }

    #[test]
    fn test_taos_result_precision() {
        unsafe {
            let taos = connect();
            let res = taos_query(taos, c"select * from test.t0".as_ptr());
            assert!(!res.is_null());
            let _: Precision = taos_result_precision(res).into();
        }
    }

    #[test]
    fn test_taos_free_result() {
        unsafe {
            let taos = connect();
            let res = taos_query(taos, c"select * from test.t0".as_ptr());
            assert!(!res.is_null());
            taos_free_result(res);
        }
    }

    #[test]
    fn test_taos_field_count() {
        unsafe {
            let taos = connect();
            let res = taos_query(taos, c"select * from test.t0".as_ptr());
            assert!(!res.is_null());
            let count = taos_field_count(res);
            assert_eq!(count, 2);
        }
    }
}
