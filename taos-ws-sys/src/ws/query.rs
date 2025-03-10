use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::OnceLock;
use std::time::Duration;
use std::{ptr, slice};

use bytes::Bytes;
use cargo_metadata::MetadataCommand;
use taos_error::Code;
use taos_query::common::{Precision, Ty};
use taos_query::util::{generate_req_id, hex, InlineBytes, InlineNChar, InlineStr};
use taos_query::{global_tokio_runtime, Fetchable, Queryable, RawBlock as Block};
use taos_ws::query::Error;
use taos_ws::{Offset, Taos};
use tracing::{error, instrument, trace, Instrument};

use crate::ws::error::{
    clear_err_and_ret_succ, format_errno, set_err_and_get_code, TaosError, TaosMaybeError,
};
use crate::ws::{
    ResultSet, ResultSetOperations, Row, SafePtr, TaosResult, __taos_async_fn_t, TAOS, TAOS_RES,
    TAOS_ROW,
};

pub const TSDB_DATA_TYPE_NULL: usize = 0;
pub const TSDB_DATA_TYPE_BOOL: usize = 1;
pub const TSDB_DATA_TYPE_TINYINT: usize = 2;
pub const TSDB_DATA_TYPE_SMALLINT: usize = 3;
pub const TSDB_DATA_TYPE_INT: usize = 4;
pub const TSDB_DATA_TYPE_BIGINT: usize = 5;
pub const TSDB_DATA_TYPE_FLOAT: usize = 6;
pub const TSDB_DATA_TYPE_DOUBLE: usize = 7;
pub const TSDB_DATA_TYPE_VARCHAR: usize = 8;
pub const TSDB_DATA_TYPE_TIMESTAMP: usize = 9;
pub const TSDB_DATA_TYPE_NCHAR: usize = 10;
pub const TSDB_DATA_TYPE_UTINYINT: usize = 11;
pub const TSDB_DATA_TYPE_USMALLINT: usize = 12;
pub const TSDB_DATA_TYPE_UINT: usize = 13;
pub const TSDB_DATA_TYPE_UBIGINT: usize = 14;
pub const TSDB_DATA_TYPE_JSON: usize = 15;
pub const TSDB_DATA_TYPE_VARBINARY: usize = 16;
pub const TSDB_DATA_TYPE_DECIMAL: usize = 17;
pub const TSDB_DATA_TYPE_BLOB: usize = 18;
pub const TSDB_DATA_TYPE_MEDIUMBLOB: usize = 19;
pub const TSDB_DATA_TYPE_BINARY: usize = TSDB_DATA_TYPE_VARCHAR;
pub const TSDB_DATA_TYPE_GEOMETRY: usize = 20;
pub const TSDB_DATA_TYPE_MAX: usize = 21;

#[allow(non_camel_case_types)]
pub type __taos_notify_fn_t = extern "C" fn(param: *mut c_void, ext: *mut c_void, r#type: c_int);

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub bytes: i32,
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct TAOS_VGROUP_HASH_INFO {
    pub vgId: i32,
    pub hashBegin: u32,
    pub hashEnd: u32,
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct TAOS_DB_ROUTE_INFO {
    pub routeVersion: i32,
    pub hashPrefix: i16,
    pub hashSuffix: i16,
    pub hashMethod: i8,
    pub vgNum: i32,
    pub vgHash: *mut TAOS_VGROUP_HASH_INFO,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub enum SET_CONF_RET_CODE {
    SET_CONF_RET_SUCC = 0,
    SET_CONF_RET_ERR_PART = -1,
    SET_CONF_RET_ERR_INNER = -2,
    SET_CONF_RET_ERR_JSON_INVALID = -3,
    SET_CONF_RET_ERR_JSON_PARSE = -4,
    SET_CONF_RET_ERR_ONLY_ONCE = -5,
    SET_CONF_RET_ERR_TOO_LONG = -6,
}

pub const RET_MSG_LENGTH: usize = 1024;

#[repr(C)]
#[derive(Debug)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct setConfRet {
    pub retCode: SET_CONF_RET_CODE,
    pub retMsg: [c_char; RET_MSG_LENGTH],
}

#[allow(non_camel_case_types)]
pub type __taos_async_whitelist_fn_t = extern "C" fn(
    param: *mut c_void,
    code: i32,
    taos: *mut TAOS,
    numOfWhiteLists: i32,
    pWhiteLists: *mut u64,
);

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub enum TSDB_SERVER_STATUS {
    TSDB_SRV_STATUS_UNAVAILABLE = 0,
    TSDB_SRV_STATUS_NETWORK_OK = 1,
    TSDB_SRV_STATUS_SERVICE_OK = 2,
    TSDB_SRV_STATUS_SERVICE_DEGRADED = 3,
    TSDB_SRV_STATUS_EXTING = 4,
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES {
    taos_query_with_reqid(taos, sql, generate_req_id() as _)
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
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

unsafe fn query(taos: *mut TAOS, sql: *const c_char, req_id: u64) -> TaosResult<ResultSet> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;
    let sql = CStr::from_ptr(sql).to_str()?;
    let rs = taos.query_with_req_id(sql, req_id)?;
    Ok(ResultSet::Query(QueryResultSet::new(rs)))
}

#[no_mangle]
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_free_result(res: *mut TAOS_RES) {
    trace!(res=?res, "taos_free_result");
    if !res.is_null() {
        let _ = Box::from_raw(res as *mut TaosMaybeError<ResultSet>);
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_num_fields(res: *mut TAOS_RES) -> c_int {
    taos_field_count(res)
}

#[no_mangle]
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_print_row(
    str: *mut c_char,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    taos_print_row_with_size(str, i32::MAX as _, row, fields, num_fields)
}

#[no_mangle]
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_is_null_by_column(
    res: *mut TAOS_RES,
    columnIndex: c_int,
    result: *mut bool,
    rows: *mut c_int,
) -> c_int {
    trace!("taos_is_null_by_column start, res: {res:?}, column_index: {columnIndex}, result: {result:?}, rows: {rows:?}");

    if res.is_null() || result.is_null() || rows.is_null() || *rows <= 0 || columnIndex < 0 {
        return Code::INVALID_PARA.into();
    }

    let maybe_err = (res as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
    if maybe_err.deref_mut().is_none() {
        return Code::INVALID_PARA.into();
    }

    match maybe_err.deref_mut().unwrap() {
        ResultSet::Query(rs) => match &rs.block {
            Some(block) => {
                let col = columnIndex as usize;
                if col >= block.ncols() || block.ncols() == 0 {
                    error!("taos_is_null_by_column failed, column index is invalid");
                    return Code::INVALID_PARA.into();
                }

                let is_nulls = block.is_null_by_col_unchecked(*rows as _, col);
                trace!("taos_is_null_by_column succ, is_nulls: {is_nulls:?}");
                *rows = is_nulls.len() as _;
                let res = slice::from_raw_parts_mut(result, is_nulls.len());
                for (i, is_null) in is_nulls.iter().enumerate() {
                    res[i] = *is_null;
                }
                maybe_err.clear_err();
                clear_err_and_ret_succ()
            }
            None => {
                maybe_err.with_err(Some(TaosError::new(Code::FAILED, "block is none")));
                format_errno(Code::FAILED.into())
            }
        },
        _ => {
            maybe_err.with_err(Some(TaosError::new(Code::FAILED, "rs is invalid")));
            format_errno(Code::FAILED.into())
        }
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_fetch_block(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int {
    trace!("taos_fetch_block start, res: {res:?}, rows: {rows:?}");
    let mut num_of_rows = 0;
    taos_fetch_block_s(res, &mut num_of_rows, rows);
    trace!("taos_fetch_block succ, num_of_rows: {num_of_rows}");
    num_of_rows
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_fetch_block_s(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    rows: *mut TAOS_ROW,
) -> c_int {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_raw_block_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    trace!("taos_fetch_raw_block_a start, res: {res:?}, fp: {fp:?}, param: {param:?}");

    let cb = fp as *const ();
    if res.is_null() || cb.is_null() {
        error!("taos_fetch_raw_block_a failed, res or fp is null");
        if !cb.is_null() {
            let code = format_errno(Code::INVALID_PARA.into());
            fp(param, res, code);
        }
        return;
    }

    let res = SafePtr(res);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let res = res;
            let param = param;

            let maybe_err = (res.0 as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
            if maybe_err.deref_mut().is_none() {
                error!("taos_fetch_raw_block_a callback failed, res is invalid");
                maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "res is invalid")));
                let code = format_errno(Code::INVALID_PARA.into());
                fp(param.0, res.0, code);
                return;
            }

            let rs = maybe_err.deref_mut().unwrap();
            if let ResultSet::Query(qrs) = rs {
                match qrs.rs.fetch_raw_block() {
                    Ok(block) => {
                        trace!("taos_fetch_raw_block_a callback succ");
                        qrs.block = block;
                        fp(param.0, res.0, 0);
                    }
                    Err(err) => {
                        error!("taos_fetch_raw_block_a callback failed, err: {err:?}");
                        let code = format_errno(err.code().into());
                        fp(param.0, res.0, code);
                    }
                }
            } else {
                error!("taos_fetch_raw_block_a callback failed, rs is invalid");
                maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "rs is invalid")));
                let code = format_errno(Code::INVALID_PARA.into());
                fp(param.0, res.0, code);
            };
        }
        .in_current_span(),
    );

    trace!("taos_fetch_raw_block_a succ");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW {
    trace!("taos_result_block start, res: {res:?}");

    if res.is_null() {
        error!("taos_result_block failed, res is null");
        return ptr::null_mut();
    }

    let maybe_err = (res as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
    if maybe_err.deref_mut().is_none() {
        error!("taos_result_block failed, res is invalid");
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "res is invalid")));
        return ptr::null_mut();
    }

    let rs = maybe_err.deref_mut().unwrap();
    if let ResultSet::Query(qrs) = rs {
        match qrs.fetch_row() {
            Ok(_) => {
                trace!("taos_result_block succ, ptr: {:?}", qrs.row_data_ptr_ptr);
                return qrs.row_data_ptr_ptr;
            }
            Err(err) => {
                error!("taos_result_block failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "rs is invalid")));
            }
        }
    } else {
        error!("taos_result_block failed, rs is invalid");
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "rs is invalid")));
    };

    return ptr::null_mut();
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_column_data_offset(
    res: *mut TAOS_RES,
    columnIndex: c_int,
) -> *mut c_int {
    trace!("taos_get_column_data_offset start, res: {res:?}, column_index: {columnIndex}");

    if res.is_null() || columnIndex < 0 {
        error!("taos_get_column_data_offset failed, res or column index is invalid");
        return ptr::null_mut();
    }

    let maybe_err = (res as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
    let rs = match maybe_err.deref_mut() {
        Some(rs) => rs,
        None => {
            error!("taos_get_column_data_offset failed, res is invalid");
            return ptr::null_mut();
        }
    };

    let col = columnIndex as usize;
    if let ResultSet::Query(rs) = rs {
        if let Some(block) = rs.block.as_ref() {
            if col < block.ncols() && block.ncols() > 0 {
                let offsets = block.get_col_data_offset_unchecked(col);
                trace!("taos_get_column_data_offset succ, offsets: {offsets:?}");
                if offsets.is_empty() {
                    return ptr::null_mut();
                }
                rs.offsets = Some(offsets);
                return rs.offsets.as_ref().unwrap().as_ptr() as *mut _;
            }
            error!("taos_get_column_data_offset failed, column index is invalid");
        } else {
            error!("taos_get_column_data_offset failed, block is none");
        }
    } else {
        error!("taos_get_column_data_offset failed, rs is invalid");
    }

    ptr::null_mut()
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> c_int {
    trace!("taos_validate_sql start, taos: {taos:?}, sql: {sql:?}");

    if taos.is_null() {
        error!("taos_validate_sql failed, taos is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
    }

    if sql.is_null() {
        error!("taos_validate_sql failed, sql is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "sql is null"));
    }

    let sql = match CStr::from_ptr(sql).to_str() {
        Ok(sql) => sql,
        Err(_) => {
            error!("taos_validate_sql failed, sql is invalid utf-8");
            return set_err_and_get_code(TaosError::new(
                Code::INVALID_PARA,
                "sql is invalid utf-8",
            ));
        }
    };

    trace!("taos_validate_sql, sql: {sql}");

    let taos = (taos as *mut Taos).as_mut().unwrap();
    if let Err(err) = taos_query::block_in_place_or_global(taos.client().validate_sql(sql)) {
        error!("taos_validate_sql failed, err: {err:?}");
        return set_err_and_get_code(err.into());
    }

    trace!("taos_validate_sql succ");
    Code::SUCCESS.into()
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_lengths(res: *mut TAOS_RES) -> *mut c_int {
    trace!("taos_fetch_lengths start, res: {res:?}");

    if res.is_null() {
        error!("taos_fetch_lengths failed, res is null");
        return ptr::null_mut();
    }

    let maybe_err = (res as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
    let rs = match maybe_err.deref_mut() {
        Some(rs) => rs,
        None => {
            error!("taos_fetch_lengths failed, res is invalid");
            return ptr::null_mut();
        }
    };

    if let ResultSet::Query(rs) = rs {
        if let Some(block) = rs.block.as_ref() {
            let lengths = if rs.has_called_fetch_row {
                let mut lengths = Vec::with_capacity(block.ncols());
                for col in 0..block.ncols() {
                    tracing::trace!(
                        "taos_fetch_lengths, row: {:?}, col: {:?}",
                        rs.row.current_row - 1,
                        col
                    );
                    let (_, len, _) = block.get_raw_value_unchecked(rs.row.current_row - 1, col);
                    lengths.push(len as i32);
                }
                lengths
            } else {
                block
                    .schemas()
                    .iter()
                    .map(|schema| schema.len() as i32)
                    .collect::<Vec<_>>()
            };

            trace!("taos_fetch_lengths succ, lengths: {lengths:?}");

            if lengths.is_empty() {
                return ptr::null_mut();
            }

            rs.lengths = Some(lengths);
            return rs.lengths.as_ref().unwrap().as_ptr() as *mut _;
        }
        error!("taos_fetch_lengths failed, block is none");
    } else {
        error!("taos_fetch_lengths failed, rs is invalid");
    }

    ptr::null_mut()
}

#[no_mangle]
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
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
#[instrument(level = "trace", ret)]
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
        taos_free_result(res);
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
        taos_free_result(res);
        return set_err_and_get_code(TaosError::new(Code::FAILED, "get value failed"));
    }

    if len_actual < len as u32 {
        ptr::copy_nonoverlapping(db as _, database, len_actual as _);
    } else {
        ptr::copy_nonoverlapping(db as _, database, len as _);
        *required = len_actual as _;
        code = -1;
    }

    taos_free_result(res);

    trace!(
        "taos_get_current_db done database={:?}",
        CStr::from_ptr(database)
    );

    code
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_data_type(r#type: c_int) -> *const c_char {
    match Ty::from_u8_option(r#type as _) {
        Some(ty) => ty.tsdb_name(),
        None => ptr::null(),
    }
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_query_a(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    taos_query_a_with_reqid(taos, sql, fp, param, generate_req_id() as _);
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_query_a_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
    reqid: i64,
) {
    trace!("taos_query_a_with_reqid start, taos: {taos:?}, sql: {sql:?}, reqid: {reqid}, fp: {fp:?}, param: {param:?}");

    let taos = SafePtr(taos);
    let sql = SafePtr(sql);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let taos = taos;
            let sql = sql;
            let param = param;

            let cb = fp as *const ();
            if taos.0.is_null() || sql.0.is_null() || cb.is_null() {
                if !cb.is_null() {
                    let code = format_errno(Code::INVALID_PARA.into());
                    fp(param.0, ptr::null_mut(), code);
                }
                return;
            }

            let sql = match CStr::from_ptr(sql.0).to_str() {
                Ok(sql) => sql,
                Err(_) => {
                    error!("taos_query_a_with_reqid failed, err: sql is invalid utf-8");
                    let code = format_errno(Code::INVALID_PARA.into());
                    fp(param.0, ptr::null_mut(), code);
                    return;
                }
            };

            let taos = (taos.0 as *mut Taos).as_mut().unwrap();

            match taos_query::AsyncQueryable::query_with_req_id(taos, sql, reqid as _).await {
                Ok(rs) => {
                    trace!("taos_query_a_with_reqid callback, result_set: {rs:?}");
                    let rs: TaosMaybeError<ResultSet> =
                        ResultSet::Query(QueryResultSet::new(rs)).into();
                    let res = Box::into_raw(Box::new(rs));
                    fp(param.0, res as _, 0);
                }
                Err(err) => {
                    // TODO: use TaosMaybeError to handle error
                    error!("taos_query_a_with_reqid failed, err: {err:?}");
                    let code = format_errno(err.code().into());
                    fp(param.0, ptr::null_mut(), code);
                }
            };
        }
        .in_current_span(),
    );

    trace!("taos_query_a_with_reqid succ");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_fetch_rows_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    trace!("taos_fetch_rows_a start, res: {res:?}, fp: {fp:?}, param: {param:?}");

    let res = SafePtr(res);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let res = res;
            let param = param;

            let cb = fp as *const ();
            if res.0.is_null() || cb.is_null() {
                if !cb.is_null() {
                    let code = format_errno(Code::INVALID_PARA.into());
                    fp(param.0, ptr::null_mut(), code);
                }
                return;
            }

            let maybe_err = (res.0 as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
            if maybe_err.deref_mut().is_none() {
                let code = format_errno(Code::INVALID_PARA.into());
                fp(param.0, ptr::null_mut(), code);
                return;
            }

            let rs = maybe_err.deref_mut().unwrap();
            let rows = if let ResultSet::Query(rs) = rs {
                match rs.fetch_rows() {
                    Ok(rows) => rows,
                    Err(err) => {
                        let code = format_errno(err.errno().into());
                        fp(param.0, ptr::null_mut(), code);
                        return;
                    }
                }
            } else {
                let code = format_errno(Code::INVALID_PARA.into());
                fp(param.0, ptr::null_mut(), code);
                return;
            };

            fp(param.0, res.0, rows as _);
        }
        .in_current_span(),
    );

    trace!("taos_fetch_rows_a succ");
}

#[no_mangle]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_get_raw_block(res: *mut TAOS_RES) -> *const c_void {
    trace!("taos_get_raw_block start, res: {res:?}");
    let mut num_of_rows = 0;
    let mut data = ptr::null_mut();
    taos_fetch_raw_block(res, &mut num_of_rows, &mut data);
    trace!("taos_get_raw_block succ, data: {data:?}");
    data
}

#[derive(Debug)]
pub struct QueryResultSet {
    rs: taos_ws::ResultSet,
    block: Option<Block>,
    fields: Vec<TAOS_FIELD>,
    row: Row,
    #[allow(dead_code)]
    row_data_ptr: TAOS_ROW,
    row_data_ptr_ptr: *mut TAOS_ROW,
    offsets: Option<Vec<i32>>,
    lengths: Option<Vec<i32>>,
    has_called_fetch_row: bool,
}

impl Drop for QueryResultSet {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.row_data_ptr_ptr);
        }
    }
}

impl QueryResultSet {
    pub fn new(rs: taos_ws::ResultSet) -> Self {
        let num_of_fields = rs.num_of_fields();
        let mut row = Row {
            data: vec![ptr::null(); num_of_fields],
            current_row: 0,
        };
        let row_data_ptr = row.data.as_mut_ptr() as _;
        let row_data_ptr_ptr = Box::into_raw(Box::new(row_data_ptr));

        Self {
            rs,
            block: None,
            fields: Vec::new(),
            row,
            row_data_ptr,
            row_data_ptr_ptr,
            offsets: None,
            lengths: None,
            has_called_fetch_row: false,
        }
    }

    pub fn fetch_rows(&mut self) -> Result<usize, Error> {
        if self.block.is_none() || self.row.current_row >= self.block.as_ref().unwrap().nrows() {
            self.block = self.rs.fetch_raw_block()?;
            self.row.current_row = 0;
        }

        if let Some(block) = self.block.as_ref() {
            return Ok(block.nrows());
        }

        Ok(0)
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
        if !self.has_called_fetch_row {
            self.has_called_fetch_row = true;
        }

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

#[cfg(test)]
mod tests {
    use std::ptr;
    use std::thread::sleep;

    use taos_query::common::Precision;

    use super::*;
    use crate::ws::error::{taos_errno, taos_errstr};
    use crate::ws::{taos_close, taos_connect, test_connect, test_exec, test_exec_many};

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

        unsafe { taos_close(taos) };
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
            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);
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

            taos_close(taos);
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

            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);
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
            taos_close(taos);

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

            taos_close(taos);
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
            taos_close(taos);
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

    #[test]
    fn test_taos_is_null_by_column() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740644681",
                    "create database test_1740644681",
                    "use test_1740644681",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now+1s, 1)",
                    "insert into t0 values (now+2s, null)",
                    "insert into t0 values (now+3s, 2)",
                    "insert into t0 values (now+4s, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 4);
            assert!(!data.is_null());

            let mut rows = 100;
            let mut result = vec![false; rows as _];
            let code = taos_is_null_by_column(res, 1, result.as_mut_ptr(), &mut rows);
            assert_eq!(code, 0);
            assert_eq!(rows, 4);

            assert!(!result[0]);
            assert!(result[1]);
            assert!(!result[2]);
            assert!(result[3]);

            taos_free_result(res);

            test_exec(taos, "drop database test_1740644681");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_query_a() {
        unsafe {
            extern "C" fn cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert_eq!(code, 0);
                    assert_eq!(CStr::from_ptr(param as _), c"hello, world");
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
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740664844",
                    "create database test_1740664844",
                    "use test_1740664844",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let sql = c"select * from t0";
            let param = c"hello, world";
            taos_query_a(taos, sql.as_ptr(), cb, param.as_ptr() as _);

            test_exec(taos, "drop database test_1740664844");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_rows_a() {
        unsafe {
            extern "C" fn fetch_rows_cb(taos: *mut c_void, res: *mut TAOS_RES, num_of_row: c_int) {
                unsafe {
                    println!("fetch_rows_cb, num_of_row: {}", num_of_row);
                    let num_fields = taos_num_fields(res);
                    let fields = taos_fetch_fields(res);
                    if num_of_row > 0 {
                        assert_eq!(num_of_row, 4);
                        for i in 0..num_of_row {
                            let row = taos_fetch_row(res);
                            let mut str = vec![0 as c_char; 1024];
                            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);
                        }
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        println!("fetch_rows_cb, no more data");
                        taos_free_result(res);
                    }
                }
            }

            extern "C" fn query_cb(taos: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    println!("query_cb");
                    if code == 0 && !res.is_null() {
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        taos_free_result(res);
                    }
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740731669",
                    "create database test_1740731669",
                    "use test_1740731669",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "insert into t0 values (now+1s, 2)",
                    "insert into t0 values (now+2s, 3)",
                    "insert into t0 values (now+3s, 4)",
                ],
            );

            let sql = c"select * from t0";
            taos_query_a(taos, sql.as_ptr(), query_cb, taos);

            sleep(Duration::from_secs(1));

            test_exec(taos, "drop database test_1740731669");
            taos_close(taos);
        }
    }

    #[test]
    #[ignore]
    fn test_taos_fetch_rows_a_() {
        unsafe {
            extern "C" fn fetch_rows_cb(taos: *mut c_void, res: *mut TAOS_RES, num_of_row: c_int) {
                unsafe {
                    println!("fetch_rows_cb, num_of_row: {}", num_of_row);
                    let num_fields = taos_num_fields(res);
                    let fields = taos_fetch_fields(res);
                    if num_of_row > 0 {
                        for i in 0..num_of_row {
                            let row = taos_fetch_row(res);
                            let mut str = vec![0 as c_char; 1024];
                            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);
                        }
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        println!("fetch_rows_cb, no more data");
                        taos_free_result(res);
                    }
                }
            }

            extern "C" fn query_cb(taos: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    println!("query_cb");
                    if code == 0 && !res.is_null() {
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        taos_free_result(res);
                    }
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740732937",
                    "create database test_1740732937",
                    "use test_1740732937",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            let num = 7000;
            for i in 0..num {
                let sql = format!("insert into t0 values ({}, {})", ts + i, i);
                test_exec(taos, &sql);
            }

            let sql = c"select * from t0";
            taos_query_a(taos, sql.as_ptr(), query_cb, taos);

            sleep(Duration::from_secs(5));

            test_exec(taos, "drop database test_1740732937");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_column_data_offset() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740785939",
                    "create database test_1740785939",
                    "use test_1740785939",
                    "create table t0 (ts timestamp, c1 varchar(20), c2 nchar(20), c3 varbinary(20), c4 geometry(50))",
                    "insert into t0 values (now+1s, 'hello', 'hello', 'hello', 'POINT(1.0 1.0)')",
                    "insert into t0 values (now+2s, 'world', 'world', 'world', 'POINT(2.0 2.0)')",
                    "insert into t0 values (now+3s, null, null, null, null)",
                    "insert into t0 values (now+4s, 'hello, world', 'hello, world', 'hello, world', 'POINT(3.0 3.0)')",
                    "insert into t0 values (now+5s, null, null, null, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 5);
            assert!(!data.is_null());

            let offset = taos_get_column_data_offset(res, 0);
            assert!(offset.is_null());

            let offset = taos_get_column_data_offset(res, 1);
            assert!(!offset.is_null());

            let offsets = slice::from_raw_parts(offset, rows as _);
            assert_eq!(offsets, [0, 7, -1, 14, -1]);

            let offset = taos_get_column_data_offset(res, 2);
            assert!(!offset.is_null());

            // TODO: confirm the offsets
            let offsets = slice::from_raw_parts(offset, rows as _);
            assert_eq!(offsets, [0, 22, -1, 44, -1]);

            let offset = taos_get_column_data_offset(res, 3);
            assert!(!offset.is_null());

            let offsets = slice::from_raw_parts(offset, rows as _);
            assert_eq!(offsets, [0, 7, -1, 14, -1]);

            let offset = taos_get_column_data_offset(res, 4);
            assert!(!offset.is_null());

            let offsets = slice::from_raw_parts(offset, rows as _);
            assert_eq!(offsets, [0, 23, -1, 46, -1]);

            taos_free_result(res);

            test_exec(taos, "drop database test_1740785939");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_lengths() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740838806",
                    "create database test_1740838806",
                    "use test_1740838806",
                    "create table t0 (ts timestamp, c1 bool, c2 int, c3 varchar(10), c4 nchar(15))",
                    "insert into t0 values (now, 1, 2025, 'hello', 'helloworld')",
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

            let lengths = taos_fetch_lengths(res);
            assert!(!lengths.is_null());

            // TODO: confirm the lengths
            let lengths = slice::from_raw_parts(lengths, 5);
            assert_eq!(lengths, [8, 1, 4, 12, 62]);

            taos_free_result(res);

            test_exec(taos, "drop database test_1740838806");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740841972",
                    "create database test_1740841972",
                    "use test_1740841972",
                    "create table t0 (ts timestamp, c1 bool, c2 int, c3 varchar(10), c4 nchar(15))",
                    "insert into t0 values (now, 1, 2025, 'hello', 'helloworld')",
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

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let lengths = taos_fetch_lengths(res);
            assert!(!lengths.is_null());

            let lengths = slice::from_raw_parts(lengths, 5);
            assert_eq!(lengths, [8, 1, 4, 5, 10]);

            taos_free_result(res);

            test_exec(taos, "drop database test_1740841972");
            taos_close(taos);
        }
    }

    // #[test]
    // fn test_taos_validate_sql() {
    //     unsafe {
    //         let taos = test_connect();
    //         let sql = c"create database if not exists test_1741339814";
    //         let code = taos_validate_sql(taos, sql.as_ptr());
    //         assert_eq!(code, 0);
    //     }
    // }

    #[test]
    fn test_taos_fetch_raw_block_a() {
        unsafe {
            extern "C" fn cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert_eq!(code, 0);
                    assert!(!res.is_null());
                    assert!(!param.is_null());
                    assert_eq!(CStr::from_ptr(param as _), c"hello");

                    let row = taos_result_block(res);
                    assert!(!row.is_null());

                    let fields = taos_fetch_fields(res);
                    assert!(!fields.is_null());

                    let num_fields = taos_num_fields(res);
                    assert_eq!(num_fields, 2);

                    let mut str = vec![0 as c_char; 1024];
                    let len = taos_print_row(str.as_mut_ptr(), *row, fields, num_fields);
                    assert!(len > 0);
                    println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741443150",
                    "create database test_1741443150",
                    "use test_1741443150",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let param = c"hello";
            taos_fetch_raw_block_a(res, cb, param.as_ptr() as _);

            sleep(Duration::from_secs(1));

            taos_free_result(res);

            test_exec(taos, "drop database test_1741443150");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_raw_block() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741489408",
                    "create database test_1741489408",
                    "use test_1741489408",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 2025)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let block = taos_get_raw_block(res);
            assert!(!block.is_null());

            taos_free_result(res);

            test_exec(taos, "drop database test_1741489408");
            taos_close(taos);
        }
    }
}
