use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::OnceLock;
use std::time::Duration;
use std::{ptr, slice};

use bytes::Bytes;
use faststr::FastStr;
use taos_error::Code;
use taos_query::common::{Precision, Ty};
use taos_query::util::{generate_req_id, hex, InlineBytes, InlineStr};
use taos_query::{
    block_in_place_or_global, global_tokio_runtime, Fetchable, Queryable, RawBlock as Block,
};
use taos_ws::query::Error;
use taos_ws::{Offset, Taos};
use tracing::{debug, error, Instrument};

use crate::taos::query::TSDB_SERVER_STATUS;
use crate::taos::{__taos_async_fn_t, TAOS_FIELD, TAOS_FIELD_E, TAOS_RES};
use crate::ws::error::{
    clear_err_and_ret_succ, format_errno, set_err_and_get_code, TaosError, TaosMaybeError,
};
use crate::ws::{config, ResultSet, ResultSetOperations, Row, SafePtr, TaosResult, TAOS, TAOS_ROW};

impl From<i32> for TSDB_SERVER_STATUS {
    fn from(value: i32) -> Self {
        match value {
            1 => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_NETWORK_OK,
            2 => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_SERVICE_OK,
            3 => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_SERVICE_DEGRADED,
            4 => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_EXTING,
            _ => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_UNAVAILABLE,
        }
    }
}

pub unsafe fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES {
    taos_query_with_reqid(taos, sql, generate_req_id() as _)
}

#[allow(non_snake_case)]
pub unsafe fn taos_query_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    reqId: i64,
) -> *mut TAOS_RES {
    debug!("taos_query_with_reqid start, taos: {taos:?}, sql: {sql:?}, req_id: {reqId}");
    match query(taos, sql, reqId as u64) {
        Ok(rs) => {
            let res: TaosMaybeError<ResultSet> = rs.into();
            let res = Box::into_raw(Box::new(res)) as _;
            debug!("taos_query_with_reqid succ, res: {res:?}");
            res
        }
        Err(err) => {
            error!("taos_query_with_reqid failed, err: {err:?}");
            set_err_and_get_code(err);
            ptr::null_mut()
        }
    }
}

unsafe fn query(taos: *mut TAOS, sql: *const c_char, req_id: u64) -> TaosResult<ResultSet> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;
    let sql = CStr::from_ptr(sql).to_str()?;
    debug!("query, sql: {sql:?}");
    let rs = taos.query_with_req_id(sql, req_id)?;
    Ok(ResultSet::Query(QueryResultSet::new(rs)))
}

pub unsafe fn taos_fetch_row(res: *mut TAOS_RES) -> TAOS_ROW {
    fn handle_error(code: Code, msg: &str) -> TAOS_ROW {
        error!("taos_fetch_row failed, code: {code:?}, msg: {msg}");
        set_err_and_get_code(TaosError::new(code, msg));
        ptr::null_mut()
    }

    debug!("taos_fetch_row start, res: {res:?}");

    let rs = match (res as *mut TaosMaybeError<ResultSet>).as_mut() {
        Some(rs) => rs,
        None => return handle_error(Code::INVALID_PARA, "res is null"),
    };

    let rs = match rs.deref_mut() {
        Some(rs) => rs,
        None => return handle_error(Code::INVALID_PARA, "res is invalid"),
    };

    match rs.fetch_row() {
        Ok(row) => {
            debug!("taos_fetch_row succ, row: {row:?}");
            row
        }
        Err(err) => handle_error(err.errno(), &err.errstr()),
    }
}

pub unsafe fn taos_result_precision(res: *mut TAOS_RES) -> c_int {
    debug!("taos_result_precision start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            debug!("taos_result_precision succ, rs: {rs:?}");
            rs.precision() as _
        }
        None => {
            error!("taos_result_precision failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

pub unsafe fn taos_free_result(res: *mut TAOS_RES) {
    debug!("taos_free_result, res: {res:?}");
    if !res.is_null() {
        let _ = Box::from_raw(res as *mut TaosMaybeError<ResultSet>);
    }
}

pub unsafe fn taos_field_count(res: *mut TAOS_RES) -> c_int {
    debug!("taos_field_count start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!(rs=?rs, "taos_field_count done");
            debug!("taos_field_count succ, rs: {rs:?}");
            rs.num_of_fields()
        }
        None => {
            error!("taos_field_count failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

pub unsafe fn taos_num_fields(res: *mut TAOS_RES) -> c_int {
    taos_field_count(res)
}

pub unsafe fn taos_affected_rows(res: *mut TAOS_RES) -> c_int {
    debug!("taos_affected_rows start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("taos_affected_rows succ, rs: {rs:?}");
            rs.affected_rows() as _
        }
        None => {
            error!("taos_affected_rows failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

pub unsafe fn taos_affected_rows64(res: *mut TAOS_RES) -> i64 {
    debug!("taos_affected_rows64 start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("taos_affected_rows64 succ, rs: {rs:?}");
            rs.affected_rows64() as _
        }
        None => {
            error!("taos_affected_rows64 failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null")) as _
        }
    }
}

pub unsafe fn taos_fetch_fields(res: *mut TAOS_RES) -> *mut TAOS_FIELD {
    debug!("taos_fetch_fields start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            debug!("taos_fetch_fields succ, rs: {rs:?}");
            rs.get_fields()
        }
        None => {
            error!("taos_fetch_fields failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"));
            ptr::null_mut()
        }
    }
}

pub unsafe fn taos_fetch_fields_e(res: *mut TAOS_RES) -> *mut TAOS_FIELD_E {
    debug!("taos_fetch_fields_e start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            debug!("taos_fetch_fields_e succ, rs: {rs:?}");
            rs.get_fields_e()
        }
        None => {
            error!("taos_fetch_fields_e failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"));
            ptr::null_mut()
        }
    }
}

pub unsafe fn taos_select_db(taos: *mut TAOS, db: *const c_char) -> c_int {
    debug!("taos_select_db start, taos: {taos:?}, db: {db:?}");

    let taos = match (taos as *mut Taos).as_mut() {
        Some(taos) => taos,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null")),
    };

    if db.is_null() {
        error!("taos_select_db failed, db is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "db is null"));
    }

    let db = match CStr::from_ptr(db).to_str() {
        Ok(db) => db,
        Err(_) => {
            error!("taos_select_db failed, db is invalid utf-8");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "db is invalid utf-8"));
        }
    };

    match taos.query(format!("use {db}")) {
        Ok(_) => {
            debug!("taos_select_db succ");
            0
        }
        Err(err) => {
            error!("taos_select_db failed, err: {err:?}");
            set_err_and_get_code(err.into())
        }
    }
}

pub unsafe fn taos_print_row(
    str: *mut c_char,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    taos_print_row_with_size(str, i32::MAX as _, row, fields, num_fields)
}

pub unsafe fn taos_print_row_with_size(
    str: *mut c_char,
    size: u32,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    unsafe fn write_to_cstr(size: &mut usize, str: *mut c_char, content: &[u8]) -> i32 {
        if content.len() > *size {
            return -1;
        }
        let cstr = content.as_ptr() as *const c_char;
        ptr::copy_nonoverlapping(cstr, str, content.len());
        *size -= content.len();
        content.len() as _
    }

    debug!("taos_print_row_with_size start, str: {str:?}, size: {size}, row: {row:?}, fields: {fields:?}, num_fields: {num_fields}");

    if str.is_null() || row.is_null() || fields.is_null() || num_fields <= 0 {
        error!("taos_print_row_with_size failed, invalid params");
        return 0;
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
            write_to_cstr(&mut size, str.add(len), b"NULL")
        } else {
            macro_rules! read_and_write {
                ($ty:ty) => {{
                    let value = ptr::read_unaligned(row[i] as *const $ty);
                    write_to_cstr(
                        &mut size,
                        str.add(len as usize),
                        format!("{value}").as_str().as_bytes(),
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
                        format!("{}", value as i32).as_str().as_bytes(),
                    )
                }
                Ty::VarBinary => {
                    let data = row[i].offset(-2) as *const InlineBytes;
                    let data = Bytes::from((*data).as_bytes());
                    let content = format!("\\x{}", hex::bytes_to_hex_string(data).to_uppercase());
                    write_to_cstr(&mut size, str.add(len), content.as_bytes())
                }
                Ty::VarChar | Ty::NChar | Ty::Geometry => {
                    let data = row[i].offset(-2) as *const InlineStr;
                    write_to_cstr(&mut size, str.add(len), (*data).as_bytes())
                }
                Ty::Decimal | Ty::Decimal64 => {
                    let data = CStr::from_ptr(row[i] as *mut c_char).to_str().unwrap();
                    write_to_cstr(&mut size, str.add(len), data.as_bytes())
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

    debug!("taos_print_row_with_size succ, str: {str:?}, len: {len}");

    len as _
}

pub unsafe fn taos_stop_query(res: *mut TAOS_RES) {
    debug!("taos_stop_query start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            debug!("taos_stop_query succ, rs: {rs:?}");
            rs.stop_query();
        }
        None => {
            error!("taos_stop_query failed, res is null");
            let _ = set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"));
        }
    }
}

pub unsafe fn taos_is_null(res: *mut TAOS_RES, row: i32, col: i32) -> bool {
    debug!("taos_is_null start, res: {res:?}, row: {row}, col: {col}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(ResultSet::Query(rs)) => match &rs.block {
            Some(block) => {
                let is_null = block.is_null(row as _, col as _);
                debug!("taos_is_null succ, is_null: {is_null}");
                is_null
            }
            None => true,
        },
        _ => true,
    }
}

#[allow(non_snake_case)]
pub unsafe fn taos_is_null_by_column(
    res: *mut TAOS_RES,
    columnIndex: c_int,
    result: *mut bool,
    rows: *mut c_int,
) -> c_int {
    debug!("taos_is_null_by_column start, res: {res:?}, column_index: {columnIndex}, result: {result:?}, rows: {rows:?}");

    if res.is_null() || result.is_null() || rows.is_null() || *rows <= 0 || columnIndex < 0 {
        error!("taos_is_null_by_column failed, invalid params");
        return Code::INVALID_PARA.into();
    }

    let maybe_err = (res as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
    if maybe_err.deref_mut().is_none() {
        error!("taos_is_null_by_column failed, res is invalid");
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
                debug!("taos_is_null_by_column succ, is_nulls: {is_nulls:?}");
                *rows = is_nulls.len() as _;
                let res = slice::from_raw_parts_mut(result, is_nulls.len());
                for (i, is_null) in is_nulls.iter().enumerate() {
                    res[i] = *is_null;
                }
                maybe_err.clear_err();
                clear_err_and_ret_succ()
            }
            None => {
                error!("taos_is_null_by_column failed, block is none");
                maybe_err.with_err(Some(TaosError::new(Code::FAILED, "block is none")));
                format_errno(Code::FAILED.into())
            }
        },
        _ => {
            error!("taos_is_null_by_column failed, rs is invalid");
            maybe_err.with_err(Some(TaosError::new(Code::FAILED, "rs is invalid")));
            format_errno(Code::FAILED.into())
        }
    }
}

pub unsafe fn taos_is_update_query(res: *mut TAOS_RES) -> bool {
    debug!("taos_is_update_query, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => rs.num_of_fields() == 0,
        None => true,
    }
}

pub unsafe fn taos_fetch_block(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int {
    debug!("taos_fetch_block start, res: {res:?}, rows: {rows:?}");
    let mut num_of_rows = 0;
    let _ = taos_fetch_block_s(res, &mut num_of_rows, rows);
    debug!("taos_fetch_block succ, num_of_rows: {num_of_rows}");
    num_of_rows
}

#[allow(non_snake_case)]
pub unsafe fn taos_fetch_block_s(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    rows: *mut TAOS_ROW,
) -> c_int {
    debug!("taos_fetch_block_s start, res: {res:?}, num_of_rows: {numOfRows:?}, rows: {rows:?}");

    if numOfRows.is_null() {
        error!("taos_fetch_block_s failed, num_of_rows is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "numOfRows is null"));
    }

    match (res as *mut TaosMaybeError<ResultSet>).as_mut() {
        Some(maybe_err) => match maybe_err.deref_mut() {
            Some(rs) => match rs.fetch_block(rows, numOfRows) {
                Ok(()) => {
                    debug!("taos_fetch_block_s succ, num_of_rows: {}", *numOfRows);
                    0
                }
                Err(err) => {
                    error!("taos_fetch_block_s failed, err: {err:?}");
                    set_err_and_get_code(TaosError::new(err.errno(), &err.errstr()));
                    maybe_err.with_err(Some(TaosError::new(err.errno(), &err.errstr())));
                    err.errno().into()
                }
            },
            None => {
                error!("taos_fetch_block_s failed, res is invalid");
                set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is invalid"))
            }
        },
        None => {
            error!("taos_fetch_block_s failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

#[allow(non_snake_case)]
pub unsafe fn taos_fetch_raw_block(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    pData: *mut *mut c_void,
) -> c_int {
    unsafe fn handle_error(message: &str, rows: *mut i32) -> i32 {
        error!("taos_fetch_raw_block failed, message: {message}, rows: {rows:?}");
        *rows = 0;
        set_err_and_get_code(TaosError::new(Code::FAILED, message))
    }

    debug!(
        "taos_fetch_raw_block start, res: {res:?}, num_of_rows: {numOfRows:?}, p_data: {pData:?}"
    );

    match (res as *mut TaosMaybeError<ResultSet>).as_mut() {
        Some(maybe_err) => match maybe_err.deref_mut() {
            Some(rs) => match rs.fetch_raw_block(pData, numOfRows) {
                Ok(()) => {
                    debug!("taos_fetch_raw_block succ, rs: {rs:?}");
                    0
                }
                Err(err) => {
                    error!("taos_fetch_raw_block failed, err: {err:?}");
                    set_err_and_get_code(TaosError::new(err.errno(), &err.errstr()));
                    let code = err.errno();
                    maybe_err.with_err(Some(err.into()));
                    code.into()
                }
            },
            None => handle_error("res is invalid", numOfRows),
        },
        None => handle_error("res is null", numOfRows),
    }
}

pub unsafe fn taos_fetch_raw_block_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    debug!("taos_fetch_raw_block_a start, res: {res:?}, fp: {fp:?}, param: {param:?}");

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
                        let rows = block.as_ref().map_or(0, |block| block.nrows());
                        qrs.block = block;
                        debug!("taos_fetch_raw_block_a callback succ, rows: {rows}");
                        fp(param.0, res.0, rows as _);
                    }
                    Err(err) => {
                        error!("taos_fetch_raw_block_a callback failed, err: {err:?}");
                        maybe_err.with_err(Some(TaosError::new(err.code(), &err.message())));
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

    debug!("taos_fetch_raw_block_a succ");
}

pub unsafe fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW {
    debug!("taos_result_block start, res: {res:?}");

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
                debug!("taos_result_block succ, ptr: {:?}", qrs.row_data_ptr_ptr);
                return qrs.row_data_ptr_ptr;
            }
            Err(err) => {
                error!("taos_result_block failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.errno(), &err.errstr())));
            }
        }
    } else {
        error!("taos_result_block failed, rs is invalid");
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "rs is invalid")));
    };

    ptr::null_mut()
}

#[allow(non_snake_case)]
pub unsafe fn taos_get_column_data_offset(res: *mut TAOS_RES, columnIndex: c_int) -> *mut c_int {
    debug!("taos_get_column_data_offset start, res: {res:?}, column_index: {columnIndex}");

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
                debug!("taos_get_column_data_offset succ, offsets: {offsets:?}");
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

pub unsafe fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> c_int {
    debug!("taos_validate_sql start, taos: {taos:?}, sql: {sql:?}");

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

    debug!("taos_validate_sql, sql: {sql}");

    let taos = (taos as *mut Taos).as_mut().unwrap();
    if let Err(err) = taos_query::block_in_place_or_global(taos.client().validate_sql(sql)) {
        error!("taos_validate_sql failed, err: {err:?}");
        return set_err_and_get_code(err.into());
    }

    debug!("taos_validate_sql succ");
    0
}

pub unsafe fn taos_fetch_lengths(res: *mut TAOS_RES) -> *mut c_int {
    debug!("taos_fetch_lengths start, res: {res:?}");

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
                    tracing::debug!(
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

            debug!("taos_fetch_lengths succ, lengths: {lengths:?}");

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

pub unsafe fn taos_get_server_info(taos: *mut TAOS) -> *const c_char {
    debug!("taos_get_server_info start, taos: {taos:?}");

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

    debug!("taos_get_server_info succ, server_info: {server_info:?}");

    server_info.as_ptr()
}

pub unsafe fn taos_get_client_info() -> *const c_char {
    debug!("taos_get_client_info");
    static VERSION: OnceLock<CString> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            let version = env!("CARGO_PKG_VERSION");
            CString::new(version).unwrap()
        })
        .as_ptr()
}

pub unsafe fn taos_get_current_db(
    taos: *mut TAOS,
    database: *mut c_char,
    len: c_int,
    required: *mut c_int,
) -> c_int {
    debug!("taos_get_current_db start, taos: {taos:?}, database: {database:?}, len: {len}, required: {required:?}");

    if taos.is_null() {
        error!("taos_get_current_db failed, taos is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
    }

    let res = taos_query(taos, c"SELECT DATABASE()".as_ptr());
    if res.is_null() {
        error!("taos_get_current_db failed, query failed");
        return set_err_and_get_code(TaosError::new(Code::FAILED, "query failed"));
    }

    let mut rows = 0;
    let mut data = ptr::null_mut();
    let mut code = taos_fetch_raw_block(res, &mut rows, &mut data);
    if code != 0 {
        taos_free_result(res);
        error!("taos_get_current_db failed, err: fetch raw block failed, code: {code}");
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
        return clear_err_and_ret_succ();
    }

    if len_actual < len as u32 {
        ptr::copy_nonoverlapping(db as _, database, len_actual as _);
    } else {
        ptr::copy_nonoverlapping(db as _, database, len as _);
        *required = len_actual as _;
        code = -1;
    }

    taos_free_result(res);

    debug!("taos_get_current_db succ, database: {database:?}");

    code
}

pub fn taos_data_type(r#type: c_int) -> *const c_char {
    debug!("taos_data_type, type: {}", r#type);
    match Ty::from_u8_option(r#type as _) {
        Some(ty) => ty.tsdb_name(),
        None => c"UNKNOWN".as_ptr(),
    }
}

pub unsafe fn taos_query_a(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    taos_query_a_with_reqid(taos, sql, fp, param, generate_req_id() as _);
}

pub unsafe fn taos_query_a_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
    reqid: i64,
) {
    debug!("taos_query_a_with_reqid start, taos: {taos:?}, sql: {sql:?}, reqid: {reqid}, fp: {fp:?}, param: {param:?}");

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
                    debug!("taos_query_a_with_reqid callback, result_set: {rs:?}");
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

    debug!("taos_query_a_with_reqid succ");
}

pub unsafe fn taos_fetch_rows_a(res: *mut TAOS_RES, fp: __taos_async_fn_t, param: *mut c_void) {
    debug!("taos_fetch_rows_a start, res: {res:?}, fp: {fp:?}, param: {param:?}");

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

    debug!("taos_fetch_rows_a succ");
}

pub unsafe fn taos_get_raw_block(res: *mut TAOS_RES) -> *const c_void {
    unsafe fn handle_error(message: &str) -> *const c_void {
        error!("taos_get_raw_block failed, {message}");
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, message));
        ptr::null()
    }

    debug!("taos_get_raw_block start, res: {res:?}");

    let maybe_err = match (res as *mut TaosMaybeError<ResultSet>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return handle_error("res is null"),
    };

    let rs = match maybe_err.deref_mut() {
        Some(rs) => rs,
        None => return handle_error("res is invalid"),
    };

    if let ResultSet::Query(rs) = rs {
        debug!("taos_get_raw_block succ, rs: {rs:?}");
        rs.get_raw_block()
    } else {
        handle_error("rs is invalid")
    }
}

pub unsafe fn taos_check_server_status(
    fqdn: *const c_char,
    mut port: i32,
    details: *mut c_char,
    maxlen: i32,
) -> TSDB_SERVER_STATUS {
    debug!("taos_check_server_status start, fqdn: {fqdn:?}, port: {port}, details: {details:?}, maxlen: {maxlen}");

    let fqdn = if fqdn.is_null() {
        config::get_global_fqdn()
    } else {
        match CStr::from_ptr(fqdn).to_str() {
            Ok(fqdn) => Some(fqdn.to_string().into()),
            Err(_) => {
                error!("taos_check_server_status failed, fqdn is invalid utf-8");
                set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "fqdn is invalid utf-8"));
                return TSDB_SERVER_STATUS::TSDB_SRV_STATUS_UNAVAILABLE;
            }
        }
    };

    if port == 0 {
        port = config::get_global_server_port() as _;
    }

    debug!("taos_check_server_status, fqdn: {fqdn:?}, port: {port}");

    let mut host = FastStr::from_static_str("localhost");
    if let Some(ep) = config::get_global_first_ep() {
        host = ep;
    } else if let Some(ep) = config::get_global_second_ep() {
        host = ep;
    }

    let dsn = if host.contains(":") {
        format!("ws://{host}")
    } else {
        format!("ws://{host}:6041")
    };

    debug!("taos_check_server_status, dsn: {dsn}");

    let (status, ds) =
        match block_in_place_or_global(taos_ws::query::check_server_status(&dsn, fqdn, port)) {
            Ok(res) => res,
            Err(err) => {
                error!("taos_check_server_status failed, err: {err:?}");
                set_err_and_get_code(err.into());
                return TSDB_SERVER_STATUS::TSDB_SRV_STATUS_UNAVAILABLE;
            }
        };

    debug!("taos_check_server_status succ, status: {status}, details: {ds:?}");

    let len = ds.len().min(maxlen as usize);
    ptr::copy_nonoverlapping(ds.as_ptr() as _, details, len);
    status.into()
}

#[derive(Debug)]
pub struct QueryResultSet {
    rs: taos_ws::ResultSet,
    block: Option<Block>,
    fields: Vec<TAOS_FIELD>,
    fields_e: Vec<TAOS_FIELD_E>,
    row: Row,
    #[allow(dead_code)]
    row_data_ptr: TAOS_ROW,
    row_data_ptr_ptr: *mut TAOS_ROW,
    offsets: Option<Vec<i32>>,
    lengths: Option<Vec<i32>>,
    has_called_fetch_row: bool,
    rows: Vec<*const c_void>,
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
            fields_e: Vec::new(),
            row,
            row_data_ptr,
            row_data_ptr_ptr,
            offsets: None,
            lengths: None,
            has_called_fetch_row: false,
            rows: vec![ptr::null(); num_of_fields],
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

    pub fn get_raw_block(&self) -> *const c_void {
        if let Some(block) = self.block.as_ref() {
            return block.as_raw_bytes().as_ptr() as _;
        }
        ptr::null()
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

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E {
        if self.fields_e.len() != self.rs.num_of_fields() {
            self.fields_e.clear();
            let precisions = self.rs.fields_precisions().unwrap();
            let scales = self.rs.fields_scales().unwrap();
            for (i, field) in self.rs.fields().iter().enumerate() {
                self.fields_e
                    .push(TAOS_FIELD_E::new(field, precisions[i] as _, scales[i] as _));
            }
        }
        self.fields_e.as_mut_ptr()
    }

    unsafe fn fetch_raw_block(
        &mut self,
        ptr: *mut *mut c_void,
        rows: *mut i32,
    ) -> Result<(), Error> {
        self.block = self.rs.fetch_raw_block()?;
        if let Some(block) = self.block.as_ref() {
            *ptr = block.as_raw_bytes().as_ptr() as _;
            *rows = block.nrows() as _;
        } else {
            *rows = 0;
        }
        Ok(())
    }

    unsafe fn fetch_block(&mut self, rows: *mut TAOS_ROW, num: *mut c_int) -> Result<(), Error> {
        if self.block.is_none() || self.row.current_row >= self.block.as_ref().unwrap().nrows() {
            self.block = self.rs.fetch_raw_block()?;
            self.row.current_row = 0;
        }

        if let Some(block) = self.block.as_ref() {
            if block.nrows() > 0 {
                for (i, col) in block.columns().enumerate() {
                    self.rows[i] = col.as_raw_ptr();
                }
                self.row.current_row = block.nrows();
                *rows = self.rows.as_ptr() as _;
                *num = block.nrows() as _;
            }
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
        if let Some(block) = self.block.as_ref() {
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
