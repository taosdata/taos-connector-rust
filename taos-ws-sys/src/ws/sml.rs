use std::ffi::{c_char, c_int, c_void, CStr};
use std::time::Duration;
use std::{ptr, slice};

use taos_error::Code;
use taos_query::common::{Precision, SchemalessPrecision, SchemalessProtocol, SmlDataBuilder, Ty};
use taos_query::util::generate_req_id;
use taos_query::Queryable;
use taos_ws::query::Error;
use taos_ws::{Offset, Taos};
use tracing::{error, instrument, trace};

use crate::ws::error::{set_err_and_get_code, TaosError, TaosMaybeError};
use crate::ws::{ResultSet, ResultSetOperations, TaosResult, TAOS, TAOS_FIELD, TAOS_RES, TAOS_ROW};

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_PROTOCOL_TYPE {
    TSDB_SML_UNKNOWN_PROTOCOL,
    TSDB_SML_LINE_PROTOCOL,
    TSDB_SML_TELNET_PROTOCOL,
    TSDB_SML_JSON_PROTOCOL,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_TIMESTAMP_TYPE {
    TSDB_SML_TIMESTAMP_NOT_CONFIGURED,
    TSDB_SML_TIMESTAMP_HOURS,
    TSDB_SML_TIMESTAMP_MINUTES,
    TSDB_SML_TIMESTAMP_SECONDS,
    TSDB_SML_TIMESTAMP_MILLI_SECONDS,
    TSDB_SML_TIMESTAMP_MICRO_SECONDS,
    TSDB_SML_TIMESTAMP_NANO_SECONDS,
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_schemaless_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_schemaless_insert_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    taos_schemaless_insert_raw_ttl_with_reqid(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        0,
        generate_req_id() as i64,
    )
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    taos_schemaless_insert_raw_ttl_with_reqid(
        taos, lines, len, totalRows, protocol, precision, 0, reqid,
    )
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_schemaless_insert_ttl(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub extern "C" fn taos_schemaless_insert_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    taos_schemaless_insert_raw_ttl_with_reqid(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        ttl,
        generate_req_id() as i64,
    )
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    trace!(taos=?taos, lines=?lines, len, total_rows=?totalRows, protocol, precision, ttl, reqid,
        "taos_schemaless_insert_raw_ttl_with_reqid");

    match schemaless_insert_raw(taos, lines, len, protocol, precision, ttl, reqid) {
        Ok(rs) => {
            trace!(rs=?rs, "taos_schemaless_insert_raw_ttl_with_reqid done");
            let rs: TaosMaybeError<ResultSet> = rs.into();
            Box::into_raw(Box::new(rs)) as _
        }
        Err(err) => {
            trace!(
                err=?err,
                "taos_schemaless_insert_raw_ttl_with_reqid done with error"
            );
            let message = format!("schemaless insert failed: {err}");
            set_err_and_get_code(TaosError::new(err.code(), &message));
            ptr::null_mut()
        }
    }
}

unsafe fn schemaless_insert_raw(
    taos: *mut TAOS,
    lines: *const c_char,
    len: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: c_int,
    reqid: i64,
) -> TaosResult<ResultSet> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;

    let slice = slice::from_raw_parts(lines as *const u8, len as usize);
    let data = String::from_utf8(slice.to_vec())?;
    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::from(protocol))
        .precision(SchemalessPrecision::from(precision))
        .data(vec![data])
        .ttl(ttl)
        .req_id(reqid as u64)
        .build()?;

    taos.put(&sml_data)?;

    Ok(ResultSet::Schemaless(SchemalessResultSet::new(
        0,
        Precision::Millisecond,
        Duration::from_millis(0),
    )))
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    trace!("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key start, taos: {taos:?}, lines: {lines:?}, len: {len}, total_rows: {totalRows:?}, protocol: {protocol}, precision: {precision}, ttl: {ttl}, reqid: {reqid}, tbname_key: {tbnameKey:?}");
    match sml_insert_raw(
        taos, lines, len, totalRows, protocol, precision, ttl, reqid, tbnameKey,
    ) {
        Ok(rs) => {
            trace!("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key succ, rs: {rs:?}");
            let res: TaosMaybeError<ResultSet> = rs.into();
            Box::into_raw(Box::new(res)) as _
        }
        Err(err) => {
            error!("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key failed, err: {err:?}");
            set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
            ptr::null_mut()
        }
    }
}

#[allow(non_snake_case)]
unsafe fn sml_insert_raw(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> TaosResult<ResultSet> {
    trace!("sml_insert_raw, taos: {taos:?}, lines: {lines:?}, len: {len}, total_rows: {totalRows:?}, protocol: {protocol}, precision: {precision}, ttl: {ttl}, reqid: {reqid}, tbname_key: {tbnameKey:?}");

    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;

    let slice = slice::from_raw_parts(lines as _, len as _);
    let data = String::from_utf8(slice.to_vec())?;

    let tbname_key = CStr::from_ptr(tbnameKey)
        .to_str()
        .map_err(|_| TaosError::new(Code::INVALID_PARA, "tbnameKey is invalid utf-8"))?;

    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::from(protocol))
        .precision(SchemalessPrecision::from(precision))
        .data(vec![data])
        .ttl(ttl)
        .req_id(reqid as u64)
        .table_name_key(tbname_key.to_string())
        .build()?;

    taos.put(&sml_data)?;

    Ok(ResultSet::Schemaless(SchemalessResultSet::new(
        0,
        Precision::Millisecond,
        Duration::from_millis(0),
    )))
}

#[no_mangle]
#[instrument(level = "trace", ret)]
#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub extern "C" fn taos_schemaless_insert_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    todo!();
}

#[derive(Debug)]
pub struct SchemalessResultSet {
    affected_rows: i64,
    precision: Precision,
    timing: Duration,
}

impl SchemalessResultSet {
    fn new(affected_rows: i64, precision: Precision, timing: Duration) -> Self {
        Self {
            affected_rows,
            precision,
            timing,
        }
    }
}

impl ResultSetOperations for SchemalessResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        0
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        0
    }

    fn tmq_get_offset(&self) -> Offset {
        todo!()
    }

    fn precision(&self) -> Precision {
        self.precision
    }

    fn affected_rows(&self) -> i32 {
        self.affected_rows as _
    }

    fn affected_rows64(&self) -> i64 {
        self.affected_rows
    }

    fn num_of_fields(&self) -> i32 {
        0
    }

    fn get_fields(&mut self) -> *mut TAOS_FIELD {
        ptr::null_mut()
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *mut c_void, rows: *mut i32) -> Result<(), Error> {
        Err(Error::CommonError(
            "schemaless result set does not support fetch block".to_owned(),
        ))
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        Err(Error::CommonError(
            "schemaless result set does not support fetch row".to_owned(),
        ))
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        (Ty::Null, 0, ptr::null())
    }

    fn take_timing(&mut self) -> Duration {
        self.timing
    }

    fn stop_query(&mut self) {}
}

#[cfg(test)]
mod tests {
    use taos_query::util::generate_req_id;

    use super::*;
    use crate::ws::sml::{TSDB_SML_PROTOCOL_TYPE, TSDB_SML_TIMESTAMP_TYPE};
    use crate::ws::{test_connect, test_exec, test_exec_many};

    #[test]
    fn test_sml_insert_raw() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737291333",
                    "create database test_1737291333",
                    "use test_1737291333",
                ],
            );

            let data = c"measurement,host=host1 field1=2i,field2=2.0 1741153642000";
            let len = data.to_bytes().len() as i32;
            let lines = data.as_ptr() as *mut _;
            let total_rows = ptr::null_mut();
            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;
            let ttl = 0;

            let res = taos_schemaless_insert_raw(taos, lines, len, total_rows, protocol, precision);
            assert!(!res.is_null());

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_raw_with_reqid(
                taos, lines, len, total_rows, protocol, precision, req_id,
            );
            assert!(!res.is_null());

            let res = taos_schemaless_insert_raw_ttl(
                taos, lines, len, total_rows, protocol, precision, ttl,
            );
            assert!(!res.is_null());

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_raw_ttl_with_reqid(
                taos, lines, len, total_rows, protocol, precision, ttl, req_id,
            );
            assert!(!res.is_null());

            let req_id = generate_req_id() as _;
            let tbname_key = c"host".as_ptr() as *mut _;
            let res = taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
                taos, lines, len, total_rows, protocol, precision, ttl, req_id, tbname_key,
            );
            assert!(!res.is_null());

            test_exec(taos, "drop database test_1737291333");
        }
    }
}
