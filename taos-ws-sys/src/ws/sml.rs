use std::ffi::{c_char, c_int, c_void};
use std::time::Duration;
use std::{ptr, slice};

use taos_error::Code;
use taos_query::common::{Precision, SchemalessPrecision, SchemalessProtocol, SmlDataBuilder, Ty};
use taos_query::util::generate_req_id;
use taos_query::Queryable;
use taos_ws::query::Error;
use taos_ws::{Offset, Taos};
use tracing::trace;

use crate::taos::{TAOS, TAOS_RES, TAOS_ROW};
use crate::ws::error::{set_err_and_get_code, TaosError, TaosMaybeError};
use crate::ws::{ResultSet, ResultSetOperations, TaosResult, TAOS_FIELD};

#[allow(non_snake_case)]
pub fn taos_schemaless_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    todo!();
}

#[allow(non_snake_case)]
pub fn taos_schemaless_insert_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    todo!();
}

#[allow(non_snake_case)]
pub unsafe fn taos_schemaless_insert_raw(
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

#[allow(non_snake_case)]
pub unsafe fn taos_schemaless_insert_raw_with_reqid(
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

#[allow(non_snake_case)]
pub fn taos_schemaless_insert_ttl(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    todo!();
}

#[allow(non_snake_case)]
pub fn taos_schemaless_insert_ttl_with_reqid(
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

#[allow(non_snake_case)]
pub unsafe fn taos_schemaless_insert_raw_ttl(
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

#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub unsafe fn taos_schemaless_insert_raw_ttl_with_reqid(
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

#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub fn taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
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
    todo!();
}

#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub fn taos_schemaless_insert_ttl_with_reqid_tbname_key(
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
    use std::ffi::CString;

    use taos_query::util::generate_req_id;

    use super::*;
    use crate::taos::sml::{TSDB_SML_PROTOCOL_TYPE, TSDB_SML_TIMESTAMP_TYPE};
    use crate::ws::{test_connect, test_exec, test_exec_many};

    #[test]
    fn test_taos_schemaless_insert_raw() {
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

            let data = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639";
            let len = data.len() as i32;
            let lines = CString::new(data).unwrap();
            let lines = lines.as_ptr();

            let mut total_rows = 0;
            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;
            let ttl = 0;

            let res = taos_schemaless_insert_raw(
                taos,
                lines as _,
                len,
                &mut total_rows as _,
                protocol,
                precision,
            );
            assert!(!res.is_null());

            let res = taos_schemaless_insert_raw_with_reqid(
                taos,
                lines as _,
                len,
                &mut total_rows as _,
                protocol,
                precision,
                generate_req_id() as i64,
            );
            assert!(!res.is_null());

            let res = taos_schemaless_insert_raw_ttl(
                taos,
                lines as _,
                len,
                &mut total_rows as _,
                protocol,
                precision,
                ttl,
            );
            assert!(!res.is_null());

            let res = taos_schemaless_insert_raw_ttl_with_reqid(
                taos,
                lines as _,
                len,
                &mut total_rows as _,
                protocol,
                precision,
                ttl,
                generate_req_id() as i64,
            );
            assert!(!res.is_null());

            test_exec(taos, "drop database test_1737291333");
        }
    }
}
