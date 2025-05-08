use std::ffi::{c_char, c_int, c_void, CStr};
use std::time::Duration;
use std::{ptr, slice};

use taos_error::Code;
use taos_query::common::{Precision, SchemalessPrecision, SchemalessProtocol, SmlDataBuilder, Ty};
use taos_query::util::generate_req_id;
use taos_query::Queryable;
use taos_ws::query::Error;
use taos_ws::{Offset, Taos};
use tracing::{debug, error};

use crate::taos::sml::TSDB_SML_PROTOCOL_TYPE;
use crate::taos::TAOS_RES;
use crate::ws::error::{set_err_and_get_code, TaosError, TaosMaybeError};
use crate::ws::{
    ResultSet, ResultSetOperations, TaosResult, TAOS, TAOS_FIELD, TAOS_FIELD_E, TAOS_ROW,
};

#[allow(non_snake_case)]
pub unsafe fn taos_schemaless_insert_raw(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        0,
        generate_req_id() as i64,
        ptr::null_mut(),
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
    taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        0,
        reqid,
        ptr::null_mut(),
    )
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
    taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        ttl,
        generate_req_id() as i64,
        ptr::null_mut(),
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
    taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        ttl,
        reqid,
        ptr::null_mut(),
    )
}

#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub unsafe fn taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
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
    debug!("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key start, taos: {taos:?}, lines: {lines:?}, len: {len}, total_rows: {totalRows:?}, protocol: {protocol}, precision: {precision}, ttl: {ttl}, reqid: {reqid}, tbname_key: {tbnameKey:?}");
    match sml_insert_raw(
        taos, lines, len, totalRows, protocol, precision, ttl, reqid, tbnameKey,
    ) {
        Ok(rs) => {
            debug!("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key succ, rs: {rs:?}");
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
#[allow(clippy::too_many_arguments)]
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
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;

    if lines.is_null() {
        return Err(TaosError::new(Code::INVALID_PARA, "lines is null"));
    }

    if len < 0 {
        return Err(TaosError::new(Code::INVALID_PARA, "len is less than 0"));
    }

    let slice: &[u8] = slice::from_raw_parts(lines as _, len as _);

    if !totalRows.is_null() {
        let mut num_lines = 0;
        let mut idx = 0;
        for (i, b) in slice.iter().enumerate() {
            if *b == b'\n' || i == len as usize - 1 {
                if !(protocol == TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as i32
                    && slice[idx] == b'#')
                {
                    num_lines += 1;
                }
                idx = i + 1;
            }
        }
        *totalRows = num_lines;
    }

    let data = String::from_utf8(slice.to_vec())?;

    debug!("sml_insert_raw, data: {data:?}");

    let sml_data = if !tbnameKey.is_null() {
        let tbname_key = CStr::from_ptr(tbnameKey)
            .to_str()
            .map_err(|_| TaosError::new(Code::INVALID_PARA, "tbnameKey is invalid utf-8"))?;

        SmlDataBuilder::default()
            .protocol(SchemalessProtocol::from(protocol))
            .precision(SchemalessPrecision::from(precision))
            .data(vec![data])
            .ttl(ttl)
            .req_id(reqid as u64)
            .table_name_key(tbname_key.to_string())
            .build()?
    } else {
        SmlDataBuilder::default()
            .protocol(SchemalessProtocol::from(protocol))
            .precision(SchemalessPrecision::from(precision))
            .data(vec![data])
            .ttl(ttl)
            .req_id(reqid as u64)
            .build()?
    };

    debug!("sml_insert_raw, sml_data: {sml_data:?}");

    taos.put(&sml_data)?;

    Ok(ResultSet::Schemaless(SchemalessResultSet::new(
        0,
        Precision::Millisecond,
        Duration::from_millis(0),
    )))
}

#[allow(non_snake_case)]
pub unsafe fn taos_schemaless_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    taos_schemaless_insert_ttl_with_reqid_tbname_key(
        taos,
        lines,
        numLines,
        protocol,
        precision,
        0,
        generate_req_id() as i64,
        ptr::null_mut(),
    )
}

#[allow(non_snake_case)]
pub unsafe fn taos_schemaless_insert_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    taos_schemaless_insert_ttl_with_reqid_tbname_key(
        taos,
        lines,
        numLines,
        protocol,
        precision,
        0,
        reqid,
        ptr::null_mut(),
    )
}

#[allow(non_snake_case)]
pub unsafe fn taos_schemaless_insert_ttl(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    taos_schemaless_insert_ttl_with_reqid_tbname_key(
        taos,
        lines,
        numLines,
        protocol,
        precision,
        ttl,
        generate_req_id() as i64,
        ptr::null_mut(),
    )
}

#[allow(non_snake_case)]
pub unsafe fn taos_schemaless_insert_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    taos_schemaless_insert_ttl_with_reqid_tbname_key(
        taos,
        lines,
        numLines,
        protocol,
        precision,
        ttl,
        reqid,
        ptr::null_mut(),
    )
}

#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub unsafe fn taos_schemaless_insert_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    debug!("taos_schemaless_insert_ttl_with_reqid_tbname_key start, taos: {taos:?}, lines: {lines:?}, num_lines: {numLines}, protocol: {protocol}, precision: {precision}, ttl: {ttl}, reqid: {reqid}, tbname_key: {tbnameKey:?}");
    match sml_insert(
        taos, lines, numLines, protocol, precision, ttl, reqid, tbnameKey,
    ) {
        Ok(rs) => {
            debug!("taos_schemaless_insert_ttl_with_reqid_tbname_key succ, rs: {rs:?}");
            let res: TaosMaybeError<ResultSet> = rs.into();
            Box::into_raw(Box::new(res)) as _
        }
        Err(err) => {
            error!("taos_schemaless_insert_ttl_with_reqid_tbname_key failed, err: {err:?}");
            set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
            ptr::null_mut()
        }
    }
}

#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
unsafe fn sml_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    mut numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> TaosResult<ResultSet> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;

    if lines.is_null() {
        return Err(TaosError::new(Code::INVALID_PARA, "lines is null"));
    }

    if numLines < 0 {
        return Err(TaosError::new(
            Code::INVALID_PARA,
            "numLines is less than 0",
        ));
    }

    if SchemalessProtocol::from(protocol) == SchemalessProtocol::Json {
        numLines = 1;
    }

    let lines: &[*mut c_char] = slice::from_raw_parts(lines as _, numLines as _);
    let mut datas = Vec::with_capacity(numLines as _);

    for line in lines {
        if line.is_null() {
            return Err(TaosError::new(Code::INVALID_PARA, "line is null"));
        }
        let data = CStr::from_ptr(*line)
            .to_str()
            .map_err(|_| TaosError::new(Code::INVALID_PARA, "line is invalid utf-8"))?;
        datas.push(data.to_string());
    }

    debug!("sml_insert, datas: {datas:?}");

    let sml_data = if !tbnameKey.is_null() {
        let tbname_key = CStr::from_ptr(tbnameKey)
            .to_str()
            .map_err(|_| TaosError::new(Code::INVALID_PARA, "tbnameKey is invalid utf-8"))?;

        SmlDataBuilder::default()
            .protocol(SchemalessProtocol::from(protocol))
            .precision(SchemalessPrecision::from(precision))
            .data(datas)
            .ttl(ttl)
            .req_id(reqid as u64)
            .table_name_key(tbname_key.to_string())
            .build()?
    } else {
        SmlDataBuilder::default()
            .protocol(SchemalessProtocol::from(protocol))
            .precision(SchemalessPrecision::from(precision))
            .data(datas)
            .ttl(ttl)
            .req_id(reqid as u64)
            .build()?
    };

    debug!("sml_insert, sml_data: {sml_data:?}");

    taos.put(&sml_data)?;

    Ok(ResultSet::Schemaless(SchemalessResultSet::new(
        0,
        Precision::Millisecond,
        Duration::from_millis(0),
    )))
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

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E {
        ptr::null_mut()
    }

    unsafe fn fetch_raw_block(
        &mut self,
        _ptr: *mut *mut c_void,
        _rows: *mut i32,
    ) -> Result<(), Error> {
        Err(Error::CommonError(
            "schemaless result set does not support fetch block".to_owned(),
        ))
    }

    unsafe fn fetch_block(&mut self, _rows: *mut TAOS_ROW, _num: *mut c_int) -> Result<(), Error> {
        todo!()
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        Err(Error::CommonError(
            "schemaless result set does not support fetch row".to_owned(),
        ))
    }

    unsafe fn get_raw_value(&mut self, _row: usize, _col: usize) -> (Ty, u32, *const c_void) {
        (Ty::Null, 0, ptr::null())
    }

    fn take_timing(&mut self) -> Duration {
        self.timing
    }

    fn stop_query(&mut self) {}
}
