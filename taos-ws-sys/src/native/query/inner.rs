use std::ffi::{c_char, c_void, CStr};
use std::ptr;
use std::time::Duration;

use taos_error::Code;
use taos_query::common::{Precision, Ty};
use taos_query::{Fetchable, Queryable, RawBlock as Block};
use taos_ws::query::Error;
use taos_ws::{Offset, Taos};
use tracing::trace;

use crate::native::error::TaosError;
use crate::native::query::{TAOS_FIELD, TAOS_ROW};
use crate::native::{ResultSet, ResultSetOperations, TaosResult, TAOS};

#[derive(Debug)]
pub struct ROW {
    data: Vec<*const c_void>,
    current_row: usize,
}

#[derive(Debug)]
pub struct QueryResultSet {
    rs: taos_ws::ResultSet,
    block: Option<Block>,
    fields: Vec<TAOS_FIELD>,
    row: ROW,
}

impl QueryResultSet {
    fn new(rs: taos_ws::ResultSet) -> Self {
        let num_of_fields = rs.num_of_fields();
        Self {
            rs,
            block: None,
            fields: Vec::new(),
            row: ROW {
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

    fn get_fields(&mut self) -> *const TAOS_FIELD {
        if self.fields.len() != self.rs.num_of_fields() {
            self.fields.clear();
            self.fields
                .extend(self.rs.fields().iter().map(TAOS_FIELD::from));
        }
        self.fields.as_ptr()
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error> {
        trace!(ptr=?ptr, rows=?rows, "fetch block");
        self.block = self.rs.fetch_raw_block()?;
        if let Some(block) = self.block.as_ref() {
            *ptr = block.as_raw_bytes().as_ptr() as _;
            *rows = block.nrows() as _;
        } else {
            *rows = 0;
        }
        trace!(ptr=?ptr, rows=*rows, "fetch block done");
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
        trace!("try to get raw value at ({row}, {col})");
        match self.block.as_ref() {
            Some(block) => {
                if row < block.nrows() && col < block.ncols() {
                    let res = block.get_raw_value_unchecked(row, col);
                    trace!(res=?res, "get raw value at ({row}, {col})");
                    res
                } else {
                    trace!("out of range at ({row}, {col}), return null");
                    (Ty::Null, 0, ptr::null())
                }
            }
            None => (Ty::Null, 0, ptr::null()),
        }
    }

    fn take_timing(&mut self) -> Duration {
        self.rs.take_timing()
    }

    fn stop_query(&mut self) {
        taos_query::block_in_place_or_global(self.rs.stop());
    }
}

pub unsafe fn query(taos: *mut TAOS, sql: *const c_char, req_id: u64) -> TaosResult<ResultSet> {
    let client = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "client pointer is null"))?;
    let sql = CStr::from_ptr(sql).to_str()?;
    let rs = client.query_with_req_id(sql, req_id)?;
    Ok(ResultSet::Query(QueryResultSet::new(rs)))
}
