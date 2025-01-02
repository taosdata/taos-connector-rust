use std::ffi::{c_char, c_void};
use std::time::Duration;

use taos_error::Error;
use taos_query::common::{Field, Precision, Ty};
use taos_query::{Fetchable, RawBlock as Block};
use taos_ws::{Offset, ResultSet};

use super::{TAOS_FIELD, TAOS_ROW};
use crate::native::result_set::ResultSetTrait;

impl From<&Field> for TAOS_FIELD {
    fn from(field: &Field) -> Self {
        let f_name = field.name();
        let mut name = [0 as c_char; 65usize];
        unsafe {
            std::ptr::copy_nonoverlapping(f_name.as_ptr(), name.as_mut_ptr() as _, f_name.len());
        };
        Self {
            name,
            r#type: field.ty() as i8,
            bytes: field.bytes() as i32,
        }
    }
}

#[derive(Debug)]
pub struct ROW {
    /// Data is used to hold the row data
    data: Vec<*const c_void>,
    /// Current row to get
    current_row: usize,
}

#[derive(Debug)]
pub struct WsSqlResultSet {
    rs: ResultSet,
    block: Option<Block>,
    fields: Vec<TAOS_FIELD>,
    row: ROW,
}

impl WsSqlResultSet {
    fn new(rs: ResultSet) -> Self {
        let num_of_fields = rs.num_of_fields();
        let mut data_vec = Vec::with_capacity(num_of_fields);
        for _col in 0..num_of_fields {
            data_vec.push(std::ptr::null());
        }

        Self {
            rs,
            block: None,
            fields: Vec::new(),
            // fields_v2: Vec::new(),
            row: ROW {
                data: data_vec,
                current_row: 0,
            },
        }
    }
}

impl ResultSetTrait for WsSqlResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        std::ptr::null()
    }
    fn tmq_get_db_name(&self) -> *const c_char {
        std::ptr::null()
    }
    fn tmq_get_table_name(&self) -> *const c_char {
        std::ptr::null()
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
        tracing::trace!("fetch block with ptr {ptr:p}");
        self.block = self.rs.fetch_raw_block()?;
        if let Some(block) = self.block.as_ref() {
            *ptr = block.as_raw_bytes().as_ptr() as _;
            *rows = block.nrows() as _;
        } else {
            *rows = 0;
        }
        tracing::trace!("fetch block with ptr {ptr:p} with rows {}", *rows);
        Ok(())
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        if self.block.is_none() || self.row.current_row >= self.block.as_ref().unwrap().nrows() {
            self.block = self.rs.fetch_raw_block()?;
            self.row.current_row = 0;
        }

        if let Some(block) = self.block.as_ref() {
            if block.nrows() == 0 {
                return Ok(std::ptr::null_mut());
            }

            for col in 0..block.ncols() {
                let tuple = block.get_raw_value_unchecked(self.row.current_row, col);
                self.row.data[col] = tuple.2;
            }

            self.row.current_row += 1;
            Ok(self.row.data.as_ptr() as _)
        } else {
            Ok(std::ptr::null_mut())
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        tracing::trace!("try to get raw value at ({row}, {col})");
        match self.block.as_ref() {
            Some(block) => {
                if row < block.nrows() && col < block.ncols() {
                    let res = block.get_raw_value_unchecked(row, col);
                    tracing::trace!("got raw value at ({row}, {col}): {:?}", res);
                    res
                } else {
                    tracing::trace!("out of range at ({row}, {col}), return null");
                    (Ty::Null, 0, std::ptr::null())
                }
            }
            None => (Ty::Null, 0, std::ptr::null()),
        }
    }

    fn take_timing(&mut self) -> Duration {
        self.rs.take_timing()
    }

    fn stop_query(&mut self) {
        taos_query::block_in_place_or_global(self.rs.stop());
    }
}
