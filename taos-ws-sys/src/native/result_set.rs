use std::ffi::{c_char, c_void};
use std::time::Duration;

use taos_error::Error;
use taos_query::common::{Precision, Ty};
use taos_ws::Offset;

use crate::native::query::query::WsSqlResultSet;
use crate::native::query::{TAOS_FIELD, TAOS_ROW};

pub trait ResultSetTrait {
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

    fn get_fields(&mut self) -> *const TAOS_FIELD;

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error>;

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error>;

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void);

    fn take_timing(&mut self) -> Duration;

    fn stop_query(&mut self);
}

#[derive(Debug)]
enum WsResultSet {
    Sql(WsSqlResultSet),
    // Sml,
    // Tmq,
}

impl ResultSetTrait for WsResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_topic_name(),
        }
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_db_name(),
        }
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_table_name(),
        }
    }

    fn tmq_get_offset(&self) -> Offset {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_offset(),
        }
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_vgroup_offset(),
        }
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_vgroup_id(),
        }
    }

    fn precision(&self) -> Precision {
        match self {
            WsResultSet::Sql(rs) => rs.precision(),
        }
    }

    fn affected_rows(&self) -> i32 {
        match self {
            WsResultSet::Sql(rs) => rs.affected_rows(),
        }
    }

    fn affected_rows64(&self) -> i64 {
        match self {
            WsResultSet::Sql(rs) => rs.affected_rows64(),
        }
    }

    fn num_of_fields(&self) -> i32 {
        match self {
            WsResultSet::Sql(rs) => rs.num_of_fields(),
        }
    }

    fn get_fields(&mut self) -> *const TAOS_FIELD {
        match self {
            WsResultSet::Sql(rs) => rs.get_fields(),
        }
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error> {
        match self {
            WsResultSet::Sql(rs) => rs.fetch_block(ptr, rows),
        }
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        match self {
            WsResultSet::Sql(rs) => rs.fetch_row(),
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        match self {
            WsResultSet::Sql(rs) => rs.get_raw_value(row, col),
        }
    }

    fn take_timing(&mut self) -> Duration {
        match self {
            WsResultSet::Sql(rs) => rs.take_timing(),
        }
    }

    fn stop_query(&mut self) {
        match self {
            WsResultSet::Sql(rs) => rs.stop_query(),
        }
    }
}
