use std::{ffi::CStr, os::raw::*};

use cfg_if::cfg_if;
use taos_query::common::{raw_data_t, SmlData};
use taos_query::prelude::{Code, RawError as Error};
use taos_query::RawBlock;

use crate::schemaless::*;
use crate::tmq::*;
use crate::{err_or, into_c_str::IntoCStr, query::QueryFuture};
use crate::{ffi::*, tmq::ffi::tmq_write_raw, RawRes, ResultSet};

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct RawTaos(*mut TAOS);

unsafe impl Send for RawTaos {}
unsafe impl Sync for RawTaos {}

impl RawTaos {
    /// Client version.
    pub fn version() -> &'static str {
        unsafe {
            CStr::from_ptr(taos_get_client_info())
                .to_str()
                .expect("client version should always be valid utf-8 str")
        }
    }

    #[inline]
    pub fn connect(
        host: *const c_char,
        user: *const c_char,
        pass: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> Result<Self, Error> {
        let ptr = unsafe { taos_connect(host, user, pass, db, port) };
        log::trace!("call taos_connect: {ptr:?}");
        let null = std::ptr::null_mut();
        let code = unsafe { taos_errno(null) };
        if code != 0 {
            let err = unsafe { CStr::from_ptr(taos_errstr(null)) }
                .to_string_lossy()
                .to_string();
            let err = Error::new(code, err);
        }

        if ptr.is_null() {
            let null = std::ptr::null_mut();
            let code = unsafe { taos_errno(null) };
            let err = unsafe { CStr::from_ptr(taos_errstr(null)) }
                .to_string_lossy()
                .to_string();
            log::trace!("error: {err}");

            Err(Error::new(code, err))
        } else {
            Ok(RawTaos(ptr))
        }
    }
    #[inline]
    pub fn connect_auth(
        host: *const c_char,
        user: *const c_char,
        auth: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> Result<Self, Error> {
        let ptr = unsafe { taos_connect_auth(host, user, auth, db, port) };
        if ptr.is_null() {
            let null = std::ptr::null_mut();
            let code = unsafe { taos_errno(null) };
            let err = unsafe { CStr::from_ptr(taos_errstr(null)) }
                .to_string_lossy()
                .to_string();
            Err(Error::new(code, err))
        } else {
            Ok(RawTaos(ptr))
        }
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut TAOS {
        self.0
    }

    #[inline]
    pub fn query<'a, S: IntoCStr<'a>>(&self, sql: S) -> Result<ResultSet, Error> {
        let sql = sql.into_c_str();
        log::trace!("query with sql: {}", sql.to_str().unwrap_or("<...>"));
        RawRes::from_ptr(unsafe { taos_query(self.as_ptr(), sql.as_ptr()) }).map(ResultSet::new)
    }

    #[inline]
    pub fn query_with_req_id<'a, S: IntoCStr<'a>>(&self, sql: S, req_id: u64) -> Result<ResultSet, Error> {
        let sql = sql.into_c_str();
        log::trace!("query with sql: {}", sql.to_str().unwrap_or("<...>"));
        #[cfg(taos_req_id)]
        return RawRes::from_ptr(
            unsafe {
                taos_query_with_reqid(
                    self.as_ptr(),
                    sql.as_ptr(),
                    req_id
                )
            }
        )
            .map(ResultSet::new);
        #[cfg(not(taos_req_id))]
        RawRes::from_ptr(
            unsafe {
                taos_query(
                    self.as_ptr(),
                    sql.as_ptr()
                )
            }
        )
            .map(ResultSet::new)
    }

    #[inline]
    pub fn query_async<'a, S: IntoCStr<'a>>(&'a self, sql: S) -> QueryFuture<'a> {
        QueryFuture::new(*self, sql)
    }

    #[inline]
    pub fn query_a<'a, S: IntoCStr<'a>>(
        &self,
        sql: S,
        fp: taos_async_query_cb,
        param: *mut c_void,
    ) {
        unsafe { taos_query_a(self.as_ptr(), sql.into_c_str().as_ptr(), fp, param) }
    }

    #[inline]
    pub fn validate_sql(self, sql: *const c_char) -> Result<(), Error> {
        let code: Code = unsafe { taos_validate_sql(self.as_ptr(), sql) }.into();
        if code.success() {
            Ok(())
        } else {
            let err = unsafe { taos_errstr(std::ptr::null_mut()) };
            let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
            Err(Error::new(code, err))
        }
    }

    #[inline]
    pub fn reset_current_db(&self) {
        unsafe { taos_reset_current_db(self.as_ptr()) }
    }

    #[inline]
    pub fn server_version(&self) -> &CStr {
        unsafe { CStr::from_ptr(taos_get_server_info(self.as_ptr())) }
    }

    #[inline]
    pub fn load_table_info(&self, list: *const c_char) -> Result<(), Error> {
        err_or!(taos_load_table_info(self.as_ptr(), list))
    }

    #[inline]
    pub fn write_raw_meta(&self, meta: raw_data_t) -> Result<(), Error> {
        // try 5 times if write_raw_meta fails with 0x2603 error.
        let mut retries = 2;
        loop {
            let code = unsafe { tmq_write_raw(self.as_ptr(), meta) };
            let code = Code::from(code);
            if code.success() {
                return Ok(());
            }
            if code != Code::from(0x2603) {
                log::trace!("received error code {code} when write raw meta");
                let err = unsafe { taos_errstr(std::ptr::null_mut()) };
                let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
                return Err(taos_query::prelude::RawError::new(code, err));
            }
            log::trace!("received error code 0x2603, try once");
            retries -= 1;
            if retries == 0 {
                let err = unsafe { taos_errstr(std::ptr::null_mut()) };
                let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
                return Err(taos_query::prelude::RawError::new(code, err));
            }
        }
    }

    #[inline]
    pub fn write_raw_block(&self, block: &RawBlock) -> Result<(), Error> {
        use itertools::Itertools;
        let nrows = block.nrows();
        let name = block
            .table_name()
            .ok_or_else(|| Error::new(Code::Failed, "raw block should have table name"))?;
        let ptr = block.as_raw_bytes().as_ptr();
        // block;
        cfg_if! {
            if #[cfg(taos_write_raw_block_with_fields)] {

                let fields = block
                    .fields()
                    .into_iter()
                    .map(|field| field.into())
                    .collect_vec();

                err_or!(self, taos_write_raw_block_with_fields(
                    self.as_ptr(),
                    nrows as _,
                    ptr as _,
                    name.into_c_str().as_ptr(),
                    fields.as_ptr(),
                    fields.len() as _,
                ))
            } else {
                err_or!(self, taos_write_raw_block(
                    self.as_ptr(),
                    nrows as _,
                    ptr as _,
                    name.into_c_str().as_ptr()
                ))
            }
        }
    }

    #[inline]
    pub fn err_as_str(&self) -> &'static str {
        unsafe {
            std::str::from_utf8_unchecked(
                CStr::from_ptr(taos_errstr(std::ptr::null_mut())).to_bytes(),
            )
        }
    }

    #[inline]
    pub fn put(&self, sml: &SmlData) -> Result<(), Error> {

        let data = sml.data().join("\n").to_string();
        log::trace!("sml insert with data: {}", data.clone());
        let length = data.clone().len() as i32;
        let mut total_rows: i32 = 0;
        let res;
        
        if sml.req_id().is_some() && sml.ttl().is_some() {
            log::debug!("sml insert with req_id: {} and ttl {}", sml.req_id().unwrap(), sml.ttl().unwrap());
            res = RawRes::from_ptr(unsafe { 
                taos_schemaless_insert_raw_ttl_with_reqid(
                    self.as_ptr(), 
                    data.into_c_str().as_ptr(),
                    length,
                    &mut total_rows,
                    std::mem::transmute(sml.protocol()),
                    std::mem::transmute(sml.precision()),
                    sml.ttl().unwrap(),
                    sml.req_id().unwrap(),
                )
            });
        } else if sml.req_id().is_some() {
            log::debug!("sml insert with req_id: {}", sml.req_id().unwrap());
            res = RawRes::from_ptr(unsafe { 
                taos_schemaless_insert_raw_with_reqid(
                    self.as_ptr(), 
                    data.into_c_str().as_ptr(),
                    length,
                    &mut total_rows,
                    std::mem::transmute(sml.protocol()),
                    std::mem::transmute(sml.precision()),
                    sml.req_id().unwrap(),
                )
            });
        } else if sml.ttl().is_some() {
            log::debug!("sml insert with ttl: {}", sml.ttl().unwrap());
            res = RawRes::from_ptr(unsafe { 
                taos_schemaless_insert_raw_ttl(
                    self.as_ptr(), 
                    data.into_c_str().as_ptr(),
                    length,
                    &mut total_rows,
                    std::mem::transmute(sml.protocol()),
                    std::mem::transmute(sml.precision()),
                    sml.ttl().unwrap(),
                )
            });
        } else {
            log::debug!("sml insert without req_id and ttl");
            res = RawRes::from_ptr(unsafe { 
                taos_schemaless_insert_raw(
                    self.as_ptr(), 
                    data.into_c_str().as_ptr(),
                    length,
                    &mut total_rows,
                    std::mem::transmute(sml.protocol()),
                    std::mem::transmute(sml.precision()),
                )
            });
        }
        
        
        log::trace!("sml total rows: {}", total_rows);
        match res {
            Ok(_) => {
                log::trace!("sml insert success");
                Ok(())
            }
            Err(e) => {
                log::trace!("sml insert failed: {:?}", e);
                return Err(e);
            }
        }
    }

    #[inline]
    pub fn close(&mut self) {
        unsafe { taos_close(self.as_ptr()) }
    }
}
