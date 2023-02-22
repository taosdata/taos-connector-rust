use std::{borrow::Cow, ffi::c_void};

use taos_query::prelude::RawError;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub(crate) struct tmq_resp_err_t(i32);

impl PartialEq<i32> for tmq_conf_res_t {
    fn eq(&self, other: &i32) -> bool {
        self == other
    }
}

impl tmq_resp_err_t {
    pub fn ok_or(self, s: impl Into<Cow<'static, str>>) -> Result<(), RawError> {
        match self {
            Self(0) => Ok(()),
            _ => Err(RawError::from_string(s.into())),
        }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_conf_t {
    _unused: [u8; 0],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_list_t {
    _unused: [u8; 0],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_message_t {
    _unused: [u8; 0],
}

#[repr(C)]
pub enum tmq_conf_res_t {
    _Unknown = -2,
    _Invalid = -1,
    _Ok = 0,
}

impl tmq_conf_res_t {
    pub fn ok(self, k: &str, v: &str) -> Result<(), RawError> {
        match self {
            Self::_Ok => Ok(()),
            Self::_Invalid => Err(RawError::from_string(format!(
                "Invalid key value pair ({k}, {v})"
            ))),
            Self::_Unknown => Err(RawError::from_string(format!("Unknown key {k}"))),
        }
    }
}

#[allow(non_camel_case_types)]
pub(crate) type tmq_commit_cb =
    unsafe extern "C" fn(tmq: *mut tmq_t, resp: tmq_resp_err_t, param: *mut c_void);

#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum tmq_res_t {
    _TmqResInvalid = -1,
    _TmqResData = 1,
    _TmqResTableMeta = 2,
    _TmqResMetadata = 3,
}
