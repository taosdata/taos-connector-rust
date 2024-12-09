use std::ffi::{c_char, c_ulong, c_void};

#[repr(C)]
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
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct setConfRet {
    pub retCode: SET_CONF_RET_CODE,
    pub retMsg: [c_char; RET_MSG_LENGTH],
}

#[no_mangle]
pub extern "C" fn taos_set_config(_config: *const c_char) -> setConfRet {
    todo!("taos_set_config");
}

pub type TAOS = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_STMT2 = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_RES = c_void;

#[allow(non_camel_case_types)]
pub type __taos_async_fn_t = extern "C" fn(param: *mut c_void, res: *mut TAOS_RES, code: i32);

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_OPTION {
    pub reqid: i64,
    pub singleStbInsert: bool,
    pub singleTableBindOnce: bool,
    pub asyncExecFn: __taos_async_fn_t,
    pub userdata: *mut c_void,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BIND {
    pub buffer_type: i32,
    pub buffer: *mut c_void,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: i32,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BINDV {
    pub count: i32,
    pub tbnames: *mut *mut c_char,
    pub tags: *mut *mut TAOS_STMT2_BIND,
    pub bind_cols: *mut *mut TAOS_STMT2_BIND,
}

#[no_mangle]
pub extern "C" fn taos_stmt2_init(
    _taos: *mut TAOS,
    _option: *mut TAOS_STMT2_OPTION,
) -> *mut TAOS_STMT2 {
    todo!("taos_stmt2_init");
}

#[no_mangle]
pub extern "C" fn taos_stmt2_prepare(
    _stmt: *mut TAOS_STMT2,
    _sql: *const c_char,
    _length: c_ulong,
) -> i32 {
    todo!("taos_stmt2_prepare");
}

#[no_mangle]
pub extern "C" fn taos_stmt2_bind_param(
    _stmt: *mut TAOS_STMT2,
    _bindv: *mut TAOS_STMT2_BINDV,
    _col_idx: i32,
) -> i32 {
    todo!("taos_stmt2_bind_param");
}

#[no_mangle]
pub extern "C" fn taos_stmt2_exec(_stmt: *mut TAOS_STMT2, _affected_rows: *mut i32) -> i32 {
    todo!("taos_stmt2_exec");
}

#[no_mangle]
pub extern "C" fn taos_stmt2_close(_stmt: *mut TAOS_STMT2) -> i32 {
    todo!("taos_stmt2_close");
}

#[no_mangle]
pub extern "C" fn taos_stmt2_is_insert(_stmt: *mut TAOS_STMT2, _insert: *mut i32) -> i32 {
    todo!("taos_stmt2_is_insert");
}

#[no_mangle]
pub extern "C" fn taos_stmt2_result(_stmt: *mut TAOS_STMT2) -> *mut TAOS_RES {
    todo!("taos_stmt2_result");
}

#[no_mangle]
pub extern "C" fn taos_stmt2_error(_stmt: *mut TAOS_STMT2) -> *const c_char {
    todo!("taos_stmt2_error");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_taos_set_config() {
        let config = c"config";
        let _ = taos_set_config(config.as_ptr());
    }

    #[test]
    #[ignore]
    fn test_taos_stmt2_init() {
        let taos = std::ptr::null_mut();
        extern "C" fn async_exec_fn(_param: *mut c_void, _res: *mut TAOS_RES, _code: i32) {}
        let mut option = TAOS_STMT2_OPTION {
            reqid: 0,
            singleStbInsert: false,
            singleTableBindOnce: false,
            asyncExecFn: async_exec_fn,
            userdata: std::ptr::null_mut(),
        };
        let _ = taos_stmt2_init(taos, &mut option);
    }
}
