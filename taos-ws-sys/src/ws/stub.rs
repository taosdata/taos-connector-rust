#![allow(unused_variables)]

use std::ffi::{c_char, c_int, c_void};
use std::ptr;

use crate::taos::query::{
    __taos_async_whitelist_fn_t, __taos_notify_fn_t, setConfRet, RET_MSG_LENGTH, SET_CONF_RET_CODE,
    TAOS_DB_ROUTE_INFO,
};
use crate::taos::tmq::{tmq_raw_data, tmq_t};
use crate::taos::{TAOS, TAOS_FIELD, TAOS_RES, TSDB_OPTION_CONNECTION};

pub fn taos_cleanup() {}

pub fn taos_connect_auth(
    ip: *const c_char,
    user: *const c_char,
    auth: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    ptr::null_mut()
}

pub fn taos_kill_query(taos: *mut TAOS) {}

pub fn taos_reset_current_db(taos: *mut TAOS) {}

#[allow(non_snake_case)]
pub fn taos_get_db_route_info(
    taos: *mut TAOS,
    db: *const c_char,
    dbInfo: *mut TAOS_DB_ROUTE_INFO,
) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_get_table_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *const c_char,
    vgId: *mut c_int,
) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_get_tables_vgId(
    taos: *mut TAOS,
    db: *const c_char,
    table: *mut *const c_char,
    tableNum: c_int,
    vgId: *mut c_int,
) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_load_table_info(taos: *mut TAOS, tableNameList: *const c_char) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_set_hb_quit(quitByKill: i8) {}

pub fn taos_set_notify_cb(
    taos: *mut TAOS,
    fp: __taos_notify_fn_t,
    param: *mut c_void,
    r#type: c_int,
) -> c_int {
    0
}

pub fn taos_set_conn_mode(taos: *mut TAOS, mode: c_int, value: c_int) -> c_int {
    0
}

pub fn tmq_get_connect(tmq: *mut tmq_t) -> *mut TAOS {
    ptr::null_mut()
}

pub fn taos_set_config(config: *const c_char) -> setConfRet {
    setConfRet {
        retCode: SET_CONF_RET_CODE::SET_CONF_RET_SUCC,
        retMsg: [0; RET_MSG_LENGTH],
    }
}

pub fn taos_fetch_whitelist_a(
    taos: *mut TAOS,
    fp: __taos_async_whitelist_fn_t,
    param: *mut c_void,
) {
}

pub fn tmq_get_raw(res: *mut TAOS_RES, raw: *mut tmq_raw_data) -> i32 {
    0
}

pub fn tmq_write_raw(taos: *mut TAOS, raw: tmq_raw_data) -> i32 {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
) -> i32 {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block_with_reqid(
    taos: *mut TAOS,
    numOfRows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    reqid: i64,
) -> i32 {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block_with_fields(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
) -> i32 {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block_with_fields_with_reqid(
    taos: *mut TAOS,
    rows: i32,
    pData: *mut c_char,
    tbname: *const c_char,
    fields: *mut TAOS_FIELD,
    numFields: i32,
    reqid: i64,
) -> i32 {
    0
}

pub fn tmq_free_raw(raw: tmq_raw_data) {}

pub fn tmq_get_json_meta(res: *mut TAOS_RES) -> *const c_char {
    ptr::null()
}

#[allow(non_snake_case)]
pub fn tmq_free_json_meta(jsonMeta: *mut c_char) {}

#[allow(non_snake_case)]
pub fn getBuildInfo() -> *const c_char {
    ptr::null()
}

pub fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_crashinfo(signum: c_int, sigInfo: *mut c_void, context: *mut c_void) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stub() {
        taos_cleanup();

        let taos = taos_connect_auth(ptr::null(), ptr::null(), ptr::null(), ptr::null(), 0);
        assert_eq!(taos, ptr::null_mut());

        taos_kill_query(ptr::null_mut());

        taos_reset_current_db(ptr::null_mut());

        let code = taos_get_db_route_info(ptr::null_mut(), ptr::null(), ptr::null_mut());
        assert_eq!(code, 0);

        let code = taos_get_table_vgId(
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null(),
            ptr::null_mut(),
        );
        assert_eq!(code, 0);

        let code = taos_get_tables_vgId(
            ptr::null_mut(),
            ptr::null(),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
        );
        assert_eq!(code, 0);

        let code = taos_load_table_info(ptr::null_mut(), ptr::null());
        assert_eq!(code, 0);

        taos_set_hb_quit(0);

        extern "C" fn taos_set_notify_cb_cb(param: *mut c_void, ext: *mut c_void, r#type: c_int) {}

        taos_set_notify_cb_cb(ptr::null_mut(), ptr::null_mut(), 0);

        let code = taos_set_notify_cb(ptr::null_mut(), taos_set_notify_cb_cb, ptr::null_mut(), 0);
        assert_eq!(code, 0);

        let code = taos_set_conn_mode(ptr::null_mut(), 0, 0);
        assert_eq!(code, 0);

        let conn = tmq_get_connect(ptr::null_mut());
        assert_eq!(conn, ptr::null_mut());

        let res = taos_set_config(ptr::null());
        assert_eq!(res.retCode, SET_CONF_RET_CODE::SET_CONF_RET_SUCC);
        assert_eq!(res.retMsg, [0; RET_MSG_LENGTH]);

        #[allow(non_snake_case)]
        extern "C" fn taos_fetch_whitelist_a_cb(
            param: *mut c_void,
            code: i32,
            taos: *mut TAOS,
            numOfWhiteLists: i32,
            pWhiteLists: *mut u64,
        ) {
        }

        taos_fetch_whitelist_a_cb(ptr::null_mut(), 0, ptr::null_mut(), 0, ptr::null_mut());

        taos_fetch_whitelist_a(ptr::null_mut(), taos_fetch_whitelist_a_cb, ptr::null_mut());

        let code = tmq_get_raw(ptr::null_mut(), ptr::null_mut());
        assert_eq!(code, 0);

        let tmq_raw_data = tmq_raw_data {
            raw: ptr::null_mut(),
            raw_len: 0,
            raw_type: 0,
        };

        let code = tmq_write_raw(ptr::null_mut(), tmq_raw_data);
        assert_eq!(code, 0);

        let code = taos_write_raw_block(ptr::null_mut(), 0, ptr::null_mut(), ptr::null());
        assert_eq!(code, 0);

        let code =
            taos_write_raw_block_with_reqid(ptr::null_mut(), 0, ptr::null_mut(), ptr::null(), 0);
        assert_eq!(code, 0);

        let code = taos_write_raw_block_with_fields(
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            ptr::null(),
            ptr::null_mut(),
            0,
        );
        assert_eq!(code, 0);

        let code = taos_write_raw_block_with_fields_with_reqid(
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            ptr::null(),
            ptr::null_mut(),
            0,
            0,
        );
        assert_eq!(code, 0);

        let tmq_raw_data = tmq_raw_data {
            raw: ptr::null_mut(),
            raw_len: 0,
            raw_type: 0,
        };

        tmq_free_raw(tmq_raw_data);

        let json_meta = tmq_get_json_meta(ptr::null_mut());
        assert_eq!(json_meta, ptr::null());

        tmq_free_json_meta(ptr::null_mut());

        let build_info = getBuildInfo();
        assert_eq!(build_info, ptr::null());

        let code = taos_options_connection(
            ptr::null_mut(),
            TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_CLEAR,
            ptr::null(),
        );
        assert_eq!(code, 0);

        taos_write_crashinfo(0, ptr::null_mut(), ptr::null_mut());
    }
}
