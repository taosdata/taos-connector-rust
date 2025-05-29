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
    _ip: *const c_char,
    _user: *const c_char,
    _auth: *const c_char,
    _db: *const c_char,
    _port: u16,
) -> *mut TAOS {
    ptr::null_mut()
}

pub fn taos_kill_query(_taos: *mut TAOS) {}

pub fn taos_reset_current_db(_taos: *mut TAOS) {}

#[allow(non_snake_case)]
pub fn taos_get_db_route_info(
    _taos: *mut TAOS,
    _db: *const c_char,
    _dbInfo: *mut TAOS_DB_ROUTE_INFO,
) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_get_table_vgId(
    _taos: *mut TAOS,
    _db: *const c_char,
    _table: *const c_char,
    _vgId: *mut c_int,
) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_get_tables_vgId(
    _taos: *mut TAOS,
    _db: *const c_char,
    _table: *mut *const c_char,
    _tableNum: c_int,
    _vgId: *mut c_int,
) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_load_table_info(_taos: *mut TAOS, _tableNameList: *const c_char) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_set_hb_quit(_quitByKill: i8) {}

pub fn taos_set_notify_cb(
    _taos: *mut TAOS,
    _fp: __taos_notify_fn_t,
    _param: *mut c_void,
    _type: c_int,
) -> c_int {
    0
}

pub fn taos_set_conn_mode(_taos: *mut TAOS, _mode: c_int, _value: c_int) -> c_int {
    0
}

pub fn tmq_get_connect(_tmq: *mut tmq_t) -> *mut TAOS {
    ptr::null_mut()
}

pub fn taos_set_config(_config: *const c_char) -> setConfRet {
    setConfRet {
        retCode: SET_CONF_RET_CODE::SET_CONF_RET_SUCC,
        retMsg: [0; RET_MSG_LENGTH],
    }
}

pub fn taos_fetch_whitelist_a(
    _taos: *mut TAOS,
    _fp: __taos_async_whitelist_fn_t,
    _param: *mut c_void,
) {
}

pub fn tmq_get_raw(_res: *mut TAOS_RES, _raw: *mut tmq_raw_data) -> i32 {
    0
}

pub fn tmq_write_raw(_taos: *mut TAOS, _raw: tmq_raw_data) -> i32 {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block(
    _taos: *mut TAOS,
    _numOfRows: i32,
    _pData: *mut c_char,
    _tbname: *const c_char,
) -> i32 {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block_with_reqid(
    _taos: *mut TAOS,
    _numOfRows: i32,
    _pData: *mut c_char,
    _tbname: *const c_char,
    _reqid: i64,
) -> i32 {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block_with_fields(
    _taos: *mut TAOS,
    _rows: i32,
    _pData: *mut c_char,
    _tbname: *const c_char,
    _fields: *mut TAOS_FIELD,
    _numFields: i32,
) -> i32 {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_raw_block_with_fields_with_reqid(
    _taos: *mut TAOS,
    _rows: i32,
    _pData: *mut c_char,
    _tbname: *const c_char,
    _fields: *mut TAOS_FIELD,
    _numFields: i32,
    _reqid: i64,
) -> i32 {
    0
}

pub fn tmq_free_raw(_raw: tmq_raw_data) {}

pub fn tmq_get_json_meta(_res: *mut TAOS_RES) -> *const c_char {
    ptr::null()
}

#[allow(non_snake_case)]
pub fn tmq_free_json_meta(_jsonMeta: *mut c_char) {}

#[allow(non_snake_case)]
pub fn getBuildInfo() -> *const c_char {
    ptr::null()
}

pub fn taos_options_connection(
    _taos: *mut TAOS,
    _option: TSDB_OPTION_CONNECTION,
    _arg: *const c_void,
) -> c_int {
    0
}

#[allow(non_snake_case)]
pub fn taos_write_crashinfo(_signum: c_int, _sigInfo: *mut c_void, _context: *mut c_void) {}

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

        extern "C" fn taos_set_notify_cb_cb(_param: *mut c_void, _ext: *mut c_void, _type: c_int) {}

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
            _param: *mut c_void,
            _code: i32,
            _taos: *mut TAOS,
            _numOfWhiteLists: i32,
            _pWhiteLists: *mut u64,
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
