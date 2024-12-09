use std::ffi::c_char;

use crate::{TAOS, TAOS_RES};

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char, // todo
    numLines: i32,
    protocol: i32,
    precision: i32,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char, // todo
    numLines: i32,
    protocol: i32,
    precision: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert_raw(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: i32,
    totalRows: *mut i32,
    protocol: i32,
    precision: i32,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert_raw_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: i32,
    totalRows: *mut i32,
    protocol: i32,
    precision: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert_ttl(
    taos: *mut TAOS,
    lines: *mut *mut c_char, // todo
    numLines: i32,
    protocol: i32,
    precision: i32,
    ttl: i32,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char, // todo
    numLines: i32,
    protocol: i32,
    precision: i32,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert_raw_ttl(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: i32,
    totalRows: *mut i32,
    protocol: i32,
    precision: i32,
    ttl: i32,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: i32,
    totalRows: *mut i32,
    protocol: i32,
    precision: i32,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: i32,
    totalRows: *mut i32,
    protocol: i32,
    precision: i32,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    todo!();
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_schemaless_insert_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut *mut c_char, // todo
    numLines: i32,
    protocol: i32,
    precision: i32,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    todo!();
}
