use std::ffi::{c_char, c_int};
use std::sync::atomic::Ordering;

use tracing::instrument;

use crate::taos::{CAPI, DRIVER, TAOS, TAOS_RES};
use crate::ws::sml;

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_PROTOCOL_TYPE {
    TSDB_SML_UNKNOWN_PROTOCOL = 0,
    TSDB_SML_LINE_PROTOCOL,
    TSDB_SML_TELNET_PROTOCOL,
    TSDB_SML_JSON_PROTOCOL,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_TIMESTAMP_TYPE {
    TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,
    TSDB_SML_TIMESTAMP_HOURS,
    TSDB_SML_TIMESTAMP_MINUTES,
    TSDB_SML_TIMESTAMP_SECONDS,
    TSDB_SML_TIMESTAMP_MILLI_SECONDS,
    TSDB_SML_TIMESTAMP_MICRO_SECONDS,
    TSDB_SML_TIMESTAMP_NANO_SECONDS,
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert(taos, lines, numLines, protocol, precision)
    } else {
        (CAPI.sml_api.taos_schemaless_insert)(taos, lines, numLines, protocol, precision)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert_with_reqid(taos, lines, numLines, protocol, precision, reqid)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_with_reqid)(
            taos, lines, numLines, protocol, precision, reqid,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert_raw(taos, lines, len, totalRows, protocol, precision)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw)(taos, lines, len, totalRows, protocol, precision)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert_raw_with_reqid(
            taos, lines, len, totalRows, protocol, precision, reqid,
        )
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw_with_reqid)(
            taos, lines, len, totalRows, protocol, precision, reqid,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_ttl(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert_ttl(taos, lines, numLines, protocol, precision, ttl)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_ttl)(taos, lines, numLines, protocol, precision, ttl)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert_ttl_with_reqid(
            taos, lines, numLines, protocol, precision, ttl, reqid,
        )
    } else {
        (CAPI.sml_api.taos_schemaless_insert_ttl_with_reqid)(
            taos, lines, numLines, protocol, precision, ttl, reqid,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert_raw_ttl(taos, lines, len, totalRows, protocol, precision, ttl)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw_ttl)(
            taos, lines, len, totalRows, protocol, precision, ttl,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert_raw_ttl_with_reqid(
            taos, lines, len, totalRows, protocol, precision, ttl, reqid,
        )
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw_ttl_with_reqid)(
            taos, lines, len, totalRows, protocol, precision, ttl, reqid,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
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
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
            taos, lines, len, totalRows, protocol, precision, ttl, reqid, tbnameKey,
        )
    } else {
        (CAPI
            .sml_api
            .taos_schemaless_insert_raw_ttl_with_reqid_tbname_key)(
            taos, lines, len, totalRows, protocol, precision, ttl, reqid, tbnameKey,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_schemaless_insert_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    if DRIVER.load(Ordering::Relaxed) {
        sml::taos_schemaless_insert_ttl_with_reqid_tbname_key(
            taos, lines, numLines, protocol, precision, ttl, reqid, tbnameKey,
        )
    } else {
        (CAPI
            .sml_api
            .taos_schemaless_insert_ttl_with_reqid_tbname_key)(
            taos, lines, numLines, protocol, precision, ttl, reqid, tbnameKey,
        )
    }
}
