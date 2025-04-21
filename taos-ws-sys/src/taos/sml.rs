use std::ffi::{c_char, c_int};

use crate::taos::{driver, CAPI, TAOS, TAOS_RES};
use crate::ws::sml;

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_PROTOCOL_TYPE {
    TSDB_SML_UNKNOWN_PROTOCOL,
    TSDB_SML_LINE_PROTOCOL,
    TSDB_SML_TELNET_PROTOCOL,
    TSDB_SML_JSON_PROTOCOL,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_TIMESTAMP_TYPE {
    TSDB_SML_TIMESTAMP_NOT_CONFIGURED,
    TSDB_SML_TIMESTAMP_HOURS,
    TSDB_SML_TIMESTAMP_MINUTES,
    TSDB_SML_TIMESTAMP_SECONDS,
    TSDB_SML_TIMESTAMP_MILLI_SECONDS,
    TSDB_SML_TIMESTAMP_MICRO_SECONDS,
    TSDB_SML_TIMESTAMP_NANO_SECONDS,
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_schemaless_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    if driver() {
        sml::taos_schemaless_insert(taos, lines, numLines, protocol, precision)
    } else {
        (CAPI.sml_api.taos_schemaless_insert)(taos, lines, numLines, protocol, precision)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_schemaless_insert_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    if driver() {
        sml::taos_schemaless_insert_with_reqid(taos, lines, numLines, protocol, precision, reqid)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_with_reqid)(
            taos, lines, numLines, protocol, precision, reqid,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_schemaless_insert_raw(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    if driver() {
        sml::taos_schemaless_insert_raw(taos, lines, len, totalRows, protocol, precision)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw)(taos, lines, len, totalRows, protocol, precision)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_schemaless_insert_raw_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    if driver() {
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
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_schemaless_insert_ttl(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    if driver() {
        sml::taos_schemaless_insert_ttl(taos, lines, numLines, protocol, precision, ttl)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_ttl)(taos, lines, numLines, protocol, precision, ttl)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_schemaless_insert_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    if driver() {
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
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    if driver() {
        sml::taos_schemaless_insert_raw_ttl(taos, lines, len, totalRows, protocol, precision, ttl)
    } else {
        (CAPI.sml_api.taos_schemaless_insert_raw_ttl)(
            taos, lines, len, totalRows, protocol, precision, ttl,
        )
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
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
    if driver() {
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
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
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
    if driver() {
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
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
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
    if driver() {
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

#[cfg(test)]
mod tests {
    use std::ptr;

    use taos_query::util::generate_req_id;

    use super::*;
    use crate::taos::query::taos_free_result;
    use crate::taos::sml::{TSDB_SML_PROTOCOL_TYPE, TSDB_SML_TIMESTAMP_TYPE};
    use crate::taos::{taos_close, test_connect, test_exec, test_exec_many};

    #[test]
    fn test_sml_insert_raw() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737291333",
                    "create database test_1737291333",
                    "use test_1737291333",
                ],
            );

            let data = c"measurement,host=host1 field1=2i,field2=2.0 1741153642000";
            let len = data.to_bytes().len() as i32;
            let lines = data.as_ptr() as *mut _;
            let total_rows = ptr::null_mut();
            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;
            let ttl = 0;

            let res = taos_schemaless_insert_raw(taos, lines, len, total_rows, protocol, precision);
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_raw_with_reqid(
                taos, lines, len, total_rows, protocol, precision, req_id,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let res = taos_schemaless_insert_raw_ttl(
                taos, lines, len, total_rows, protocol, precision, ttl,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_raw_ttl_with_reqid(
                taos, lines, len, total_rows, protocol, precision, ttl, req_id,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let tbname_key = c"host".as_ptr() as *mut _;
            let res = taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
                taos, lines, len, total_rows, protocol, precision, ttl, req_id, tbname_key,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
                taos,
                lines,
                len,
                total_rows,
                protocol,
                precision,
                ttl,
                req_id,
                ptr::null_mut(),
            );
            assert!(!res.is_null());
            taos_free_result(res);

            test_exec(taos, "drop database test_1737291333");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741168851",
                    "create database test_1741168851",
                    "use test_1741168851",
                ],
            );

            let data = "measurement field1=2i,field2=2.0 1741153642000\n#measurement field1=2i,field2=2.0 1741153643000\nmeasurement field1=2i,field2=2.0 1741153644000\n#comment";
            let data = data.as_bytes();
            let len = data.len() as i32;
            let lines = data.as_ptr() as *mut _;
            let mut total_rows = 0;
            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;

            let res =
                taos_schemaless_insert_raw(taos, lines, len, &mut total_rows, protocol, precision);
            assert!(!res.is_null());
            assert_eq!(total_rows, 2);
            taos_free_result(res);

            test_exec(taos, "drop database test_1741168851");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741608340",
                    "create database test_1741608340",
                    "use test_1741608340",
                ],
            );

            let data = r#"[{"metric":"metric_json","timestamp":1626846400,"value":10.3,"tags":{"groupid":2,"location":"California.SanFrancisco","id":"d1001"}}]"#;

            let data = data.as_bytes();
            let len = data.len() as i32;
            let lines = data.as_ptr() as *mut _;
            let mut total_rows = 0;
            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_JSON_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;

            let res =
                taos_schemaless_insert_raw(taos, lines, len, &mut total_rows, protocol, precision);
            assert!(!res.is_null());
            assert_eq!(total_rows, 1);
            taos_free_result(res);

            test_exec(taos, "drop database test_1741608340");
            taos_close(taos);
        }
    }

    #[test]
    fn test_sml_insert() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741166212",
                    "create database test_1741166212",
                    "use test_1741166212",
                ],
            );

            let line1 = c"measurement,host=host1 field1=2i,field2=2.0 1741153642000";
            let line2 = c"measurement,host=host2 field1=3i,field2=3.0 1741153643000";
            let mut lines = vec![line1.as_ptr() as _, line2.as_ptr() as _];

            let num_lines = lines.len() as i32;
            let lines = lines.as_mut_ptr();

            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;
            let ttl = 20;

            let res = taos_schemaless_insert(taos, lines, num_lines, protocol, precision);
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_with_reqid(
                taos, lines, num_lines, protocol, precision, req_id,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let res = taos_schemaless_insert_ttl(taos, lines, num_lines, protocol, precision, ttl);
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_ttl_with_reqid(
                taos, lines, num_lines, protocol, precision, ttl, req_id,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let tbname_key = c"host".as_ptr() as *mut _;
            let res = taos_schemaless_insert_ttl_with_reqid_tbname_key(
                taos, lines, num_lines, protocol, precision, ttl, req_id, tbname_key,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_ttl_with_reqid_tbname_key(
                taos,
                lines,
                num_lines,
                protocol,
                precision,
                ttl,
                req_id,
                ptr::null_mut(),
            );
            assert!(!res.is_null());
            taos_free_result(res);

            test_exec(taos, "drop database test_1741166212");
            taos_close(taos);
        }
    }

    #[test]
    fn test_sml_telnet() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1742376152",
                    "create database test_1742376152",
                    "use test_1742376152",
                ],
            );

            let line1 = cr#"stb13 1742375795074 L"wuxX"  t0=-12859061i32 t1=-876196531i64 t2=783043840.000000f32 t3=851331526.930689f64 t4=-5047i16 t5=102i8 t6=false t7=L"QSfm7v""#;
            let line2 = cr#"stb13 1742375795075 L""  t0=-12859061i32 t1=-876196531i64 t2=783043840.000000f32 t3=851331526.930689f64 t4=-5047i16 t5=102i8 t6=false t7=L"QSfm7v""#;
            let mut lines = vec![line1.as_ptr() as _, line2.as_ptr() as _];

            let num_lines = lines.len() as i32;
            let lines = lines.as_mut_ptr();

            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_TELNET_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;

            let res = taos_schemaless_insert(taos, lines, num_lines, protocol, precision);
            assert!(!res.is_null());
            taos_free_result(res);

            test_exec(taos, "drop database test_1742376152");
            taos_close(taos);
        }
    }
}
