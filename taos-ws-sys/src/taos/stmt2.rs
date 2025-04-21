use std::ffi::{c_char, c_int, c_ulong, c_void};

use crate::taos::{__taos_async_fn_t, driver, CAPI, TAOS, TAOS_RES};
use crate::ws::stmt2;

#[allow(non_camel_case_types)]
pub type TAOS_STMT2 = c_void;

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub struct TAOS_STMT2_OPTION {
    pub reqid: i64,
    pub singleStbInsert: bool,
    pub singleTableBindOnce: bool,
    pub asyncExecFn: __taos_async_fn_t,
    pub userdata: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: c_int,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BINDV {
    pub count: c_int,
    pub tbnames: *mut *mut c_char,
    pub tags: *mut *mut TAOS_STMT2_BIND,
    pub bind_cols: *mut *mut TAOS_STMT2_BIND,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD_ALL {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
    pub field_type: u8,
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_init(
    taos: *mut TAOS,
    option: *mut TAOS_STMT2_OPTION,
) -> *mut TAOS_STMT2 {
    if driver() {
        stmt2::taos_stmt2_init(taos, option)
    } else {
        (CAPI.stmt2_api.taos_stmt2_init)(taos, option)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_prepare(
    stmt: *mut TAOS_STMT2,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_prepare(stmt, sql, length)
    } else {
        (CAPI.stmt2_api.taos_stmt2_prepare)(stmt, sql, length)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_bind_param(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_bind_param(stmt, bindv, col_idx)
    } else {
        (CAPI.stmt2_api.taos_stmt2_bind_param)(stmt, bindv, col_idx)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_bind_param_a(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_bind_param_a(stmt, bindv, col_idx, fp, param)
    } else {
        (CAPI.stmt2_api.taos_stmt2_bind_param_a)(stmt, bindv, col_idx, fp, param)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_exec(
    stmt: *mut TAOS_STMT2,
    affected_rows: *mut c_int,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_exec(stmt, affected_rows)
    } else {
        (CAPI.stmt2_api.taos_stmt2_exec)(stmt, affected_rows)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_close(stmt: *mut TAOS_STMT2) -> c_int {
    if driver() {
        stmt2::taos_stmt2_close(stmt)
    } else {
        (CAPI.stmt2_api.taos_stmt2_close)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_is_insert(stmt: *mut TAOS_STMT2, insert: *mut c_int) -> c_int {
    if driver() {
        stmt2::taos_stmt2_is_insert(stmt, insert)
    } else {
        (CAPI.stmt2_api.taos_stmt2_is_insert)(stmt, insert)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_get_fields(
    stmt: *mut TAOS_STMT2,
    count: *mut c_int,
    fields: *mut *mut TAOS_FIELD_ALL,
) -> c_int {
    if driver() {
        stmt2::taos_stmt2_get_fields(stmt, count, fields)
    } else {
        (CAPI.stmt2_api.taos_stmt2_get_fields)(stmt, count, fields)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_free_fields(
    stmt: *mut TAOS_STMT2,
    fields: *mut TAOS_FIELD_ALL,
) {
    if driver() {
        stmt2::taos_stmt2_free_fields(stmt, fields);
    } else {
        (CAPI.stmt2_api.taos_stmt2_free_fields)(stmt, fields);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_result(stmt: *mut TAOS_STMT2) -> *mut TAOS_RES {
    if driver() {
        stmt2::taos_stmt2_result(stmt)
    } else {
        (CAPI.stmt2_api.taos_stmt2_result)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt2_error(stmt: *mut TAOS_STMT2) -> *mut c_char {
    if driver() {
        stmt2::taos_stmt2_error(stmt)
    } else {
        (CAPI.stmt2_api.taos_stmt2_error)(stmt)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use taos_query::common::Ty;

    use super::*;
    use crate::taos::query::*;
    use crate::taos::{taos_close, test_connect, test_exec, test_exec_many};

    macro_rules! new_bind {
        ($ty:expr, $buffer:ident, $length:ident, $is_null:ident) => {
            TAOS_STMT2_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                length: $length.as_mut_ptr(),
                is_null: $is_null.as_mut_ptr(),
                num: $is_null.len() as _,
            }
        };
    }

    macro_rules! new_bind_without_is_null {
        ($ty:expr, $buffer:ident, $length:ident) => {
            TAOS_STMT2_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                length: $length.as_mut_ptr(),
                is_null: ptr::null_mut(),
                num: $length.len() as _,
            }
        };
    }

    #[test]
    fn test_stmt2() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739274502",
                    "create database test_1739274502",
                    "use test_1739274502",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let len = sql.to_bytes().len();
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), len as _);
            assert_eq!(code, 0);

            let mut buffer = vec![1739763276172i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![2];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 1);

            let mut insert = 0;
            let code = taos_stmt2_is_insert(stmt2, &mut insert);
            assert_eq!(code, 0);
            assert_eq!(insert, 1);

            let mut count = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt2_get_fields(stmt2, &mut count, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(count, 2);
            assert!(!fields.is_null());

            taos_stmt2_free_fields(stmt2, fields);

            let error = taos_stmt2_error(stmt2);
            assert_eq!(CStr::from_ptr(error), c"");

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739274502");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_bind_param() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739502440",
                    "create database test_1739502440",
                    "use test_1739502440",
                    "create table s0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                    c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                    c9 bigint unsigned, c10 float, c11 double, c12 varchar(10), c13 nchar(10), \
                    c14 varbinary(10), c15 geometry(50)) \
                    tags (t1 timestamp, t2 bool, t3 tinyint, t4 smallint, t5 int, t6 bigint, \
                    t7 tinyint unsigned, t8 smallint unsigned, t9 int unsigned, \
                    t10 bigint unsigned, t11 float, t12 double, t13 varchar(10), t14 nchar(10), \
                    t15 varbinary(10), t16 geometry(50), t17 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql =
                c"insert into ? using s0 tags(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname1 = c"d0";
            let tbname2 = c"d1";
            let mut tbnames = vec![tbname1.as_ptr() as _, tbname2.as_ptr() as _];

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t1 = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![1i8];
            let mut length = vec![1];
            let mut is_null = vec![0];
            let t2 = new_bind!(Ty::Bool, buffer, length, is_null);

            let mut buffer = vec![10i8];
            let mut length = vec![1];
            let mut is_null = vec![0];
            let t3 = new_bind!(Ty::TinyInt, buffer, length, is_null);

            let mut buffer = vec![23i16];
            let mut length = vec![2];
            let mut is_null = vec![0];
            let t4 = new_bind!(Ty::SmallInt, buffer, length, is_null);

            let mut buffer = vec![479i32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t5 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut buffer = vec![1999i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t6 = new_bind!(Ty::BigInt, buffer, length, is_null);

            let mut buffer = vec![27u8];
            let mut length = vec![1];
            let mut is_null = vec![0];
            let t7 = new_bind!(Ty::UTinyInt, buffer, length, is_null);

            let mut buffer = vec![89u16];
            let mut length = vec![2];
            let mut is_null = vec![0];
            let t8 = new_bind!(Ty::USmallInt, buffer, length, is_null);

            let mut buffer = vec![234578u32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t9 = new_bind!(Ty::UInt, buffer, length, is_null);

            let mut buffer = vec![234578u64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t10 = new_bind!(Ty::UBigInt, buffer, length, is_null);

            let mut buffer = vec![1.23f32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t11 = new_bind!(Ty::Float, buffer, length, is_null);

            let mut buffer = vec![2345345.99f64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t12 = new_bind!(Ty::Double, buffer, length, is_null);

            let mut buffer = vec![104u8, 101, 108, 108, 111];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t13 = new_bind!(Ty::VarChar, buffer, length, is_null);

            let mut buffer = vec![104u8, 101, 108, 108, 111];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t14 = new_bind!(Ty::NChar, buffer, length, is_null);

            let mut buffer = vec![118u8, 97, 114, 98, 105, 110, 97, 114, 121];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t15 = new_bind!(Ty::VarBinary, buffer, length, is_null);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
            ];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t16 = new_bind!(Ty::Geometry, buffer, length, is_null);

            let mut is_null = vec![1];
            let t17 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: ptr::null_mut(),
                length: ptr::null_mut(),
                is_null: is_null.as_mut_ptr(),
                num: is_null.len() as _,
            };

            let mut tag1 = vec![
                t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17,
            ];
            let mut tag2 = tag1.clone();
            let mut tags = vec![tag1.as_mut_ptr(), tag2.as_mut_ptr()];

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![1i8, 1, 0, 0];
            let mut length = vec![1, 1, 1, 1];
            let mut is_null = vec![0, 0, 0, 1];
            let c1 = new_bind!(Ty::Bool, buffer, length, is_null);

            let mut buffer = vec![23i8, 0, -23, 0];
            let mut length = vec![1, 1, 1, 1];
            let mut is_null = vec![0, 0, 0, 1];
            let c2 = new_bind!(Ty::TinyInt, buffer, length, is_null);

            let mut buffer = vec![34i16, 0, -34, 0];
            let mut length = vec![2, 2, 2, 2];
            let mut is_null = vec![0, 0, 0, 1];
            let c3 = new_bind!(Ty::SmallInt, buffer, length, is_null);

            let mut buffer = vec![45i32, 46, -45, 0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c4 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut buffer = vec![56i64, 57, -56, 0];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 1];
            let c5 = new_bind!(Ty::BigInt, buffer, length, is_null);

            let mut buffer = vec![67u8, 68, 67, 0];
            let mut length = vec![1, 1, 1, 1];
            let mut is_null = vec![0, 0, 0, 1];
            let c6 = new_bind!(Ty::UTinyInt, buffer, length, is_null);

            let mut buffer = vec![78u16, 79, 78, 0];
            let mut length = vec![2, 2, 2, 2];
            let mut is_null = vec![0, 0, 0, 1];
            let c7 = new_bind!(Ty::USmallInt, buffer, length, is_null);

            let mut buffer = vec![89u32, 90, 89, 0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c8 = new_bind!(Ty::UInt, buffer, length, is_null);

            let mut buffer = vec![100u64, 101, 100, 0];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 1];
            let c9 = new_bind!(Ty::UBigInt, buffer, length, is_null);

            let mut buffer = vec![1.23f32, 1.24, -1.23, 0.0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c10 = new_bind!(Ty::Float, buffer, length, is_null);

            let mut buffer = vec![2345.67f64, 2345.68, -2345.67, 0.0];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 1];
            let c11 = new_bind!(Ty::Double, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c12 = new_bind!(Ty::VarChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c13 = new_bind!(Ty::NChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c14 = new_bind!(Ty::VarBinary, buffer, length, is_null);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
            ];
            let mut length = vec![21, 0, 21, 0];
            let mut is_null = vec![0, 1, 0, 1];
            let c15 = new_bind!(Ty::Geometry, buffer, length, is_null);

            let mut col1 = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let ts = new_bind_without_is_null!(Ty::Timestamp, buffer, length);

            let mut buffer = vec![1i8, 1, 0, 0];
            let mut length = vec![1, 1, 1, 1];
            let c1 = new_bind_without_is_null!(Ty::Bool, buffer, length);

            let mut buffer = vec![23i8, 0, -23, 0];
            let mut length = vec![1, 1, 1, 1];
            let c2 = new_bind_without_is_null!(Ty::TinyInt, buffer, length);

            let mut buffer = vec![34i16, 0, -34, 0];
            let mut length = vec![2, 2, 2, 2];
            let c3 = new_bind_without_is_null!(Ty::SmallInt, buffer, length);

            let mut buffer = vec![45i32, 46, -45, 0];
            let mut length = vec![4, 4, 4, 4];
            let c4 = new_bind_without_is_null!(Ty::Int, buffer, length);

            let mut buffer = vec![56i64, 57, -56, 0];
            let mut length = vec![8, 8, 8, 8];
            let c5 = new_bind_without_is_null!(Ty::BigInt, buffer, length);

            let mut buffer = vec![67u8, 68, 67, 0];
            let mut length = vec![1, 1, 1, 1];
            let c6 = new_bind_without_is_null!(Ty::UTinyInt, buffer, length);

            let mut buffer = vec![78u16, 79, 78, 0];
            let mut length = vec![2, 2, 2, 2];
            let c7 = new_bind_without_is_null!(Ty::USmallInt, buffer, length);

            let mut buffer = vec![89u32, 90, 89, 0];
            let mut length = vec![4, 4, 4, 4];
            let c8 = new_bind_without_is_null!(Ty::UInt, buffer, length);

            let mut buffer = vec![100u64, 101, 100, 0];
            let mut length = vec![8, 8, 8, 8];
            let c9 = new_bind_without_is_null!(Ty::UBigInt, buffer, length);

            let mut buffer = vec![1.23f32, 1.24, -1.23, 0.0];
            let mut length = vec![4, 4, 4, 4];
            let c10 = new_bind_without_is_null!(Ty::Float, buffer, length);

            let mut buffer = vec![2345.67f64, 2345.68, -2345.67, 0.0];
            let mut length = vec![8, 8, 8, 8];
            let c11 = new_bind_without_is_null!(Ty::Double, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c12 = new_bind_without_is_null!(Ty::VarChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c13 = new_bind_without_is_null!(Ty::NChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c14 = new_bind_without_is_null!(Ty::VarBinary, buffer, length);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
                0, 0, 0, 0, 0, 0, 240, 63,
            ];
            let mut length = vec![21, 21, 21, 21];
            let c15 = new_bind_without_is_null!(Ty::Geometry, buffer, length);

            let mut col2 = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let mut cols = vec![col1.as_mut_ptr(), col2.as_mut_ptr()];

            let mut bindv = TAOS_STMT2_BINDV {
                count: tbnames.len() as _,
                tbnames: tbnames.as_mut_ptr(),
                tags: tags.as_mut_ptr(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 8);

            let sql = c"select * from s0 where c4 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c4 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c4];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 0);

            let res = taos_stmt2_result(stmt2);
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 33);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            taos_free_result(res);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739502440");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739966021",
                    "create database test_1739966021",
                    "use test_1739966021",
                    "create table s0 (ts timestamp, c1 int) tags (t1 json)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname = c"d0";
            let mut tbnames = vec![tbname.as_ptr() as _];

            let mut buffer = vec![
                123u8, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125,
            ];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t1 = new_bind!(Ty::Json, buffer, length, is_null);

            let mut tag = vec![t1];
            let mut tags = vec![tag.as_mut_ptr()];

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];

            let mut bindv = TAOS_STMT2_BINDV {
                count: tbnames.len() as _,
                tbnames: tbnames.as_mut_ptr(),
                tags: tags.as_mut_ptr(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 1);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739966021");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_exec_async() {
        unsafe {
            extern "C" fn fp(userdata: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert_eq!(code, 0);
                    assert!(!res.is_null());

                    let userdata = CStr::from_ptr(userdata as _);
                    assert_eq!(userdata, c"hello, world");

                    let row = taos_fetch_row(res);
                    assert!(!row.is_null());

                    let fields = taos_fetch_fields(res);
                    assert!(!fields.is_null());

                    let num_fields = taos_num_fields(res);
                    assert_eq!(num_fields, 2);

                    let mut str = vec![0 as c_char; 1024];
                    let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                    assert!(len > 0);
                    println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

                    taos_free_result(res);
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739864837",
                    "create database test_1739864837",
                    "use test_1739864837",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 2)",
                ],
            );

            let userdata = c"hello, world";
            let mut option = TAOS_STMT2_OPTION {
                reqid: 1001,
                singleStbInsert: true,
                singleTableBindOnce: false,
                asyncExecFn: fp,
                userdata: userdata.as_ptr() as _,
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![1];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let code = taos_stmt2_exec(stmt2, ptr::null_mut());
            assert_eq!(code, 0);

            std::thread::sleep(std::time::Duration::from_secs(1));

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739864837");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_result() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739876374",
                    "create database test_1739876374",
                    "use test_1739876374",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 1)",
                    "insert into t0 values(1739762261438, 99)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 0);

            let res = taos_stmt2_result(stmt2);
            assert!(!res.is_null());

            let affected_rows = taos_affected_rows(res);
            assert_eq!(affected_rows, 0);

            let precision = taos_result_precision(res);
            assert_eq!(precision, 0);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 2);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            taos_stop_query(res);

            taos_free_result(res);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739876374");
            taos_close(taos);
        }
    }
}
