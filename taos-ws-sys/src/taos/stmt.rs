use std::ffi::{c_char, c_int, c_ulong, c_void};

use crate::taos::{driver, CAPI, TAOS, TAOS_FIELD_E, TAOS_RES};
use crate::ws::stmt;

#[allow(non_camel_case_types)]
pub type TAOS_STMT = c_void;

#[repr(C)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub struct TAOS_STMT_OPTIONS {
    pub reqId: i64,
    pub singleStbInsert: bool,
    pub singleTableBindOnce: bool,
}

#[repr(C)]
#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub struct TAOS_MULTI_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub buffer_length: usize,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: c_int,
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_init(taos: *mut TAOS) -> *mut TAOS_STMT {
    if driver() {
        stmt::taos_stmt_init(taos)
    } else {
        (CAPI.stmt_api.taos_stmt_init)(taos)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_init_with_reqid(taos: *mut TAOS, reqid: i64) -> *mut TAOS_STMT {
    if driver() {
        stmt::taos_stmt_init_with_reqid(taos, reqid)
    } else {
        (CAPI.stmt_api.taos_stmt_init_with_reqid)(taos, reqid)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_init_with_options(
    taos: *mut TAOS,
    options: *mut TAOS_STMT_OPTIONS,
) -> *mut TAOS_STMT {
    if driver() {
        stmt::taos_stmt_init_with_options(taos, options)
    } else {
        (CAPI.stmt_api.taos_stmt_init_with_options)(taos, options)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_prepare(
    stmt: *mut TAOS_STMT,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    if driver() {
        stmt::taos_stmt_prepare(stmt, sql, length)
    } else {
        (CAPI.stmt_api.taos_stmt_prepare)(stmt, sql, length)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_set_tbname_tags(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    if driver() {
        stmt::taos_stmt_set_tbname_tags(stmt, name, tags)
    } else {
        (CAPI.stmt_api.taos_stmt_set_tbname_tags)(stmt, name, tags)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    if driver() {
        stmt::taos_stmt_set_tbname(stmt, name)
    } else {
        (CAPI.stmt_api.taos_stmt_set_tbname)(stmt, name)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_set_tags(
    stmt: *mut TAOS_STMT,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    if driver() {
        stmt::taos_stmt_set_tags(stmt, tags)
    } else {
        (CAPI.stmt_api.taos_stmt_set_tags)(stmt, tags)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_set_sub_tbname(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
) -> c_int {
    if driver() {
        stmt::taos_stmt_set_sub_tbname(stmt, name)
    } else {
        (CAPI.stmt_api.taos_stmt_set_sub_tbname)(stmt, name)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_get_tag_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    if driver() {
        stmt::taos_stmt_get_tag_fields(stmt, fieldNum, fields)
    } else {
        (CAPI.stmt_api.taos_stmt_get_tag_fields)(stmt, fieldNum, fields)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_get_col_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    if driver() {
        stmt::taos_stmt_get_col_fields(stmt, fieldNum, fields)
    } else {
        (CAPI.stmt_api.taos_stmt_get_col_fields)(stmt, fieldNum, fields)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_reclaim_fields(stmt: *mut TAOS_STMT, fields: *mut TAOS_FIELD_E) {
    if driver() {
        stmt::taos_stmt_reclaim_fields(stmt, fields);
    } else {
        (CAPI.stmt_api.taos_stmt_reclaim_fields)(stmt, fields);
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int {
    if driver() {
        stmt::taos_stmt_is_insert(stmt, insert)
    } else {
        (CAPI.stmt_api.taos_stmt_is_insert)(stmt, insert)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int {
    if driver() {
        stmt::taos_stmt_num_params(stmt, nums)
    } else {
        (CAPI.stmt_api.taos_stmt_num_params)(stmt, nums)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_get_param(
    stmt: *mut TAOS_STMT,
    idx: c_int,
    r#type: *mut c_int,
    bytes: *mut c_int,
) -> c_int {
    if driver() {
        stmt::taos_stmt_get_param(stmt, idx, r#type, bytes)
    } else {
        (CAPI.stmt_api.taos_stmt_get_param)(stmt, idx, r#type, bytes)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_bind_param(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    if driver() {
        stmt::taos_stmt_bind_param(stmt, bind)
    } else {
        (CAPI.stmt_api.taos_stmt_bind_param)(stmt, bind)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_bind_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    if driver() {
        stmt::taos_stmt_bind_param_batch(stmt, bind)
    } else {
        (CAPI.stmt_api.taos_stmt_bind_param_batch)(stmt, bind)
    }
}

#[no_mangle]
#[allow(non_snake_case)]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_bind_single_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
    colIdx: c_int,
) -> c_int {
    if driver() {
        stmt::taos_stmt_bind_single_param_batch(stmt, bind, colIdx)
    } else {
        (CAPI.stmt_api.taos_stmt_bind_single_param_batch)(stmt, bind, colIdx)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int {
    if driver() {
        stmt::taos_stmt_add_batch(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_add_batch)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int {
    if driver() {
        stmt::taos_stmt_execute(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_execute)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES {
    if driver() {
        stmt::taos_stmt_use_result(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_use_result)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int {
    if driver() {
        stmt::taos_stmt_close(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_close)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_errstr(stmt: *mut TAOS_STMT) -> *mut c_char {
    if driver() {
        stmt::taos_stmt_errstr(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_errstr)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int {
    if driver() {
        stmt::taos_stmt_affected_rows(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_affected_rows)(stmt)
    }
}

#[no_mangle]
#[cfg_attr(not(test), tracing::instrument(level = "debug", ret))]
pub unsafe extern "C" fn taos_stmt_affected_rows_once(stmt: *mut TAOS_STMT) -> c_int {
    if driver() {
        stmt::taos_stmt_affected_rows_once(stmt)
    } else {
        (CAPI.stmt_api.taos_stmt_affected_rows_once)(stmt)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::ptr;

    use taos_query::common::Ty;

    use super::*;
    use crate::taos::query::*;
    use crate::taos::{taos_close, test_connect, test_exec, test_exec_many};

    macro_rules! new_bind {
        ($ty:expr, $buffer:ident, $length:ident, $is_null:ident) => {
            TAOS_MULTI_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                buffer_length: *$length.iter().max().unwrap() as _,
                length: $length.as_mut_ptr(),
                is_null: $is_null.as_mut_ptr(),
                num: $is_null.len() as _,
            }
        };
    }

    macro_rules! new_bind_without_is_null {
        ($ty:expr, $buffer:ident, $length:ident) => {
            TAOS_MULTI_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                buffer_length: *$length.iter().max().unwrap() as _,
                length: $length.as_mut_ptr(),
                is_null: ptr::null_mut(),
                num: $length.len() as _,
            }
        };
    }

    #[test]
    fn test_stmt() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740472346",
                    "create database test_1740472346",
                    "use test_1740472346",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"insert into ? using s0 tags (?) values (?, ?)";
            let len = sql.to_bytes().len();
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), len as _);
            assert_eq!(code, 0);

            let name = c"d0";
            let mut tags = vec![TAOS_MULTI_BIND::from_primitives(&[99], &[false], &[4])];
            let code = taos_stmt_set_tbname_tags(stmt, name.as_ptr(), tags.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_set_tbname(stmt, name.as_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_set_sub_tbname(stmt, name.as_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_set_tags(stmt, tags.as_mut_ptr());
            assert_eq!(code, 0);

            let mut field_num = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt_get_tag_fields(stmt, &mut field_num, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(field_num, 1);

            taos_stmt_reclaim_fields(stmt, fields);

            let mut field_num = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt_get_col_fields(stmt, &mut field_num, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(field_num, 2);

            taos_stmt_reclaim_fields(stmt, fields);

            let mut insert = 0;
            let code = taos_stmt_is_insert(stmt, &mut insert);
            assert_eq!(code, 0);
            assert_eq!(insert, 1);

            let mut nums = 0;
            let code = taos_stmt_num_params(stmt, &mut nums);
            assert_eq!(code, 0);
            assert_eq!(nums, 2);

            let mut ty = 0;
            let mut bytes = 0;
            let code = taos_stmt_get_param(stmt, 1, &mut ty, &mut bytes);
            assert_eq!(code, 0);
            assert_eq!(ty, Ty::Int as i32);
            assert_eq!(bytes, 4);

            let mut cols = vec![
                TAOS_MULTI_BIND::from_timestamps(&[1738910658659i64], &[false], &[8]),
                TAOS_MULTI_BIND::from_primitives(&[20], &[false], &[4]),
            ];
            let code = taos_stmt_bind_param_batch(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let mut cols = vec![
                TAOS_MULTI_BIND::from_timestamps(&[1738910658659i64], &[false], &[8]),
                TAOS_MULTI_BIND::from_primitives(&[2025], &[false], &[4]),
            ];
            let code = taos_stmt_bind_param(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let errstr = taos_stmt_errstr(stmt);
            assert_eq!(CStr::from_ptr(errstr), c"success");

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 1);

            let affected_rows_once = taos_stmt_affected_rows_once(stmt);
            assert_eq!(affected_rows_once, 1);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740472346");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt_init_with_options() {
        unsafe {
            let taos = test_connect();
            let mut option = TAOS_STMT_OPTIONS {
                reqId: 1001,
                singleStbInsert: true,
                singleTableBindOnce: false,
            };

            let stmt = taos_stmt_init_with_options(taos, &mut option);
            assert!(!stmt.is_null());

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            let stmt = taos_stmt_init_with_options(taos, ptr::null_mut());
            assert!(!stmt.is_null());

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt_use_result() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740560525",
                    "create database test_1740560525",
                    "use test_1740560525",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 1)",
                    "insert into t0 values(1739762261438, 99)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut bind = vec![c1];
            let code = taos_stmt_bind_param(stmt, bind.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let res = taos_stmt_use_result(stmt);
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

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740560525");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt_bind_param_batch() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740563439",
                    "create database test_1740563439",
                    "use test_1740563439",
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

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql =
                c"insert into ? using s0 tags(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname1 = c"d0";
            let tbname2 = c"d1";

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
            let t17 = TAOS_MULTI_BIND {
                buffer_type: Ty::Int as _,
                buffer: ptr::null_mut(),
                buffer_length: 0,
                length: ptr::null_mut(),
                is_null: is_null.as_mut_ptr(),
                num: is_null.len() as _,
            };

            let mut tags = vec![
                t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17,
            ];

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
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c12 = new_bind!(Ty::VarChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c13 = new_bind!(Ty::NChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c14 = new_bind!(Ty::VarBinary, buffer, length, is_null);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
            ];
            let mut length = vec![21, 0, 21, 0];
            let mut is_null = vec![0, 1, 0, 1];
            let c15 = new_bind!(Ty::Geometry, buffer, length, is_null);

            let mut cols = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let code = taos_stmt_set_tbname(stmt, tbname1.as_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_set_tags(stmt, tags.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_bind_param(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

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
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c12 = new_bind_without_is_null!(Ty::VarChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c13 = new_bind_without_is_null!(Ty::NChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
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

            let mut cols = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let code = taos_stmt_set_tbname_tags(stmt, tbname2.as_ptr(), tags.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_bind_param(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 8);

            let sql = c"select * from s0 where c4 > ?";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c4 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut bind = vec![c4];

            let code = taos_stmt_bind_param(stmt, bind.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows_once = taos_stmt_affected_rows_once(stmt);
            assert_eq!(affected_rows_once, 0);

            let res = taos_stmt_use_result(stmt);
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

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740563439");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740573608",
                    "create database test_1740573608",
                    "use test_1740573608",
                    "create table s0 (ts timestamp, c1 int) tags (t1 json)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname = c"d0";

            let mut buffer = vec![
                123u8, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125,
            ];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t1 = new_bind!(Ty::Json, buffer, length, is_null);

            let mut tags = vec![t1];

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut cols = vec![ts, c1];

            let code = taos_stmt_set_tbname_tags(stmt, tbname.as_ptr(), tags.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_bind_param_batch(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 1);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740573608");
            taos_close(taos);
        }
    }

    #[test]
    fn test_stmt_stb_insert() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741246945",
                    "create database test_1741246945",
                    "use test_1741246945",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let mut option = TAOS_STMT_OPTIONS {
                reqId: 1001,
                singleStbInsert: true,
                singleTableBindOnce: true,
            };
            let stmt = taos_stmt_init_with_options(taos, &mut option);
            assert!(!stmt.is_null());

            let sql = c"insert into s0 (tbname, ts, c1) values(?, ?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let num_of_sub_table = 10;

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            for i in 0..num_of_sub_table {
                test_exec(taos, format!("create table d{i} using s0 tags({i})"));

                let name = CString::new(format!("d{i}")).unwrap();
                let code = taos_stmt_set_tbname(stmt, name.as_ptr());
                assert_eq!(code, 0);

                let mut cols = vec![ts.clone(), c1.clone()];
                let code = taos_stmt_bind_param(stmt, cols.as_mut_ptr());
                assert_eq!(code, 0);
            }

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows as i64, num_of_sub_table);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1741246945");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741251640",
                    "create database test_1741251640",
                    "use test_1741251640",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let mut option = TAOS_STMT_OPTIONS {
                reqId: 1001,
                singleStbInsert: true,
                singleTableBindOnce: true,
            };
            let stmt = taos_stmt_init_with_options(taos, &mut option);
            assert!(!stmt.is_null());

            let sql = c"insert into s0 (tbname, ts, c1) values(?, ?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let num_of_sub_table = 10;

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            for i in 0..num_of_sub_table {
                test_exec(taos, format!("create table d{i} using s0 tags({i})"));

                let name = CString::new(format!("d{i}")).unwrap();
                let code = taos_stmt_set_tbname(stmt, name.as_ptr());
                assert_eq!(code, 0);

                let mut cols = vec![ts.clone(), c1.clone()];
                let code = taos_stmt_bind_param_batch(stmt, cols.as_mut_ptr());
                assert_eq!(code, 0);
            }

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows as i64, num_of_sub_table);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1741251640");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt_bind_single_param_batch() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741438739",
                    "create database test_1741438739",
                    "use test_1741438739",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                    "create table d0 using s0 tags(0)",
                    "create table d1 using s0 tags(1)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"insert into s0 (tbname, ts, c1) values(?, ?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname = c"d0";
            let code = taos_stmt_set_tbname(stmt, tbname.as_ptr());
            assert_eq!(code, 0);

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let mut ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let mut c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let code = taos_stmt_bind_single_param_batch(stmt, &mut ts, 0);
            assert_eq!(code, 0);

            let code = taos_stmt_bind_single_param_batch(stmt, &mut c1, 1);
            assert_eq!(code, 0);

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let tbname = c"d1";
            let code = taos_stmt_set_tbname(stmt, tbname.as_ptr());
            assert_eq!(code, 0);

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let mut ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let mut c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let code = taos_stmt_bind_single_param_batch(stmt, &mut ts, 0);
            assert_eq!(code, 0);

            let code = taos_stmt_bind_single_param_batch(stmt, &mut c1, 1);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 2);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1741438739");
            taos_close(taos);
        }
    }
}
