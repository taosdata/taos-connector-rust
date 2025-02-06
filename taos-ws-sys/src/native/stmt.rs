use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::ptr;

use taos_error::Code;
use taos_query::stmt::Bindable;
use taos_query::util::generate_req_id;
use taos_ws::stmt::{StmtField as WsStmtField, WsFieldsable};
use taos_ws::{Stmt, Taos};
use tracing::error;

use crate::native::error::{
    clear_error_info, format_errno, set_err_and_get_code, TaosError, TaosMaybeError,
};
use crate::native::{TaosResult, TAOS, TAOS_RES};

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
#[allow(non_camel_case_types)]
pub struct TAOS_MULTI_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub buffer_length: usize,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: c_int,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD_E {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
}

impl From<&WsStmtField> for TAOS_FIELD_E {
    fn from(field: &WsStmtField) -> Self {
        let field_name = field.name.as_str();
        let mut name = [0 as c_char; 65];
        unsafe {
            std::ptr::copy_nonoverlapping(
                field_name.as_ptr(),
                name.as_mut_ptr() as _,
                field_name.len(),
            );
        };

        Self {
            name,
            r#type: field.field_type,
            precision: field.precision,
            scale: field.scale,
            bytes: field.bytes,
        }
    }
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_init(taos: *mut TAOS) -> *mut TAOS_STMT {
    taos_stmt_init_with_reqid(taos, generate_req_id() as _)
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_init_with_reqid(taos: *mut TAOS, reqid: i64) -> *mut TAOS_STMT {
    let stmt: TaosMaybeError<Stmt> = stmt_init(taos, reqid as _).into();
    Box::into_raw(Box::new(stmt)) as _
}

#[no_mangle]
pub extern "C" fn taos_stmt_init_with_options(
    taos: *mut TAOS,
    options: *mut TAOS_STMT_OPTIONS,
) -> *mut TAOS_STMT {
    todo!()
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_prepare(
    stmt: *mut TAOS_STMT,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    let maybe_err = match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(stmt) => stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    if let Some(errno) = maybe_err.errno() {
        return format_errno(errno);
    }

    let stmt = match maybe_err.deref_mut() {
        Some(stmt) => stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "data is null")),
    };

    let sql = if length > 0 {
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql as _, length as _))
    } else {
        CStr::from_ptr(sql).to_str().expect(
            "taos_stmt_prepare with a sql len 0 means the input should always be valid utf-8",
        )
    };

    if let Err(err) = stmt.prepare(sql) {
        error!("stmt prepare error, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
    }

    match stmt.get_tag_fields() {
        Ok(fields) => stmt.with_tag_fields(fields),
        Err(err) => {
            error!("get_tag_fields error, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            return set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
        }
    }

    maybe_err.with_err(None);
    clear_error_info();
    Code::SUCCESS.into()
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_tbname_tags(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_set_tags(stmt: *mut TAOS_STMT, tags: *mut TAOS_MULTI_BIND) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_set_sub_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_get_tag_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    let maybe_err = match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(stmt) => stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt = match maybe_err.deref_mut() {
        Some(stmt) => stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "data is null")),
    };

    match stmt.tag_fields() {
        Some(stmt_fields) => {
            let taos_fields: Vec<TAOS_FIELD_E> =
                stmt_fields.into_iter().map(|field| field.into()).collect();
            *fieldNum = taos_fields.len() as _;
            *fields = Box::into_raw(taos_fields.into_boxed_slice()) as _;
        }
        None => {
            *fieldNum = 0;
            *fields = ptr::null_mut();
        }
    };

    maybe_err.with_err(None);
    clear_error_info();
    Code::SUCCESS.into()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_stmt_get_col_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_reclaim_fields(stmt: *mut TAOS_STMT, fields: *mut TAOS_FIELD_E) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_get_param(
    stmt: *mut TAOS_STMT,
    idx: c_int,
    r#type: *mut c_int,
    bytes: *mut c_int,
) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_bind_param(stmt: *mut TAOS_STMT, bind: *mut TAOS_MULTI_BIND) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_bind_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    todo!()
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "C" fn taos_stmt_bind_single_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
    colIdx: c_int,
) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_errstr(stmt: *mut TAOS_STMT) -> *mut c_char {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt_affected_rows_once(stmt: *mut TAOS_STMT) -> c_int {
    todo!()
}

unsafe fn stmt_init(taos: *mut TAOS, reqid: u64) -> TaosResult<Stmt> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::FAILED, "taos is null"))?;
    Ok(Stmt::init_with_req_id(taos, reqid)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::{test_connect, test_exec, test_exec_many};

    #[test]
    fn test_stmt() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1738740951",
                    "create database test_1738740951",
                    "use test_1738740951",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"insert into d0 using s0 tags (1) values (?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut field_num = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt_get_tag_fields(stmt, &mut field_num, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(field_num, 1);

            let fields = Vec::from_raw_parts(fields, field_num as _, field_num as _);
            println!("fields: {fields:?}");

            test_exec(taos, "drop database test_1738740951");
        }
    }
}
