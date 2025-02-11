use std::ffi::{c_char, c_int, c_ulong, c_void};

use taos_error::Code;
use taos_query::block_in_place_or_global;
use taos_query::util::generate_req_id;
use taos_ws::{Stmt2, Taos};
use tracing::trace;

use crate::native::error::{TaosError, TaosMaybeError};
use crate::native::{TaosResult, __taos_async_fn_t, TAOS, TAOS_RES};

#[allow(non_camel_case_types)]
pub type TAOS_STMT2 = c_void;

#[repr(C)]
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
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_init(
    taos: *mut TAOS,
    option: *mut TAOS_STMT2_OPTION,
) -> *mut TAOS_STMT2 {
    let stmt2: TaosMaybeError<Stmt2> = stmt2_init(taos, option).into();
    Box::into_raw(Box::new(stmt2)) as _
}

unsafe fn stmt2_init(taos: *mut TAOS, option: *mut TAOS_STMT2_OPTION) -> TaosResult<Stmt2> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is invalid"))?;

    let mut stmt2 = Stmt2::new(taos.client());

    let (req_id, single_stb_insert, single_table_bind_once) = if !option.is_null() {
        let option = option
            .as_ref()
            .ok_or(TaosError::new(Code::INVALID_PARA, "option is invalid"))?;
        (
            option.reqid as u64,
            option.singleStbInsert,
            option.singleTableBindOnce,
        )
    } else {
        (generate_req_id(), true, false)
    };

    trace!("stmt2_init, req_id: {req_id}, single_stb_insert: {single_stb_insert}, single_table_bind_once: {single_table_bind_once}");

    block_in_place_or_global(stmt2.init_with_options(
        req_id,
        single_stb_insert,
        single_table_bind_once,
    ))?;

    Ok(stmt2)
}

#[no_mangle]
pub extern "C" fn taos_stmt2_prepare(
    stmt: *mut TAOS_STMT2,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt2_bind_param(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt2_exec(stmt: *mut TAOS_STMT2, affected_rows: *mut c_int) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt2_close(stmt: *mut TAOS_STMT2) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt2_is_insert(stmt: *mut TAOS_STMT2, insert: *mut c_int) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt2_get_fields(
    stmt: *mut TAOS_STMT2,
    count: *mut c_int,
    fields: *mut *mut TAOS_FIELD_ALL,
) -> c_int {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt2_free_fields(stmt: *mut TAOS_STMT2, fields: *mut TAOS_FIELD_ALL) {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt2_result(stmt: *mut TAOS_STMT2) -> *mut TAOS_RES {
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_stmt2_error(stmt: *mut TAOS_STMT2) -> *mut c_char {
    todo!()
}

#[cfg(test)]
mod tests {
    use std::{mem, ptr};

    use super::*;
    use crate::native::test_connect;

    #[test]
    fn test_stmt2() {
        unsafe {
            let taos = test_connect();
            let mut option = TAOS_STMT2_OPTION {
                reqid: 1001,
                singleStbInsert: true,
                singleTableBindOnce: false,
                asyncExecFn: mem::transmute::<*const (), __taos_async_fn_t>(ptr::null()),
                userdata: ptr::null_mut(),
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());
        }
    }
}
