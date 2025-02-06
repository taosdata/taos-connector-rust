use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::ptr;

use taos_error::{Code, Error as RawError};
use taos_query::common::{Timestamp, Ty, Value};
use taos_query::prelude::Itertools;
use taos_query::stmt::Bindable;
use taos_query::util::generate_req_id;
use taos_ws::stmt::{StmtField, WsFieldsable};
use taos_ws::{Stmt, Taos};
use tracing::error;

use crate::native::error::{
    clear_error_info, format_errno, set_err_and_get_code, TaosError, TaosMaybeError,
};
use crate::native::{TaosResult, TAOS, TAOS_RES};

pub const TSDB_DATA_TYPE_NULL: usize = 0;
pub const TSDB_DATA_TYPE_BOOL: usize = 1;
pub const TSDB_DATA_TYPE_TINYINT: usize = 2;
pub const TSDB_DATA_TYPE_SMALLINT: usize = 3;
pub const TSDB_DATA_TYPE_INT: usize = 4;
pub const TSDB_DATA_TYPE_BIGINT: usize = 5;
pub const TSDB_DATA_TYPE_FLOAT: usize = 6;
pub const TSDB_DATA_TYPE_DOUBLE: usize = 7;
pub const TSDB_DATA_TYPE_VARCHAR: usize = 8;
pub const TSDB_DATA_TYPE_TIMESTAMP: usize = 9;
pub const TSDB_DATA_TYPE_NCHAR: usize = 10;
pub const TSDB_DATA_TYPE_UTINYINT: usize = 11;
pub const TSDB_DATA_TYPE_USMALLINT: usize = 12;
pub const TSDB_DATA_TYPE_UINT: usize = 13;
pub const TSDB_DATA_TYPE_UBIGINT: usize = 14;
pub const TSDB_DATA_TYPE_JSON: usize = 15;
pub const TSDB_DATA_TYPE_VARBINARY: usize = 16;
pub const TSDB_DATA_TYPE_DECIMAL: usize = 17;
pub const TSDB_DATA_TYPE_BLOB: usize = 18;
pub const TSDB_DATA_TYPE_MEDIUMBLOB: usize = 19;
pub const TSDB_DATA_TYPE_BINARY: usize = TSDB_DATA_TYPE_VARCHAR;
pub const TSDB_DATA_TYPE_GEOMETRY: usize = 20;
pub const TSDB_DATA_TYPE_MAX: usize = 21;

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

impl TAOS_MULTI_BIND {
    pub fn new(ty: Ty) -> Self {
        Self {
            buffer_type: ty as _,
            buffer: ptr::null_mut(),
            buffer_length: 0,
            length: ptr::null_mut(),
            is_null: ptr::null_mut(),
            num: 1,
        }
    }

    pub fn to_tag_value(&self) -> Value {
        if !self.is_null.is_null() && unsafe { self.is_null.read() != 0 } {
            return Value::Null(self.ty());
        }

        match Ty::from(self.buffer_type) {
            Ty::Null => Value::Null(self.ty()),
            Ty::Bool => unsafe { Value::Bool(*(self.buffer as *const bool)) },
            Ty::TinyInt => unsafe { Value::TinyInt(*(self.buffer as *const i8)) },
            Ty::SmallInt => unsafe { Value::SmallInt(*(self.buffer as *const i16)) },
            Ty::Int => unsafe { Value::Int(*(self.buffer as *const i32)) },
            Ty::BigInt => unsafe { Value::BigInt(*(self.buffer as *const _)) },
            Ty::UTinyInt => unsafe { Value::UTinyInt(*(self.buffer as *const _)) },
            Ty::USmallInt => unsafe { Value::USmallInt(*(self.buffer as *const _)) },
            Ty::UInt => unsafe { Value::UInt(*(self.buffer as *const _)) },
            Ty::UBigInt => unsafe { Value::UBigInt(*(self.buffer as *const _)) },
            Ty::Float => unsafe { Value::Float(*(self.buffer as *const _)) },
            Ty::Double => unsafe { Value::Double(*(self.buffer as *const _)) },
            Ty::Timestamp => unsafe {
                Value::Timestamp(Timestamp::Milliseconds(*(self.buffer as *const _)))
            },
            Ty::VarChar => unsafe {
                assert!(!self.length.is_null());
                assert!(!self.buffer.is_null());
                let slice =
                    std::slice::from_raw_parts(self.buffer as _, self.length.read() as usize);
                let v = std::str::from_utf8_unchecked(slice);
                Value::VarChar(v.to_string())
            },
            Ty::NChar => unsafe {
                assert!(!self.length.is_null());
                assert!(!self.buffer.is_null());
                let slice =
                    std::slice::from_raw_parts(self.buffer as _, self.length.read() as usize);
                let v = std::str::from_utf8_unchecked(slice);
                Value::NChar(v.to_string())
            },
            Ty::Json => unsafe {
                assert!(!self.length.is_null());
                assert!(!self.buffer.is_null());
                let slice =
                    std::slice::from_raw_parts(self.buffer as _, self.length.read() as usize);
                Value::Json(serde_json::from_slice(slice).unwrap())
            },
            _ => todo!(),
        }
    }

    pub fn first_to_json(&self) -> serde_json::Value {
        self.to_json().as_array().unwrap().first().unwrap().clone()
    }

    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::{json, Value};

        assert!(self.num > 0, "invalid bind value");
        let len = self.num as usize;

        macro_rules! _nulls {
            () => {
                json!(std::iter::repeat(Value::Null).take(len).collect::<Vec<_>>())
            };
        }

        if self.buffer.is_null() {
            return _nulls!();
        }

        macro_rules! _impl_primitive {
            ($t:ty) => {{
                let slice = std::slice::from_raw_parts(self.buffer as *const $t, len);
                match self.is_null.is_null() {
                    true => json!(slice),
                    false => {
                        let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
                        let column: Vec<_> = slice
                            .iter()
                            .zip(nulls)
                            .map(
                                |(value, is_null)| {
                                    if *is_null {
                                        None
                                    } else {
                                        Some(*value)
                                    }
                                },
                            )
                            .collect();
                        json!(column)
                    }
                }
            }};
        }

        unsafe {
            match Ty::from(self.buffer_type) {
                Ty::Null => _nulls!(),
                Ty::Bool => _impl_primitive!(bool),
                Ty::TinyInt => _impl_primitive!(i8),
                Ty::SmallInt => _impl_primitive!(i16),
                Ty::Int => _impl_primitive!(i32),
                Ty::BigInt => _impl_primitive!(i64),
                Ty::UTinyInt => _impl_primitive!(u8),
                Ty::USmallInt => _impl_primitive!(u16),
                Ty::UInt => _impl_primitive!(u32),
                Ty::UBigInt => _impl_primitive!(u64),
                Ty::Float => _impl_primitive!(f32),
                Ty::Double => _impl_primitive!(f64),
                Ty::Timestamp => _impl_primitive!(i64),
                Ty::VarChar => {
                    let len = self.num as usize;
                    if self.is_null.is_null() {
                        let column = (0..len)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        return json!(column);
                    }
                    let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
                    let column = (0..len)
                        .zip(nulls)
                        .map(|(i, is_null)| {
                            if *is_null {
                                None
                            } else {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                Some(std::str::from_utf8_unchecked(bytes))
                            }
                        })
                        .collect::<Vec<_>>();
                    json!(column)
                }
                Ty::NChar => {
                    let len = self.num as usize;
                    if self.is_null.is_null() {
                        let column = (0..len)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        return json!(column);
                    }
                    let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
                    let column = (0..len)
                        .zip(nulls)
                        .map(|(i, is_null)| {
                            if *is_null {
                                None
                            } else {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                Some(std::str::from_utf8_unchecked(bytes))
                            }
                        })
                        .collect::<Vec<_>>();
                    json!(column)
                }
                Ty::Json => {
                    let len = self.num as usize;
                    if self.is_null.is_null() {
                        let column = (0..len)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        return json!(column);
                    }
                    let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
                    let column = (0..len)
                        .zip(nulls)
                        .map(|(i, is_null)| {
                            if *is_null {
                                None
                            } else {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                Some(serde_json::from_slice::<serde_json::Value>(bytes).unwrap())
                            }
                        })
                        .collect::<Vec<_>>();
                    json!(column)
                }
                _ => todo!(),
            }
        }
    }

    pub fn ty(&self) -> Ty {
        self.buffer_type.into()
    }
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

impl From<&StmtField> for TAOS_FIELD_E {
    fn from(field: &StmtField) -> Self {
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
    match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => {
            let sql = if length > 0 {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql as _, length as _))
            } else {
                CStr::from_ptr(sql).to_str().expect(
                    "taos_stmt_prepare with a sql len 0 means the input should always be valid utf-8",
                )
            };

            if let Some(errno) = maybe_err.errno() {
                return format_errno(errno);
            }

            if let Err(err) = maybe_err
                .deref_mut()
                .ok_or_else(|| RawError::from_string("data is null"))
                .and_then(|stmt| stmt.prepare(sql))
            {
                error!("stmt prepare error, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
            } else {
                maybe_err.with_err(None);
                clear_error_info();
                Code::SUCCESS.into()
            }
        }
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    }
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_tbname_tags(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    let maybe_err = match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    match stmt_set_tbname_tags(maybe_err, name, tags) {
        Ok(_) => {
            maybe_err.with_err(None);
            clear_error_info();
            Code::SUCCESS.into()
        }
        Err(err) => {
            error!("stmt set_tbname_tags error, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
        }
    }
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => {
            let name = CStr::from_ptr(name).to_str().unwrap();
            if let Err(err) = maybe_err
                .deref_mut()
                .ok_or_else(|| RawError::from_string("data is null"))
                .and_then(|stmt| stmt.set_tbname(name))
            {
                error!("stmt set tbname error, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
            } else {
                maybe_err.with_err(None);
                clear_error_info();
                Code::SUCCESS.into()
            }
        }
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    }
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_tags(
    stmt: *mut TAOS_STMT,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    let maybe_err = match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt = match maybe_err.deref_mut() {
        Some(stmt) => stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "data is null")),
    };

    let fields = match stmt_get_tag_fields(stmt) {
        Ok(fields) => fields,
        Err(err) => {
            error!("stmt get tag fields error, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            return set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
        }
    };

    let tags = std::slice::from_raw_parts(tags, fields.len())
        .iter()
        .map(TAOS_MULTI_BIND::to_tag_value)
        .collect_vec();

    if let Err(err) = stmt.set_tags(&tags) {
        error!("stmt set tags error, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
    }

    Code::SUCCESS.into()
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_set_sub_tbname(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
) -> c_int {
    taos_stmt_set_tbname(stmt, name)
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

    let stmt_fields = match stmt_get_tag_fields(stmt) {
        Ok(fields) => fields,
        Err(err) => {
            error!("stmt get tag fields error, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            return set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
        }
    };

    let taos_fields: Vec<TAOS_FIELD_E> =
        stmt_fields.into_iter().map(|field| field.into()).collect();

    *fieldNum = taos_fields.len() as _;
    *fields = Box::into_raw(taos_fields.into_boxed_slice()) as _;

    maybe_err.with_err(None);
    clear_error_info();
    Code::SUCCESS.into()
}

#[no_mangle]
#[allow(non_snake_case)]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_get_col_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => match maybe_err
            .deref_mut()
            .ok_or_else(|| RawError::from_string("data is null"))
            .and_then(WsFieldsable::get_col_fields)
        {
            Ok(stmt_fields) => {
                *fieldNum = stmt_fields.len() as _;
                *fields = Box::into_raw(stmt_fields.into_boxed_slice()) as _;
                maybe_err.with_err(None);
                clear_error_info();
                Code::SUCCESS.into()
            }
            Err(err) => {
                error!("stmt get col fields error, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
            }
        },
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    }
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

fn stmt_get_tag_fields(stmt: &mut Stmt) -> TaosResult<&Vec<StmtField>> {
    if stmt.tag_fields().is_none() {
        let fields = stmt.get_tag_fields()?;
        stmt.with_tag_fields(fields);
    }
    Ok(stmt.tag_fields().unwrap())
}

unsafe fn stmt_set_tbname_tags(
    maybe_err: &mut TaosMaybeError<Stmt>,
    name: *const c_char,
    tags: *mut TAOS_MULTI_BIND,
) -> TaosResult<()> {
    let stmt = maybe_err
        .deref_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "data is null"))?;

    let name = CStr::from_ptr(name).to_str().unwrap();
    stmt.set_tbname(name)?;

    let fields = stmt_get_tag_fields(stmt)?;
    let tags = std::slice::from_raw_parts(tags, fields.len())
        .iter()
        .map(TAOS_MULTI_BIND::to_tag_value)
        .collect_vec();

    stmt.set_tags(&tags)?;

    Ok(())
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

            let sql = c"insert into ? using s0 tags (?) values (?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let name = c"d0";
            let buffer = vec![1];
            let length = vec![4];
            let is_null = vec![0];
            let mut tags = vec![TAOS_MULTI_BIND {
                buffer_type: TSDB_DATA_TYPE_INT as _,
                buffer: buffer.as_ptr() as _,
                buffer_length: 4,
                length: length.as_ptr() as _,
                is_null: is_null.as_ptr() as _,
                num: 1,
            }];

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

            // let fields = Vec::from_raw_parts(fields, field_num as _, field_num as _);
            // println!("fields: {fields:?}");

            test_exec(taos, "drop database test_1738740951");
        }
    }
}
