use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::ptr;

use dashmap::DashMap;
use once_cell::sync::Lazy;
use taos_error::{Code, Error as RawError};
use taos_query::common::{Timestamp, Ty, Value};
use taos_query::prelude::Itertools;
use taos_query::stmt::Bindable;
use taos_query::util::generate_req_id;
use taos_ws::stmt::{StmtField, WsFieldsable};
use taos_ws::{Stmt, Taos};
use tracing::error;

use crate::native::error::{
    clear_error_info, format_errno, set_err_and_get_code, taos_errstr, TaosError, TaosMaybeError,
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

#[cfg(test)]
#[allow(dead_code)]
impl TAOS_MULTI_BIND {
    fn from_primitives<T: taos_query::common::itypes::IValue>(
        nulls: &[bool],
        values: &[T],
    ) -> Self {
        Self {
            buffer_type: T::TY as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<T>(),
            length: values.len() as _,
            is_null: nulls.as_ptr() as _,
            num: values.len() as _,
        }
    }

    fn from_raw_timestamps(nulls: &[bool], values: &[i64]) -> Self {
        Self {
            buffer_type: Ty::Timestamp as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<i64>(),
            length: values.len() as _,
            is_null: nulls.as_ptr() as _,
            num: values.len() as _,
        }
    }

    fn from_binary_vec(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        let mut buf_len = 0;
        let num = values.len();

        let mut nulls = std::mem::ManuallyDrop::new(Vec::with_capacity(num));
        nulls.resize(num, false);

        let mut len = std::mem::ManuallyDrop::new(Vec::with_capacity(num));
        for (i, v) in values.iter().enumerate() {
            match v {
                Some(v) => {
                    let v = v.as_ref();
                    len.push(v.len() as _);
                    if v.len() > buf_len {
                        buf_len = v.len();
                    }
                }
                None => {
                    len.push(-1);
                    nulls[i] = true;
                }
            }
        }

        let buf_size = buf_len * values.len();
        let mut buf = std::mem::ManuallyDrop::new(Vec::with_capacity(buf_size));
        unsafe { buf.set_len(buf_size) };
        buf.fill(0);

        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                unsafe {
                    let dst = buf.as_mut_ptr().add(buf_len * i);
                    std::ptr::copy_nonoverlapping(v.as_ptr(), dst, v.len());
                }
            }
        }

        Self {
            buffer_type: Ty::VarChar as _,
            buffer: buf.as_ptr() as _,
            buffer_length: buf_len,
            length: len.as_ptr() as _,
            is_null: nulls.as_ptr() as _,
            num: num as _,
        }
    }

    fn from_string_vec(values: &[Option<impl AsRef<str>>]) -> Self {
        let values: Vec<_> = values
            .iter()
            .map(|f| f.as_ref().map(|s| s.as_ref().as_bytes()))
            .collect();
        let mut bind = Self::from_binary_vec(&values);
        bind.buffer_type = Ty::NChar as _;
        bind
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
    let len = taos_fields.len();
    let cap = taos_fields.capacity();

    *fieldNum = len as _;

    if !taos_fields.is_empty() {
        *fields = Box::into_raw(taos_fields.into_boxed_slice()) as _;
    }

    STMT_FIELDS_MAP.insert(*fields as usize, (len, cap));

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
    let maybe_err = match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(stmt) => stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt = match maybe_err.deref_mut() {
        Some(stmt) => stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "data is null")),
    };

    let stmt_fields = match stmt_get_col_fields(stmt) {
        Ok(fields) => fields,
        Err(err) => {
            error!("stmt get col fields error, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            return set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
        }
    };

    let taos_fields: Vec<TAOS_FIELD_E> =
        stmt_fields.into_iter().map(|field| field.into()).collect();
    let len = taos_fields.len();
    let cap = taos_fields.capacity();

    *fieldNum = len as _;

    if !taos_fields.is_empty() {
        *fields = Box::into_raw(taos_fields.into_boxed_slice()) as _;
    }

    STMT_FIELDS_MAP.insert(*fields as usize, (len, cap));

    maybe_err.with_err(None);
    clear_error_info();
    Code::SUCCESS.into()
}

static STMT_FIELDS_MAP: Lazy<DashMap<usize, (usize, usize)>> = Lazy::new(DashMap::new);

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_reclaim_fields(stmt: *mut TAOS_STMT, fields: *mut TAOS_FIELD_E) {
    if !fields.is_null() {
        let (_, (len, cap)) = STMT_FIELDS_MAP.remove(&(fields as usize)).unwrap();
        let _ = Vec::from_raw_parts(fields, len, cap);
    }
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int {
    *insert = 1;
    Code::SUCCESS.into()
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int {
    match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => match maybe_err
            .deref_mut()
            .ok_or_else(|| RawError::from_string("data is null"))
            .and_then(Stmt::s_num_params)
        {
            Ok(num) => {
                *nums = num as _;
                maybe_err.with_err(None);
                clear_error_info();
                Code::SUCCESS.into()
            }
            Err(err) => {
                error!("taos_stmt_num_params failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
            }
        },
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    }
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_get_param(
    stmt: *mut TAOS_STMT,
    idx: c_int,
    r#type: *mut c_int,
    bytes: *mut c_int,
) -> c_int {
    match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => match maybe_err
            .deref_mut()
            .ok_or_else(|| RawError::from_string("data is null"))
            .and_then(|stmt| stmt.s_get_param(idx as _))
        {
            Ok(param) => {
                *r#type = param.data_type as _;
                *bytes = param.length as _;
                maybe_err.with_err(None);
                clear_error_info();
                Code::SUCCESS.into()
            }
            Err(err) => {
                error!("taos_stmt_get_param failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
            }
        },
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    }
}

#[no_mangle]
pub extern "C" fn taos_stmt_bind_param(stmt: *mut TAOS_STMT, bind: *mut TAOS_MULTI_BIND) -> c_int {
    todo!()
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_bind_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    let maybe_err = match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt = match maybe_err.deref_mut() {
        Some(stmt) => stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "data is null")),
    };

    let fields = match stmt_get_col_fields(stmt) {
        Ok(fields) => fields,
        Err(err) => {
            error!("stmt_get_col_fields failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            return set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
        }
    };

    let cols = std::slice::from_raw_parts(bind, fields.len())
        .iter()
        .map(TAOS_MULTI_BIND::to_json)
        .collect();

    if let Err(err) = taos_query::block_in_place_or_global(stmt.stmt_bind(cols)) {
        error!("taos_stmt_bind_param_batch failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
    }

    maybe_err.with_err(None);
    clear_error_info();
    Code::SUCCESS.into()
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
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int {
    match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => {
            if let Err(err) = maybe_err
                .deref_mut()
                .ok_or_else(|| RawError::from_string("data is null"))
                .and_then(|stmt| stmt.add_batch())
            {
                error!("taos_stmt_add_batch failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
            } else {
                Code::SUCCESS.into()
            }
        }
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    }
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int {
    match (stmt as *mut TaosMaybeError<Stmt>).as_mut() {
        Some(maybe_err) => match maybe_err
            .deref_mut()
            .ok_or_else(|| RawError::from_string("data is null"))
            .and_then(Bindable::execute)
        {
            Ok(_) => Code::SUCCESS.into(),
            Err(err) => {
                error!("taos_stmt_execute failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
            }
        },
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    }
}

#[no_mangle]
pub extern "C" fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES {
    todo!()
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int {
    let _ = Box::from_raw(stmt as *mut TaosMaybeError<Stmt>);
    Code::SUCCESS.into()
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_errstr(stmt: *mut TAOS_STMT) -> *mut c_char {
    taos_errstr(stmt as _) as _
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int {
    match (stmt as *mut TaosMaybeError<Stmt>)
        .as_mut()
        .and_then(|maybe_err| maybe_err.deref_mut())
    {
        Some(stmt) => stmt.affected_rows() as _,
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    }
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt_affected_rows_once(stmt: *mut TAOS_STMT) -> c_int {
    match (stmt as *mut TaosMaybeError<Stmt>)
        .as_mut()
        .and_then(|maybe_err| maybe_err.deref_mut())
    {
        Some(stmt) => stmt.affected_rows_once() as _,
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    }
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

fn stmt_get_col_fields(stmt: &mut Stmt) -> TaosResult<&Vec<StmtField>> {
    if stmt.col_fields().is_none() {
        let fields = stmt.get_col_fields()?;
        stmt.with_col_fields(fields);
    }
    Ok(stmt.col_fields().unwrap())
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
            let mut tags = vec![TAOS_MULTI_BIND::from_primitives(&[false], &[99])];
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
            assert_eq!(ty, TSDB_DATA_TYPE_INT as i32);
            assert_eq!(bytes, 4);

            let mut cols = vec![
                TAOS_MULTI_BIND::from_raw_timestamps(&[false], &[1738910658659i64]),
                TAOS_MULTI_BIND::from_primitives(&[false], &[20]),
            ];
            let code = taos_stmt_bind_param_batch(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let errstr = taos_stmt_errstr(stmt);
            assert_eq!(CStr::from_ptr(errstr), c"");

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 1);

            let affected_rows_once = taos_stmt_affected_rows_once(stmt);
            assert_eq!(affected_rows_once, 1);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1738740951");
        }
    }
}
