use std::ffi::c_void;
use std::fmt::Debug;
use std::os::raw::*;

use taos_query::block_in_place_or_global;
use taos_query::common::Value;
use taos_query::prelude::Itertools;
use taos_query::stmt::Bindable;
use taos_ws::Stmt;

use crate::*;

/// Opaque STMT type alias.
#[allow(non_camel_case_types)]
pub type WS_STMT = c_void;

unsafe fn stmt_init(taos: *const WS_TAOS) -> WsResult<Stmt> {
    let client = (taos as *mut Taos)
        .as_mut()
        .ok_or(WsError::new(Code::Failed, "client pointer it null"))?;
    Ok(taos_ws::Stmt::init(client)?)
    // Ok(client.stmt_init()?)
}

/// Create new stmt object.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_init(taos: *const WS_TAOS) -> *mut WS_STMT {
    let stmt: WsMaybeError<Stmt> = stmt_init(taos).into();
    Box::into_raw(Box::new(stmt)) as _
}

/// Prepare with sql command
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_prepare(
    stmt: *mut WS_STMT,
    sql: *const c_char,
    len: c_ulong,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let sql = if len > 0 {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql as _, len as _))
            } else {
                CStr::from_ptr(sql).to_str().expect(
                    "ws_stmt_prepare with a sql len 0 means the input should always be valid utf-8",
                )
            };

            if let Some(no) = stmt.errno() {
                return no;
            }

            if let Err(e) = stmt.prepare(sql) {
                let errno = e.errno();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                errno.into()
            } else {
                0
            }
        }
        _ => 0,
    }
}

/// Set table name.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_set_tbname(stmt: *mut WS_STMT, name: *const c_char) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let name = CStr::from_ptr(name).to_str().unwrap();

            if let Err(e) = stmt.set_tbname(name) {
                let errno = e.errno();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                errno.into()
            } else {
                0
            }
        }
        _ => 0,
    }
}

/// Set table name and tags.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_set_tbname_tags(
    stmt: *mut WS_STMT,
    name: *const c_char,
    bind: *const WS_MULTI_BIND,
    len: u32,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let name = CStr::from_ptr(name).to_str().unwrap();
            let tags = std::slice::from_raw_parts(bind, len as usize)
                .iter()
                .map(|bind| bind.to_tag_value())
                .collect_vec();

            if let Err(e) = stmt.set_tbname_tags(name, &tags) {
                let errno = e.errno();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                errno.into()
            } else {
                0
            }
        }
        _ => 0,
    }
}

/// Currently only insert sql is supported.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_is_insert(stmt: *mut WS_STMT, insert: *mut c_int) -> c_int {
    let _ = stmt;
    *insert = 1;
    0
}

#[derive(Debug)]
#[repr(transparent)]
#[allow(non_camel_case_types)]
pub struct WS_BIND(TaosMultiBind);

#[repr(C)]
#[derive(Debug, Clone)]
pub struct TaosMultiBind {
    pub buffer_type: c_int,
    pub buffer: *const c_void,
    pub buffer_length: usize,
    pub length: *const i32,
    pub is_null: *const c_char,
    pub num: c_int,
}

#[allow(non_camel_case_types)]
pub type WS_MULTI_BIND = TaosMultiBind;

impl TaosMultiBind {
    pub fn new(ty: Ty) -> Self {
        Self {
            buffer_type: ty as _,
            buffer: std::ptr::null_mut(),
            buffer_length: 0,
            length: std::ptr::null_mut(),
            is_null: std::ptr::null_mut(),
            num: 1,
        }
    }

    pub fn ty(&self) -> Ty {
        self.buffer_type.into()
    }

    pub fn first_to_json(&self) -> serde_json::Value {
        self.to_json().as_array().unwrap().first().unwrap().clone()
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
                // let v = std::str::from_utf8_unchecked(slice);
                Value::Json(serde_json::from_slice(slice).unwrap())
            },
            _ => todo!(),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::json;
        use serde_json::Value;
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
                Ty::Float => _impl_primitive!(f32),
                Ty::Double => _impl_primitive!(f64),
                Ty::VarChar => {
                    let len = self.num as usize;
                    if self.is_null.is_null() {
                        let column = (0..len)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.offset(i as isize) as usize;
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
                                let len = *self.length.offset(i as isize) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                Some(std::str::from_utf8_unchecked(bytes))
                                // Some(bytes.escape_ascii().to_string())
                            }
                        })
                        .collect::<Vec<_>>();
                    json!(column)
                }
                Ty::Timestamp => _impl_primitive!(i64),
                Ty::NChar => {
                    let len = self.num as usize;
                    if self.is_null.is_null() {
                        let column = (0..len)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.offset(i as isize) as usize;
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
                                let len = *self.length.offset(i as isize) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                Some(std::str::from_utf8_unchecked(bytes))
                            }
                        })
                        .collect::<Vec<_>>();
                    json!(column)
                }
                Ty::UTinyInt => _impl_primitive!(u8),
                Ty::USmallInt => _impl_primitive!(u16),
                Ty::UInt => _impl_primitive!(u32),
                Ty::UBigInt => _impl_primitive!(u64),
                Ty::Json => {
                    let len = self.num as usize;
                    if self.is_null.is_null() {
                        let column = (0..len)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.offset(i as isize) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                                // serde_json::from_slice::<serde_json::Value>(bytes)
                                //     .expect("input should be valid json format")
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
                                let len = *self.length.offset(i as isize) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                Some(serde_json::from_slice::<serde_json::Value>(bytes).unwrap())
                            }
                        })
                        .collect::<Vec<_>>();
                    json!(column)
                }
                Ty::VarBinary => todo!(),
                Ty::Decimal => todo!(),
                Ty::Blob => todo!(),
                Ty::MediumBlob => todo!(),
                _ => todo!(),
            }
        }
        // serde_json::json!([1, 2])
    }
}

#[cfg(test)]
impl TaosMultiBind {
    // pub(crate) fn nulls(n: usize) -> Self {
    //     TaosMultiBind {
    //         buffer_type: Ty::Null as _,
    //         buffer: std::ptr::null_mut(),
    //         buffer_length: 0,
    //         length: n as _,
    //         is_null: std::ptr::null_mut(),
    //         num: n as _,
    //     }
    // }
    pub(crate) fn from_primitives<T: taos_query::common::itypes::IValue>(
        nulls: Vec<bool>,
        values: &[T],
    ) -> Self {
        TaosMultiBind {
            buffer_type: T::TY as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<T>(),
            length: values.len() as _,
            is_null: std::mem::ManuallyDrop::new(nulls).as_ptr() as _,
            num: values.len() as _,
        }
    }
    pub(crate) fn from_raw_timestamps(nulls: Vec<bool>, values: &[i64]) -> Self {
        TaosMultiBind {
            buffer_type: Ty::Timestamp as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<i64>(),
            length: values.len() as _,
            is_null: std::mem::ManuallyDrop::new(nulls).as_ptr() as _,
            num: values.len() as _,
        }
    }

    pub(crate) fn from_binary_vec(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        let mut buffer_length = 0;
        let num = values.len();
        let mut nulls = std::mem::ManuallyDrop::new(Vec::with_capacity(num));
        nulls.resize(num, false);
        let mut length: std::mem::ManuallyDrop<Vec<i32>> =
            std::mem::ManuallyDrop::new(Vec::with_capacity(num));
        // unsafe { length.set_len(num) };
        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                // length[i] = v.len() as _;
                length.push(v.len() as _);
                if v.len() > buffer_length {
                    buffer_length = v.len();
                }
            } else {
                length.push(-1);
                nulls[i] = true;
            }
        }
        let buffer_size = buffer_length * values.len();
        let mut buffer: std::mem::ManuallyDrop<Vec<u8>> =
            std::mem::ManuallyDrop::new(Vec::with_capacity(buffer_size));
        unsafe { buffer.set_len(buffer_size) };
        buffer.fill(0);
        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                unsafe {
                    let dst = buffer.as_mut_ptr().add(buffer_length * i);
                    std::intrinsics::copy_nonoverlapping(v.as_ptr(), dst, v.len());
                }
            }
        }
        TaosMultiBind {
            buffer_type: Ty::VarChar as _,
            buffer: buffer.as_ptr() as _,
            buffer_length,
            length: length.as_ptr() as _,
            is_null: nulls.as_ptr() as _,
            num: num as _,
        }
    }
    pub(crate) fn from_string_vec(values: &[Option<impl AsRef<str>>]) -> Self {
        let values: Vec<_> = values
            .iter()
            .map(|f| f.as_ref().map(|s| s.as_ref().as_bytes()))
            .collect();
        let mut s = Self::from_binary_vec(&values);
        s.buffer_type = Ty::NChar as _;
        s
    }
    // pub(crate) fn from_json(values: &[Option<impl AsRef<str>>]) -> Self {
    //     let values: Vec<_> = values
    //         .iter()
    //         .map(|f| f.as_ref().map(|s| s.as_ref().as_bytes()))
    //         .collect();
    //     let mut s = Self::from_binary_vec(&values);
    //     s.buffer_type = Ty::Json as _;
    //     s
    // }

    // pub(crate) fn buffer(&self) -> *const c_void {
    //     self.buffer
    // }
}

impl Drop for TaosMultiBind {
    fn drop(&mut self) {
        unsafe { Vec::from_raw_parts(self.is_null as *mut i8, self.num as _, self.num as _) };
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_stmt_set_tags(
    stmt: *mut WS_STMT,
    bind: *const WS_MULTI_BIND,
    len: u32,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let columns = std::slice::from_raw_parts(bind, len as usize)
                .iter()
                .map(|bind| bind.to_tag_value())
                .collect_vec();

            if let Err(e) = stmt.set_tags(&columns) {
                let errno = e.errno();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                errno.into()
            } else {
                0
            }
        }
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_stmt_bind_param_batch(
    stmt: *mut WS_STMT,
    bind: *const WS_MULTI_BIND,
    len: u32,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let columns = std::slice::from_raw_parts(bind, len as usize)
                .iter()
                .map(|bind| bind.to_json())
                .collect();

            if let Err(e) = block_in_place_or_global(stmt.stmt_bind(columns)) {
                let errno = e.errno();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                errno.into()
            } else {
                0
            }
        }
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_stmt_add_batch(stmt: *mut WS_STMT) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            if let Err(e) = stmt.add_batch() {
                let errno = e.errno();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                errno.into()
            } else {
                0
            }
        }
        _ => 0,
    }
}

/// Execute the bind batch, get inserted rows in `affected_row` pointer.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_execute(stmt: *mut WS_STMT, affected_rows: *mut i32) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => match stmt.execute() {
            Ok(rows) => {
                *affected_rows = rows as _;
                0
            }
            Err(e) => {
                let errno = e.errno();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                errno.into()
            }
        },
        _ => 0,
    }
}

/// Get inserted rows in current statement.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_affected_rows(stmt: *mut WS_STMT) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => stmt.affected_rows() as _,
        _ => 0,
    }
}

/// Equivalent to ws_errstr
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_errstr(stmt: *mut WS_STMT) -> *const c_char {
    ws_errstr(stmt as _)
}

/// Same to taos_stmt_close
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_close(stmt: *mut WS_STMT) {
    let _ = Box::from_raw(stmt as *mut WsMaybeError<Stmt>);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stmt_common() {
        use crate::*;
        init_env();
        unsafe {
            let taos = ws_connect_with_dsn(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! query {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            query!(b"drop database if exists ws_stmt_i\0");
            query!(b"create database ws_stmt_i keep 36500\0");
            query!(b"create table ws_stmt_i.s1 (ts timestamp, v int, b binary(100))\0");

            let stmt = ws_stmt_init(taos);

            let sql = "insert into ? values(?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            let code = ws_stmt_set_tbname(stmt, b"ws_stmt_i.`s1`\0".as_ptr() as _);

            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }
            let params = vec![
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[2, 3]),
                TaosMultiBind::from_binary_vec(&vec![None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);
            assert_eq!(rows, 2);

            assert_eq!(rows, 2);
            ws_stmt_close(stmt);
            query!(b"drop database ws_stmt_i\0");
        }
    }

    #[test]
    fn stmt_child() {
        use crate::*;
        init_env();
        unsafe {
            let taos = ws_connect_with_dsn(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! query {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            query!(b"drop database if exists ws_stmt_i_child\0");
            query!(b"create database ws_stmt_i_child keep 36500\0");
            query!(b"use ws_stmt_i_child\0");
            query!(b"CREATE STABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(16))\0");
            query!(b"CREATE TABLE `d0` USING `meters` (`groupid`, `location`) TAGS (7, \"San Francisco\")\0");

            let stmt = ws_stmt_init(taos);

            let sql = "insert into ? values(?, ?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            let code = ws_stmt_set_tbname(stmt, b"ws_stmt_i_child.`d0`\0".as_ptr() as _);

            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }
            let params = vec![
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[0.0f32, 0.1f32]),
                TaosMultiBind::from_primitives(vec![false, false], &[0, 0]),
                TaosMultiBind::from_primitives(vec![false, false], &[0.0f32, 0.1f32]),
                // TaosMultiBind::from_binary_vec(&vec![None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);
            assert_eq!(rows, 2);

            assert_eq!(rows, 2);
            ws_stmt_close(stmt);
            // query!(b"drop database ws_stmt_i\0");
        }
    }

    #[test]
    fn stmt_tiny_int_null() {
        use crate::*;
        init_env();
        unsafe {
            let taos = ws_connect_with_dsn(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! query {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            query!(b"drop database if exists ws_stmt_i_null\0");
            query!(b"create database ws_stmt_i_null keep 36500\0");
            query!(b"use ws_stmt_i_null\0");
            query!(b"create table st(ts timestamp, c1 TINYINT UNSIGNED) tags(utntag TINYINT UNSIGNED)\0");
            query!(b"create table t1 using st tags(0)\0");
            query!(b"create table t2 using st tags(255)\0");
            query!(b"create table t3 using st tags(NULL)\0");

            let stmt = ws_stmt_init(taos);

            let sql = "insert into ? values(?,?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            for tbname in ["t1", "t2", "t3"] {
                let name = format!("ws_stmt_i_null.`{}`\0", tbname);
                let code = ws_stmt_set_tbname(stmt, name.as_ptr() as _);

                if code != 0 {
                    dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                    panic!()
                }
                let params = vec![
                    TaosMultiBind::from_raw_timestamps(vec![false], &[0]),
                    TaosMultiBind::from_primitives(vec![true], &[0u8]),
                ];
                let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
                if code != 0 {
                    dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                    panic!()
                }

                ws_stmt_add_batch(stmt);
                let mut rows = 0;
                ws_stmt_execute(stmt, &mut rows);
                assert_eq!(rows, 1);

                let sql = format!("select * from st where tbname = '{tbname}'\0");
                let rs = ws_query(taos, sql.as_bytes().as_ptr() as _);
                let code = ws_errno(rs);
                assert!(code == 0);
                loop {
                    let mut ptr = std::ptr::null();
                    let mut rows = 0;
                    ws_fetch_block(rs, &mut ptr, &mut rows);
                    if rows == 0 {
                        break;
                    }
                    for row in 0..rows {
                        print!("{tbname} row {row}: ");
                        for col in 0..3 {
                            let mut ty = Ty::Null;
                            let mut len = 0;
                            let v = ws_get_value_in_block(
                                rs,
                                row,
                                col,
                                &mut ty as *mut _ as _,
                                &mut len,
                            );
                            if v.is_null() {
                                print!(",NULL");
                            } else {
                                match ty {
                                    Ty::Timestamp => print!("ts: {}", *(v as *const i64)),
                                    _ => print!(",{}", *(v as *const u8)),
                                }
                            }
                        }
                        println!("");
                    }
                }
            }

            ws_stmt_close(stmt);
            // query!(b"drop database ws_stmt_i_null\0");
        }
    }

    #[test]
    fn stmt_with_tags() {
        use crate::*;
        init_env();
        unsafe {
            let taos = ws_connect_with_dsn(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_stmt_t\0");
            execute!(b"create database ws_stmt_t keep 36500\0");
            execute!(
                b"create table ws_stmt_t.s1 (ts timestamp, v int, b binary(100)) tags(jt json)\0"
            );

            let stmt = ws_stmt_init(taos);
            let sql = "insert into ? using ws_stmt_t.s1 tags(?) values(?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }
            ws_stmt_set_tbname(stmt, b"ws_stmt_t.t1\0".as_ptr() as _);

            let tags = vec![TaosMultiBind::from_string_vec(&vec![Some(
                r#"{"name":"姓名"}"#.to_string(),
            )])];

            ws_stmt_set_tags(stmt, tags.as_ptr(), tags.len() as _);

            let params = vec![
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[2, 3]),
                TaosMultiBind::from_binary_vec(&vec![None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);

            assert_eq!(rows, 2);
            ws_stmt_close(stmt);

            ws_close(taos)
        }
    }
}

#[test]
fn json_test() {
    use serde_json::json;

    let s = json!(vec![Option::<u8>::None]);
    assert_eq!(dbg!(serde_json::to_string(&s).unwrap()), "[null]");
}
