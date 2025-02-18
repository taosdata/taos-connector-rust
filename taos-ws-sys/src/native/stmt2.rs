use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::{ptr, slice, str};

use taos_error::{Code, Error as RawError};
use taos_query::block_in_place_or_global;
use taos_query::common::{ColumnView, Timestamp, Ty, Value};
use taos_query::stmt2::{Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_ws::query::BindType;
use taos_ws::{ResultSet, Stmt2, Taos};
use tracing::{error, trace, Instrument};

use crate::native::error::{
    clear_error_info, format_errno, set_err_and_get_code, TaosError, TaosMaybeError,
};
use crate::native::{TaosResult, __taos_async_fn_t, TAOS, TAOS_RES};

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

struct TaosStmt2 {
    stmt2: Stmt2,
    async_exec_fn: Option<__taos_async_fn_t>,
    userdata: *mut c_void,
}

impl TaosStmt2 {
    fn new(stmt2: Stmt2, async_exec_fn: Option<__taos_async_fn_t>, userdata: *mut c_void) -> Self {
        Self {
            stmt2,
            async_exec_fn,
            userdata,
        }
    }
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

impl TAOS_STMT2_BIND {
    fn to_value(&self) -> Value {
        if !self.is_null.is_null() && unsafe { self.is_null.read() != 0 } {
            return Value::Null(self.ty());
        }

        assert!(!self.buffer.is_null());
        assert!(!self.length.is_null());

        match self.ty() {
            Ty::Null => Value::Null(self.ty()),
            Ty::Bool => unsafe { Value::Bool(*(self.buffer as *const _)) },
            Ty::TinyInt => unsafe { Value::TinyInt(*(self.buffer as *const _)) },
            Ty::SmallInt => unsafe { Value::SmallInt(*(self.buffer as *const _)) },
            Ty::Int => unsafe { Value::Int(*(self.buffer as *const _)) },
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
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                let val = str::from_utf8_unchecked(slice).to_owned();
                Value::VarChar(val)
            },
            Ty::NChar => unsafe {
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                let val = std::str::from_utf8_unchecked(slice).to_owned();
                Value::NChar(val)
            },
            Ty::Json => unsafe {
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                let val = serde_json::from_slice(slice).unwrap();
                Value::Json(val)
            },
            Ty::VarBinary => unsafe {
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                Value::VarBinary(slice.into())
            },
            Ty::Geometry => unsafe {
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                Value::Geometry(slice.into())
            },
            _ => todo!(),
        }
    }

    fn to_column_view(&self) -> ColumnView {
        let num = self.num as usize;
        let is_nulls = unsafe { slice::from_raw_parts(self.is_null, num) };
        let lens = unsafe { slice::from_raw_parts(self.length, num) };
        let len = *lens.iter().max().unwrap() as usize;

        trace!("to_column_view, is_nulls: {is_nulls:?}, lens: {lens:?}, max_len: {len}");

        match self.ty() {
            Ty::Bool => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const bool, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_bools(vals)
            }
            Ty::TinyInt => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const i8, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_tiny_ints(vals)
            }
            Ty::SmallInt => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const i16, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_small_ints(vals)
            }
            Ty::Int => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const i32, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_ints(vals)
            }
            Ty::BigInt => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const i64, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_big_ints(vals)
            }
            Ty::UTinyInt => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_unsigned_tiny_ints(vals)
            }
            Ty::USmallInt => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u16, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_unsigned_small_ints(vals)
            }
            Ty::UInt => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u32, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_unsigned_ints(vals)
            }
            Ty::UBigInt => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u64, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_unsigned_big_ints(vals)
            }
            Ty::Float => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const f32, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_floats(vals)
            }
            Ty::Double => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const f64, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_doubles(vals)
            }
            Ty::Timestamp => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const i64, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                ColumnView::from_millis_timestamp(vals)
            }
            Ty::VarChar => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len * num) };
                let mut vals = vec![None; num];
                let mut idx = 0;
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        let bytes = &slice[idx..idx + lens[i] as usize];
                        idx += lens[i] as usize;
                        vals[i] = unsafe { Some(str::from_utf8_unchecked(bytes)) };
                    }
                }
                ColumnView::from_varchar::<&str, _, _, _>(vals)
            }
            Ty::NChar => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len * num) };
                let mut vals = vec![None; num];
                let mut idx = 0;
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        let bytes = &slice[idx..idx + lens[i] as usize];
                        idx += lens[i] as usize;
                        vals[i] = unsafe { Some(str::from_utf8_unchecked(bytes)) };
                    }
                }
                ColumnView::from_nchar::<&str, _, _, _>(vals)
            }
            Ty::Json => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len * num) };
                let mut vals = vec![None; num];
                let mut idx = 0;
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        let bytes = &slice[idx..idx + lens[i] as usize];
                        idx += lens[i] as usize;
                        vals[i] = serde_json::from_slice(bytes).unwrap();
                    }
                }
                ColumnView::from_json::<&str, _, _, _>(vals)
            }
            Ty::VarBinary => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len * num) };
                let mut vals = vec![None; num];
                let mut idx = 0;
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        if is_nulls[i] == 0 {
                            let val = &slice[idx..idx + lens[i] as usize];
                            idx += lens[i] as usize;
                            vals[i] = Some(val);
                        }
                    }
                }
                ColumnView::from_bytes::<&[u8], _, _, _>(vals)
            }
            Ty::Geometry => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len * num) };
                let mut vals = vec![None; num];
                let mut idx = 0;
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        let val = &slice[idx..idx + lens[i] as usize];
                        idx += lens[i] as usize;
                        vals[i] = Some(val);
                    }
                }
                ColumnView::from_geobytes::<&[u8], _, _, _>(vals)
            }
            _ => todo!(),
        }
    }

    fn ty(&self) -> Ty {
        self.buffer_type.into()
    }
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BINDV {
    pub count: c_int,
    pub tbnames: *mut *mut c_char,
    pub tags: *mut *mut TAOS_STMT2_BIND,
    pub bind_cols: *mut *mut TAOS_STMT2_BIND,
}

impl TAOS_STMT2_BINDV {
    fn to_bind_params(&self, stmt2: &Stmt2) -> Vec<Stmt2BindParam> {
        let cnt = self.count as usize;

        let mut tag_cnt = 0;
        let mut col_cnt = 0;

        if stmt2.is_insert().unwrap() {
            for field in stmt2.fields().unwrap() {
                match field.bind_type {
                    BindType::Tag => tag_cnt += 1,
                    BindType::Column => col_cnt += 1,
                    BindType::TableName => {}
                }
            }
        } else {
            col_cnt = stmt2.fields_count().unwrap();
        }

        let mut tbnames = vec![None; cnt];
        if !self.tbnames.is_null() {
            let slice = unsafe { slice::from_raw_parts(self.tbnames, cnt) };
            for i in 0..cnt {
                if !slice[i].is_null() {
                    let tbname = unsafe { CStr::from_ptr(slice[i]).to_str().unwrap().to_owned() };
                    tbnames[i] = Some(tbname);
                }
            }
        }

        let mut tags = vec![None; cnt];
        if !self.tags.is_null() && tag_cnt > 0 {
            let slice = unsafe { slice::from_raw_parts(self.tags, cnt) };
            for i in 0..cnt {
                let mut vals = Vec::with_capacity(tag_cnt);
                let binds = unsafe { slice::from_raw_parts(slice[i], tag_cnt) };
                for bind in binds {
                    vals.push(bind.to_value());
                }
                tags[i] = Some(vals);
            }
        }

        let mut cols = vec![None; cnt];
        if !self.bind_cols.is_null() && col_cnt > 0 {
            let slice = unsafe { slice::from_raw_parts(self.bind_cols, cnt) };
            for i in 0..cnt {
                let mut views = Vec::with_capacity(col_cnt);
                let binds = unsafe { slice::from_raw_parts(slice[i], col_cnt) };
                for bind in binds {
                    views.push(bind.to_column_view());
                }
                cols[i] = Some(views);
            }
        }

        let mut params = Vec::with_capacity(cnt);
        for i in 0..cnt {
            let param = Stmt2BindParam::new(tbnames[i].take(), tags[i].take(), cols[i].take());
            params.push(param);
        }

        params
    }
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
    let taos_stmt2: TaosMaybeError<TaosStmt2> = stmt2_init(taos, option).into();
    Box::into_raw(Box::new(taos_stmt2)) as _
}

unsafe fn stmt2_init(taos: *mut TAOS, option: *mut TAOS_STMT2_OPTION) -> TaosResult<TaosStmt2> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is invalid"))?;

    let mut stmt2 = Stmt2::new(taos.client());

    let mut req_id = generate_req_id();
    let mut single_stb_insert = true;
    let mut single_table_bind_once = false;
    let mut async_exec_fn = None;
    let mut userdata = ptr::null_mut();
    if !option.is_null() {
        let option = option.as_ref().unwrap();
        req_id = option.reqid as _;
        single_stb_insert = option.singleStbInsert;
        single_table_bind_once = option.singleTableBindOnce;
        userdata = option.userdata;
        let exec_fn = option.asyncExecFn as *const ();
        if !exec_fn.is_null() {
            async_exec_fn = Some(option.asyncExecFn);
        }
    }

    trace!("stmt2_init, req_id: {req_id}, single_stb_insert: {single_stb_insert},
        single_table_bind_once: {single_table_bind_once}, async_exec_fn: {async_exec_fn:?}, userdata: {userdata:?}");

    block_in_place_or_global(stmt2.init_with_options(
        req_id,
        single_stb_insert,
        single_table_bind_once,
    ))?;

    Ok(TaosStmt2::new(stmt2, async_exec_fn, userdata))
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_prepare(
    stmt: *mut TAOS_STMT2,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => {
            let sql = if length > 0 {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql as _, length as _))
            } else {
                CStr::from_ptr(sql).to_str().expect(
                    "taos_stmt2_prepare with a sql len 0 means the input should always be valid utf-8",
                )
            };

            if let Some(errno) = maybe_err.errno() {
                return format_errno(errno);
            }

            if let Err(err) = maybe_err
                .deref_mut()
                .ok_or_else(|| RawError::from_string("data is null"))
                .and_then(|taos_stmt2| taos_stmt2.stmt2.prepare(sql))
            {
                error!("stmt2 prepare error, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
            } else {
                maybe_err.with_err(None);
                clear_error_info();
                Code::SUCCESS.into()
            }
        }
        None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    }
}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_bind_param(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
) -> c_int {
    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => &mut taos_stmt2.stmt2,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    let bindv = match bindv.as_ref() {
        Some(bindv) => bindv,
        None => {
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "bindv is invalid"))
        }
    };

    let params = bindv.to_bind_params(stmt2);
    trace!("taos_stmt2_bind_param, params: {params:?}");

    match stmt2.bind(&params) {
        Ok(_) => {
            maybe_err.with_err(None);
            clear_error_info();
            Code::SUCCESS.into()
        }
        Err(err) => {
            error!("stmt2 bind failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
        }
    }
}

#[derive(Debug)]
pub struct SafePtr<T>(T);

unsafe impl<T> Send for SafePtr<T> {}
unsafe impl<T> Sync for SafePtr<T> {}

#[no_mangle]
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_exec(
    stmt: *mut TAOS_STMT2,
    affected_rows: *mut c_int,
) -> c_int {
    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    let taos_stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => taos_stmt2,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    if taos_stmt2.async_exec_fn.is_none() {
        match taos_stmt2.stmt2.exec() {
            Ok(rows) => {
                *affected_rows = rows as _;
                maybe_err.with_err(None);
                clear_error_info();
                return Code::SUCCESS.into();
            }
            Err(err) => {
                error!("stmt2 sync exec failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                return set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
            }
        }
    }

    let stmt2 = SafePtr(&mut taos_stmt2.stmt2 as *mut _);
    let userdata = SafePtr(taos_stmt2.userdata);
    let cb = taos_stmt2.async_exec_fn.unwrap();

    block_in_place_or_global(
        async {
            tokio::spawn(async move {
                use taos_query::stmt2::Stmt2AsyncBindable;

                let stmt2 = stmt2;
                let userdata = userdata;

                let stmt2 = unsafe { &mut *stmt2.0 };
                if let Err(err) = Stmt2AsyncBindable::exec(stmt2).await {
                    error!("stmt2 async exec failed, err: {err:?}");
                    let code = format_errno(err.code().into());
                    cb(userdata.0, ptr::null_mut(), code as _);
                    return;
                }

                match Stmt2AsyncBindable::result_set(stmt2).await {
                    Ok(res) => {
                        let res: *mut ResultSet = Box::into_raw(Box::new(res));
                        cb(userdata.0, res as _, Code::SUCCESS.into());
                    }
                    Err(err) => {
                        error!("stmt2 async exec failed, err: {err:?}");
                        let code = format_errno(err.code().into());
                        cb(userdata.0, ptr::null_mut(), code as _);
                    }
                }
            })
        }
        .in_current_span(),
    );

    maybe_err.with_err(None);
    clear_error_info();
    return Code::SUCCESS.into();
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_close(stmt: *mut TAOS_STMT2) -> c_int {
    let _ = Box::from_raw(stmt as *mut TaosMaybeError<TaosStmt2>);
    Code::SUCCESS.into()
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
    use crate::native::{test_connect, test_exec, test_exec_many};

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

            let mut option = TAOS_STMT2_OPTION {
                reqid: 1001,
                singleStbInsert: true,
                singleTableBindOnce: false,
                asyncExecFn: mem::transmute::<*const (), __taos_async_fn_t>(ptr::null()),
                userdata: ptr::null_mut(),
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let len = sql.to_bytes().len();
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), len as _);
            assert_eq!(code, 0);

            let mut buffer = vec![1739763276172i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = TAOS_STMT2_BIND {
                buffer_type: Ty::Timestamp as _,
                buffer: buffer.as_mut_ptr() as _,
                length: length.as_mut_ptr(),
                is_null: is_null.as_mut_ptr(),
                num: buffer.len() as _,
            };

            let mut buffer = vec![2];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: buffer.as_mut_ptr() as _,
                length: length.as_mut_ptr(),
                is_null: is_null.as_mut_ptr(),
                num: buffer.len() as _,
            };

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

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739274502");
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
                    "create table s0 (ts timestamp, c1 int, c2 varchar(20)) tags (t1 int)",
                ],
            );

            let mut option = TAOS_STMT2_OPTION {
                reqid: 1001,
                singleStbInsert: true,
                singleTableBindOnce: false,
                asyncExecFn: mem::transmute::<*const (), __taos_async_fn_t>(ptr::null()),
                userdata: ptr::null_mut(),
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?, ?)";
            let len = sql.to_bytes().len();
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), len as _);
            assert_eq!(code, 0);

            let tbname1 = c"d0";
            let tbname2 = c"d1";
            let mut tbnames = vec![tbname1.as_ptr() as _, tbname2.as_ptr() as _];

            let mut buffer = vec![1999];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t1 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: buffer.as_mut_ptr() as _,
                length: length.as_mut_ptr(),
                is_null: is_null.as_mut_ptr(),
                num: buffer.len() as _,
            };

            let mut tag1 = vec![t1];
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
            let ts = TAOS_STMT2_BIND {
                buffer_type: Ty::Timestamp as _,
                buffer: buffer.as_mut_ptr() as _,
                length: length.as_mut_ptr(),
                is_null: is_null.as_mut_ptr(),
                num: buffer.len() as _,
            };

            let mut buffer = vec![1, 2, 3, 4];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 0];
            let c1 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: buffer.as_mut_ptr() as _,
                length: length.as_mut_ptr(),
                is_null: is_null.as_mut_ptr(),
                num: buffer.len() as _,
            };

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c2 = TAOS_STMT2_BIND {
                buffer_type: Ty::VarChar as _,
                buffer: buffer.as_mut_ptr() as _,
                length: length.as_mut_ptr(),
                is_null: is_null.as_mut_ptr(),
                num: length.len() as _,
            };

            let mut col1 = vec![ts, c1, c2];
            let mut col2 = col1.clone();
            let mut cols = vec![col1.as_mut_ptr(), col2.as_mut_ptr()];

            let mut bindv = TAOS_STMT2_BINDV {
                count: tbnames.len() as _,
                tbnames: tbnames.as_mut_ptr(),
                tags: tags.as_mut_ptr(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739502440");
        }
    }

    #[test]
    fn test_taos_stmt2_exec() {
        unsafe {
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

            extern "C" fn fp(userdata: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                assert_eq!(code, 0);
                assert!(!res.is_null());

                let userdata = unsafe { CStr::from_ptr(userdata as _) };
                assert_eq!(userdata, c"hello, world");
            }

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
            let len = sql.to_bytes().len();
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), len as _);
            assert_eq!(code, 0);

            let mut buffer = vec![1];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: buffer.as_mut_ptr() as _,
                length: length.as_mut_ptr(),
                is_null: is_null.as_mut_ptr(),
                num: buffer.len() as _,
            };

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
        }
    }
}
