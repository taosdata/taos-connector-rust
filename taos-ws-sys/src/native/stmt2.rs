use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::{slice, str};

use taos_error::{Code, Error as RawError};
use taos_query::block_in_place_or_global;
use taos_query::common::{ColumnView, Timestamp, Ty, Value};
use taos_query::stmt2::{Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_ws::query::BindType;
use taos_ws::{Stmt2, Taos};
use tracing::{error, trace};

use crate::native::error::{
    clear_error_info, format_errno, set_err_and_get_code, TaosError, TaosMaybeError,
};
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
#[tracing::instrument(level = "trace", ret)]
pub unsafe extern "C" fn taos_stmt2_prepare(
    stmt: *mut TAOS_STMT2,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    match (stmt as *mut TaosMaybeError<Stmt2>).as_mut() {
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
                .and_then(|stmt2| stmt2.prepare(sql))
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
    let maybe_err = match (stmt as *mut TaosMaybeError<Stmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(stmt2) => stmt2,
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

    if let Err(err) = stmt2.bind(&params) {
        error!("stmt2 bind failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        set_err_and_get_code(TaosError::new(err.code(), &err.to_string()))
    } else {
        maybe_err.with_err(None);
        clear_error_info();
        Code::SUCCESS.into()
    }
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
    use crate::native::{test_connect, test_exec_many};

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

            let mut buffer = vec![1739456248i64];
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
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);
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
        }
    }
}
