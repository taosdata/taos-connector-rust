use std::alloc::{alloc, dealloc, Layout};
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::{ptr, slice};

use taos_error::Code;
use taos_query::block_in_place_or_global;
use taos_query::common::{ColumnView, Timestamp, Ty, Value};
use taos_query::stmt2::{Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_ws::query::{BindType, Stmt2Field};
use taos_ws::{Stmt2, Taos};
use tracing::{debug, error, trace};

use crate::taos::stmt::{TAOS_MULTI_BIND, TAOS_STMT, TAOS_STMT_OPTIONS};
use crate::taos::{TAOS_FIELD_E, TAOS_RES};
use crate::ws::error::{
    clear_err_and_ret_succ, format_errno, set_err_and_get_code, taos_errstr, TaosError,
    TaosMaybeError,
};
use crate::ws::query::{taos_free_result, QueryResultSet};
use crate::ws::{ResultSet, TaosResult, TAOS};

#[derive(Debug)]
struct TaosStmt {
    stmt2: Stmt2,
    stb_insert: bool,
    tag_fields: Option<Vec<Stmt2Field>>,
    col_fields: Option<Vec<Stmt2Field>>,
    bind_params: Vec<Stmt2BindParam>,
    tbname: Option<String>,
    tags: Option<Vec<Value>>,
    cols: Option<Vec<ColumnView>>,
    single_cols: Option<HashMap<usize, ColumnView>>,
    res: Option<*mut c_void>,
}

impl TaosStmt {
    fn new(stmt2: Stmt2, stb_insert: bool) -> Self {
        Self {
            stmt2,
            stb_insert,
            tag_fields: None,
            col_fields: None,
            bind_params: Vec::new(),
            tbname: None,
            tags: None,
            cols: None,
            single_cols: None,
            res: None,
        }
    }

    fn set_tbname(&mut self, tbname: String) {
        self.tbname = Some(tbname);
    }

    fn set_tags(&mut self, tags: Vec<Value>) {
        self.tags = Some(tags);
    }

    fn set_cols(&mut self, cols: Vec<ColumnView>) {
        self.cols = Some(cols);
    }

    fn set_single_col(&mut self, idx: usize, col: ColumnView) {
        if self.single_cols.is_none() {
            self.single_cols = Some(HashMap::new());
        }
        self.single_cols.as_mut().unwrap().insert(idx, col);
    }

    fn move_to_bind_params(&mut self) -> TaosResult<()> {
        if let Some(cols) = self.cols.take() {
            let param = Stmt2BindParam::new(self.tbname.clone(), self.tags.clone(), Some(cols));
            self.bind_params.push(param);
        }

        if let Some(mut map) = self.single_cols.take() {
            let len = self.col_fields.as_ref().unwrap().len();
            if map.len() != len {
                error!("move_to_bind_params, incorrect number of fields bound");
                return Err(TaosError::new(
                    Code::FAILED,
                    "incorrect number of fields bound",
                ));
            }

            let mut cols = Vec::with_capacity(len);
            for i in 0..len {
                if let Some(col) = map.remove(&i) {
                    debug!("move_to_bind_params, idx: {i}, col: {col:?}");
                    cols.push(col);
                }
            }

            let param = Stmt2BindParam::new(self.tbname.clone(), self.tags.clone(), Some(cols));
            self.bind_params.push(param);
        }

        Ok(())
    }
}

impl Drop for TaosStmt {
    fn drop(&mut self) {
        if let Some(res) = self.res.take() {
            unsafe { taos_free_result(res) };
        }
    }
}

struct StmtFields {
    len: usize,
    fields: [TAOS_FIELD_E; 0],
}

impl StmtFields {
    unsafe fn alloc(len: usize) -> *mut StmtFields {
        let total_size = size_of::<StmtFields>() + len * size_of::<TAOS_FIELD_E>();
        let layout = Layout::from_size_align_unchecked(total_size, align_of::<StmtFields>());
        alloc(layout) as _
    }
}

impl TAOS_MULTI_BIND {
    fn to_value(&self) -> Value {
        debug!("to_value, bind: {self:?}");

        if !self.is_null.is_null() && unsafe { self.is_null.read() != 0 } {
            let val = Value::Null(self.ty());
            debug!("to_value, value: {val:?}");
            return val;
        }

        assert!(!self.length.is_null());
        assert!(!self.buffer.is_null());

        let val = match self.ty() {
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
                let slice = slice::from_raw_parts(self.buffer as _, self.length.read() as _);
                let val = str::from_utf8_unchecked(slice).to_owned();
                Value::VarChar(val)
            },
            Ty::NChar => unsafe {
                let slice = slice::from_raw_parts(self.buffer as _, self.length.read() as _);
                let val = str::from_utf8_unchecked(slice).to_owned();
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
        };

        debug!("to_value, value: {val:?}");

        val
    }

    fn to_column_view(&self) -> ColumnView {
        debug!("to_column_view, bind: {self:?}");

        let ty = self.ty();
        let num = self.num as usize;
        let lens = unsafe { slice::from_raw_parts(self.length, num) };

        let mut is_nulls = None;
        if !self.is_null.is_null() {
            is_nulls = Some(unsafe { slice::from_raw_parts(self.is_null, num) });
        }

        debug!("to_column_view, ty: {ty}, num: {num}, is_nulls: {is_nulls:?}, lens: {lens:?}");

        macro_rules! view {
            ($ty:ty, $from:expr) => {{
                let buf = self.buffer as *const $ty;
                let mut vals = vec![None; num];
                for _ in 0..num {
                    if let Some(is_nulls) = is_nulls {
                        for i in 0..num {
                            if is_nulls[i] == 0 {
                                vals[i] = Some(ptr::read_unaligned(buf.add(i)));
                            }
                        }
                    } else {
                        for i in 0..num {
                            vals[i] = Some(ptr::read_unaligned(buf.add(i)));
                        }
                    }
                }
                $from(vals)
            }};
        }

        let view = unsafe {
            match ty {
                Ty::Bool => view!(bool, ColumnView::from_bools),
                Ty::TinyInt => view!(i8, ColumnView::from_tiny_ints),
                Ty::SmallInt => view!(i16, ColumnView::from_small_ints),
                Ty::Int => view!(i32, ColumnView::from_ints),
                Ty::BigInt => view!(i64, ColumnView::from_big_ints),
                Ty::UTinyInt => view!(u8, ColumnView::from_unsigned_tiny_ints),
                Ty::USmallInt => view!(u16, ColumnView::from_unsigned_small_ints),
                Ty::UInt => view!(u32, ColumnView::from_unsigned_ints),
                Ty::UBigInt => view!(u64, ColumnView::from_unsigned_big_ints),
                Ty::Float => view!(f32, ColumnView::from_floats),
                Ty::Double => view!(f64, ColumnView::from_doubles),
                Ty::Timestamp => view!(i64, ColumnView::from_millis_timestamp),
                Ty::VarChar => {
                    if let Some(is_nulls) = is_nulls {
                        let vals = (0..num)
                            .zip(is_nulls)
                            .map(|(i, is_null)| {
                                if *is_null != 0 {
                                    None
                                } else {
                                    let ptr = (self.buffer as *const u8)
                                        .offset(self.buffer_length as isize * i as isize);
                                    let len = *self.length.add(i) as usize;
                                    let bytes = slice::from_raw_parts(ptr, len);
                                    Some(str::from_utf8_unchecked(bytes))
                                }
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_varchar::<&str, _, _, _>(vals)
                    } else {
                        let vals = (0..num)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_varchar::<&str, _, _, _>(vals)
                    }
                }
                Ty::NChar => {
                    if let Some(is_nulls) = is_nulls {
                        let vals = (0..num)
                            .zip(is_nulls)
                            .map(|(i, is_null)| {
                                if *is_null != 0 {
                                    None
                                } else {
                                    let ptr = (self.buffer as *const u8)
                                        .offset(self.buffer_length as isize * i as isize);
                                    let len = *self.length.add(i) as usize;
                                    let bytes = slice::from_raw_parts(ptr, len);
                                    Some(std::str::from_utf8_unchecked(bytes))
                                }
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_nchar::<&str, _, _, _>(vals)
                    } else {
                        let vals = (0..num)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_nchar::<&str, _, _, _>(vals)
                    }
                }
                Ty::VarBinary => {
                    if let Some(is_nulls) = is_nulls {
                        let vals = (0..num)
                            .zip(is_nulls)
                            .map(|(i, is_null)| {
                                if *is_null != 0 {
                                    None
                                } else {
                                    let ptr = (self.buffer as *const u8)
                                        .offset(self.buffer_length as isize * i as isize);
                                    let len = *self.length.add(i) as usize;
                                    Some(slice::from_raw_parts(ptr, len))
                                }
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_bytes::<&[u8], _, _, _>(vals)
                    } else {
                        let vals = (0..num)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                slice::from_raw_parts(ptr, len)
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_bytes::<&[u8], _, _, _>(vals)
                    }
                }
                Ty::Geometry => {
                    if let Some(is_nulls) = is_nulls {
                        let vals = (0..num)
                            .zip(is_nulls)
                            .map(|(i, is_null)| {
                                if *is_null != 0 {
                                    None
                                } else {
                                    let ptr = (self.buffer as *const u8)
                                        .offset(self.buffer_length as isize * i as isize);
                                    let len = *self.length.add(i) as usize;
                                    Some(slice::from_raw_parts(ptr, len))
                                }
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_geobytes::<&[u8], _, _, _>(vals)
                    } else {
                        let vals = (0..num)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                slice::from_raw_parts(ptr, len)
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_geobytes::<&[u8], _, _, _>(vals)
                    }
                }
                _ => todo!(),
            }
        };

        debug!("to_column_view, view: {view:?}");

        view
    }

    fn ty(&self) -> Ty {
        self.buffer_type.into()
    }

    #[cfg(test)]
    pub(crate) fn from_primitives<T: taos_query::common::itypes::IValue>(
        values: &[T],
        nulls: &[bool],
        lens: &[i32],
    ) -> Self {
        Self {
            buffer_type: T::TY as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<T>(),
            length: lens.as_ptr() as _,
            is_null: nulls.as_ptr() as _,
            num: values.len() as _,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_timestamps(values: &[i64], nulls: &[bool], lens: &[i32]) -> Self {
        Self {
            buffer_type: Ty::Timestamp as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<i64>(),
            length: lens.as_ptr() as _,
            is_null: nulls.as_ptr() as _,
            num: values.len() as _,
        }
    }
}

impl From<&Stmt2Field> for TAOS_FIELD_E {
    fn from(field: &Stmt2Field) -> Self {
        let field_name = field.name.as_str();
        let mut name = [0 as c_char; 65];
        unsafe {
            ptr::copy_nonoverlapping(
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

pub unsafe fn taos_stmt_init(taos: *mut TAOS) -> *mut TAOS_STMT {
    taos_stmt_init_with_reqid(taos, generate_req_id() as _)
}

pub unsafe fn taos_stmt_init_with_reqid(taos: *mut TAOS, reqid: i64) -> *mut TAOS_STMT {
    debug!("taos_stmt_init_with_reqid start, taos: {taos:?}, reqid: {reqid}");
    let taos_stmt: TaosMaybeError<TaosStmt> = match stmt_init(taos, reqid as _, false, false) {
        Ok(taos_stmt) => taos_stmt.into(),
        Err(err) => {
            error!("taos_stmt_init_with_reqid failed, err: {err:?}");
            set_err_and_get_code(err);
            return ptr::null_mut();
        }
    };
    debug!("taos_stmt_init_with_reqid, taos_stmt: {taos_stmt:?}");
    let res = Box::into_raw(Box::new(taos_stmt)) as _;
    debug!("taos_stmt_init_with_reqid succ, res: {res:?}");
    res
}

pub unsafe fn taos_stmt_init_with_options(
    taos: *mut TAOS,
    options: *mut TAOS_STMT_OPTIONS,
) -> *mut TAOS_STMT {
    debug!("taos_stmt_init_with_options start, taos: {taos:?}, options: {options:?}");

    if options.is_null() {
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "options is null"));
        return ptr::null_mut();
    }

    let options = &*options;
    let taos_stmt: TaosMaybeError<TaosStmt> = match stmt_init(
        taos,
        options.reqId as _,
        options.singleStbInsert,
        options.singleTableBindOnce,
    ) {
        Ok(taos_stmt) => taos_stmt.into(),
        Err(err) => {
            error!("taos_stmt_init_with_options failed, err: {err:?}");
            set_err_and_get_code(err);
            return ptr::null_mut();
        }
    };
    debug!("taos_stmt_init_with_options, taos_stmt: {taos_stmt:?}");
    let res = Box::into_raw(Box::new(taos_stmt)) as _;
    debug!("taos_stmt_init_with_options succ, res: {res:?}");
    res
}

unsafe fn stmt_init(
    taos: *mut TAOS,
    req_id: u64,
    single_stb_insert: bool,
    single_table_bind_once: bool,
) -> TaosResult<TaosStmt> {
    debug!("stmt_init start, req_id: {req_id}, single_stb_insert: {single_stb_insert}, single_table_bind_once: {single_table_bind_once}");

    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::FAILED, "taos is null"))?;

    let mut stmt2 = Stmt2::new(taos.client());

    block_in_place_or_global(stmt2.init_with_options(
        req_id,
        single_stb_insert,
        single_table_bind_once,
    ))?;

    let taos_stmt = TaosStmt::new(stmt2, single_stb_insert && single_table_bind_once);
    debug!("stmt_init succ, taos_stmt: {taos_stmt:?}");
    Ok(taos_stmt)
}

pub unsafe fn taos_stmt_prepare(
    stmt: *mut TAOS_STMT,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    debug!("taos_stmt_prepare start, stmt: {stmt:?}, sql: {sql:?}, length: {length}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if sql.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "sql is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let sql = if length > 0 {
        // TODO: check utf-8
        str::from_utf8_unchecked(slice::from_raw_parts(sql as _, length as _))
    } else {
        match CStr::from_ptr(sql).to_str() {
            Ok(sql) => sql,
            Err(_) => {
                maybe_err.with_err(Some(TaosError::new(
                    Code::INVALID_PARA,
                    "sql is invalid utf-8",
                )));
                return format_errno(Code::INVALID_PARA.into());
            }
        }
    };

    debug!("taos_stmt_prepare, sql: {sql}");

    let stmt2 = &mut taos_stmt.stmt2;

    if let Err(err) = stmt2.prepare(sql) {
        error!("taos_stmt_prepare failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return format_errno(err.code().into());
    }

    if stmt2.is_insert().unwrap() {
        let mut tag_fields = Vec::new();
        let mut col_fields = Vec::new();

        for field in stmt2.fields().unwrap() {
            if field.bind_type == BindType::Tag {
                tag_fields.push(field.clone());
            } else if field.bind_type == BindType::Column {
                col_fields.push(field.clone());
            }
        }

        taos_stmt.tag_fields = Some(tag_fields);
        taos_stmt.col_fields = Some(col_fields);
    }

    debug!("taos_stmt_prepare succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_set_tbname_tags(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    debug!("taos_stmt_set_tbname_tags, stmt: {stmt:?}, name: {name:?}, tags: {tags:?}");
    let code = taos_stmt_set_tbname(stmt, name);
    if code != 0 {
        return code;
    }
    taos_stmt_set_tags(stmt, tags)
}

pub unsafe fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    debug!("taos_stmt_set_tbname start, stmt: {stmt:?}, name: {name:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let name = match CStr::from_ptr(name).to_str() {
        Ok(name) => name,
        Err(_) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::INVALID_PARA,
                "name is invalid utf-8",
            )));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    taos_stmt.set_tbname(name.to_owned());

    debug!("taos_stmt_set_tbname succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_set_tags(stmt: *mut TAOS_STMT, tags: *mut TAOS_MULTI_BIND) -> c_int {
    debug!("taos_stmt_set_tags start, stmt: {stmt:?}, tags: {tags:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_set_tags can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let tag_cnt = taos_stmt
        .tag_fields
        .as_ref()
        .map_or(0, |fields| fields.len());

    let binds = slice::from_raw_parts(tags, tag_cnt);

    let mut tags = Vec::with_capacity(tag_cnt);
    for bind in binds {
        tags.push(bind.to_value());
    }

    taos_stmt.set_tags(tags);

    debug!("taos_stmt_set_tags succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_set_sub_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    taos_stmt_set_tbname(stmt, name)
}

#[allow(non_snake_case)]
pub unsafe fn taos_stmt_get_tag_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    debug!("taos_stmt_get_tag_fields start, stmt: {stmt:?}, field_num: {fieldNum:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if fieldNum.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fieldNum is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    if fields.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fields is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_get_tag_fields can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let tag_fields: Vec<TAOS_FIELD_E> = taos_stmt
        .tag_fields
        .as_ref()
        .unwrap()
        .iter()
        .map(|field| field.into())
        .collect();

    debug!("taos_stmt_get_tag_fields, fields: {tag_fields:?}");

    let len = tag_fields.len();

    *fieldNum = len as _;
    *fields = ptr::null_mut();

    if !tag_fields.is_empty() {
        let ptr = StmtFields::alloc(len);
        if ptr.is_null() {
            error!("taos_stmt_get_tag_fields failed, alloc null");
            maybe_err.with_err(Some(TaosError::new(Code::FAILED, "alloc failed")));
            return format_errno(Code::FAILED.into());
        }

        (*ptr).len = len;
        let fields_ptr = (*ptr).fields.as_mut_ptr();
        ptr::copy_nonoverlapping(tag_fields.as_ptr(), fields_ptr, len);

        *fields = fields_ptr;
    }

    trace!("taos_stmt_get_tag_fields succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[allow(non_snake_case)]
pub unsafe fn taos_stmt_get_col_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    debug!("taos_stmt_get_col_fields start, stmt: {stmt:?}, field_num: {fieldNum:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if fieldNum.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fieldNum is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    if fields.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fields is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_get_col_fields can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let col_fields: Vec<TAOS_FIELD_E> = taos_stmt
        .col_fields
        .as_ref()
        .unwrap()
        .iter()
        .map(|field| field.into())
        .collect();

    debug!("taos_stmt_get_col_fields, fields: {col_fields:?}");

    let len = col_fields.len();

    *fieldNum = len as _;
    *fields = ptr::null_mut();

    if !col_fields.is_empty() {
        let ptr = StmtFields::alloc(len);
        if ptr.is_null() {
            error!("taos_stmt_get_col_fields failed, alloc null");
            maybe_err.with_err(Some(TaosError::new(Code::FAILED, "alloc failed")));
            return format_errno(Code::FAILED.into());
        }

        (*ptr).len = len;
        let fields_ptr = (*ptr).fields.as_mut_ptr();
        ptr::copy_nonoverlapping(col_fields.as_ptr(), fields_ptr, len);

        *fields = fields_ptr;
    }

    trace!("taos_stmt_get_col_fields succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_reclaim_fields(stmt: *mut TAOS_STMT, fields: *mut TAOS_FIELD_E) {
    debug!("taos_stmt_reclaim_fields start, stmt: {stmt:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return;
        }
    };

    if fields.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fields is null")));
        return;
    }

    let ptr = (fields as *mut u8).sub(size_of::<StmtFields>()) as *mut StmtFields;
    let len = (*ptr).len;
    let total_size = size_of::<StmtFields>() + len * size_of::<TAOS_FIELD_E>();
    let layout = Layout::from_size_align_unchecked(total_size, align_of::<StmtFields>());
    dealloc(ptr as *mut u8, layout);

    debug!("taos_stmt_reclaim_fields succ");
    maybe_err.clear_err();
    clear_err_and_ret_succ();
}

pub unsafe fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int {
    debug!("taos_stmt_is_insert start, stmt: {stmt:?}, insert: {insert:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if insert.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "insert is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    if stmt2.is_insert().is_none() {
        maybe_err.with_err(Some(TaosError::new(
            Code::FAILED,
            "taos_stmt_prepare is not called",
        )));
        return format_errno(Code::FAILED.into());
    }

    let is_insert = stmt2.is_insert().unwrap();
    *insert = is_insert as _;

    debug!("taos_stmt_is_insert succ, is_insert: {is_insert}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int {
    debug!("taos_stmt_num_params start, stmt: {stmt:?}, nums: {nums:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if nums.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "nums is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {
            *nums = taos_stmt
                .col_fields
                .as_ref()
                .map_or(0, |fields| fields.len() as _);
        }
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_num_params can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    debug!("taos_stmt_num_params succ");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_get_param(
    stmt: *mut TAOS_STMT,
    idx: c_int,
    r#type: *mut c_int,
    bytes: *mut c_int,
) -> c_int {
    debug!(
        "taos_stmt_get_param start, stmt: {stmt:?}, idx: {idx}, type: {type:?}, bytes: {bytes:?}"
    );

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if r#type.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "type is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    if bytes.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "bytes is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_get_param can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let col_cnt = taos_stmt
        .col_fields
        .as_ref()
        .map_or(0, |fields| fields.len());

    if idx < 0 || (idx as usize) >= col_cnt {
        maybe_err.with_err(Some(TaosError::new(
            Code::INVALID_PARA,
            "idx is out of range",
        )));
        return format_errno(Code::INVALID_PARA.into());
    }

    let field = taos_stmt
        .col_fields
        .as_ref()
        .unwrap()
        .get(idx as usize)
        .unwrap();

    *r#type = field.field_type as _;
    *bytes = field.bytes as _;

    debug!("taos_stmt_get_param succ, field: {field:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_bind_param(stmt: *mut TAOS_STMT, bind: *mut TAOS_MULTI_BIND) -> c_int {
    debug!("taos_stmt_bind_param start, stmt: {stmt:?}, bind: {bind:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    let stmt2 = &mut taos_stmt.stmt2;

    let col_cnt = match stmt2.is_insert() {
        Some(true) => taos_stmt
            .col_fields
            .as_ref()
            .map_or(0, |fields| fields.len()),
        Some(false) => stmt2.fields_count().unwrap_or(0),
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    };

    let binds = slice::from_raw_parts(bind, col_cnt);

    let mut cols = Vec::with_capacity(col_cnt);
    for bind in binds {
        cols.push(bind.to_column_view());
    }

    taos_stmt.set_cols(cols);

    if taos_stmt.stb_insert {
        if let Err(err) = taos_stmt.move_to_bind_params() {
            error!("taos_stmt_bind_param failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            return format_errno(err.code().into());
        }
    }

    debug!("taos_stmt_bind_param succ, taos_stmt: {taos_stmt:?}");

    maybe_err.with_err(None);
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_bind_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    debug!("taos_stmt_bind_param_batch start, stmt: {stmt:?}, bind: {bind:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_bind_param_batch can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let col_cnt = taos_stmt
        .col_fields
        .as_ref()
        .map_or(0, |fields| fields.len());

    let binds = slice::from_raw_parts(bind, col_cnt);

    let mut cols = Vec::with_capacity(col_cnt);
    for bind in binds {
        cols.push(bind.to_column_view());
    }

    taos_stmt.set_cols(cols);

    if taos_stmt.stb_insert {
        if let Err(err) = taos_stmt.move_to_bind_params() {
            error!("taos_stmt_bind_param_batch failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            return format_errno(err.code().into());
        }
    }

    debug!("taos_stmt_bind_param_batch succ, taos_stmt: {taos_stmt:?}");

    maybe_err.with_err(None);
    clear_err_and_ret_succ()
}

#[allow(non_snake_case)]
pub unsafe fn taos_stmt_bind_single_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
    colIdx: c_int,
) -> c_int {
    debug!(
        "taos_stmt_bind_single_param_batch start, stmt: {stmt:?}, bind: {bind:?}, col_idx: {colIdx}"
    );

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_bind_single_param_batch can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_bind_single_param_batch is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let view = match bind.as_ref() {
        Some(bind) => bind.to_column_view(),
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "bind is null")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    debug!("taos_stmt_bind_single_param_batch, idx: {colIdx}, view: {view:?}");

    taos_stmt.set_single_col(colIdx as _, view);

    debug!("taos_stmt_bind_single_param_batch succ, taos_stmt: {taos_stmt:?}");
    maybe_err.with_err(None);
    0
}

pub unsafe fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_add_batch start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_add_batch can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    if let Err(err) = taos_stmt.move_to_bind_params() {
        error!("taos_stmt_add_batch failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return format_errno(err.code().into());
    }

    debug!("taos_stmt_add_batch succ, taos_stmt: {taos_stmt:?}");

    maybe_err.with_err(None);
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_execute start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    if let Err(err) = taos_stmt.move_to_bind_params() {
        error!("taos_stmt_execute failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return format_errno(err.code().into());
    }

    debug!("taos_stmt_execute, taos_stmt: {taos_stmt:?}");

    let stmt2 = &mut taos_stmt.stmt2;

    let params = std::mem::take(&mut taos_stmt.bind_params);
    debug!("taos_stmt_execute, params: {params:?}");

    if let Err(err) = stmt2.bind(&params) {
        error!("taos_stmt_execute failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return format_errno(err.code().into());
    }

    match stmt2.exec() {
        Ok(_) => {
            debug!("taos_stmt_execute succ");
            maybe_err.clear_err();
            clear_err_and_ret_succ()
        }
        Err(err) => {
            error!("taos_stmt_execute failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            format_errno(err.code().into())
        }
    }
}

pub unsafe fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES {
    debug!("taos_stmt_use_result start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return ptr::null_mut();
        }
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid"));
            return ptr::null_mut();
        }
    };

    let stmt2 = &mut taos_stmt.stmt2;
    match stmt2.result_set() {
        Ok(rs) => {
            let rs: TaosMaybeError<ResultSet> = ResultSet::Query(QueryResultSet::new(rs)).into();
            debug!("taos_stmt_use_result succ, result_set: {rs:?}");
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            let res = Box::into_raw(Box::new(rs)) as _;
            taos_stmt.res = Some(res);
            res
        }
        Err(err) => {
            error!("taos_stmt_use_result failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            ptr::null_mut()
        }
    }
}

pub unsafe fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_close, stmt: {stmt:?}");
    if stmt.is_null() {
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
    }
    let _ = Box::from_raw(stmt as *mut TaosMaybeError<TaosStmt>);
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_errstr(stmt: *mut TAOS_STMT) -> *mut c_char {
    taos_errstr(stmt as _) as _
}

pub unsafe fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_affected_rows start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt) => &mut taos_stmt.stmt2,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    debug!(
        "taos_stmt_affected_rows succ, affected_rows: {}",
        stmt2.affected_rows()
    );

    stmt2.affected_rows() as _
}

pub unsafe fn taos_stmt_affected_rows_once(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_affected_rows_once start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt) => &mut taos_stmt.stmt2,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    debug!(
        "taos_stmt_affected_rows_once succ, affected_rows_once: {}",
        stmt2.affected_rows_once(),
    );

    stmt2.affected_rows_once() as _
}
