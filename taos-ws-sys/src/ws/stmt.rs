use std::ffi::{c_char, c_int, c_ulong, CStr};
use std::{ptr, slice};

use taos_error::Code;
use taos_query::block_in_place_or_global;
use taos_query::common::{ColumnView, Timestamp, Ty, Value};
use taos_query::stmt2::{Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_ws::query::{BindType, Stmt2Field};
use taos_ws::{Stmt2, Taos};
use tracing::{error, trace};

use crate::taos::stmt::{TAOS_FIELD_E, TAOS_MULTI_BIND, TAOS_STMT, TAOS_STMT_OPTIONS};
use crate::taos::{TAOS, TAOS_RES};
use crate::ws::error::{
    clear_err_and_ret_succ, format_errno, set_err_and_get_code, taos_errstr, TaosError,
    TaosMaybeError,
};
use crate::ws::query::QueryResultSet;
use crate::ws::{ResultSet, TaosResult};

#[derive(Debug)]
struct TaosStmt {
    stmt2: Stmt2,
    tag_fields: Option<Vec<Stmt2Field>>,
    col_fields: Option<Vec<Stmt2Field>>,
    tag_fields_addr: Option<usize>,
    col_fields_addr: Option<usize>,
    params: Vec<Stmt2BindParam>,
    cur_param: Option<Stmt2BindParam>,
}

impl TaosStmt {
    fn new(stmt2: Stmt2) -> Self {
        Self {
            stmt2,
            tag_fields: None,
            col_fields: None,
            tag_fields_addr: None,
            col_fields_addr: None,
            params: Vec::new(),
            cur_param: None,
        }
    }

    fn init_cur_param_if_none(&mut self) {
        if self.cur_param.is_none() {
            self.cur_param = Some(Stmt2BindParam::new(None, None, None));
        }
    }

    fn set_cur_param_tbname(&mut self, name: String) {
        self.init_cur_param_if_none();
        self.cur_param
            .as_mut()
            .map(|param| param.with_table_name(name));
    }

    fn set_cur_param_tags(&mut self, tags: Vec<Value>) {
        self.init_cur_param_if_none();
        self.cur_param.as_mut().map(|param| param.with_tags(tags));
    }

    fn set_cur_param_cols(&mut self, cols: Vec<ColumnView>) {
        self.init_cur_param_if_none();
        self.cur_param
            .as_mut()
            .map(|param| param.with_columns(cols));
    }

    fn move_cur_param_to_params(&mut self) {
        if let Some(cur_param) = self.cur_param.take() {
            self.params.push(cur_param);
        }
    }
}

pub unsafe fn taos_stmt_init(taos: *mut TAOS) -> *mut TAOS_STMT {
    taos_stmt_init_with_reqid(taos, generate_req_id() as _)
}

pub unsafe fn taos_stmt_init_with_reqid(taos: *mut TAOS, reqid: i64) -> *mut TAOS_STMT {
    let taos_stmt: TaosMaybeError<TaosStmt> = stmt_init(taos, reqid as _, false, false).into();
    Box::into_raw(Box::new(taos_stmt)) as _
}

pub unsafe fn taos_stmt_init_with_options(
    taos: *mut TAOS,
    options: *mut TAOS_STMT_OPTIONS,
) -> *mut TAOS_STMT {
    let (req_id, single_stb_insert, single_table_bind_once) = match options.as_ref() {
        Some(options) => (
            options.reqId as u64,
            options.singleStbInsert,
            options.singleTableBindOnce,
        ),
        None => (generate_req_id(), false, false),
    };

    let taos_stmt: TaosMaybeError<TaosStmt> =
        stmt_init(taos, req_id, single_stb_insert, single_table_bind_once).into();
    Box::into_raw(Box::new(taos_stmt)) as _
}

unsafe fn stmt_init(
    taos: *mut TAOS,
    req_id: u64,
    single_stb_insert: bool,
    single_table_bind_once: bool,
) -> TaosResult<TaosStmt> {
    trace!(
        "stmt_init, req_id: {req_id}, single_stb_insert: {single_stb_insert}, \
        single_table_bind_once: {single_table_bind_once}"
    );

    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::FAILED, "taos is null"))?;

    let mut stmt2 = Stmt2::new(taos.client());

    block_in_place_or_global(stmt2.init_with_options(
        req_id,
        single_stb_insert,
        single_table_bind_once,
    ))?;

    Ok(TaosStmt::new(stmt2))
}

pub unsafe fn taos_stmt_prepare(
    stmt: *mut TAOS_STMT,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    trace!("taos_stmt_prepare start, stmt: {stmt:?}, sql: {sql:?}, length: {length}");

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

    trace!("taos_stmt_prepare, sql: {sql}");

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

    trace!("taos_stmt_prepare succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_set_tbname_tags(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    trace!("taos_stmt_set_tbname_tags, stmt: {stmt:?}, name: {name:?}, tags: {tags:?}");
    let code = taos_stmt_set_tbname(stmt, name);
    if code != 0 {
        return code;
    }
    taos_stmt_set_tags(stmt, tags)
}

pub unsafe fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    trace!("taos_stmt_set_tbname start, stmt: {stmt:?}, name: {name:?}");

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

    taos_stmt.set_cur_param_tbname(name.to_owned());

    trace!("taos_stmt_set_tbname succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_set_tags(stmt: *mut TAOS_STMT, tags: *mut TAOS_MULTI_BIND) -> c_int {
    trace!("taos_stmt_set_tags start, stmt: {stmt:?}, tags: {tags:?}");

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
        .map(|fields| fields.len())
        .unwrap_or(0);

    let binds = slice::from_raw_parts(tags, tag_cnt);

    let mut tags = Vec::with_capacity(tag_cnt);
    for bind in binds {
        tags.push(bind.to_value());
    }

    taos_stmt.set_cur_param_tags(tags);

    trace!("taos_stmt_set_tags succ, taos_stmt: {taos_stmt:?}");

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
    trace!("taos_stmt_get_tag_fields start, stmt: {stmt:?}, field_num: {fieldNum:?}, fields: {fields:?}");

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

    trace!("taos_stmt_get_tag_fields, fields: {tag_fields:?}");

    *fieldNum = tag_fields.len() as _;

    if !tag_fields.is_empty() {
        *fields = Box::into_raw(tag_fields.into_boxed_slice()) as _;
        taos_stmt.tag_fields_addr = Some(*fields as usize);
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
    trace!("taos_stmt_get_col_fields start, stmt: {stmt:?}, field_num: {fieldNum:?}, fields: {fields:?}");

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

    trace!("taos_stmt_get_col_fields, fields: {col_fields:?}");

    *fieldNum = col_fields.len() as _;

    if !col_fields.is_empty() {
        *fields = Box::into_raw(col_fields.into_boxed_slice()) as _;
        taos_stmt.col_fields_addr = Some(*fields as usize);
    }

    trace!("taos_stmt_get_col_fields succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_reclaim_fields(stmt: *mut TAOS_STMT, fields: *mut TAOS_FIELD_E) {
    trace!("taos_stmt_reclaim_fields start, stmt: {stmt:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return;
        }
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return;
        }
    };

    if fields.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fields is null")));
        return;
    }

    if let Some(addr) = taos_stmt.tag_fields_addr {
        if addr == fields as usize {
            let len = taos_stmt
                .tag_fields
                .as_ref()
                .map(|fields| fields.len())
                .unwrap_or(0);
            let _ = Vec::from_raw_parts(fields, len, len);
            taos_stmt.tag_fields_addr = None;
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            return;
        }
    }

    if let Some(addr) = taos_stmt.col_fields_addr {
        if addr == fields as usize {
            let len = taos_stmt
                .col_fields
                .as_ref()
                .map(|fields| fields.len())
                .unwrap_or(0);
            let _ = Vec::from_raw_parts(fields, len, len);
            taos_stmt.col_fields_addr = None;
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            return;
        }
    }

    maybe_err.with_err(Some(TaosError::new(
        Code::INVALID_PARA,
        "fields is invalid",
    )));
}

pub unsafe fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int {
    trace!("taos_stmt_is_insert start, stmt: {stmt:?}, insert: {insert:?}");

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

    trace!("taos_stmt_is_insert succ, is_insert: {is_insert}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int {
    trace!("taos_stmt_num_params start, stmt: {stmt:?}, nums: {nums:?}");

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
                .map(|fields| fields.len())
                .unwrap_or(0) as _;
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

    trace!("taos_stmt_num_params succ");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_get_param(
    stmt: *mut TAOS_STMT,
    idx: c_int,
    r#type: *mut c_int,
    bytes: *mut c_int,
) -> c_int {
    trace!(
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
        .map(|fields| fields.len())
        .unwrap_or(0);

    if idx < 0 || idx >= col_cnt as _ {
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

    trace!("taos_stmt_get_param succ, field: {field:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_bind_param(stmt: *mut TAOS_STMT, bind: *mut TAOS_MULTI_BIND) -> c_int {
    trace!("taos_stmt_bind_param start, stmt: {stmt:?}, bind: {bind:?}");

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
            .map(|fields| fields.len())
            .unwrap_or(0),
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

    taos_stmt.set_cur_param_cols(cols);

    trace!("taos_stmt_bind_param succ, taos_stmt: {taos_stmt:?}");

    maybe_err.with_err(None);
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_bind_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    trace!("taos_stmt_bind_param_batch start, stmt: {stmt:?}, bind: {bind:?}");

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
        .map(|fields| fields.len())
        .unwrap_or(0);

    let binds = slice::from_raw_parts(bind, col_cnt);

    let mut cols = Vec::with_capacity(col_cnt);
    for bind in binds {
        cols.push(bind.to_column_view());
    }

    taos_stmt.set_cur_param_cols(cols);

    trace!("taos_stmt_bind_param_batch succ, taos_stmt: {taos_stmt:?}");

    maybe_err.with_err(None);
    clear_err_and_ret_succ()
}

#[allow(non_snake_case)]
pub fn taos_stmt_bind_single_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
    colIdx: c_int,
) -> c_int {
    todo!("taos_stmt_bind_single_param_batch")
}

pub unsafe fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int {
    trace!("taos_stmt_add_batch start, stmt: {stmt:?}");

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

    taos_stmt.move_cur_param_to_params();

    trace!("taos_stmt_add_batch succ, taos_stmt: {taos_stmt:?}");

    maybe_err.with_err(None);
    clear_err_and_ret_succ()
}

pub unsafe fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int {
    trace!("taos_stmt_execute start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    taos_stmt.move_cur_param_to_params();

    trace!("taos_stmt_execute, taos_stmt: {taos_stmt:?}");

    let stmt2 = &mut taos_stmt.stmt2;

    let params = std::mem::take(&mut taos_stmt.params);
    if let Err(err) = stmt2.bind(&params) {
        error!("taos_stmt_execute failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return format_errno(err.code().into());
    }

    match stmt2.exec() {
        Ok(_) => {
            trace!("taos_stmt_execute succ");
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
    trace!("taos_stmt_use_result start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return ptr::null_mut();
        }
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt) => &mut taos_stmt.stmt2,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid"));
            return ptr::null_mut();
        }
    };

    match stmt2.result_set() {
        Ok(rs) => {
            let rs: TaosMaybeError<ResultSet> = ResultSet::Query(QueryResultSet::new(rs)).into();
            trace!("taos_stmt_use_result succ, result_set: {rs:?}");
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            Box::into_raw(Box::new(rs)) as _
        }
        Err(err) => {
            error!("taos_stmt_use_result failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            ptr::null_mut()
        }
    }
}

pub unsafe fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int {
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
    trace!("taos_stmt_affected_rows start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt) => &mut taos_stmt.stmt2,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    trace!(
        "taos_stmt_affected_rows succ, affected_rows: {}",
        stmt2.affected_rows()
    );

    stmt2.affected_rows() as _
}

pub unsafe fn taos_stmt_affected_rows_once(stmt: *mut TAOS_STMT) -> c_int {
    trace!("taos_stmt_affected_rows_once start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt) => &mut taos_stmt.stmt2,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    trace!(
        "taos_stmt_affected_rows_once succ, affected_rows_once: {}",
        stmt2.affected_rows_once(),
    );

    stmt2.affected_rows_once() as _
}

impl TAOS_MULTI_BIND {
    fn to_value(&self) -> Value {
        if !self.is_null.is_null() && unsafe { self.is_null.read() != 0 } {
            let val = Value::Null(self.ty());
            trace!("to_value, value: {val:?}");
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

        trace!("to_value, value: {val:?}");

        val
    }

    fn to_column_view(&self) -> ColumnView {
        let ty = self.ty();
        let num = self.num as usize;
        let is_nulls = unsafe { slice::from_raw_parts(self.is_null, num) };
        let lens = unsafe { slice::from_raw_parts(self.length, num) };

        trace!("to_column_view, ty: {ty}, num: {num}, is_nulls: {is_nulls:?}, lens: {lens:?}");

        macro_rules! view {
            ($from:expr) => {{
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const _, num) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        vals[i] = Some(slice[i]);
                    }
                }
                $from(vals)
            }};
        }

        let view = match ty {
            Ty::Bool => view!(ColumnView::from_bools),
            Ty::TinyInt => view!(ColumnView::from_tiny_ints),
            Ty::SmallInt => view!(ColumnView::from_small_ints),
            Ty::Int => view!(ColumnView::from_ints),
            Ty::BigInt => view!(ColumnView::from_big_ints),
            Ty::UTinyInt => view!(ColumnView::from_unsigned_tiny_ints),
            Ty::USmallInt => view!(ColumnView::from_unsigned_small_ints),
            Ty::UInt => view!(ColumnView::from_unsigned_ints),
            Ty::UBigInt => view!(ColumnView::from_unsigned_big_ints),
            Ty::Float => view!(ColumnView::from_floats),
            Ty::Double => view!(ColumnView::from_doubles),
            Ty::Timestamp => view!(ColumnView::from_millis_timestamp),
            Ty::VarChar => {
                let vals = (0..num)
                    .zip(is_nulls)
                    .map(|(i, is_null)| {
                        if *is_null != 0 {
                            None
                        } else {
                            unsafe {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = slice::from_raw_parts(ptr, len);
                                Some(str::from_utf8_unchecked(bytes))
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_varchar::<&str, _, _, _>(vals)
            }
            Ty::NChar => {
                let vals = (0..num)
                    .zip(is_nulls)
                    .map(|(i, is_null)| {
                        if *is_null != 0 {
                            None
                        } else {
                            unsafe {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = slice::from_raw_parts(ptr, len);
                                Some(str::from_utf8_unchecked(bytes))
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_nchar::<&str, _, _, _>(vals)
            }
            Ty::VarBinary => {
                let vals = (0..num)
                    .zip(is_nulls)
                    .map(|(i, is_null)| {
                        if *is_null != 0 {
                            None
                        } else {
                            unsafe {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                Some(slice::from_raw_parts(ptr, len))
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_bytes::<&[u8], _, _, _>(vals)
            }
            Ty::Geometry => {
                let vals = (0..num)
                    .zip(is_nulls)
                    .map(|(i, is_null)| {
                        if *is_null != 0 {
                            None
                        } else {
                            unsafe {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                Some(slice::from_raw_parts(ptr, len))
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_geobytes::<&[u8], _, _, _>(vals)
            }
            _ => todo!(),
        };

        trace!("to_column_view, view: {view:?}");

        view
    }

    fn ty(&self) -> Ty {
        self.buffer_type.into()
    }

    #[cfg(test)]
    fn from_primitives<T: taos_query::common::itypes::IValue>(
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
    fn from_timestamps(values: &[i64], nulls: &[bool], lens: &[i32]) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::taos::query::taos_affected_rows;
    use crate::ws::query::*;
    use crate::ws::{test_connect, test_exec, test_exec_many};

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
            assert_eq!(CStr::from_ptr(errstr), c"");

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 1);

            let affected_rows_once = taos_stmt_affected_rows_once(stmt);
            assert_eq!(affected_rows_once, 1);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740472346");
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
        }

        unsafe {
            let taos = test_connect();
            let stmt = taos_stmt_init_with_options(taos, ptr::null_mut());
            assert!(!stmt.is_null());

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);
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
        }
    }
}
