use std::ffi::{c_char, c_int, c_ulong, CStr};
use std::{ptr, slice};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use taos_error::{Code, Error as RawError};
use taos_query::block_in_place_or_global;
use taos_query::common::{ColumnView, Timestamp, Ty, Value};
use taos_query::prelude::Itertools;
use taos_query::stmt::Bindable;
use taos_query::stmt2::{Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_ws::query::{BindType, Stmt2Field};
use taos_ws::stmt::{StmtField, WsFieldsable};
use taos_ws::{Stmt, Stmt2, Taos};
use tracing::{error, trace};

use crate::taos::stmt::{TAOS_FIELD_E, TAOS_MULTI_BIND, TAOS_STMT, TAOS_STMT_OPTIONS};
use crate::taos::{TAOS, TAOS_RES};
use crate::ws::error::{
    clear_err_and_ret_succ, clear_error_info, format_errno, set_err_and_get_code, taos_errstr,
    TaosError, TaosMaybeError,
};
use crate::ws::TaosResult;

#[derive(Debug)]
struct TaosStmt {
    stmt2: Stmt2,
    tag_fields: Option<Vec<Stmt2Field>>,
    col_fields: Option<Vec<Stmt2Field>>,
    tag_fields_addr: Option<usize>,
    col_fields_addr: Option<usize>,
    params: Vec<Stmt2BindParam>,
    // TODO: use Stmt2BindParam instead of Option<Stmt2BindParam>
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
            cur_param: Some(Stmt2BindParam::new(None, None, None)),
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

    taos_stmt
        .cur_param
        .as_mut()
        .map(|param| param.with_table_name(name.to_owned()));

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

    taos_stmt
        .cur_param
        .as_mut()
        .map(|param| param.with_tags(tags));

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
        Some(false) => *nums = stmt2.fields_count().unwrap_or(0) as _,
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

    taos_stmt
        .cur_param
        .as_mut()
        .map(|param| param.with_columns(cols));

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

    taos_stmt
        .cur_param
        .as_mut()
        .map(|param| param.with_columns(cols));

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

    taos_stmt.params.push(taos_stmt.cur_param.take().unwrap());
    taos_stmt.cur_param = Some(Stmt2BindParam::new(None, None, None));

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

// pub fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES {
//     todo!()
// }

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

// pub unsafe fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int {
//     match (stmt as *mut TaosMaybeError<Stmt>)
//         .as_mut()
//         .and_then(|maybe_err| maybe_err.deref_mut())
//     {
//         Some(stmt) => stmt.affected_rows() as _,
//         None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
//     }
// }

// pub unsafe fn taos_stmt_affected_rows_once(stmt: *mut TAOS_STMT) -> c_int {
//     match (stmt as *mut TaosMaybeError<Stmt>)
//         .as_mut()
//         .and_then(|maybe_err| maybe_err.deref_mut())
//     {
//         Some(stmt) => stmt.affected_rows_once() as _,
//         None => set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
//     }
// }

impl TAOS_MULTI_BIND {
    // pub fn new(ty: Ty) -> Self {
    //     Self {
    //         buffer_type: ty as _,
    //         buffer: ptr::null_mut(),
    //         buffer_length: 0,
    //         length: ptr::null_mut(),
    //         is_null: ptr::null_mut(),
    //         num: 1,
    //     }
    // }

    fn to_value(&self) -> Value {
        if !self.is_null.is_null() && unsafe { self.is_null.read() != 0 } {
            let val = Value::Null(self.ty_());
            trace!("to_value, value: {val:?}");
            return val;
        }

        assert!(!self.length.is_null());
        assert!(!self.buffer.is_null());

        let val = match self.ty_() {
            Ty::Null => Value::Null(self.ty_()),
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
        let ty = self.ty_();
        let num = self.num as usize;
        let is_nulls = unsafe { slice::from_raw_parts(self.is_null, num) };
        let lens = unsafe { slice::from_raw_parts(self.length, num) };
        let len = self.buffer_length * num;

        trace!(
            "to_column_view, ty: {ty}, num: {num}, is_nulls: {is_nulls:?}, lens: {lens:?}, \
            total_len: {len}"
        );

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
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const _, len) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] != 0 {
                        let idx = i * self.buffer_length as usize;
                        let bytes = &slice[idx..idx + lens[i] as usize];
                        vals[i] = unsafe { Some(str::from_utf8_unchecked(bytes)) };
                    }
                }
                ColumnView::from_varchar::<&str, _, _, _>(vals)
            }
            Ty::NChar => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const _, len) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        let idx = i * self.buffer_length as usize;
                        let bytes = &slice[idx..idx + lens[i] as usize];
                        vals[i] = unsafe { Some(str::from_utf8_unchecked(bytes)) };
                    }
                }
                ColumnView::from_nchar::<&str, _, _, _>(vals)
            }
            Ty::Json => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        let idx = i * self.buffer_length as usize;
                        let bytes = &slice[idx..idx + lens[i] as usize];
                        vals[i] = serde_json::from_slice(bytes).unwrap();
                    }
                }
                ColumnView::from_json::<&str, _, _, _>(vals)
            }
            Ty::VarBinary => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        let idx = i * self.buffer_length as usize;
                        let val = &slice[idx..idx + lens[i] as usize];
                        vals[i] = Some(val);
                    }
                }
                ColumnView::from_bytes::<&[u8], _, _, _>(vals)
            }
            Ty::Geometry => {
                let slice = unsafe { slice::from_raw_parts(self.buffer as *const u8, len) };
                let mut vals = vec![None; num];
                for i in 0..num {
                    if is_nulls[i] == 0 {
                        let idx = i * self.buffer_length as usize;
                        let val = &slice[idx..idx + lens[i] as usize];
                        vals[i] = Some(val);
                    }
                }
                ColumnView::from_geobytes::<&[u8], _, _, _>(vals)
            }
            _ => todo!(),
        };

        trace!("to_column_view, view: {view:?}");

        view
    }

    #[cfg(test)]
    fn from_primitives_<T: taos_query::common::itypes::IValue>(
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
    fn from_raw_timestamps_(values: &[i64], nulls: &[bool], lens: &[i32]) -> Self {
        Self {
            buffer_type: Ty::Timestamp as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<i64>(),
            length: lens.as_ptr() as _,
            is_null: nulls.as_ptr() as _,
            num: values.len() as _,
        }
    }

    // pub fn first_to_json(&self) -> serde_json::Value {
    //     self.to_json().as_array().unwrap().first().unwrap().clone()
    // }

    // pub fn to_json(&self) -> serde_json::Value {
    //     use serde_json::{json, Value};

    //     assert!(self.num > 0, "invalid bind value");
    //     let len = self.num as usize;

    //     macro_rules! _nulls {
    //         () => {
    //             json!(std::iter::repeat(Value::Null).take(len).collect::<Vec<_>>())
    //         };
    //     }

    //     if self.buffer.is_null() {
    //         return _nulls!();
    //     }

    //     macro_rules! _impl_primitive {
    //         ($t:ty) => {{
    //             let slice = std::slice::from_raw_parts(self.buffer as *const $t, len);
    //             match self.is_null.is_null() {
    //                 true => json!(slice),
    //                 false => {
    //                     let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
    //                     let column: Vec<_> = slice
    //                         .iter()
    //                         .zip(nulls)
    //                         .map(
    //                             |(value, is_null)| {
    //                                 if *is_null {
    //                                     None
    //                                 } else {
    //                                     Some(*value)
    //                                 }
    //                             },
    //                         )
    //                         .collect();
    //                     json!(column)
    //                 }
    //             }
    //         }};
    //     }

    //     unsafe {
    //         match Ty::from(self.buffer_type) {
    //             Ty::Null => _nulls!(),
    //             Ty::Bool => _impl_primitive!(bool),
    //             Ty::TinyInt => _impl_primitive!(i8),
    //             Ty::SmallInt => _impl_primitive!(i16),
    //             Ty::Int => _impl_primitive!(i32),
    //             Ty::BigInt => _impl_primitive!(i64),
    //             Ty::UTinyInt => _impl_primitive!(u8),
    //             Ty::USmallInt => _impl_primitive!(u16),
    //             Ty::UInt => _impl_primitive!(u32),
    //             Ty::UBigInt => _impl_primitive!(u64),
    //             Ty::Float => _impl_primitive!(f32),
    //             Ty::Double => _impl_primitive!(f64),
    //             Ty::Timestamp => _impl_primitive!(i64),
    //             Ty::VarChar => {
    //                 let len = self.num as usize;
    //                 if self.is_null.is_null() {
    //                     let column = (0..len)
    //                         .map(|i| {
    //                             let ptr = (self.buffer as *const u8)
    //                                 .offset(self.buffer_length as isize * i as isize);
    //                             let len = *self.length.add(i) as usize;
    //                             let bytes = std::slice::from_raw_parts(ptr, len);
    //                             std::str::from_utf8_unchecked(bytes)
    //                         })
    //                         .collect::<Vec<_>>();
    //                     return json!(column);
    //                 }
    //                 let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
    //                 let column = (0..len)
    //                     .zip(nulls)
    //                     .map(|(i, is_null)| {
    //                         if *is_null {
    //                             None
    //                         } else {
    //                             let ptr = (self.buffer as *const u8)
    //                                 .offset(self.buffer_length as isize * i as isize);
    //                             let len = *self.length.add(i) as usize;
    //                             let bytes = std::slice::from_raw_parts(ptr, len);
    //                             Some(std::str::from_utf8_unchecked(bytes))
    //                         }
    //                     })
    //                     .collect::<Vec<_>>();
    //                 json!(column)
    //             }
    //             Ty::NChar => {
    //                 let len = self.num as usize;
    //                 if self.is_null.is_null() {
    //                     let column = (0..len)
    //                         .map(|i| {
    //                             let ptr = (self.buffer as *const u8)
    //                                 .offset(self.buffer_length as isize * i as isize);
    //                             let len = *self.length.add(i) as usize;
    //                             let bytes = std::slice::from_raw_parts(ptr, len);
    //                             std::str::from_utf8_unchecked(bytes)
    //                         })
    //                         .collect::<Vec<_>>();
    //                     return json!(column);
    //                 }
    //                 let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
    //                 let column = (0..len)
    //                     .zip(nulls)
    //                     .map(|(i, is_null)| {
    //                         if *is_null {
    //                             None
    //                         } else {
    //                             let ptr = (self.buffer as *const u8)
    //                                 .offset(self.buffer_length as isize * i as isize);
    //                             let len = *self.length.add(i) as usize;
    //                             let bytes = std::slice::from_raw_parts(ptr, len);
    //                             Some(std::str::from_utf8_unchecked(bytes))
    //                         }
    //                     })
    //                     .collect::<Vec<_>>();
    //                 json!(column)
    //             }
    //             Ty::Json => {
    //                 let len = self.num as usize;
    //                 if self.is_null.is_null() {
    //                     let column = (0..len)
    //                         .map(|i| {
    //                             let ptr = (self.buffer as *const u8)
    //                                 .offset(self.buffer_length as isize * i as isize);
    //                             let len = *self.length.add(i) as usize;
    //                             let bytes = std::slice::from_raw_parts(ptr, len);
    //                             std::str::from_utf8_unchecked(bytes)
    //                         })
    //                         .collect::<Vec<_>>();
    //                     return json!(column);
    //                 }
    //                 let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
    //                 let column = (0..len)
    //                     .zip(nulls)
    //                     .map(|(i, is_null)| {
    //                         if *is_null {
    //                             None
    //                         } else {
    //                             let ptr = (self.buffer as *const u8)
    //                                 .offset(self.buffer_length as isize * i as isize);
    //                             let len = *self.length.add(i) as usize;
    //                             let bytes = std::slice::from_raw_parts(ptr, len);
    //                             Some(serde_json::from_slice::<serde_json::Value>(bytes).unwrap())
    //                         }
    //                     })
    //                     .collect::<Vec<_>>();
    //                 json!(column)
    //             }
    //             _ => todo!(),
    //         }
    //     }
    // }

    fn ty_(&self) -> Ty {
        self.buffer_type.into()
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

// #[cfg(test)]
// #[allow(dead_code)]
// impl TAOS_MULTI_BIND {
//     fn from_primitives<T: taos_query::common::itypes::IValue>(
//         nulls: &[bool],
//         values: &[T],
//     ) -> Self {
//         Self {
//             buffer_type: T::TY as _,
//             buffer: values.as_ptr() as _,
//             buffer_length: std::mem::size_of::<T>(),
//             length: values.len() as _,
//             is_null: nulls.as_ptr() as _,
//             num: values.len() as _,
//         }
//     }

//     fn from_raw_timestamps(nulls: &[bool], values: &[i64]) -> Self {
//         Self {
//             buffer_type: Ty::Timestamp as _,
//             buffer: values.as_ptr() as _,
//             buffer_length: std::mem::size_of::<i64>(),
//             length: values.len() as _,
//             is_null: nulls.as_ptr() as _,
//             num: values.len() as _,
//         }
//     }

//     fn from_binary_vec(values: &[Option<impl AsRef<[u8]>>]) -> Self {
//         let mut buf_len = 0;
//         let num = values.len();

//         let mut nulls = std::mem::ManuallyDrop::new(Vec::with_capacity(num));
//         nulls.resize(num, false);

//         let mut len = std::mem::ManuallyDrop::new(Vec::with_capacity(num));
//         for (i, v) in values.iter().enumerate() {
//             match v {
//                 Some(v) => {
//                     let v = v.as_ref();
//                     len.push(v.len() as _);
//                     if v.len() > buf_len {
//                         buf_len = v.len();
//                     }
//                 }
//                 None => {
//                     len.push(-1);
//                     nulls[i] = true;
//                 }
//             }
//         }

//         let buf_size = buf_len * values.len();
//         let mut buf = std::mem::ManuallyDrop::new(Vec::with_capacity(buf_size));
//         unsafe { buf.set_len(buf_size) };
//         buf.fill(0);

//         for (i, v) in values.iter().enumerate() {
//             if let Some(v) = v {
//                 let v = v.as_ref();
//                 unsafe {
//                     let dst = buf.as_mut_ptr().add(buf_len * i);
//                     std::ptr::copy_nonoverlapping(v.as_ptr(), dst, v.len());
//                 }
//             }
//         }

//         Self {
//             buffer_type: Ty::VarChar as _,
//             buffer: buf.as_ptr() as _,
//             buffer_length: buf_len,
//             length: len.as_ptr() as _,
//             is_null: nulls.as_ptr() as _,
//             num: num as _,
//         }
//     }

//     fn from_string_vec(values: &[Option<impl AsRef<str>>]) -> Self {
//         let values: Vec<_> = values
//             .iter()
//             .map(|f| f.as_ref().map(|s| s.as_ref().as_bytes()))
//             .collect();
//         let mut bind = Self::from_binary_vec(&values);
//         bind.buffer_type = Ty::NChar as _;
//         bind
//     }
// }

// impl From<&StmtField> for TAOS_FIELD_E {
//     fn from(field: &StmtField) -> Self {
//         let field_name = field.name.as_str();
//         let mut name = [0 as c_char; 65];
//         unsafe {
//             std::ptr::copy_nonoverlapping(
//                 field_name.as_ptr(),
//                 name.as_mut_ptr() as _,
//                 field_name.len(),
//             );
//         };

//         Self {
//             name,
//             r#type: field.field_type,
//             precision: field.precision,
//             scale: field.scale,
//             bytes: field.bytes,
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ws::{test_connect, test_exec, test_exec_many};

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
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let name = c"d0";
            let mut tags = vec![TAOS_MULTI_BIND::from_primitives_(&[99], &[false], &[4])];
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
                TAOS_MULTI_BIND::from_raw_timestamps_(&[1738910658659i64], &[false], &[8]),
                TAOS_MULTI_BIND::from_primitives_(&[20], &[false], &[4]),
            ];
            let code = taos_stmt_bind_param_batch(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let mut cols = vec![
                TAOS_MULTI_BIND::from_raw_timestamps_(&[1738910658659i64], &[false], &[8]),
                TAOS_MULTI_BIND::from_primitives_(&[2025], &[false], &[4]),
            ];
            let code = taos_stmt_bind_param(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let errstr = taos_stmt_errstr(stmt);
            assert_eq!(CStr::from_ptr(errstr), c"");

            // let affected_rows = taos_stmt_affected_rows(stmt);
            // assert_eq!(affected_rows, 1);

            // let affected_rows_once = taos_stmt_affected_rows_once(stmt);
            // assert_eq!(affected_rows_once, 1);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740472346");
        }
    }
}
