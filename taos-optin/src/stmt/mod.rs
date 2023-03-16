use crate::{
    err_or,
    into_c_str::IntoCStr,
    raw::{ApiEntry, StmtApi},
    RawRes, RawTaos, ResultSet,
};

use std::{
    ffi::{c_void, CStr, CString},
    sync::Arc,
};

use itertools::Itertools;
// use taos_error::{Code, Error};
use taos_query::prelude::{
    sync::{Bindable, Queryable, RawError as Error },
    Code,
};

use crate::types::*;

mod bind;
mod multi;

#[derive(Debug)]
pub struct Stmt {
    raw: RawStmt,
}

impl Bindable<super::Taos> for Stmt {
    type Error = super::Error;

    fn init(taos: &super::Taos) -> Result<Self, Self::Error> {
        Ok(Self {
            raw: RawStmt::from_raw_taos(&taos.raw),
        })
    }

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> Result<&mut Self, Self::Error> {
        self.raw.prepare(sql.as_ref())?;
        Ok(self)
    }

    fn set_tbname<S: AsRef<str>>(&mut self, sql: S) -> Result<&mut Self, Self::Error> {
        self.raw.set_tbname(sql.as_ref())?;
        Ok(self)
    }

    fn set_tags(&mut self, tags: &[taos_query::common::Value]) -> Result<&mut Self, Self::Error> {
        if self.raw.is_v3() {
            let tags = tags.iter().map(TaosBindV3::from_value).collect_vec();
            self.raw.set_tags(tags.as_ptr() as _)?;
        } else {
            let tags = tags.iter().map(TaosBindV2::from_value).collect_vec();
            self.raw.set_tags(tags.as_ptr() as _)?;
        }
        Ok(self)
    }

    fn bind(
        &mut self,
        params: &[taos_query::common::ColumnView],
    ) -> Result<&mut Self, Self::Error> {
        let params: Vec<DropMultiBind> = params.iter().map(|c| c.into()).collect_vec();
        self.raw.bind_param_batch(unsafe { std::mem::transmute(params.as_slice()) })?;
        Ok(self)
    }

    fn add_batch(&mut self) -> Result<&mut Self, Self::Error> {
        self.raw.add_batch()?;
        Ok(self)
    }

    fn execute(&mut self) -> Result<usize, Self::Error> {
        self.raw.execute().map_err(Into::into)
    }

    fn result_set(&mut self) -> Result<<super::Taos as Queryable>::ResultSet, Self::Error> {
        self.raw.use_result().map_err(Into::into)
    }

    fn affected_rows(&self) -> usize {
        self.raw.affected_rows() as _
    }
}

#[derive(Debug)]
pub(crate) struct RawStmt {
    c: Arc<ApiEntry>,
    api: StmtApi,
    ptr: *mut TAOS_STMT,
    tbname: Option<CString>,
}

unsafe impl Sync for RawStmt {}
unsafe impl Send for RawStmt {}
impl Drop for RawStmt {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl RawStmt {
    fn is_v3(&self) -> bool {
        self.c.version().starts_with('3')
    }
    #[inline(always)]
    fn ok(&self, code: impl Into<Code>) -> Result<(), Error> {
        let code = code.into();

        if code.success() {
            Ok(())
        } else {
            Err(Error::from_string(self.err_as_str()))
        }
    }

    #[inline]
    pub unsafe fn as_ptr(&self) -> *mut TAOS_STMT {
        self.ptr
    }

    // #[inline]
    // pub fn errstr(&self) -> &CStr {
    //     unsafe { CStr::from_ptr((self.api.taos_stmt_errstr)(self.as_ptr())) }
    // }

    #[inline]
    pub fn err_as_str(&self) -> String {
        unsafe {
            CStr::from_ptr((self.api.taos_stmt_errstr)(self.as_ptr()))
                .to_string_lossy()
                .to_string()
        }
    }

    #[inline]
    pub fn from_raw_taos(taos: &RawTaos) -> RawStmt {
        RawStmt {
            c: taos.c.clone(),
            api: taos.c.stmt,
            ptr: unsafe { (taos.c.stmt.taos_stmt_init)(taos.as_ptr()) },
            tbname: None,
        }
    }
    #[inline]
    pub fn close(&mut self) -> Result<(), Error> {
        err_or!(self, (self.api.taos_stmt_close)(self.as_ptr()))
    }

    #[inline]
    pub fn prepare<'c>(&mut self, sql: impl IntoCStr<'c>) -> Result<(), Error> {
        let sql = sql.into_c_str();
        log::trace!("prepare stmt with sql: {sql:?}");
        self.ok(unsafe {
            (self.api.taos_stmt_prepare)(self.as_ptr(), sql.as_ptr(), sql.to_bytes().len() as _)
        })
    }

    // pub fn set_tbname_tags_v3<'a>(
    //     &mut self,
    //     name: impl IntoCStr<'a>,
    //     tags: &[TaosBindV3],
    // ) -> Result<(), Error> {
    //     self.ok(unsafe {
    //         (self.api.taos_stmt_set_tbname_tags)(
    //             self.as_ptr(),
    //             name.into_c_str().as_ptr(),
    //             tags.as_ptr() as _,
    //         )
    //     })
    // }

    #[inline]
    pub fn set_tbname<'c>(&mut self, name: impl IntoCStr<'c>) -> Result<(), Error> {
        let name = name.into_c_str();
        let res = self.ok(unsafe {
            (self.api.taos_stmt_set_tbname)(self.as_ptr(), name.into_c_str().as_ptr())
        });
        if !self.is_v3() {
            self.tbname = Some(name.into_owned());
        }
        res
    }

    // #[inline]
    // pub fn set_sub_tbname<'c>(&mut self, name: impl IntoCStr<'c>) -> Result<(), Error> {
    //     self.ok(unsafe {
    //         (self.api.taos_stmt_set_sub_tbname)(self.as_ptr(), name.into_c_str().as_ptr())
    //     })
    // }

    #[inline]
    pub fn set_tags(&mut self, tags: *const c_void) -> Result<(), Error> {
        if self.is_v3() {
            self.ok(unsafe { (self.api.taos_stmt_set_tags.unwrap())(self.as_ptr(), tags as _) })
        } else {
            self.ok(unsafe {
                (self.api.taos_stmt_set_tbname_tags)(
                    self.as_ptr(),
                    self.tbname.as_deref().unwrap().as_ptr(),
                    tags as _,
                )
            })
        }
    }

    #[inline]
    pub fn use_result(&mut self) -> Result<ResultSet, Error> {
        unsafe {
            RawRes::from_ptr(
                self.c.clone(),
                (self.api.taos_stmt_use_result)(self.as_ptr()),
            )
            .map(ResultSet::new)
        }
    }

    #[inline]
    pub fn affected_rows(&self) -> i32 {
        unsafe { (self.api.taos_stmt_affected_rows)(self.as_ptr()) }
    }

    #[inline]
    pub fn execute(&self) -> Result<usize, Error> {
        let cur = self.affected_rows();
        err_or!(self, (self.api.taos_stmt_execute)(self.as_ptr()))?;
        let new = self.affected_rows();
        Ok((new - cur) as usize)
    }

    #[inline]
    pub fn add_batch(&self) -> Result<(), Error> {
        err_or!(self, (self.api.taos_stmt_add_batch)(self.as_ptr()))
    }

    // #[inline]
    // pub fn is_insert(&self) -> Result<bool, Error> {
    //     let mut is_insert = 0;
    //     err_or!(
    //         self,
    //         (self.api.taos_stmt_is_insert)(self.as_ptr(), &mut is_insert as _),
    //         is_insert != 0
    //     )
    // }

    // #[inline]
    // pub fn num_params(&self) -> Result<usize, Error> {
    //     let mut num = 0i32;
    //     err_or!(
    //         self,
    //         (self.api.taos_stmt_num_params)(self.as_ptr(), &mut num as _),
    //         num as usize
    //     )
    // }

    // #[inline]
    // pub fn get_param(&mut self, idx: i32) -> Result<(Ty, i32), Error> {
    //     let (mut type_, mut bytes) = (0, 0);
    //     err_or!(
    //         self,
    //         (self.api.taos_stmt_get_param)(self.as_ptr(), idx, &mut type_ as _, &mut bytes as _),
    //         ((type_ as u8).into(), bytes)
    //     )
    // }
    // #[inline]
    // pub fn bind_param(&mut self, bind: *const c_void) -> Result<(), Error> {
    //     err_or!(self, (self.api.taos_stmt_bind_param)(self.as_ptr(), bind))
    // }

    #[inline]
    pub fn bind_param_batch(&mut self, bind: &[TaosMultiBind]) -> Result<(), Error> {
        err_or!(
            self,
            (self.api.taos_stmt_bind_param_batch)(self.as_ptr(), bind.as_ptr())
        )
    }

    // #[inline]
    // pub fn bind_single_param_batch(&self, bind: &TaosMultiBind, col: i32) -> Result<(), Error> {
    //     self.ok(unsafe {
    //         (self.api.taos_stmt_bind_single_param_batch)(self.as_ptr(), bind as *const _ as _, col)
    //     })
    // }
}

#[cfg(test)]
mod tests {

    use crate::{Stmt, TaosBuilder};

    #[test]
    fn test_tbname_tags() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let builder = TaosBuilder::from_dsn("taos:///")?;
        let taos = builder.build()?;
        taos.query("drop database if exists stt1")?;
        taos.query("create database if not exists stt1 keep 36500")?;
        taos.query("use stt1")?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create stable if not exists st1(ts timestamp, v int) tags(jt int, t1 varchar(32))",
        )?;

        let mut stmt = Stmt::init(&taos)?;
        let sql = "insert into ? using st1 tags(?, ?) values(?, ?)";
        stmt.prepare(sql)?;

        // let tags = vec![TaosBind::from_json(r#"{"name":"value"}"#)];
        let tbname = "tb1";
        stmt.set_tbname(tbname)?;

        let tags = vec![Value::Int(0), Value::VarChar(String::from("taos"))];
        stmt.set_tags(&tags)?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_ints(vec![0]),
        ];
        stmt.bind(&params)?;
        println!("bind");

        let params = vec![
            ColumnView::from_millis_timestamp(vec![1]),
            ColumnView::from_ints(vec![0]),
        ];
        stmt.bind(&params)?;
        println!("add batch");

        stmt.add_batch()?;
        println!("execute");
        stmt.execute()?;

        assert_eq!(stmt.affected_rows(), 2);
        println!("done");

        taos.query("drop database stt1")?;
        Ok(())
    }

    #[test]
    fn test_tbname_tags_json() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let builder = TaosBuilder::from_dsn("taos:///")?;
        let taos = builder.build()?;
        taos.query("drop database if exists stt2")?;
        taos.query("create database if not exists stt2 keep 36500")?;
        taos.query("use stt2")?;
        taos.query(
            "create stable if not exists st1(ts timestamp, v int) tags(jt json)", // "create stable if not exists st1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;

        let mut stmt = Stmt::init(&taos)?;
        let sql = "insert into ? using st1 tags(?) values(?, ?)";
        stmt.prepare(sql)?;

        let tags = vec![Value::Json(serde_json::from_str(r#"{"name":"value"}"#)?)];
        // let tags = vec![TaosBind::from(&0i32), TaosBind::from(&0.0f32)];
        println!("tags: {tags:#?}");
        let tbname = "tb1";
        stmt.set_tbname(tbname)?;

        stmt.set_tags(&tags)?;
        // stmt.set_tbname_tags_v3(&tbname, &tags)?;
        println!("bind");

        // todo: get_param not implemented in taosc 3.0
        // let p = stmt.get_param(0)?;
        // dbg!(p);
        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_ints(vec![0]),
        ];
        stmt.bind(&params)?;
        println!("add batch");

        stmt.add_batch()?;
        stmt.execute()?;

        assert!(stmt.affected_rows() == 1);

        stmt.prepare("insert into tb1 values(?,?)")?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![1]),
            ColumnView::from_ints(vec![0]),
        ];
        stmt.bind(&params)?;
        println!("add batch");

        stmt.add_batch()?;
        stmt.execute()?;

        let mut res = taos.query("select * from st1")?;

        if let Some(raw) = res.fetch_raw_block()? {
            assert_eq!(raw.nrows(), 2);
        } else {
            panic!("no data retrieved");
        }

        taos.query("drop database stt2")?;
        Ok(())
    }

    #[test]
    fn test_bindable() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
        taos.exec_many([
            "drop database if exists test_bindable",
            "create database test_bindable keep 36500",
            "use test_bindable",
            "create table tb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned,
            c10 float, c11 double, c12 varchar(100), c13 nchar(100))",
        ])?;
        let mut stmt = Stmt::init(&taos)?;
        stmt.prepare("insert into tb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
        let params = vec![
            ColumnView::from_millis_timestamp(vec![0]),
            ColumnView::from_bools(vec![true]),
            ColumnView::from_tiny_ints(vec![0]),
            ColumnView::from_small_ints(vec![0]),
            ColumnView::from_ints(vec![0]),
            ColumnView::from_big_ints(vec![0]),
            ColumnView::from_unsigned_tiny_ints(vec![0]),
            ColumnView::from_unsigned_small_ints(vec![0]),
            ColumnView::from_unsigned_ints(vec![0]),
            ColumnView::from_unsigned_big_ints(vec![0]),
            ColumnView::from_floats(vec![0.0]),
            ColumnView::from_doubles(vec![0.]),
            ColumnView::from_varchar(vec!["ABC"]),
            ColumnView::from_nchar(vec!["涛思数据"]),
        ];
        let rows = stmt.bind(&params)?.add_batch()?.execute()?;
        assert_eq!(rows, 1);

        let rows: Vec<(
            String,
            bool,
            i8,
            i16,
            i32,
            i64,
            u8,
            u16,
            u32,
            u64,
            f32,
            f64,
            String,
            String,
        )> = taos
            .query("select * from tb1")?
            .deserialize()
            .try_collect()?;
        let row = &rows[0];
        assert_eq!(row.12, "ABC");
        assert_eq!(row.13, "涛思数据");
        taos.query("drop database test_bindable")?;

        Ok(())
    }
}
