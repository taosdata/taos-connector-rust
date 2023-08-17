use crate::{err_or, ffi::*, into_c_str::IntoCStr, RawRes, RawTaos, ResultSet};

use std::ffi::CStr;

use itertools::Itertools;
use taos_query::prelude::{Code, RawError};
use taos_query::stmt::AsyncBindable;
use taos_query::{common::Ty, stmt::Bindable, Queryable, RawResult};

use crate::types::*;

mod bind;
mod multi;

#[derive(Debug)]
pub struct Stmt {
    raw: RawStmt,
}

unsafe impl Send for Stmt {}
unsafe impl Sync for Stmt {}
impl Bindable<super::Taos> for Stmt {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt::from_raw_taos(&taos.raw),
        })
    }

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self> {
        self.raw.prepare(sql.as_ref())?;
        Ok(self)
    }

    fn set_tbname<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self> {
        self.raw.set_tbname(sql.as_ref())?;
        Ok(self)
    }

    fn set_tags(&mut self, tags: &[taos_query::common::Value]) -> RawResult<&mut Self> {
        let tags = tags.iter().map(TaosBind::from_value).collect_vec();
        self.raw.set_tags(&tags)?;
        Ok(self)
    }

    fn bind(&mut self, params: &[taos_query::common::ColumnView]) -> RawResult<&mut Self> {
        let params: Vec<DropMultiBind> = params.iter().map(|c| c.into()).collect_vec();
        self.raw
            .bind_param_batch(unsafe { std::mem::transmute(params.as_slice()) })?;
        Ok(self)
    }

    fn add_batch(&mut self) -> RawResult<&mut Self> {
        self.raw.add_batch()?;
        Ok(self)
    }

    fn execute(&mut self) -> RawResult<usize> {
        self.raw.execute().map_err(Into::into)
    }

    fn result_set(&mut self) -> RawResult<<super::Taos as Queryable>::ResultSet> {
        self.raw.use_result().map_err(Into::into)
    }

    fn affected_rows(&self) -> usize {
        self.raw.affected_rows() as _
    }
}

#[async_trait::async_trait]
impl AsyncBindable<super::Taos> for Stmt {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt::from_raw_taos(&taos.raw),
        })
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.raw.prepare(sql)?;
        Ok(self)
    }

    async fn set_tbname(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.raw.set_tbname(sql)?;
        Ok(self)
    }

    async fn set_tags(&mut self, tags: &[taos_query::common::Value]) -> RawResult<&mut Self> {
        let tags = tags.iter().map(TaosBind::from_value).collect_vec();
        self.raw.set_tags(&tags)?;
        Ok(self)
    }

    async fn bind(&mut self, params: &[taos_query::common::ColumnView]) -> RawResult<&mut Self> {
        let params: Vec<DropMultiBind> = params.iter().map(|c| c.into()).collect_vec();
        self.raw
            .bind_param_batch(unsafe { std::mem::transmute(params.as_slice()) })?;
        Ok(self)
    }

    async fn add_batch(&mut self) -> RawResult<&mut Self> {
        self.raw.add_batch()?;
        Ok(self)
    }

    async fn execute(&mut self) -> RawResult<usize> {
        self.raw.execute().map_err(Into::into)
    }

    async fn result_set(&mut self) -> RawResult<<super::Taos as Queryable>::ResultSet> {
        self.raw.use_result().map_err(Into::into)
    }

    async fn affected_rows(&self) -> usize {
        self.raw.affected_rows() as _
    }
}

#[derive(Debug)]
pub(crate) struct RawStmt(*mut TAOS_STMT);

impl Drop for RawStmt {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl RawStmt {
    #[inline(always)]
    fn ok(&self, code: impl Into<Code>) -> RawResult<()> {
        let code = code.into();

        if code.success() {
            Ok(())
        } else {
            Err(RawError::from_string(self.err_as_str()))
        }
    }

    #[inline]
    pub unsafe fn as_ptr(&self) -> *mut TAOS_STMT {
        self.0
    }

    #[inline]
    pub fn errstr(&self) -> &CStr {
        unsafe { CStr::from_ptr(taos_stmt_errstr(self.as_ptr())) }
    }

    #[inline]
    pub fn err_as_str(&self) -> String {
        unsafe {
            CStr::from_ptr(taos_stmt_errstr(self.as_ptr()))
                .to_string_lossy()
                .to_string()
        }
    }

    #[inline]
    pub fn from_raw_taos(taos: &RawTaos) -> RawStmt {
        RawStmt(unsafe { taos_stmt_init(taos.as_ptr()) })
    }
    #[inline]
    pub fn close(&mut self) -> RawResult<()> {
        err_or!(self, taos_stmt_close(self.as_ptr()))
    }

    #[inline]
    pub fn prepare<'c>(&mut self, sql: impl IntoCStr<'c>) -> RawResult<()> {
        let sql = sql.into_c_str();
        self.ok(unsafe {
            taos_stmt_prepare(self.as_ptr(), sql.as_ptr(), sql.to_bytes().len() as _)
        })
    }

    pub fn set_tbname_tags_v3<'a>(
        &mut self,
        name: impl IntoCStr<'a>,
        tags: &[TaosBind],
    ) -> RawResult<()> {
        self.ok(unsafe {
            taos_stmt_set_tbname_tags(
                self.as_ptr(),
                name.into_c_str().as_ptr(),
                tags.as_ptr() as _,
            )
        })
    }

    #[inline]
    pub fn set_tbname<'c>(&mut self, name: impl IntoCStr<'c>) -> RawResult<()> {
        self.ok(unsafe { taos_stmt_set_tbname(self.as_ptr(), name.into_c_str().as_ptr()) })
    }

    #[inline]
    pub fn set_sub_tbname<'c>(&mut self, name: impl IntoCStr<'c>) -> RawResult<()> {
        self.ok(unsafe { taos_stmt_set_sub_tbname(self.as_ptr(), name.into_c_str().as_ptr()) })
    }

    #[inline]
    pub fn set_tags(&mut self, tags: &[TaosBind]) -> RawResult<()> {
        self.ok(unsafe { taos_stmt_set_tags(self.as_ptr(), tags.as_ptr() as _) })
    }

    #[inline]
    pub fn use_result(&mut self) -> RawResult<ResultSet> {
        unsafe { RawRes::from_ptr(taos_stmt_use_result(self.as_ptr())).map(ResultSet::new) }
    }

    #[inline]
    pub fn affected_rows(&self) -> i32 {
        unsafe { taos_stmt_affected_rows(self.as_ptr()) }
    }

    #[inline]
    pub fn execute(&self) -> RawResult<usize> {
        let cur = self.affected_rows();
        err_or!(self, taos_stmt_execute(self.as_ptr()))?;
        let new = self.affected_rows();
        Ok((new - cur) as usize)
    }

    #[inline]
    pub fn add_batch(&self) -> RawResult<()> {
        err_or!(self, taos_stmt_add_batch(self.as_ptr()))
    }

    #[inline]
    pub fn is_insert(&self) -> RawResult<bool> {
        let mut is_insert = 0;
        err_or!(
            self,
            taos_stmt_is_insert(self.as_ptr(), &mut is_insert as _),
            is_insert != 0
        )
    }

    #[inline]
    pub fn num_params(&self) -> RawResult<usize> {
        let mut num = 0i32;
        err_or!(
            self,
            taos_stmt_num_params(self.as_ptr(), &mut num as _),
            num as usize
        )
    }

    #[inline]
    pub fn get_param(&mut self, idx: i32) -> RawResult<(Ty, i32)> {
        let (mut type_, mut bytes) = (0, 0);
        err_or!(
            self,
            taos_stmt_get_param(self.as_ptr(), idx, &mut type_ as _, &mut bytes as _),
            ((type_ as u8).into(), bytes)
        )
    }
    #[inline]
    pub fn bind_param(&mut self, bind: &[TaosBind]) -> RawResult<()> {
        err_or!(self, taos_stmt_bind_param(self.as_ptr(), bind.as_ptr()))
    }

    #[inline]
    pub fn bind_param_batch(&mut self, bind: &[TaosMultiBind]) -> RawResult<()> {
        err_or!(
            self,
            taos_stmt_bind_param_batch(self.as_ptr(), bind.as_ptr())
        )
    }

    #[inline]
    pub fn bind_single_param_batch(&self, bind: &TaosMultiBind, col: i32) -> RawResult<()> {
        self.ok(unsafe {
            taos_stmt_bind_single_param_batch(self.as_ptr(), bind as *const _ as _, col)
        })
    }
}

#[cfg(test)]
mod tests {
    use taos_query::{
        prelude::ColumnView, stmt::Bindable, Fetchable, Queryable, RawResult, TBuilder,
    };

    use crate::{stmt::RawStmt, types::*, RawTaos, Stmt, TaosBuilder};

    use itertools::Itertools;

    #[test]
    fn test_tbname_tags() -> RawResult<()> {
        use std::ptr::null;
        use taos_query::common::itypes::ITimestamp;

        let host = null();
        let user = null();
        let pass = null();
        let db = null();
        let port = 0;
        let taos = RawTaos::connect(host, user, pass, db, port)?;
        taos.query("drop database if exists stt1")?;
        taos.query("create database if not exists stt1 keep 36500")?;
        taos.query("use stt1")?;
        taos.query(
            // "create stable if not exists st1(ts timestamp, v int) tags(jt json)"
            "create stable if not exists st1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;

        let mut stmt = RawStmt::from_raw_taos(&taos);
        let sql = "insert into ? using st1 tags(?, ?) values(?, ?)";
        stmt.prepare(sql)?;

        // let tags = vec![TaosBind::from_json(r#"{"name":"value"}"#)];
        let tags = vec![TaosBind::from(&0i32), TaosBind::from(&0.0f32)];
        println!("tags: {tags:#?}");
        let tbname = "tb1";
        stmt.set_tbname(tbname)?;
        stmt.set_tags(&tags)?;
        // stmt.set_tbname_tags_v3(&tbname, &tags)?;
        println!("bind");

        // todo: get_param not implemented in taosc 3.0
        // let p = stmt.get_param(0)?;
        // dbg!(p);

        let params = vec![TaosBind::from(&ITimestamp(0)), TaosBind::from(&0i32)];
        stmt.bind_param(&params)?;
        println!("add batch");

        stmt.add_batch()?;
        stmt.execute()?;

        assert!(stmt.affected_rows() == 1);
        println!("done");

        // let mut stmt = RawStmt::from_raw_taos(&taos);
        // stmt.prepare("select * from ?")?;
        // stmt.set_tbname("st1")?;
        // stmt.execute()?;

        // let mut res = stmt.use_result()?;
        // use taos_query::prelude::*;
        // let blocks = res.blocks().try_for_each(|block| {
        //     dbg!(block);
        //     futures::future::ready(Ok(()))
        // });

        taos.query("drop database stt1")?;
        Ok(())
    }

    #[test]
    fn test_tbname_tags_json() -> RawResult<()> {
        use std::ptr::null;
        use taos_query::common::itypes::ITimestamp;
        let host = null();
        let user = null();
        let pass = null();
        let db = null();
        let port = 0;
        let taos = RawTaos::connect(host, user, pass, db, port)?;
        taos.query("drop database if exists stt2")?;
        taos.query("create database if not exists stt2 keep 36500")?;
        taos.query("use stt2")?;
        taos.query(
            "create stable if not exists st1(ts timestamp, v int) tags(jt json)", // "create stable if not exists st1(ts timestamp, v int) tags(jt int, t1 float)",
        )?;

        let mut stmt = RawStmt::from_raw_taos(&taos);
        let sql = "insert into ? using st1 tags(?) values(?, ?)";
        stmt.prepare(sql)?;

        let tags = vec![TaosBind::from_json(r#"{"name":"value"}"#)];
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

        let params = vec![TaosBind::from(&ITimestamp(0)), TaosBind::from(&0i32)];
        stmt.bind_param(&params)?;
        println!("add batch");

        stmt.add_batch()?;
        stmt.execute()?;

        assert!(stmt.affected_rows() == 1);

        stmt.prepare("insert into tb1 values(?,?)")?;
        let params = vec![TaosBind::from(&ITimestamp(1)), TaosBind::from(&0i32)];
        stmt.bind_param(&params)?;
        println!("add batch");

        stmt.add_batch()?;
        stmt.execute()?;

        let res = taos.query("select * from st1")?;

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
