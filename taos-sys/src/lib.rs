#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]

//! TDengine database connector use native C library.
//!

use std::{
    cell::UnsafeCell,
    ffi::CString,
    fmt::Display,
    sync::Arc,
    task::{Context, Poll},
};

use once_cell::sync::OnceCell;
use query::blocks::SharedState;
pub use taos_query::prelude::*;
// use taos_query::{AsyncFetchable, AsyncQueryable, DsnError, Fetchable, Queryable, TBuilder};

pub mod sync {
    pub use super::{Consumer, Stmt, Taos, TaosBuilder, TmqBuilder};
    pub use taos_query::prelude::sync::*;
}

mod into_c_str;
pub mod stmt;

use into_c_str::IntoCStr;

pub(crate) mod types;

mod ffi;
// use ffi::*;

mod set_config;

use ffi::taos_options;

pub mod schemaless;
// pub use schemaless::*;

pub mod tmq;
pub use tmq::{Consumer, TmqBuilder};

mod conn;
use conn::RawTaos;

mod query;
use query::RawRes;

pub use stmt::Stmt;
pub use taos_query::prelude::*;
pub use types::TaosMultiBind;

#[macro_export(local_inner_macros)]
macro_rules! err_or {
    ($res:ident, $code:expr, $ret:expr) => {
        unsafe {
            let code: Code = { $code }.into();
            if code.success() {
                Ok($ret)
            } else {
                Err(taos_query::prelude::RawError::new(code, $res.err_as_str()))
            }
        }
    };

    ($res:ident, $code:expr) => {{
        err_or!($res, $code, ())
    }};
    ($code:expr, $ret:expr) => {
        unsafe {
            let code: Code = { $code }.into();
            if code.success() {
                Ok($ret)
            } else {
                Err(Error::from_code(code))
            }
        }
    };

    ($code:expr) => {
        err_or!($code, ())
    };
}

#[derive(Debug)]
pub struct Taos {
    raw: RawTaos,
}

impl Drop for Taos {
    fn drop(&mut self) {
        self.raw.close();
    }
}

impl taos_query::Queryable for Taos {
    type Error = RawError;

    type ResultSet = ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> Result<Self::ResultSet, Self::Error> {
        log::debug!("Query with SQL: {}", sql.as_ref());
        self.raw.query(sql.as_ref())
    }

    fn query_with_req_id<T: AsRef<str>>(
        &self,
        sql: T,
        req_id: u64,
    ) -> Result<Self::ResultSet, Self::Error> {
        log::debug!("Query with SQL: {}", sql.as_ref());
        self.raw.query_with_req_id(sql.as_ref(), req_id)
    }

    fn write_raw_meta(&self, meta: &RawMeta) -> Result<(), Self::Error> {
        let raw = meta.as_raw_data_t();
        self.raw.write_raw_meta(raw)
    }

    fn write_raw_block(&self, block: &RawBlock) -> Result<(), Self::Error> {
        self.raw.write_raw_block(block)
    }

    fn put(&self, data: &taos_query::common::SmlData) -> Result<(), Self::Error> {
        self.raw.put(data)
    }
}

#[async_trait::async_trait]
impl AsyncQueryable for Taos {
    type Error = RawError;

    type AsyncResultSet = ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        log::debug!("Async query with SQL: {}", sql.as_ref());
        self.raw.query_async(sql.as_ref()).await.map(ResultSet::new)
    }

    async fn query_with_req_id<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
        req_id: u64,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        log::debug!("Async query with SQL: {}", sql.as_ref());
        self.raw.query_async(sql.as_ref()).await.map(ResultSet::new)
    }

    async fn write_raw_meta(&self, meta: &taos_query::common::RawMeta) -> Result<(), Self::Error> {
        self.raw.write_raw_meta(meta.as_raw_data_t())
    }

    async fn write_raw_block(&self, block: &RawBlock) -> Result<(), Self::Error> {
        self.raw.write_raw_block(block)
    }

    async fn put(&self, data: &taos_query::common::SmlData) -> Result<(), Self::Error> {
        todo!()
    }
}

/// Connection builder.
///
/// ## Examples
///
/// ### Synchronous
///
/// ```rust
/// use taos_sys::sync::*;
/// use std::str::FromStr;
/// fn main() -> anyhow::Result<()> {
///     let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
///     let dsn = Dsn::from_str(&dsn)?;
///     let builder = TaosBuilder::from_dsn(&dsn)?;
///     let taos = builder.build()?;
///     let mut query = taos.query("show databases")?;
///     for row in query.rows() {
///         println!("{:?}", row?.into_values());
///     }
///     Ok(())
/// }
/// ```
///
/// ### Async
///
/// ```rust
/// use taos_sys::*;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let builder = TaosBuilder::from_dsn("taos://localhost:6030")?;
///     let taos = builder.build()?;
///     let mut query = taos.query("show databases").await?;
///
///     while let Some(row) = query.rows().try_next().await? {
///         println!("{:?}", row.into_values());
///     }
///     Ok(())
/// }
/// #
/// ```
#[derive(Debug, Default)]
pub struct TaosBuilder {
    host: Option<CString>,
    user: Option<CString>,
    pass: Option<CString>,
    db: Option<CString>,
    port: u16,
    inner_conn: OnceCell<Taos>,
    server_version: OnceCell<String>,
}

impl TaosBuilder {
    fn from_dsn<D: taos_query::IntoDsn>(dsn: D) -> Result<Self, Error> {
        let dsn = dsn.into_dsn()?;
        let mut builder = TaosBuilder::default();
        if let Some(addr) = dsn.addresses.into_iter().next() {
            if let Some(host) = addr.host {
                builder.host.replace(CString::new(host).unwrap());
            }
            if let Some(port) = addr.port {
                builder.port = port;
            }
        }
        if let Some(db) = dsn.subject {
            builder.db.replace(CString::new(db).unwrap());
        }
        if let Some(user) = dsn.username {
            builder.user.replace(CString::new(user).unwrap());
        }
        if let Some(pass) = dsn.password {
            builder.pass.replace(CString::new(pass).unwrap());
        }
        let params = dsn.params;
        if let Some(dir) = params.get("configDir") {
            let dir = CString::new(dir.as_bytes()).unwrap();
            unsafe {
                taos_options(types::TSDB_OPTION::ConfigDir, dir.as_ptr() as _);
            }
        }
        // let raw = RawTaos::connect(host, user, pass, db, port)
        Ok(builder)
    }
    fn inner_connection(&self) -> Result<&Taos, Error> {
        use taos_query::prelude::sync::TBuilder;
        self.inner_conn
            .get_or_try_init(|| <Self as TBuilder>::build(&self))
    }

    async fn async_inner_connection(&self) -> Result<&Taos, Error> {
        match self.inner_conn.get() {
            None => {
                let inner = <Self as AsyncTBuilder>::build(&self).await;
                self.inner_conn.get_or_try_init(|| inner)
            }
            Some(v) => Ok(v),
        }
    }
}

#[derive(Debug)]
pub struct Error(RawError);

impl From<DsnError> for Error {
    fn from(err: DsnError) -> Self {
        Self(RawError::from_string(err.to_string()))
    }
}
impl From<RawError> for Error {
    fn from(err: RawError) -> Self {
        Self(err)
    }
}
impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl taos_query::TBuilder for TaosBuilder {
    type Target = Taos;

    type Error = Error;

    fn available_params() -> &'static [&'static str] {
        const PARAMS: &[&str] = &["configDir"];
        PARAMS
    }

    fn from_dsn<D: taos_query::IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        let dsn = dsn.into_dsn()?;
        let mut builder = TaosBuilder::default();
        if let Some(addr) = dsn.addresses.into_iter().next() {
            if let Some(host) = addr.host {
                builder.host.replace(CString::new(host).unwrap());
            }
            if let Some(port) = addr.port {
                builder.port = port;
            }
        }
        if let Some(db) = dsn.subject {
            builder.db.replace(CString::new(db).unwrap());
        }
        if let Some(user) = dsn.username {
            builder.user.replace(CString::new(user).unwrap());
        }
        if let Some(pass) = dsn.password {
            builder.pass.replace(CString::new(pass).unwrap());
        }
        let params = dsn.params;
        if let Some(dir) = params.get("configDir") {
            let dir = CString::new(dir.as_bytes()).unwrap();
            unsafe {
                taos_options(types::TSDB_OPTION::ConfigDir, dir.as_ptr() as _);
            }
        }
        // let raw = RawTaos::connect(host, user, pass, db, port)
        Ok(builder)
    }

    fn client_version() -> &'static str {
        RawTaos::version()
    }
    fn ping(&self, conn: &mut Self::Target) -> Result<(), Self::Error> {
        conn.raw.query("select 1")?;
        Ok(())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> Result<Self::Target, Self::Error> {
        let raw = RawTaos::connect(
            self.host
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.user
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.pass
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.db
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.port,
        )?;

        Ok(Taos { raw })
    }

    fn server_version(&self) -> Result<&str, Self::Error> {
        if let Some(v) = self.server_version.get() {
            Ok(v.as_str())
        } else {
            let conn = self.inner_connection()?;
            use taos_query::prelude::sync::Queryable;
            let v: String = Queryable::query_one(conn, "select server_version()")?.unwrap();
            Ok(match self.server_version.try_insert(v) {
                Ok(v) => v.as_str(),
                Err((v, _)) => v.as_str(),
            })
        }
    }

    fn is_enterprise_edition(&self) -> bool {
        if let Ok(taos) = self.inner_connection() {
            use taos_query::prelude::sync::Queryable;
            let grant: Option<(String, bool)> = Queryable::query_one(
                taos,
                "select version, (expire_time < now) as valid from information_schema.ins_cluster",
            )
            .unwrap_or_default();

            if let Some((edition, expired)) = grant {
                if expired {
                    return false;
                }
                return match edition.as_str() {
                    "cloud" | "official" | "trial" => true,
                    _ => false,
                };
            }

            let grant: Option<(String, (), String)> =
                Queryable::query_one(taos, "show grants").unwrap_or_default();

            if let Some((edition, _, expired)) = grant {
                match (edition.trim(), expired.trim()) {
                    ("cloud" | "official" | "trial", "false") => true,
                    _ => false,
                }
            } else {
                false
            }
        } else {
            false
        }
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TaosBuilder {
    type Target = Taos;

    type Error = Error;

    fn from_dsn<D: taos_query::IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        let dsn = dsn.into_dsn()?;
        let mut builder = TaosBuilder::default();
        if let Some(addr) = dsn.addresses.into_iter().next() {
            if let Some(host) = addr.host {
                builder.host.replace(CString::new(host).unwrap());
            }
            if let Some(port) = addr.port {
                builder.port = port;
            }
        }
        if let Some(db) = dsn.subject {
            builder.db.replace(CString::new(db).unwrap());
        }
        if let Some(user) = dsn.username {
            builder.user.replace(CString::new(user).unwrap());
        }
        if let Some(pass) = dsn.password {
            builder.pass.replace(CString::new(pass).unwrap());
        }
        let params = dsn.params;
        if let Some(dir) = params.get("configDir") {
            let dir = CString::new(dir.as_bytes()).unwrap();
            unsafe {
                taos_options(types::TSDB_OPTION::ConfigDir, dir.as_ptr() as _);
            }
        }
        // let raw = RawTaos::connect(host, user, pass, db, port)
        Ok(builder)
    }

    fn client_version() -> &'static str {
        RawTaos::version()
    }
    async fn ping(&self, conn: &mut Self::Target) -> Result<(), Self::Error> {
        conn.raw.query("select 1")?;
        Ok(())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> Result<Self::Target, Self::Error> {
        let raw = RawTaos::connect(
            self.host
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.user
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.pass
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.db
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.port,
        )?;

        Ok(Taos { raw })
    }

    async fn server_version(&self) -> Result<&str, Self::Error> {
        if let Some(v) = self.server_version.get() {
            Ok(v.as_str())
        } else {
            let conn = self.async_inner_connection().await?;
            use taos_query::prelude::AsyncQueryable;
            let v: String = AsyncQueryable::query_one(conn, "select server_version()")
                .await?
                .unwrap();
            Ok(match self.server_version.try_insert(v) {
                Ok(v) => v.as_str(),
                Err((v, _)) => v.as_str(),
            })
        }
    }

    async fn is_enterprise_edition(&self) -> bool {
        if let Ok(taos) = self.async_inner_connection().await {
            use taos_query::prelude::AsyncQueryable;
            let grant: Option<(String, bool)> = AsyncQueryable::query_one(
                taos,
                "select version, (expire_time < now) as valid from information_schema.ins_cluster",
            )
            .await
            .unwrap_or_default();

            if let Some((edition, expired)) = grant {
                if expired {
                    return false;
                }
                return match edition.as_str() {
                    "cloud" | "official" | "trial" => true,
                    _ => false,
                };
            }

            let grant: Option<(String, (), String)> =
                AsyncQueryable::query_one(taos, "show grants")
                    .await
                    .unwrap_or_default();

            if let Some((edition, _, expired)) = grant {
                match (edition.trim(), expired.trim()) {
                    ("cloud" | "official" | "trial", "false") => true,
                    _ => false,
                }
            } else {
                false
            }
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct ResultSet {
    raw: RawRes,
    fields: OnceCell<Vec<Field>>,
    summary: UnsafeCell<(usize, usize)>,
    state: Arc<UnsafeCell<SharedState>>,
}

impl ResultSet {
    fn new(raw: RawRes) -> Self {
        Self {
            raw,
            fields: OnceCell::new(),
            summary: UnsafeCell::new((0, 0)),
            state: Arc::new(UnsafeCell::new(SharedState::default())),
        }
    }

    pub fn precision(&self) -> Precision {
        self.raw.precision()
    }

    pub fn fields(&self) -> &[Field] {
        self.fields.get_or_init(|| self.raw.fetch_fields())
    }

    pub fn ncols(&self) -> usize {
        self.raw.field_count()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.fields().iter().map(|f| f.name())
    }

    fn update_summary(&mut self, nrows: usize) {
        let summary = self.summary.get_mut();
        summary.0 += 1;
        summary.1 += nrows;
    }

    // pub(crate) fn blocks(&self) -> Blocks {
    //     self.raw.to_blocks()
    // }

    pub(crate) fn summary(&self) -> &(usize, usize) {
        unsafe { &*self.summary.get() }
    }

    pub(crate) fn affected_rows(&self) -> i32 {
        self.raw.affected_rows() as _
    }

    pub(crate) fn fetch_raw_block(&self) -> Result<Option<RawBlock>, RawError> {
        self.raw.fetch_raw_block(self.fields())
    }

    pub(crate) fn fetch_raw_block_async(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, RawError>> {
        self.raw
            .fetch_raw_block_async(self.fields(), self.precision(), &self.state, cx)
    }
}

impl Iterator for ResultSet {
    type Item = Result<RawBlock, RawError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw
            .fetch_raw_block(self.fields())
            .transpose()
            .map(|block| {
                block.map(|raw| {
                    let mut summary = unsafe { &mut *self.summary.get() };
                    summary.0 += 1;
                    summary.1 += raw.nrows();
                    raw
                })
            })
    }
}

impl taos_query::Fetchable for ResultSet {
    type Error = RawError;
    fn affected_rows(&self) -> i32 {
        self.affected_rows()
    }

    fn precision(&self) -> Precision {
        self.precision()
    }

    fn fields(&self) -> &[Field] {
        self.fields()
    }

    fn summary(&self) -> (usize, usize) {
        *self.summary()
    }

    fn update_summary(&mut self, nrows: usize) {
        self.update_summary(nrows)
    }

    fn fetch_raw_block(&mut self) -> Result<Option<RawBlock>, Self::Error> {
        self.raw.fetch_raw_block(self.fields())
    }
}

impl AsyncFetchable for ResultSet {
    type Error = RawError;

    fn affected_rows(&self) -> i32 {
        self.affected_rows()
    }

    fn precision(&self) -> Precision {
        self.precision()
    }

    fn fields(&self) -> &[Field] {
        self.fields()
    }

    fn summary(&self) -> (usize, usize) {
        *self.summary()
    }

    fn fetch_raw_block(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<RawBlock>, Self::Error>> {
        self.fetch_raw_block_async(cx)
    }

    fn update_summary(&mut self, nrows: usize) {
        self.update_summary(nrows)
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        self.raw.drop();
    }
}

unsafe impl Send for ResultSet {}
unsafe impl Sync for ResultSet {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_databases() -> Result<(), Error> {
        let null = std::ptr::null();
        let taos = RawTaos::connect(null, null, null, null, 0)?;
        let mut set = taos.query("show databases")?;

        for raw in &mut set {
            let raw = raw?;
            for (col, view) in raw.columns().into_iter().enumerate() {
                for (row, value) in view.iter().enumerate() {
                    println!("Value at (row: {}, col: {}) is: {}", row, col, value);
                }
            }

            for (row, view) in raw.rows().enumerate() {
                for (col, value) in view.enumerate() {
                    println!("Value at (row: {}, col: {}) is: {:?}", row, col, value);
                }
            }
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }

    #[test]
    #[cfg(taos_parse_time)]
    fn test_parse_time() {
        use std::ffi::CString;
        let s = CString::new("1970-01-01 00:00:00").unwrap();
        let mut time = 0i64;
        unsafe {
            taos_options(
                TSDB_OPTION_TIMEZONE,
                b"Europe/Landon\0" as *const u8 as *const _,
            )
        };
        let res = unsafe {
            taos_parse_time(
                s.as_ptr(),
                &mut time as _,
                s.to_bytes().len() as _,
                Precision::Microsecond,
                0,
            )
        };
        assert_eq!(res, 0, "success");
        assert_eq!(time, 0, "parse time");

        let s = CString::new("1970-01-01 08:00:00").unwrap(); // CST +8
                                                              // timezone could be set multiple times
        unsafe {
            taos_options(
                TSDB_OPTION_TIMEZONE,
                b"Asia/Shanghai\0" as *const u8 as *const _,
            )
        };
        let res = unsafe {
            taos_parse_time(
                s.as_ptr(),
                &mut time as _,
                s.to_bytes().len() as _,
                Precision::Microsecond,
                0,
            )
        };
        assert_eq!(res, 0, "success");
        assert_eq!(time, 0, "parse time");
    }
}
