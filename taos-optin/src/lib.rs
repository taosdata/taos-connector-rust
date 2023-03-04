use std::{
    cell::UnsafeCell,
    ffi::{c_char, CStr, CString},
    fmt::Display,
    sync::Arc,
};

use once_cell::sync::OnceCell;
use raw::{ApiEntry, RawRes, RawTaos, SharedState};
// use taos_error::Error as RawError;
use taos_query::{
    prelude::{Field, Precision, RawError, RawMeta},
    Dsn, DsnError, RawBlock, TBuilder,
};

mod version {
    use std::fmt::Display;

    #[derive(Debug, PartialEq, PartialOrd)]
    struct Version {
        mainline: u8,
        major: u8,
        minor: u8,
        patch: u8,
    }

    impl Display for Version {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let Version {
                mainline,
                major,
                minor,
                patch,
            } = self;
            f.write_fmt(format_args!("{mainline}.{major}.{minor}.{patch}"))
        }
    }

    impl Version {
        pub(crate) const fn new(mainline: u8, major: u8, minor: u8, patch: u8) -> Self {
            Self {
                mainline,
                major,
                minor,
                patch,
            }
        }
        fn parse(version: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let version_items: Vec<_> = version.split('.').collect();
            let items = version_items.len();
            if items == 0 || items > 4 {
                Err("parse version error: {version}")?
            }

            let mainline = version_items[0].parse()?;
            let major = version_items
                .get(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or_default();
            let minor = version_items
                .get(2)
                .and_then(|s| s.parse().ok())
                .unwrap_or_default();
            let patch = version_items
                .get(3)
                .and_then(|s| s.parse().ok())
                .unwrap_or_default();

            Ok(Self::new(mainline, major, minor, patch))
        }
    }
}
mod into_c_str;
mod raw;
mod stmt;

#[allow(non_camel_case_types)]
pub(crate) mod types;

pub mod tmq;
pub use stmt::Stmt;
pub use tmq::{Consumer, TmqBuilder};

pub mod prelude {
    pub use super::{Consumer, ResultSet, Stmt, Taos, TaosBuilder, TmqBuilder};

    pub use taos_query::prelude::*;

    pub mod sync {
        pub use crate::{Consumer, ResultSet, Stmt, Taos, TaosBuilder, TmqBuilder};
        pub use taos_query::prelude::sync::*;
    }
}

#[macro_export(local_inner_macros)]
macro_rules! err_or {
    ($res:ident, $code:expr, $ret:expr) => {
        unsafe {
            let code: taos_query::prelude::Code = { $code }.into();
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
                Err(RawError::from_code(code))
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
        self.raw.query(sql.as_ref()).map(ResultSet::new)
    }

    fn write_raw_meta(&self, meta: &RawMeta) -> Result<(), Self::Error> {
        let raw = meta.as_raw_data_t();
        self.raw.write_raw_meta(raw)
    }

    fn write_raw_block(&self, raw: &RawBlock) -> Result<(), Self::Error> {
        self.raw.write_raw_block(raw)
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncQueryable for Taos {
    type Error = RawError;

    type AsyncResultSet = ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        let sql = sql.as_ref();
        log::debug!("query with sql: {}", sql);
        self.raw.query_async(sql).await.map(ResultSet::new)
    }

    async fn write_raw_meta(&self, meta: &taos_query::common::RawMeta) -> Result<(), Self::Error> {
        self.raw.write_raw_meta(meta.as_raw_data_t())
    }

    async fn write_raw_block(&self, block: &RawBlock) -> Result<(), Self::Error> {
        self.raw.write_raw_block(block)
    }
}

/// Connection builder.
///
/// ## Examples
///
/// ### Synchronous
///
/// ```rust
/// use taos_optin::prelude::sync::*;
/// fn main() -> anyhow::Result<()> {
///     let builder = TaosBuilder::from_dsn("taos://localhost:6030")?;
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
/// use taos_optin::prelude::*;
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
    dsn: Dsn,
    auth: Auth,
    lib: Arc<ApiEntry>,
    inner_conn: OnceCell<Taos>,
    server_version: OnceCell<String>,
}
impl TaosBuilder {
    fn inner_connection(&self) -> Result<&Taos, Error> {
        self.inner_conn.get_or_try_init(|| self.build())
    }
}

#[derive(Debug, Default)]
struct Auth {
    host: Option<CString>,
    user: Option<CString>,
    pass: Option<CString>,
    db: Option<CString>,
    port: u16,
}

impl Auth {
    pub(crate) fn host(&self) -> Option<&CStr> {
        self.host.as_deref()
    }
    pub(crate) fn host_as_ptr(&self) -> *const c_char {
        self.host().map_or_else(std::ptr::null, |s| s.as_ptr())
    }
    pub(crate) fn user(&self) -> Option<&CStr> {
        self.user.as_deref()
    }
    pub(crate) fn user_as_ptr(&self) -> *const c_char {
        self.user().map_or_else(std::ptr::null, |s| s.as_ptr())
    }
    pub(crate) fn password(&self) -> Option<&CStr> {
        self.pass.as_deref()
    }
    pub(crate) fn password_as_ptr(&self) -> *const c_char {
        self.password().map_or_else(std::ptr::null, |s| s.as_ptr())
    }
    pub(crate) fn database(&self) -> Option<&CStr> {
        self.db.as_deref()
    }
    pub(crate) fn database_as_ptr(&self) -> *const c_char {
        self.database().map_or_else(std::ptr::null, |s| s.as_ptr())
    }
    pub(crate) fn port(&self) -> u16 {
        self.port
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

impl TBuilder for TaosBuilder {
    type Target = Taos;

    type Error = Error;

    fn available_params() -> &'static [&'static str] {
        const PARAMS: &[&str] = &["configDir", "libraryPath"];
        PARAMS
    }

    fn from_dsn<D: taos_query::IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        let mut dsn = dsn.into_dsn()?;

        let lib = if let Some(path) = dsn.params.remove("libraryPath") {
            log::debug!("using library path: {path}");
            ApiEntry::dlopen(path).unwrap()
        } else {
            log::debug!("using default library of taos");
            ApiEntry::default()
        };
        let mut auth = Auth::default();
        // let mut builder = TaosBuilder::default();
        if let Some(addr) = dsn.addresses.first() {
            if let Some(host) = &addr.host {
                auth.host.replace(CString::new(host.as_str()).unwrap());
            }
            if let Some(port) = addr.port {
                auth.port = port;
            }
        }
        if let Some(db) = dsn.subject.as_deref() {
            auth.db.replace(CString::new(db).unwrap());
        }
        if let Some(user) = dsn.username.as_deref() {
            auth.user.replace(CString::new(user).unwrap());
        }
        if let Some(pass) = dsn.password.as_deref() {
            auth.pass.replace(CString::new(pass).unwrap());
        }
        let params = &dsn.params;
        if let Some(dir) = params.get("configDir") {
            lib.options(types::TSDB_OPTION::ConfigDir, dir);
        }

        lib.options(types::TSDB_OPTION::ShellActivityTimer, "3600");

        Ok(Self {
            dsn,
            auth,
            lib: Arc::new(lib),
            inner_conn: OnceCell::new(),
            server_version: OnceCell::new(),
        })
    }

    fn client_version() -> &'static str {
        "dynamic"
    }

    fn ping(&self, conn: &mut Self::Target) -> Result<(), Self::Error> {
        conn.raw.query("select 1")?;
        Ok(())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> Result<Self::Target, Self::Error> {
        let ptr = self.lib.connect(&self.auth);

        let raw = RawTaos::new(self.lib.clone(), ptr)?;
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

    fn precision(&self) -> Precision {
        self.raw.precision()
    }

    fn fields(&self) -> &[Field] {
        self.fields.get_or_init(|| self.raw.fetch_fields())
    }

    fn ncols(&self) -> usize {
        self.raw.field_count()
    }

    fn names(&self) -> impl Iterator<Item = &str> {
        self.fields().iter().map(|f| f.name())
    }

    fn update_summary(&mut self, nrows: usize) {
        let summary = self.summary.get_mut();
        summary.0 += 1;
        summary.1 += nrows;
    }

    pub(crate) fn summary(&self) -> &(usize, usize) {
        unsafe { &*self.summary.get() }
    }

    pub(crate) fn affected_rows(&self) -> i32 {
        self.raw.affected_rows() as _
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

impl taos_query::AsyncFetchable for ResultSet {
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
        self.raw
            .fetch_raw_block_async(self.fields(), self.precision(), &self.state, cx)
    }

    fn update_summary(&mut self, nrows: usize) {
        self.update_summary(nrows)
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        self.raw.free_result();
    }
}

unsafe impl Send for ResultSet {}
unsafe impl Sync for ResultSet {}

#[cfg(test)]
pub(crate) mod constants {
    pub const DSN_V2: &str = "taos://localhost:16030?libraryPath=tests/libs/libtaos.so.2.6.0.16";
    pub const DSN_V3: &str = "taos://localhost:26030?libraryPath=tests/libs/libtaos.so.3.0.1.5";
}

#[cfg(test)]
mod tests {
    use crate::constants::{DSN_V2, DSN_V3};

    use super::*;

    #[test]
    fn show_databases() -> Result<(), Error> {
        use taos_query::prelude::sync::*;
        let builder = TaosBuilder::from_dsn(DSN_V3)?;
        let taos = builder.build()?;
        let mut set = taos.query("show databases")?;

        for raw in &mut set.blocks() {
            let raw = raw?;
            for (col, view) in raw.columns().into_iter().enumerate() {
                for (row, value) in view.iter().enumerate().take(10) {
                    println!("Value at (row: {}, col: {}) is: {}", row, col, value);
                }
            }

            for (row, view) in raw.rows().enumerate().take(10) {
                for (col, value) in view.enumerate() {
                    println!("Value at (row: {}, col: {}) is: {:?}", row, col, value);
                }
            }
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }
    #[test]
    fn long_query() -> Result<(), Error> {
        use taos_query::prelude::sync::*;
        let builder = TaosBuilder::from_dsn(DSN_V3)?;
        let taos = builder.build()?;
        let mut set = taos.query("show databases")?;

        for raw in &mut set.blocks() {
            let raw = raw?;
            for (col, view) in raw.columns().into_iter().enumerate() {
                for (row, value) in view.iter().enumerate().take(10) {
                    println!("Value at (row: {}, col: {}) is: {}", row, col, value);
                }
            }

            for (row, view) in raw.rows().enumerate().take(10) {
                for (col, value) in view.enumerate() {
                    println!("Value at (row: {}, col: {}) is: {:?}", row, col, value);
                }
            }
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn long_query_async() -> Result<(), Error> {
        use taos_query::prelude::*;
        let builder = TaosBuilder::from_dsn(DSN_V3)?;
        let taos = builder.build()?;
        let mut set = taos.query("select * from test.meters limit 100000").await?;

        set.blocks()
            .try_for_each_concurrent(10, |block| async move {
                println!("{}", block.pretty_format());
                Ok(())
            })
            .await?;

        let mut set = taos.query("select * from test.meters limit 100000").await?;

        set.rows()
            .try_for_each_concurrent(10, |row| async move {
                println!(
                    "{}",
                    row.map(|(_, value)| value.to_string().unwrap()).join(",")
                );
                Ok(())
            })
            .await?;

        println!("summary: {:?}", set.summary());

        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn show_databases_async() -> Result<(), Error> {
        use taos_query::prelude::*;
        let builder = TaosBuilder::from_dsn(DSN_V3)?;
        let taos = builder.build()?;
        let mut set = taos.query("show databases").await?;

        let mut rows = set.rows();
        let mut nrows = 0;
        while let Some(row) = rows.try_next().await? {
            for (col, (name, value)) in row.enumerate() {
                println!("[{}, {}] (named `{:>4}`): {}", nrows, col, name, value);
            }
            nrows += 1;
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }

    #[test]
    fn show_databases_v2() -> Result<(), Error> {
        use taos_query::prelude::sync::*;
        let builder = TaosBuilder::from_dsn(crate::constants::DSN_V2)?;
        let taos = builder.build()?;
        let mut set = taos.query("show databases")?;

        for raw in &mut set.blocks() {
            let raw = raw?;
            for (col, view) in raw.columns().into_iter().enumerate() {
                for (row, value) in view.iter().enumerate().take(10) {
                    println!("Value at (row: {}, col: {}) is: {}", row, col, value);
                }
            }

            for (row, view) in raw.rows().enumerate().take(10) {
                for (col, value) in view.enumerate() {
                    println!("Value at (row: {}, col: {}) is: {:?}", row, col, value);
                }
            }
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn show_databases_async_v2() -> Result<(), Error> {
        use taos_query::prelude::*;
        let builder = TaosBuilder::from_dsn(DSN_V2)?;
        let taos = builder.build()?;
        let mut set = taos.query("show databases").await?;

        let mut rows = set.rows();
        let mut nrows = 0;
        while let Some(row) = rows.try_next().await? {
            for (col, (name, value)) in row.enumerate() {
                println!("[{}, {}] (named `{:>4}`): {}", nrows, col, name, value);
            }
            nrows += 1;
        }

        println!("summary: {:?}", set.summary());
        Ok(())
    }
}
