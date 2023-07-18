#![recursion_limit = "256"]
use std::fmt::{Debug, Display};

use once_cell::sync::OnceCell;

use taos_query::prelude::Code;
use taos_query::{DsnError, IntoDsn, RawResult};

mod stmt;
pub use stmt::Stmt;

// pub mod tmq;
pub mod consumer;
pub use consumer::{Consumer, Offset, TmqBuilder};

pub mod query;
pub use query::ResultSet;
pub use query::Taos;

use query::WsConnReq;

pub mod schemaless;

#[derive(Debug, Clone)]
pub enum WsAuth {
    Token(String),
    Plain(String, String),
}

#[derive(Debug, Clone)]
pub struct TaosBuilder {
    scheme: &'static str, // ws or wss
    addr: String,
    auth: WsAuth,
    database: Option<String>,
    server_version: OnceCell<String>,
    // timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub struct Error {
    code: Code,
    source: anyhow::Error,
}

impl Error {
    pub const fn errno(&self) -> Code {
        self.code
    }
    pub fn errstr(&self) -> String {
        self.source.to_string()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.source.to_string())
    }
}

impl From<DsnError> for Error {
    fn from(err: DsnError) -> Self {
        Error {
            code: Code::FAILED,
            source: err.into(),
        }
    }
}
impl From<query::asyn::Error> for Error {
    fn from(err: query::asyn::Error) -> Self {
        Error {
            code: Code::FAILED,
            source: err.into(),
        }
    }
}

impl taos_query::TBuilder for TaosBuilder {
    type Target = Taos;

    fn available_params() -> &'static [&'static str] {
        &["token"]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::from_dsn(dsn.into_dsn()?)
    }

    fn client_version() -> &'static str {
        "0"
    }
    fn ping(&self, taos: &mut Self::Target) -> RawResult<()> {
        taos_query::Queryable::exec(taos, "SELECT 1").map(|_| ())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> RawResult<Self::Target> {
        Ok(Taos {
            dsn: self.clone(),
            async_client: OnceCell::new(),
            async_sml: OnceCell::new(),
        })
    }

    fn server_version(&self) -> RawResult<&str> {
        if let Some(v) = self.server_version.get() {
            Ok(v.as_str())
        } else {
            let conn = self.build()?;
            use taos_query::prelude::sync::Queryable;
            let v: String = Queryable::query_one(&conn, "select server_version()")?.unwrap();
            Ok(match self.server_version.try_insert(v) {
                Ok(v) => v.as_str(),
                Err((v, _)) => v.as_str(),
            })
        }
    }
    fn is_enterprise_edition(&self) -> RawResult<bool> {
        if self.addr.matches(".cloud.tdengine.com").next().is_some()
            || self.addr.matches(".cloud.taosdata.com").next().is_some()
        {
            return Ok(true);
        }

        let taos = self.build()?;

        use taos_query::prelude::sync::Queryable;
        let grant: Option<(String, bool)> = Queryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        )
        .unwrap_or_default();

        if let Some((edition, expired)) = grant {
            if expired {
                return Ok(false);
            }
            return match edition.trim() {
                "cloud" | "official" | "trial" => Ok(true),
                _ => Ok(false),
            };
        }

        let grant: Option<(String, (), String)> =
            Queryable::query_one(&taos, "show grants").unwrap_or_default();

        if let Some((edition, _, expired)) = grant {
            match (edition.trim(), expired.trim()) {
                ("cloud" | "official" | "trial", "false") => Ok(true),
                _ => Ok(false),
            }
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TaosBuilder {
    type Target = Taos;

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::from_dsn(dsn.into_dsn()?)
    }

    fn client_version() -> &'static str {
        "0"
    }
    async fn ping(&self, taos: &mut Self::Target) -> RawResult<()> {
        taos_query::AsyncQueryable::exec(taos, "SELECT 1")
            .await
            .map(|_| ())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> RawResult<Self::Target> {
        Ok(Taos {
            dsn: self.clone(),
            async_client: OnceCell::new(),
            async_sml: OnceCell::new(),
        })
    }

    async fn server_version(&self) -> RawResult<&str> {
        if let Some(v) = self.server_version.get() {
            Ok(v.as_str())
        } else {
            let conn = <Self as taos_query::AsyncTBuilder>::build(self).await?;
            use taos_query::prelude::AsyncQueryable;
            let v: String = AsyncQueryable::query_one(&conn, "select server_version()")
                .await?
                .unwrap();
            Ok(match self.server_version.try_insert(v) {
                Ok(v) => v.as_str(),
                Err((v, _)) => v.as_str(),
            })
        }
    }
    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        match self.addr.matches(".cloud.tdengine.com").next().is_some()
            || self.addr.matches(".cloud.taosdata.com").next().is_some()
        {
            true => return Ok(true),
            false => (),
        }

        let taos = self.build().await?;
        use taos_query::prelude::AsyncQueryable;
        let grant: Option<(String, bool)> = AsyncQueryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        )
        .await
        .unwrap_or_default();

        if let Some((edition, expired)) = grant {
            if expired {
                return Ok(false);
            }
            return match edition.trim() {
                "cloud" | "official" | "trial" => Ok(true),
                _ => Ok(false),
            };
        }

        let grant: Option<(String, (), String)> = AsyncQueryable::query_one(&taos, "show grants")
            .await
            .unwrap_or_default();

        if let Some((edition, _, expired)) = grant {
            match (edition.trim(), expired.trim()) {
                ("cloud" | "official" | "trial", "false") => Ok(true),
                _ => Ok(false),
            }
        } else {
            Ok(false)
        }
    }
}

impl TaosBuilder {
    pub fn from_dsn(dsn: impl IntoDsn) -> RawResult<Self> {
        let mut dsn = dsn.into_dsn()?;
        let scheme = match (dsn.driver.as_str(), dsn.protocol.as_deref()) {
            ("ws" | "http", _) => "ws",
            ("wss" | "https", _) => "wss",
            ("taos" | "taosws" | "tmq", Some("ws" | "http") | None) => "ws",
            ("taos" | "taosws" | "tmq", Some("wss" | "https")) => "wss",
            _ => Err(DsnError::InvalidDriver(dsn.to_string()))?,
        };
        let token = dsn.params.remove("token");

        let addr = match dsn.addresses.first() {
            Some(addr) => {
                if addr.port.is_none() && addr.host.as_deref() == Some("localhost") {
                    "localhost:6041".to_string()
                } else {
                    addr.to_string()
                }
            }
            None => "localhost:6041".to_string(),
        };

        // let timeout = dsn
        //     .params
        //     .remove("timeout")
        //     .and_then(|s| parse_duration::parse(&s).ok())
        //     .unwrap_or(Duration::from_secs(60 * 5)); // default to 5m

        if let Some(token) = token {
            Ok(TaosBuilder {
                scheme,
                addr,
                auth: WsAuth::Token(token),
                database: dsn.subject,
                server_version: OnceCell::new(),
                // timeout,
            })
        } else {
            let username = dsn.username.unwrap_or_else(|| "root".to_string());
            let password = dsn.password.unwrap_or_else(|| "taosdata".to_string());
            Ok(TaosBuilder {
                scheme,
                addr,
                auth: WsAuth::Plain(username, password),
                database: dsn.subject,
                server_version: OnceCell::new(),
                // timeout,
            })
        }
    }
    pub(crate) fn to_query_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/rest/ws?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/ws", self.scheme, self.addr),
        }
    }

    pub(crate) fn to_stmt_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/rest/stmt?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/stmt", self.scheme, self.addr),
        }
    }

    pub(crate) fn to_tmq_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/rest/tmq?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/tmq", self.scheme, self.addr),
        }
    }

    pub(crate) fn to_schemaless_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!(
                    "{}://{}/rest/schemaless?token={}",
                    self.scheme, self.addr, token
                )
            }
            WsAuth::Plain(_, _) => format!("{}://{}/rest/schemaless", self.scheme, self.addr),
        }
    }

    pub(crate) fn to_conn_request(&self) -> WsConnReq {
        match &self.auth {
            WsAuth::Token(_token) => WsConnReq {
                user: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                db: self.database.as_ref().map(Clone::clone),
            },
            WsAuth::Plain(user, pass) => WsConnReq {
                user: Some(user.to_string()),
                password: Some(pass.to_string()),
                db: self.database.as_ref().map(Clone::clone),
            },
        }
    }
}
