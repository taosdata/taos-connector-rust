#![recursion_limit = "256"]
use std::fmt::{Debug, Display};

use once_cell::sync::OnceCell;

use taos_query::prelude::Code;
use taos_query::{DsnError, IntoDsn, TBuilder};

mod stmt;
pub use stmt::Stmt;

// pub mod tmq;
pub mod consumer;
pub use consumer::{Consumer, TmqBuilder};

pub mod query;
pub use query::ResultSet;
pub use query::Taos;

use query::WsConnReq;

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
            code: Code::Failed,
            source: err.into(),
        }
    }
}

impl TBuilder for TaosBuilder {
    type Target = Taos;

    type Error = Error;

    fn available_params() -> &'static [&'static str] {
        &["token"]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        Ok(Self::from_dsn(dsn.into_dsn()?)?)
    }

    fn client_version() -> &'static str {
        "0"
    }
    fn ping(&self, taos: &mut Self::Target) -> Result<(), Self::Error> {
        taos_query::Queryable::exec(taos, "SELECT 1")
            .map_err(|e| Error {
                code: e.errno(),
                source: e.into(),
            })
            .map(|_| ())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> Result<Self::Target, Self::Error> {
        Ok(Taos {
            dsn: self.clone(),
            async_client: OnceCell::new(),
        })
    }
}

impl TaosBuilder {
    pub fn from_dsn(dsn: impl IntoDsn) -> Result<Self, DsnError> {
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
