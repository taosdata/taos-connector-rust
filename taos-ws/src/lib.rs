#![recursion_limit = "256"]
use std::fmt::{Debug, Display};

use log::warn;
use once_cell::sync::OnceCell;

use taos_query::prelude::Code;
use taos_query::util::Edition;
use taos_query::{DsnError, IntoDsn, RawResult};

pub mod stmt;
pub use stmt::Stmt;

// pub mod tmq;
pub mod consumer;
pub use consumer::{Consumer, Offset, TmqBuilder};

pub mod query;
pub use query::ResultSet;
pub use query::Taos;

use query::WsConnReq;
use query::Error as QueryError;

pub mod schemaless;

pub(crate) use taos_query::block_in_place_or_global;
use tokio_tungstenite::{WebSocketStream, connect_async_with_config};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::MaybeTlsStream;
use tokio::net::TcpStream;

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
    conn_mode: Option<u32>
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
        taos_query::Queryable::exec(taos, "select server_status()").map(|_| ())
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
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(&taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition.is_enterprise_edition())
    }

    fn get_edition(&self) -> RawResult<Edition> {
        if self.addr.matches(".cloud.tdengine.com").next().is_some()
            || self.addr.matches(".cloud.taosdata.com").next().is_some()
        {
            let edition = Edition::new("cloud", false);
            return Ok(edition);
        }

        let taos = self.build()?;

        use taos_query::prelude::sync::Queryable;
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(&taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
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
        taos_query::AsyncQueryable::exec(taos, "select server_status()")
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
        use taos_query::prelude::AsyncQueryable;

        let taos = self.build().await?;
        // Ensue server is ready.
        taos.exec("select server_status()").await?;

        match self.addr.matches(".cloud.tdengine.com").next().is_some()
            || self.addr.matches(".cloud.taosdata.com").next().is_some()
        {
            true => return Ok(true),
            false => (),
        }

        let grant: RawResult<Option<(String, bool)>> = AsyncQueryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        )
        .await;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                AsyncQueryable::query_one(&taos, "show grants").await;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition.is_enterprise_edition())
    }

    async fn get_edition(&self) -> RawResult<Edition> {
        use taos_query::prelude::AsyncQueryable;

        let taos = self.build().await?;
        // Ensure server is ready.
        taos.exec("select server_status()").await?;

        match self.addr.matches(".cloud.tdengine.com").next().is_some()
            || self.addr.matches(".cloud.taosdata.com").next().is_some()
        {
            true => {
                let edition = Edition::new("cloud", false);
                return Ok(edition);
            }
            false => (),
        }

        let grant: RawResult<Option<(String, bool)>> = AsyncQueryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        )
        .await;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                AsyncQueryable::query_one(&taos, "show grants").await;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
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

        let conn_mode =  match dsn.params.get("conn_mode") {
            Some(s) => match s.parse::<u32>() {
                Ok(num) => Some(num),
                Err(_) => Err(DsnError::InvalidDriver(dsn.to_string()))?,
            }
            None => None
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
                conn_mode
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
                conn_mode
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

    pub(crate) fn to_ws_url(&self) -> String {
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/ws?token={}", self.scheme, self.addr, token)
            }
            WsAuth::Plain(_, _) => format!("{}://{}/ws", self.scheme, self.addr),
        }
    }

    pub(crate) fn to_conn_request(&self) -> WsConnReq {
        let mode = match self.conn_mode{
            Some(1) => Some(0), //for adapter, 0 is bi mode
            _ => None,
        };

        match &self.auth {
            WsAuth::Token(_token) => WsConnReq {
                user: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                db: self.database.as_ref().map(Clone::clone),
                mode
            },
            WsAuth::Plain(user, pass) => WsConnReq {
                user: Some(user.to_string()),
                password: Some(pass.to_string()),
                db: self.database.as_ref().map(Clone::clone),
                mode
            },
        }
    }

    pub(crate) async fn build_stream(&self, url: String) -> RawResult<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let mut config = WebSocketConfig::default();
        config.max_frame_size = None;

        let res = connect_async_with_config(self.to_ws_url(), Some(config), false)
            .await
            .map_err(|err| {
                let err_string = err.to_string();
                if err_string.contains("401 Unauthorized") {
                    QueryError::Unauthorized(self.to_ws_url())
                } else {
                    err.into()
                }
            });
            
        let (ws, _) = match res {
            Ok(res) => res,
            Err(err) => {
                if err.to_string().contains("404 Not Found") || err.to_string().contains("400") {
                    connect_async_with_config(&url, Some(config), false)
                        .await
                        .map_err(|err| {
                            let err_string = err.to_string();
                            if err_string.contains("401 Unauthorized") {
                                QueryError::Unauthorized(url)
                            } else {
                                err.into()
                            }
                        })?
                } else {
                    return Err(err.into());
                }
            }
        };
        Ok(ws)
    }
}
