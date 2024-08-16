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

use query::Error as QueryError;
use query::WsConnReq;

pub mod schemaless;

pub(crate) use taos_query::block_in_place_or_global;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::{connect_async_with_config, WebSocketStream};

#[derive(Debug, Clone)]
pub enum WsAuth {
    Token(String),
    Plain(String, String),
}

#[derive(Debug, Clone, Copy)]
pub struct Retries(u32);

impl Default for Retries {
    fn default() -> Self {
        Self(5)
    }
}
#[derive(Debug, Clone)]
pub struct TaosBuilder {
    scheme: &'static str, // ws or wss
    addr: String,
    auth: WsAuth,
    database: Option<String>,
    server_version: OnceCell<String>,
    // timeout: Duration,
    conn_mode: Option<u32>,
    compression: bool,
    conn_retries: Retries,
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
        taos_query::Queryable::exec(taos, "select server_version()").map(|_| ())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> RawResult<Self::Target> {
        block_in_place_or_global(Taos::from_builder(self.clone()))
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
        taos_query::AsyncQueryable::exec(taos, "select server_version()")
            .await
            .map(|_| ())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> RawResult<Self::Target> {
        Taos::from_builder(self.clone()).await
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
        taos.exec("select server_version()").await?;

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
        taos.exec("select server_version()").await?;

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

        let conn_mode = match dsn.params.get("conn_mode") {
            Some(s) => match s.parse::<u32>() {
                Ok(num) => Some(num),
                Err(_) => Err(DsnError::InvalidDriver(dsn.to_string()))?,
            },
            None => None,
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

        let compression = dsn
            .params
            .remove("compression")
            .and_then(|s| {
                if s.trim().is_empty() {
                    Some(true)
                } else {
                    s.trim().parse::<bool>().ok()
                }
            })
            .unwrap_or(false);

        let conn_retries = dsn.remove("conn_retries").map_or_else(
            || Retries::default(),
            |s| Retries(s.parse::<u32>().unwrap_or(5)),
        );

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
                conn_mode,
                compression,
                conn_retries,
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
                conn_mode,
                compression,
                conn_retries,
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
        let mode = match self.conn_mode {
            Some(1) => Some(0), //for adapter, 0 is bi mode
            _ => None,
        };

        match &self.auth {
            WsAuth::Token(_token) => WsConnReq {
                user: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                db: self.database.as_ref().map(Clone::clone),
                mode,
            },
            WsAuth::Plain(user, pass) => WsConnReq {
                user: Some(user.to_string()),
                password: Some(pass.to_string()),
                db: self.database.as_ref().map(Clone::clone),
                mode,
            },
        }
    }

    pub(crate) async fn build_stream(
        &self,
        url: String,
    ) -> RawResult<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        self.build_stream_opt(url, true).await
    }
    pub(crate) async fn build_stream_opt(
        &self,
        url: String,
        use_global_endpoint: bool,
    ) -> RawResult<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let mut config = WebSocketConfig::default();
        config.max_frame_size = None;
        config.max_message_size = None;
        if self.compression {
            cfg_if::cfg_if! {
                if #[cfg(feature = "deflate")] {
                    tracing::debug!(url, "Enable compression");
                    config.compression = Some(Default::default());
                } else {
                    tracing::warn!("WebSocket compression is not supported unless with `deflate` feature");
                }
            }
        }

        let mut retries = 0;
        let ws_url = self.to_ws_url();
        loop {
            match connect_async_with_config(
                if use_global_endpoint {
                    ws_url.as_str()
                } else {
                    url.as_str()
                },
                Some(config),
                false,
            )
            .await
            {
                Ok((ws, _)) => {
                    return Ok(ws);
                }
                Err(err) => {
                    let err_string = err.to_string();
                    if err_string.contains("401 Unauthorized") {
                        return Err(QueryError::Unauthorized(self.to_ws_url()).into());
                    } else if err.to_string().contains("404 Not Found")
                        || err.to_string().contains("400")
                    {
                        if !use_global_endpoint {
                            return Err(QueryError::from(err).into());
                        }
                        match connect_async_with_config(&url, Some(config), false).await {
                            Ok((ws, _)) => return Ok(ws),
                            Err(err) => {
                                let err_string = err.to_string();
                                if err_string.contains("401 Unauthorized") {
                                    return Err(QueryError::Unauthorized(url).into());
                                }
                            }
                        }
                    }

                    if retries >= self.conn_retries.0 {
                        return Err(QueryError::from(err).into());
                    }
                    retries += 1;
                    tracing::warn!(
                        "Failed to connect to {}, retrying...({})",
                        self.to_ws_url(),
                        retries
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(retries as u64 * 500)).await;
                }
            }
        }
    }
}

#[cfg(feature = "rustls")]
#[cfg(test)]
mod lib_tests {

    use crate::{
        query::infra::{ToMessage, WsRecv, WsSend},
        *,
    };
    use futures::{SinkExt, StreamExt};
    use std::time::Duration;
    use tracing::*;
    use tracing_subscriber::util::SubscriberInitExt;

    #[cfg(feature = "rustls")]
    #[tokio::test]
    async fn test_build_stream() -> Result<(), anyhow::Error> {
        let _subscriber = tracing_subscriber::fmt::fmt()
            .with_max_level(Level::INFO)
            .with_file(true)
            .with_line_number(true)
            .finish();
        let _ = _subscriber.try_init();

        let dsn = std::env::var("TEST_CLOUD_DSN").unwrap_or("http://localhost:6041".to_string());
        let builder = TaosBuilder::from_dsn(dsn).unwrap();
        let url = builder.to_query_url();
        info!("url: {}", url);
        let ws = builder.build_stream(url).await.unwrap();
        trace!("ws: {:?}", ws);

        let (mut sender, mut reader) = ws.split();

        let version = WsSend::Version;
        sender.send(version.to_msg()).await?;

        let _handle = tokio::spawn(async move {
            loop {
                if let Some(Ok(msg)) = reader.next().await {
                    let text = msg.to_text().unwrap();
                    let recv: WsRecv = serde_json::from_str(text).unwrap();
                    info!("recv: {:?}", recv);
                    assert_eq!(recv.code, 0);
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        Ok(())
    }
}
