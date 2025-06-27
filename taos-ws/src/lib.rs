#![recursion_limit = "256"]

use futures::StreamExt;
use futures_util::SinkExt;
use once_cell::sync::OnceCell;
use rand::Rng;
use std::cmp;
use std::fmt::{Debug, Display};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use taos_query::prelude::Code;
use taos_query::util::{generate_req_id, Edition};
use taos_query::{DsnError, IntoDsn, RawError, RawResult};
use tokio::time;
use tokio_tungstenite::tungstenite::extensions::DeflateConfig;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;

pub mod stmt;
pub use stmt::Stmt;

pub mod stmt2;
pub use stmt2::Stmt2;

pub mod consumer;
pub use consumer::{Consumer, Offset, TmqBuilder};

pub mod query;
use query::{Error as QueryError, WsConnReq};
pub use query::{ResultSet, Taos};
pub(crate) use taos_query::block_in_place_or_global;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{connect_async_with_config, MaybeTlsStream, WebSocketStream};

use crate::query::asyn::WS_ERROR_NO;
use crate::query::messages::{ToMessage, WsRecv, WsRecvData, WsSend};

type Version = String;

#[derive(Debug, Clone)]
pub enum WsAuth {
    Token(String),
    Plain(String, String),
}

const DEFAULT_RETRY_RETRIES: u32 = 5;
const DEFAULT_RETRY_BACKOFF_MS: u64 = 200;
const DEFAULT_RETRY_BACKOFF_MAX_MS: u64 = 2000;

#[derive(Debug, Clone)]
struct RetryPolicy {
    retries: u32,
    backoff_ms: u64,
    backoff_max_ms: u64,
}

#[derive(Debug, Clone)]
pub struct TaosBuilder {
    https: Arc<AtomicBool>,
    addrs: Vec<String>,
    current_addr_index: Arc<AtomicUsize>,
    auth: WsAuth,
    database: Option<String>,
    server_version: OnceCell<String>,
    conn_mode: Option<u32>,
    compression: bool,
    retry_policy: RetryPolicy,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum UrlKind {
    Ws,
    Stmt,
    Tmq,
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
                Ok(v) | Err((v, _)) => v.as_str(),
            })
        }
    }

    fn is_enterprise_edition(&self) -> RawResult<bool> {
        let addr = self.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
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
                tracing::warn!(
                    "Can't check enterprise edition with either \"show cluster\" or \"show grants\""
                );
                Edition::new("unknown", true)
            }
        };
        Ok(edition.is_enterprise_edition())
    }

    fn get_edition(&self) -> RawResult<Edition> {
        let addr = self.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
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
                tracing::warn!(
                    "Can't check enterprise edition with either \"show cluster\" or \"show grants\""
                );
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
                Ok(v) | Err((v, _)) => v.as_str(),
            })
        }
    }

    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        use taos_query::prelude::AsyncQueryable;

        let taos = self.build().await?;
        // Ensure server is ready
        taos.exec("select server_version()").await?;

        let addr = self.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
        {
            return Ok(true);
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
                    // Valid choices: false/unlimited, otherwise expired.
                    !(expired.trim() == "false" || expired.trim() == "unlimited"),
                )
            } else {
                tracing::warn!(
                    "Can't check enterprise edition with either \"show cluster\" or \"show grants\""
                );
                Edition::new("unknown", true)
            }
        };

        Ok(edition.is_enterprise_edition())
    }

    async fn get_edition(&self) -> RawResult<Edition> {
        use taos_query::prelude::AsyncQueryable;

        let taos = self.build().await?;
        // Ensure server is ready
        taos.exec("select server_version()").await?;

        let addr = self.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
        {
            let edition = Edition::new("cloud", false);
            return Ok(edition);
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
                    // Valid choices: false/unlimited, otherwise expired.
                    !(expired.trim() == "false" || expired.trim() == "unlimited"),
                )
            } else {
                tracing::warn!(
                    "Can't check enterprise edition with either \"show cluster\" or \"show grants\""
                );
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

impl TaosBuilder {
    fn scheme(&self) -> &'static str {
        if self.https.load(Ordering::SeqCst) {
            "wss"
        } else {
            "ws"
        }
    }

    fn set_https(&self, https: bool) {
        self.https.store(https, Ordering::SeqCst);
    }

    pub fn from_dsn<T: IntoDsn>(dsn: T) -> RawResult<Self> {
        let mut dsn = dsn.into_dsn()?;

        let https = match (dsn.driver.as_str(), dsn.protocol.as_deref()) {
            ("ws" | "http", _) | ("taos" | "taosws" | "tmq", Some("ws" | "http") | None) => false,
            ("wss" | "https", _) | ("taos" | "taosws" | "tmq", Some("wss" | "https")) => true,
            _ => Err(DsnError::InvalidDriver(dsn.to_string()))?,
        };

        let https = Arc::new(AtomicBool::new(https));

        let conn_mode = match dsn.params.get("conn_mode") {
            Some(s) => match s.parse::<u32>() {
                Ok(num) => Some(num),
                Err(_) => Err(DsnError::InvalidDriver(dsn.to_string()))?,
            },
            None => None,
        };

        let token = dsn.params.remove("token");

        let mut addrs = Vec::with_capacity(dsn.addresses.len());
        for addr in &dsn.addresses {
            let addr = if addr.host.as_deref() == Some("localhost") && addr.port.is_none() {
                "localhost:6041".to_string()
            } else {
                addr.to_string()
            };
            addrs.push(addr);
        }

        if addrs.is_empty() {
            addrs.push("localhost:6041".to_string());
        }

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

        let retries = dsn
            .remove("conn_retries")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(DEFAULT_RETRY_RETRIES);

        let backoff_ms = dsn
            .remove("retry_backoff_ms")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_RETRY_BACKOFF_MS);

        let backoff_max_ms = dsn
            .remove("retry_backoff_max_ms")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_RETRY_BACKOFF_MAX_MS);

        let retry_policy = RetryPolicy {
            retries,
            backoff_ms,
            backoff_max_ms,
        };

        if let Some(token) = token {
            Ok(TaosBuilder {
                https,
                addrs,
                auth: WsAuth::Token(token),
                database: dsn.subject,
                server_version: OnceCell::new(),
                conn_mode,
                compression,
                retry_policy,
                current_addr_index: Arc::new(AtomicUsize::new(0)),
            })
        } else {
            let username = dsn.username.unwrap_or_else(|| "root".to_string());
            let password = dsn.password.unwrap_or_else(|| "taosdata".to_string());
            Ok(TaosBuilder {
                https,
                addrs,
                auth: WsAuth::Plain(username, password),
                database: dsn.subject,
                server_version: OnceCell::new(),
                conn_mode,
                compression,
                retry_policy,
                current_addr_index: Arc::new(AtomicUsize::new(0)),
            })
        }
    }

    async fn send_version_request(
        &self,
        ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> RawResult<Version> {
        let timeout = Duration::from_secs(8);

        time::timeout(timeout, ws_stream.send(WsSend::Version.to_msg()))
            .await
            .map_err(|_| {
                RawError::from_code(WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code())
                    .context("timeout sending version request")
            })?
            .map_err(handle_disconnect_error)?;

        let version_fut = async {
            let max_non_version_cnt = 5;
            let mut non_version_cnt = 0;
            loop {
                if let Some(res) = ws_stream.next().await {
                    let message = res.map_err(handle_disconnect_error)?;
                    tracing::trace!("send_version_request, received message: {message}");
                    match message {
                        Message::Text(text) => {
                            let resp: WsRecv = serde_json::from_str(&text).map_err(|e| {
                                RawError::any(e)
                                    .with_code(WS_ERROR_NO::DE_ERROR.as_code())
                                    .context("invalid json response")
                            })?;
                            let (_, data, ok) = resp.ok();
                            ok?;
                            match data {
                                WsRecvData::Version { version } => {
                                    return Ok(version);
                                }
                                _ => return Ok("2.x".to_string()),
                            }
                        }
                        Message::Ping(bytes) => {
                            ws_stream
                                .send(Message::Pong(bytes))
                                .await
                                .map_err(handle_disconnect_error)?;

                            non_version_cnt += 1;
                            if non_version_cnt >= max_non_version_cnt {
                                return Ok("2.x".to_string());
                            }
                        }
                        _ => return Ok("2.x".to_string()),
                    }
                } else {
                    return Err(RawError::from_code(
                        WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code(),
                    ));
                }
            }
        };

        let version = match time::timeout(timeout, version_fut).await {
            Ok(Ok(ver)) => ver,
            Ok(Err(err)) => return Err(err),
            Err(_) => "2.x".to_string(),
        };

        tracing::trace!("send_version_request, server version: {version}");

        Ok(version)
    }

    async fn send_conn_request(
        &self,
        ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> RawResult<()> {
        let req = WsSend::Conn {
            req_id: generate_req_id(),
            req: self.build_conn_request(),
        };

        let timeout = Duration::from_secs(8);

        time::timeout(timeout, ws_stream.send(req.to_msg()))
            .await
            .map_err(|_| {
                RawError::from_code(WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code())
                    .context("timeout sending conn request")
            })?
            .map_err(handle_disconnect_error)?;

        loop {
            let res = time::timeout(timeout, ws_stream.next())
                .await
                .map_err(|_| {
                    RawError::from_code(WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code())
                        .context("timeout waiting for conn response")
                })?;

            let Some(res) = res else {
                return Err(RawError::from_code(Code::WS_DISCONNECTED));
            };

            let message = res.map_err(handle_disconnect_error)?;
            tracing::trace!("send_conn_request, received message: {message}");

            match message {
                Message::Text(text) => {
                    let resp: WsRecv = serde_json::from_str(&text).map_err(|e| {
                        RawError::any(e)
                            .with_code(WS_ERROR_NO::DE_ERROR.as_code())
                            .context("invalid json response")
                    })?;
                    let (_, data, ok) = resp.ok();
                    ok?;
                    match data {
                        WsRecvData::Conn => return Ok(()),
                        WsRecvData::Version { .. } => {}
                        _ => {
                            return Err(RawError::from_string(format!(
                                "unexpected conn response: {data:?}"
                            )))
                        }
                    }
                }
                Message::Ping(bytes) => {
                    ws_stream
                        .send(Message::Pong(bytes))
                        .await
                        .map_err(handle_disconnect_error)?;
                }
                _ => {
                    return Err(RawError::from_string(format!(
                        "unexpected message during conn: {message:?}"
                    )))
                }
            }
        }
    }

    pub(crate) fn build_conn_request(&self) -> WsConnReq {
        let mode = match self.conn_mode {
            Some(1) => Some(0), // for adapter, 0 is bi mode
            _ => None,
        };

        match &self.auth {
            WsAuth::Token(_) => WsConnReq {
                user: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                db: self.database.clone(),
                mode,
            },
            WsAuth::Plain(user, pass) => WsConnReq {
                user: Some(user.to_string()),
                password: Some(pass.to_string()),
                db: self.database.clone(),
                mode,
            },
        }
    }

    #[inline]
    pub(crate) fn to_url(&self, kind: UrlKind) -> String {
        match kind {
            UrlKind::Tmq => self.to_tmq_url(),
            _ => self.to_ws_url(),
        }
    }

    #[inline]
    pub(crate) fn to_ws_url(&self) -> String {
        self.format_url("ws")
    }

    #[inline]
    pub(crate) fn to_query_url(&self) -> String {
        self.format_url("rest/ws")
    }

    #[inline]
    pub(crate) fn to_stmt_url(&self) -> String {
        self.format_url("rest/stmt")
    }

    #[inline]
    pub(crate) fn to_tmq_url(&self) -> String {
        self.format_url("rest/tmq")
    }

    fn format_url(&self, path: &str) -> String {
        let addr = self.active_addr();
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/{}?token={}", self.scheme(), addr, path, token)
            }
            WsAuth::Plain(_, _) => {
                format!("{}://{}/{}", self.scheme(), addr, path)
            }
        }
    }

    fn active_addr(&self) -> &String {
        let cur_addr_idx = self.current_addr_index.load(Ordering::Relaxed);
        &self.addrs[cur_addr_idx]
    }

    pub(crate) async fn connect(
        &self,
        kind: UrlKind,
    ) -> RawResult<(WebSocketStream<MaybeTlsStream<TcpStream>>, Version)> {
        let mut config = WebSocketConfig::default();
        config.max_frame_size = None;
        config.max_message_size = None;
        if self.compression {
            cfg_if::cfg_if! {
                if #[cfg(feature = "deflate")] {
                    tracing::trace!("WebSocket compression enabled");
                    config.compression = Some(DeflateConfig::default());
                } else {
                    tracing::warn!("WebSocket compression is not supported, missing `deflate` feature");
                }
            }
        }

        for _ in 0..self.addrs.len() {
            let mut url = self.to_url(kind);
            tracing::trace!("connecting to TDengine WebSocket server, url: {url}");

            for i in 0..self.retry_policy.retries {
                match connect_async_with_config(&url, Some(config), false).await {
                    Ok((mut ws_stream, _)) => {
                        let version_res = self.send_version_request(&mut ws_stream).await;
                        if let Err(err) = &version_res {
                            tracing::warn!("failed to send version request: {err}");
                            if err.code() != WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code() {
                                break;
                            }
                        }

                        if let Err(err) = self.send_conn_request(&mut ws_stream).await {
                            tracing::warn!("failed to send conn request: {err}");
                            if err.code() != WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code() {
                                break;
                            }
                        }

                        return Ok((ws_stream, version_res.unwrap()));
                    }
                    Err(e) => {
                        let errstr = e.to_string();
                        tracing::warn!("failed to connect to {url}, err: {errstr}");
                        if errstr.contains("307") {
                            self.set_https(true);
                            url = url.replace("ws://", "wss://");
                            continue;
                        } else if errstr.contains("400") || errstr.contains("404 Not Found") {
                            url = match kind {
                                UrlKind::Ws => self.to_query_url(),
                                UrlKind::Stmt => self.to_stmt_url(),
                                UrlKind::Tmq => self.to_tmq_url(),
                            };
                            continue;
                        } else if errstr.contains("401 Unauthorized") {
                            break;
                        }
                    }
                }

                tracing::warn!("failed to connect to {}, retrying...({})", url, i + 1);

                let base_delay = cmp::min(
                    self.retry_policy.backoff_max_ms,
                    self.retry_policy.backoff_ms * 2u64.saturating_pow(i),
                );

                let jitter = 1.0 + rand::rng().random_range(-0.2..=0.2);
                let delay = (base_delay as f64 * jitter) as u64;

                tokio::time::sleep(Duration::from_millis(delay)).await;
            }

            let cur_idx = self.current_addr_index.load(Ordering::Relaxed);
            let next_idx = (cur_idx + 1) % self.addrs.len();
            self.current_addr_index.store(next_idx, Ordering::Relaxed);
        }

        tracing::error!("failed to connect to all addresses: {:?}", self.addrs);

        Err(RawError::from_code(WS_ERROR_NO::WEBSOCKET_ERROR.as_code())
            .context("failed to connect to all addresses"))
    }

    pub(crate) async fn build_stream(
        &self,
        url: String,
    ) -> RawResult<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        self.build_stream_opt(url, true).await
    }

    pub(crate) async fn build_stream_opt(
        &self,
        mut url: String,
        use_global_endpoint: bool,
    ) -> RawResult<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let mut config = WebSocketConfig::default();
        config.max_frame_size = None;
        config.max_message_size = None;
        if self.compression {
            cfg_if::cfg_if! {
                if #[cfg(feature = "deflate")] {
                    tracing::debug!(url, "Enable compression");
                    config.compression = Some(DeflateConfig::default());
                } else {
                    tracing::warn!("WebSocket compression is not supported unless with `deflate` feature");
                }
            }
        }

        let mut retries = 0;
        let mut ws_url = self.to_ws_url();
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
                    if err_string.contains("307") {
                        tracing::debug!(error = err_string, "Redirecting to HTTPS link");
                        self.set_https(true);
                        url = url.replace("ws://", "wss://");
                        ws_url = self.to_ws_url();
                    } else if err_string.contains("401 Unauthorized") {
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

                    if retries >= self.retry_policy.retries {
                        return Err(QueryError::from(err).into());
                    }
                    retries += 1;
                    tracing::warn!(
                        "Failed to connect to {}, retrying...({})",
                        self.to_ws_url(),
                        retries
                    );

                    // every retry, wait more 500ms, max wait time 30s
                    let wait_millis = std::cmp::min(retries as u64 * 500, 30000);
                    tokio::time::sleep(std::time::Duration::from_millis(wait_millis)).await;
                }
            }
        }
    }
}

#[inline]
fn handle_disconnect_error(err: WsError) -> RawError {
    match err {
        WsError::ConnectionClosed
        | WsError::AlreadyClosed
        | WsError::Io(_)
        | WsError::Tls(_)
        | WsError::Protocol(_) => {
            RawError::from_code(WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code())
                .context("WebSocket connection disconnected")
        }
        _ => RawError::any(err).context("WebSocket error"),
    }
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt};
    use tracing::*;

    use crate::query::messages::{ToMessage, WsRecv, WsRecvData, WsSend};
    use crate::*;

    #[tokio::test]
    async fn test_build_stream() -> Result<(), anyhow::Error> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let dsn = "ws://localhost:6041";
        let builder = TaosBuilder::from_dsn(dsn)?;
        let url = builder.to_ws_url();

        let ws = builder.build_stream(url).await?;
        let (mut sender, mut reader) = ws.split();

        let version = WsSend::Version;
        sender.send(version.to_msg()).await?;

        loop {
            if let Some(Ok(msg)) = reader.next().await {
                let text = msg.to_text().unwrap();
                let recv: WsRecv = serde_json::from_str(text).unwrap();
                assert_eq!(recv.code, 0);
                if let WsRecvData::Version { version, .. } = recv.data {
                    info!("server version: {}", version);
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(feature = "rustls-aws-lc-crypto-provider")]
#[cfg(test)]
mod cloud_tests {
    use futures::{SinkExt, StreamExt};
    use tracing::*;

    use crate::query::messages::{ToMessage, WsRecv, WsRecvData, WsSend};
    use crate::*;

    #[tokio::test]
    async fn test_build_stream() -> Result<(), anyhow::Error> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_put_line_cloud");
            return Ok(());
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_put_line_cloud");
            return Ok(());
        }

        let dsn = format!("{}/rust_test?token={}", url.unwrap(), token.unwrap());

        let builder = TaosBuilder::from_dsn(dsn)?;
        let url = builder.to_query_url();

        let ws = builder.build_stream(url).await?;
        let (mut sender, mut reader) = ws.split();

        let version = WsSend::Version;
        sender.send(version.to_msg()).await?;

        loop {
            if let Some(Ok(msg)) = reader.next().await {
                let text = msg.to_text().unwrap();
                let recv: WsRecv = serde_json::from_str(text).unwrap();
                assert_eq!(recv.code, 0);
                if let WsRecvData::Version { version, .. } = recv.data {
                    info!("server version: {}", version);
                    break;
                }
            }
        }

        Ok(())
    }
}
