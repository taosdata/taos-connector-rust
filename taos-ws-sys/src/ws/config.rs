use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::str::FromStr;
use std::sync::{OnceLock, RwLock};

use faststr::FastStr;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use taos_error::Code;
use tracing::level_filters::LevelFilter;

use super::error::TaosError;

static CONFIG: RwLock<Config> = RwLock::new(Config::new());

pub fn init() -> Result<(), String> {
    static ONCE: OnceLock<Result<(), String>> = OnceLock::new();

    const DEFAULT_CONFIG: &str = if cfg!(windows) {
        "C:\\TDengine\\cfg\\taos.cfg"
    } else {
        "/etc/taos/taos.cfg"
    };

    ONCE.get_or_init(|| {
        let mut config = CONFIG.write().unwrap();

        if let Ok(s) = std::env::var("TAOS_COMPRESSION") {
            if let Ok(compression) = s.parse() {
                config.set_compression(compression);
            }
        }

        if let Ok(s) = std::env::var("TAOS_LOG_DIR") {
            config.set_log_dir(s);
        }

        if let Ok(s) = std::env::var("RUST_LOG") {
            if let Ok(level) = LevelFilter::from_str(&s) {
                config.set_log_level(level);
            }
        }

        if let Ok(s) = std::env::var("TAOS_DEBUG_FLAG") {
            config.set_debug_flag(&s);
        }

        if let Ok(s) = std::env::var("TAOS_LOG_OUTPUT_TO_SCREEN") {
            config.set_log_output_to_screen(s == "1");
        }

        if let Ok(s) = std::env::var("TAOS_TIMEZONE") {
            config.set_timezone(s);
        }

        if let Ok(s) = std::env::var("TAOS_FQDN") {
            config.set_fqdn(s);
        }

        if let Ok(s) = std::env::var("TAOS_SERVER_PORT") {
            if let Ok(port) = s.parse() {
                config.set_server_port(port);
            }
        }

        if let Ok(s) = std::env::var("TAOS_ADAPTER_LIST") {
            config.set_adapter_list(s);
        }

        if let Ok(s) = std::env::var("TAOS_CONN_RETRIES") {
            if let Ok(retries) = s.parse() {
                config.set_conn_retries(retries);
            }
        }

        if let Ok(s) = std::env::var("TAOS_RETRY_BACKOFF_MS") {
            if let Ok(backoff_ms) = s.parse() {
                config.set_retry_backoff_ms(backoff_ms);
            }
        }

        if let Ok(s) = std::env::var("TAOS_RETRY_BACKOFF_MAX_MS") {
            if let Ok(backoff_max_ms) = s.parse() {
                config.set_retry_backoff_max_ms(backoff_max_ms);
            }
        }

        if let Ok(s) = std::env::var("TAOS_LOG_KEEP_DAYS") {
            if let Ok(days) = s.parse() {
                config.set_log_keep_days(days);
            }
        }

        if let Ok(s) = std::env::var("TAOS_ROTATION_SIZE") {
            config.set_rotation_size(s);
        }

        let cfg_dir = if let Some(dir) = &config.config_dir {
            dir.to_string()
        } else if let Ok(dir) = std::env::var("TAOS_CONFIG_DIR") {
            if Path::new(&dir).exists() {
                dir
            } else if Path::new(DEFAULT_CONFIG).exists() {
                DEFAULT_CONFIG.to_string()
            } else {
                eprintln!("TAOS_CONFIG_DIR not found: {dir}");
                return Ok(());
            }
        } else if Path::new(DEFAULT_CONFIG).exists() {
            DEFAULT_CONFIG.to_string()
        } else {
            return Ok(());
        };

        if let Err(err) = config.load_from_path(&cfg_dir) {
            return Err(err.to_string());
        }

        Ok(())
    })
    .clone()
}

pub fn config() -> Config {
    CONFIG.read().unwrap().clone()
}

#[allow(dead_code)]
pub fn config_dir() -> FastStr {
    CONFIG.read().unwrap().config_dir().clone()
}

pub fn compression() -> bool {
    CONFIG.read().unwrap().compression()
}

pub fn log_dir() -> FastStr {
    CONFIG.read().unwrap().log_dir().clone()
}

pub fn log_level() -> LevelFilter {
    CONFIG.read().unwrap().log_level()
}

pub fn log_output_to_screen() -> bool {
    CONFIG.read().unwrap().log_output_to_screen()
}

pub fn timezone() -> FastStr {
    CONFIG.read().unwrap().timezone().clone()
}

pub fn fqdn() -> Option<FastStr> {
    CONFIG.read().unwrap().fqdn().cloned()
}

pub fn server_port() -> u16 {
    CONFIG.read().unwrap().server_port()
}

pub fn adapter_list() -> Option<FastStr> {
    CONFIG.read().unwrap().adapter_list().cloned()
}

pub fn conn_retries() -> u32 {
    CONFIG.read().unwrap().conn_retries()
}

pub fn retry_backoff_ms() -> u64 {
    CONFIG.read().unwrap().retry_backoff_ms()
}

pub fn retry_backoff_max_ms() -> u64 {
    CONFIG.read().unwrap().retry_backoff_max_ms()
}

pub fn log_keep_days() -> u16 {
    CONFIG.read().unwrap().log_keep_days()
}

pub fn rotation_size() -> FastStr {
    CONFIG.read().unwrap().rotation_size().clone()
}

pub fn tls_mode() -> WsTlsMode {
    CONFIG.read().unwrap().tls_mode()
}

pub fn tls_versions() -> Vec<WsTlsVersion> {
    CONFIG.read().unwrap().tls_versions().to_vec()
}

pub fn tls_certs() -> Option<Vec<CertificateDer<'static>>> {
    CONFIG.read().unwrap().tls_certs().map(|v| v.to_vec())
}

pub fn set_config_dir<T: Into<FastStr>>(cfg_dir: T) {
    CONFIG.write().unwrap().set_config_dir(cfg_dir);
}

pub fn set_timezone<T: Into<FastStr>>(timezone: T) {
    CONFIG.write().unwrap().set_timezone(timezone);
}

pub fn print() {
    CONFIG.read().unwrap().print();
}

const DEFAULT_CONFIG_DIR: &str = if cfg!(windows) {
    "C:\\TDengine\\cfg"
} else {
    "/etc/taos"
};

const DEFAULT_LOG_DIR: &str = if cfg!(windows) {
    "C:\\TDengine\\log"
} else {
    "/var/log/taos"
};

const DEFAULT_COMPRESSION: bool = false;
const DEFAULT_LOG_LEVEL: LevelFilter = LevelFilter::WARN;
const DEFAULT_LOG_OUTPUT_TO_SCREEN: bool = false;
const DEFAULT_TIMEZONE: &str = "Asia/Shanghai";
const DEFAULT_SERVER_PORT: u16 = 0;
const DEFAULT_CONN_RETRIES: u32 = 5;
const DEFAULT_RETRY_BACKOFF_MS: u64 = 200;
const DEFAULT_RETRY_BACKOFF_MAX_MS: u64 = 2000;
const DEFAULT_LOG_KEEP_DAYS: u16 = 30;
const DEFAULT_ROTATION_SIZE: &str = "1GB";
const DEFAULT_DEBUG_FLAG: u16 = 0;
const DEFAULT_TLS_MODE: WsTlsMode = WsTlsMode::Disabled;
const DEFAULT_TLS_VERSIONS: &[WsTlsVersion] = &[WsTlsVersion::TlsV1_3];

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsTlsMode {
    Disabled,
    Required,
    VerifyCa,
    VerifyIdentity,
}

impl From<WsTlsMode> for i32 {
    fn from(mode: WsTlsMode) -> Self {
        mode as i32
    }
}

impl std::str::FromStr for WsTlsMode {
    type Err = TaosError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "0" => Ok(WsTlsMode::Disabled),
            "1" => Ok(WsTlsMode::Required),
            "2" => Ok(WsTlsMode::VerifyCa),
            "3" => Ok(WsTlsMode::VerifyIdentity),
            _ => Err(TaosError::new(
                Code::INVALID_PARA,
                &format!("invalid value for wsTlsMode: {s}"),
            )),
        }
    }
}

impl std::fmt::Display for WsTlsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as i32)
    }
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsTlsVersion {
    TlsV1_2,
    TlsV1_3,
}

impl std::str::FromStr for WsTlsVersion {
    type Err = TaosError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "TLSv1.2" => Ok(WsTlsVersion::TlsV1_2),
            "TLSv1.3" => Ok(WsTlsVersion::TlsV1_3),
            _ => Err(TaosError::new(
                Code::INVALID_PARA,
                &format!("invalid value for wsTlsVersion: {s}"),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    config_dir: Option<FastStr>,
    compression: Option<bool>,
    log_dir: Option<FastStr>,
    log_level: Option<LevelFilter>,
    log_output_to_screen: Option<bool>,
    timezone: Option<FastStr>,
    fqdn: Option<FastStr>,
    server_port: Option<u16>,
    adapter_list: Option<FastStr>,
    conn_retries: Option<u32>,
    retry_backoff_ms: Option<u64>,
    retry_backoff_max_ms: Option<u64>,
    log_keep_days: Option<u16>,
    rotation_size: Option<FastStr>,
    tls_mode: Option<WsTlsMode>,
    tls_versions: Option<Vec<WsTlsVersion>>,
    tls_certs: Option<Vec<CertificateDer<'static>>>,
}

impl Config {
    const fn new() -> Config {
        Self {
            config_dir: None,
            compression: None,
            log_dir: None,
            log_level: None,
            log_output_to_screen: None,
            timezone: None,
            fqdn: None,
            server_port: None,
            adapter_list: None,
            conn_retries: None,
            retry_backoff_ms: None,
            retry_backoff_max_ms: None,
            log_keep_days: None,
            rotation_size: None,
            tls_mode: None,
            tls_versions: None,
            tls_certs: None,
        }
    }

    #[allow(dead_code)]
    fn config_dir(&self) -> &FastStr {
        static CONFIG_DIR: FastStr = FastStr::from_static_str(DEFAULT_CONFIG_DIR);
        self.config_dir.as_ref().unwrap_or(&CONFIG_DIR)
    }

    fn compression(&self) -> bool {
        self.compression.unwrap_or(DEFAULT_COMPRESSION)
    }

    fn log_dir(&self) -> &FastStr {
        static LOG_DIR: FastStr = FastStr::from_static_str(DEFAULT_LOG_DIR);
        self.log_dir.as_ref().unwrap_or(&LOG_DIR)
    }

    fn log_level(&self) -> LevelFilter {
        self.log_level.unwrap_or(DEFAULT_LOG_LEVEL)
    }

    fn log_output_to_screen(&self) -> bool {
        self.log_output_to_screen
            .unwrap_or(DEFAULT_LOG_OUTPUT_TO_SCREEN)
    }

    fn timezone(&self) -> &FastStr {
        static TIMEZONE: FastStr = FastStr::from_static_str(DEFAULT_TIMEZONE);
        self.timezone.as_ref().unwrap_or(&TIMEZONE)
    }

    fn fqdn(&self) -> Option<&FastStr> {
        self.fqdn.as_ref()
    }

    fn server_port(&self) -> u16 {
        self.server_port.unwrap_or(DEFAULT_SERVER_PORT)
    }

    fn adapter_list(&self) -> Option<&FastStr> {
        self.adapter_list.as_ref()
    }

    fn conn_retries(&self) -> u32 {
        self.conn_retries.unwrap_or(DEFAULT_CONN_RETRIES)
    }

    fn retry_backoff_ms(&self) -> u64 {
        self.retry_backoff_ms.unwrap_or(DEFAULT_RETRY_BACKOFF_MS)
    }

    fn retry_backoff_max_ms(&self) -> u64 {
        self.retry_backoff_max_ms
            .unwrap_or(DEFAULT_RETRY_BACKOFF_MAX_MS)
    }

    fn log_keep_days(&self) -> u16 {
        self.log_keep_days.unwrap_or(DEFAULT_LOG_KEEP_DAYS)
    }

    fn rotation_size(&self) -> &FastStr {
        static ROTATION_SIZE: FastStr = FastStr::from_static_str(DEFAULT_ROTATION_SIZE);
        self.rotation_size.as_ref().unwrap_or(&ROTATION_SIZE)
    }

    fn tls_mode(&self) -> WsTlsMode {
        self.tls_mode.unwrap_or(DEFAULT_TLS_MODE)
    }

    fn tls_versions(&self) -> &[WsTlsVersion] {
        self.tls_versions.as_deref().unwrap_or(DEFAULT_TLS_VERSIONS)
    }

    fn tls_certs(&self) -> Option<&[CertificateDer<'static>]> {
        self.tls_certs.as_deref()
    }

    fn debug_flag(&self) -> Option<u16> {
        match self.log_level {
            Some(LevelFilter::WARN) => Some(131),
            Some(LevelFilter::DEBUG) => {
                if self.log_output_to_screen() {
                    Some(199)
                } else {
                    Some(135)
                }
            }
            Some(LevelFilter::TRACE) => {
                if self.log_output_to_screen() {
                    Some(207)
                } else {
                    Some(143)
                }
            }
            _ => None,
        }
    }

    fn set_config_dir<T: Into<FastStr>>(&mut self, cfg_dir: T) {
        self.config_dir = Some(cfg_dir.into());
    }

    fn set_compression(&mut self, compression: bool) {
        self.compression = Some(compression);
    }

    fn set_log_dir<T: Into<FastStr>>(&mut self, log_dir: T) {
        self.log_dir = Some(log_dir.into());
    }

    fn set_log_level(&mut self, log_level: LevelFilter) {
        self.log_level = Some(log_level);
    }

    fn set_log_output_to_screen(&mut self, log_output_to_screen: bool) {
        self.log_output_to_screen = Some(log_output_to_screen);
    }

    fn set_debug_flag(&mut self, flag: &str) {
        match flag {
            "131" | "warn" | "WARN" => self.log_level = Some(LevelFilter::WARN),
            "135" | "debug" | "DEBUG" => self.log_level = Some(LevelFilter::DEBUG),
            "143" | "trace" | "TRACE" => self.log_level = Some(LevelFilter::TRACE),
            "199" | "debug!" => {
                self.log_level = Some(LevelFilter::DEBUG);
                self.log_output_to_screen = Some(true);
            }
            "207" | "trace!" => {
                self.log_level = Some(LevelFilter::TRACE);
                self.log_output_to_screen = Some(true);
            }
            _ => {}
        }
    }

    fn set_timezone<T: Into<FastStr>>(&mut self, timezone: T) {
        if self.timezone.is_none() {
            self.timezone = Some(timezone.into());
        }
    }

    fn set_fqdn<T: Into<FastStr>>(&mut self, fqdn: T) {
        self.fqdn = Some(fqdn.into());
    }

    fn set_server_port(&mut self, server_port: u16) {
        self.server_port = Some(server_port);
    }

    fn set_adapter_list<T: Into<FastStr>>(&mut self, adapter_list: T) {
        self.adapter_list = Some(adapter_list.into());
    }

    fn set_conn_retries(&mut self, conn_retries: u32) {
        self.conn_retries = Some(conn_retries);
    }

    fn set_retry_backoff_ms(&mut self, retry_backoff_ms: u64) {
        self.retry_backoff_ms = Some(retry_backoff_ms);
    }

    fn set_retry_backoff_max_ms(&mut self, retry_backoff_max_ms: u64) {
        self.retry_backoff_max_ms = Some(retry_backoff_max_ms);
    }

    fn set_log_keep_days(&mut self, days: u16) {
        self.log_keep_days = Some(days);
    }

    fn set_rotation_size<T: Into<FastStr>>(&mut self, size: T) {
        self.rotation_size = Some(size.into());
    }

    fn set_tls_mode(&mut self, mode: &str) -> Result<(), TaosError> {
        self.tls_mode = Some(mode.parse()?);
        Ok(())
    }

    fn set_tls_versions(&mut self, version: &str) -> Result<(), TaosError> {
        let versions: Vec<WsTlsVersion> = version
            .split(',')
            .map(|s| s.trim().parse())
            .collect::<Result<_, TaosError>>()?;
        self.tls_versions = Some(versions);
        Ok(())
    }

    fn set_tls_certs(&mut self, pem: &str) -> Result<(), TaosError> {
        let pem = pem.trim();
        let certs: Vec<CertificateDer<'static>> = if pem.contains("-----BEGIN CERTIFICATE-----") {
            CertificateDer::pem_slice_iter(pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    TaosError::new(Code::INVALID_PARA, &format!("parse PEM failed: {e}"))
                })?
        } else {
            CertificateDer::pem_file_iter(Path::new(pem))
                .map_err(|e| {
                    TaosError::new(Code::INVALID_PARA, &format!("open PEM file failed: {e}"))
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    TaosError::new(Code::INVALID_PARA, &format!("parse PEM file failed: {e}"))
                })?
        };
        if certs.is_empty() {
            return Err(TaosError::new(
                Code::INVALID_PARA,
                "no certificate found in PEM (file)",
            ));
        }
        self.tls_certs = Some(certs);
        return Ok(());
    }

    fn load_from_path(&mut self, path: &str) -> Result<(), TaosError> {
        let path = Path::new(path);
        let config_file = if path.is_file() {
            path.to_path_buf()
        } else if path.is_dir() {
            let cfg_file = path.join("taos.cfg");
            if cfg_file.exists() {
                cfg_file
            } else {
                eprintln!("config file not found: {}", cfg_file.display());
                return Err(TaosError::new(
                    Code::INVALID_PARA,
                    &format!("config file not found: {}", cfg_file.display()),
                ));
            }
        } else {
            eprintln!("config dir not found: {}", path.display());
            return Err(TaosError::new(
                Code::INVALID_PARA,
                &format!("config dir not found: {}", path.display()),
            ));
        };

        read_config_from_path(&config_file)
            .map(|cfg| self.update_from(cfg))
            .inspect_err(|err| {
                eprintln!("failed to load config: {err:#}");
            })?;

        Ok(())
    }

    fn update_from(&mut self, rhs: Config) {
        macro_rules! update_if_none {
            ($($f:ident),+) => {
                $(
                    if self.$f.is_none() {
                        self.$f = rhs.$f.clone();
                    }
                )+
            };
        }

        update_if_none!(
            compression,
            log_dir,
            log_level,
            log_output_to_screen,
            timezone,
            fqdn,
            server_port,
            adapter_list,
            conn_retries,
            retry_backoff_ms,
            retry_backoff_max_ms,
            log_keep_days,
            rotation_size
        );
    }

    fn print(&self) {
        tracing::info!("{}global config{}", " ".repeat(31), " ".repeat(32));
        tracing::info!("{}", "=".repeat(76));

        macro_rules! show {
            ($opt:expr, $key:expr, $default:expr) => {
                if let Some(ref v) = $opt {
                    tracing::info!("{: <13}{: <33}{: <30}", "cfg_file", $key, v);
                } else {
                    tracing::info!("{: <13}{: <33}{: <30}", "default", $key, $default);
                }
            };
        }

        show!(self.adapter_list, "adapterList", "");
        show!(
            self.compression.map(|b| b as u8),
            "compression",
            DEFAULT_COMPRESSION as u8
        );
        show!(self.timezone, "timezone", DEFAULT_TIMEZONE);
        show!(self.log_dir, "logDir", DEFAULT_LOG_DIR);
        show!(self.debug_flag(), "debugFlag", DEFAULT_DEBUG_FLAG);
        show!(self.log_keep_days, "logKeepDays", DEFAULT_LOG_KEEP_DAYS);
        show!(self.rotation_size, "rotationSize", DEFAULT_ROTATION_SIZE);
        show!(self.conn_retries, "connRetries", DEFAULT_CONN_RETRIES);
        show!(
            self.retry_backoff_ms,
            "retryBackoffMs",
            DEFAULT_RETRY_BACKOFF_MS
        );
        show!(
            self.retry_backoff_max_ms,
            "retryBackoffMaxMs",
            DEFAULT_RETRY_BACKOFF_MAX_MS
        );

        tracing::info!("{}", "=".repeat(76));
    }
}

fn read_config_from_path(path: &Path) -> Result<Config, TaosError> {
    read_config_lines(path)
        .map_err(|e| {
            TaosError::new(
                Code::INVALID_PARA,
                &format!("failed to read config file: {}, err: {}", path.display(), e),
            )
        })
        .and_then(parse_config)
}

fn read_config_lines<P: AsRef<Path>>(path: P) -> Result<Vec<String>, io::Error> {
    let file = File::open(path)?;
    let reader = io::BufReader::new(file);
    reader.lines().collect()
}

fn parse_config(lines: Vec<String>) -> Result<Config, TaosError> {
    let mut config = Config::new();

    for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once(char::is_whitespace) {
            let value = value.trim();
            match key {
                "compression" => match value {
                    "1" => config.compression = Some(true),
                    "0" => config.compression = Some(false),
                    _ => {
                        return Err(TaosError::new(
                            Code::INVALID_PARA,
                            &format!("invalid value for compression: {value}"),
                        ));
                    }
                },
                "logDir" => config.log_dir = Some(value.to_string().into()),
                "debugFlag" => config.set_debug_flag(value),
                "timezone" => config.set_timezone::<FastStr>(value.to_string().into()),
                "fqdn" => config.fqdn = Some(value.to_string().into()),
                "serverPort" => {
                    config.server_port = Some(value.parse::<u16>().map_err(|_| {
                        TaosError::new(
                            Code::INVALID_PARA,
                            &format!("invalid value for serverPort: {value}"),
                        )
                    })?);
                }
                "adapterList" => config.adapter_list = Some(value.to_string().into()),
                "connRetries" => {
                    config.conn_retries = Some(value.parse::<u32>().map_err(|_| {
                        TaosError::new(
                            Code::INVALID_PARA,
                            &format!("invalid value for connRetries: {value}"),
                        )
                    })?);
                }
                "retryBackoffMs" => {
                    config.retry_backoff_ms = Some(value.parse::<u64>().map_err(|_| {
                        TaosError::new(
                            Code::INVALID_PARA,
                            &format!("invalid value for retryBackoffMs: {value}"),
                        )
                    })?);
                }
                "retryBackoffMaxMs" => {
                    config.retry_backoff_max_ms = Some(value.parse::<u64>().map_err(|_| {
                        TaosError::new(
                            Code::INVALID_PARA,
                            &format!("invalid value for retryBackoffMaxMs: {value}"),
                        )
                    })?);
                }
                "logKeepDays" => {
                    config.log_keep_days = Some(value.parse::<u16>().map_err(|_| {
                        TaosError::new(
                            Code::INVALID_PARA,
                            &format!("invalid value for logKeepDays: {value}"),
                        )
                    })?);
                }
                "rotationSize" => config.rotation_size = Some(value.to_string().into()),
                "wsTlsMode" => config.set_tls_mode(value)?,
                "wsTlsVersion" => config.set_tls_versions(value)?,
                "wsTlsCa" => config.set_tls_certs(value)?,
                _ => {}
            }
        }
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use std::env::set_var;

    use super::*;
    use crate::ws::{taos_options, TSDB_OPTION};

    #[test]
    fn test_read_config_from_path() {
        let config = read_config_from_path("./tests/taos.cfg".as_ref()).unwrap();
        assert_eq!(config.compression(), true);
        assert_eq!(config.log_dir(), "/path/to/logDir/");
        assert_eq!(config.log_level(), LevelFilter::DEBUG);
        assert_eq!(config.log_output_to_screen(), true);
        assert_eq!(config.timezone(), &FastStr::from("Asia/Shanghai"));
        assert_eq!(config.fqdn(), Some(&FastStr::from("hostname")));
        assert_eq!(config.server_port(), 8030);
        assert_eq!(config.adapter_list(), None);
        assert_eq!(config.conn_retries(), 5);
        assert_eq!(config.retry_backoff_ms(), 200);
        assert_eq!(config.retry_backoff_max_ms(), 2000);
        assert_eq!(config.log_keep_days(), 30);
        assert_eq!(config.rotation_size(), "1GB");
    }

    #[test]
    fn test_config_load_from_path() -> Result<(), TaosError> {
        {
            let mut config = Config::new();
            config.load_from_path("./tests")?;
            assert_eq!(config.compression(), true);
            assert_eq!(config.log_dir(), "/path/to/logDir/");
            assert_eq!(config.log_level(), LevelFilter::DEBUG);
            assert_eq!(config.log_output_to_screen(), true);
            assert_eq!(config.timezone(), &FastStr::from("Asia/Shanghai"));
            assert_eq!(config.fqdn(), Some(&FastStr::from("hostname")));
            assert_eq!(config.server_port(), 8030);
            assert_eq!(config.adapter_list(), None);
            assert_eq!(config.conn_retries(), 5);
            assert_eq!(config.retry_backoff_ms(), 200);
            assert_eq!(config.retry_backoff_max_ms(), 2000);
            assert_eq!(config.log_keep_days(), 30);
            assert_eq!(config.rotation_size(), "1GB");
        }

        {
            let mut config = Config::new();
            config.load_from_path("./tests/taos.cfg")?;
            assert_eq!(config.compression(), true);
            assert_eq!(config.log_dir(), "/path/to/logDir/");
            assert_eq!(config.log_level(), LevelFilter::DEBUG);
            assert_eq!(config.log_output_to_screen(), true);
            assert_eq!(config.timezone(), &FastStr::from("Asia/Shanghai"));
            assert_eq!(config.fqdn(), Some(&FastStr::from("hostname")));
            assert_eq!(config.server_port(), 8030);
            assert_eq!(config.adapter_list(), None);
            assert_eq!(config.conn_retries(), 5);
            assert_eq!(config.retry_backoff_ms(), 200);
            assert_eq!(config.retry_backoff_max_ms(), 2000);
            assert_eq!(config.log_keep_days(), 30);
            assert_eq!(config.rotation_size(), "1GB");
        }

        Ok(())
    }

    #[test]
    fn test_init() -> Result<(), String> {
        unsafe {
            let timezone = c"Asia/Shanghai";
            let code = taos_options(TSDB_OPTION::TSDB_OPTION_TIMEZONE, timezone.as_ptr() as _);
            assert_eq!(code, 0);

            let config_dir = c"./tests/taos.cfg";
            let code = taos_options(TSDB_OPTION::TSDB_OPTION_CONFIGDIR, config_dir.as_ptr() as _);
            assert_eq!(code, 0);
        }

        unsafe {
            set_var("RUST_LOG", "warn");
            set_var("TAOS_LOG_DIR", "/var/log/taos");
            set_var("TAOS_DEBUG_FLAG", "135");
            set_var("TAOS_DEBUG_FLAG", "143");
            set_var("TAOS_DEBUG_FLAG", "199");
            set_var("TAOS_DEBUG_FLAG", "207");
            set_var("TAOS_DEBUG_FLAG", "131");
            set_var("TAOS_LOG_OUTPUT_TO_SCREEN", "0");
            set_var("TAOS_TIMEZONE", "Asia/Shanghai");
            set_var("TAOS_FIRST_EP", "localhost");
            set_var("TAOS_SECOND_EP", "hostname:16030");
            set_var("TAOS_FQDN", "localhost");
            set_var("TAOS_SERVER_PORT", "6030");
            set_var("TAOS_COMPRESSION", "false");
            set_var("TAOS_CONFIG_DIR", "./tests");
            set_var("TAOS_CONN_RETRIES", "3");
            set_var("TAOS_RETRY_BACKOFF_MS", "100");
            set_var("TAOS_RETRY_BACKOFF_MAX_MS", "1000");
            set_var("TAOS_LOG_KEEP_DAYS", "30");
            set_var("TAOS_ROTATION_SIZE", "1GB");
        }

        init()?;

        assert_eq!(compression(), false);
        assert_eq!(fqdn(), Some(FastStr::from("localhost")));
        assert_eq!(server_port(), 6030);
        assert_eq!(conn_retries(), 3);
        assert_eq!(retry_backoff_ms(), 100);
        assert_eq!(retry_backoff_max_ms(), 1000);
        assert_eq!(log_keep_days(), 30);
        assert_eq!(rotation_size(), FastStr::from("1GB"));

        Ok(())
    }
}
