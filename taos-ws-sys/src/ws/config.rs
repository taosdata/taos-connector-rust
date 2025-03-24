use std::fmt;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::str::FromStr;
use std::sync::{OnceLock, RwLock};

use chrono_tz::Tz;
use faststr::FastStr;
use taos_error::Code;
use tracing::level_filters::LevelFilter;

use super::error::TaosError;

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub config_dir: Option<FastStr>,
    pub compression: Option<bool>,
    pub log_dir: Option<FastStr>,
    pub log_level: Option<LevelFilter>,
    pub log_output_to_screen: Option<bool>,
    pub timezone: Option<Tz>,
    pub first_ep: Option<FastStr>,
    pub second_ep: Option<FastStr>,
    pub fqdn: Option<FastStr>,
    pub server_port: Option<u16>,
}

pub static CONFIG: RwLock<Config> = RwLock::new(Config::const_new());

// FIXME
#[allow(dead_code)]
pub fn get_global_timezone() -> Option<Tz> {
    CONFIG.read().unwrap().timezone
}
#[allow(dead_code)]
pub fn get_global_log_dir() -> FastStr {
    CONFIG.read().unwrap().log_dir().clone()
}

// FIXME
#[allow(dead_code)]
pub fn get_global_log_level() -> LevelFilter {
    CONFIG.read().unwrap().log_level()
}
pub fn get_global_compression() -> bool {
    CONFIG.read().unwrap().compression()
}

// FIXME
#[allow(dead_code)]
pub fn get_global_log_output_to_screen() -> bool {
    CONFIG.read().unwrap().log_output_to_screen()
}
pub fn get_global_first_ep() -> Option<FastStr> {
    CONFIG.read().unwrap().first_ep().cloned()
}
pub fn get_global_second_ep() -> Option<FastStr> {
    CONFIG.read().unwrap().second_ep().cloned()
}
pub fn get_global_fqdn() -> Option<FastStr> {
    CONFIG.read().unwrap().fqdn().cloned()
}
pub fn get_global_server_port() -> u16 {
    let config = CONFIG.read().unwrap();
    config.server_port.unwrap_or(0)
}
pub fn init() -> Result<(), String> {
    static ONCE: OnceLock<Result<(), String>> = OnceLock::new();
    const DEFAULT_CONFIG: &str = if cfg!(windows) {
        "C:\\TDengine\\cfg\\taos.cfg"
    } else {
        "/etc/taos/taos.cfg"
    };
    ONCE.get_or_init(|| {
        let mut config = CONFIG.write().unwrap();
        if let Ok(e) = std::env::var("TAOS_LOG_DIR") {
            config.set_log_dir(e);
        }
        if let Ok(e) = std::env::var("RUST_LOG") {
            config.set_log_level(LevelFilter::from_str(&e).unwrap());
        }
        if let Ok(e) = std::env::var("TAOS_DEBUG_FLAG") {
            config.set_debug_flag_str(&e);
        }
        if let Ok(e) = std::env::var("TAOS_LOG_OUTPUT_TO_SCREEN") {
            config.set_log_output_to_screen(e == "1");
        }
        if let Ok(e) = std::env::var("TAOS_TIMEZONE") {
            config.set_timezone(Tz::from_str(&e).unwrap());
        }
        if let Ok(e) = std::env::var("TAOS_FIRST_EP") {
            config.set_first_ep(e);
        }
        if let Ok(e) = std::env::var("TAOS_SECOND_EP") {
            config.set_second_ep(e);
        }
        if let Ok(e) = std::env::var("TAOS_FQDN") {
            config.set_fqdn(e);
        }
        if let Ok(e) = std::env::var("TAOS_SERVER_PORT") {
            config.set_server_port(e.parse::<u16>().unwrap());
        }
        if let Ok(e) = std::env::var("TAOS_COMPRESSION") {
            config.set_compression(e.parse().unwrap());
        }

        let cfg_dir = if let Some(dir) = &config.config_dir {
            dir.to_string()
        } else if let Ok(dir) = std::env::var("TAOS_CONFIG_DIR") {
            if Path::new(&dir).exists() {
                dir
            } else {
                eprintln!("TAOS_CONFIG_DIR not found: {dir}");
                return Ok(());
            }
        } else if Path::new(DEFAULT_CONFIG).exists() {
            DEFAULT_CONFIG.to_string()
        } else {
            return Ok(());
        };
        if let Err(err) = config.read_config(&cfg_dir) {
            return Err(err.to_string());
        }
        Ok(())
    })
    .clone()
}

impl Config {
    pub fn compression(&self) -> bool {
        const DEFAULT_COMPRESSION: bool = false;
        self.compression.unwrap_or(DEFAULT_COMPRESSION)
    }

    pub fn log_dir(&self) -> &FastStr {
        static DEFAULT_LOG_DIR: FastStr = FastStr::from_static_str(if cfg!(windows) {
            "C:\\TDengine\\log"
        } else {
            "/var/log/taos"
        });

        self.log_dir.as_ref().unwrap_or(&DEFAULT_LOG_DIR)
    }

    // FIXME
    #[allow(dead_code)]
    pub fn log_level(&self) -> LevelFilter {
        const DEFAULT_LOG_LEVEL: LevelFilter = LevelFilter::WARN;
        self.log_level.unwrap_or(DEFAULT_LOG_LEVEL)
    }

    pub fn log_output_to_screen(&self) -> bool {
        self.log_output_to_screen.unwrap_or(false)
    }

    // FIXME
    #[allow(dead_code)]
    pub fn timezone(&self) -> Option<Tz> {
        self.timezone
    }

    pub fn first_ep(&self) -> Option<&FastStr> {
        self.first_ep.as_ref()
    }

    pub fn second_ep(&self) -> Option<&FastStr> {
        self.second_ep.as_ref()
    }

    pub fn fqdn(&self) -> Option<&FastStr> {
        self.fqdn.as_ref()
    }

    // FIXME
    #[allow(dead_code)]
    pub fn server_port(&self) -> Option<u16> {
        self.server_port
    }

    pub fn set_compression(&mut self, compression: bool) {
        self.compression = Some(compression);
    }
    pub fn set_log_dir<T: Into<FastStr>>(&mut self, log_dir: T) {
        self.log_dir = Some(log_dir.into());
    }

    // FIXME
    #[allow(dead_code)]
    pub fn set_debug_flag(&mut self, flag: i32) {
        match flag {
            131 => self.log_level = Some(LevelFilter::WARN),
            135 => self.log_level = Some(LevelFilter::DEBUG),
            143 => self.log_level = Some(LevelFilter::TRACE),
            199 => {
                self.log_level = Some(LevelFilter::DEBUG);
                self.log_output_to_screen = Some(true);
            }
            207 => {
                self.log_level = Some(LevelFilter::TRACE);
                self.log_output_to_screen = Some(true);
            }
            _ => {}
        }
    }
    pub fn set_debug_flag_str(&mut self, flag: &str) {
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
    pub fn set_log_level(&mut self, log_level: LevelFilter) {
        self.log_level = Some(log_level);
    }
    pub fn set_log_output_to_screen(&mut self, log_output_to_screen: bool) {
        self.log_output_to_screen = Some(log_output_to_screen);
    }
    pub fn set_timezone(&mut self, timezone: Tz) {
        if self.timezone.is_none() {
            self.timezone = Some(timezone);
        }
    }
    pub fn set_first_ep<T: Into<FastStr>>(&mut self, first_ep: T) {
        self.first_ep = Some(first_ep.into());
    }
    pub fn set_second_ep<T: Into<FastStr>>(&mut self, second_ep: T) {
        self.second_ep = Some(second_ep.into());
    }
    pub fn set_fqdn<T: Into<FastStr>>(&mut self, fqdn: T) {
        self.fqdn = Some(fqdn.into());
    }
    pub fn set_server_port(&mut self, server_port: u16) {
        self.server_port = Some(server_port);
    }
    pub fn set(&mut self, rhs: Config) {
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
            first_ep,
            second_ep,
            fqdn,
            server_port
        );
    }

    pub fn set_config_dir<T: Into<FastStr>>(&mut self, cfg_dir: T) {
        self.config_dir = Some(cfg_dir.into());
    }

    pub fn read_config(&mut self, cfg_dir: &str) -> Result<(), TaosError> {
        let config_dir = Path::new(cfg_dir);
        let config_file = if config_dir.is_file() {
            config_dir.to_path_buf()
        } else if config_dir.is_dir() {
            let f = config_dir.join("taos.cfg");
            if f.exists() {
                f
            } else {
                eprintln!("config file not found: {}", f.display());
                return Err(TaosError::new(
                    Code::INVALID_PARA,
                    &format!("config file not found: {}", f.display()),
                ));
            }
        } else {
            eprintln!("config dir not found: {}", config_dir.display());
            return Err(TaosError::new(
                Code::INVALID_PARA,
                &format!("config dir not found: {}", config_dir.display()),
            ));
        };

        Config::new(&config_file)
            .map(|config| self.set(config))
            .inspect_err(|err| {
                eprintln!("failed to set config: {err:#}");
            })?;
        Ok(())
    }

    const fn const_new() -> Config {
        Self {
            config_dir: None,
            compression: None,          // "false".to_string(),
            log_dir: None,              // "/var/log/taos".to_string(),
            log_level: None,            // LevelFilter::WARN,
            log_output_to_screen: None, // false,
            timezone: None,
            first_ep: None,
            second_ep: None,
            fqdn: None,
            server_port: None,
        }
    }
    fn new(filename: &Path) -> Result<Config, TaosError> {
        read_config_file(filename)
            .map_err(|_| {
                TaosError::new(
                    Code::INVALID_PARA,
                    &format!("failed to read config file: {}", filename.display()),
                )
            })
            .and_then(|lines| {
                parse_config(lines).map_err(|_| {
                    TaosError::new(
                        Code::INVALID_PARA,
                        &format!("failed to parse config file: {}", filename.display()),
                    )
                })
            })
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ConfigError {
    Io(io::Error),
    Parse(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(err) => write!(f, "Config IO error: {err}"),
            ConfigError::Parse(msg) => write!(f, "Config parse error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Io(err) => Some(err),
            ConfigError::Parse(_) => None,
        }
    }
}

impl From<io::Error> for ConfigError {
    fn from(e: io::Error) -> Self {
        ConfigError::Io(e)
    }
}

fn read_config_file<P: AsRef<Path>>(filename: P) -> Result<Vec<String>, ConfigError> {
    let file = File::open(filename)?;
    let reader = io::BufReader::new(file);
    let lines = reader.lines().collect::<io::Result<_>>()?;
    Ok(lines)
}

fn parse_config(lines: Vec<String>) -> Result<Config, ConfigError> {
    let mut config = Config::default();

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
                        return Err(ConfigError::Parse(format!(
                            "failed to parse compression: {value}",
                        )));
                    }
                },
                "logDir" => config.log_dir = Some(value.to_string().into()),
                "debugFlag" => config.set_debug_flag_str(value),
                "timezone" => {
                    config.timezone = Some(Tz::from_str(value).map_err(|_| {
                        ConfigError::Parse(format!("failed to parse timezone: {value}"))
                    })?);
                }
                "firstEp" => config.first_ep = Some(value.to_string().into()),
                "secondEp" => config.second_ep = Some(value.to_string().into()),
                "fqdn" => config.fqdn = Some(value.to_string().into()),
                "serverPort" => {
                    config.server_port = Some(value.parse::<u16>().map_err(|_| {
                        ConfigError::Parse(format!("failed to parse serverPort: {value}",))
                    })?);
                }
                _ => {}
            }
        }
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let config = Config::new("./tests/taos.cfg".as_ref()).unwrap();
        assert_eq!(config.compression(), true);
        assert_eq!(config.log_dir.as_deref().unwrap(), "/path/to/logDir/");
        assert_eq!(config.log_level.unwrap(), LevelFilter::DEBUG);
        assert_eq!(config.log_output_to_screen.unwrap(), true);
        assert_eq!(config.timezone, Some(Tz::Asia__Shanghai));
        assert_eq!(config.first_ep.as_deref().unwrap(), "hostname:7030");
        assert_eq!(config.second_ep.as_deref().unwrap(), "hostname:16030");
        assert_eq!(config.fqdn.as_deref().unwrap(), "hostname");
        assert_eq!(config.server_port, Some(8030));
    }
}
