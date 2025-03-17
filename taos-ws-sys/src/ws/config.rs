#![allow(dead_code)]

use std::fmt;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::str::FromStr;
use std::sync::OnceLock;

use chrono_tz::Tz;
use tracing::level_filters::LevelFilter;

#[derive(Debug)]
pub struct Config {
    pub compression: String,
    pub log_dir: String,
    pub log_level: LevelFilter,
    pub log_output_to_screen: bool,
    pub timezone: Option<Tz>,
    pub first_ep: Option<String>,
    pub second_ep: Option<String>,
    pub fqdn: Option<String>,
    pub server_port: Option<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            compression: "false".to_string(),
            log_dir: "/var/log/taos".to_string(),
            log_level: LevelFilter::WARN,
            log_output_to_screen: false,
            timezone: None,
            first_ep: None,
            second_ep: None,
            fqdn: None,
            server_port: None,
        }
    }
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn init() -> Result<(), ConfigError> {
    let config = Config::new("/etc/taos/taos.cfg")?;
    CONFIG
        .set(config)
        .map_err(|_| ConfigError::Init("Config has been initialized".to_string()))?;
    Ok(())
}

pub fn config() -> &'static Config {
    CONFIG.get().expect("config not initialized")
}

impl Config {
    fn new(filename: &str) -> Result<Config, ConfigError> {
        let lines = read_config_file(filename)?;
        parse_config(lines)
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ConfigError {
    Io(io::Error),
    Parse(String),
    Init(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "Config IO error: {e}"),
            ConfigError::Parse(msg) => write!(f, "Config parse error: {msg}"),
            ConfigError::Init(msg) => write!(f, "Config initialization error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Io(e) => Some(e),
            _ => None,
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
                    "1" => config.compression = "true".to_string(),
                    "0" => config.compression = "false".to_string(),
                    _ => {
                        return Err(ConfigError::Parse(format!(
                            "failed to parse compression: {value}",
                        )));
                    }
                },
                "logDir" => config.log_dir = value.to_string(),
                "debugFlag" => {
                    match value.parse::<u8>() {
                        Ok(131) => config.log_level = LevelFilter::WARN,
                        Ok(135) => config.log_level = LevelFilter::DEBUG,
                        Ok(143) => config.log_level = LevelFilter::TRACE,
                        Ok(199) => {
                            config.log_level = LevelFilter::DEBUG;
                            config.log_output_to_screen = true;
                        }
                        Ok(207) => {
                            config.log_level = LevelFilter::TRACE;
                            config.log_output_to_screen = true;
                        }
                        _ => {
                            return Err(ConfigError::Parse(format!(
                                "failed to parse debugFlag: {value}",
                            )));
                        }
                    }

                    if let Ok(level) = std::env::var("RUST_LOG") {
                        config.log_level = LevelFilter::from_str(&level).map_err(|_| {
                            ConfigError::Parse(format!("failed to parse RUST_LOG: {value}"))
                        })?;
                        config.log_output_to_screen = true;
                    }
                }
                "timezone" => {
                    config.timezone = Some(Tz::from_str(value).map_err(|_| {
                        ConfigError::Parse(format!("failed to parse timezone: {value}"))
                    })?);
                }
                "firstEp" => config.first_ep = Some(value.to_string()),
                "secondEp" => config.second_ep = Some(value.to_string()),
                "fqdn" => config.fqdn = Some(value.to_string()),
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
    fn test_config() -> Result<(), ConfigError> {
        let config = Config::new("./tests/taos.cfg")?;
        assert_eq!(config.compression, "true");
        assert_eq!(config.log_dir, "/var/log/taos".to_string());
        assert_eq!(config.log_level, LevelFilter::DEBUG);
        assert_eq!(config.log_output_to_screen, true);
        assert_eq!(config.timezone, Some(Tz::Asia__Shanghai));
        assert_eq!(config.first_ep, Some("hostname:6030".to_string()));
        assert_eq!(config.second_ep, Some("hostname:16030".to_string()));
        assert_eq!(config.fqdn, Some("hostname".to_string()));
        assert_eq!(config.server_port, Some(6030));
        Ok(())
    }
}
