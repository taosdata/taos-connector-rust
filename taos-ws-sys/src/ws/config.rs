#![allow(dead_code)]

use std::fmt;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

#[derive(Debug)]
pub struct Config {
    pub compression: bool,
    pub log_dir: String,
    pub log_level: String,
    pub timezone: Option<String>,
    pub first_ep: Option<String>,
    pub second_ep: Option<String>,
    pub fqdn: Option<String>,
    pub server_port: Option<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            compression: false,
            log_dir: "/var/log/taos".to_string(),
            log_level: "warn".to_string(),
            timezone: None,
            first_ep: None,
            second_ep: None,
            fqdn: None,
            server_port: None,
        }
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
            ConfigError::Io(e) => write!(f, "Config IO error: {e}"),
            ConfigError::Parse(msg) => write!(f, "Config parse error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Io(e) => Some(e),
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
                "compression" => match value.parse::<u8>() {
                    Ok(1) => config.compression = true,
                    Ok(0) => config.compression = false,
                    _ => {
                        return Err(ConfigError::Parse(format!(
                            "Failed to parse compression: {value}",
                        )));
                    }
                },
                "logDir" => config.log_dir = value.to_string(),
                "debugFlag" => match value.parse::<u8>() {
                    Ok(131) => config.log_level = "warn".to_string(),
                    Ok(135) => config.log_level = "debug".to_string(),
                    Ok(143) => config.log_level = "trace".to_string(),
                    Ok(199) => todo!(),
                    Ok(207) => todo!(),
                    _ => {
                        return Err(ConfigError::Parse(format!(
                            "Failed to parse debugFlag: {value}",
                        )));
                    }
                },
                "timezone" => config.timezone = Some(value.to_string()),
                "firstEp" => config.first_ep = Some(value.to_string()),
                "secondEp" => config.second_ep = Some(value.to_string()),
                "fqdn" => config.fqdn = Some(value.to_string()),
                "serverPort" => match value.parse::<u16>() {
                    Ok(port) => config.server_port = Some(port),
                    Err(_) => {
                        return Err(ConfigError::Parse(format!(
                            "Failed to parse serverPort: {value}",
                        )));
                    }
                },
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
        let lines = read_config_file("./tests/taos.cfg")?;
        let config = parse_config(lines)?;
        assert_eq!(config.compression, true);
        assert_eq!(config.log_dir, "/var/log/taos".to_string());
        assert_eq!(config.log_level, "warn".to_string());
        assert_eq!(config.timezone, Some("UTC-8".to_string()));
        assert_eq!(config.first_ep, Some("hostname:6030".to_string()));
        assert_eq!(config.second_ep, Some("hostname:16030".to_string()));
        assert_eq!(config.fqdn, Some("hostname".to_string()));
        assert_eq!(config.server_port, Some(6030));
        Ok(())
    }
}
