use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::str::FromStr;
use std::sync::{OnceLock, RwLock};

use faststr::FastStr;
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
            if let Ok(compression) = s.parse::<bool>() {
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

pub fn timezone() -> Option<FastStr> {
    CONFIG.read().unwrap().timezone().cloned()
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

pub fn set_config_dir<T: Into<FastStr>>(cfg_dir: T) {
    CONFIG.write().unwrap().set_config_dir(cfg_dir);
}

pub fn set_timezone<T: Into<FastStr>>(timezone: T) {
    CONFIG.write().unwrap().set_timezone(timezone);
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
        }
    }

    #[allow(dead_code)]
    fn config_dir(&self) -> &FastStr {
        static DEFAULT_CONFIG_DIR: FastStr = FastStr::from_static_str(if cfg!(windows) {
            "C:\\TDengine\\cfg"
        } else {
            "/etc/taos"
        });

        self.config_dir.as_ref().unwrap_or(&DEFAULT_CONFIG_DIR)
    }

    fn compression(&self) -> bool {
        self.compression.unwrap_or(false)
    }

    fn log_dir(&self) -> &FastStr {
        static DEFAULT_LOG_DIR: FastStr = FastStr::from_static_str(if cfg!(windows) {
            "C:\\TDengine\\log"
        } else {
            "/var/log/taos"
        });

        self.log_dir.as_ref().unwrap_or(&DEFAULT_LOG_DIR)
    }

    fn log_level(&self) -> LevelFilter {
        self.log_level.unwrap_or(LevelFilter::WARN)
    }

    fn log_output_to_screen(&self) -> bool {
        self.log_output_to_screen.unwrap_or(false)
    }

    fn timezone(&self) -> Option<&FastStr> {
        self.timezone.as_ref()
    }

    fn fqdn(&self) -> Option<&FastStr> {
        self.fqdn.as_ref()
    }

    fn server_port(&self) -> u16 {
        self.server_port.unwrap_or(0)
    }

    fn adapter_list(&self) -> Option<&FastStr> {
        self.adapter_list.as_ref()
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
            adapter_list
        );
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
                "adapterList" => {
                    config.adapter_list = Some(value.to_string().into());
                }
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
        assert_eq!(config.timezone(), Some(&FastStr::from("Asia/Shanghai")));
        assert_eq!(config.fqdn(), Some(&FastStr::from("hostname")));
        assert_eq!(config.server_port(), 8030);
        assert_eq!(
            config.adapter_list(),
            Some(&FastStr::from("dev1:6041,dev2:6041,dev3:6041"))
        );
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
            assert_eq!(config.timezone(), Some(&FastStr::from("Asia/Shanghai")));
            assert_eq!(config.fqdn(), Some(&FastStr::from("hostname")));
            assert_eq!(config.server_port(), 8030);
            assert_eq!(
                config.adapter_list(),
                Some(&FastStr::from("dev1:6041,dev2:6041,dev3:6041"))
            );
        }

        {
            let mut config = Config::new();
            config.load_from_path("./tests/taos.cfg")?;
            assert_eq!(config.compression(), true);
            assert_eq!(config.log_dir(), "/path/to/logDir/");
            assert_eq!(config.log_level(), LevelFilter::DEBUG);
            assert_eq!(config.log_output_to_screen(), true);
            assert_eq!(config.timezone(), Some(&FastStr::from("Asia/Shanghai")));
            assert_eq!(config.fqdn(), Some(&FastStr::from("hostname")));
            assert_eq!(config.server_port(), 8030);
            assert_eq!(
                config.adapter_list(),
                Some(&FastStr::from("dev1:6041,dev2:6041,dev3:6041"))
            );
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
            set_var("TAOS_ADAPTER_LIST", "dev1:6041,dev2:6041,dev3:6041");
        }

        init()?;

        assert_eq!(compression(), false);
        assert_eq!(fqdn(), Some(FastStr::from("localhost")));
        assert_eq!(server_port(), 6030);
        assert_eq!(
            adapter_list(),
            Some(FastStr::from("dev1:6041,dev2:6041,dev3:6041"))
        );

        Ok(())
    }
}
