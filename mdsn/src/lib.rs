//! M-DSN: A Multi-address DSN(Data Source Name) parser.
//!
//! M-DSN support two kind of DSN format:
//!
//! 1. `<driver>[+<protocol>]://<username>:<password>@<addresses>/<database>?<params>`
//! 2. `<driver>[+<protocol>]://<username>:<password>@<fragment>?<params>`
//! 3. `<driver>://<username>:<password>@<protocol>(<addresses>)/<database>?<params>`
//!
//! All the items will be parsed into struct [Dsn](crate::Dsn).
//!
//! ## Parser
//!
//! ```rust
//! use mdsn::Dsn;
//! use std::str::FromStr;
//!
//! # fn main() -> Result<(), mdsn::DsnError> {
//! // The two styles are equivalent.
//! let dsn = Dsn::from_str("taos://root:taosdata@host1:6030,host2:6030/db")?;
//! let dsn: Dsn = "taos://root:taosdata@host1:6030,host2:6030/db".parse()?;
//!
//! assert_eq!(dsn.driver, "taos");
//! assert_eq!(dsn.username.unwrap(), "root");
//! assert_eq!(dsn.password.unwrap(), "taosdata");
//! assert_eq!(dsn.subject.unwrap(), "db");
//! assert_eq!(dsn.addresses.len(), 2);
//! assert_eq!(dsn.addresses, vec![
//!     mdsn::Address::new("host1", 6030),
//!     mdsn::Address::new("host2", 6030),
//! ]);
//! # Ok(())
//! # }
//! ```
//!
//! ## DSN Examples
//!
//! A DSN for [TDengine](https://taosdata.com) driver [taos](https://docs.rs/taos).
//!
//! ```dsn
//! taos://root:taosdata@localhost:6030/db?timezone=Asia/Shanghai&asyncLog=1
//! ```
//!
//! With multi-address:
//!
//! ```dsn
//! taos://root:taosdata@host1:6030,host2:6030/db?timezone=Asia/Shanghai
//! ```
//!
//! A DSN for unix socket:
//!
//! ```dsn
//! unix:///path/to/unix.sock?param1=value
//! ```
//!
//! A DSN for postgresql with url-encoded socket directory path.
//!
//! ```dsn
//! postgresql://%2Fvar%2Flib%2Fpostgresql/db
//! ```
//!
//! A DSN for sqlite db file, note that you must use prefix `./` for a relative path file.
//!
//! ```dsn
//! sqlite://./file.db
//! ```
//!
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::num::ParseIntError;
use std::str::FromStr;
use std::string::FromUtf8Error;

use itertools::Itertools;
#[cfg(feature = "pest")]
use pest::Parser;
#[cfg(feature = "pest")]
use pest_derive::Parser;
use regex::Regex;
use thiserror::Error;
use urlencoding::{decode, encode};

#[cfg(feature = "pest")]
#[derive(Parser)]
#[grammar = "dsn.pest"]
struct DsnParser;

/// Error caused by [pest](https://docs.rs/pest) DSN parser.
#[derive(Debug, Error)]
pub enum DsnError {
    #[cfg(feature = "pest")]
    #[error("{0}")]
    ParseErr(#[from] pest::error::Error<Rule>),
    #[error("unable to parse port from {0}")]
    PortErr(#[from] ParseIntError),
    #[error("invalid dsn format: {0}, use either path:/path or driver://user:pass@host:port?k=v")]
    InvalidFormat(String),
    #[error("No `:` found in {0}, use either path:/path or driver://user:pass@host:port?k=v")]
    NoColonFound(String),
    #[error("dsn contains '@' character({0}), please use full DSN format: <driver>://<username>:<password>@<addresses>/<database>?<params>")]
    InvalidSpecialCharacterFormat(String),
    #[error("invalid driver {0}")]
    InvalidDriver(String),
    #[error("invalid protocol {0}")]
    InvalidProtocol(String),
    #[error("invalid password {0}: {1}")]
    InvalidPassword(String, FromUtf8Error),
    #[error("invalid connection {0}")]
    InvalidConnection(String),
    #[error("invalid addresses: {0}, error: {1}")]
    InvalidAddresses(String, String),
    #[error("requires database: {0}")]
    RequireDatabase(String),
    #[error("requires parameter: {0}")]
    RequireParam(String),
    #[error("invalid parameter for {0}: {1}")]
    InvalidParam(String, String),
    #[error("non utf8 character: {0}")]
    NonUtf8Character(#[from] FromUtf8Error),
}

/// A simple struct to represent a server address, with host:port or socket path.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Address {
    /// Host or ip address of the server.
    pub host: Option<String>,
    /// Port to connect to the server.
    pub port: Option<u16>,
    /// Use unix socket path to connect.
    pub path: Option<String>,
}

impl Address {
    /// Construct server address with host and port.
    #[inline]
    pub fn new<T: Into<String>>(host: T, port: u16) -> Self {
        let host = host.into();
        let host = if host.is_empty() { None } else { Some(host) };
        Self {
            host,
            port: Some(port),
            ..Default::default()
        }
    }

    /// Construct server address with host or ip address only.
    #[inline]
    pub fn from_host<T: Into<String>>(host: T) -> Self {
        let host = host.into();
        let host = if host.is_empty() { None } else { Some(host) };
        Self {
            host,
            ..Default::default()
        }
    }

    /// Construct server address with unix socket path.
    #[inline]
    pub fn from_path<T: Into<String>>(path: T) -> Self {
        Self {
            path: Some(path.into()),
            ..Default::default()
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.host.is_none() && self.port.is_none() && self.path.is_none()
    }
}

impl FromStr for Address {
    type Err = DsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #[cfg(feature = "pest")]
        {
            let mut addr = Self::default();
            if let Some(dsn) = DsnParser::parse(Rule::address, &s)?.next() {
                for inner in dsn.into_inner() {
                    match inner.as_rule() {
                        Rule::host => addr.host = Some(inner.as_str().to_string()),
                        Rule::port => addr.port = Some(inner.as_str().parse()?),
                        Rule::path => {
                            addr.path = Some(
                                urlencoding::decode(inner.as_str())
                                    .expect("UTF-8")
                                    .to_string(),
                            )
                        }
                        _ => unreachable!(),
                    }
                }
            }
            Ok(addr)
        }
        #[cfg(not(feature = "pest"))]
        {
            if s.is_empty() {
                Ok(Self::default())
            } else if let Some((host, port)) = s.split_once(':') {
                Ok(Address::new(host, port.parse().unwrap()))
            } else if s.contains('%') {
                Ok(Address::from_path(urlencoding::decode(s).unwrap()))
            } else {
                Ok(Address::from_host(s))
            }
        }
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.host, self.port, &self.path) {
            (Some(host), None, None) => write!(f, "{host}"),
            (Some(host), Some(port), None) => write!(f, "{host}:{port}"),
            (None, Some(port), None) => write!(f, ":{port}"),
            (None, None, Some(path)) => write!(f, "{}", urlencoding::encode(path)),
            (None, None, None) => Ok(()),
            _ => unreachable!("path will be conflict with host/port"),
        }
    }
}

#[test]
#[cfg(feature = "pest")]
fn addr_parse() {
    let s = "taosdata:6030";
    let addr = Address::from_str(s).unwrap();
    assert_eq!(addr.to_string(), s);
    assert!(!addr.is_empty());

    let s = "/var/lib/taos";
    let addr = Address::from_str(&urlencoding::encode(s)).unwrap();
    assert_eq!(addr.path.as_ref().unwrap(), s);
    assert_eq!(addr.to_string(), urlencoding::encode(s));

    assert_eq!(
        Address::new("localhost", 6030).to_string(),
        "localhost:6030"
    );
    assert_eq!(Address::from_host("localhost").to_string(), "localhost");
    assert_eq!(
        Address::from_path("/path/unix.sock").to_string(),
        "%2Fpath%2Funix.sock"
    );

    let none = Address {
        host: None,
        port: None,
        path: None,
    };
    assert!(none.is_empty());
    assert_eq!(none.to_string(), "");
}

/// A DSN(**Data Source Name**) parser.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Dsn {
    pub driver: String,
    pub protocol: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub addresses: Vec<Address>,
    pub path: Option<String>,
    pub subject: Option<String>,
    pub params: BTreeMap<String, String>,
}

impl Dsn {
    pub fn from_regex(input: &str) -> Result<Self, DsnError> {
        // lazy_static::lazy_static! {
        //     static ref RE: Regex = Regex::new(r"(?x)
        //         (?P<driver>[\w.-]+)(\+(?P<protocol>[^@/?\#]+))?: # abc
        //         (
        //             //((?P<username>[\w\s\-_%.]+)?(:(?P<password>[^@/?\#]+))?@)? # for authorization
        //                 (((?P<protocol2>[\w\s.-]+)\()?
        //                     (?P<addr>[\w\-_%.:]*(:\d{0,5})?(,[\w\-:_.]*(:\d{0,5})?)*)  # for addresses
        //                 \)?)?
        //                 (/(?P<subject>[\w\s%$@.,/-]+)?)?                             # for subject
        //             |
        //             # url-like dsn
        //             //((?P<username>[\w\s\-_%.]+)?(:(?P<password>[^@/?\#]+))?@)? # for authorization
        //                 (((?P<protocol2>[\w\s.-]+)\()?
        //                     (?P<addr>[\w\-_%.:]*(:\d{0,5})?(,[\w\-:_.]*(:\d{0,5})?)*)?  # for addresses
        //                 \)?)?
        //                 (/(?P<subject>[\w\s%$@.,/-]+)?)?                             # for subject
        //             | # or
        //             # path-like dsn
        //             (?P<path>([\\/.~]$|/\s*\w+[\w\s %$@*:,.\\\-/ _\(\)\[\]{}（）【】｛｝]*|[\.~\w\s]?[\w\s %$@*:,.\\\-/ _\(\)\[\]{}（）【】｛｝]+))
        //         ) # abc
        //         (\?(?P<params>(?s:.)*))?").unwrap();
        // }

        if !input.contains(':') {
            return Err(DsnError::NoColonFound(input.to_string()));
        }

        let (driver, conf) = input.split_once(':').unwrap();

        let (driver, protocol) = if let Some((driver, protocol)) = driver.split_once('+') {
            (driver.to_string(), Some(protocol.to_string()))
        } else {
            (driver.to_string(), None)
        };

        #[inline]
        fn percent_decode(encoded: String) -> String {
            decode(&encoded).map(|s| s.to_string()).unwrap_or(encoded)
        }
        type ParsedAddr = (Option<String>, Option<String>, Option<String>, Vec<Address>);
        fn parse_addr(main: &str) -> Result<ParsedAddr, DsnError> {
            if main.trim().is_empty() {
                return Ok((None, None, None, vec![]));
            }
            let mut protocol = None;
            if let Some(at) = main
                .bytes()
                .enumerate()
                .rev()
                .find(|(_, v)| *v == b'@')
                .map(|(at, _)| at)
            {
                let (user_pass, addrs) = main.split_at(at);
                let mut addrs = addrs.strip_prefix('@').expect("strip prefix @");

                let addrs = if addrs.is_empty() {
                    vec![]
                } else {
                    lazy_static::lazy_static! {
                        static ref AT_SINGLE_ADDR_REQUIRE_PORT_REGEX: Regex = Regex::new(r"^(.+):(\d{1,5})$").unwrap();
                        static ref AT_MULTI_ADDR_REQUIRE_PORT_REGEX: Regex = Regex::new(r"^(?P<host>.+):(?P<port>\d{1,5})(,(([\w\-_%.]+)(:\d{0,5})?))*$").unwrap();
                        static ref PROTOCOL2_MULTI_ADDR_REQUIRE_PORT_REGEX: Regex = Regex::new(r"^((?P<protocol>.*)\((?P<addrs>.*)\))$").unwrap();
                    }
                    if !addrs.contains(':') {
                        vec![Address::from_host(percent_decode(addrs.to_string()))]
                    } else {
                        if let Some(cap) = PROTOCOL2_MULTI_ADDR_REQUIRE_PORT_REGEX.captures(addrs) {
                            protocol = cap
                                .name("protocol")
                                .map(|m: regex::Match<'_>| m.as_str().to_string());
                            addrs = cap.name("addrs").map(|m| m.as_str()).unwrap();
                        }
                        addrs
                            .split(',')
                            .filter_map(|addr| {
                                if addr.is_empty() {
                                    return None;
                                }
                                if let Some(port) =
                                    addr.strip_prefix(":").map(|port| port.parse::<u16>())
                                {
                                    return port
                                        .map_err(|e| {
                                            DsnError::InvalidAddresses(
                                                addr.to_string(),
                                                e.to_string(),
                                            )
                                        })
                                        .map(|port| Address {
                                            port: Some(port),
                                            ..Default::default()
                                        })
                                        .map(Some)
                                        .transpose();
                                }
                                if let Some(host) = addr.strip_suffix(":") {
                                    return Some(Ok(Address::from_host(percent_decode(
                                        host.to_string(),
                                    ))));
                                }
                                AT_SINGLE_ADDR_REQUIRE_PORT_REGEX
                                    .captures(addr)
                                    .ok_or_else(|| {
                                        DsnError::InvalidAddresses(
                                            addr.to_string(),
                                            "format error, use host:port syntax".to_string(),
                                        )
                                    })
                                    .and_then(|cap| {
                                        let host = cap
                                            .get(1)
                                            .map(|m| m.as_str().to_string())
                                            .map(percent_decode);
                                        let port = cap
                                            .get(2)
                                            .map(|m| {
                                                m.as_str().parse::<u16>().map_err(|e| {
                                                    DsnError::InvalidAddresses(
                                                        addr.to_string(),
                                                        e.to_string(),
                                                    )
                                                })
                                            })
                                            .transpose()?;
                                        Ok(Some(Address {
                                            host,
                                            port,
                                            ..Default::default()
                                        }))
                                    })
                                    .transpose()
                            })
                            .try_collect()?
                    }
                };

                let (username, password) = if let Some((user, pass)) = user_pass.split_once(':') {
                    (Some(user.to_string()), Some(pass.to_string()))
                } else {
                    (Some(user_pass.to_string()), None)
                };

                Ok((protocol, username, password, addrs))
            } else {
                lazy_static::lazy_static! {
                    static ref AT_SINGLE_ADDR_WITH_PORT_REGEX: Regex = Regex::new(r"^([\w\-_%.]+)(:(\d{0,5}))?$").unwrap();
                    static ref AT_MULTI_ADDR_WITH_PORT_REGEX: Regex = Regex::new(r"^(?P<host>[\w\-_%.]+)(:(?P<port>\d{0,5}))?[,(?P<addr2>(?P<host2>[\w\-_%.]+)(:(?P<port2>\d{0,5}))?)]*$").unwrap();
                }
                if let Some(cap) = AT_SINGLE_ADDR_WITH_PORT_REGEX.captures(main) {
                    let host = cap
                        .get(1)
                        .map(|m| m.as_str().to_string())
                        .map(percent_decode);
                    let port = cap.get(3).map(|m| m.as_str().parse::<u16>().unwrap());
                    match (host, port) {
                        (Some(host), Some(port)) => {
                            Ok((protocol, None, None, vec![Address::new(host, port)]))
                        }
                        (Some(host), None) => {
                            if host.contains("/") {
                                Ok((protocol, None, None, vec![Address::from_path(host)]))
                            } else {
                                Ok((protocol, None, None, vec![Address::from_host(host)]))
                            }
                        }
                        _ => Err(DsnError::InvalidFormat(main.to_string())),
                    }
                } else {
                    Err(DsnError::InvalidFormat(main.to_string()))
                }
            }
        }
        if conf.starts_with("//") {
            let main = conf.strip_prefix("//").expect("strip prefix");

            lazy_static::lazy_static! {
                static ref URL_PARAMS_REGEX: Regex = Regex::new(r"^(?P<main>.*)(\?(?P<params>[^?]+))$").unwrap();
                static ref ENDS_WITH_ADDR_REGEX: Regex = Regex::new(r"@[\w\-_%.]+(:\d{1,5})?(,[\w\-_%.]+(:\d{1,5})?)*$").unwrap();
            }
            let mut dsn = Dsn {
                driver,
                protocol,
                path: None,
                ..Default::default()
            };

            let (main, params) = if ENDS_WITH_ADDR_REGEX.is_match(main) {
                (main, None)
            } else if let Some(v) = main
                .bytes()
                .enumerate()
                .rev()
                .find(|(_, v)| *v == b'?')
                .map(|(at, _v)| at)
            {
                let (main, params) = main.split_at(v);
                let params = params
                    .strip_prefix('?')
                    .expect("split_at.1 must contain ?")
                    .trim();
                if params.is_empty() {
                    (main, None)
                } else {
                    (main, Some(params))
                }
            } else {
                (main, None)
            };
            if let Some(p) = params {
                for p in p.split('&') {
                    if p.contains('=') {
                        if let Some((k, v)) = p.split_once('=') {
                            let k = urlencoding::decode(k)?;
                            let v = urlencoding::decode(v)?;
                            dsn.params.insert(k.to_string(), v.to_string());
                        }
                    } else {
                        let p = urlencoding::decode(p)?;
                        dsn.params.insert(p.to_string(), String::new());
                    }
                }
            }

            if let Some((addr, subject)) = main.split_once('/') {
                let (addr, subject) = (addr.trim(), subject.trim());
                let (protocol, username, password, addresses) = parse_addr(addr)?;
                let subject = if subject.is_empty() {
                    None
                } else {
                    Some(subject.to_string())
                };

                if protocol.is_some() {
                    dsn.protocol = protocol;
                }
                dsn.subject = subject;
                dsn.username = username.map(percent_decode);
                dsn.password = password.map(percent_decode);
                dsn.addresses = addresses;
            } else {
                let (protocol, username, password, addresses) = parse_addr(main)?;

                if protocol.is_some() {
                    dsn.protocol = protocol;
                }
                dsn.username = username.map(percent_decode);
                dsn.password = password.map(percent_decode);
                dsn.addresses = addresses;
            }
            Ok(dsn)
        } else {
            // path-like dsn
            lazy_static::lazy_static! {
                static ref PATH_REGEX: Regex = Regex::new(r"(?P<path>.*)\?(?P<params>[^?]+)").unwrap();
            }
            let mut dsn = Dsn {
                driver,
                protocol,
                path: None,
                ..Default::default()
            };
            if let Some(cap) = PATH_REGEX.captures(conf) {
                let path = cap.name("path").map(|m| m.as_str().to_string());
                let params = cap.name("params").map(|m| m.as_str().to_string());
                dsn.path = path;
                dsn.params = params
                    .map(|s| {
                        s.split('&')
                            .map(|p| {
                                if let Some((k, v)) = p.split_once('=') {
                                    (percent_decode(k.to_string()), percent_decode(v.to_string()))
                                } else {
                                    (percent_decode(p.to_string()), percent_decode(String::new()))
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();
            } else {
                dsn.path = Some(percent_decode(conf.to_string()));
            }
            Ok(dsn)
        }
    }

    #[cfg(feature = "pest")]
    pub fn from_pest(input: &str) -> Result<Self, DsnError> {
        let dsn = DsnParser::parse(Rule::dsn, input)?.next().unwrap();

        let mut to = Dsn::default();
        for pair in dsn.into_inner() {
            match pair.as_rule() {
                Rule::scheme => {
                    for inner in pair.into_inner() {
                        match inner.as_rule() {
                            Rule::driver => to.driver = inner.as_str().to_string(),
                            Rule::protocol => to.protocol = Some(inner.as_str().to_string()),
                            _ => unreachable!(),
                        }
                    }
                }
                Rule::SCHEME_IDENT => (),
                Rule::username_with_password => {
                    for inner in pair.into_inner() {
                        match inner.as_rule() {
                            Rule::username => to.username = Some(inner.as_str().to_string()),
                            Rule::password => to.password = Some(inner.as_str().to_string()),
                            _ => unreachable!(),
                        }
                    }
                }
                Rule::protocol_with_addresses => {
                    for inner in pair.into_inner() {
                        match inner.as_rule() {
                            Rule::addresses => {
                                for inner in inner.into_inner() {
                                    match inner.as_rule() {
                                        Rule::address => {
                                            let mut addr = Address::default();
                                            for inner in inner.into_inner() {
                                                match inner.as_rule() {
                                                    Rule::host => {
                                                        addr.host = Some(inner.as_str().to_string())
                                                    }
                                                    Rule::port => {
                                                        addr.port = Some(inner.as_str().parse()?)
                                                    }
                                                    Rule::path => {
                                                        addr.path = Some(
                                                            urlencoding::decode(inner.as_str())
                                                                .expect("UTF-8")
                                                                .to_string(),
                                                        )
                                                    }
                                                    _ => unreachable!(),
                                                }
                                            }
                                            to.addresses.push(addr);
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                            }
                            Rule::protocol => to.protocol = Some(inner.as_str().to_string()),
                            _ => unreachable!(),
                        }
                    }
                }
                Rule::database => {
                    to.subject = Some(pair.as_str().to_string());
                }
                Rule::fragment => {
                    to.path = Some(pair.as_str().to_string());
                }
                Rule::path_like => {
                    to.path = Some(pair.as_str().to_string());
                }
                Rule::param => {
                    let (mut name, mut value) = (String::new(), String::new());
                    for inner in pair.into_inner() {
                        match inner.as_rule() {
                            Rule::name => name = inner.as_str().to_string(),
                            Rule::value => value = inner.as_str().to_string(),
                            _ => unreachable!(),
                        }
                    }
                    to.params.insert(name, value);
                }
                Rule::EOI => {}
                _ => unreachable!(),
            }
        }
        Ok(to)
    }

    #[inline]
    pub fn drain_params(&mut self) -> BTreeMap<String, String> {
        let drained = self
            .params
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.params.clear();
        drained
    }

    #[inline]
    pub fn set<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) -> Option<String> {
        self.params.insert(key.into(), value.into())
    }

    #[inline]
    pub fn get<T: AsRef<str>>(&self, key: T) -> Option<&String> {
        self.params.get(key.as_ref())
    }

    #[inline]
    pub fn remove<T: AsRef<str>>(&mut self, key: T) -> Option<String> {
        self.params.remove(key.as_ref())
    }

    fn is_path_like(&self) -> bool {
        self.username.is_none()
            && self.password.is_none()
            && self.addresses.is_empty()
            && self.path.is_some()
    }
}

pub trait IntoDsn: Send {
    fn into_dsn(self) -> Result<Dsn, DsnError>;
}

impl IntoDsn for &str {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        self.parse()
    }
}

impl IntoDsn for String {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        self.as_str().into_dsn()
    }
}

impl IntoDsn for &String {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        self.as_str().into_dsn()
    }
}

impl IntoDsn for &Dsn {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        Ok(self.clone())
    }
}
impl IntoDsn for Dsn {
    fn into_dsn(self) -> Result<Dsn, DsnError> {
        Ok(self)
    }
}

impl Display for Dsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.driver)?;
        if let Some(protocol) = &self.protocol {
            write!(f, "+{protocol}")?;
        }

        if self.is_path_like() {
            write!(f, ":")?;
        } else {
            write!(f, "://")?;
        }

        match (&self.username, &self.password) {
            (Some(username), Some(password)) => {
                write!(f, "{}:{}@", encode(username), encode(password))?;
            }
            (Some(username), None) => write!(f, "{}@", encode(username))?,
            (None, Some(password)) => write!(f, ":{}@", encode(password))?,
            (None, None) => {}
        }

        if !self.addresses.is_empty() {
            write!(
                f,
                "{}",
                self.addresses.iter().map(ToString::to_string).join(",")
            )?;
        }
        if let Some(database) = &self.subject {
            write!(f, "/{database}")?;
        } else if let Some(path) = &self.path {
            write!(f, "{path}")?;
        }

        if !self.params.is_empty() {
            fn percent_encode_or_not(v: &str) -> Cow<str> {
                if v.contains(['=', '&', '#', '@']) {
                    urlencoding::encode(v)
                } else {
                    v.into()
                }
            }
            write!(
                f,
                "?{}",
                self.params
                    .iter()
                    .map(|(k, v)| format!(
                        "{}={}",
                        percent_encode_or_not(k),
                        percent_encode_or_not(v)
                    ))
                    .join("&")
            )?;
        }
        Ok(())
    }
}

impl TryFrom<&str> for Dsn {
    type Error = DsnError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Dsn::from_str(value)
    }
}

impl TryFrom<&String> for Dsn {
    type Error = DsnError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Dsn::from_str(value)
    }
}

impl TryFrom<String> for Dsn {
    type Error = DsnError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Dsn::from_str(&value)
    }
}

impl TryFrom<&Dsn> for Dsn {
    type Error = DsnError;

    fn try_from(value: &Dsn) -> Result<Self, Self::Error> {
        Ok(value.clone())
    }
}
impl FromStr for Dsn {
    type Err = DsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #[cfg(feature = "pest")]
        return Self::from_pest(s);
        #[cfg(not(feature = "pest"))]
        return Self::from_regex(s);
    }
}

/// Returns if a dsn param value is true or false.
///
/// - Empty means true.
/// - 1/t/true/yes/on/enable/enabled are true.
/// - All others are false.
///
/// # Examples
///
/// ```rust
/// use mdsn::value_is_true;
///
/// for s in ["", "1", "true", "t", "yes", "y", "on", "enable", "enabled"] {
///     assert_eq!(value_is_true(s), true);
/// }
/// for s in ["0", "false", "f", "no", "n", "disable", "disabled", "any-other-str"] {
///     assert_eq!(value_is_true(s), false);
/// }
/// ```
pub fn value_is_true<T: AsRef<str>>(s: T) -> bool {
    matches!(
        s.as_ref(),
        "" | "1"
            | "true"
            | "t"
            | "yes"
            | "y"
            | "on"
            | "enable"
            | "enabled"
            | "T"
            | "YES"
            | "TRUE"
            | "ON"
            | "ENABLE"
            | "ENABLED"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_into_dsn() {
        let _ = "taos://".into_dsn().unwrap();
        let _ = "taos://".to_string().into_dsn().unwrap();
        let _ = (&"taos://".to_string()).into_dsn().unwrap();

        let a: Dsn = "taos://".parse().unwrap();
        let b = a.into_dsn().unwrap();
        let c = (&b).into_dsn().unwrap();

        let d: Dsn = (&c).try_into().unwrap();
        let _: Dsn = (d).try_into().unwrap();
        let _: Dsn = ("taos://").try_into().unwrap();
        let _: Dsn = ("taos://".to_string()).try_into().unwrap();
        let _: Dsn = (&"taos://:password@".parse::<Dsn>().unwrap().to_string())
            .try_into()
            .unwrap();
    }

    #[test]
    fn test_methods() {
        let mut dsn: Dsn = "taos://localhost:6030/test?debugFlag=135".parse().unwrap();

        let flag = dsn.get("debugFlag").unwrap();
        assert_eq!(flag, "135");

        dsn.set("configDir", "/tmp/taos");
        assert_eq!(dsn.get("configDir").unwrap(), "/tmp/taos");
        let config = dsn.remove("configDir").unwrap();
        assert_eq!(config, "/tmp/taos");

        let params = dsn.drain_params();
        assert!(dsn.params.is_empty());
        assert!(params.contains_key("debugFlag"));
        assert!(params.len() == 1);
    }

    #[test]
    fn username_with_password() {
        let s = "taos://";

        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos:///";

        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), "taos://");

        let s = "taos://root@";

        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
        let s = "taos://root:taosdata@";

        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn host_port_mix() {
        let s = "taos://localhost";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                addresses: vec![Address {
                    host: Some("localhost".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    port: Some(6030),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@localhost:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    host: Some("localhost".to_string()),
                    port: Some(6030),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@localhost:1234port";
        let e = Dsn::from_str(s).expect_err("port error");
        assert_eq!(
            e.to_string(),
            "invalid addresses: localhost:1234port, error: format error, use host:port syntax"
        );

        let s = "taos://root@localhost:";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    host: Some("localhost".to_string()),
                    port: None,
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
    }
    #[test]
    fn username_with_host() {
        let s = "taos://root@localhost";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    host: Some("localhost".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address {
                    port: Some(6030),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root@localhost:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address::new("localhost", 6030)],
                ..Default::default()
            }
        );

        let s = "taos://root:taosdata@localhost:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                addresses: vec![Address::new("localhost", 6030)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn username_with_multi_addresses() {
        let s = "taos://root@host1:6030,,,,/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                subject: Some("db1".to_string()),
                addresses: vec![Address::new("host1", 6030)],
                ..Default::default()
            }
        );

        let s = "taos://root@host1.domain:6030,host2.domain:6031";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![
                    Address::new("host1.domain", 6030),
                    Address::new("host2.domain", 6031)
                ],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root:taosdata@host1:6030,host2:6031";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn db_only() {
        let s = "taos:///db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                subject: Some("db1".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos:///db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                subject: Some("db1".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn username_with_multi_addresses_database() {
        let s = "taos://root@host1:6030,host2:6031/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                subject: Some("db1".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "taos://root:taosdata@host1:6030,host2:6031/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                subject: Some("db1".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn protocol() {
        let s = "taos://root@tcp(host1:6030,host2:6031)/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                subject: Some("db1".to_string()),
                protocol: Some("tcp".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), "taos+tcp://root@host1:6030,host2:6031/db1");

        let s = "taos+tcp://root@host1:6030,host2:6031/db1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                subject: Some("db1".to_string()),
                protocol: Some("tcp".to_string()),
                addresses: vec![Address::new("host1", 6030), Address::new("host2", 6031)],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), "taos+tcp://root@host1:6030,host2:6031/db1");
    }

    #[test]
    fn fragment() {
        let s = "postgresql://%2Fvar%2Flib%2Fpostgresql/dbname";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "postgresql".to_string(),
                subject: Some("dbname".to_string()),
                addresses: vec![Address {
                    path: Some("/var/lib/postgresql".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "unix:/path/to/unix.sock";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "unix".to_string(),
                path: Some("/path/to/unix.sock".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "sqlite:/c:/full/windows/path/to/file.db";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "sqlite".to_string(),
                path: Some("/c:/full/windows/path/to/file.db".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "sqlite:./file.db";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "sqlite".to_string(),
                path: Some("./file.db".to_string()),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = "sqlite://root:pass@//full/unix/path/to/file.db?mode=0666&readonly=true";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "sqlite".to_string(),
                username: Some("root".to_string()),
                password: Some("pass".to_string()),
                subject: Some("/full/unix/path/to/file.db".to_string()),
                params: (BTreeMap::from_iter(vec![
                    ("mode".to_string(), "0666".to_string()),
                    ("readonly".to_string(), "true".to_string())
                ])),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);
    }

    #[test]
    fn path_special_chars() {
        // support `Chinese characters`, `uppercase and lowercase letters`, `digits`, `spaces`, `%`, `$`, `@`, `.`, `-`, `_`, `(`, `)`, `[`, `]`, `{`, `}`, `（`, `）`, `【`, `】`, `｛`, `｝`
        let s = "csv:./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝.csv";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "csv".to_string(),
                path: Some(
                    "./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝.csv".to_string()
                ),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        // do not support other characters which are not in the list above(such as `&` .etc)
        // Edited by @zitsen on 2024-12-16: `&` is supported now.
        let s = "csv:./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝&.csv";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "csv".to_string(),
                path: Some(
                    "./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝&.csv".to_string()
                ),
                ..Default::default()
            }
        );
    }

    #[test]
    fn params() {
        let s = r#"taos://?abc=abc"#;
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                params: (BTreeMap::from_iter(vec![("abc".to_string(), "abc".to_string())])),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = r#"taos://root@localhost?abc=abc"#;
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address::from_host("localhost")],
                params: (BTreeMap::from_iter(vec![("abc".to_string(), "abc".to_string())])),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), s);

        let s = r#"taos://root@localhost?a%20b"#;
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "taos".to_string(),
                username: Some("root".to_string()),
                addresses: vec![Address::from_host("localhost")],
                params: (BTreeMap::from_iter(vec![("a b".to_string(), String::new())])),
                ..Default::default()
            }
        );
        assert_eq!(dsn.to_string(), "taos://root@localhost?a b=");
    }

    #[test]
    fn parse_taos_tmq() {
        let s = "taos://root:taosdata@localhost/aa23d04011eca42cf7d8c1dd05a37985?topics=aa23d04011eca42cf7d8c1dd05a37985&group.id=tg2";
        let _ = Dsn::from_str(s).unwrap();
    }

    #[test]
    fn tmq_ws_driver() {
        let dsn = Dsn::from_str("tmq+ws:///abc1?group.id=abc3&timeout=50ms").unwrap();
        assert_eq!(dsn.driver, "tmq");
        assert_eq!(dsn.to_string(), "tmq+ws:///abc1?group.id=abc3&timeout=50ms");
    }

    #[test]
    fn tmq_offset() {
        let dsn = Dsn::from_str("tmq+ws:///abc1?offset=10:20,11:40").unwrap();
        let offset = dsn.get("offset").unwrap();
        dbg!(&dsn);
        dbg!(&offset);
    }

    #[test]
    fn password_special_chars() {
        let p = "!@#$%^&*()";
        let e = urlencoding::encode(p);
        dbg!(&e);

        let dsn = Dsn::from_str(&format!("taos://root:{e}@localhost:6030/")).unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.password.as_deref().unwrap(), p);
        assert_eq!(dsn.to_string(), format!("taos://root:{e}@localhost:6030"));
    }

    #[test]
    fn param_special_chars() {
        let p = "!@#$%^&*()";
        let e = urlencoding::encode(p);
        dbg!(&e);

        let dsn = Dsn::from_str(&format!("taos://root:{e}@localhost:6030?code1={e}")).unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.password.as_deref().unwrap(), p);
        assert_eq!(dsn.get("code1").unwrap(), p);
        assert_eq!(
            dsn.to_string(),
            format!("taos://root:{e}@localhost:6030?code1={e}")
        );
    }

    #[test]
    fn param_special_chars_all() {
        let u = "a";
        let p = "!@#$%^&*()";
        let e = urlencoding::encode(p);
        dbg!(&e);

        let dsn = Dsn::from_str(&format!("taos://{u}:{p}@localhost:6030?{e}={e}")).unwrap();
        dbg!(&dsn);
        let dsn2 = Dsn::from_str(&format!("taos://{u}:{e}@localhost:6030?{e}={e}")).unwrap();
        assert_eq!(dsn, dsn2);
        assert_eq!(dsn.password.as_deref().unwrap(), p);
        assert_eq!(dsn.get(p).unwrap(), p);
        assert_eq!(
            dsn.to_string(),
            format!("taos://{u}:{e}@localhost:6030?{e}={e}")
        );
    }

    #[test]
    fn unix_path_with_glob() {
        let dsn = Dsn::from_str("csv:./**.csv?param=1").unwrap();
        dbg!(&dsn);
        assert!(dsn.path.is_some());
        assert_eq!(dsn.get("param").unwrap(), "1");

        let dsn = Dsn::from_str("csv:./**.csv?param=1").unwrap();
        dbg!(&dsn);
        assert!(dsn.path.is_some());
        assert_eq!(dsn.get("param").unwrap(), "1");

        let dsn = Dsn::from_str("csv:.\\**.csv?param=1").unwrap();
        dbg!(&dsn);
        assert!(dsn.path.is_some());
        assert_eq!(dsn.get("param").unwrap(), "1");
    }

    #[test]
    fn unix_path_with_space() {
        let dsn = Dsn::from_str("csv:./a b.csv?param=1").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.path.unwrap(), "./a b.csv");
    }

    #[test]
    fn unix_multiple_path() {
        let dsn = Dsn::from_str("csv:./a b.csv,c d .csv?param=1").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.path.unwrap(), "./a b.csv,c d .csv");
    }

    #[test]
    fn unit_path_with_utf8() {
        let dsn = Dsn::from_str("csv:./文件 Aa1.csv?param=1").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.path.unwrap(), "./文件 Aa1.csv");
    }

    #[test]
    fn with_space() {
        let dsn = Dsn::from_str("pi:///Met1 ABC").unwrap();
        assert_eq!(dsn.subject, Some("Met1 ABC".to_string()));

        let dsn = Dsn::from_str("pi://u ser:pa ss@host:80/Met1 ABC").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.subject, Some("Met1 ABC".to_string()));
        assert_eq!(dsn.username, Some("u ser".to_string()));
        assert_eq!(dsn.password, Some("pa ss".to_string()));
    }

    #[test]
    fn test_address() {
        let addr = Address::from_str("").unwrap();
        assert!(addr.is_empty());
        assert_eq!(addr.to_string(), "");
        let addr = Address::from_str("192.168.1.32").unwrap();
        assert!(!addr.is_empty());
        assert_eq!(addr.host.expect("host set").as_str(), "192.168.1.32");

        let addr = Address::from_str("192.168.1.32:0").unwrap();
        assert!(!addr.is_empty());
        assert_eq!(addr.host.expect("host set").as_str(), "192.168.1.32");
        assert_eq!(addr.port.expect("port set"), 0);

        let addr = Address::from_str("/path/to/file%20name").unwrap();
        assert_eq!(addr.path.as_ref().expect("path set"), "/path/to/file name");
        assert!(addr.host.is_none());
        assert!(addr.port.is_none());
        assert!(!addr.is_empty());
    }

    #[test]
    #[should_panic]
    fn test_address_panic() {
        let addr = Address {
            host: Some("localhost".to_string()),
            port: Some(6030),
            path: Some("/path/to/file".to_string()),
        };
        addr.to_string();
    }

    #[test]
    fn test_value_is_true() {
        for s in ["", "1", "true", "t", "yes", "y", "on", "enable", "enabled"] {
            assert_eq!(value_is_true(s), true);
        }
        for s in [
            "0",
            "false",
            "f",
            "no",
            "n",
            "disable",
            "disabled",
            "any-other-str",
        ] {
            assert_eq!(value_is_true(s), false);
        }
    }

    #[test]
    fn url_single_addr_special_chars() {
        let s = "taos://root:!@#$%^&*()-_+=[]{}:;><?|~,@localhost:6030";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(dsn.password.unwrap(), "!@#$%^&*()-_+=[]{}:;><?|~,");
    }
    #[test]
    fn path_with_special_chars() {
        let s = "taos:/!@#$%^&*()-_+=[]{}:;><?|~,?params=1";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(dsn.path.unwrap(), "/!@#$%^&*()-_+=[]{}:;><?|~,");
        assert_eq!(dsn.params.get("params").unwrap(), "1");
    }
    #[test]
    fn at_question_position_mix() {
        let s = "taos:/!@#$%^&*()-_+=[]{}:;><?|~,?params=./@abc.txt&b=!@#$%^*()-_+=[]{}:;><中文汉字£¢§®";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(dsn.path.unwrap(), "/!@#$%^&*()-_+=[]{}:;><?|~,");
        assert_eq!(dsn.params.get("params").unwrap(), "./@abc.txt");
        assert_eq!(
            dsn.params.get("b").unwrap(),
            "!@#$%^*()-_+=[]{}:;><中文汉字£¢§®"
        );
    }
}
