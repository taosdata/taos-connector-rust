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
use urlencoding::encode;

impl Dsn {
    pub fn from_regex(input: &str) -> Result<Self, DsnError> {
        lazy_static::lazy_static! {
            static ref RE: Regex = Regex::new(r"(?x)
                (?P<driver>[\w.-]+)(\+(?P<protocol>[^@/?\#]+))?: # abc
                (
                    # url-like dsn
                    //((?P<username>[\w\s\-_%.]+)?(:(?P<password>[^@/?\#]+))?@)? # for authorization
                        (((?P<protocol2>[\w\s.-]+)\()?
                            (?P<addr>[\w\-_%.:]*(:\d{0,5})?(,[\w\-:_.]*(:\d{0,5})?)*)?  # for addresses
                        \)?)?
                        (/(?P<subject>[\w\s%$@.,/-]+)?)?                             # for subject
                    | # or
                    # path-like dsn
                    (?P<path>([\\/.~]$|/\s*\w+[\w\s %$@*:.\\\-/ _\(\)\[\]{}（）【】｛｝]*|[\.~\w\s]?[\w\s %$@*:.\\\-/ _\(\)\[\]{}（）【】｛｝]+))
                ) # abc
                (\?(?P<params>(?s:.)*))?").unwrap();
        }

        let cap = RE
            .captures(input)
            .ok_or_else(|| DsnError::InvalidConnection(input.to_string()))?;

        let driver = cap.name("driver").unwrap().as_str().to_string();
        let protocol = cap.name("protocol").map(|m| m.as_str().to_string());
        let protocol2 = cap.name("protocol2").map(|m| m.as_str().to_string());
        let protocol = match (protocol, protocol2) {
            (Some(_), Some(_)) => Err(DsnError::InvalidProtocol("".to_string()))?,
            (Some(p), None) | (None, Some(p)) => Some(p),
            _ => None,
        };
        let username = cap
            .name("username")
            .map(|m| {
                let s = m.as_str();
                urlencoding::decode(s)
                    .map(|s| s.to_string())
                    .map_err(|err| DsnError::InvalidPassword(s.to_string(), err))
            })
            .transpose()?;
        let password = cap
            .name("password")
            .map(|m| {
                let s = m.as_str();
                urlencoding::decode(s)
                    .map(|s| s.to_string())
                    .map_err(|err| DsnError::InvalidPassword(s.to_string(), err))
            })
            .transpose()?;
        let subject = cap.name("subject").map(|m| m.as_str().to_string());
        let path = cap.name("path").map(|m| m.as_str().to_string());
        let addresses = if let Some(addr) = cap.name("addr") {
            let addr = addr.as_str();
            if addr.is_empty() {
                vec![]
            } else {
                let mut addrs = Vec::new();

                for s in addr.split(',') {
                    if s.is_empty() {
                        continue;
                    }
                    if let Some((host, port)) = s.split_once(':') {
                        let port = if port.is_empty() {
                            0
                        } else {
                            port.parse::<u16>().map_err(|err| {
                                DsnError::InvalidAddresses(s.to_string(), err.to_string())
                            })?
                        };
                        addrs.push(Address::new(host, port));
                    } else if s.contains('%') {
                        addrs.push(Address::from_path(urlencoding::decode(s).unwrap()));
                    } else {
                        addrs.push(Address::from_host(s));
                    }
                }
                addrs
            }
        } else {
            vec![]
        };
        let mut params = BTreeMap::new();
        if let Some(p) = cap.name("params") {
            for p in p.as_str().split_terminator('&') {
                if p.contains('=') {
                    if let Some((k, v)) = p.split_once('=') {
                        let k = urlencoding::decode(k)?;
                        let v = urlencoding::decode(v)?;
                        params.insert(k.to_string(), v.to_string());
                    }
                } else {
                    let p = urlencoding::decode(p)?;
                    params.insert(p.to_string(), "".to_string());
                }
            }
        }
        Ok(Dsn {
            driver,
            protocol,
            username,
            password,
            addresses,
            path,
            subject,
            params,
        })
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
                    let (mut name, mut value) = ("".to_string(), "".to_string());
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
}

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
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let host = host.into();
        let host = if host.is_empty() { None } else { Some(host) };
        Self {
            host,
            port: Some(port),
            ..Default::default()
        }
    }
    // ident: Ident,
    /// Construct server address with host or ip address only.
    #[inline]
    pub fn from_host(host: impl Into<String>) -> Self {
        let host = host.into();
        let host = if host.is_empty() { None } else { Some(host) };
        Self {
            host,
            ..Default::default()
        }
    }

    /// Construct server address with unix socket path.
    #[inline]
    pub fn from_path(path: impl Into<String>) -> Self {
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
    fn is_path_like(&self) -> bool {
        self.username.is_none()
            && self.password.is_none()
            && self.addresses.is_empty()
            && self.path.is_some()
    }
}

pub trait IntoDsn {
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
                write!(f, "{}:{}@", encode(username), encode(password))?
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
                if v.contains(|c| c == '=' || c == '&' || c == '#' || c == '@') {
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

impl Dsn {
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
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        self.params.insert(key.into(), value.into())
    }

    #[inline]
    pub fn get(&self, key: impl AsRef<str>) -> Option<&String> {
        self.params.get(key.as_ref())
    }

    #[inline]
    pub fn remove(&mut self, key: impl AsRef<str>) -> Option<String> {
        self.params.remove(key.as_ref())
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
        let s = "csv:./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝&.csv";
        let dsn = Dsn::from_str(s).unwrap();
        assert_eq!(
            dsn,
            Dsn {
                driver: "csv".to_string(),
                path: Some("./files/1718243049903/文件Aa1 %$@.-_()[]{}（）【】｛｝".to_string()),
                ..Default::default()
            }
        );
        assert_ne!(dsn.to_string(), s);
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
        let p = "!@#$%^&*()";
        let e = urlencoding::encode(p);
        dbg!(&e);

        let dsn = Dsn::from_str(&format!("taos://{e}:{e}@localhost:6030?{e}={e}")).unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.password.as_deref().unwrap(), p);
        assert_eq!(dsn.get(p).unwrap(), p);
        assert_eq!(
            dsn.to_string(),
            format!("taos://{e}:{e}@localhost:6030?{e}={e}")
        );
    }
    #[test]
    fn unix_path_with_glob() {
        let dsn = Dsn::from_str(&format!("csv:./**.csv?param=1")).unwrap();
        dbg!(&dsn);
        assert!(dsn.path.is_some());
        assert_eq!(dsn.get("param").unwrap(), "1");

        let dsn = Dsn::from_str(&format!("csv:./**.csv?param=1")).unwrap();
        dbg!(&dsn);
        assert!(dsn.path.is_some());
        assert_eq!(dsn.get("param").unwrap(), "1");

        let dsn = Dsn::from_str(&format!("csv:.\\**.csv?param=1")).unwrap();
        dbg!(&dsn);
        assert!(dsn.path.is_some());
        assert_eq!(dsn.get("param").unwrap(), "1");
    }

    #[test]
    fn unix_path_with_space() {
        let dsn = Dsn::from_str(&format!("csv:./a b.csv?param=1")).unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.path.unwrap(), "./a b.csv");
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
}
