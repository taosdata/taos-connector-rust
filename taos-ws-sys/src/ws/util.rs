use std::ffi::CStr;

use libc::{setlocale, LC_CTYPE};

use crate::ws::{self, config};

pub fn get_system_locale() -> String {
    #[cfg(target_os = "windows")]
    {
        let locale_ptr = unsafe { setlocale(LC_CTYPE, c"en_US.UTF-8".as_ptr()) };
        if !locale_ptr.is_null() {
            return "en_US.UTF-8".to_string();
        }
        String::new()
    }

    #[cfg(not(target_os = "windows"))]
    {
        let locale_ptr = unsafe { setlocale(LC_CTYPE, c"".as_ptr()) };
        if !locale_ptr.is_null() {
            if let Ok(locale) = unsafe { CStr::from_ptr(locale_ptr).to_str() } {
                return locale.to_string();
            }
        }
        "en_US.UTF-8".to_string()
    }
}

pub fn is_cloud_host(host: &str) -> bool {
    host.contains("cloud.tdengine") || host.contains("cloud.taosdata")
}

pub fn resolve_port(host: &str, port: u16) -> u16 {
    if port != 0 {
        port
    } else if is_cloud_host(host) {
        ws::DEFAULT_CLOUD_PORT
    } else {
        ws::DEFAULT_PORT
    }
}

pub fn camel_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            out.push('_');
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

pub fn build_dsn(
    addr: Option<&str>,
    user: Option<&str>,
    pass: Option<&str>,
    db: Option<&str>,
    ws_tls_mode: Option<config::WsTlsMode>,
    ws_tls_version: Option<&str>,
    ws_tls_ca: Option<&str>,
) -> String {
    let addr = match addr {
        Some(addr) => addr.to_string(),
        None => match config::adapter_list() {
            Some(addr) => addr.to_string(),
            None => format!("{}:{}", ws::DEFAULT_HOST, ws::DEFAULT_PORT),
        },
    };

    let user = user.unwrap_or(ws::DEFAULT_USER);
    let pass = pass.unwrap_or(ws::DEFAULT_PASS);
    let db = db.unwrap_or(ws::DEFAULT_DB);

    let ws_tls_mode = ws_tls_mode.unwrap_or_else(config::ws_tls_mode);

    let ws_tls_version =
        ws_tls_version.map_or_else(|| config::ws_tls_version().to_string(), |s| s.to_string());

    let ws_tls_ca = ws_tls_ca
        .map(|s| s.to_string())
        .or_else(|| config::ws_tls_ca().map(|s| s.to_string()));

    let compression = config::compression();
    let conn_retries = config::conn_retries();
    let retry_backoff_ms = config::retry_backoff_ms();
    let retry_backoff_max_ms = config::retry_backoff_max_ms();

    let protocol = match ws_tls_mode {
        config::WsTlsMode::Disabled => "ws",
        _ => "wss",
    };

    let ws_tls_mode = match ws_tls_mode {
        config::WsTlsMode::VerifyCa => Some("verify_ca"),
        config::WsTlsMode::VerifyIdentity => Some("verify_identity"),
        _ => None,
    };

    let tls_mode = match ws_tls_mode {
        Some(mode) => format!("&tls_mode={mode}"),
        None => String::new(),
    };

    let tls_ca = match ws_tls_ca {
        Some(ca) => format!("&tls_ca={ca}"),
        None => String::new(),
    };

    let tls_params = format!("&tls_version={ws_tls_version}{tls_mode}{tls_ca}");

    let params = format!(
        "compression={compression}\
        &conn_retries={conn_retries}\
        &retry_backoff_ms={retry_backoff_ms}\
        &retry_backoff_max_ms={retry_backoff_max_ms}"
    );

    if is_cloud_host(&addr) && user == "token" {
        format!("wss://{addr}/{db}?token={pass}&{params}")
    } else {
        format!("{protocol}://{user}:{pass}@{addr}/{db}?{params}{tls_params}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_system_locale() {
        let locale = get_system_locale();
        println!("system locale: {}", locale);
        #[cfg(not(target_os = "windows"))]
        assert!(!locale.is_empty());
    }

    #[test]
    fn test_camel_to_snake() {
        assert_eq!(camel_to_snake("CamelCase"), "_camel_case");
        assert_eq!(camel_to_snake("camelCaseTest"), "camel_case_test");
        assert_eq!(camel_to_snake("lowercase"), "lowercase");
        assert_eq!(camel_to_snake("UPPERCASE"), "_u_p_p_e_r_c_a_s_e");
    }
}
