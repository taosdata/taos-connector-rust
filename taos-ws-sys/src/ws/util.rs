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

pub fn build_dsn(addr: &str, user: &str, pass: &str, db: &str) -> String {
    let compression = config::compression();
    let conn_retries = config::conn_retries();
    let retry_backoff_ms = config::retry_backoff_ms();
    let retry_backoff_max_ms = config::retry_backoff_max_ms();
    let ws_tls_mode = config::ws_tls_mode();
    let ws_tls_version = config::ws_tls_version();
    let ws_tls_ca = config::ws_tls_ca();

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

    let tls_ca = match config::ws_tls_ca() {
        Some(ca) => format!("&tls_ca={ca}"),
        None => String::new(),
    };

    let params = format!(
        "compression={compression}\
        &conn_retries={conn_retries}\
        &retry_backoff_ms={retry_backoff_ms}\
        &retry_backoff_max_ms={retry_backoff_max_ms}\
        &tls_version={ws_tls_version}{tls_mode}{tls_ca}"
    );

    if is_cloud_host(addr) && user == "token" {
        format!("wss://{addr}/{db}?token={pass}&{params}")
    } else {
        format!("{protocol}://{user}:{pass}@{addr}/{db}?{params}")
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
