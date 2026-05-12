#[cfg(not(windows))]
use std::ffi::CStr;

#[cfg(windows)]
use chrono::Offset;
use libc::{setlocale, LC_CTYPE};
#[cfg(windows)]
use tracing::warn;

use crate::ws;

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

#[cfg(windows)]
unsafe extern "C" {
    fn _putenv_s(name: *const std::ffi::c_char, value: *const std::ffi::c_char) -> std::ffi::c_int;
    fn _tzset();
}

#[cfg(not(windows))]
pub fn set_tz_env(tz: &str) {
    unsafe { std::env::set_var("TZ", tz) };
}

#[cfg(windows)]
pub fn set_tz_env(tz: &str) {
    let posix = iana_to_posix_tz(tz);
    let key = c"TZ";

    match std::ffi::CString::new(posix.as_str()) {
        Ok(value) => {
            if unsafe { _putenv_s(key.as_ptr(), value.as_ptr()) } != 0 {
                warn!(
                    "Failed to set timezone via _putenv_s, fallback to process env only: {posix}"
                );
            }
        }
        Err(err) => {
            warn!("Failed to build TZ environment string: {err}");
        }
    }

    unsafe { std::env::set_var("TZ", &posix) };
    unsafe { _tzset() };
}

#[cfg(windows)]
fn iana_to_posix_tz(tz: &str) -> String {
    let parsed_tz = match tz.parse::<chrono_tz::Tz>() {
        Ok(parsed_tz) => parsed_tz,
        Err(_) => {
            warn!("Unknown timezone: {tz}, falling back to UTC");
            return "UTC+0:00".to_string();
        }
    };

    let offset_seconds = chrono::Utc::now()
        .with_timezone(&parsed_tz)
        .offset()
        .fix()
        .local_minus_utc();

    let sign = if offset_seconds > 0 { '-' } else { '+' };
    let absolute_offset = offset_seconds.unsigned_abs();
    let hours = absolute_offset / 3600;
    let minutes = (absolute_offset % 3600) / 60;

    format!("UTC{sign}{hours}:{minutes:02}")
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
    fn test_set_tz_env_sets_env_var() {
        let previous = std::env::var("TZ").ok();

        set_tz_env("Asia/Shanghai");
        let current = std::env::var("TZ").unwrap();

        #[cfg(windows)]
        assert_eq!(current, super::iana_to_posix_tz("Asia/Shanghai"));
        #[cfg(not(windows))]
        assert_eq!(current, "Asia/Shanghai");

        if let Some(previous) = previous {
            unsafe { std::env::set_var("TZ", previous) };
        } else {
            unsafe { std::env::remove_var("TZ") };
        }
    }

    #[cfg(windows)]
    #[test]
    fn test_iana_to_posix_shanghai() {
        assert_eq!(super::iana_to_posix_tz("Asia/Shanghai"), "UTC-8:00");
    }

    #[cfg(windows)]
    #[test]
    fn test_iana_to_posix_utc() {
        assert_eq!(super::iana_to_posix_tz("UTC"), "UTC+0:00");
    }

    #[cfg(windows)]
    #[test]
    fn test_iana_to_posix_half_hour_offset() {
        assert_eq!(super::iana_to_posix_tz("Asia/Kolkata"), "UTC-5:30");
    }

    #[cfg(windows)]
    #[test]
    fn test_iana_to_posix_negative_offset() {
        assert_eq!(super::iana_to_posix_tz("Pacific/Apia"), "UTC-13:00");
    }

    #[cfg(windows)]
    #[test]
    fn test_unknown_fallback() {
        assert_eq!(super::iana_to_posix_tz("invalid_tz"), "UTC+0:00");
    }
}
