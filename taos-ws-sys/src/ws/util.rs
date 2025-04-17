use std::ffi::CStr;

use libc::{setlocale, LC_CTYPE};

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
}
