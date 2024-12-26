use std::cell::RefCell;
use std::ffi::{c_char, c_int, CStr};

use taos_error::Error;

use super::TAOS_RES;

#[no_mangle]
pub extern "C" fn taos_errno(res: *mut TAOS_RES) -> c_int {
    if res.is_null() {
        return errno();
    }
    todo!()
}

#[no_mangle]
pub extern "C" fn taos_errstr(res: *mut TAOS_RES) -> *const c_char {
    if res.is_null() {
        if errno() == 0 {
            return EMPTY.as_ptr();
        }
        return errstr();
    }
    todo!()
}

const EMPTY: &CStr = c"";
const MAX_ERRSTR_LEN: usize = 4096;

thread_local! {
    static ERRNO: RefCell<i32> = const { RefCell::new(0) };
    static ERRSTR: RefCell<[u8; MAX_ERRSTR_LEN]> = const { RefCell::new([0; MAX_ERRSTR_LEN]) };
}

pub fn set_err_and_get_code(err: Error) -> i32 {
    ERRNO.with(|errno| {
        let code: u32 = err.code().into();
        *errno.borrow_mut() = (code | 0x80000000) as i32;
    });

    ERRSTR.with(|errstr| {
        let bytes = err.message().into_bytes();
        let len = bytes.len().min(MAX_ERRSTR_LEN - 1);
        let mut errstr = errstr.borrow_mut();
        errstr[..len].copy_from_slice(&bytes[..len]);
        errstr[len] = 0;
    });

    errno()
}

pub fn errno() -> i32 {
    ERRNO.with(|errno| *errno.borrow())
}

pub fn errstr() -> *const c_char {
    ERRSTR.with(|errstr| errstr.borrow().as_ptr() as _)
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr::null_mut;

    use taos_error::Error;

    use super::*;

    #[test]
    fn test_set_err_and_get_code() {
        let err = Error::new(1, "test error");
        let code = set_err_and_get_code(err);
        assert_eq!(code as u32, 0x80000001);

        let code = taos_errno(null_mut());
        assert_eq!(code as u32, 0x80000001);

        let errstr_ptr = taos_errstr(null_mut());
        let errstr = unsafe { CStr::from_ptr(errstr_ptr) };
        assert_eq!(errstr, c"Internal error: `test error`"); // todo
    }
}
