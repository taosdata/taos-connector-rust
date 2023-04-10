use std::borrow::Cow;
use std::cell::UnsafeCell;

use std::ffi::CStr;
use std::future::Future;
use std::os::raw::{c_int, c_void};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::ffi::{TAOS_RES, taos_free_result, taos_errstr};
use crate::into_c_str::IntoCStr;
use crate::{RawRes, RawTaos};
use taos_query::prelude::RawError;

pub struct QueryFuture<'a> {
    raw: RawTaos,
    sql: Cow<'a, CStr>,
    state: Arc<UnsafeCell<State>>,
}

unsafe impl<'a> Send for QueryFuture<'a> {}

/// Shared state between the future and the waiting thread
struct State {
    result: Option<Result<RawRes, RawError>>,
    done: bool,
    waiting: bool,
    time: Instant,
    callback_cost: Option<Duration>,
}

unsafe impl Send for State {}
unsafe impl Sync for State {}

impl State {
    pub fn new() -> Self {
        Self {
            result: None,
            done: false,
            waiting: false,
            time: Instant::now(),
            callback_cost: None,
        }
    }
}
impl Unpin for State {}
impl<'a> Unpin for QueryFuture<'a> {}
impl<'a> Future for QueryFuture<'a> {
    type Output = Result<RawRes, RawError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ptr = self.state.get();
        let state = unsafe { &mut *self.state.get() };
        if state.done {
            let d = state.time.elapsed();
            log::debug!(
                "Waken {:?} after callback received",
                d - state.callback_cost.unwrap()
            );
            Poll::Ready(state.result.take().unwrap())
        } else {
            #[no_mangle]
            unsafe extern "C" fn taos_sys_async_query_callback(
                param: *mut c_void,
                res: *mut TAOS_RES,
                code: c_int,
            ) {
                let param = param as *mut (Arc<UnsafeCell<State>>, Waker);
                let state = param.read();
                let mut s = { &mut *state.0.get() };
                let cost = s.time.elapsed();
                log::debug!("Received query callback in {:?}", cost);
                s.callback_cost.replace(cost);
                if res.is_null() && code == 0 {
                    unreachable!("query callback should be ok or error");
                }
                if (code & 0xffff) == 0x032C {
                    log::warn!("Received 0x032C (Object is creating) error, retry");
                    s.waiting = false;
                    taos_free_result(res);
                    state.1.wake();
                    return;
                }

                let result = if code < 0 {
                    let ptr = taos_errstr(res);
                    taos_free_result(res);
                    let err = RawError::new(code, CStr::from_ptr(ptr).to_string_lossy());
                    Err(err)
                } else {
                    Ok(RawRes::from_ptr_unchecked(res))
                };

                s.result.replace(result);
                s.done = true;
                s.waiting = false;
                state.1.wake();
            }

            let param = Box::new((self.state.clone(), cx.waker().clone()));

            self.raw.query_a(
                self.sql.as_ref(),
                taos_sys_async_query_callback as _,
                Box::into_raw(param) as *mut _,
            );
            Poll::Pending
        }
    }
}
impl<'a> QueryFuture<'a> {
    /// Create a new `TimerFuture` which will complete after the provided
    /// timeout.
    pub fn new(taos: RawTaos, sql: impl IntoCStr<'a>) -> Self {
        let state = Arc::new(UnsafeCell::new(State::new()));

        let sql = sql.into_c_str();
        // log::trace!("async query with sql: {:?}", sql);

        QueryFuture {
            raw: taos,
            sql,
            state,
        }
    }
}
