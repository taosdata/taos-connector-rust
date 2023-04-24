use std::borrow::Cow;
use std::cell::UnsafeCell;

use std::ffi::CStr;
use std::future::Future;
use std::os::raw::{c_int, c_void};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::into_c_str::IntoCStr;
use crate::types::TAOS_RES;
use crate::{RawRes, RawTaos};
use taos_query::prelude::RawError;

use super::ApiEntry;

pub struct QueryFuture<'a> {
    raw: RawTaos,
    sql: Cow<'a, CStr>,
    state: Arc<UnsafeCell<State>>,
}

unsafe impl<'a> Send for QueryFuture<'a> {}

/// Shared state between the future and the waiting thread
struct State {
    api: Arc<ApiEntry>,
    result: Option<Result<RawRes, RawError>>,
    done: bool,
    waiting: bool,
    time: Instant,
    callback_cost: Option<Duration>,
}

impl State {
    pub fn new(api: Arc<ApiEntry>) -> Self {
        State {
            api: api.clone(),
            result: None,
            done: false,
            waiting: false,
            time: Instant::now(),
            callback_cost: None,
        }
    }
}

unsafe impl Send for State {}
unsafe impl Sync for State {}

impl Unpin for State {}
impl<'a> Unpin for QueryFuture<'a> {}
impl<'a> Future for QueryFuture<'a> {
    type Output = Result<RawRes, RawError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = unsafe { &mut *self.state.get() };

        if state.done {
            let d = state.time.elapsed();
            log::debug!(
                "Waken {:?} after callback received",
                d - state.callback_cost.unwrap()
            );
            Poll::Ready(state.result.take().unwrap())
        } else {
            if state.waiting {
                log::trace!("It's waked but still waiting for taos_query_a callback.");
                return Poll::Pending;
            } else {
                state.waiting = true;
            }
            #[no_mangle]
            unsafe extern "C" fn taos_optin_query_future_callback(
                param: *mut c_void,
                res: *mut TAOS_RES,
                code: c_int,
            ) {
                let param = Box::from_raw(param as *mut (Arc<UnsafeCell<State>>, Waker));
                // let state = param.read();
                let mut s = { &mut *param.0.get() };
                let cost = s.time.elapsed();
                log::debug!("Received query callback in {:?}", cost);
                s.callback_cost.replace(cost);
                if res.is_null() && code == 0 {
                    unreachable!("query callback should be ok or error");
                }
                if (code & 0xffff) == 0x032C {
                    log::warn!("Received 0x032C (Object is creating) error, retry");
                    s.waiting = false;
                    (s.api.taos_free_result)(res);
                    param.1.wake();
                    return;
                }

                let result = if code < 0 {
                    let ptr = (s.api.taos_errstr)(res);
                    (s.api.taos_free_result)(res);
                    let err = RawError::new(code, CStr::from_ptr(ptr).to_string_lossy());
                    Err(err)
                } else {
                    Ok(RawRes::from_ptr_unchecked(s.api.clone(), res))
                };

                s.result.replace(result);
                s.done = true;
                s.waiting = false;
                param.1.wake();
            }

            let param = Box::new((self.state.clone(), cx.waker().clone()));
            log::trace!("calling taos_query_a");
            self.raw.query_a(
                self.sql.as_ref(),
                taos_optin_query_future_callback as _,
                Box::into_raw(param) as *mut _,
            );
            log::trace!("waiting taos_query_a callback");
            Poll::Pending
        }
    }
}
impl<'a> QueryFuture<'a> {
    /// Create a new `TimerFuture` which will complete after the provided
    /// timeout.
    pub fn new(taos: RawTaos, sql: impl IntoCStr<'a>) -> Self {
        let state = Arc::new(UnsafeCell::new(State::new(taos.c.clone())));
        let sql = sql.into_c_str();
        log::trace!("query with: {}", sql.to_str().unwrap_or("<...>"));

        QueryFuture {
            raw: taos,
            sql,
            state,
        }
    }
}
