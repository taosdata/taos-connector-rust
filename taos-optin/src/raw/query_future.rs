use std::borrow::Cow;
use std::cell::UnsafeCell;

use std::ffi::CStr;
use std::future::Future;
use std::os::raw::{c_int, c_void};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use crate::into_c_str::IntoCStr;
use crate::types::TAOS_RES;
use crate::{RawRes, RawTaos};
use taos_query::prelude::RawError;

pub struct QueryFuture<'a> {
    raw: RawTaos,
    sql: Cow<'a, CStr>,
    state: UnsafeCell<State>,
}

/// Shared state between the future and the waiting thread
struct State {
    result: *mut TAOS_RES,
    code: i32,
    done: bool,
    waiting: bool,
    time: Instant,
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
            Poll::Ready(RawRes::from_ptr_with_code(
                self.raw.c.clone(),
                state.result,
                state.code.into(),
            ))
        } else {
            if state.waiting {
                log::trace!("waken, still waiting for taos_query_a callback.");
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
                let param = param as *mut (&UnsafeCell<State>, Waker);
                let state = param.read();
                let mut s = { &mut *state.0.get() };
                log::debug!("Receive query callback in {:?}", s.time.elapsed());

                s.result = res;
                s.code = code;
                s.done = true;
                state.1.wake();
            }

            let param = Box::new((&self.state, cx.waker().clone()));
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
        let state = UnsafeCell::new(State {
            result: std::ptr::null_mut(),
            code: 0,
            done: false,
            waiting: false,
            time: Instant::now(),
        });
        let sql = sql.into_c_str();
        log::trace!("query with: {}", sql.to_str().unwrap_or("<...>"));

        QueryFuture {
            raw: taos,
            sql,
            state,
        }
    }
}
