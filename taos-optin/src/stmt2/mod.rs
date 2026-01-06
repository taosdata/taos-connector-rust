use std::{
    cell::UnsafeCell,
    ffi::{c_int, c_void, CStr},
    future::Future,
    pin::Pin,
    ptr,
    rc::{Rc, Weak},
    sync::Arc,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use taos_query::{
    prelude::Code, stmt2::*, util::generate_req_id, AsyncQueryable, Queryable, RawError, RawResult,
};

use crate::{
    into_c_str::IntoCStr,
    raw::{ApiEntry, RawRes, RawTaos, Stmt2Api},
    types::{taos_async_stmt2_exec_cb, TaosStmt2Option, TAOS_RES, TAOS_STMT2},
    ResultSet,
};

#[derive(Debug)]
pub struct Stmt2 {
    raw: RawStmt2,
}

impl Stmt2Bindable<super::Taos> for Stmt2 {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        todo!()
    }

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        todo!()
    }

    fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        todo!()
    }

    fn exec(&mut self) -> RawResult<usize> {
        todo!()
    }

    fn affected_rows(&self) -> usize {
        todo!()
    }

    fn result_set(&self) -> RawResult<<super::Taos as Queryable>::ResultSet> {
        todo!()
    }
}

#[async_trait::async_trait]
impl Stmt2AsyncBindable<super::Taos> for Stmt2 {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        todo!()
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        todo!()
    }

    async fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        todo!()
    }

    async fn exec(&mut self) -> RawResult<usize> {
        todo!()
    }

    async fn affected_rows(&self) -> usize {
        todo!()
    }

    async fn result_set(&self) -> RawResult<<super::Taos as AsyncQueryable>::AsyncResultSet> {
        todo!()
    }
}

#[derive(Debug)]
pub struct RawStmt2 {
    api: Stmt2Api,
    ptr: *mut TAOS_STMT2,
    res: Option<RawRes>,
    state: Rc<UnsafeCell<Stmt2ExecState>>,
}

unsafe impl Send for RawStmt2 {}
unsafe impl Sync for RawStmt2 {}

impl Drop for RawStmt2 {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            tracing::error!("failed to close Stmt2: {err}");
        }
    }
}

impl RawStmt2 {
    #[inline]
    fn from_raw_taos(taos: &RawTaos) -> RawResult<Self> {
        let taos_stmt2_init = taos.c.stmt2.taos_stmt2_init.ok_or_else(|| {
            RawError::from_string(format!(
                "Stmt2 API missing (requires TDengine >= v3.3.5.0, current: {})",
                taos.c.version()
            ))
        })?;

        let state = Rc::new(UnsafeCell::new(Stmt2ExecState::new(taos.c.clone())));
        let userdata = Rc::into_raw(state.clone()) as *mut c_void;

        let mut option = TaosStmt2Option {
            reqid: generate_req_id() as _,
            single_stb_insert: true,
            single_table_bind_once: false,
            async_exec_fn: stmt2_exec_cb,
            userdata,
        };

        let stmt2_ptr = unsafe { taos_stmt2_init(taos.as_ptr(), &mut option) };
        if stmt2_ptr.is_null() {
            unsafe { Rc::from_raw(userdata) };
            return Err(RawError::from_string(
                taos.c.stmt2.err_as_str(ptr::null_mut()),
            ));
        }

        let s = unsafe { &mut *state.get() };
        s.stmt2_ptr = Some(stmt2_ptr);

        Ok(RawStmt2 {
            api: taos.c.stmt2,
            ptr: stmt2_ptr,
            res: None,
            state,
        })
    }

    #[inline]
    fn prepare<'c, T: IntoCStr<'c>>(&self, sql: T) -> RawResult<()> {
        let sql = sql.into_c_str();
        tracing::trace!("stmt2 prepare, sql: {sql:?}");
        self.ok(unsafe {
            (self.api.taos_stmt2_prepare.unwrap())(
                self.as_ptr(),
                sql.as_ptr(),
                sql.to_bytes().len() as _,
            )
        })
    }

    // TODO: impl bind

    #[inline]
    async fn exec(&mut self) -> RawResult<usize> {
        let state = unsafe { &mut *self.state.get() };
        state.clear();

        let fut = Stmt2ExecFuture {
            state: self.state.clone(),
        };
        let (res, affected_rows) = fut.await?;
        self.res = Some(res);
        Ok(affected_rows)
    }

    #[inline]
    fn close(&self) -> RawResult<()> {
        self.ok(unsafe { (self.api.taos_stmt2_close.unwrap())(self.as_ptr()) })
    }

    #[inline]
    fn result_set(&mut self) -> RawResult<ResultSet> {
        match self.res.take() {
            Some(res) => Ok(ResultSet::new(res)),
            None => Err(RawError::from_string("No result available from statement")),
        }
    }

    #[inline]
    const fn as_ptr(&self) -> *mut TAOS_STMT2 {
        self.ptr
    }

    #[inline(always)]
    fn ok(&self, code: impl Into<Code>) -> RawResult<()> {
        let code = code.into();
        if code.success() {
            Ok(())
        } else {
            let err = unsafe {
                let err_ptr = (self.api.taos_stmt2_error.unwrap())(self.as_ptr());
                CStr::from_ptr(err_ptr).to_string_lossy().to_string()
            };
            Err(RawError::from_string(err))
        }
    }
}

struct Stmt2ExecState {
    api: Arc<ApiEntry>,
    stmt2_ptr: Option<*mut TAOS_STMT2>,
    result: Option<Result<(RawRes, usize), RawError>>,
    waiting: bool,
    start_time: Option<Instant>,
    callback_cost: Option<Duration>,
    waker: Option<Waker>,
}

impl Stmt2ExecState {
    const fn new(api: Arc<ApiEntry>) -> Self {
        Self {
            api,
            stmt2_ptr: None,
            result: None,
            waiting: false,
            start_time: None,
            callback_cost: None,
            waker: None,
        }
    }

    fn clear(&mut self) {
        self.result = None;
        self.waiting = false;
        self.start_time = None;
        self.callback_cost = None;
        self.waker = None;
    }
}

struct Stmt2ExecFuture {
    state: Rc<UnsafeCell<Stmt2ExecState>>,
}

impl Future for Stmt2ExecFuture {
    type Output = Result<(RawRes, usize), RawError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = unsafe { &mut *self.state.get() };
        if let Some(result) = state.result.take() {
            tracing::trace!(
                "Stmt2 exec future woken {:?} after callback",
                state.start_time.unwrap().elapsed() - state.callback_cost.unwrap()
            );
            return Poll::Ready(result);
        }

        if state.waiting {
            tracing::trace!("It's waked but still waiting for stmt2 exec callback");
            return Poll::Pending;
        }

        state.waiting = true;
        state.start_time = Some(Instant::now());
        state.waker = Some(cx.waker().clone());
        state.api.stmt2.exec(state.stmt2_ptr.unwrap());
        Poll::Pending
    }
}

unsafe extern "C" fn stmt2_exec_cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
    if param.is_null() {
        tracing::error!("stmt2 exec callback param should not be null");
        return;
    }

    let state = &*(param as *mut Stmt2ExecState);
    // TODO: use Weak
    if let Some(state) = state.upgrade() {
        let s = unsafe { &mut *state.get() };
        let cost = s.start_time.elapsed();
        tracing::trace!("Received stmt2 exec callback in {:?}", cost);
        s.callback_cost.replace(cost);
        // TODO: res maybe null???
        if res.is_null() && code == 0 {
            unreachable!("stmt2 exec callback should be ok or error");
        }

        let result = if code < 0 {
            let str = s.stmt2.as_ref().map_or_else(
                || "Unknown error".to_string(),
                |stmt2| {
                    let stmt2 = stmt2.upgrade().unwrap();
                    let stmt2 = unsafe { &*stmt2.get() };
                    stmt2.err_as_str()
                },
            );
            let err = RawError::new_with_context(
                code,
                str,
                "Error while executing prepared statement".to_string(),
            );
            s.api.free_result(res);
            Err(err)
        } else {
            debug_assert!(!res.is_null());
            // TODO: check?
            assert_ne!(res as usize, 1, "res should not be 1");
            let raw_res = RawRes::from_ptr_unchecked(s.api.clone(), res);
            let affected_rows = raw_res.affected_rows() as usize;
            // s.api.free_result(res);
            Ok((raw_res, affected_rows))
        };

        s.result.replace(result);
        s.waiting = false;
        if let Some(waker) = s.waker.take() {
            waker.wake();
        }
    } else {
        tracing::trace!("Stmt2 exec callback received but no listener");
    }
}
