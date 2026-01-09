use std::{
    cell::UnsafeCell,
    ffi::{c_int, c_void, CStr},
    future::Future,
    pin::Pin,
    ptr,
    rc::Rc,
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
    types::{TaosStmt2Option, TAOS_RES, TAOS_STMT2},
    ResultSet,
};

mod bind;

#[derive(Debug)]
pub struct Stmt2 {
    raw: RawStmt2,
}

impl Stmt2Bindable<super::Taos> for Stmt2 {
    fn init(taos: &super::Taos) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt2::from_raw_taos(&taos.raw)?,
        })
    }

    fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.raw.prepare(sql)?;
        Ok(self)
    }

    fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        todo!()
    }

    fn exec(&mut self) -> RawResult<usize> {
        taos_query::block_in_place_or_global(self.raw.exec())
    }

    fn affected_rows(&self) -> usize {
        todo!()
    }

    fn result_set(&self) -> RawResult<<super::Taos as Queryable>::ResultSet> {
        self.raw.result_set()
    }
}

#[async_trait::async_trait]
impl Stmt2AsyncBindable<super::Taos> for Stmt2 {
    async fn init(taos: &super::Taos) -> RawResult<Self> {
        Ok(Self {
            raw: RawStmt2::from_raw_taos(&taos.raw)?,
        })
    }

    async fn prepare(&mut self, sql: &str) -> RawResult<&mut Self> {
        self.raw.prepare(sql)?;
        Ok(self)
    }

    async fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self> {
        todo!()
    }

    async fn exec(&mut self) -> RawResult<usize> {
        self.raw.exec().await
    }

    async fn affected_rows(&self) -> usize {
        todo!()
    }

    async fn result_set(&self) -> RawResult<<super::Taos as AsyncQueryable>::AsyncResultSet> {
        self.raw.result_set()
    }
}

#[derive(Debug)]
pub struct RawStmt2 {
    api: Stmt2Api,
    ptr: *mut TAOS_STMT2,
    res: UnsafeCell<Option<RawRes>>,
    state: Rc<UnsafeCell<Stmt2ExecState>>,
}

unsafe impl Send for RawStmt2 {}
unsafe impl Sync for RawStmt2 {}

impl Drop for RawStmt2 {
    fn drop(&mut self) {
        if let Err(err) = self.close() {
            tracing::error!("failed to close Stmt2: {err}");
        }
        unsafe {
            if let Some(mut res) = (&mut *self.res.get()).take() {
                res.free_result();
            }
        }
    }
}

impl RawStmt2 {
    fn from_raw_taos(taos: &RawTaos) -> RawResult<Self> {
        let taos_stmt2_init = taos.c.stmt2.taos_stmt2_init.ok_or_else(|| {
            RawError::from_string(format!(
                "Stmt2 API missing (requires TDengine >= v3.3.6.0, current: {})",
                taos.c.version()
            ))
        })?;

        let state = Rc::new(UnsafeCell::new(Stmt2ExecState::new(taos.c.clone())));
        let userdata = Rc::as_ptr(&state) as *mut c_void;

        let mut option = TaosStmt2Option {
            reqid: generate_req_id() as _,
            single_stb_insert: true,
            single_table_bind_once: false,
            async_exec_fn: stmt2_exec_cb,
            userdata,
        };

        let stmt2_ptr = unsafe { taos_stmt2_init(taos.as_ptr(), &mut option) };
        if stmt2_ptr.is_null() {
            return Err(RawError::from_string(
                taos.c.stmt2.err_as_str(ptr::null_mut()),
            ));
        }

        let s = unsafe { &mut *state.get() };
        s.stmt2_ptr = Some(stmt2_ptr);

        Ok(RawStmt2 {
            api: taos.c.stmt2,
            ptr: stmt2_ptr,
            res: UnsafeCell::new(None),
            state,
        })
    }

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

    fn bind(&self, params: &[Stmt2BindParam]) -> RawResult<()> {
        let mut bindv_owned = bind::build_bindv(params)?;
        self.ok(unsafe {
            (self.api.taos_stmt2_bind_param.unwrap())(self.as_ptr(), &mut bindv_owned.bindv, -1)
        })
    }

    async fn exec(&mut self) -> RawResult<usize> {
        let state = unsafe { &mut *self.state.get() };
        state.clear();

        let fut = Stmt2ExecFuture {
            state: self.state.clone(),
        };
        let (affected_rows, res) = fut.await?;
        unsafe { *self.res.get() = Some(res) };
        Ok(affected_rows)
    }

    fn close(&self) -> RawResult<()> {
        self.ok(unsafe { (self.api.taos_stmt2_close.unwrap())(self.as_ptr()) })
    }

    fn result_set(&self) -> RawResult<ResultSet> {
        let slot = unsafe { &mut *self.res.get() };
        match slot.take() {
            Some(res) => Ok(ResultSet::new(res)),
            None => Err(RawError::from_string("No result available from statement")),
        }
    }

    fn ok(&self, code: impl Into<Code>) -> RawResult<()> {
        let code = code.into();
        if code.success() {
            Ok(())
        } else {
            Err(RawError::from_string(self.api.err_as_str(self.as_ptr())))
        }
    }

    const fn as_ptr(&self) -> *mut TAOS_STMT2 {
        self.ptr
    }
}

struct Stmt2ExecState {
    api: Arc<ApiEntry>,
    stmt2_ptr: Option<*mut TAOS_STMT2>,
    result: Option<Result<(usize, RawRes), RawError>>,
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

unsafe impl Send for Stmt2ExecFuture {}

impl Future for Stmt2ExecFuture {
    type Output = Result<(usize, RawRes), RawError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = unsafe { &mut *self.state.get() };
        if let Some(result) = state.result.take() {
            tracing::trace!(
                elapsed = ?state.start_time.unwrap().elapsed() - state.callback_cost.unwrap(),
                "Stmt2 exec future woken after callback",
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

        if let Err(err) = state.api.stmt2.exec(state.stmt2_ptr.unwrap()) {
            tracing::error!(
                err = %err,
                stmt2_ptr = ?state.stmt2_ptr.unwrap(),
                "Failed to start stmt2 exec async"
            );
            return Poll::Ready(Err(err));
        }

        Poll::Pending
    }
}

unsafe extern "C" fn stmt2_exec_cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
    if res.is_null() && code == 0 {
        unreachable!("Stmt2 exec callback should be ok or error");
    }

    if param.is_null() {
        tracing::error!("Stmt2 exec callback param should not be null");
        return;
    }

    let cell = &*(param as *const UnsafeCell<Stmt2ExecState>);
    let state = unsafe { &mut *cell.get() };
    let elapsed = state.start_time.unwrap().elapsed();
    tracing::trace!(elapsed = ?elapsed, "Received stmt2 exec callback");
    state.callback_cost.replace(elapsed);

    let result = if code < 0 {
        state.api.free_result(res);
        let err = state.api.stmt2.err_as_str(state.stmt2_ptr.unwrap());
        Err(RawError::new(code, err))
    } else {
        debug_assert!(!res.is_null());
        assert_ne!(res as usize, 1, "res should not be 1");
        let raw_res = RawRes::from_ptr_unchecked(state.api.clone(), res);
        let affected_rows = raw_res.affected_rows() as usize;
        Ok((affected_rows, raw_res))
    };

    state.result.replace(result);
    state.waiting = false;
    if let Some(waker) = state.waker.take() {
        waker.wake();
    }
}
