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
    c: Arc<ApiEntry>,
    api: Stmt2Api,
    ptr: *mut TAOS_STMT2,
    // tbname: Option<CString>,
    option: TaosStmt2Option,
    res: Option<*mut TAOS_RES>,
}

unsafe impl Send for RawStmt2 {}
unsafe impl Sync for RawStmt2 {}

impl Drop for RawStmt2 {
    fn drop(&mut self) {
        // let _ = self.close();
    }
}

impl RawStmt2 {
    // fn is_v3(&self) -> bool {
    //     self.c.version().starts_with('3')
    // }

    fn support_stmt2(&self) -> bool {
        // check version v3.3.5.0
        self.api.taos_stmt2_init.is_some()
    }

    #[inline(always)]
    fn ok(&self, code: impl Into<Code>) -> RawResult<()> {
        let code = code.into();
        if code.success() {
            Ok(())
        } else {
            Err(RawError::from_string(self.err_as_str()))
        }
    }

    #[inline]
    fn err_as_str(&self) -> String {
        unsafe {
            let err_ptr = (self.api.taos_stmt2_error.unwrap())(self.as_ptr());
            CStr::from_ptr(err_ptr).to_string_lossy().to_string()
        }
    }

    #[inline]
    fn err_as_str2(api: Stmt2Api) -> String {
        unsafe {
            let err_ptr = (api.taos_stmt2_error.unwrap())(ptr::null_mut());
            CStr::from_ptr(err_ptr).to_string_lossy().to_string()
        }
    }

    #[inline]
    fn as_ptr(&self) -> *mut TAOS_STMT2 {
        self.ptr
    }

    #[inline]
    fn from_raw_taos(taos: &RawTaos) -> RawResult<Self> {
        let init = taos.c.stmt2.taos_stmt2_init.ok_or_else(|| {
            RawError::from_string(format!(
                "STMT2 API not available (requires TDengine >= v3.3.5.0, current: {})",
                taos.c.version()
            ))
        })?;

        let mut option = TaosStmt2Option {
            reqid: generate_req_id() as i64,
            single_stb_insert: true,
            single_table_bind_once: false,
            async_exec_fn: stmt2_exec_cb,
            userdata: ptr::null_mut(), // TODO: add userdata
        };

        let stmt2_ptr = unsafe { init(taos.as_ptr(), &mut option) };
        if stmt2_ptr.is_null() {
            // TODO: check errno
            return Err(RawError::from_string(Self::err_as_str2(taos.c.stmt2)));
        }

        Ok(RawStmt2 {
            c: taos.c.clone(),
            api: taos.c.stmt2,
            ptr: stmt2_ptr,
            // tbname: None,
            option,
            res: None,
        })
    }

    #[inline]
    fn prepare<'c, T: IntoCStr<'c>>(&mut self, sql: T) -> RawResult<()> {
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
    async fn exec(&self) -> RawResult<usize> {
        self.ok(unsafe { (self.api.taos_stmt2_exec.unwrap())(self.as_ptr(), ptr::null_mut()) })?;
        // TODO: wait callback complete
        // let fut = Stmt2ExecFuture {
        //     raw: todo!(),
        //     state: todo!(),
        // };
        // fut.await?;
        Ok(0)
    }

    #[inline]
    fn close(&self) -> RawResult<()> {
        self.ok(unsafe { (self.api.taos_stmt2_close.unwrap())(self.as_ptr()) })
    }

    #[inline]
    fn use_result(&self) -> RawResult<ResultSet> {
        // TODO: remove unwrap
        unsafe { RawRes::from_ptr(self.c.clone(), self.res.unwrap()).map(ResultSet::new) }
    }
}

unsafe extern "C" fn stmt2_exec_cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
    if param.is_null() {
        tracing::error!("stmt2 exec callback param should not be null");
        return;
    }

    let param = Box::from_raw(param as *mut Stmt2ExecParam);
    if let Some(state) = param.state.upgrade() {
        let s = unsafe { &mut *state.get() };
        let cost = s.time.elapsed();
        tracing::trace!("Received stmt2 exec callback in {:?}", cost);
        s.callback_cost.replace(cost);
        // TODO: res maybe null???
        if res.is_null() && code == 0 {
            unreachable!("stmt2 exec callback should be ok or error");
        }

        let result = if code < 0 {
            let str = s.api.err_str(res);
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
            Ok(RawRes::from_ptr_unchecked(s.api.clone(), res))
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

struct Stmt2State {
    api: Arc<ApiEntry>,
    result: Option<Result<RawRes, RawError>>,
    waiting: bool,
    time: Instant,
    callback_cost: Option<Duration>,
    waker: Option<Waker>,
}

impl Stmt2State {
    fn new(api: Arc<ApiEntry>) -> Self {
        Stmt2State {
            api,
            result: None,
            waiting: false,
            time: Instant::now(),
            callback_cost: None,
            waker: None,
        }
    }
}

// auto
// impl Unpin for State {}

// unsafe impl Send for State {}
// unsafe impl Sync for State {}

struct Stmt2ExecParam {
    state: Weak<UnsafeCell<Stmt2State>>,
    // sql: *mut c_char,
    // waker: Waker,
}

struct Stmt2ExecFuture {
    // stmt2_exec_cb
    // cb_fn: taos_async_stmt2_exec_cb,
    raw: RawTaos,
    // sql: Cow<'a, CStr>,
    state: Rc<UnsafeCell<Stmt2State>>,
}

// auto
// impl Unpin for Stmt2ExecFuture {}

// unsafe impl Send for Stmt2ExecFuture {}

impl Future for Stmt2ExecFuture {
    type Output = Result<RawRes, RawError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // let this = unsafe { self.get_unchecked_mut() };
        // let state = *this.state.get();
        let state = unsafe { &mut *self.state.get() };
        if let Some(result) = state.result.take() {
            // TODO: optimize log
            tracing::trace!(
                "Waken {:?} after callback received",
                state.time.elapsed() - state.callback_cost.unwrap()
            );
            return Poll::Ready(result);
        }

        // if state.waiting {
        //     tracing::trace!("It's waked but still waiting for stmt2 exec callback");
        //     return Poll::Pending;
        // }

        // state.waiting = true;
        // TODO: lock?
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
