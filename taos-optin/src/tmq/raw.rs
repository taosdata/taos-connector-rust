pub(super) use conf::Conf;
pub(super) use list::Topics;
pub(super) use tmq::RawTmq;

pub(super) mod tmq {
    use std::{
        ffi::CStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    use taos_query::{
        prelude::tokio::sync::oneshot,
        tmq::{Assignment, VGroupId},
        RawError,
    };

    use crate::{
        into_c_str::IntoCStr,
        raw::{ApiEntry, TmqApi},
        types::{tmq_resp_err_t, tmq_t, SafeTmqT},
        RawRes, RawResult,
    };

    use super::Topics;

    #[derive(Debug)]
    pub(crate) struct RawTmq {
        api: Arc<ApiEntry>,
        tmq_api: TmqApi,
        tmq_ptr: *mut tmq_t,
        timeout: i64,
        sender: flume::Sender<oneshot::Sender<Option<RawRes>>>,
        receiver: Option<flume::Receiver<oneshot::Sender<Option<RawRes>>>>,
        has_cache: Arc<AtomicBool>,
    }

    unsafe impl Send for RawTmq {}
    unsafe impl Sync for RawTmq {}

    impl RawTmq {
        pub(crate) fn new(
            api: Arc<ApiEntry>,
            tmq_api: TmqApi,
            tmq_ptr: *mut tmq_t,
            timeout: i64,
        ) -> Self {
            let (sender, receiver) = flume::bounded(10);
            Self {
                api,
                tmq_api,
                tmq_ptr,
                timeout,
                sender,
                receiver: Some(receiver),
                has_cache: Arc::new(AtomicBool::new(false)),
            }
        }

        fn as_ptr(&self) -> *mut tmq_t {
            self.tmq_ptr
        }

        pub(crate) fn subscribe(&mut self, topics: &Topics) -> RawResult<()> {
            let rsp = unsafe { (self.tmq_api.tmq_subscribe)(self.as_ptr(), topics.as_ptr()) };
            if rsp.is_err() {
                let str =
                    unsafe { CStr::from_ptr((self.tmq_api.tmq_err2str)(rsp)) }.to_string_lossy();
                return Err(taos_query::RawError::new(rsp.0, str));
            }
            Ok(())
        }

        pub fn err_as_str(&self, tmq_resp: tmq_resp_err_t) -> String {
            unsafe {
                CStr::from_ptr((self.tmq_api.tmq_err2str)(tmq_resp))
                    .to_string_lossy()
                    .to_string()
            }
        }

        pub fn subscription(&self) -> Topics {
            let tl = Topics::new(self.tmq_api.list_api);

            unsafe { (self.tmq_api.tmq_subscription)(self.as_ptr(), &mut tl.as_ptr()) }
                .ok_or("get topic list failed")
                .expect("get topic should always success");
            tl
        }

        pub fn commit_sync(&self, msg: RawRes) -> RawResult<()> {
            unsafe { (self.tmq_api.tmq_commit_sync)(self.as_ptr(), msg.as_ptr() as _) }
                .ok_or("commit failed")
        }

        pub fn commit_offset_sync(
            &self,
            topic_name: &str,
            vgroup_id: VGroupId,
            offset: i64,
        ) -> RawResult<()> {
            if let Some(tmq_commit_offset_sync) = self.tmq_api.tmq_commit_offset_sync {
                unsafe {
                    tmq_commit_offset_sync(
                        self.as_ptr(),
                        topic_name.into_c_str().as_ptr(),
                        vgroup_id,
                        offset,
                    )
                    .ok_or("commit failed")
                }
            } else {
                unimplemented!("does not support tmq_commit_offset_sync");
            }
        }

        pub async fn commit(&self, msg: RawRes) -> RawResult<()> {
            if self.has_cache.load(Ordering::Relaxed) {
                return Ok(());
            }

            use std::sync::mpsc::{channel, Sender};
            let (sender, rx) = channel::<RawResult<()>>();
            unsafe extern "C" fn tmq_commit_async_cb(
                _tmq: *mut tmq_t,
                resp: tmq_resp_err_t,
                param: *mut std::os::raw::c_void,
            ) {
                let offsets = resp.ok_or("commit failed").map(|_| ());
                let sender = param as *mut Sender<_>;
                let sender = Box::from_raw(sender);
                tracing::trace!("commit async callback");
                sender.send(offsets).unwrap();
            }

            unsafe {
                tracing::trace!("commit async with {:p}", msg.as_ptr());
                (self.tmq_api.tmq_commit_async)(
                    self.as_ptr(),
                    msg.as_ptr(),
                    tmq_commit_async_cb,
                    Box::into_raw(Box::new(sender)) as *mut _,
                )
            }
            rx.recv().unwrap()
        }

        pub async fn commit_offset_async(
            &self,
            topic_name: &str,
            vgroup_id: VGroupId,
            offset: i64,
        ) -> RawResult<()> {
            if self.has_cache.load(Ordering::Relaxed) {
                return Ok(());
            }

            if let Some(tmq_commit_offset_async) = self.tmq_api.tmq_commit_offset_async {
                use std::sync::mpsc::{channel, Sender};
                let (sender, rx) = channel::<RawResult<()>>();
                unsafe extern "C" fn tmq_commit_offset_async_cb(
                    _tmq: *mut tmq_t,
                    resp: tmq_resp_err_t,
                    param: *mut std::os::raw::c_void,
                ) {
                    let offsets = resp.ok_or("commit offset failed").map(|_| ());
                    let sender = param as *mut Sender<_>;
                    let sender = Box::from_raw(sender);
                    tracing::trace!("commit offset async callback");
                    sender.send(offsets).unwrap();
                }

                unsafe {
                    tracing::trace!("commit offset async with {:p}", self.as_ptr());
                    (tmq_commit_offset_async)(
                        self.as_ptr(),
                        topic_name.into_c_str().as_ptr(),
                        vgroup_id,
                        offset,
                        tmq_commit_offset_async_cb,
                        Box::into_raw(Box::new(sender)) as *mut _,
                    )
                }
                rx.recv().unwrap()
            } else {
                unimplemented!("does not support tmq_commit_offset_async");
            }
        }

        pub fn poll_timeout(&self, timeout: i64) -> Option<RawRes> {
            tracing::trace!("poll next message with timeout {}", timeout);
            let res = unsafe { (self.tmq_api.tmq_consumer_poll)(self.as_ptr(), timeout) };
            if res.is_null() {
                None
            } else {
                Some(unsafe { RawRes::from_ptr_unchecked(self.api.clone(), res) })
            }
        }

        pub fn unsubscribe(&mut self) {
            unsafe {
                (self.tmq_api.tmq_unsubscribe)(self.as_ptr());
            }
        }

        pub fn get_topic_assignment(&self, topic_name: &str) -> Vec<Assignment> {
            let pt: *mut *mut Assignment = Box::into_raw(Box::new(std::ptr::null_mut()));
            if let Some(tmq_get_topic_assignment) = self.tmq_api.tmq_get_topic_assignment {
                let mut num: i32 = 0;

                let tmq_resp = unsafe {
                    tmq_get_topic_assignment(
                        self.as_ptr(),
                        topic_name.into_c_str().as_ptr(),
                        pt,
                        &mut num,
                    )
                };

                if tmq_resp.is_err() || num == 0 {
                    return vec![];
                }
                let vec = unsafe { std::slice::from_raw_parts(*pt, num as usize).to_vec() };
                unsafe {
                    self.tmq_api
                        .tmq_free_assignment
                        .expect("tmq_free_assignment not found")(*pt)
                };
                vec
            } else {
                vec![]
            }
        }

        pub fn offset_seek(
            &mut self,
            topic_name: &str,
            vgroup_id: VGroupId,
            offset: i64,
        ) -> RawResult<()> {
            let tmq_resp;
            if let Some(tmq_offset_seek) = self.tmq_api.tmq_offset_seek {
                tmq_resp = unsafe {
                    tmq_offset_seek(
                        self.as_ptr(),
                        topic_name.into_c_str().as_ptr(),
                        vgroup_id,
                        offset,
                    )
                };
            } else {
                // unimplemented!("does not support tmq_offset_seek")
                return Ok(());
            }
            tracing::trace!(
                "offset_seek tmq_resp: {:?}, topic_name: {}, vgroup_id: {}, offset: {}",
                tmq_resp,
                topic_name,
                vgroup_id,
                offset
            );

            let err_str = self.err_as_str(tmq_resp);
            tracing::trace!("offset_seek tmq_resp as str: {}", err_str);

            tmq_resp.ok_or(format!("offset seek failed: {err_str}"))
        }

        pub fn committed(&self, topic_name: &str, vgroup_id: VGroupId) -> RawResult<i64> {
            let tmq_resp;
            if let Some(tmq_committed) = self.tmq_api.tmq_committed {
                tmq_resp = unsafe {
                    tmq_committed(self.as_ptr(), topic_name.into_c_str().as_ptr(), vgroup_id)
                };
            } else {
                unimplemented!("does not support tmq_committed");
            }
            tracing::trace!(
                "committed tmq_resp: {:?}, topic_name: {}, vgroup_id: {}",
                tmq_resp,
                topic_name,
                vgroup_id
            );

            if tmq_resp.0 as i32 > 0 {
                return Ok(tmq_resp.0 as _);
            } else {
                let err_str = self.err_as_str(tmq_resp);
                tracing::trace!("committed tmq_resp err string: {}", err_str);

                return Err(RawError::new(
                    tmq_resp.0,
                    format!("get committed failed: {err_str}"),
                ));
            }
        }

        pub fn position(&self, topic_name: &str, vgroup_id: VGroupId) -> RawResult<i64> {
            let tmq_resp;
            if let Some(tmq_position) = self.tmq_api.tmq_position {
                tmq_resp = unsafe {
                    tmq_position(self.as_ptr(), topic_name.into_c_str().as_ptr(), vgroup_id)
                };
            } else {
                unimplemented!("does not support tmq_position");
            }
            tracing::trace!(
                "position tmq_resp: {:?}, topic_name: {}, vgroup_id: {}",
                tmq_resp,
                topic_name,
                vgroup_id
            );

            if tmq_resp.0 as i32 > 0 {
                return Ok(tmq_resp.0 as _);
            } else {
                let err_str = self.err_as_str(tmq_resp);
                tracing::trace!("position tmq_resp err string: {}", err_str);

                return Err(RawError::new(
                    tmq_resp.0,
                    format!("get position failed: {err_str}"),
                ));
            }
        }

        pub fn close(&mut self) {
            unsafe {
                (self.tmq_api.tmq_consumer_close)(self.as_ptr());
            }
        }

        pub(crate) fn spawn_thread(&mut self) {
            tracing::trace!("Spawn thread to call C function `tmq_consumer_poll`");

            if self.receiver.is_none() {
                return;
            }

            let receiver = self.receiver.take().unwrap();
            let safe_tmq = SafeTmqT(self.as_ptr());
            let tmq_api = self.tmq_api;
            let api = self.api.clone();
            let timeout = self.timeout;
            let has_cache = self.has_cache.clone();

            std::thread::spawn(move || {
                let safe_tmq = safe_tmq;
                let mut cache = None;
                while let Ok(sender) = receiver.recv() {
                    let elapsed = std::time::Instant::now();
                    if let Some(res) = cache.take() {
                        match sender.send(Some(res)) {
                            Ok(_) => has_cache.store(false, Ordering::Relaxed),
                            Err(res) => {
                                tracing::trace!("Receiver has been closed, cached res: {res:?}");
                                cache = res;
                            }
                        }
                        tracing::trace!(elapsed = ?elapsed.elapsed(), "Use cache, poll next message");
                        continue;
                    }

                    let tmq_ptr = safe_tmq.0;
                    tracing::trace!("Calling C function `tmq_consumer_poll` with ptr: {tmq_ptr:?}, timeout: {timeout}");
                    let res = unsafe { (tmq_api.tmq_consumer_poll)(tmq_ptr, timeout) };
                    tracing::trace!("C function `tmq_consumer_poll` returned a pointer: {res:?}");

                    if res.is_null() {
                        if let Err(_) = sender.send(None) {
                            tracing::trace!("Receiver has been closed");
                        }
                        tracing::trace!(elapsed = ?elapsed.elapsed(), "Res is null, poll next message");
                        continue;
                    }

                    let res = unsafe { RawRes::from_ptr_unchecked(api.clone(), res) };
                    if let Err(res) = sender.send(Some(res)) {
                        tracing::trace!("Receiver has been closed, cached res: {res:?}");
                        cache = res;
                        has_cache.store(true, Ordering::Relaxed);
                    }
                    tracing::trace!(elapsed = ?elapsed.elapsed(), "Poll next message");
                }
            });
        }

        pub(crate) fn tmq(&self) -> TmqApi {
            self.tmq_api
        }

        pub(crate) fn sender(&self) -> flume::Sender<oneshot::Sender<Option<RawRes>>> {
            self.sender.clone()
        }
    }
}

pub(super) mod conf {
    use taos_query::Dsn;

    use crate::*;
    use crate::{
        raw::TmqConfApi,
        types::{tmq_conf_t, tmq_t},
    };

    /* tmq conf */
    #[derive(Debug)]
    pub struct Conf {
        api: TmqConfApi,
        ptr: *mut tmq_conf_t,
    }

    impl Conf {
        pub(crate) fn as_ptr(&self) -> *mut tmq_conf_t {
            self.ptr
        }

        pub(crate) fn new(api: TmqConfApi) -> Self {
            Self {
                api,
                ptr: unsafe { api.new() },
            }
            .disable_auto_commit()
            .enable_heartbeat_background()
            .disable_snapshot()
            .enable_batch_meta()
            .with_table_name()
        }

        pub(crate) fn from_dsn(dsn: &Dsn, api: TmqConfApi) -> RawResult<Self> {
            let mut conf = Self::new(api);
            macro_rules! _set_opt {
                ($f:ident, $c:literal) => {
                    if let Some($f) = &dsn.$f {
                        conf.set(format!("td.connect.{}", $c), format!("{}", $f))?;
                    }
                };
                ($f:ident) => {
                    if let Some($f) = &dsn.$f {
                        conf.set(format!("td.connect.{}", stringify!($c)), format!("{}", $f))?;
                    }
                };
            }

            _set_opt!(username, "user");
            _set_opt!(password, "pass");
            _set_opt!(subject, "db");

            if let Some(addr) = dsn.addresses.first() {
                if let Some(host) = addr.host.as_ref() {
                    conf.set("td.connect.ip", host)?;
                }
                if let Some(port) = addr.port.as_ref() {
                    conf.set("td.connect.port", format!("{port}"))?;
                }
            }

            // todo: do explicitly param name filter.
            conf.with(dsn.params.iter().filter(|(k, _)| k.contains('.')))
        }

        pub fn disable_auto_commit(mut self) -> Self {
            self.set("enable.auto.commit", "false")
                .expect("set group.id should always be ok");
            self
        }

        pub(crate) fn enable_heartbeat_background(mut self) -> Self {
            tracing::trace!("[tmq-conf] enable heartbeat in the background");
            let _ = self.set("enable.heartbeat.background", "true");
            self
        }

        #[allow(dead_code)]
        pub(crate) fn enable_snapshot(mut self) -> Self {
            tracing::trace!("[tmq-conf] enable snapshot");
            self.set("experimental.snapshot.enable", "true")
                .expect("enable experimental snapshot");
            self
        }

        pub(crate) fn disable_snapshot(mut self) -> Self {
            self.set("experimental.snapshot.enable", "false")
                .expect("disable experimental snapshot");
            self
        }

        pub fn with_table_name(mut self) -> Self {
            tracing::trace!("set msg.with.table.name as true");
            self.set("msg.with.table.name", "true")
                .expect("set group.id should always be ok");
            self
        }

        pub fn enable_batch_meta(mut self) -> Self {
            // Safety: set enable.batch.meta as true, ignore error when not supported.
            let _ = self.set("msg.enable.batchmeta", "1");
            self
        }

        // pub fn without_table_name(mut self) -> Self {
        //     self.set("msg.with.table.name", "false")
        //         .expect("set group.id should always be ok");
        //     self
        // }

        pub(crate) fn with<K: AsRef<str>, V: AsRef<str>>(
            mut self,
            iter: impl Iterator<Item = (K, V)>,
        ) -> RawResult<Self> {
            for (k, v) in iter {
                self.set(k, v)?;
            }
            Ok(self)
        }

        fn set<K: AsRef<str>, V: AsRef<str>>(&mut self, key: K, value: V) -> RawResult<&mut Self> {
            unsafe { self.api.set(self.as_ptr(), key.as_ref(), value.as_ref()) }.map(|_| self)
        }

        // pub(crate) fn with_auto_commit_cb(&mut self, cb: tmq_commit_cb, param: *mut c_void) {
        //     unsafe {
        //         self.api.auto_commit_cb(self.as_ptr(), cb, param);
        //     }
        // }

        pub(crate) fn build(&self) -> RawResult<*mut tmq_t> {
            unsafe { self.api.consumer(self.as_ptr()) }
        }
    }

    impl Drop for Conf {
        fn drop(&mut self) {
            unsafe { self.api.destroy(self.as_ptr()) };
        }
    }
}

pub(super) mod list {
    use std::ffi::CStr;
    use std::os::raw::c_char;

    use taos_query::prelude::RawResult;

    use crate::{into_c_str::IntoCStr, raw::TmqListApi, types::tmq_list_t};

    #[derive(Debug)]
    pub(crate) struct Topics {
        api: TmqListApi,
        ptr: *mut tmq_list_t,
    }

    impl Topics {
        pub(super) fn as_ptr(&self) -> *mut tmq_list_t {
            self.ptr
        }

        pub(crate) fn new(api: TmqListApi) -> Self {
            Self {
                api,
                ptr: unsafe { api.new() },
            }
        }

        // pub(crate) fn append<'a>(&mut self, c_str: impl IntoCStr<'a>) -> Result<()> {
        //     self.api.append(self.as_ptr(), c_str)
        // }

        pub(crate) fn from_topics<'a, T: IntoCStr<'a>>(
            api: TmqListApi,
            topics: impl IntoIterator<Item = T>,
        ) -> RawResult<Self> {
            let ptr = unsafe { api.from_c_str_iter(topics)? };
            Ok(Self { api, ptr })
        }

        pub fn iter(&self) -> Iter {
            let inner = self.api.as_c_str_slice(self.as_ptr());
            let len = inner.len();
            Iter {
                len,
                inner,
                index: 0,
            }
        }

        pub fn to_strings(&self) -> Vec<String> {
            self.iter().map(|s| s.to_string()).collect()
        }
    }

    impl<'a> IntoIterator for &'a Topics {
        type Item = &'a str;

        type IntoIter = Iter<'a>;

        fn into_iter(self) -> Self::IntoIter {
            self.iter()
        }
    }

    ///
    pub struct Iter<'a> {
        inner: &'a [*mut c_char],
        len: usize,
        index: usize,
    }

    impl<'a> Iterator for Iter<'a> {
        type Item = &'a str;

        fn next(&mut self) -> Option<Self::Item> {
            self.inner
                .get(self.index)
                .map(|ptr| {
                    unsafe { CStr::from_ptr(*ptr) }
                        .to_str()
                        .expect("topic name must be valid utf8 str")
                })
                .map(|s| {
                    self.index += 1;
                    s
                })
        }
    }

    impl<'a> ExactSizeIterator for Iter<'a> {
        fn len(&self) -> usize {
            if self.len >= self.index {
                self.len - self.index
            } else {
                0
            }
        }
    }

    // todo: tmq_list_destroy cause double free error.
    impl Drop for Topics {
        fn drop(&mut self) {
            unsafe {
                self.api.destroy(self.as_ptr());
            }
        }
    }
}
