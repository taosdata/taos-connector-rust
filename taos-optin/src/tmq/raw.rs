pub(super) use conf::Conf;
pub(super) use list::Topics;
pub(super) use tmq::RawTmq;

pub(super) mod tmq {
    use std::{sync::Arc, time::Duration};

    use itertools::Itertools;

    use crate::{
        raw::{ApiEntry, TmqApi},
        types::{tmq_resp_err_t, tmq_t},
        RawError, RawRes,
    };

    use super::Topics;

    #[derive(Debug, Clone)]
    pub(crate) struct RawTmq {
        pub(crate) c: Arc<ApiEntry>,
        pub(crate) tmq: TmqApi,
        pub(crate) ptr: *mut tmq_t,
    }

    unsafe impl Send for RawTmq {}
    unsafe impl Sync for RawTmq {}

    impl RawTmq {
        fn as_ptr(&self) -> *mut tmq_t {
            self.ptr
        }
        pub(crate) fn subscribe(&mut self, topics: &Topics) -> Result<(), RawError> {
            unsafe {
                (self.tmq.tmq_subscribe)(self.as_ptr(), topics.as_ptr()).ok_or(format!(
                    "subscribe failed with topics: [{}]",
                    topics.iter().join(",")
                ))
            }
        }

        pub fn commit_sync(&self, msg: RawRes) -> Result<(), RawError> {
            unsafe { (self.tmq.tmq_commit_sync)(self.as_ptr(), msg.as_ptr() as _) }
                .ok_or("commit failed")
        }

        pub async fn commit(&self, msg: RawRes) -> Result<(), RawError> {
            // use tokio::sync::oneshot::{channel, Sender};
            use std::sync::mpsc::{channel, Sender};
            let (sender, rx) = channel::<Result<(), RawError>>();
            unsafe extern "C" fn tmq_commit_async_cb(
                _tmq: *mut tmq_t,
                resp: tmq_resp_err_t,
                param: *mut std::os::raw::c_void,
            ) {
                let offsets = resp.ok_or("commit failed").map(|_| ());
                let sender = param as *mut Sender<_>;
                let sender = Box::from_raw(sender);
                log::trace!("commit async callback");
                sender.send(offsets).unwrap();
            }

            unsafe {
                log::trace!("commit async with {:p}", msg.as_ptr());
                (self.tmq.tmq_commit_async)(
                    self.as_ptr(),
                    msg.as_ptr(),
                    tmq_commit_async_cb,
                    Box::into_raw(Box::new(sender)) as *mut _,
                )
            }
            rx.recv().unwrap()
        }

        pub fn poll_timeout(&self, timeout: i64) -> Option<RawRes> {
            log::trace!("poll next message with timeout {}", timeout);
            let res = unsafe { (self.tmq.tmq_consumer_poll)(self.as_ptr(), timeout) };
            if res.is_null() {
                None
            } else {
                Some(unsafe { RawRes::from_ptr_unchecked(self.c.clone(), res) })
            }
        }

        pub async fn poll_async(&self) -> RawRes {
            let elapsed = std::time::Instant::now();
            #[cfg(not(test))]
            use taos_query::prelude::tokio;

            loop {
                // poll with 50ms timeout.
                // let ptr = UnsafeCell::new(self.0);
                log::trace!("try poll next message with 200ms timeout");
                let raw = self.clone();
                let res = tokio::task::spawn_blocking(move || {
                    let raw = raw;
                    let res = unsafe { (raw.tmq.tmq_consumer_poll)(raw.as_ptr(), 200) };
                    if res.is_null() {
                        None
                    } else {
                        Some(unsafe { RawRes::from_ptr_unchecked(raw.c.clone(), res) })
                    }
                    // result
                })
                .await
                .unwrap_or_default();
                if let Some(res) = res {
                    log::trace!("received tmq message in {:?}", elapsed.elapsed());
                    break res;
                } else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }

        pub fn unsubscribe(&mut self) {
            unsafe {
                log::trace!("unsubscribe {:p}", self.as_ptr());
                (self.tmq.tmq_unsubscribe)(self.as_ptr());
                log::trace!("consumer closed safely");
            }
        }

        pub fn close(&mut self) {
            unsafe {
                (self.tmq.tmq_consumer_close)(self.as_ptr());
            }
        }
    }
}

pub(super) mod conf {
    use crate::{
        raw::TmqConfApi,
        types::{tmq_conf_t, tmq_t},
    };
    // use taos_error::*;

    use crate::*;
    use std::iter::Iterator;
    use taos_query::Dsn;

    /* tmq conf */
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
            .enable_snapshot()
            .with_table_name()
        }

        pub(crate) fn from_dsn(dsn: &Dsn, api: TmqConfApi) -> Result<Self, RawError> {
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
            log::trace!("[tmq-conf] enable heartbeat in the background");
            let _ = self.set("enable.heartbeat.background", "true");
            self
        }

        pub(crate) fn enable_snapshot(mut self) -> Self {
            log::trace!("[tmq-conf] enable snapshot");
            self.set("experimental.snapshot.enable", "true")
                .expect("enable experimental snapshot");
            self
        }

        // pub fn disable_snapshot(mut self) -> Self {
        //     self.set("experimental.snapshot.enable", "false")
        //         .expect("enable experimental snapshot");
        //     self
        // }

        pub fn with_table_name(mut self) -> Self {
            log::trace!("set msg.with.table.name as true");
            self.set("msg.with.table.name", "true")
                .expect("set group.id should always be ok");
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
        ) -> Result<Self, RawError> {
            for (k, v) in iter {
                self.set(k, v)?;
            }
            Ok(self)
        }

        fn set<K: AsRef<str>, V: AsRef<str>>(
            &mut self,
            key: K,
            value: V,
        ) -> Result<&mut Self, RawError> {
            unsafe { self.api.set(self.as_ptr(), key.as_ref(), value.as_ref()) }.map(|_| self)
        }

        // pub(crate) fn with_auto_commit_cb(&mut self, cb: tmq_commit_cb, param: *mut c_void) {
        //     unsafe {
        //         self.api.auto_commit_cb(self.as_ptr(), cb, param);
        //     }
        // }

        pub(crate) fn build(&self) -> Result<*mut tmq_t, RawError> {
            unsafe { self.api.consumer(self.as_ptr()) }
        }
    }

    impl Drop for Conf {
        fn drop(&mut self) {
            log::trace!("tmq config destroy");
            unsafe { self.api.destroy(self.as_ptr()) };
            log::trace!("tmq config destroyed safely");
        }
    }
}

pub(super) mod list {
    use std::ffi::CStr;
    use std::os::raw::c_char;

    use taos_query::prelude::RawError;

    use crate::{into_c_str::IntoCStr, raw::TmqListApi, types::tmq_list_t};

    type Result<T> = std::result::Result<T, RawError>;

    #[derive(Debug)]
    pub(crate) struct Topics {
        api: TmqListApi,
        ptr: *mut tmq_list_t,
    }

    impl Topics {
        pub(super) fn as_ptr(&self) -> *mut tmq_list_t {
            self.ptr
        }
        // pub(crate) fn new(api: TmqListApi) -> Self {
        //     Self {
        //         api,
        //         ptr: unsafe { api.new() },
        //     }
        // }

        // pub(crate) fn append<'a>(&mut self, c_str: impl IntoCStr<'a>) -> Result<()> {
        //     self.api.append(self.as_ptr(), c_str)
        // }

        pub(crate) fn from_topics<'a, T: IntoCStr<'a>>(
            api: TmqListApi,
            topics: impl IntoIterator<Item = T>,
        ) -> Result<Self> {
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

        // pub fn to_strings(&self) -> Vec<String> {
        //     self.iter().map(|s| s.to_string()).collect()
        // }
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
                log::trace!("list destroy");
                self.api.destroy(self.as_ptr());
                log::trace!("list destroy destroyed");
            }
        }
    }
}
