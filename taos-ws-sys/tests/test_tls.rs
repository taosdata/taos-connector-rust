#[cfg(feature = "test-tls")]
mod tests {
    use std::ffi::{c_char, CStr, CString};
    use std::ptr;

    use taosws::ws::error::{taos_errno, taos_errstr};
    use taosws::ws::query::*;
    use taosws::ws::tmq::*;
    use taosws::ws::{taos_close, taos_connect, taos_options, TAOS, TSDB_OPTION};

    #[test]
    fn test_taos_connect() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1762409610",
                    "create database test_1762409610",
                    "use test_1762409610",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let res = taos_query(taos, c"insert into t0 values (now, 1)".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 1);

            taos_free_result(res);
            test_exec(taos, "drop database test_1762409610");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_connect_failed() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let taos = taos_connect(
                c"localhost".as_ptr(),
                c"root".as_ptr(),
                c"taosdata".as_ptr(),
                ptr::null(),
                6041,
            );
            assert!(taos.is_null());

            let code = taos_errno(taos);
            assert_eq!(code, 0x8000000Bu32 as i32);

            let err = taos_errstr(taos);
            let err_str = CStr::from_ptr(err).to_str().unwrap();
            assert_eq!(err_str, "Unable to establish connection");
        }
    }

    #[test]
    fn test_tmq_connect() {
        unsafe {
            let code = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                c"./tests/cfg/test_tls.cfg".as_ptr() as _,
            );
            assert_eq!(code, 0);

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1762410862",
                    "drop database if exists test_1762410862",
                    "create database test_1762410862",
                    "create topic topic_1762410862 as database test_1762410862",
                    "use test_1762410862",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1753174694276, 1)",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"td.connect.port".as_ptr();
            let value = c"6443".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"group.id".as_ptr();
            let value = c"1001".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"topic_1762410862";
            let code = tmq_list_append(list, topic.as_ptr());
            assert_eq!(code, 0);

            let code = tmq_subscribe(consumer, list);
            assert_eq!(code, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let mut running = false;

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                tracing::debug!("poll res: {res:?}");
                if res.is_null() {
                    break;
                }

                running = true;

                let fields = taos_fetch_fields(res);
                assert!(!fields.is_null());

                let num_fields = taos_num_fields(res);
                assert_eq!(num_fields, 2);

                let row = taos_fetch_row(res);
                assert!(!row.is_null());

                let mut str = vec![0 as c_char; 1024];
                let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                assert_eq!(
                    CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                    "1753174694276 1"
                );

                let code = tmq_commit_sync(consumer, res);
                assert_eq!(code, 0);

                taos_free_result(res);
            }

            assert!(running);

            let code = tmq_unsubscribe(consumer);
            assert_eq!(code, 0);

            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic topic_1762410862",
                    "drop database test_1762410862",
                ],
            );

            taos_close(taos);
        }
    }

    unsafe fn test_connect() -> *mut TAOS {
        let taos = taos_connect(
            c"localhost".as_ptr(),
            c"root".as_ptr(),
            c"taosdata".as_ptr(),
            ptr::null(),
            6443,
        );
        assert!(!taos.is_null());
        taos
    }

    unsafe fn test_exec<S: AsRef<str>>(taos: *mut TAOS, sql: S) {
        let sql = CString::new(sql.as_ref()).unwrap();
        let res = taos_query(taos, sql.as_ptr());
        if res.is_null() {
            let code = taos_errno(res);
            let err = taos_errstr(res);
            println!("code: {}, err: {:?}", code, CStr::from_ptr(err));
        }
        assert!(!res.is_null());
        taos_free_result(res);
    }

    unsafe fn test_exec_many<T, S>(taos: *mut TAOS, sqls: S)
    where
        T: AsRef<str>,
        S: IntoIterator<Item = T>,
    {
        for sql in sqls {
            test_exec(taos, sql);
        }
    }
}
