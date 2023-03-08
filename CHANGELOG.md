# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [0.5.0] - 2023-03-08

### Bug Fixes

- *(pool)* Set connection timeout default to 5s
- *(sys)* Remove log::debug! macro in query future
- *(ws)* Unimpleted type when message_type = 3- Retry write meta when got code 0x2603
- Fix test error
- Impl to_string for bool type
- Fix taos_query_a called twice
- Optin server edition check
- Fix macos compile error
- Fix edition version pattern (cloud)
- Macos compile error in optin feature
- Fix async block called twice
- Force Send/Sync for column view
- Fix double free when execute set_tag before bind (#142)
- Fix warnings and syntax error after 3.0.3.0


### Enhancements
- Use mimimum features set of tokio instead of full


### Features
- Do not move metga when use write_raw_meta.
- Pretty print block with prettytable-rs
- Expose taos-query at root of taos
- Add sesrver_version() and is_enterprise_edition()


## [taos-query-v0.3.14] - 2023-01-16

### Bug Fixes

- *(optin)* Fix memory leak in stmt bind
- *(py)* Fix token parsing for cloud
- *(py)* Fix query error with cloud token
- *(sys)* Fix async runtime error under v2
- *(sys)* Fix memory leak in stmt bind- Fix unaligned pointer to support sanitizer build
- Stmt mem leak for binary/nchar bind


### Features
- Use rustls with native roots by default


## [taos-ws-v0.3.17] - 2023-01-08

### Bug Fixes
- Fix create topic error
- Fix create topic error for older version
- Fix slice of null bits error
- Add a macro to prepare fixing typo


### Features

- *(taos-ws-py)* Support multiple python version by abc3-py37- Taosws PEP-249 fully support
- Support column view slice


### Testing
- Fix sql syntax error


### Dep
- Taos-query v0.3.13
- Update taos-optin v0.1.10
- Use taos-ws v0.3.17


## [taos-ws-v0.3.16] - 2022-12-24

### Bug Fixes
- Default max pool size to 5000
- Fix memory leak when query error


### Enhancements
- Support delete meta in json


## [taos-sys-v0.3.10] - 2022-12-21

### Bug Fixes
- Fix complie errors in sanitizer mode
- Fix tmq_free_raw lost that causes mem leak
- Fix mem leak in taos_query_a callback
- Fix DSN parsing with localhost and specified port


### Examples
- Add example for database creation


### Features
- Support write_raw_block_with_fields
- Add server_version method for connection object


## [taos-ws-v0.3.15] - 2022-12-10

### Bug Fixes
- Fix test error in CI
- Support api changes since 3.0.2.0


## [taos-ws-v0.3.14] - 2022-12-09

### Bug Fixes
- Support write_raw_block_with_fields action


## [taos-v0.4.14] - 2022-12-09

### Bug Fixes
- Use dlopen2 instead of libloading


## [taos-v0.4.13] - 2022-12-09

### Bug Fixes
- Fix r2d2 timedout error with default pool options
- Use spawn_blocking for tmq poll async


## [taos-v0.4.12] - 2022-12-06

### Bug Fixes
- Produce exlicit error for write_raw_meta in 2.x


### Features

- *(mdsn)* Support percent encoding of password

## [taos-ws-v0.3.13] - 2022-12-01

### Bug Fixes
- Support utf-8 table names in describe


## [taos-query-v0.3.7] - 2022-12-01

### Bug Fixes
- Support `?key`-like params
- Support special database names in use_database method
- Fix coredump when optin dropped


### Features
- Expose TaosPool type alias for r2d2 pool


## [taos-sys-v0.3.7] - 2022-11-30

### Bug Fixes

- *(sys)* Fix compile error when _raw_block_with_fields not avalible

## [taos-ws-v0.3.12] - 2022-11-29

### Bug Fixes
- Use from_timestamp_opt for the deprecation warning of chrono
- Use new write_raw_block_with_fields API if avaliable


### Enhancements
- Expose fields method to public for RawBlock


## [taos-ws-v0.3.11] - 2022-11-22

### Bug Fixes
- Remove simd-json for mips64 compatibility


### Enhancements

- *(ws)* Add features `rustls`/`native-tls` for ssl libs

### Testing
- Add test case for TS-2035
- Add test case for partial update block


## [taos-ws-v0.3.10] - 2022-11-16

### Bug Fixes

- *(ws)* Fix websocket handout in release build

## [taos-ws-v0.3.9] - 2022-11-11

### Bug Fixes

- *(ws)* Fix call blocking error in async runtime
- *(ws)* Fix send error- Use oneshot sender for async drop impls


### Features
- DsnError::InvalidAddresses error changed


## [taos-ws-v0.3.8] - 2022-11-04

### Bug Fixes

- *(ws)* Fix error catching for query failed

## [taos-ws-v0.3.7] - 2022-11-02

### Bug Fixes

- *(ws)* Broadcast errors when connection broken- Async future not cancellable when timeout=never


### Refactor
- Expose tokio in prelude mod


## [taos-ws-v0.3.6] - 2022-11-01

### Bug Fixes
- Remove initial poll since first msg not always none


## [taos-ws-v0.3.5] - 2022-10-31

### Bug Fixes
- Cover timeout=never feature for websocket


## [taos-v0.4.4] - 2022-10-31

### Bug Fixes
- Fix Stmt Send/Sync error under async/await


## [taos-v0.4.3] - 2022-10-28

### Bug Fixes
- Check tmq pointer is null


## [taos-v0.4.2] - 2022-10-28

### Bug Fixes
- Fix drop errors when use optin libs


## [taos-ws-v0.3.4] - 2022-10-28

### Features
- Support optin native library loading by dlopen


## [taos-query-v0.3.2] - 2022-10-27

### Features
- Refactor write_raw_* apis
- Add optin package for 2.x/3.x native comatible loading


## [taos-v0.4.0] - 2022-10-25

### Bug Fixes
- Remove select-rustc as dep
- Implicitly set protocol as ws when token param exists


## [taos-ws-v0.3.3] - 2022-10-13

### Bug Fixes

- *(ws)* Fix close on closed websocket connection

## [taos-ws-v0.3.2] - 2022-10-12

### Bug Fixes

- *(ws)* Fix coredump when websocket already closed

## [taos-ws-v0.3.1] - 2022-10-09

### Bug Fixes
- Expose Meta/Data struct


## [taos-ws-v0.3.0] - 2022-09-22

### Bug Fixes

- *(taosws)* Fix coredump with cmd: `taos -c test/cfg -R`- Handle error on ws disconnected abnormally


### Features
- Add MetaData variant for tmq message.
  - **BREAKING**: MessageSet enum updated:


## [taos-v0.2.12] - 2022-09-17

### Bug Fixes
- Public MetaAlter fields and AlterType enum


## [taos-ws-v0.2.7] - 2022-09-16

### Bug Fixes
- Catch error in case of action fetch_block
- Publish meta data structures


### Performance

- *(mdsn)* Improve dsn parse performance and reduce binary size

### Refactor
- Move taos as workspace member


## [taos-v0.2.10] - 2022-09-07

### Bug Fixes
- Fix std::ffi::c_char not found in rust v1.63.0


## [taos-ws-v0.2.6] - 2022-09-07

### Bug Fixes
- Sync on async websocket query support
- Fix arm64 specific compile error


### Testing
- Fix test cases compile error


## [taos-ws-v0.2.5] - 2022-09-06

### Bug Fixes

- *(ws)* Fix data lost in large query set- Fix unknown action 'close', use 'free_result'


## [taos-ws-v0.2.4] - 2022-09-02

### Bug Fixes
- Fix version action response error in websocket


## [taos-ws-v0.2.3] - 2022-09-01

### Bug Fixes
- Fix websocket stmt set json tags error


## [taos-v0.2.5] - 2022-08-31

### Bug Fixes

- *(tmq)* Enable expriment snapshot by default- Support . in database part
- Topics table rename


## [taos-ws-v0.2.2] - 2022-08-30

### Bug Fixes
- Remove unused log print


## [mdsn-v0.2.3] - 2022-08-22

### Testing
- Fix llvm-cov test error and report code coverage


## [taos-v0.2.2] - 2022-08-11

### Bug Fixes
- Fix derive error since rustc-1.65.0-nightly


## [taos-v0.2.1] - 2022-08-11

### Documentation
- Fix build error on docs.rs


## [mdsn-v0.2.2] - 2022-08-11

### Bug Fixes

- *(C)* Use char instead of uint8_t in field names
- *(query)* Remove debug info of json view
- *(query)* Fix websocket v2 float null
- *(ws)* Not block on ws_get_server_info
- *(ws)* Use ws_errno/errstr in ws_fetch_block
- *(ws)* Add ws-related error codes, fix version detection
- *(ws)* Fix fetch block unexpect exit
- *(ws)* Fix query errno when connection is not alive
- *(ws)* Fix stmt bind with non-null values coredump
- *(ws)* Fix timing compatibility bug for v2
- *(ws)* Fix stmt null error when buffer is empty
- *(ws)* Fix bigint/unsigned-bigint precision bug
- *(ws-sys)* Fix ws_free_result with NULL- Fix column length calculation for v2 block
- Gcc 7 compile error
- Taos-ws-sys/examples/show-databases.c
- Fix duplicate defines when use with taos.h
- .github/workflows/build.yml branch
- Remove usless file


### Documentation
- Add license and refine readme
- Add README and query/tmq examples
- Add example for bind and r2d2


### Enhancements

- *(ws)* Feature gated ssl support for websocket

### Features

- *(libtaosws)* Add ws_get_server_info
- *(ws)* Add stmt API
- *(ws)* Add ws_take_timing method for taosc cost
- *(ws)* Add ws_stop_query, support write raw block- Support python lib for websocket
- Add main connector


### Refactor

- *(query)* Refactor query interface
- *(query)* New version of raw block- Use stable channel
- Change ws_rs to ws_res to align with original naming style
- Merge with main branch
- Fix nchar/json error, refactor error handling
- Fix stmt bind with raw block error


