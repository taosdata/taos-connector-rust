# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [0.2.8] - 2022-09-06

### Bug Fixes

- *(C)* Use char instead of uint8_t in field names
- *(query)* Fix websocket v2 float null
- *(tmq)* Enable expriment snapshot by default
- *(ws)* Fix data lost in large query set
- *(ws)* Fix bigint/unsigned-bigint precision bug
- *(ws)* Fix stmt null error when buffer is empty
- *(ws)* Fix timing compatibility bug for v2
- *(ws)* Fix stmt bind with non-null values coredump
- *(ws)* Fix query errno when connection is not alive
- *(ws)* Fix fetch block unexpect exit
- *(ws)* Add ws-related error codes, fix version detection
- *(ws)* Use ws_errno/errstr in ws_fetch_block
- *(ws)* Not block on ws_get_server_info
- *(ws-sys)* Fix ws_free_result with NULL- Fix version action response error in websocket
- Fix websocket stmt set json tags error
- Topics table rename
- Support . in database part
- Remove unused log print
- Fix derive error since rustc-1.65.0-nightly
- Remove usless file
- .github/workflows/build.yml branch
- Fix duplicate defines when use with taos.h
- Taos-ws-sys/examples/show-databases.c
- Gcc 7 compile error
- Fix column length calculation for v2 block


### Documentation
- Fix build error on docs.rs
- Add example for bind and r2d2
- Add README and query/tmq examples
- Add license and refine readme


### Enhancements

- *(ws)* Feature gated ssl support for websocket

### Features

- *(libtaosws)* Add ws_get_server_info
- *(ws)* Add ws_stop_query, support write raw block
- *(ws)* Add ws_take_timing method for taosc cost
- *(ws)* Add stmt API- Add main connector
- Support python lib for websocket


### Refactor

- *(query)* New version of raw block
- *(query)* Refactor query interface- Fix stmt bind with raw block error
- Fix nchar/json error, refactor error handling
- Merge with main branch
- Use stable channel


### Testing
- Fix llvm-cov test error and report code coverage


## [0.2.7] - 2022-09-02

### Bug Fixes

- *(C)* Use char instead of uint8_t in field names
- *(query)* Fix websocket v2 float null
- *(tmq)* Enable expriment snapshot by default
- *(ws)* Fix bigint/unsigned-bigint precision bug
- *(ws)* Fix stmt null error when buffer is empty
- *(ws)* Fix timing compatibility bug for v2
- *(ws)* Fix stmt bind with non-null values coredump
- *(ws)* Fix query errno when connection is not alive
- *(ws)* Fix fetch block unexpect exit
- *(ws)* Add ws-related error codes, fix version detection
- *(ws)* Use ws_errno/errstr in ws_fetch_block
- *(ws)* Not block on ws_get_server_info
- *(ws-sys)* Fix ws_free_result with NULL- Fix version action response error in websocket
- Fix websocket stmt set json tags error
- Topics table rename
- Support . in database part
- Remove unused log print
- Fix derive error since rustc-1.65.0-nightly
- Remove usless file
- .github/workflows/build.yml branch
- Fix duplicate defines when use with taos.h
- Taos-ws-sys/examples/show-databases.c
- Gcc 7 compile error
- Fix column length calculation for v2 block


### Documentation
- Fix build error on docs.rs
- Add example for bind and r2d2
- Add README and query/tmq examples
- Add license and refine readme


### Enhancements

- *(ws)* Feature gated ssl support for websocket

### Features

- *(libtaosws)* Add ws_get_server_info
- *(ws)* Add ws_stop_query, support write raw block
- *(ws)* Add ws_take_timing method for taosc cost
- *(ws)* Add stmt API- Add main connector
- Support python lib for websocket


### Refactor

- *(query)* New version of raw block
- *(query)* Refactor query interface- Fix stmt bind with raw block error
- Fix nchar/json error, refactor error handling
- Merge with main branch
- Use stable channel


### Testing
- Fix llvm-cov test error and report code coverage


## [0.2.6] - 2022-09-01

### Bug Fixes

- *(C)* Use char instead of uint8_t in field names
- *(query)* Fix websocket v2 float null
- *(tmq)* Enable expriment snapshot by default
- *(ws)* Fix bigint/unsigned-bigint precision bug
- *(ws)* Fix stmt null error when buffer is empty
- *(ws)* Fix timing compatibility bug for v2
- *(ws)* Fix stmt bind with non-null values coredump
- *(ws)* Fix query errno when connection is not alive
- *(ws)* Fix fetch block unexpect exit
- *(ws)* Add ws-related error codes, fix version detection
- *(ws)* Use ws_errno/errstr in ws_fetch_block
- *(ws)* Not block on ws_get_server_info
- *(ws-sys)* Fix ws_free_result with NULL- Fix websocket stmt set json tags error
- Topics table rename
- Support . in database part
- Remove unused log print
- Fix derive error since rustc-1.65.0-nightly
- Remove usless file
- .github/workflows/build.yml branch
- Fix duplicate defines when use with taos.h
- Taos-ws-sys/examples/show-databases.c
- Gcc 7 compile error
- Fix column length calculation for v2 block


### Documentation
- Fix build error on docs.rs
- Add example for bind and r2d2
- Add README and query/tmq examples
- Add license and refine readme


### Enhancements

- *(ws)* Feature gated ssl support for websocket

### Features

- *(libtaosws)* Add ws_get_server_info
- *(ws)* Add ws_stop_query, support write raw block
- *(ws)* Add ws_take_timing method for taosc cost
- *(ws)* Add stmt API- Add main connector
- Support python lib for websocket


### Refactor

- *(query)* New version of raw block
- *(query)* Refactor query interface- Fix stmt bind with raw block error
- Fix nchar/json error, refactor error handling
- Merge with main branch
- Use stable channel


### Testing
- Fix llvm-cov test error and report code coverage


## [0.2.5] - 2022-08-31

### Bug Fixes

- *(C)* Use char instead of uint8_t in field names
- *(query)* Fix websocket v2 float null
- *(tmq)* Enable expriment snapshot by default
- *(ws)* Fix bigint/unsigned-bigint precision bug
- *(ws)* Fix stmt null error when buffer is empty
- *(ws)* Fix timing compatibility bug for v2
- *(ws)* Fix stmt bind with non-null values coredump
- *(ws)* Fix query errno when connection is not alive
- *(ws)* Fix fetch block unexpect exit
- *(ws)* Add ws-related error codes, fix version detection
- *(ws)* Use ws_errno/errstr in ws_fetch_block
- *(ws)* Not block on ws_get_server_info
- *(ws-sys)* Fix ws_free_result with NULL- Topics table rename
- Support . in database part
- Remove unused log print
- Fix derive error since rustc-1.65.0-nightly
- Remove usless file
- .github/workflows/build.yml branch
- Fix duplicate defines when use with taos.h
- Taos-ws-sys/examples/show-databases.c
- Gcc 7 compile error
- Fix column length calculation for v2 block


### Documentation
- Fix build error on docs.rs
- Add example for bind and r2d2
- Add README and query/tmq examples
- Add license and refine readme


### Enhancements

- *(ws)* Feature gated ssl support for websocket

### Features

- *(libtaosws)* Add ws_get_server_info
- *(ws)* Add ws_stop_query, support write raw block
- *(ws)* Add ws_take_timing method for taosc cost
- *(ws)* Add stmt API- Add main connector
- Support python lib for websocket


### Refactor

- *(query)* New version of raw block
- *(query)* Refactor query interface- Fix stmt bind with raw block error
- Fix nchar/json error, refactor error handling
- Merge with main branch
- Use stable channel


### Testing
- Fix llvm-cov test error and report code coverage


## [0.2.4] - 2022-08-30

### Bug Fixes

- *(C)* Use char instead of uint8_t in field names
- *(query)* Fix websocket v2 float null
- *(ws)* Fix bigint/unsigned-bigint precision bug
- *(ws)* Fix stmt null error when buffer is empty
- *(ws)* Fix timing compatibility bug for v2
- *(ws)* Fix stmt bind with non-null values coredump
- *(ws)* Fix query errno when connection is not alive
- *(ws)* Fix fetch block unexpect exit
- *(ws)* Add ws-related error codes, fix version detection
- *(ws)* Use ws_errno/errstr in ws_fetch_block
- *(ws)* Not block on ws_get_server_info
- *(ws-sys)* Fix ws_free_result with NULL- Remove unused log print
- Fix derive error since rustc-1.65.0-nightly
- Remove usless file
- .github/workflows/build.yml branch
- Fix duplicate defines when use with taos.h
- Taos-ws-sys/examples/show-databases.c
- Gcc 7 compile error
- Fix column length calculation for v2 block


### Documentation
- Fix build error on docs.rs
- Add example for bind and r2d2
- Add README and query/tmq examples
- Add license and refine readme


### Enhancements

- *(ws)* Feature gated ssl support for websocket

### Features

- *(libtaosws)* Add ws_get_server_info
- *(ws)* Add ws_stop_query, support write raw block
- *(ws)* Add ws_take_timing method for taosc cost
- *(ws)* Add stmt API- Add main connector
- Support python lib for websocket


### Refactor

- *(query)* New version of raw block
- *(query)* Refactor query interface- Fix stmt bind with raw block error
- Fix nchar/json error, refactor error handling
- Merge with main branch
- Use stable channel


### Testing
- Fix llvm-cov test error and report code coverage


## [0.2.3] - 2022-08-22

### Bug Fixes

- *(C)* Use char instead of uint8_t in field names
- *(query)* Fix websocket v2 float null
- *(ws)* Fix bigint/unsigned-bigint precision bug
- *(ws)* Fix stmt null error when buffer is empty
- *(ws)* Fix timing compatibility bug for v2
- *(ws)* Fix stmt bind with non-null values coredump
- *(ws)* Fix query errno when connection is not alive
- *(ws)* Fix fetch block unexpect exit
- *(ws)* Add ws-related error codes, fix version detection
- *(ws)* Use ws_errno/errstr in ws_fetch_block
- *(ws)* Not block on ws_get_server_info
- *(ws-sys)* Fix ws_free_result with NULL- Fix derive error since rustc-1.65.0-nightly
- Remove usless file
- .github/workflows/build.yml branch
- Fix duplicate defines when use with taos.h
- Taos-ws-sys/examples/show-databases.c
- Gcc 7 compile error
- Fix column length calculation for v2 block


### Documentation
- Fix build error on docs.rs
- Add example for bind and r2d2
- Add README and query/tmq examples
- Add license and refine readme


### Enhancements

- *(ws)* Feature gated ssl support for websocket

### Features

- *(libtaosws)* Add ws_get_server_info
- *(ws)* Add ws_stop_query, support write raw block
- *(ws)* Add ws_take_timing method for taosc cost
- *(ws)* Add stmt API- Add main connector
- Support python lib for websocket


### Refactor

- *(query)* New version of raw block
- *(query)* Refactor query interface- Fix stmt bind with raw block error
- Fix nchar/json error, refactor error handling
- Merge with main branch
- Use stable channel


### Testing
- Fix llvm-cov test error and report code coverage


## [0.2.2] - 2022-08-11

### Bug Fixes

- *(C)* Use char instead of uint8_t in field names
- *(query)* Fix websocket v2 float null
- *(ws)* Fix bigint/unsigned-bigint precision bug
- *(ws)* Fix stmt null error when buffer is empty
- *(ws)* Fix timing compatibility bug for v2
- *(ws)* Fix stmt bind with non-null values coredump
- *(ws)* Fix query errno when connection is not alive
- *(ws)* Fix fetch block unexpect exit
- *(ws)* Add ws-related error codes, fix version detection
- *(ws)* Use ws_errno/errstr in ws_fetch_block
- *(ws)* Not block on ws_get_server_info
- *(ws-sys)* Fix ws_free_result with NULL- Fix derive error since rustc-1.65.0-nightly
- Remove usless file
- .github/workflows/build.yml branch
- Fix duplicate defines when use with taos.h
- Taos-ws-sys/examples/show-databases.c
- Gcc 7 compile error
- Fix column length calculation for v2 block


### Documentation
- Fix build error on docs.rs
- Add example for bind and r2d2
- Add README and query/tmq examples
- Add license and refine readme


### Enhancements

- *(ws)* Feature gated ssl support for websocket

### Features

- *(libtaosws)* Add ws_get_server_info
- *(ws)* Add ws_stop_query, support write raw block
- *(ws)* Add ws_take_timing method for taosc cost
- *(ws)* Add stmt API- Add main connector
- Support python lib for websocket


### Refactor

- *(query)* New version of raw block
- *(query)* Refactor query interface- Fix stmt bind with raw block error
- Fix nchar/json error, refactor error handling
- Merge with main branch
- Use stable channel


<!-- generated by git-cliff -->
## [0.2.1] - 2022-08-11

### Bug Fixes

- *(C)* Use char instead of uint8_t in field names
- *(query)* Fix websocket v2 float null
- *(ws)* Fix bigint/unsigned-bigint precision bug
- *(ws)* Fix stmt null error when buffer is empty
- *(ws)* Fix timing compatibility bug for v2
- *(ws)* Fix stmt bind with non-null values coredump
- *(ws)* Fix query errno when connection is not alive
- *(ws)* Fix fetch block unexpect exit
- *(ws)* Add ws-related error codes, fix version detection
- *(ws)* Use ws_errno/errstr in ws_fetch_block
- *(ws)* Not block on ws_get_server_info
- *(ws-sys)* Fix ws_free_result with NULL- Remove usless file
- .github/workflows/build.yml branch
- Fix duplicate defines when use with taos.h
- Taos-ws-sys/examples/show-databases.c
- Gcc 7 compile error
- Fix column length calculation for v2 block


### CI
- Use actions/setup-go@v3 to cache go packages


### Documentation
- Fix build error on docs.rs
- Add example for bind and r2d2
- Add README and query/tmq examples
- Add license and refine readme


### Enhancements

- *(ws)* Feature gated ssl support for websocket

### Features

- *(libtaosws)* Add ws_get_server_info
- *(ws)* Add ws_stop_query, support write raw block
- *(ws)* Add ws_take_timing method for taosc cost
- *(ws)* Add stmt API- Add main connector
- Support python lib for websocket


### Refactor

- *(query)* New version of raw block
- *(query)* Refactor query interface- Fix stmt bind with raw block error
- Fix nchar/json error, refactor error handling
- Merge with main branch
- Use stable channel


### Init
- First version for C libtaosws


<!-- generated by git-cliff -->
