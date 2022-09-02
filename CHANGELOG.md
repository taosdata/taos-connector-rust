# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.7] - 2022-09-02

### Added
- add main connector
- support python lib for websocket
- *(ws)* add ws_stop_query, support write raw block
- *(ws)* add ws_take_timing method for taosc cost
- *(libtaosws)* add ws_get_server_info
- *(ws)* add stmt API

### Fixed
- fix version action response error in websocket
- fix websocket stmt set json tags error
- *(tmq)* enable expriment snapshot by default
- topics table rename
- support . in database part
- remove unused log print
- fix derive error since rustc-1.65.0-nightly
- fix sub-title
- *(ws)* fix bigint/unsigned-bigint precision bug
- *(ws)* fix stmt null error when buffer is empty
- remove usless file
- *(ws)* fix timing compatibility bug for v2
- *(query)* fix websocket v2 float null
- *(ws-sys)* fix ws_free_result with NULL
- *(ws)* fix stmt bind with non-null values coredump
- *(ws)* fix query errno when connection is not alive
- *(ws)* fix fetch block unexpect exit
- *(ws)* add ws-related error codes, fix version detection
- *(ws)* use ws_errno/errstr in ws_fetch_block
- *(ws)* not block on ws_get_server_info
- *(C)* use char instead of uint8_t in field names
- .github/workflows/build.yml branch
- fix duplicate defines when use with taos.h
- taos-ws-sys/examples/show-databases.c
- gcc 7 compile error
- fix column length calculation for v2 block

### Other
- release
- test write_raw_meta
- fix warnings in taos-query
- release
- release
- fix release error
- release
- use codecov token in github actions
- simplify cliff config
- apply clippy
- add coverage test in CI
- fix llvm-cov test error and report code coverage
- release
- better format changelog
- release
- fix build error on docs.rs
- prepare for release
- release
- do not publish taos-ws-sys/py to crates
- fix ci
- fix stmt bind with raw block error
- ignore list update
- bump version
- add release configuration
- use actions/setup-go@v3 to cache go packages
- enable checkout on main branch
- fix warnings
- fix branch name error in GitHub Action
- *(query)* new version of raw block
- *(ci)* cache rust staff
- add example for bind and r2d2
- *(ci)* replace gittag for taosws in GA
- *(ci)* do not use submodule
- *(ci)* use 3.0 branch for test
- add README and query/tmq examples
- add license and refine readme
- remove dbg macro in libtaos stmt
- fix nchar view while more than one nchar cols
- fix stable chnnel compile error
- Merge pull request #14 from taosdata/refactor/query
- *(query)* refactor query interface
- *(ws)* feature gated ssl support for websocket
- fix send failed for closed query
- *(ws)* correct error code when connection closed
- debug get_raw_value
- more log on ws fetch
- fail when message not expected
- more log for debug
- rename ws_num_of_fields to ws_field_count as taosc does
- fix taos-ws-sys test
- fix ws_get_server_info unexpect output
- *(ws)* deeper log information
- add debug logs for ws query
- fix document for ws_enable_log
- fix query panic
- fix ws_close panic
- fix ws_query hangout when connection closed
- timeout when query
- remove debug wrappers
- *(ci)* update .github/workflows/build.yml
- fix const signature diff for stable/nightly
- fix nchar/json error, refactor error handling
- merge with main branch
- Create build.yml
- re-use tmp variable to pass gcc 7
- remove coverage report file
- use stable channel
- refine README
- first version for C libtaosws
# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
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
