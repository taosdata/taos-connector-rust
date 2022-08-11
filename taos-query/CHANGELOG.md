# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2022-08-11

### Added
- add main connector
- *(ws)* add ws_stop_query, support write raw block
- *(ws)* add ws_take_timing method for taosc cost
- *(libtaosws)* add ws_get_server_info
- *(ws)* add stmt API

### Fixed
- *(ws)* fix bigint/unsigned-bigint precision bug
- *(query)* fix websocket v2 float null
- *(ws)* fix stmt bind with non-null values coredump
- *(ws)* fix query errno when connection is not alive
- *(ws)* fix fetch block unexpect exit
- *(ws)* add ws-related error codes, fix version detection
- fix column length calculation for v2 block

### Other
- bump version
- fix warnings
- *(query)* new version of raw block
- add example for bind and r2d2
- add README and query/tmq examples
- fix nchar view while more than one nchar cols
- fix stable chnnel compile error
- Merge pull request #14 from taosdata/refactor/query
- *(query)* refactor query interface
- debug get_raw_value
- more log for debug
- fix query panic
- fix const signature diff for stable/nightly
- fix nchar/json error, refactor error handling
- remove coverage report file
- first version for C libtaosws
