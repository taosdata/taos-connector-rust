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
- *(ws)* fix stmt null error when buffer is empty
- *(ws)* fix timing compatibility bug for v2
- *(query)* fix websocket v2 float null
- *(ws)* fix stmt bind with non-null values coredump
- *(ws)* fix query errno when connection is not alive
- *(ws)* fix fetch block unexpect exit
- *(ws)* add ws-related error codes, fix version detection
- *(ws)* use ws_errno/errstr in ws_fetch_block
- *(ws)* not block on ws_get_server_info
- fix column length calculation for v2 block

### Other
- bump version
- fix warnings
- *(query)* new version of raw block
- add README and query/tmq examples
- *(query)* refactor query interface
- *(ws)* feature gated ssl support for websocket
- fix send failed for closed query
- *(ws)* correct error code when connection closed
- debug get_raw_value
- fail when message not expected
- more log for debug
- fix ws_get_server_info unexpect output
- *(ws)* deeper log information
- add debug logs for ws query
- fix query panic
- fix ws_close panic
- fix ws_query hangout when connection closed
- timeout when query
- remove debug wrappers
- fix nchar/json error, refactor error handling
- use stable channel
- first version for C libtaosws
