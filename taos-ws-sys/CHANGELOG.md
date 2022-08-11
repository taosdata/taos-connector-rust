# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2022-08-11

### Added
- *(ws)* add ws_stop_query, support write raw block
- *(ws)* add ws_take_timing method for taosc cost
- *(libtaosws)* add ws_get_server_info
- *(ws)* add stmt API

### Fixed
- *(ws)* fix bigint/unsigned-bigint precision bug
- *(ws)* fix stmt null error when buffer is empty
- *(ws-sys)* fix ws_free_result with NULL
- *(ws)* fix stmt bind with non-null values coredump
- *(ws)* fix query errno when connection is not alive
- *(ws)* add ws-related error codes, fix version detection
- *(ws)* use ws_errno/errstr in ws_fetch_block
- *(ws)* not block on ws_get_server_info
- *(C)* use char instead of uint8_t in field names
- fix duplicate defines when use with taos.h
- taos-ws-sys/examples/show-databases.c
- gcc 7 compile error
- fix column length calculation for v2 block

### Other
- bump version
- fix warnings
- remove dbg macro in libtaos stmt
- *(ws)* feature gated ssl support for websocket
- fix send failed for closed query
- *(ws)* correct error code when connection closed
- debug get_raw_value
- more log on ws fetch
- more log for debug
- rename ws_num_of_fields to ws_field_count as taosc does
- fix taos-ws-sys test
- fix ws_get_server_info unexpect output
- *(ws)* deeper log information
- add debug logs for ws query
- fix document for ws_enable_log
- fix ws_query hangout when connection closed
- remove debug wrappers
- fix nchar/json error, refactor error handling
- change ws_rs to ws_res to align with original naming style
- re-use tmp variable to pass gcc 7
- use stable channel
- refine README
- first version for C libtaosws
