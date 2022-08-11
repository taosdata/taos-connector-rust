# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2022-08-11

### Added
- *(ws)* add ws_take_timing method for taosc cost
- *(libtaosws)* add ws_get_server_info

### Fixed
- *(ws)* fix stmt bind with non-null values coredump
- *(ws)* add ws-related error codes, fix version detection
- fix column length calculation for v2 block

### Other
- bump version
- *(query)* refactor query interface
- fix nchar/json error, refactor error handling
- first version for C libtaosws
