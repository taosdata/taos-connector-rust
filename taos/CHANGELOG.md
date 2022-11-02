# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [0.4.7] - 2022-11-02

### Bug Fixes

- *(ws)* Broadcast errors when connection broken

## [0.4.1] - 2022-10-27

### Features
- Support optin native library loading by dlopen
- Refactor write_raw_* apis


## [0.4.0] - 2022-10-25

### Bug Fixes
- Implicitly set protocol as ws when token param exists
- Expose Meta/Data struct


### Features
- Add MetaData variant for tmq message.
  - **BREAKING**: add MetaData variant for tmq message.


### Refactor
- Move taos as workspace member


## [0.3.1] - 2022-10-09

### Bug Fixes
- Expose Meta/Data struct


## [0.3.0] - 2022-09-22

### Features
- Add MetaData variant for tmq message.
  - **BREAKING**: add MetaData variant for tmq message.


## [0.2.11] - 2022-09-16

### Refactor
- Move taos as workspace member


