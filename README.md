<!-- omit in toc -->
# TDengine Rust Connector
<!-- omit in toc -->

| Docs.rs                                        | Crates.io Version                                  | Crates.io Downloads                                | CodeCov                                                                                                                                                           |
| ---------------------------------------------- | -------------------------------------------------- | -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ![docs.rs](https://img.shields.io/docsrs/taos) | ![Crates.io](https://img.shields.io/crates/v/taos) | ![Crates.io](https://img.shields.io/crates/d/taos) | [![codecov](https://codecov.io/gh/taosdata/taos-connector-rust/branch/main/graph/badge.svg?token=P11UKNLTVO)](https://codecov.io/gh/taosdata/taos-connector-rust) |

English | [简体中文](./README-CN.md)

<!-- omit in toc -->
## Table of Contents
<!-- omit in toc -->

- [1. Introduction](#1-introduction)
  - [1.1 Connection Methods](#11-connection-methods)
  - [1.2 Rust Version Compatibility](#12-rust-version-compatibility)
  - [1.3 Supported Platforms](#13-supported-platforms)
- [2. Getting the Driver](#2-getting-the-driver)
- [3. Documentation](#3-documentation)
- [4. Prerequisites](#4-prerequisites)
- [5. Build](#5-build)
- [6. Testing](#6-testing)
  - [6.1 Test Execution](#61-test-execution)
  - [6.2 Test Case Addition](#62-test-case-addition)
  - [6.3 Performance Testing](#63-performance-testing)
- [7. Submitting Issues](#7-submitting-issues)
- [8. Submitting PRs](#8-submitting-prs)
- [9. References](#9-references)
- [10. License](#10-license)

## 1. Introduction

`taos` is the official Rust language connector of TDengine, through which Rust developers can develop applications that access TDengine databases. It supports data writing, data query, data subscription, schemaless writing, parameter binding and other functions.

### 1.1 Connection Methods

`taos` provides two ways to establish a connection:

- Native connection: Establish a connection directly with the server program taosd through the client driver taosc. When using this connection method, you need to ensure that the client driver taosc and the server taosd version are consistent.
- WebSocket connection: Establish a connection with taosd through the WebSocket API provided by the taosAdapter component. This method does not rely on the TDengine client driver, supports cross-platform use, is more convenient and flexible, and has performance similar to native connection.

It is recommended to use the WebSocket connection method. For detailed description, please refer to [Connection Methods](https://docs.tdengine.com/developer-guide/connecting-to-tdengine/#connection-methods).

### 1.2 Rust Version Compatibility

Supports Rust 1.70 and above.

### 1.3 Supported Platforms

- The platforms supported by the native connection are consistent with those supported by the TDengine client driver.
- WebSocket connection supports all platforms that can run Rust.

## 2. Getting the Driver

Add the following to your `Cargo.toml`:

```toml
[dependencies]
taos = "0.12.3"
```

## 3. Documentation

- For development examples, please visit the [Developer Guide](https://docs.tdengine.com/developer-guide/), which includes examples of data writing, data querying, data subscription, schemaless writing, and parameter binding.
- For more information, please visit the [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/rust/), which includes version history, data type mapping, sample program summary, API reference, and FAQ.

## 4. Prerequisites

- Rust 1.70 or above has been installed.
- TDengine has been deployed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/), and taosd and taosAdapter have been started.

## 5. Build

Run the following command in the project directory to build the project:

```sh
cargo build
```

## 6. Testing

### 6.1 Test Execution

Before running the test, please add the following configuration to the `taos.cfg` file:

```text
supportVnodes 256
```

After completing the configuration, execute the following command in the project directory to run the test:

```sh
cargo test
```

### 6.2 Test Case Addition

Add test cases in the `#[cfg(test)]` module of the corresponding `.rs` file. For synchronous code, use the `#[test]` macro; for asynchronous code, use the `#[tokio::test]` macro.

### 6.3 Performance Testing

Performance testing is under development.

## 7. Submitting Issues

We welcome the submission of [GitHub Issue](https://github.com/taosdata/taos-connector-rust/issues/new?template=Blank+issue). When submitting, please provide the following information:

- Description of the problem, whether it must occur, preferably with detailed call stack.
- Rust connector version.
- Connection parameters (no username or password required).
- TDengine server version.

## 8. Submitting PRs

We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project, refer to ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
2. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`). Do not modify the main branch directly.
3. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
4. Push the changes to the remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
6. After submitting the PR, if CI passes, you can find your PR on the [codecov](https://app.codecov.io/gh/taosdata/taos-connector-rust/pulls) page to check the test coverage.

## 9. References

- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. License

[MIT License](./LICENSE)
