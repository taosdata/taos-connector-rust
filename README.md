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

We welcome you to submit [GitHub Issue](https://github.com/taosdata/taos-connector-rust/issues/new?template=Blank+issue). Please provide the following information when submitting so that we can quickly locate the problem:

- Problem description: The specific problem manifestation and whether it must occur. It is recommended to attach detailed call stack or log information.
- Rust connector version: The version number can be obtained through `Cargo.toml` or `cargo metadata`.
- Connection parameters: Provide key connection parameters (no need to include username and password).
- TDengine server version: The version information can be obtained through `select server_version()`.

If you have other relevant information (such as environment configuration, operating system version, etc.), please add it so that we can have a more comprehensive understanding of the problem.

## 8. Submitting PRs

We welcome developers to participate in the development of this project. Please follow the steps below when submitting a PR:

1. Fork the repository: Please fork this repository first. For specific steps, please refer to [How to Fork a Repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo).
2. Create a new branch: Create a new branch based on the `main` branch and use a meaningful branch name (for example: `git checkout -b feature/my_feature`). Do not modify it directly on the main branch.
3. Development and testing: After completing the code modification, make sure that all unit tests pass, and add corresponding test cases for new features or fixed bugs.
4. Submit code: Submit the changes to the remote branch (for example: `git push origin feature/my_feature`).
5. Create a Pull Request: Initiate a Pull Request on GitHub. For specific steps, please refer to [How to create a Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).
6. Check CI and code coverage: After submitting the PR, make sure the CI process passes. You can check the code coverage of the corresponding PR on the [Codecov](https://app.codecov.io/gh/taosdata/taos-connector-rust/pulls).

Thank you for your contribution! We look forward to working with you to improve and optimize the project.

## 9. References

- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. License

[MIT License](./LICENSE)
