<!-- omit in toc -->
# TDengine Rust Connector
<!-- omit in toc -->

| Docs.rs                                        | Crates.io Version                                  | Crates.io Downloads                                | CodeCov                                                                                                                                                           |
| ---------------------------------------------- | -------------------------------------------------- | -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ![docs.rs](https://img.shields.io/docsrs/taos) | ![Crates.io](https://img.shields.io/crates/v/taos) | ![Crates.io](https://img.shields.io/crates/d/taos) | [![codecov](https://codecov.io/gh/taosdata/taos-connector-rust/branch/main/graph/badge.svg?token=P11UKNLTVO)](https://codecov.io/gh/taosdata/taos-connector-rust) |

简体中文 | [English](./README.md)

<!-- omit in toc -->
## 目录
<!-- omit in toc -->

- [1. 简介](#1-简介)
  - [1.1 连接方式](#11-连接方式)
  - [1.2 Rust 版本兼容性](#12-rust-版本兼容性)
  - [1.3 支持的平台](#13-支持的平台)
- [2. 获取驱动](#2-获取驱动)
- [3. 文档](#3-文档)
- [4. 前置条件](#4-前置条件)
- [5. 构建](#5-构建)
- [6. 测试](#6-测试)
  - [6.1 运行测试](#61-运行测试)
  - [6.2 添加用例](#62-添加用例)
  - [6.3 性能测试](#63-性能测试)
- [7. 提交 Issue](#7-提交-issue)
- [8. 提交 PR](#8-提交-pr)
- [9. 引用](#9-引用)
- [10. 许可证](#10-许可证)

## 1. 简介

`taos` 是 TDengine 的官方 Rust 语言连接器，Rust 开发人员可以通过它开发存取 TDengine 数据库的应用软件。它支持数据写入、数据查询、数据订阅、无模式写入以及参数绑定等功能。

### 1.1 连接方式

`taos` 提供了两种建立连接的方式：

- 原生连接：通过客户端驱动程序 taosc 直接与服务端程序 taosd 建立连接。采用这种连接方式时，需确保客户端的驱动程序 taosc 与服务端的 taosd 版本保持一致。
- WebSocket 连接：通过 taosAdapter 组件提供的 WebSocket API 建立与 taosd 的连接。此方式不依赖 TDengine 客户端驱动，支持跨平台使用，更为便捷灵活，且性能与原生连接相近。

建议使用 WebSocket 连接方式。详细说明请参考 [连接方式](https://docs.taosdata.com/develop/connect/#%E8%BF%9E%E6%8E%A5%E6%96%B9%E5%BC%8F)。

### 1.2 Rust 版本兼容性

支持 Rust 1.70 及以上版本。

### 1.3 支持的平台

- 原生连接支持的平台与 TDengine 客户端驱动支持的平台一致。
- WebSocket 连接支持所有能运行 Rust 的平台。

## 2. 获取驱动

将以下内容添加到 `Cargo.toml`：

```toml
[dependencies]
taos = "0.12.3"
```

## 3. 文档

- 开发示例请访问 [开发指南](https://docs.taosdata.com/develop/)，其中包括数据写入、数据查询、数据订阅、无模式写入以及参数绑定等示例。
- 更多信息请访问 [参考手册](https://docs.taosdata.com/reference/connector/rust/)，其中包括版本历史、数据类型映射、示例程序汇总、API 参考以及常见问题等内容。

## 4. 前置条件

- 已安装 Rust 1.70 及以上版本。
- 本地已部署 TDengine，具体步骤请参考 [部署服务端](https://docs.taosdata.com/get-started/package/)，且已经启动 taosd 与 taosAdapter。

## 5. 构建

在项目目录下运行以下命令以构建项目：

```sh
cargo build
```

## 6. 测试

### 6.1 运行测试

运行测试前，请在 `taos.cfg` 文件中添加以下配置：

```text
supportVnodes 256
```

完成配置后，在项目目录下执行以下命令运行测试：

```sh
cargo test
```

### 6.2 添加用例

在相应的 `.rs` 文件的 `#[cfg(test)]` 模块内添加测试用例。对于同步代码，使用 `#[test]` 宏；对于异步代码，使用 `#[tokio::test]` 宏。

### 6.3 性能测试

性能测试正在开发中。

## 7. 提交 Issue

我们欢迎提交 [GitHub Issue](https://github.com/taosdata/taos-connector-rust/issues/new?template=Blank+issue)。提交时请尽量提供以下信息，以便快速定位问题：

- 问题描述：具体问题表现及是否必现，建议附上详细调用堆栈或日志信息。
- Rust 连接器版本：可通过 `Cargo.toml` 或 `cargo metadata` 获取版本号。
- 连接参数：提供关键连接参数（无需包含用户名和密码）。
- TDengine 服务端版本：可通过 `select server_version()` 获取版本信息。

如有其他相关信息（如环境配置、操作系统版本等），请一并补充，以便我们更全面地了解问题。

## 8. 提交 PR

我们欢迎开发者共同参与本项目开发，提交 PR 时请按照以下步骤操作：

1. Fork 仓库：请先 Fork 本仓库，具体步骤请参考 [如何 Fork 仓库](https://docs.github.com/en/get-started/quickstart/fork-a-repo)。
2. 创建新分支：基于 `main` 分支创建一个新分支，并使用有意义的分支名称（例如：`git checkout -b feature/my_feature`）。请勿直接在 main 分支上进行修改。
3. 开发与测试：完成代码修改后，确保所有单元测试都能通过，并为新增功能或修复的 Bug 添加相应的测试用例。
4. 提交代码：将修改提交到远程分支（例如：`git push origin feature/my_feature`）。
5. 创建 Pull Request：在 GitHub 上发起 [Pull Request](https://github.com/taosdata/taos-connector-rust/pulls)，具体步骤请参考 [如何创建 Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)。
6. 检查 CI 和代码覆盖率：提交 PR 后，确保 CI 流程通过。您可以在 [Codecov](https://app.codecov.io/gh/taosdata/taos-connector-rust/pulls) 查看对应 PR 的代码覆盖率。

感谢您的贡献！我们期待与您共同完善和优化该项目。

## 9. 引用

- [TDengine 官网](https://www.taosdata.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. 许可证

[MIT License](./LICENSE)
