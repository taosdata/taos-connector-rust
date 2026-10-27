// Copyright (c) 2025 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#[cfg(all(feature = "optin", feature = "deadpool"))]
#[tokio::test]
async fn test_deadpool_optin() -> anyhow::Result<()> {
    use taos::*;

    let pool = TaosBuilder::from_dsn("taos://localhost:6030")?.pool()?;
    let taos = pool.get().await?;
    taos.exec_many([
        "drop database if exists test_1752561201",
        "create database test_1752561201",
        "use test_1752561201",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752561201",
    ])
    .await?;

    Ok(())
}

#[cfg(all(feature = "optin", feature = "ws", feature = "deadpool"))]
#[tokio::test]
async fn test_deadpool_optin2() -> anyhow::Result<()> {
    use taos::*;

    let pool = TaosBuilder::from_dsn("taos://localhost:6030")?.pool()?;
    let taos = pool.get().await?;
    taos.exec_many([
        "drop database if exists test_1752561305",
        "create database test_1752561305",
        "use test_1752561305",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752561305",
    ])
    .await?;

    Ok(())
}

#[cfg(all(feature = "ws", feature = "deadpool"))]
#[tokio::test]
async fn test_deadpool_ws() -> anyhow::Result<()> {
    use taos::*;

    let pool = TaosBuilder::from_dsn("ws://localhost:6041")?.pool()?;
    let taos = pool.get().await?;
    taos.exec_many([
        "drop database if exists test_1752561357",
        "create database test_1752561357",
        "use test_1752561357",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752561357",
    ])
    .await?;

    Ok(())
}

#[cfg(all(feature = "ws", feature = "optin", feature = "deadpool"))]
#[tokio::test]
async fn test_deadpool_ws2() -> anyhow::Result<()> {
    use taos::*;

    let pool = TaosBuilder::from_dsn("ws://localhost:6041")?.pool()?;
    let taos = pool.get().await?;
    taos.exec_many([
        "drop database if exists test_1752561405",
        "create database test_1752561405",
        "use test_1752561405",
        "create table t0(ts timestamp, c1 int)",
        "insert into t0 values(now, 1)",
        "drop database test_1752561405",
    ])
    .await?;

    Ok(())
}
