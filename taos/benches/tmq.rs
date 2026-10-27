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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use taos::*;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
        .build()
        .await?;

    taos.exec_many([
        "drop topic if exists topic_1747812142",
        "drop database if exists test_1747812142",
        "create database test_1747812142 vgroups 10",
        "create topic topic_1747812142 as database test_1747812142",
        "use test_1747812142",
        "create table t0 (ts timestamp, c1 int, c2 float, c3 float)",
    ])
    .await?;

    let poll_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async {
        let tmq = TmqBuilder::from_dsn("ws://localhost:6041/?group.id=10")?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1747812142"]).await?;

        println!("poll start");
        let start = std::time::Instant::now();

        let timeout = Timeout::Duration(Duration::from_secs(2));
        while let Some((offset, _message)) = consumer.recv_timeout(timeout).await? {
            consumer.commit(offset).await?;
        }

        println!("poll end, elapsed: {:?}", start.elapsed());

        consumer.unsubscribe().await;

        Ok(())
    });

    tokio::time::sleep(Duration::from_secs(2)).await;

    let write_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async {
        write_data().await?;
        Ok(())
    });

    write_handle.await??;
    poll_handle.await??;

    Ok(())
}

async fn write_data() -> anyhow::Result<()> {
    println!("write data start");
    let start = std::time::Instant::now();

    let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
        .build()
        .await?;

    taos.exec_many(["use test_1747812142"]).await?;

    let num = 50000;
    let mut sqls = Vec::with_capacity(1000);

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    for i in 0..num {
        sqls.push(format!(
            "insert into t0 values ({}, {}, {}, {})",
            ts + i,
            i,
            i as f32 * 1.1,
            i as f32 * 2.2
        ));

        if (i + 1) % 1000 == 0 {
            taos.exec_many(&sqls).await?;
            sqls.clear();
        }
    }

    println!("write data end, elapsed: {:?}", start.elapsed());

    Ok(())
}
