// Copyright (c) 2024 TAOS Data, Inc.
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

use std::ops::Deref;
use std::time::{Duration, Instant};

use sync::MessageSet;
use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    unsafe { std::env::set_var("RUST_LOG", "trace") };
    pretty_env_logger::init();

    let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
        .pool()?
        .get()
        .await?;
    println!("Connected to taos://localhost:6030");

    taos.exec_many([
        "drop topic if exists ts5250",
        "drop database if exists ts5250",
        "create database ts5250",
        "use ts5250",
        "create table st1(ts timestamp, val int) tags(t1 int)",
        "create table d1 using st1 tags(1)",
        "insert into d1 values(now, 0)",
        "insert into d1 Values(now, 1)",
        "create topic ts5250 with meta as database ts5250",
    ])
    .await?;
    /*
    drop topic if exists ts5250;
    drop database if exists ts5250;
    create database ts5250;
    use ts5250;
    create table st1(ts timestamp, val int) tags(t1 int);
    insert into d1 using st1 tags(1) values(now, 0);
    insert into ts5250.d1 using st1 tags(1) values(now, 0);
    insert into ts5250.d1 using ts5250.st1 tags(1) values(now, 0);
    create topic ts5250 with meta as database ts5250;
     */

    // subscribe
    let group = chrono::Local::now().timestamp_nanos_opt().unwrap();
    let tmq = TmqBuilder::from_dsn(format!(
        "taos://localhost:6030/ts5250?group.id={group}&experimental.snapshot.enable=true&auto.offset.reset=earliest",
    ))?;
    println!("group id: {}", group);

    let mut consumer = tmq.build().await?;
    consumer.subscribe(["ts5250"]).await?;
    // let assignment = consumer.assignments().await;
    // println!("assignments: {:?}", assignment);

    let blocks_cost = Duration::ZERO;
    {
        let mut stream = consumer.stream();
        println!("start consuming");

        let begin = Instant::now();

        let mut mid = 0;
        while let Some((offset, message)) = stream.try_next().await? {
            println!("{mid} offset: {:?}", offset);
            // get information from offset
            match message {
                MessageSet::Meta(meta) => {
                    println!("{mid} meta: {:?}", meta);
                    let raw = meta.as_raw_meta().await?;
                    let bytes = raw.to_bytes();
                    let path = format!("raw_{}.bin", mid);
                    std::fs::write(path, bytes.deref())?;
                }
                MessageSet::Data(data) => {
                    println!("{mid} data: {:?}", data);
                    let raw = data.as_raw_data().await?;
                    let bytes = raw.to_bytes();
                    let path = format!("raw_{}.bin", mid);
                    std::fs::write(path, bytes.deref())?;
                }
                MessageSet::MetaData(meta, ..) => {
                    println!("{mid} meta data: {:?}", meta);
                    let raw = meta.as_raw_meta().await?;
                    let bytes = raw.to_bytes();
                    let path = format!("raw_{}.bin", mid);
                    std::fs::write(path, bytes.deref())?;
                }
            }
            mid += 1;

            consumer.commit(offset).await?;
        }
        println!("total cost: {:?}", begin.elapsed());
    }
    println!("blocks cost: {:?}", blocks_cost);

    consumer.unsubscribe().await;

    Ok(())
}
