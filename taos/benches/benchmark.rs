// Copyright (c) 2023 TAOS Data, Inc.
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

use criterion::{criterion_group, criterion_main, Criterion};
use taos::TaosBuilder;
use taos_query::prelude::sync::*;

fn criterion_benchmark(c: &mut Criterion) {
    let db = "bench_insert";
    let prepare_db = [
        format!("drop database if exists {db}"),
        format!("create database if not exists {db}"),
        format!("use {db}"),
        "CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)".to_string(),
    ];

    let mut t: i64 = 1688456534091;
    let mut sql = "INSERT INTO".to_string();
    for i in 0..333 {
        let table = format!(" d100{}", i);
        t += 1;
        let sub_sql = format!("{table}_1 USING meters TAGS ('California.SanFrancisco', 2) (ts, current) VALUES ({t}, 10.2)");
        sql += &sub_sql;
        t += 1;
        let sub_sql = format!("{table}_2 USING meters TAGS ('California.SanFrancisco', 2) (ts, voltage) VALUES ({t}, 219)");
        sql += &sub_sql;
        t += 1;
        let sub_sql = format!("{table}_3 USING meters TAGS ('California.SanFrancisco', 2) (ts, phase) VALUES ({t}, 0.32)");
        sql += &sub_sql;
    }
    println!("{}", sql);

    let dsn = "taos://localhost:6030";
    let native = TaosBuilder::from_dsn(dsn).unwrap().build().unwrap();

    native.exec_many(&prepare_db).unwrap();

    c.bench_function("native", |b| b.iter(|| native.exec(&sql).unwrap()));

    let dsn_ws = "taosws://localhost:6041";
    let ws = TaosBuilder::from_dsn(dsn_ws).unwrap().build().unwrap();

    ws.exec_many(&prepare_db).unwrap();

    c.bench_function("ws", |b| b.iter(|| ws.exec(&sql).unwrap()));
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
