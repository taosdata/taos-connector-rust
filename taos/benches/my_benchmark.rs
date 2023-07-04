use criterion::{criterion_group, criterion_main, Criterion};
use taos::TaosBuilder;

use taos_query::prelude::sync::*;

fn criterion_benchmark(c: &mut Criterion) {
    let db = "bench_insert";
    let prepare_db = [
        format!("drop database if exists {db}"),
        format!("create database if not exists {db}"),
        format!("use {db}"),
        format!("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)"),
    ];

    let values = " (NOW, 10.2, 219, 0.32)".repeat(100);
    let mut sql = format!("INSERT INTO");
    for i in 0..10 {
        let table = format!(" d100{}", i);
        let sub_sql = format!("{table} USING meters TAGS ('California.SanFrancisco', 2) VALUES {values}");
        sql += &sub_sql;
    }
    println!("{}", sql);

    let dsn = "taos://localhost:6030";
    let native = TaosBuilder::from_dsn(dsn).unwrap().build().unwrap();

    native.exec_many(&prepare_db).unwrap();

    c.bench_function(
        "native", 
        |b| b.iter(|| 
            native.exec(&sql).unwrap()
        )
    );

    let dsn_ws = "taosws://localhost:6041";
    let ws = TaosBuilder::from_dsn(dsn_ws).unwrap().build().unwrap();

    ws.exec_many(&prepare_db).unwrap();

    c.bench_function(
        "ws", 
        |b| b.iter(|| 
            ws.exec(&sql).unwrap()
        )
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
