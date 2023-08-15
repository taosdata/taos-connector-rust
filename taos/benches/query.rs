use criterion::async_executor::{AsyncExecutor, FuturesExecutor};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use taos::*;
use tokio::runtime::Runtime;

async fn insert(taos: &Taos, tables: &[&str], ts: i64, records: usize) {
    static mut TS: i64 = 1691726395000;
    for table in tables {
        for val in 0..records {
            let ts = unsafe { TS + val as i64 };
            taos.exec(format!("insert into {table} values ({ts}, {val})"))
                .await
                .unwrap();
        }
    }
    unsafe { TS += records as i64 };
}

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

fn bench_fib(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let f = async move {
        let taos = TaosBuilder::from_dsn("taos+ws:///")
            .unwrap()
            .build()
            .await
            .unwrap();
        let db = "db1";
        taos.exec(format!("create database if not exists {db}"))
            .await
            .unwrap();
        let stable = "stb1";
        taos.exec("use db1").await.unwrap();
        taos.exec("create stable if not exists stb1 (ts timestamp, val int) tags(t1 int)")
            .await
            .unwrap();
        taos.exec("create table if not exists tb1 using stb1 tags(1) if not exists tb2 using stb1 tags(1)")
            .await.unwrap();
        taos
    };
    let taos = runtime.block_on(f);
    let records = 1;
    let p = (runtime, taos);
    // c.bench_function("fib 20", |b| b.iter(|| fibonacci(black_box(20))));
    c.bench_with_input(BenchmarkId::new("insert", records), &p, |b, (rt, taos)| {
        // let now = chro
        b.to_async(rt)
            .iter(|| insert(taos, &["tb1", "tb2"], 1691726395000, records));
    });

    let records = 10;
    c.bench_with_input(BenchmarkId::new("insert", records), &p, |b, (rt, taos)| {
        // let now = chro
        b.to_async(rt)
            .iter(|| insert(taos, &["tb1", "tb2"], 1691726395000, records));
    });
    println!("------------------------------------------------------------------------");
}

criterion_group!(name = benches;config = Criterion::default().nresamples(20).sample_size(20);targets= bench_fib);
criterion_main!(benches);
