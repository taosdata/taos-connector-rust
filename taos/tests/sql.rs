use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use chrono::Local;
use rand::Rng;
use taos::{AsyncQueryable, AsyncTBuilder, TaosBuilder};

#[tokio::test]
async fn test_sql() -> anyhow::Result<()> {
    let subtable_cnt = 100_0000;
    let data_cnt = 1_0000_0000;

    let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
        .build()
        .await?;

    let taos = Arc::new(taos);

    let db = "db_202412111551";

    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db} vgroups 10"),
        &format!("use {db}"),
        "create stable s0 (ts timestamp, c1 int, c2 float, c3 float) tags(t1 int)",
    ])
    .await?;

    create_subtables(db, subtable_cnt).await;

    let sqls = generate_sqls(subtable_cnt).await;

    exec_sqls(db, sqls).await;

    taos.exec(format!("drop database {db}")).await?;

    Ok(())
}

async fn create_subtables(db: &str, subtable_cnt: usize) {
    println!("create subtables start");

    let start = Instant::now();
    let batch_cnt = 1_0000;
    let thread_cnt = 10;
    let thread_subtable_cnt = subtable_cnt / thread_cnt;

    let mut tasks = vec![];

    for i in (0..subtable_cnt).step_by(thread_subtable_cnt) {
        let db = db.to_owned();
        let task = tokio::spawn(async move {
            let start = Instant::now();
            println!("thread[{}] create subtables start", i / 10_0000);

            let taos = TaosBuilder::from_dsn("ws://localhost:6041")
                .unwrap()
                .build()
                .await
                .unwrap();

            taos.exec(format!("use {db}")).await.unwrap();

            for j in (0..thread_subtable_cnt).step_by(batch_cnt) {
                // creata table d0 using s0 tags(0) d1 using s0 tags(0) ...
                let mut sql = String::with_capacity(25 * batch_cnt);
                sql.push_str("create table ");
                for k in 0..batch_cnt {
                    sql.push_str(&format!("d{} using s0 tags(0) ", i + j + k));
                }
                taos.exec(sql).await.unwrap();
            }

            println!(
                "thread[{}] create subtables done, elapsed = {:?}",
                i / 10_0000,
                start.elapsed()
            );
        });

        tasks.push(task);
    }

    for task in tasks {
        task.await.unwrap();
    }

    println!("create subtables done, elapsed = {:?}\n", start.elapsed());
}

async fn generate_sqls(subtable_cnt: usize) -> Vec<String> {
    println!("generate sqls start");

    let start = Instant::now();
    let batch_cnt = 1_0000;
    let thread_cnt = 10;
    let thread_subtable_cnt = subtable_cnt / thread_cnt;
    let mut tasks = Vec::with_capacity(thread_cnt);

    for i in (0..subtable_cnt).step_by(thread_subtable_cnt) {
        let task = tokio::spawn(async move {
            let mut sqls = Vec::with_capacity(thread_subtable_cnt);
            let mut rng = rand::thread_rng();

            for j in 0..thread_subtable_cnt {
                let mut sql = String::with_capacity(100 * batch_cnt);
                sql.push_str(&format!("insert into d{} values ", i + j));

                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                for k in 0..batch_cnt {
                    let ts = ts + k as i64;
                    let c1: i32 = rng.gen();
                    let c2: f32 = rng.gen_range(0.0..1000_0000.);
                    let c2: f32 = (c2 * 100.0).round() / 100.0;
                    let c3: f32 = rng.gen_range(0.0..1000_0000.);
                    let c3: f32 = (c3 * 100.0).round() / 100.0;
                    sql.push_str(&format!("({ts},{c1},{c2},{c3}),"));
                }

                sqls.push(sql);
            }

            sqls
        });

        tasks.push(task);
    }

    let mut sqls = Vec::with_capacity(subtable_cnt);
    for task in tasks {
        sqls.extend(task.await.unwrap());
    }

    println!("generate {} sqls", sqls.len());
    println!("generate sqls done, elapsed = {:?}\n", start.elapsed());

    sqls
}

async fn exec_sqls(db: &str, sqls: Vec<String>) {
    println!("exec sqls start, start = {:?}", Local::now());

    let start = Instant::now();

    let thread_cnt = 4;
    let thread_sql_cnt = sqls.len() / thread_cnt;

    let mut tasks = vec![];

    for (i, sqls) in sqls.chunks(thread_sql_cnt).enumerate() {
        let sqls = sqls.to_vec();

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")
            .unwrap()
            .build()
            .await
            .unwrap();

        taos.exec(format!("use {db}")).await.unwrap();

        let task = tokio::spawn(async move {
            println!("thread[{i}] exec sqls start");
            let start = Instant::now();
            for sql in sqls {
                if let Err(err) = taos.exec(sql).await {
                    eprintln!("thread[{i}] exec sql failed, err = {err:?}");
                }
            }

            let elapsed = start.elapsed();
            println!("thread[{i}] exec sqls done, elapsed = {elapsed:?}");
            elapsed
        });

        tasks.push(task);
    }

    let mut durations = Vec::with_capacity(thread_cnt);
    for task in tasks {
        durations.push(task.await.unwrap());
    }

    let elapsed = durations.iter().max();
    println!("Executing sqls took {:?}", elapsed.unwrap());

    println!("exec sqls done, elapsed = {:?}", start.elapsed());
}
