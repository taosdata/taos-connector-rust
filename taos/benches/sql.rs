use std::time::{Instant, SystemTime, UNIX_EPOCH};

use chrono::Local;
use rand::Rng;
use taos::{AsyncQueryable, AsyncTBuilder, TaosBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "SQL: One million subtables, each with one thousand records, \
        a total of one billion records."
    );

    // One million subtables
    let subtable_cnt = 1000000;
    // One thousand records per subtable
    let record_cnt = 1000;

    let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
        .build()
        .await?;

    let db = "db_202412172303";

    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db} vgroups 10"),
        &format!("use {db}"),
        "create stable s0 (ts timestamp, c1 int, c2 float, c3 float) tags(t1 int)",
    ])
    .await?;

    create_subtables(db, subtable_cnt).await;

    let sqls = produce_sqls(subtable_cnt, record_cnt).await;

    consume_sqls(db, sqls).await;

    taos.exec(format!("drop database {db}")).await?;

    Ok(())
}

async fn create_subtables(db: &str, subtable_cnt: usize) {
    println!("Creating subtables start");

    let start = Instant::now();
    let batch_cnt = 10000;
    let thread_cnt = 10;
    let thread_subtable_cnt = subtable_cnt / thread_cnt;
    let mut tasks = vec![];

    for i in (0..subtable_cnt).step_by(thread_subtable_cnt) {
        let db = db.to_owned();

        let task = tokio::spawn(async move {
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
        });

        tasks.push(task);
    }

    for task in tasks {
        task.await.unwrap();
    }

    println!("Creating subtables end, elapsed = {:?}", start.elapsed());
}

async fn produce_sqls(subtable_cnt: usize, record_cnt: usize) -> Vec<String> {
    println!("Producing sqls start");

    let start = Instant::now();
    let batch_cnt = 10000;
    let batch_subtable_cnt = batch_cnt / record_cnt;
    let thread_cnt = 10;
    let thread_subtable_cnt = subtable_cnt / thread_cnt;
    let mut handles = Vec::with_capacity(thread_cnt);

    for i in (0..subtable_cnt).step_by(thread_subtable_cnt) {
        let handle = tokio::spawn(async move {
            let mut rng = rand::thread_rng();
            let mut sqls = Vec::with_capacity(thread_subtable_cnt * record_cnt);

            for j in (0..thread_subtable_cnt).step_by(batch_subtable_cnt) {
                let mut sql = String::with_capacity(100 * batch_cnt);
                sql.push_str("insert into ");

                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                for k in 0..batch_subtable_cnt {
                    sql.push_str(&format!("d{} values ", i + j + k));

                    for l in 0..record_cnt {
                        let ts = ts + l as i64;
                        let c1: i32 = rng.gen();
                        let c2: f32 = rng.gen_range(0.0..1000_0000.);
                        let c2: f32 = (c2 * 100.0).round() / 100.0;
                        let c3: f32 = rng.gen_range(0.0..1000_0000.);
                        let c3: f32 = (c3 * 100.0).round() / 100.0;
                        sql.push_str(&format!("({ts},{c1},{c2},{c3}),"));
                    }
                }

                sqls.push(sql);
            }

            sqls
        });

        handles.push(handle);
    }

    let mut sqls = Vec::with_capacity(subtable_cnt * record_cnt);
    for handle in handles {
        sqls.extend(handle.await.unwrap());
    }

    println!("Producing {} sqls", sqls.len());
    println!("Producing sqls end, elapsed = {:?}", start.elapsed());

    sqls
}

async fn consume_sqls(db: &str, sqls: Vec<String>) {
    let now = Local::now();
    let time = now.format("%Y-%m-%d %H:%M:%S").to_string();
    println!("Consuming sqls start, time = {time}");

    let start = Instant::now();
    let thread_cnt = 4;
    let thread_sql_cnt = sqls.len() / thread_cnt;
    let mut tasks = vec![];

    for (i, sqls) in sqls.chunks(thread_sql_cnt).enumerate() {
        let db = db.to_owned();
        let sqls = sqls.to_vec();

        let task = tokio::spawn(async move {
            let taos = TaosBuilder::from_dsn("ws://localhost:6041")
                .unwrap()
                .build()
                .await
                .unwrap();

            taos.exec(format!("use {db}")).await.unwrap();

            println!("Consumer thread[{i}] starts consuming data");

            let start = Instant::now();
            for sql in sqls {
                taos.exec(sql).await.unwrap();
            }

            println!(
                "Consumer thread[{i}] ends consuming data, elapsed = {:?}",
                start.elapsed()
            );
        });

        tasks.push(task);
    }

    for task in tasks {
        task.await.unwrap();
    }

    println!("Consuming sqls end, elapsed = {:?}\n", start.elapsed());
}
