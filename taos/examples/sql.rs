use std::{sync::Arc, time::Instant};

use rand::Rng;
use taos::{AsyncQueryable, AsyncTBuilder, Taos, TaosBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subtable_cnt = 100_0000;
    let data_cnt = 1_0000_0000;

    let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
        .build()
        .await?;

    let taos = Arc::new(taos);

    let db = "db_202412111551";

    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db}"),
        &format!("use {db}"),
        "create stable s0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 double, c7 binary(20)) tags(t1 int)",
    ])
    .await?;

    create_subtables(taos.clone(), subtable_cnt).await?;

    let sqls = generate_sqls(data_cnt, subtable_cnt).await;

    exec_sqls(taos.clone(), sqls).await;

    taos.exec(format!("drop database {db}")).await?;

    Ok(())
}

async fn create_subtables(taos: Arc<Taos>, subtable_cnt: usize) -> anyhow::Result<()> {
    println!("create subtables start");
    let start = Instant::now();
    let batch_cnt = 1_0000;
    for i in (0..subtable_cnt).step_by(batch_cnt) {
        // creata table d0 using s0 tags(0) d1 using s0 tags(0) ...
        let mut sql = String::with_capacity(25 * batch_cnt);
        sql.push_str("create table ");
        for j in 0..batch_cnt {
            sql.push_str(&format!("d{} using s0 tags(0) ", i + j));
        }
        taos.exec(sql).await?;
    }
    println!("create subtables done, elapsed = {:?}", start.elapsed());
    Ok(())
}

async fn generate_sqls(data_cnt: usize, subtable_cnt: usize) -> Vec<String> {
    println!("generate sqls start");

    let start = Instant::now();

    let batch_cnt = 1_0000;
    let sql_cnt = data_cnt / batch_cnt;

    let thread_cnt = 10;
    let thread_sql_cnt = sql_cnt / thread_cnt;
    let thread_loop_cnt = data_cnt / thread_cnt / subtable_cnt;

    let mut tasks = Vec::with_capacity(thread_cnt);

    for i in 0..thread_cnt {
        let task = tokio::spawn(async move {
            println!("thread[{i}] generate sqls start");

            let start = Instant::now();
            let mut rng = rand::thread_rng();
            let mut sqls = Vec::with_capacity(thread_sql_cnt);

            for _ in 0..thread_loop_cnt {
                for k in (0..subtable_cnt).step_by(batch_cnt) {
                    // insert into d0 values() d1 values() ...
                    let mut sql = String::with_capacity(100 * batch_cnt);
                    sql.push_str("insert into ");
                    for l in 0..batch_cnt {
                        let tbname = format!("d{}", k + l);

                        let c1 = rng.gen::<bool>() as u8;
                        let c2 = rng.gen::<i8>();
                        let c3 = rng.gen::<i16>();
                        let c4 = rng.gen::<i32>();
                        let c5 = rng.gen::<i64>();
                        let c6: f64 = rng.gen_range(0.0..1000_0000.);
                        let c6 = (c6 * 100.0).round() / 100.0;
                        let c7: String = (0..20).map(|_| rng.gen_range('a'..='z')).collect();

                        sql.push_str(&format!(
                            "{tbname} values(now,{c1},{c2},{c3},{c4},{c5},{c6},'{c7}') "
                        ));
                    }

                    sqls.push(sql);
                }
            }

            println!(
                "thread[{i}] generate sqls done, elapsed = {:?}",
                start.elapsed()
            );

            sqls
        });

        tasks.push(task);
    }

    let mut sqls = Vec::with_capacity(sql_cnt);
    for task in tasks {
        sqls.extend(task.await.unwrap());
    }

    println!("generate {} sqls", sqls.len());
    println!("generate sqls done, elapsed = {:?}", start.elapsed());

    sqls
}

async fn exec_sqls(taos: Arc<Taos>, sqls: Vec<String>) {
    println!("exec sqls start");

    let start = Instant::now();

    let thread_cnt = 10;
    let chunk_size = sqls.len() / thread_cnt;

    let mut tasks = vec![];

    for (i, chunk) in sqls.chunks(chunk_size).enumerate() {
        let chunk = chunk.to_vec();
        let taos = taos.clone();

        let task = tokio::spawn(async move {
            println!("thread[{i}] exec sqls start");
            let start = Instant::now();
            for sql in chunk {
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
