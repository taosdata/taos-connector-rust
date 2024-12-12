use std::{sync::Arc, time::Instant};

use rand::Rng;
use taos::{AsyncQueryable, AsyncTBuilder, Taos, TaosBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "ERROR");
    pretty_env_logger::env_logger::init();

    // let subtable_cnt = 100_0000;
    let data_cnt = 1_0000_0000;
    let batch_cnt = 1_0000;
    let sql_cnt = data_cnt / batch_cnt;

    // let subtable_cnt = 100;
    // let data_cnt = 1_0000;
    // let batch_cnt = 100;
    // let sql_cnt = data_cnt / batch_cnt;

    let thread_cnt = 10;

    let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
        .build()
        .await?;

    let db = "db_202412111551";

    taos.exec_many([
        // &format!("drop database if exists {db}"),
        // &format!("create database {db}"),
        &format!("use {db}"),
        // "create stable s0 (ts timestamp, c1 bool, c2 tinyint, c3 tinyint unsigned, c4 smallint,
        //     c5 smallint unsigned, c6 int, c7 int unsigned, c8 bigint, c9 bigint unsigned, c10 float,
        //     c11 double, c12 binary(30), c13 nchar(30), c14 varbinary(30))
        //     tags(t1 int)",
    ])
    .await?;

    // tracing::error!("create subtables start");

    // let start = Instant::now();

    // // create subtables
    // for i in 0..subtable_cnt {
    //     let sql = format!("create table d{i} using s0 tags(0)");
    //     taos.exec(sql).await?;
    // }

    // tracing::error!(elapsed = ?start.elapsed(), "create subtables done");
    tracing::error!("generate sqls start");

    let start = Instant::now();

    // generate sqls
    let sqls = gen_sqls(sql_cnt, batch_cnt);

    tracing::error!(elapsed = ?start.elapsed(), "generate sqls done");
    tracing::error!("exec sqls start");

    let start = Instant::now();

    // execute sqls
    let taos = Arc::new(taos);
    exec_sqls(taos.clone(), sqls, thread_cnt).await;

    tracing::error!(elapsed = ?start.elapsed(), "exec sqls done");

    taos.exec(format!("drop database {db}")).await?;

    Ok(())
}

fn gen_sqls(sql_cnt: usize, batch_cnt: usize) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let mut sqls = Vec::with_capacity(sql_cnt);
    for _ in 0..sql_cnt {
        // insert into d0 values() d1 values() ...
        let mut sql = "insert into ".to_owned();
        for j in 0..batch_cnt {
            let c1: bool = rng.gen();
            let c2: i8 = rng.gen();
            let c3: u8 = rng.gen();
            let c4: i16 = rng.gen();
            let c5: u16 = rng.gen();
            let c6: i32 = rng.gen();
            let c7: u32 = rng.gen();
            let c8: i64 = rng.gen();
            let c9: u64 = rng.gen();
            let c10: f32 = rng.gen::<f32>() * 1000_0000.;
            let c11: f64 = rng.gen::<f64>() * 1000_0000.;
            let c12: String = (0..30).map(|_| rng.gen_range('a'..='z')).collect();
            let c13: String = (0..30).map(|_| rng.gen_range('a'..='z')).collect();
            let c14: String = (0..30).map(|_| rng.gen_range('a'..='z')).collect();

            sql.push_str(&format!(
                "d{j} values(now, {c1}, {c2}, {c3}, {c4}, {c5}, {c6}, {c7}, {c8}, {c9}, {c10}, {c11}, '{c12}', '{c13}', '{c14}') "
            ));
        }
        sqls.push(sql);
    }

    sqls
}

async fn exec_sqls(taos: Arc<Taos>, sqls: Vec<String>, thread_cnt: usize) {
    let mut tasks = vec![];

    let chunk_size = sqls.len() / thread_cnt;
    for (i, chunk) in sqls.chunks(chunk_size).enumerate() {
        let chunk = chunk.to_vec();
        let taos = taos.clone();
        let task = tokio::spawn(async move {
            let start = Instant::now();
            for sql in chunk {
                taos.exec(sql).await.unwrap();
            }
            let ms = start.elapsed().as_millis();
            tracing::error!("thread[{i}] takes [{ms}]ms to exec sqls");
            ms
        });

        tasks.push(task);
    }

    let mut total_time = 0;
    for task in tasks {
        total_time += task.await.unwrap();
    }
    tracing::error!("exec sqls total time: {total_time}ms");
}

#[cfg(test)]
mod tests {
    use super::gen_sqls;

    #[test]
    fn test_gen_sqls() {
        let sql_cnt = 1;
        let batch_cnt = 2;
        let sqls = gen_sqls(sql_cnt, batch_cnt);
        println!("{sqls:?}");
    }
}
