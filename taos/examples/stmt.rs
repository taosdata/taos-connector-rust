use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use rand::Rng;
use taos::{AsyncQueryable, AsyncTBuilder, ColumnView, Stmt2, Taos, TaosBuilder};
use taos_query::stmt2::{AsyncBindable, Stmt2BindData};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subtable_cnt = 100_0000;
    let data_cnt = 1_0000_0000;

    let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
        .build()
        .await?;

    let taos = Arc::new(taos);

    let db = "db_202412130932";

    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db}"),
        &format!("use {db}"),
        "create stable s0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,
            c6 double, c7 binary(20)) tags(t1 int)",
    ])
    .await?;

    create_subtables(taos.clone(), subtable_cnt).await?;

    let datas = generate_datas(data_cnt, subtable_cnt).await;

    stmt_exec(taos.clone(), datas).await?;

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

async fn generate_datas(data_cnt: usize, subtable_cnt: usize) -> Vec<Vec<Stmt2BindData>> {
    println!("generate datas start");

    let start = Instant::now();

    let batch_cnt = 1_0000;
    let bind_datas_cnt = data_cnt / batch_cnt;

    let thread_cnt = 10;
    let thread_bind_data_cnt = bind_datas_cnt / thread_cnt;
    let thread_loop_cnt = data_cnt / thread_cnt / subtable_cnt;

    let mut tasks = Vec::with_capacity(thread_cnt);

    for i in 0..thread_cnt {
        let task = tokio::spawn(async move {
            println!("thread[{i}] generate datas start");

            let start = Instant::now();
            let mut rng = rand::thread_rng();
            let mut bind_datas = Vec::with_capacity(thread_bind_data_cnt);

            for _ in 0..thread_loop_cnt {
                for k in (0..subtable_cnt).step_by(batch_cnt) {
                    let mut datas = Vec::with_capacity(batch_cnt);
                    for l in 0..batch_cnt {
                        let ts = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64;

                        let c1 = rng.gen::<bool>();
                        let c2 = rng.gen::<i8>();
                        let c3 = rng.gen::<i16>();
                        let c4 = rng.gen::<i32>();
                        let c5 = rng.gen::<i64>();
                        let c6: f64 = rng.gen_range(0.0..1000_0000.);
                        let c6 = (c6 * 100.0).round() / 100.0;
                        let c7: String = (0..20).map(|_| rng.gen_range('a'..='z')).collect();

                        let cols = vec![
                            ColumnView::from_millis_timestamp(vec![ts]),
                            ColumnView::from_bools(vec![c1]),
                            ColumnView::from_tiny_ints(vec![c2]),
                            ColumnView::from_small_ints(vec![c3]),
                            ColumnView::from_ints(vec![c4]),
                            ColumnView::from_big_ints(vec![c5]),
                            ColumnView::from_doubles(vec![c6]),
                            ColumnView::from_varchar(vec![c7]),
                        ];

                        let tbname = format!("d{}", k + l);
                        let data = Stmt2BindData::new(Some(tbname), None, Some(cols));
                        datas.push(data);
                    }

                    bind_datas.push(datas);
                }
            }

            println!(
                "thread[{i}] generate datas done, elapsed = {:?}",
                start.elapsed()
            );

            bind_datas
        });

        tasks.push(task);
    }

    let mut bind_datas = Vec::with_capacity(bind_datas_cnt);
    for task in tasks {
        bind_datas.extend(task.await.unwrap());
    }

    println!("generate {} datas", bind_datas.len());
    println!("generate datas done, elapsed = {:?}", start.elapsed());

    bind_datas
}

async fn stmt_exec(taos: Arc<Taos>, datas: Vec<Vec<Stmt2BindData>>) -> anyhow::Result<()> {
    println!("stmt2 exec start");

    let start = Instant::now();

    let thread_cnt = 10;
    let chunk_size = datas.len() / thread_cnt;

    let mut tasks = vec![];

    for (i, chunk) in datas.chunks(chunk_size).enumerate() {
        let chunk = chunk.to_vec();
        let taos = taos.clone();

        let task = tokio::spawn(async move {
            let start = Instant::now();
            println!("thread[{i}] stmt2 exec start");

            let mut stmt2 = Stmt2::init(&taos).await.unwrap();

            let sql = "insert into ? using s0 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            stmt2.prepare(sql).await.unwrap();

            for data in chunk {
                if let Err(err) = stmt2.bind(&data).await {
                    eprintln!("thread[{i}] bind data failed, err = {err:?}");
                }

                let affacted = stmt2.exec().await.unwrap();
                assert_eq!(affacted, 10000);
            }

            let elapsed = start.elapsed();
            println!("thread[{i}] stmt2 exec done, elapsed = {elapsed:?}");
            elapsed
        });

        tasks.push(task);
    }

    let durations: Vec<_> = futures::future::join_all(tasks)
        .await
        .into_iter()
        .filter_map(|res| res.ok())
        .collect();

    let elapsed = durations.iter().max();
    println!("Stmt2 exec took {:?}", elapsed.unwrap());
    println!("stmt2 exec done, elapsed = {:?}", start.elapsed());

    Ok(())
}
