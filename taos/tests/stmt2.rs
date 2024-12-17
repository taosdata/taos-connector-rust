use std::time::{Instant, SystemTime, UNIX_EPOCH};

use chrono::Local;
use flume::{Receiver, Sender};
use rand::Rng;
use taos::{AsyncQueryable, AsyncTBuilder, ColumnView, Stmt2, TaosBuilder};
use taos_query::stmt2::{AsyncBindable, Stmt2BindData};

#[tokio::test]
async fn test_stmt2() -> anyhow::Result<()> {
    let subtable_cnt = 100_0000;
    let data_cnt = 1_0000_0000;

    let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
        .build()
        .await?;

    let db = "db_202412161921";

    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db} vgroups 10"),
        &format!("use {db}"),
        "create stable s0 (ts timestamp, c1 int, c2 float, c3 float) tags(t1 int)",
    ])
    .await?;

    create_subtables(db, subtable_cnt).await;

    let thread_cnt = 4;
    let mut txs = vec![];
    let mut rxs = vec![];
    for _ in 0..thread_cnt {
        let (tx, rx) = flume::bounded(64);
        txs.push(tx);
        rxs.push(rx);
    }

    generate_datas(txs, data_cnt, subtable_cnt).await;

    stmt2_exec(rxs, db).await;

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

async fn generate_datas(txs: Vec<Sender<Stmt2BindData>>, data_cnt: usize, subtable_cnt: usize) {
    let now = Local::now();
    println!("generate datas start, start = {now}");

    let start = Instant::now();

    let batch_cnt = 1_0000;
    // let bind_datas_cnt = data_cnt / batch_cnt;

    let thread_cnt = txs.len();
    let thread_subtable_cnt = subtable_cnt / thread_cnt;
    // let thread_bind_data_cnt = bind_datas_cnt / thread_cnt;
    // let thread_loop_cnt = data_cnt / thread_cnt / subtable_cnt;

    // let mut tasks = Vec::with_capacity(thread_cnt);

    for i in (0..subtable_cnt).step_by(thread_subtable_cnt) {
        let sender = txs[i].clone();
        tokio::spawn(async move {
            println!("thread[{i}] generate datas start");

            let mut rng = rand::thread_rng();
            // 10000 * 10000 = 100000000
            for j in 0..thread_subtable_cnt {
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                // for k in (0..subtable_cnt).step_by(thread_subtable_cnt) {
                let mut tss = Vec::with_capacity(batch_cnt);
                let mut c1s = Vec::with_capacity(batch_cnt);
                let mut c2s = Vec::with_capacity(batch_cnt);
                let mut c3s = Vec::with_capacity(batch_cnt);

                for l in 0..batch_cnt {
                    let ts = ts + l as i64;
                    let c1 = rng.gen::<i32>();
                    let c2: f32 = rng.gen_range(0.0..1000_0000.);
                    let c2 = (c2 * 100.0).round() / 100.0;
                    let c3: f32 = rng.gen_range(0.0..1000_0000.);
                    let c3 = (c3 * 100.0).round() / 100.0;

                    tss.push(ts);
                    c1s.push(c1);
                    c2s.push(c2);
                    c3s.push(c3);
                }

                let tbname = format!("d{}", i + j);
                let cols = vec![
                    ColumnView::from_millis_timestamp(tss),
                    ColumnView::from_ints(c1s),
                    ColumnView::from_floats(c2s),
                    ColumnView::from_floats(c3s),
                ];
                let data = Stmt2BindData::new(Some(tbname), None, Some(cols));
                sender.send(data).unwrap();
                // }
            }
        });
    }

    // let mut bind_datas = Vec::with_capacity(bind_datas_cnt);
    // for task in tasks {
    //     bind_datas.extend(task.await.unwrap());
    // }

    // println!("generate {} datas", bind_datas.len());
    println!("generate datas done, elapsed = {:?}\n", start.elapsed());

    // bind_datas
}

async fn stmt2_exec(mut rxs: Vec<Receiver<Stmt2BindData>>, db: &str) {
    let now = Local::now();
    println!("stmt2 exec start, start = {now}");
    let start = Instant::now();

    let mut tasks = vec![];

    // 4 threads
    for i in 0..rxs.len() {
        let rx = rxs.pop().unwrap();
        let db = db.to_owned();
        let task = tokio::spawn(async move {
            let taos = TaosBuilder::from_dsn("ws://localhost:6041")
                .unwrap()
                .build()
                .await
                .unwrap();

            taos.exec(format!("use {db}")).await.unwrap();

            let mut stmt2 = Stmt2::init(&taos).await.unwrap();

            let sql = "insert into s0 (tbname, ts, c1, c2, c3) values(?, ?, ?, ?, ?)";
            stmt2.prepare(sql).await.unwrap();

            let start = Instant::now();
            println!("thread[{i}] stmt2 exec start");

            while let Ok(datas) = rx.recv() {
                if let Err(err) = stmt2.bind(&[datas]).await {
                    eprintln!("thread[{i}] bind data failed, err = {err:?}");
                }

                let affacted = stmt2.exec().await.unwrap();
                assert_eq!(affacted, 10000);
            }

            println!(
                "thread[{i}] stmt2 exec done, elapsed = {:?}",
                start.elapsed()
            );
        });

        tasks.push(task);

        // println!("Stmt2 exec took {:?}", elapsed.unwrap());
    }

    for task in tasks {
        task.await.unwrap();
    }

    println!("stmt2 exec done, elapsed = {:?}", start.elapsed());
}
