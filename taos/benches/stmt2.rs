use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::Local;
use flume::{Receiver, Sender};
use rand::Rng;
use taos::*;

const DSN: &str = "ws://localhost:6041";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "Stmt2: One million subtables, each with one thousand records, \
        a total of one billion records."
    );

    // One million subtables
    let subtable_cnt = 1000000;
    // One thousand records per subtable
    let record_cnt = 1000;

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let db = &format!("db_{}", ts);

    let taos = TaosBuilder::from_dsn(DSN)?.build().await?;
    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db} vgroups 10"),
        &format!("use {db}"),
        "create stable s0 (ts timestamp, c1 int, c2 float, c3 float) tags(t1 int)",
    ])
    .await?;

    create_subtables(db, subtable_cnt).await;

    let thread_cnt = 4;
    let mut senders = vec![];
    let mut receivers = vec![];

    for _ in 0..thread_cnt {
        let (sender, receiver) = flume::bounded(64);
        senders.push(sender);
        receivers.push(receiver);
    }

    produce_data(senders, subtable_cnt, record_cnt).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    consume_data(db, receivers).await;

    check_count(&taos, subtable_cnt * record_cnt).await?;

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
            let taos = TaosBuilder::from_dsn(DSN).unwrap().build().await.unwrap();
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

async fn produce_data(
    senders: Vec<Sender<Vec<Stmt2BindParam>>>,
    subtable_cnt: usize,
    record_cnt: usize,
) {
    let batch_cnt = 10000;
    let batch_subtable_cnt = batch_cnt / record_cnt;
    let thread_cnt = senders.len();
    let thread_subtable_cnt = subtable_cnt / thread_cnt;

    for i in (0..subtable_cnt).step_by(thread_subtable_cnt) {
        let idx = i / thread_subtable_cnt;
        let sender = senders[idx].clone();
        tokio::spawn(async move {
            println!("Producer thread[{idx}] starts producing data");

            let mut rng = rand::thread_rng();

            for j in (0..thread_subtable_cnt).step_by(batch_subtable_cnt) {
                let mut params = Vec::with_capacity(batch_subtable_cnt);

                for k in 0..batch_subtable_cnt {
                    let mut tss = Vec::with_capacity(record_cnt);
                    let mut c1s = Vec::with_capacity(record_cnt);
                    let mut c2s = Vec::with_capacity(record_cnt);
                    let mut c3s = Vec::with_capacity(record_cnt);

                    let ts = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    for l in 0..record_cnt {
                        let ts = ts + l as i64;
                        let c1 = rng.gen::<i32>();
                        let c2: f32 = rng.gen_range(0.0..10000000.);
                        let c2 = (c2 * 100.0).round() / 100.0;
                        let c3: f32 = rng.gen_range(0.0..10000000.);
                        let c3 = (c3 * 100.0).round() / 100.0;

                        tss.push(ts);
                        c1s.push(c1);
                        c2s.push(c2);
                        c3s.push(c3);
                    }

                    let tbname = format!("d{}", i + j + k);
                    let cols = vec![
                        ColumnView::from_millis_timestamp(tss),
                        ColumnView::from_ints(c1s),
                        ColumnView::from_floats(c2s),
                        ColumnView::from_floats(c3s),
                    ];

                    let param = Stmt2BindParam::new(Some(tbname), None, Some(cols));
                    params.push(param);
                }

                sender.send(params).unwrap();
            }

            println!("Producer thread[{idx}] ends producing data");
        });
    }
}

async fn consume_data(db: &str, mut receivers: Vec<Receiver<Vec<Stmt2BindParam>>>) {
    let now = Local::now();
    let time = now.format("%Y-%m-%d %H:%M:%S").to_string();
    println!("Consuming data start, time = {time}");

    let start = Instant::now();
    let mut tasks = vec![];

    for i in 0..receivers.len() {
        let db = db.to_owned();
        let receiver = receivers.pop().unwrap();

        let task = tokio::spawn(async move {
            let taos = TaosBuilder::from_dsn(DSN).unwrap().build().await.unwrap();
            taos.exec(format!("use {db}")).await.unwrap();

            let mut stmt2 = Stmt2::init(&taos).await.unwrap();

            let sql = "insert into s0 (tbname, ts, c1, c2, c3) values(?, ?, ?, ?, ?)";
            stmt2.prepare(sql).await.unwrap();

            println!("Consumer thread[{i}] starts consuming data");

            let start = Instant::now();
            while let Ok(params) = receiver.recv_async().await {
                stmt2.bind(&params).await.unwrap();
                stmt2.exec().await.unwrap();
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

    println!("Consuming data end, elapsed = {:?}\n", start.elapsed());
}

async fn check_count(taos: &Taos, cnt: usize) -> anyhow::Result<()> {
    #[derive(Debug, serde::Deserialize)]
    struct Record {
        cnt: usize,
    }

    let res: Vec<Record> = taos
        .query("select count(*) as cnt from s0")
        .await?
        .deserialize()
        .try_collect()
        .await?;

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].cnt, cnt);

    Ok(())
}