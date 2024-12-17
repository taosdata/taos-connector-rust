#[cfg(test)]
mod stmt {
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    use chrono::Local;
    use flume::{Receiver, Sender};
    use rand::Rng;
    use taos::{AsyncBindable, AsyncQueryable, AsyncTBuilder, ColumnView, Stmt, TaosBuilder};

    // Scenario: 10,000 subtables, each subtable has 10,000 records, a total of 100 million records.
    #[tokio::test]
    async fn test_stmt() -> anyhow::Result<()> {
        // 10,000 subtables
        let subtable_cnt = 10000;
        // 10,000 records per subtable
        let record_cnt = 10000;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        let db = "db_202412171129";

        taos.exec_many([
            &format!("drop database if exists {db}"),
            &format!("create database {db} vgroups 10"),
            &format!("use {db}"),
            "create stable s0 (ts timestamp, c1 int, c2 float, c3 float) tags(t1 int)",
        ])
        .await?;

        create_subtables(db, subtable_cnt).await?;

        let thread_cnt = 4;
        let mut senders = vec![];
        let mut receivers = vec![];

        for _ in 0..thread_cnt {
            let (sender, receiver) = flume::bounded(64);
            senders.push(sender);
            receivers.push(receiver);
        }

        produce_data(senders, subtable_cnt, record_cnt).await;

        consume_data(db, receivers).await;

        taos.exec(format!("drop database {db}")).await?;

        Ok(())
    }

    async fn create_subtables(db: &str, subtable_cnt: usize) -> anyhow::Result<()> {
        println!("Creating subtables start");

        let start = Instant::now();

        // creata table d0 using s0 tags(0) d1 using s0 tags(0) ...
        let mut sql = String::with_capacity(25 * subtable_cnt);
        sql.push_str("create table ");
        for i in 0..subtable_cnt {
            sql.push_str(&format!("d{} using s0 tags(0) ", i));
        }

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many(vec![format!("use {db}"), sql]).await?;

        println!("Creating subtables end, elapsed = {:?}", start.elapsed());

        Ok(())
    }

    async fn produce_data(
        senders: Vec<Sender<(String, Vec<ColumnView>)>>,
        subtable_cnt: usize,
        record_cnt: usize,
    ) {
        let thread_cnt = senders.len();
        let thread_subtable_cnt = subtable_cnt / thread_cnt;

        for i in (0..subtable_cnt).step_by(thread_subtable_cnt) {
            let sender = senders[i].clone();
            tokio::spawn(async move {
                println!("Thread[{}] starts producing data", i / thread_subtable_cnt);

                let mut rng = rand::thread_rng();

                for j in 0..thread_subtable_cnt {
                    let mut tss = Vec::with_capacity(record_cnt);
                    let mut c1s = Vec::with_capacity(record_cnt);
                    let mut c2s = Vec::with_capacity(record_cnt);
                    let mut c3s = Vec::with_capacity(record_cnt);

                    let ts = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    for k in 0..record_cnt {
                        let ts = ts + k as i64;
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

                    sender.send((tbname, cols)).unwrap();
                }

                println!("Thread[{}] ends producing data", i / thread_subtable_cnt);
            });
        }
    }

    async fn consume_data(db: &str, mut receivers: Vec<Receiver<(String, Vec<ColumnView>)>>) {
        let now = Local::now();
        let time = now.format("%Y-%m-%d %H:%M:%S").to_string();
        println!("Consuming data start, time = {time}");

        let start = Instant::now();
        let mut tasks = vec![];

        for i in 0..receivers.len() {
            let db = db.to_owned();
            let receiver = receivers.pop().unwrap();

            let task = tokio::spawn(async move {
                let taos = TaosBuilder::from_dsn("ws://localhost:6041")
                    .unwrap()
                    .build()
                    .await
                    .unwrap();

                taos.exec(format!("use {db}")).await.unwrap();

                let mut stmt = Stmt::init(&taos).await.unwrap();

                let sql = "insert into s0 (tbname, ts, c1, c2, c3) values(?, ?, ?, ?, ?)";
                stmt.prepare(sql).await.unwrap();

                println!("Thread[{i}] starts consuming data");

                let start = Instant::now();

                while let Ok((tbname, cols)) = receiver.recv_async().await {
                    stmt.set_tbname(&tbname).await.unwrap();
                    stmt.bind(&cols).await.unwrap();
                    stmt.execute().await.unwrap();
                }

                println!(
                    "Thread[{i}] ends consuming data, elapsed = {:?}",
                    start.elapsed()
                );
            });

            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap();
        }

        println!("Consuming data end, elapsed = {:?}", start.elapsed());
    }
}

#[cfg(test)]
mod stmt_interlace {
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    use chrono::Local;
    use flume::{Receiver, Sender};
    use rand::Rng;
    use taos::{AsyncBindable, AsyncQueryable, AsyncTBuilder, ColumnView, Stmt, TaosBuilder};

    // Scenario: interlace=1, 1,000,000 subtables, each subtable has 100 records, a total of 100 million records.
    #[tokio::test]
    async fn test_stmt_interlace() -> anyhow::Result<()> {
        // 1,000,000 subtables
        let subtable_cnt = 1000000;
        // 100 records per subtable
        let record_cnt = 100;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        let db = "db_202412171506";

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

        consume_data(db, receivers).await;

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

    async fn produce_data(
        senders: Vec<Sender<Vec<(String, Vec<ColumnView>)>>>,
        subtable_cnt: usize,
        record_cnt: usize,
    ) {
        let batch_cnt = 10000;
        let thread_cnt = senders.len();
        let thread_record_cnt = record_cnt / thread_cnt;

        for i in 0..thread_cnt {
            let sender = senders[i].clone();
            tokio::spawn(async move {
                println!("Thread[{i}] starts producing data");

                let mut rng = rand::thread_rng();

                for _ in 0..thread_record_cnt {
                    let ts = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;

                    for j in (0..subtable_cnt).step_by(batch_cnt) {
                        let mut datas = Vec::with_capacity(batch_cnt);

                        for k in 0..batch_cnt {
                            let c1 = rng.gen::<i32>();
                            let c2: f32 = rng.gen_range(0.0..1000_0000.);
                            let c2 = (c2 * 100.0).round() / 100.0;
                            let c3: f32 = rng.gen_range(0.0..1000_0000.);
                            let c3 = (c3 * 100.0).round() / 100.0;

                            let tbname = format!("d{}", j + k);
                            let cols = vec![
                                ColumnView::from_millis_timestamp(vec![ts]),
                                ColumnView::from_ints(vec![c1]),
                                ColumnView::from_floats(vec![c2]),
                                ColumnView::from_floats(vec![c3]),
                            ];

                            datas.push((tbname, cols));
                        }

                        sender.send(datas).unwrap();
                    }
                }

                println!("Thread[{i}] ends producing data");
            });
        }
    }

    async fn consume_data(db: &str, mut receivers: Vec<Receiver<Vec<(String, Vec<ColumnView>)>>>) {
        let now = Local::now();
        let time = now.format("%Y-%m-%d %H:%M:%S").to_string();
        println!("Consuming data start, time = {time}");

        let start = Instant::now();
        let mut tasks = vec![];

        for i in 0..receivers.len() {
            let db = db.to_owned();
            let receiver = receivers.pop().unwrap();

            let task = tokio::spawn(async move {
                let taos = TaosBuilder::from_dsn("ws://localhost:6041")
                    .unwrap()
                    .build()
                    .await
                    .unwrap();

                taos.exec(format!("use {db}")).await.unwrap();

                let mut stmt = Stmt::init(&taos).await.unwrap();

                let sql = "insert into s0 (tbname, ts, c1, c2, c3) values(?, ?, ?, ?, ?)";
                stmt.prepare(sql).await.unwrap();

                let start = Instant::now();
                println!("Thread[{i}] starts consuming data");

                while let Ok(datas) = receiver.recv() {
                    for (tbname, cols) in datas {
                        stmt.set_tbname(&tbname).await.unwrap();
                        stmt.bind(&cols).await.unwrap();
                        stmt.add_batch().await.unwrap();
                    }
                    stmt.execute().await.unwrap();
                }

                println!(
                    "Thread[{i}] ends consuming data, elapsed = {:?}",
                    start.elapsed()
                );
            });

            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap();
        }

        println!("Consuming data end, elapsed = {:?}", start.elapsed());
    }
}
