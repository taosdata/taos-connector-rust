use taos_optin::TaosBuilder;
use taos_query::{AsyncTBuilder, RawResult};

/*
cargo test --package taos-optin --test test_stmt2 -- test_stmt2 --exact --nocapture


RUSTFLAGS="-Z sanitizer=address --cfg tokio_unstable" cargo +nightly test -Z build-std --target x86_64-unknown-linux-gnu -p taos-optin --test test_stmt2 -- test_stmt2 --exact --nocapture

*/

#[tokio::test]
async fn test_stmt2() -> RawResult<()> {
    let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
        .build()
        .await?;

    // taos.exec_many([
    //     "drop database if exists test_1768466795",
    //     "create database test_1768466795",
    //     "use test_1768466795",
    //     "create table t0 (ts timestamp, c1 int, c2 varchar(50))",
    // ])
    // .await?;

    // let mut stmt2 = Stmt2::init(&taos).await?;
    // stmt2.prepare("insert into t0 values(?, ?, ?)").await?;

    // let cols = vec![
    //     ColumnView::from_millis_timestamp(vec![
    //         1726803356466,
    //         1726803357466,
    //         1726803358466,
    //         1726803359466,
    //     ]),
    //     ColumnView::from_ints(vec![99, 100, 101, 102]),
    //     ColumnView::from_varchar(vec!["hello", "taos", "stmt2", "world"]),
    // ];

    // let param = Stmt2BindParam::new(None, None, Some(cols));
    // stmt2.bind(&[param]).await?;

    // let affected = stmt2.exec().await?;
    // assert_eq!(affected, 4);

    Ok(())
}
