use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::formatted_timed_builder().init();
    let dsn = "taos://root:taosdata@";

    let pool = TaosBuilder::from_dsn(dsn)?.pool()?;

    let taos = pool.get().await?;
    log::trace!("got connection");
    // Query options 2, use deserialization with serde.
    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        db_name: String,
        table_name: String,
        vgroup_id: i32,
    }

    let records: Vec<Record> = taos
        .query("select table_name, db_name, vgroup_id from information_schema.ins_tables where vgroup_id is not null")
        .await?
        .deserialize()
        .try_collect()
        .await?;
    dbg!(&records);
    for record in &records {
        let vgid = taos
            .table_vgroup_id(&record.db_name, &record.table_name)
            .await
            .unwrap();
        dbg!(&vgid);
        assert_eq!(vgid, record.vgroup_id);
    }

    let groups = records.iter().into_group_map_by(|r| r.db_name.clone());

    for (db, records) in groups {
        println!("db_name: {}", db);
        let tables = records.iter().map(|r| r.table_name.clone()).collect::<Vec<_>>();

        let ids = taos.tables_vgroup_ids(&db, &tables).await.unwrap();
        dbg!(&ids);
    }

    Ok(())
}
