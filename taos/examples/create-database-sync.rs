use taos::sync::*;
use anyhow::Result;

fn main() -> Result<()> {
    let taos = TaosBuilder::from_dsn("taos://")?.build()?;
    taos.exec("create database if not exists db1")?;
    Ok(())
}
