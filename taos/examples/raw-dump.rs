use std::{
    ops::Deref,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::bail;
use clap::Parser;
use sync::MessageSet;
use taos::*;

#[derive(Debug, Parser)]
struct Opts {
    /// The target to connect to.
    #[clap(
        default_value = "tmq://localhost:6030/ts5250?group.id=dump&experimental.snapshot.enable=true&auto.offset.reset=earliest"
    )]
    tmq: String,

    /// Read raw data from directory.
    #[clap(short, long, default_value = "./")]
    raw_dir: PathBuf,

    /// Print data block
    #[clap(short, long)]
    print_block: bool,

    /// Do not dump meta message.
    #[clap(long)]
    no_meta: bool,

    /// Do not dump metadata message (`insert into .. using ...`).
    #[clap(long)]
    no_metadata: bool,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // std::env::set_var("RUST_LOG", "trace");
    pretty_env_logger::init();

    let args = Opts::parse();
    if args.raw_dir.exists() {
        if !args.raw_dir.is_dir() {
            bail!("{} is not a directory", args.raw_dir.display());
        }
    } else {
        std::fs::create_dir_all(&args.raw_dir)?;
    }

    let mut dsn: Dsn = args.tmq.parse()?;

    let topic = dsn
        .subject
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing topic"))?;

    // subscribe
    let group = chrono::Local::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow::anyhow!("can't get nanosecond timestamp"))?;
    if dsn.get("group.id").is_none() {
        dsn.params.insert("group.id".to_string(), group.to_string());
    }
    eprintln!("group id: {}", dsn.get("group.id").unwrap());

    let tmq = TmqBuilder::from_dsn(&dsn)?;

    let mut consumer = tmq.build().await?;
    consumer.subscribe([&topic]).await?;
    // let assignment = consumer.assignments().await;
    // println!("assignments: {:?}", assignment);

    let blocks_cost = Duration::ZERO;
    {
        let mut stream = consumer.stream();
        eprintln!("start consuming");

        let begin = Instant::now();

        let mut mid = 0;
        println!("id,type,size");
        while let Some((offset, message)) = stream.try_next().await? {
            // println!("{mid} offset: {:?}", offset);
            // get information from offset
            match message {
                MessageSet::Meta(meta) => {
                    if args.no_meta {
                        continue;
                    }
                    let raw = meta.as_raw_meta().await?;
                    let bytes = raw.as_bytes();
                    println!("{mid},meta,{}", bytes.len());
                    let path = args.raw_dir.join(format!("raw_{}_meta.bin", mid));
                    std::fs::write(path, bytes.deref())?;
                }
                MessageSet::Data(data) => {
                    // println!("{mid} data: {:?}", data);
                    let raw = data.as_raw_data().await?;
                    let bytes = raw.as_bytes();
                    println!("{mid},data,{}", bytes.len());

                    if args.print_block {
                        while let Some(block) = data.fetch_raw_block().await? {
                            // one block for one table, get table name if needed
                            let name = block.table_name();
                            let nrows = block.nrows();
                            let ncols = block.ncols();
                            if let Some(name) = name {
                                eprintln!(
                                    "data[{mid}] block with table name {}, cols: {}, rows: {}",
                                    name, ncols, nrows
                                );
                                eprintln!("{}", block.pretty_format());
                            } else {
                                eprintln!(
                                    "data[{mid}] block without table name, cols: {}, rows: {}",
                                    ncols, nrows
                                );
                                eprintln!("{}", block.pretty_format());
                            }
                        }
                    }
                    let path = args.raw_dir.join(format!("raw_{}_data.bin", mid));
                    std::fs::write(path, bytes.deref())?;
                }
                MessageSet::MetaData(meta, data) => {
                    if args.no_metadata {
                        continue;
                    }
                    // println!("{mid} meta data: {:?}", meta);
                    let raw = meta.as_raw_meta().await?;
                    let bytes = raw.as_bytes();
                    println!("{mid},metadata,{}", bytes.len());
                    if args.print_block {
                        while let Some(block) = data.fetch_raw_block().await? {
                            // one block for one table, get table name if needed
                            let name = block.table_name();
                            let nrows = block.nrows();
                            let ncols = block.ncols();
                            if let Some(name) = name {
                                eprintln!(
                                    "data[{mid}] block with table name {}, cols: {}, rows: {}",
                                    name, ncols, nrows
                                );
                                eprintln!("{}", block.pretty_format());
                            } else {
                                eprintln!(
                                    "data[{mid}] block without table name, cols: {}, rows: {}",
                                    ncols, nrows
                                );
                                eprintln!("{}", block.pretty_format());
                            }
                        }
                    }
                    let path = args.raw_dir.join(format!("raw_{}_metadata.bin", mid));
                    std::fs::write(path, bytes.deref())?;
                }
            }
            mid += 1;

            consumer.commit(offset).await?;
        }
        eprintln!("total cost: {:?}", begin.elapsed());
    }
    eprintln!("blocks cost: {:?}", blocks_cost);

    consumer.unsubscribe().await;

    Ok(())
}
