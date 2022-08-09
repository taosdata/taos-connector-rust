# The Official Rust Connector for [TDengine]

| Crates.io Version                                     | Crates.io Downloads                                   | CodeCov                                                                                                                                         |
| ----------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| ![Crates.io](https://img.shields.io/crates/v/taos) | ![Crates.io](https://img.shields.io/crates/d/taos) | [![codecov](https://codecov.io/gh/taosdata/taos-connector-rust/branch/main/graph/badge.svg?token=P11UKNLTVO)](https://codecov.io/gh/taosdata/taos-connector-rust) |

This is the official TDengine connector in Rust.

## Dependencies

- [Rust](https://www.rust-lang.org/learn/get-started) of course.

if you use the default features, it'll depend on:

- [TDengine] Client library and headers.

## Usage

For default C-based client API, set in Cargo.toml

```toml
[dependencies]
taos = "*"
```

For r2d2 support:

```toml
[dependencies]
taos = { version = "*", features = ["r2d2"] }
```

For REST client:

```toml
[dependencies]
taos = { version = "*", features = ["rest"] }
```

```rust
#[tokio::main]
async fn main() -> Result<(), Error> {
    init();
    let taos = taos_connect()?;

    assert_eq!(
        taos.query("drop database if exists demo").await.is_ok(),
        true
    );
    assert_eq!(taos.query("create database demo").await.is_ok(), true);
    assert_eq!(taos.query("use demo").await.is_ok(), true);
    assert_eq!(
        taos.query("create table m1 (ts timestamp, speed int)")
            .await
            .is_ok(),
        true
    );

    for i in 0..10i32 {
        assert_eq!(
            taos.query(format!("insert into m1 values (now+{}s, {})", i, i).as_str())
                .await
                .is_ok(),
            true
        );
    }
    let rows = taos.query("select * from m1").await?;

    println!("{}", rows.column_meta.into_iter().map(|col| col.name).join(","));
    for row in rows.rows {
        println!("{}", row.into_iter().join(","));
    }
    Ok(())
}
```

## Contribution

Welcome for all contributions.

## License

Keep same with [TDengine].

[TDengine]: https://www.taosdata.com/en/getting-started/
[r2d2]: https://crates.io/crates/r2d2
