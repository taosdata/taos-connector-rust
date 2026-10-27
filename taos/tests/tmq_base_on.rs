// Copyright (c) 2022 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::str::FromStr;
use std::time::Duration;

use taos::sync::*;

// VST inheritance (BASE ON) subscription integration test.
//
// Manual / live-cluster test (mirrors tmq_sync.rs: `#[test]` is commented out so it
// does not run in a normal unit-test pass). Run explicitly against a running taosd:
//
//   TEST_DSN=taos://localhost:6030 \
//     cargo test -p taos --test tmq_base_on test_tmq_base_on_sync -- --nocapture --ignored
//
// It subscribes to a "with meta as database" topic, consumes the json meta produced
// by a live server for CREATE STABLE ... BASE ON and ALTER ... ADD/DROP BASE ON, and
// asserts the meta renders the inheritance back into replayable DDL. This exercises
// the real wire path that the meta.rs unit tests (fixed JSON) cannot.
#[test]
#[ignore = "requires a running taosd (live cluster); run explicitly with --ignored"]
fn test_tmq_base_on_sync() -> anyhow::Result<()> {
    use taos_query::prelude::sync::*;
    let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
    let mut dsn = Dsn::from_str(&dsn)?;

    let taos = TaosBuilder::from_dsn(&dsn)?.build()?;
    taos.exec_many([
        "drop topic if exists t_base_on_rs",
        "drop database if exists t_base_on_rs",
        "create database t_base_on_rs vgroups 2 wal_retention_period 3600",
        "create topic t_base_on_rs with meta as database t_base_on_rs",
        "use t_base_on_rs",
        // parents + inherited child + standalone with ADD/DROP BASE ON
        "create stable p_device (ts timestamp, status int, temp float) tags (region int, site binary(32)) virtual 1",
        "create stable p_metric (ts timestamp, val double) tags (unit nchar(8)) virtual 1",
        "create stable leaf_child (ts timestamp, accuracy int) tags (sensor_id int) base on t_base_on_rs.p_device, t_base_on_rs.p_metric virtual 1",
        "create stable standalone (ts timestamp, own_col int) tags (own_tag int) virtual 1",
        "alter stable t_base_on_rs.standalone add base on t_base_on_rs.p_metric",
        "alter stable t_base_on_rs.standalone drop base on t_base_on_rs.p_metric",
    ])?;

    // target db for raw-meta replay (cross-db inheritance reconstruction)
    taos.exec_many([
        "drop database if exists t_base_on_rs2",
        "create database t_base_on_rs2 vgroups 2 wal_retention_period 3600",
    ])?;

    dsn.params
        .insert("group.id".to_string(), "base_on_rs".to_string());
    dsn.params
        .insert("auto.offset.reset".to_string(), "earliest".to_string());
    let builder = TmqBuilder::from_dsn(&dsn)?;
    let mut consumer = builder.build()?;
    consumer.subscribe(["t_base_on_rs"])?;

    let mut saw_create_baseon = false;
    let mut saw_create_standalone_no_baseon = false;
    let mut saw_alter_add = false;
    let mut saw_alter_drop = false;

    let taos2 = {
        let mut d2 = dsn.clone();
        d2.params.clear();
        d2.subject = Some("t_base_on_rs2".to_string());
        TaosBuilder::from_dsn(&d2)?.build()?
    };

    for message in consumer.iter_with_timeout(Timeout::from_secs(5)) {
        let (offset, message) = message?;
        let _ = offset.topic();
        if let MessageSet::Meta(meta) = message {
            // 1. raw replay into the target db -> reconstructs inheritance cross-db
            let raw = meta.as_raw_meta()?;
            // replay may report "already applied" for per-vnode duplicate alters; tolerate.
            let _ = taos2.write_raw_meta(&raw);

            // 2. json meta must render BASE ON back into DDL for inherited objects
            let json = meta.as_json_meta()?;
            for m in json.iter() {
                let sql = m.to_string();
                if sql.contains("`leaf_child`") && sql.contains("CREATE") {
                    assert!(
                        sql.contains("BASE ON")
                            && sql.contains("`p_device`")
                            && sql.contains("`p_metric`"),
                        "leaf_child create DDL must carry BASE ON parents: {sql}"
                    );
                    saw_create_baseon = true;
                }
                if sql.contains("`standalone`") && sql.contains("CREATE") {
                    assert!(
                        !sql.contains("BASE ON"),
                        "standalone create DDL must NOT carry BASE ON: {sql}"
                    );
                    saw_create_standalone_no_baseon = true;
                }
                if sql.contains("ADD BASE ON") && sql.contains("`p_metric`") {
                    saw_alter_add = true;
                }
                if sql.contains("DROP BASE ON") && sql.contains("`p_metric`") {
                    saw_alter_drop = true;
                }
            }
        }
        consumer.commit(offset)?;
    }
    consumer.unsubscribe();

    assert!(
        saw_create_baseon,
        "never saw CREATE leaf_child with BASE ON"
    );
    assert!(
        saw_create_standalone_no_baseon,
        "never saw CREATE standalone without BASE ON"
    );
    assert!(saw_alter_add, "never saw ALTER ADD BASE ON");
    assert!(saw_alter_drop, "never saw ALTER DROP BASE ON");

    // inheritance reconstructed in the replay target
    let count: i64 = taos2
        .query_one::<_, i64>(
            "select count(*) from information_schema.ins_vstable_inherits \
             where db_name='t_base_on_rs2' and child_stable_name='leaf_child'",
        )?
        .unwrap_or(0);
    assert_eq!(count, 2, "replayed leaf_child should inherit 2 parents");

    std::thread::sleep(Duration::from_secs(1));
    taos.exec_many([
        "drop topic if exists t_base_on_rs",
        "drop database if exists t_base_on_rs",
        "drop database if exists t_base_on_rs2",
    ])?;
    Ok(())
}
