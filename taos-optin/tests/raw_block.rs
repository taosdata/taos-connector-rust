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

#[test]
fn raw_block() -> anyhow::Result<()> {
    use taos_optin::TaosBuilder;
    use taos_query::prelude::sync::*;

    let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
    let mut rs = taos.query("show databases")?;
    let field_count = rs.num_of_fields();
    let inner = rs.fetch_raw_block()?.unwrap();

    // let inner = unsafe { Raw::from_ptr(ptr, rows as _, field_count as _, precision) };
    let gid = inner.group_id();
    println!("group id: {gid}");

    dbg!(inner.schemas());

    dbg!(unsafe { inner.get_ref_unchecked(0, field_count - 1) });
    for row in 0..dbg!(inner.nrows()) as usize {
        for col in 0..field_count {
            println!("({row}, {col}): ");
            let v = unsafe { inner.get_ref_unchecked(row, col) };
            dbg!(v);
        }
    }
    Ok(())
}

#[tokio::test]
async fn raw_block_async() -> anyhow::Result<()> {
    use taos_optin::TaosBuilder;
    use taos_query::prelude::*;
    let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
    let mut rs = taos.query("show databases").await?;
    if let Some(inner) = rs.blocks().try_next().await? {
        // let inner = unsafe { Raw::from_ptr(ptr, rows as _, field_count as _, precision) };
        let gid = inner.group_id();
        println!("group id: {gid}");

        dbg!(inner.schemas());

        for row in 0..dbg!(inner.nrows()) as usize {
            for col in 0..inner.ncols() {
                println!("({row}, {col}): ");
                let v = unsafe { inner.get_ref_unchecked(row, col) };
                dbg!(v);
            }
        }
    }

    Ok(())
}

#[test]
fn raw_block_full_test() -> anyhow::Result<()> {
    use taos_optin::TaosBuilder;
    use taos_query::prelude::sync::*;

    let taos = TaosBuilder::from_dsn("taos:///")?.build()?;

    let _ = taos.query("drop database if exists _rs_ts_raw_block_full_")?;
    let _ = taos.query("create database if not exists _rs_ts_raw_block_full_")?;
    let _ = taos.query("use _rs_ts_raw_block_full_")?;
    let _ = taos.query("create stable stb1 (ts timestamp,vb bool,vi8 tinyint,vi16 smallint,\
        vi32 int,vi64 bigint, vu8 tinyint unsigned,vu16 smallint unsigned,vu32 int unsigned,vu64 bigint unsigned,\
        vf float,vd double,vv varchar(100), vn nchar(100)) tags(tj json)")?;

    let _ = taos.query(
        "insert into tb1 using stb1 tags(NULL) values\
        (1655793421375,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
    )?;
    let _ = taos.query(
        "insert into tb2 using stb1 tags('{\"a\":\"涛思𝄞数据\"}') values\
        (1655793421374,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)\
        (1655793421375,true, -1, -1, -1, -1, 1, 1, 1, 1, 0.0, 0.0, 'abc', '涛思𝄞数据')",
    )?;
    // let rs = taos.query("select * from stb1 order by tbname,ts")?;
    let mut rs = taos.query("select ts from tb1 limit 1")?;
    let inner = rs.fetch_raw_block()?.unwrap();
    let fields = rs.fields();
    let _precision = rs.precision();
    let field_count = rs.num_of_fields();
    let gid = inner.group_id();
    println!("group id: {gid}");

    use std::ascii::escape_default;

    pub fn show_buf<B: AsRef<[u8]>>(buf: B) -> String {
        String::from_utf8(
            buf.as_ref()
                .iter()
                .flat_map(|b| escape_default(*b))
                .collect(),
        )
        .unwrap()
    }
    let bytes = inner.as_raw_bytes();
    println!("{}", show_buf(bytes));

    let schemas = inner.schemas();
    for i in 0..field_count {
        let col = schemas[i];
        let field = &fields[i];
        println!("{field:?}, {col:#x?}");
    }
    dbg!(unsafe { inner.get_ref_unchecked(0, field_count - 1) });
    for row in 0..dbg!(inner.nrows()) as usize {
        for col in 0..field_count {
            println!("({row}, {col}): ");
            let v = unsafe { inner.get_ref_unchecked(row, col) };

            dbg!(v);
        }
    }
    Ok(())
}
