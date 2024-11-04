use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use taos_query::{
    common::{BorrowedValue, ColumnView},
    RawResult,
};
use tracing::debug;

const TOTAL_LENGTH_POSITION: usize = 0;
const TABLE_COUNT_POSITION: usize = TOTAL_LENGTH_POSITION + 4;
const TAG_COUNT_POSITION: usize = TABLE_COUNT_POSITION + 4;
const COL_COUNT_POSITION: usize = TAG_COUNT_POSITION + 4;
const TABLE_NAMES_OFFSET_POSITION: usize = COL_COUNT_POSITION + 4;
const TAGS_OFFSET_POSITION: usize = TABLE_NAMES_OFFSET_POSITION + 4;
const COLS_OFFSET_POSITION: usize = TAGS_OFFSET_POSITION + 4;
const DATA_POSITION: usize = COLS_OFFSET_POSITION + 4;

const BIND_DATA_TOTAL_LENGTH_OFFSET: usize = 0;
const BIND_DATA_TYPE_OFFSET: usize = BIND_DATA_TOTAL_LENGTH_OFFSET + 4;
const BIND_DATA_NUM_OFFSET: usize = BIND_DATA_TYPE_OFFSET + 4;
const BIND_DATA_IS_NULL_OFFSET: usize = BIND_DATA_NUM_OFFSET + 4;

pub struct Stmt2BindData<'a> {
    table_name: Option<&'a str>,
    tags: Option<&'a [ColumnView]>,
    columns: &'a [ColumnView], // Option<Columns>
}

impl<'a> Stmt2BindData<'a> {
    pub fn new(
        table_name: Option<&'a str>,
        tags: Option<&'a [ColumnView]>,
        columns: &'a [ColumnView],
    ) -> Self {
        Self {
            table_name,
            tags,
            columns,
        }
    }
}

pub fn bind_datas_as_bytes(datas: &[Stmt2BindData], is_insert: bool) -> RawResult<Vec<u8>> {
    let table_cnt = datas.len();
    if table_cnt == 0 {
        return Err("empty data".into());
    }

    let mut have_table_names = false;

    let mut tag_cnt = 0;
    let mut col_cnt = 0;

    if is_insert {
        for data in datas {
            if data.table_name.map_or(false, |s| !s.is_empty()) {
                have_table_names = true;
            }

            if let Some(views) = data.tags {
                tag_cnt = views.len();
                if views.iter().any(|view| view.len() != 1) {
                    return Err("tag can only have one row".into());
                };
            }

            col_cnt = data.columns.len();
            for i in 1..col_cnt {
                if data.columns[i - 1].len() != data.columns[i].len() {
                    return Err("columns row count not match".into());
                }
            }

            // TODO: check type use stmt2_get_fields
        }
    } else {
        if table_cnt != 1 {
            return Err("query can only has one table".into());
        }

        let data = &datas[0];
        if data.table_name.is_some() {
            return Err("query not needs table name".into());
        }
        if data.tags.is_some() {
            return Err("query not needs tags".into());
        }
        if data.columns.len() == 0 {
            return Err("query needs columns".into());
        }
        for (i, view) in data.columns.iter().enumerate() {
            if view.len() != 1 {
                return Err(format!(
                    "query columns must be one row, column: {}, count: {}",
                    i,
                    view.len()
                )
                .as_str()
                .into());
            }
            if view.get(0).unwrap().is_null() {
                return Err("query column cannot be null".into());
            }
        }
    }

    let have_tags = tag_cnt != 0;
    let have_cols = col_cnt != 0;

    if !have_table_names && !have_tags && !have_cols {
        return Err("no data".into());
    }

    debug!(
        "have_table_names: {}, have_tags: {}, have_cols: {}",
        have_table_names, have_tags, have_cols
    );

    let mut tmp_buf = BytesMut::new();

    let mut table_name_buf = BytesMut::new();
    let mut table_name_lens = vec![0; table_cnt];

    let mut tag_buf = BytesMut::new();
    let mut tag_data_lens = if have_tags {
        vec![0; table_cnt]
    } else {
        vec![]
    };

    let mut col_buf = BytesMut::new();
    let mut col_data_lens = if have_cols {
        vec![0; table_cnt]
    } else {
        vec![]
    };

    for (i, data) in datas.iter().enumerate() {
        if have_table_names {
            let mut len = 0;
            if let Some(tbname) = data.table_name {
                if !tbname.is_empty() {
                    if tbname.len() > (u16::MAX - 1) as _ {
                        return Err(format!(
                            "table name too long, index: {}, len: {}",
                            i,
                            tbname.len()
                        )
                        .as_str()
                        .into());
                    }

                    len = tbname.len();
                    table_name_buf.extend_from_slice(tbname.as_bytes());
                }
            }

            table_name_buf.put_u8(0);
            table_name_lens[i] = len + 1;
        }

        if have_tags {
            let mut len = 0;
            for tag in data.tags.unwrap() {
                let tag_data_buf = generate_bind_insert_data(tag, &mut tmp_buf)?;
                len += tag_data_buf.len();
                tag_buf.extend_from_slice(&tag_data_buf);
            }
            tag_data_lens[i] = len;
        }

        if have_cols {
            let mut len = 0;
            for col in data.columns {
                let col_data_buf = if is_insert {
                    generate_bind_insert_data(col, &mut tmp_buf)?
                } else {
                    generate_bind_query_data(col)?
                };
                len += col_data_buf.len();
                col_buf.extend_from_slice(&col_data_buf);
            }
            col_data_lens[i] = len;
        }
    }

    let tag_offset = DATA_POSITION + table_name_buf.len() + table_name_lens.len() * 2;
    let col_offset = tag_offset + tag_buf.len() + tag_data_lens.len() * 4;
    let total_len = col_offset + col_buf.len() + col_data_lens.len() * 4;

    debug!(
        "table_name_lens_len: {}, tag_data_lens_len: {}, col_data_lens_len: {}",
        table_name_lens.len(),
        tag_data_lens.len(),
        col_data_lens.len()
    );

    debug!(
        "table_name_buf_len: {}, tag_buf_len: {}, col_buf_len: {}",
        table_name_buf.len(),
        tag_buf.len(),
        col_buf.len(),
    );

    debug!(
        "tag_offset: {}, col_offset: {}, total_len: {}",
        tag_offset, col_offset, total_len
    );

    let mut buf = vec![0u8; total_len];

    // total length
    LittleEndian::write_u32(&mut buf, total_len as _);

    // table count
    LittleEndian::write_u32(&mut buf[TABLE_COUNT_POSITION..], table_cnt as _);

    if have_table_names {
        // table names offset
        LittleEndian::write_u32(&mut buf[TABLE_NAMES_OFFSET_POSITION..], DATA_POSITION as _);
        let mut offset = DATA_POSITION;
        // table name length
        for len in table_name_lens {
            LittleEndian::write_u16(&mut buf[offset..], len as _);
            offset += 2;
        }
        // table name buffer
        buf[offset..offset + table_name_buf.len()].copy_from_slice(&table_name_buf);
    }

    if have_tags {
        // tag count
        LittleEndian::write_u32(&mut buf[TAG_COUNT_POSITION..], tag_cnt as _);
        // tags offset
        LittleEndian::write_u32(&mut buf[TAGS_OFFSET_POSITION..], tag_offset as _);
        let mut offset = tag_offset;
        // tags data length
        for len in tag_data_lens {
            LittleEndian::write_u32(&mut buf[offset..], len as _);
            offset += 4;
        }
        // tags buffer
        buf[offset..offset + tag_buf.len()].copy_from_slice(&tag_buf);
    }

    if have_cols {
        // col count
        LittleEndian::write_u32(&mut buf[COL_COUNT_POSITION..], col_cnt as _);
        // cols offset
        LittleEndian::write_u32(&mut buf[COLS_OFFSET_POSITION..], col_offset as _);
        let mut offset = col_offset;
        // col data length
        for len in col_data_lens {
            LittleEndian::write_u32(&mut buf[offset..], len as _);
            offset += 4;
        }
        // col buffer
        buf[offset..offset + col_buf.len()].copy_from_slice(&col_buf);
    }

    Ok(buf)
}

fn generate_bind_insert_data(view: &ColumnView, tmp_buf: &mut BytesMut) -> RawResult<Vec<u8>> {
    tmp_buf.clear();

    let num = view.len();
    let ty = view.as_ty();
    let have_len = ty.fixed_length() == 0;
    let header_len = get_bind_data_header_len(num, have_len);

    let mut is_nulls = vec![0u8; num];
    let mut lens = vec![0u8; num * 4];

    if check_all_null(view) {
        is_nulls.iter_mut().for_each(|x| *x = 1);
    } else {
        use ColumnView::*;
        match view {
            Bool(bool_view) => {
                for (i, v) in bool_view.iter().enumerate() {
                    match v {
                        Some(b) => {
                            if b {
                                tmp_buf.put_u8(1);
                            } else {
                                tmp_buf.put_u8(0);
                            }
                        }
                        None => {
                            is_nulls[i] = 1;
                            tmp_buf.put_u8(0);
                        }
                    }
                }
            }
            TinyInt(tinyint_view) => {
                for (i, v) in tinyint_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_i8(v.unwrap_or(0));
                }
            }
            SmallInt(smallint_view) => {
                for (i, v) in smallint_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_i16_le(v.unwrap_or(0));
                }
            }
            Int(int_view) => {
                for (i, v) in int_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_i32_le(v.unwrap_or(0));
                }
            }
            BigInt(big_int_view) => {
                for (i, v) in big_int_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_i64_le(v.unwrap_or(0));
                }
            }
            Float(float_view) => {
                for (i, v) in float_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_f32_le(v.unwrap_or(0f32));
                }
            }
            Double(double_view) => {
                for (i, v) in double_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_f64_le(v.unwrap_or(0f64));
                }
            }
            VarChar(varchar_view) => {
                for (i, v) in varchar_view.iter().enumerate() {
                    let mut len = 0;
                    match v {
                        Some(is) => {
                            tmp_buf.extend_from_slice(is.as_bytes());
                            len = is.len();
                        }
                        None => is_nulls[i] = 1,
                    };
                    lens.put_i32_le(len as _);
                }
            }
            Timestamp(timestamp_view) => {
                for (i, v) in timestamp_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_i64_le(
                        v.unwrap_or(taos_query::common::Timestamp::Microseconds(0))
                            .as_raw_i64(),
                    );
                }
            }
            NChar(nchar_view) => {
                for (i, v) in nchar_view.iter().enumerate() {
                    let mut len = 0;
                    match v {
                        Some(is) => {
                            tmp_buf.extend_from_slice(is.as_bytes());
                            len = is.len();
                        }
                        None => is_nulls[i] = 1,
                    }
                    lens.put_i32_le(len as _);
                }
            }
            UTinyInt(utinyint_view) => {
                for (i, v) in utinyint_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_u8(v.unwrap_or(0));
                }
            }
            USmallInt(usmallint_view) => {
                for (i, v) in usmallint_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_u16_le(v.unwrap_or(0));
                }
            }
            UInt(uint_view) => {
                for (i, v) in uint_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_u32_le(v.unwrap_or(0));
                }
            }
            UBigInt(ubigint_view) => {
                for (i, v) in ubigint_view.iter().enumerate() {
                    if v.is_none() {
                        is_nulls[i] = 1;
                    }
                    tmp_buf.put_u64_le(v.unwrap_or(0));
                }
            }
            Json(json_view) => {
                for (i, v) in json_view.iter().enumerate() {
                    let mut len = 0;
                    match v {
                        Some(is) => {
                            tmp_buf.extend_from_slice(is.as_bytes());
                            len = is.len();
                        }
                        None => is_nulls[i] = 1,
                    }
                    lens.put_i32_le(len as _);
                }
            }
            VarBinary(varbinary_view) => {
                for (i, v) in varbinary_view.iter().enumerate() {
                    let mut len = 0;
                    match v {
                        Some(is) => {
                            tmp_buf.extend_from_slice(is.as_bytes());
                            len = is.len();
                        }
                        None => is_nulls[i] = 1,
                    }
                    lens.put_i32_le(len as _);
                }
            }
            Geometry(geometry_view) => {
                let mut len = 0;
                for (i, v) in geometry_view.iter().enumerate() {
                    match v {
                        Some(is) => {
                            tmp_buf.extend_from_slice(is.as_bytes());
                            len = is.len();
                        }
                        None => is_nulls[i] = 1,
                    }
                    lens.put_i32_le(len as _);
                }
            }
        }
    }

    let total_len = header_len + tmp_buf.len();
    let mut data_buf = vec![0u8; total_len];
    // total length
    LittleEndian::write_u32(&mut data_buf, total_len as _);
    // type
    LittleEndian::write_u32(&mut data_buf[BIND_DATA_TYPE_OFFSET..], ty as _);
    // num
    LittleEndian::write_u32(&mut data_buf[BIND_DATA_NUM_OFFSET..], num as _);
    // is null
    data_buf[BIND_DATA_IS_NULL_OFFSET..is_nulls.len()].copy_from_slice(&is_nulls);
    if have_len {
        // have length
        data_buf[BIND_DATA_IS_NULL_OFFSET + num] = 1;
        // length
        data_buf[BIND_DATA_IS_NULL_OFFSET + num + 1..lens.len()].copy_from_slice(&lens);
    }
    // buffer length
    LittleEndian::write_u32(&mut data_buf[header_len - 4..], tmp_buf.len() as _);
    // buffer
    data_buf[header_len..].copy_from_slice(&tmp_buf);

    Ok(data_buf)
}

fn generate_bind_query_data(view: &ColumnView) -> RawResult<Vec<u8>> {
    let bval = view.get(0).unwrap();
    let ty = bval.ty();
    let have_len = ty.fixed_length() == 0;
    let mut len = 0;
    let mut buf;

    use BorrowedValue::*;
    match bval {
        Bool(v) => {
            buf = vec![0u8; 1];
            buf[0] = if v { 1 } else { 0 };
        }
        TinyInt(v) => {
            buf = vec![v as u8];
        }
        SmallInt(v) => {
            buf = vec![0u8; 2];
            LittleEndian::write_i16(&mut buf, v);
        }
        Int(v) => {
            buf = vec![0u8; 4];
            LittleEndian::write_i32(&mut buf, v);
        }
        BigInt(v) => {
            buf = vec![0u8; 8];
            LittleEndian::write_i64(&mut buf, v);
        }
        Float(v) => {
            buf = vec![0u8; 4];
            LittleEndian::write_f32(&mut buf, v);
        }
        Double(v) => {
            buf = vec![0u8; 8];
            LittleEndian::write_f64(&mut buf, v);
        }
        VarChar(v) => {
            len = v.len();
            buf = vec![0u8; len];
            buf.copy_from_slice(v.as_bytes());
        }
        Timestamp(ts) => {
            buf = vec![0u8; 8];
            LittleEndian::write_i64(&mut buf, ts.as_raw_i64());
        }
        NChar(v) => {
            len = v.len();
            buf = vec![0u8; len];
            buf.copy_from_slice(v.as_bytes());
        }
        UTinyInt(v) => {
            buf = vec![v];
        }
        USmallInt(v) => {
            buf = vec![0u8; 2];
            LittleEndian::write_u16(&mut buf, v);
        }
        UInt(v) => {
            buf = vec![0u8; 4];
            LittleEndian::write_u32(&mut buf, v);
        }
        UBigInt(v) => {
            buf = vec![0u8; 8];
            LittleEndian::write_u64(&mut buf, v);
        }
        Json(v) | VarBinary(v) | Geometry(v) => {
            len = v.len();
            buf = vec![0u8; len];
            buf.copy_from_slice(v.as_ref());
        }
        _ => {
            return Err("unsupported type".into());
        }
    };

    let header_len = get_bind_data_header_len(1, have_len);
    let total_len = header_len + buf.len();
    let mut data_buf = vec![0u8; total_len];
    // total length
    LittleEndian::write_u32(&mut data_buf, total_len as u32);
    // type
    LittleEndian::write_u32(&mut data_buf[BIND_DATA_TYPE_OFFSET..], ty as u32);
    // num
    LittleEndian::write_u32(&mut data_buf[BIND_DATA_NUM_OFFSET..], 1);
    // is null
    data_buf[BIND_DATA_IS_NULL_OFFSET] = 0;
    if have_len {
        // have length
        data_buf[BIND_DATA_IS_NULL_OFFSET + 1] = 1;
        // length
        LittleEndian::write_u32(&mut data_buf[BIND_DATA_IS_NULL_OFFSET + 2..], len as u32);
    }
    // buffer length
    LittleEndian::write_u32(&mut data_buf[header_len - 4..], buf.len() as u32);
    // buffer
    data_buf[header_len..].copy_from_slice(&buf);

    Ok(data_buf)
}

fn get_bind_data_header_len(num: usize, have_len: bool) -> usize {
    let mut len = 17 + num;
    if have_len {
        len += num * 4;
    }
    len
}

fn check_all_null(view: &ColumnView) -> bool {
    for bval in view.iter() {
        if !bval.is_null() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use ctor::ctor;

    use super::*;

    #[ctor]
    fn setup() {
        tracing_subscriber::fmt::init();
    }

    #[test]
    fn test_bind_datas_as_bytes() -> Result<()> {
        {
            let data1 = Stmt2BindData::new(Some("test1"), None, &[]);
            let data2 = Stmt2BindData::new(Some(""), None, &[]);
            let data3 = Stmt2BindData::new(Some("test2"), None, &[]);
            let res = bind_datas_as_bytes(&[data1, data2, data3], true)?;
            let expected = [
                0x2f, 0x00, 0x00, 0x00, // total Length
                0x03, 0x00, 0x00, 0x00, // table count
                0x00, 0x00, 0x00, 0x00, // tag count
                0x00, 0x00, 0x00, 0x00, // col count
                0x1c, 0x00, 0x00, 0x00, // table names offset
                0x00, 0x00, 0x00, 0x00, // tags offset
                0x00, 0x00, 0x00, 0x00, // col offset
                // table names
                0x06, 0x00, 0x01, 0x00, 0x06, 0x00, // table name length
                // table name buffer
                0x74, 0x65, 0x73, 0x74, 0x31, 0x00, // test1
                0x00, // nil
                0x74, 0x65, 0x73, 0x74, 0x32, 0x00, // test2
            ];
            assert_eq!(res, expected);
        }

        Ok(())
    }
}
