use std::{
    ffi::c_void,
    fmt::{Display, Write},
};

use bitvec::vec;
use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{
    common::{Field, Ty},
    util::{Inlinable, InlinableRead, InlinableWrite},
};

#[derive(Debug, Clone)]
pub struct RawMeta {
    raw: Bytes,
}

impl RawMeta {
    const META_OFFSET: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u32>();

    pub fn new(raw: Bytes) -> Self {
        RawMeta { raw }
    }
    pub fn meta_len(&self) -> u32 {
        unsafe { *(self.raw.as_ptr() as *const u32) }
    }
    pub fn meta_type(&self) -> u16 {
        unsafe {
            *(self
                .raw
                .as_ptr()
                .offset(std::mem::size_of::<u32>() as isize) as *const u16)
        }
    }
    pub fn meta_data_ptr(&self) -> *const c_void {
        unsafe { self.raw.as_ptr().offset(Self::META_OFFSET as isize) as _ }
    }
}

impl AsRef<[u8]> for RawMeta {
    fn as_ref(&self) -> &[u8] {
        self.raw.as_ref()
    }
}

impl Inlinable for RawMeta {
    fn read_inlined<R: std::io::Read>(mut reader: R) -> std::io::Result<Self> {
        let mut data = Vec::new();

        let len = reader.read_u32()?;
        data.extend(len.to_le_bytes());

        let meta_type = reader.read_u16()?;
        data.extend(meta_type.to_le_bytes());

        data.resize(data.len() + len as usize, 0);

        let buf = &mut data[RawMeta::META_OFFSET..];

        reader.read_exact(buf)?;
        Ok(Self { raw: data.into() })
    }

    fn write_inlined<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        wtr.write(self.raw.as_ref())
    }
}

// pub struct Field {
//     name: String,
//     ty: Ty,
//     length: u32,
// }

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TagWithValue {
    #[serde(flatten)]
    field: Field,
    value: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "tableType")]
#[serde(rename_all = "camelCase")]
pub enum MetaCreate {
    #[serde(rename_all = "camelCase")]
    Super {
        table_name: String,
        columns: Vec<Field>,
        tags: Vec<Field>,
    },
    #[serde(rename_all = "camelCase")]
    Child {
        table_name: String,
        using: Option<String>,
        tags: Vec<TagWithValue>,
    },
    #[serde(rename_all = "camelCase")]
    Normal {
        table_name: String,
        columns: Vec<Field>,
    },
}

impl Display for MetaCreate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CREATE TABLE IF NOT EXISTS ")?;
        // f.write_fmt(format_args!("`{}`", &self.table_name))?;
        // f.write_char('(')?;
        // f.write_str(&self.columns.iter().map(|f| f.sql_repr()).join(", "))?;
        // f.write_char(')')?;
        // if self.tags.len() > 0 {
        //     f.write_str(" tags(")?;
        //     f.write_str(&self.tags.iter().map(|f| f.sql_repr()).join(", "))?;
        //     f.write_char(')')?;
        // }
        Ok(())
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum AlterType {
    AddTag = 1,
    DropTag = 2,
    RenameTag = 3,
    SetTagValue = 4,
    AddColumn = 5,
    DropColumn = 6,
    ModifyColumnLength,
    ModifyTagLength,
    ModifyTableOption,
    RenameColumn,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
// #[serde(tag = "tableType")]
#[serde(rename_all = "camelCase")]
pub struct MetaAlter {
    table_name: String,
    alter_type: AlterType,
    col_new_name: Option<String>,
    #[serde(flatten, with = "ColField")]
    field: Field,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MetaDrop {
    /// Use table_name when drop super table
    table_name: Option<String>,
    /// Available for child and normal tables.
    table_name_list: Option<Vec<String>>,
}


#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum JsonMeta {
    Create(MetaCreate),
    Alter(MetaAlter),
    Drop(MetaDrop),
}

impl Display for JsonMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonMeta::Create(meta) => meta.fmt(f),
            _ => Ok(()),
        }
    }
}

#[test]
fn test_meta_create_to_sql() {
    // let sql = MetaCreate {
    //     table_name: "abc".to_string(),
    //     table_type: TableType::Super,
    //     using: None,
    //     columns: vec![
    //         Field::new("ts", Ty::Timestamp, 0),
    //         Field::new("location", Ty::VarChar, 16),
    //     ],
    //     tags: vec![],
    // }
    // .to_string();

    // assert_eq!(
    //     sql,
    //     "CREATE TABLE IF NOT EXISTS `abc`(`ts` TIMESTAMP, `location` BINARY(16))"
    // );
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(remote = "Field")]
pub struct ColField {
    #[serde(rename = "colName")]
    name: String,
    #[serde(default)]
    #[serde(rename = "colType")]
    ty: Ty,
    #[serde(default)]
    #[serde(rename = "colLength")]
    bytes: u32,
}

impl From<ColField> for Field {
    fn from(f: ColField) -> Self {
        Self {
            name: f.name,
            ty: f.ty,
            bytes: f.bytes,
        }
    }
}
