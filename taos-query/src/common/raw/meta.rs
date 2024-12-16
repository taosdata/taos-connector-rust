use std::{
    fmt::{Display, Write},
    ops::Deref,
};

use bytes::Bytes;

use either::Either;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{
    common::{Field, Ty},
    helpers::CompressOptions,
    util::Inlinable,
};

use super::RawData;

#[derive(Debug, Clone)]
pub struct RawMeta(RawData);

impl<T: Into<RawData>> From<T> for RawMeta {
    fn from(bytes: T) -> Self {
        RawMeta(bytes.into())
    }
}

impl Deref for RawMeta {
    type Target = RawData;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RawMeta {
    pub fn new(raw: Bytes) -> Self {
        RawMeta(raw.into())
    }
}

impl Inlinable for RawMeta {
    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        RawData::read_inlined(reader).map(RawMeta)
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        self.deref().write_inlined(wtr)
    }
}

#[async_trait::async_trait]
impl crate::util::AsyncInlinable for RawMeta {
    async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        <RawData as crate::util::AsyncInlinable>::read_inlined(reader)
            .await
            .map(RawMeta)
    }

    async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        crate::util::AsyncInlinable::write_inlined(self.deref(), wtr).await
    }
}

// #[derive(Debug, Clone)]
// pub struct RawMeta {
//     raw: Bytes,
// }

// impl RawMeta {
//     pub const META_OFFSET: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u16>();

//     pub fn new(raw: Bytes) -> Self {
//         RawMeta { raw }
//     }
//     pub fn meta_len(&self) -> u32 {
//         unsafe { *(self.raw.as_ptr() as *const u32) }
//     }
//     pub fn meta_type(&self) -> u16 {
//         unsafe {
//             *(self
//                 .raw
//                 .as_ptr()
//                 .offset(std::mem::size_of::<u32>() as isize) as *const u16)
//         }
//     }
//     pub fn meta_data_ptr(&self) -> *const c_void {
//         unsafe { self.raw.as_ptr().offset(Self::META_OFFSET as isize) as _ }
//     }
// }

// impl AsRef<[u8]> for RawMeta {
//     fn as_ref(&self) -> &[u8] {
//         self.raw.as_ref()
//     }
// }

// impl Inlinable for RawMeta {
//     fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
//         let mut data = Vec::new();

//         let len = reader.read_u32()?;
//         data.extend(len.to_le_bytes());

//         let meta_type = reader.read_u16()?;
//         data.extend(meta_type.to_le_bytes());

//         data.resize(data.len() + len as usize, 0);

//         let buf = &mut data[RawMeta::META_OFFSET..];

//         reader.read_exact(buf)?;
//         Ok(Self { raw: data.into() })
//     }

//     fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
//         wtr.write_all(self.raw.as_ref())?;
//         Ok(self.raw.len())
//     }
// }

// #[async_trait::async_trait]
// impl crate::util::AsyncInlinable for RawMeta {
//     async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
//         reader: &mut R,
//     ) -> std::io::Result<Self> {
//         use tokio::io::*;
//         let mut data = Vec::new();

//         let len = reader.read_u32_le().await?;
//         data.extend(len.to_le_bytes());

//         let meta_type = reader.read_u16_le().await?;
//         data.extend(meta_type.to_le_bytes());

//         data.resize(data.len() + len as usize, 0);

//         let buf = &mut data[RawMeta::META_OFFSET..];

//         reader.read_exact(buf).await?;
//         Ok(Self { raw: data.into() })
//     }

//     async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
//         &self,
//         wtr: &mut W,
//     ) -> std::io::Result<usize> {
//         use tokio::io::*;
//         wtr.write_all(self.raw.as_ref()).await?;
//         Ok(self.raw.len())
//     }
// }

/// TMQ json meta isPrimaryKey/encode/compress/level support since 3.3.0.0
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FieldMore {
    #[serde(flatten)]
    field: Field,
    #[serde(default)]
    is_primary_key: bool,
    #[serde(default, flatten)]
    compression: Option<CompressOptions>,
}

impl From<Field> for FieldMore {
    fn from(f: Field) -> Self {
        Self {
            field: f,
            is_primary_key: false,
            compression: None,
        }
    }
}

impl FieldMore {
    pub fn is_primary_key(&self) -> bool {
        self.is_primary_key
    }

    fn sql_repr(&self) -> String {
        let mut sql = self.field.to_string();
        if let Some(compression) = &self.compression {
            sql.push(' ');
            write!(&mut sql, "{}", compression).unwrap();
            // sql.push_str(&format!(" {}", compression));
        }
        if self.is_primary_key {
            sql.push_str(" PRIMARY KEY");
        }
        sql
    }
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TagWithValue {
    #[serde(flatten)]
    pub field: Field,
    pub value: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "tableType")]
#[serde(rename_all = "camelCase")]
pub enum MetaCreate {
    #[serde(rename_all = "camelCase")]
    Super {
        table_name: String,
        columns: Vec<FieldMore>,
        tags: Vec<Field>,
    },
    #[serde(rename_all = "camelCase")]
    Child {
        table_name: String,
        using: String,
        tags: Vec<TagWithValue>,
        tag_num: Option<usize>,
    },
    #[serde(rename_all = "camelCase")]
    Normal {
        table_name: String,
        columns: Vec<FieldMore>,
    },
}

impl Display for MetaCreate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CREATE TABLE IF NOT EXISTS ")?;
        match self {
            MetaCreate::Super {
                table_name,
                columns,
                tags,
            } => {
                debug_assert!(!columns.is_empty(), "{:?}", self);
                debug_assert!(!tags.is_empty());

                f.write_fmt(format_args!("`{}`", table_name))?;
                f.write_char('(')?;
                f.write_str(&columns.iter().map(|f| f.sql_repr()).join(", "))?;
                f.write_char(')')?;

                f.write_str(" TAGS(")?;
                f.write_str(&tags.iter().map(|f| f.sql_repr()).join(", "))?;
                f.write_char(')')?;
            }
            MetaCreate::Child {
                table_name,
                using,
                tags,
                tag_num,
            } => {
                if !tags.is_empty() {
                    f.write_fmt(format_args!(
                        "`{}` USING `{}` ({}) TAGS({})",
                        table_name,
                        using,
                        tags.iter().map(|t| t.field.escaped_name()).join(", "),
                        tags.iter()
                            .map(|t| {
                                match t.field.ty() {
                                    Ty::Json => format!("'{}'", t.value.as_str().unwrap()),
                                    Ty::VarChar | Ty::NChar => {
                                        format!("{}", t.value.as_str().unwrap())
                                    }
                                    _ => format!("{}", t.value),
                                }
                            })
                            .join(", ")
                    ))?;
                } else {
                    f.write_fmt(format_args!(
                        "`{}` USING `{}` TAGS({})",
                        table_name,
                        using,
                        std::iter::repeat("NULL").take(tag_num.unwrap()).join(",")
                    ))?;
                }
            }
            MetaCreate::Normal {
                table_name,
                columns,
            } => {
                debug_assert!(!columns.is_empty());

                f.write_fmt(format_args!("`{}`", table_name))?;
                f.write_char('(')?;
                f.write_str(&columns.iter().map(|f| f.sql_repr()).join(", "))?;
                f.write_char(')')?;
            }
        }
        Ok(())
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

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum AlterType {
    AddTag = 1,
    DropTag = 2,
    RenameTag = 3,
    SetTagValue = 4,
    AddColumn = 5,
    DropColumn = 6,
    ModifyColumnLength = 7,
    ModifyTagLength = 8,
    ModifyTableOption = 9,
    RenameColumn = 10,
    // TODO: TDengine 3.3.0 encode/compress/level support.
    // ModifyColumnCompression = 13,
    SetMultiTagValue = 15,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
// #[serde(tag = "tableType")]
#[serde(rename_all = "camelCase")]
pub struct MetaAlter {
    pub table_name: String,
    pub alter_type: AlterType,
    #[serde(flatten, with = "ColField")]
    pub field: Field,
    pub col_new_name: Option<String>,
    pub col_value: Option<String>,
    pub col_value_null: Option<bool>,
    pub tags: Option<Vec<Tag>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Tag {
    pub col_name: String,
    #[serde(default)]
    pub col_value: String,
    pub col_value_null: bool,
}

impl Display for MetaAlter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.alter_type {
            AlterType::AddTag => f.write_fmt(format_args!(
                "ALTER TABLE `{}` ADD TAG {}",
                self.table_name,
                self.field.sql_repr()
            )),
            AlterType::DropTag => f.write_fmt(format_args!(
                "ALTER TABLE `{}` DROP TAG `{}`",
                self.table_name,
                self.field.name()
            )),
            AlterType::RenameTag => f.write_fmt(format_args!(
                "ALTER TABLE `{}` RENAME TAG `{}` `{}`",
                self.table_name,
                self.field.name(),
                self.col_new_name.as_ref().unwrap()
            )),
            AlterType::SetTagValue => {
                f.write_fmt(format_args!(
                    "ALTER TABLE `{}` SET TAG `{}` = ",
                    self.table_name,
                    self.field.name()
                ))?;
                if self.col_value_null.unwrap_or(false) {
                    f.write_str("NULL")
                } else {
                    f.write_fmt(format_args!("{}", self.col_value.as_ref().unwrap()))
                }
            }
            AlterType::AddColumn => f.write_fmt(format_args!(
                "ALTER TABLE `{}` ADD COLUMN {}",
                self.table_name,
                self.field.sql_repr()
            )),
            AlterType::DropColumn => f.write_fmt(format_args!(
                "ALTER TABLE `{}` DROP COLUMN `{}`",
                self.table_name,
                self.field.name()
            )),
            AlterType::ModifyColumnLength => f.write_fmt(format_args!(
                "ALTER TABLE `{}` MODIFY COLUMN {}",
                self.table_name,
                self.field.sql_repr(),
            )),
            AlterType::ModifyTagLength => f.write_fmt(format_args!(
                "ALTER TABLE `{}` MODIFY TAG {}",
                self.table_name,
                self.field.sql_repr(),
            )),
            AlterType::ModifyTableOption => todo!(),
            AlterType::RenameColumn => f.write_fmt(format_args!(
                "ALTER TABLE `{}` RENAME COLUMN `{}` `{}`",
                self.table_name,
                self.field.name(),
                self.col_new_name.as_ref().unwrap()
            )),
            // ModifyColumnCompression => f.write_fmt(format_args!(
            //     "ALTER TABLE `{}` MODIFY COLUMN {}",
            //     self.table_name,
            //     self.field.sql_repr(),
            // )),
            AlterType::SetMultiTagValue => {
                f.write_fmt(format_args!("ALTER TABLE `{}` SET TAG ", self.table_name))?;
                if let Some(tags) = self.tags.as_deref() {
                    for (i, tag) in tags.iter().enumerate() {
                        if tag.col_value_null {
                            f.write_fmt(format_args!("`{}` = NULL", tag.col_name))?;
                        } else {
                            f.write_fmt(format_args!("`{}` = {}", tag.col_name, tag.col_value))?;
                        }
                        if i < tags.len() - 1 {
                            f.write_str(", ")?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum MetaDrop {
    #[serde(rename_all = "camelCase")]
    Super {
        /// Use table_name when drop super table
        table_name: String,
    },
    #[serde(rename_all = "camelCase")]
    Other {
        /// Available for child and normal tables.
        table_name_list: Vec<String>,
    },
}

impl Display for MetaDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaDrop::Super { table_name } => {
                f.write_fmt(format_args!("DROP TABLE IF EXISTS `{}`", table_name))
            }
            MetaDrop::Other { table_name_list } => f.write_fmt(format_args!(
                "DROP TABLE IF EXISTS {}",
                table_name_list.iter().map(|n| format!("`{n}`")).join(" ")
            )),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MetaDelete {
    sql: String,
}

impl Display for MetaDelete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.sql)
    }
}
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum MetaUnit {
    Create(MetaCreate),
    Alter(MetaAlter),
    Drop(MetaDrop),
    Delete(MetaDelete),
}

impl Display for MetaUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaUnit::Create(meta) => meta.fmt(f),
            MetaUnit::Alter(alter) => alter.fmt(f),
            MetaUnit::Drop(drop) => drop.fmt(f),
            MetaUnit::Delete(delete) => delete.fmt(f),
            // _ => Ok(()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(remote = "Field")]
pub struct ColField {
    #[serde(rename = "colName", default)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum JsonMeta {
    Plural {
        tmq_meta_version: faststr::FastStr,
        metas: Vec<MetaUnit>,
    },
    Single(MetaUnit),
}

impl JsonMeta {
    /// Check if the meta is a single meta.
    pub fn is_single(&self) -> bool {
        matches!(self, JsonMeta::Single { .. })
    }

    /// Check if the meta is a plural meta.
    pub fn is_plural(&self) -> bool {
        matches!(self, JsonMeta::Plural { .. })
    }

    pub fn iter(&self) -> JsonMetaIter {
        match self {
            JsonMeta::Plural { metas, .. } => JsonMetaIter {
                iter: Either::Left(metas.iter()),
            },
            JsonMeta::Single(meta) => JsonMetaIter {
                iter: Either::Right(std::iter::once(meta)),
            },
        }
    }

    pub fn iter_mut(&mut self) -> JsonMetaIterMut {
        match self {
            JsonMeta::Plural { metas, .. } => JsonMetaIterMut {
                iter: Either::Left(metas.iter_mut()),
            },
            JsonMeta::Single(meta) => JsonMetaIterMut {
                iter: Either::Right(std::iter::once(meta)),
            },
        }
    }
}

pub struct JsonMetaIter<'a> {
    iter: Either<std::slice::Iter<'a, MetaUnit>, std::iter::Once<&'a MetaUnit>>,
}

impl<'a> Iterator for JsonMetaIter<'a> {
    type Item = &'a MetaUnit;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            Either::Left(iter) => iter.next(),
            Either::Right(iter) => iter.next(),
        }
    }
}

impl<'a> ExactSizeIterator for JsonMetaIter<'a> {
    #[inline]
    fn len(&self) -> usize {
        match &self.iter {
            Either::Left(iter) => iter.len(),
            Either::Right(iter) => iter.len(),
        }
    }
}

impl<'a> DoubleEndedIterator for JsonMetaIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            Either::Left(iter) => iter.next_back(),
            Either::Right(iter) => iter.next_back(),
        }
    }
}

pub struct JsonMetaIterMut<'a> {
    iter: Either<std::slice::IterMut<'a, MetaUnit>, std::iter::Once<&'a mut MetaUnit>>,
}

impl<'a> Iterator for JsonMetaIterMut<'a> {
    type Item = &'a mut MetaUnit;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            Either::Left(iter) => iter.next(),
            Either::Right(iter) => iter.next(),
        }
    }
}

impl<'a> ExactSizeIterator for JsonMetaIterMut<'a> {
    #[inline]
    fn len(&self) -> usize {
        match &self.iter {
            Either::Left(iter) => iter.len(),
            Either::Right(iter) => iter.len(),
        }
    }
}

impl<'a> DoubleEndedIterator for JsonMetaIterMut<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            Either::Left(iter) => iter.next_back(),
            Either::Right(iter) => iter.next_back(),
        }
    }
}

pub struct JsonMetaIntoIter(Either<std::vec::IntoIter<MetaUnit>, std::iter::Once<MetaUnit>>);

impl Iterator for JsonMetaIntoIter {
    type Item = MetaUnit;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            Either::Left(iter) => iter.next(),
            Either::Right(iter) => iter.next(),
        }
    }
}

impl ExactSizeIterator for JsonMetaIntoIter {
    fn len(&self) -> usize {
        match &self.0 {
            Either::Left(iter) => iter.len(),
            Either::Right(_) => 1,
        }
    }
}

impl DoubleEndedIterator for JsonMetaIntoIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            Either::Left(iter) => iter.next_back(),
            Either::Right(iter) => iter.next(),
        }
    }
}

impl IntoIterator for JsonMeta {
    type Item = MetaUnit;
    type IntoIter = JsonMetaIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            JsonMeta::Plural { metas, .. } => JsonMetaIntoIter(Either::Left(metas.into_iter())),
            JsonMeta::Single(meta) => JsonMetaIntoIter(Either::Right(std::iter::once(meta))),
        }
    }
}

impl<'a> IntoIterator for &'a JsonMeta {
    type Item = &'a MetaUnit;
    type IntoIter = JsonMetaIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a> IntoIterator for &'a mut JsonMeta {
    type Item = &'a mut MetaUnit;
    type IntoIter = JsonMetaIterMut<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::itypes::IsJson;

    use super::{JsonMeta, MetaUnit};

    #[test]
    fn test_json_meta_compress() {
        let json ="{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"t3\",\"columns\":[{\"name\":\"ts\",\"type\":9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"obj_id\",\"type\":5,\"isPrimarykey\":true,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"data1\",\"type\":6,\"isPrimarykey\":false,\"encode\":\"delta-d\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"data2\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"high\"}],\"tags\":[]}";
        let meta = serde_json::from_str::<MetaUnit>(json).unwrap();
        println!("{}", meta);
        assert_eq!(meta.to_string(), "CREATE TABLE IF NOT EXISTS `t3`(`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `obj_id` BIGINT ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `data1` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `data2` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'high')");
    }

    #[test]
    fn test_json_meta_plural() {
        let json ="{\"tmq_meta_version\":\"1.0\",\"metas\":[{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"t3\",\"columns\":[{\"name\":\"ts\",\"type\":9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"obj_id\",\"type\":5,\"isPrimarykey\":true,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"data1\",\"type\":6,\"isPrimarykey\":false,\"encode\":\"delta-d\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"data2\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"high\"}],\"tags\":[]}] }";
        let meta = serde_json::from_str::<JsonMeta>(json).unwrap();
        println!("{:?}", meta);
        assert_eq!(meta.iter().map(ToString::to_string).next().unwrap(), "CREATE TABLE IF NOT EXISTS `t3`(`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `obj_id` BIGINT ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `data1` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `data2` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'high')");
    }

    #[test]
    fn test_meta_alter_set_tag_val() {
        let value = serde_json::json!({
            "type": "alter",
            "tableType": "child",
            "tableName": "ctb",
            "alterType": 4,
            "colName": "t1",
            "colValue": "5000",
            "colValueNull": false
        });
        let meta = serde_json::from_str::<MetaUnit>(&value.to_json()).unwrap();
        assert_eq!(meta.to_string(), "ALTER TABLE `ctb` SET TAG `t1` = 5000");
    }

    #[test]
    fn test_meta_alter_set_multi_tag_val() {
        let value = serde_json::json!({
            "type": "alter",
            "tableType": "child",
            "tableName": "ctb",
            "alterType": 15,
            "tags": [{
                "colName": "t1",
                "colValue": "5000",
                "colValueNull": false
            }, {
                "colName": "t2",
                "colValue": "1000",
                "colValueNull": false
            }, {
                "colName": "t3",
                "colValue": "'hello'",
                "colValueNull": false
            }, {
                "colName": "t4",
                "colValueNull": true
            }]
        });
        let meta = serde_json::from_str::<MetaUnit>(&value.to_json()).unwrap();
        assert_eq!(
            meta.to_string(),
            "ALTER TABLE `ctb` SET TAG `t1` = 5000, `t2` = 1000, `t3` = 'hello', `t4` = NULL"
        );
    }
}
