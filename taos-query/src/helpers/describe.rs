use std::{
    fmt,
    ops::{Deref, DerefMut},
    path::Display,
    str::FromStr,
};

use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};

use crate::common::Ty;

/// Compress options for column, supported since TDengine 3.3.0.0 .
///
/// The `encode` field is the encoding method for the column, it can be one of the following values:
/// - `disabled`
/// - `delta-i`
/// - `delta-d`
/// - `simple8b`
///
/// The `compress` field is the compression method for the column, it can be one of the following values:
/// - `none`
/// - `lz4`
/// - `gzip`
/// - `zstd`
///
/// The `level` field is the compression level for the column, it can be one of the following values:
/// - `low`
/// - `medium`
/// - `high`
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct CompressOptions {
    pub encode: String,
    pub compress: String,
    pub level: String,
}

impl CompressOptions {
    pub fn new(
        encode: impl Into<String>,
        compress: impl Into<String>,
        level: impl Into<String>,
    ) -> Self {
        Self {
            encode: encode.into(),
            compress: compress.into(),
            level: level.into(),
        }
    }
}

impl fmt::Display for CompressOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ENCODE '{}' COMPRESS '{}' LEVEL '{}'",
            self.encode, self.compress, self.level
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Described {
    pub field: String,
    #[serde(rename = "type")]
    pub ty: Ty,
    pub length: usize,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(flatten, default)]
    pub compression: Option<CompressOptions>,
}

impl Described {
    /// Represent the data type in sql.
    ///
    /// For example: "INT", "VARCHAR(100)".
    pub fn sql_repr(&self) -> String {
        let ty = self.ty;
        match (self.is_primary_key(), ty.is_var_type(), &self.compression) {
            (true, true, None) => format!("`{}` {}({}) PRIMARY KEY", self.field, ty, self.length),
            (true, false, None) => format!("`{}` {} PRIMARY KEY", self.field, self.ty),
            (true, true, Some(t)) => {
                format!("`{}` {}({}) {} PRIMARY KEY", self.field, ty, self.length, t)
            }
            (true, false, Some(t)) => {
                format!("`{}` {} {} PRIMARY KEY", self.field, ty, t)
            }

            (false, true, None) => format!("`{}` {}({})", self.field, ty, self.length),
            (false, false, None) => format!("`{}` {}", self.field, self.ty),
            (false, true, Some(t)) => {
                format!("`{}` {}({}) {}", self.field, ty, self.length, t)
            }
            (false, false, Some(t)) => {
                format!("`{}` {} {}", self.field, ty, t)
            }
        }
    }

    /// Create a new column description without primary-key/compression feature.
    pub fn new(field: impl Into<String>, ty: Ty, length: impl Into<Option<usize>>) -> Self {
        let field = field.into();
        let length = length.into();
        let length = length.unwrap_or_else(|| {
            if ty.is_var_type() {
                32
            } else {
                ty.fixed_length()
            }
        });
        Self {
            field,
            ty,
            length,
            note: None,
            compression: None,
        }
    }

    /// Return true if the field is primary key.
    pub fn is_primary_key(&self) -> bool {
        self.note.as_deref() == Some("PRIMARY KEY")
    }

    /// Return true if the field is tag.
    pub fn is_tag(&self) -> bool {
        self.note.as_deref() == Some("TAG")
    }
}
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum ColumnMeta {
    Column(Described),
    Tag(Described),
}

impl Deref for ColumnMeta {
    type Target = Described;

    fn deref(&self) -> &Self::Target {
        match self {
            ColumnMeta::Column(v) => v,
            ColumnMeta::Tag(v) => v,
        }
    }
}

impl DerefMut for ColumnMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            ColumnMeta::Column(v) => v,
            ColumnMeta::Tag(v) => v,
        }
    }
}

#[inline(always)]
fn empty_as_none(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}
unsafe impl Send for ColumnMeta {}
impl<'de> Deserialize<'de> for ColumnMeta {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Meta {
            Field,
            Type,
            Length,
            Note,
            Encode,
            Compress,
            Level,
        }

        impl<'de> Deserialize<'de> for Meta {
            fn deserialize<D>(deserializer: D) -> Result<Meta, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Meta;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str(
                            "one of `field`, `type`, `length`, `note`, `encode`, `compress`, `level`",
                        )
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Meta, E>
                    where
                        E: de::Error,
                    {
                        match value.to_lowercase().as_str() {
                            "field" => Ok(Meta::Field),
                            "type" => Ok(Meta::Type),
                            "length" => Ok(Meta::Length),
                            "note" => Ok(Meta::Note),
                            "encode" => Ok(Meta::Encode),
                            "compress" => Ok(Meta::Compress),
                            "level" => Ok(Meta::Level),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct MetaVisitor;

        impl<'de> Visitor<'de> for MetaVisitor {
            type Value = ColumnMeta;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ColumnMeta")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let field = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let ty = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))
                    .and_then(|s| Ty::from_str(s).map_err(de::Error::custom))?;
                let length = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let note: Option<String> = seq
                    .next_element::<Option<&str>>()?
                    .and_then(|opt| opt)
                    .and_then(empty_as_none);

                // let is_primary_key = &note == "PRIMARY KEY";

                let encode: Option<String> = seq
                    .next_element::<Option<&str>>()?
                    .and_then(|opt| opt)
                    .and_then(empty_as_none);
                let compress: Option<String> = seq
                    .next_element::<Option<&str>>()?
                    .and_then(|opt| opt)
                    .and_then(empty_as_none);
                let level: Option<String> = seq
                    .next_element::<Option<&str>>()?
                    .and_then(|opt| opt)
                    .and_then(empty_as_none);

                let compression = if let (Some(encode), Some(compress), Some(level)) =
                    (encode, compress, level)
                {
                    Some(CompressOptions::new(encode, compress, level))
                } else {
                    None
                };
                let desc = Described {
                    field,
                    ty,
                    length,
                    note,
                    compression,
                };
                if !desc.is_tag() {
                    Ok(ColumnMeta::Column(desc))
                } else {
                    Ok(ColumnMeta::Tag(desc))
                }
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut field = None;
                let mut ty = None;
                let mut length = None;
                let mut note = None;
                let mut encode = None;
                let mut compress = None;
                let mut level = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Meta::Field => {
                            if field.is_some() {
                                return Err(de::Error::duplicate_field("field"));
                            }
                            field = Some(map.next_value()?);
                        }
                        Meta::Type => {
                            if ty.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            let t: Ty = map.next_value()?;
                            ty = Some(t);
                        }
                        Meta::Length => {
                            if length.is_some() {
                                return Err(de::Error::duplicate_field("length"));
                            }
                            length = Some(map.next_value()?);
                        }
                        Meta::Note => {
                            if note.is_some() {
                                return Err(de::Error::duplicate_field("note"));
                            }
                            note = map.next_value::<Option<&str>>()?.and_then(empty_as_none);
                        }
                        Meta::Encode => {
                            if encode.is_some() {
                                return Err(de::Error::duplicate_field("encode"));
                            }
                            encode = map.next_value::<Option<&str>>()?.and_then(empty_as_none);
                        }
                        Meta::Compress => {
                            if compress.is_some() {
                                return Err(de::Error::duplicate_field("compress"));
                            }
                            compress = map.next_value::<Option<&str>>()?.and_then(empty_as_none);
                        }
                        Meta::Level => {
                            if level.is_some() {
                                return Err(de::Error::duplicate_field("level"));
                            }
                            level = map.next_value::<Option<&str>>()?.and_then(empty_as_none);
                        }
                    }
                }
                let field = field.ok_or_else(|| de::Error::missing_field("field"))?;
                let ty = ty.ok_or_else(|| de::Error::missing_field("type"))?;
                let length = length.ok_or_else(|| de::Error::missing_field("length"))?;
                let note = note.map(|s| s.to_string());
                let compression = if let (Some(encode), Some(compress), Some(level)) =
                    (encode, compress, level)
                {
                    Some(CompressOptions::new(encode, compress, level))
                } else {
                    None
                };
                let desc = Described {
                    field,
                    ty,
                    length,
                    note,
                    compression,
                };
                if !desc.is_tag() {
                    Ok(ColumnMeta::Column(desc))
                } else {
                    Ok(ColumnMeta::Tag(desc))
                }
            }
        }

        const FIELDS: &[&str] = &[
            "field", "type", "length", "note", "encode", "compress", "level",
        ];
        deserializer.deserialize_struct("ColumnMeta", FIELDS, MetaVisitor)
    }
}
impl ColumnMeta {
    pub fn field(&self) -> &str {
        match self {
            ColumnMeta::Column(desc) | ColumnMeta::Tag(desc) => desc.field.as_str(),
        }
    }
    pub fn ty(&self) -> Ty {
        match self {
            ColumnMeta::Column(desc) | ColumnMeta::Tag(desc) => desc.ty,
        }
    }
    pub fn length(&self) -> usize {
        match self {
            ColumnMeta::Column(desc) | ColumnMeta::Tag(desc) => desc.length,
        }
    }
    pub fn note(&self) -> &str {
        match self {
            ColumnMeta::Tag(_) => "TAG",
            _ => "",
        }
    }
    pub fn is_tag(&self) -> bool {
        matches!(self, ColumnMeta::Tag(_))
    }
}

#[test]
fn serde_meta() {
    // ordinary column
    let meta = ColumnMeta::Column(Described {
        field: "name".to_string(),
        ty: Ty::BigInt,
        length: 8,
        note: None,
        compression: None,
    });

    let sql = meta.deref().sql_repr();

    assert_eq!(sql, "`name` BIGINT");

    let a = serde_json::to_string(&meta).unwrap();

    let d: ColumnMeta = serde_json::from_str(&a).unwrap();

    assert_eq!(meta, d);

    // primary key column
    let meta = ColumnMeta::Column(Described {
        field: "name".to_string(),
        ty: Ty::BigInt,
        length: 8,
        note: Some("PRIMARY KEY".to_string()),
        compression: None,
    });
    let sql = meta.deref().sql_repr();

    assert_eq!(sql, "`name` BIGINT PRIMARY KEY");

    let a = serde_json::to_string(&meta).unwrap();

    let d: ColumnMeta = serde_json::from_str(&a).unwrap();

    assert_eq!(meta, d);

    // with compression
    let meta = ColumnMeta::Column(Described {
        field: "name".to_string(),
        ty: Ty::BigInt,
        length: 8,
        note: None,
        compression: Some(CompressOptions::new("delta-i", "lz4", "medium")),
    });
    let sql = meta.deref().sql_repr();

    assert_eq!(
        sql,
        "`name` BIGINT ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium'"
    );

    let a = serde_json::to_string(&meta).unwrap();

    let d: ColumnMeta = serde_json::from_str(&a).unwrap();

    assert_eq!(meta, d);

    // primary key with compression
    let meta = ColumnMeta::Column(Described {
        field: "name".to_string(),
        ty: Ty::BigInt,
        length: 8,
        note: Some("PRIMARY KEY".to_string()),
        compression: Some(CompressOptions::new("delta-i", "lz4", "medium")),
    });
    let sql = meta.deref().sql_repr();

    assert_eq!(
        sql,
        "`name` BIGINT ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY"
    );

    let a = serde_json::to_string(&meta).unwrap();

    let d: ColumnMeta = serde_json::from_str(&a).unwrap();

    assert_eq!(meta, d);

    // deserialize from sequence.
    let a = r#"["name", "BIGINT", 8, null, null, null, null]"#;
    let d: ColumnMeta = serde_json::from_str(a).unwrap();
    assert_eq!(
        d,
        ColumnMeta::Column(Described {
            field: "name".to_string(),
            ty: Ty::BigInt,
            length: 8,
            note: None,
            compression: None,
        })
    );
}
