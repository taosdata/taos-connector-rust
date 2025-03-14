use std::marker::PhantomData;

use serde::de::value::Error;
use serde::de::{DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::Deserializer;

use crate::common::BorrowedValue;
use crate::Field;

/// Row-based deserializer helper.
///
/// 'b: field lifetime may go across the whole query.
pub struct RecordDeserializer<'b, R>
where
    R: Iterator<Item = (&'b Field, BorrowedValue<'b>)>,
{
    inner: R,
    value: Option<BorrowedValue<'b>>,
    _marker: PhantomData<&'b u8>,
}

impl<'b, R, T> From<T> for RecordDeserializer<'b, R>
where
    R: Iterator<Item = (&'b Field, BorrowedValue<'b>)>,
    T: IntoIterator<Item = (&'b Field, BorrowedValue<'b>), IntoIter = R>,
{
    fn from(input: T) -> Self {
        Self {
            inner: input.into_iter(),
            value: None,
            _marker: PhantomData,
        }
    }
}

impl<'b, R> RecordDeserializer<'b, R>
where
    R: Iterator<Item = (&'b Field, BorrowedValue<'b>)>,
{
    fn next_value(&mut self) -> Option<BorrowedValue<'b>> {
        self.inner.next().map(|(_, v)| v)
    }
}

// impl<'b, R> RecordDeserializer<'b, R>
// where
//     R: Iterator<Item = (&'b Field, BorrowedValue<'b>)>,
// {
//     fn cloned(&self) -> RecordDeserializer<'b, std::iter::Cloned<R>> {
//         RecordDeserializer::from(self.inner.ier().cloned())
//     }
// }
impl<'de, 'b: 'de, R> MapAccess<'de> for RecordDeserializer<'b, R>
where
    R: Iterator<Item = (&'b Field, BorrowedValue<'b>)>,
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.inner.next() {
            Some((field, value)) => {
                self.value = Some(value);
                seed.deserialize(field.name().into_deserializer()).map(Some)
            }
            _ => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let value = self.value.take().unwrap(); // always be here, so it's safe to unwrap

        seed.deserialize(value)
            .map_err(<Self::Error as serde::de::Error>::custom)
    }
}

impl<'de, 'b: 'de, R> SeqAccess<'de> for RecordDeserializer<'b, R>
where
    R: Iterator<Item = (&'b Field, BorrowedValue<'b>)>,
{
    type Error = Error;

    fn next_element_seed<S>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: DeserializeSeed<'de>,
    {
        match self.inner.next() {
            Some((_, v)) => seed
                .deserialize(v)
                .map_err(<Self::Error as serde::de::Error>::custom)
                .map(Some),
            None => Ok(None),
        }
    }
}

impl<'de, 'b: 'de, R> Deserializer<'de> for RecordDeserializer<'b, R>
where
    R: Iterator<Item = (&'b Field, BorrowedValue<'b>)>,
{
    type Error = Error;

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
        // match self.next_value() {
        //     Some(v) => v
        //         .deserialize_any(visitor)
        //         .map_err(<Self::Error as serde::de::Error>::custom),
        //     None => Err(<Self::Error as serde::de::Error>::custom(
        //         "expect value, not none",
        //     )),
        // }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes byte_buf enum
        identifier ignored_any
    }

    // Refer to the "Understanding deserializer lifetimes" page for information
    // about the three deserialization flavors of strings in Serde.
    fn deserialize_str<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.next_value() {
            Some(v) => v
                .deserialize_str(visitor)
                .map_err(<Self::Error as serde::de::Error>::custom),
            None => Err(<Self::Error as serde::de::Error>::custom(
                "expect value, not none",
            )),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.next_value() {
            Some(v) => {
                if v.is_null() {
                    visitor.visit_none()
                } else {
                    visitor
                        .visit_some(v)
                        .map_err(<Self::Error as serde::de::Error>::custom)
                }
            }
            _ => Err(<Self::Error as serde::de::Error>::custom(
                "expect next value",
            )),
        }
    }

    // In Serde, unit means an anonymous value containing no data.
    fn deserialize_unit<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.next_value() {
            Some(_v) => visitor.visit_unit(),
            _ => Err(<Self::Error as serde::de::Error>::custom(
                "there's no enough value",
            )),
        }
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain. That means not
    // parsing anything other than the contained value.
    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    // Deserialization of compound types like sequences and maps happens by
    // passing the visitor an "Access" object that gives it the ability to
    // iterate through the data contained in the sequence.
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    // Tuples look just like sequences.
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // self.deserialize_any(visitor)
        visitor.visit_seq(self)
    }

    // Tuple structs look just like sequences.
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // Much like `deserialize_seq` but calls the visitors `visit_map` method
    // with a `MapAccess` implementation, rather than the visitor's `visit_seq`
    // method with a `SeqAccess` implementation.
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // let value = visitor.visit_map(self);
        // unimplemented!();
        visitor.visit_map(self)
    }

    // Structs look just like maps in JSON.
    //
    // Notice the `fields` parameter - a "struct" in the Serde data model means
    // that the `Deserialize` implementation is required to know what the fields
    // are before even looking at the input data. Any key-value pairing in which
    // the fields cannot be known ahead of time is probably a map.
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::common::Ty;

    #[test]
    fn test_empty_deserializer() {
        let fields = vec![];
        let values = vec![];

        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record {}
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(record, Record {});
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record {
                a: Option<i32>,
            }
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(record, Record { a: None });
        }
    }
    #[test]
    fn test_borrowed_values_deserializer() {
        let fields = vec![
            Field::new("a", Ty::Int, 4),
            Field::new("b", Ty::VarChar, 128),
            Field::new("c", Ty::NChar, 128),
        ];
        let values = vec![
            BorrowedValue::Null(Ty::Int),
            BorrowedValue::Null(Ty::VarChar),
            BorrowedValue::Null(Ty::NChar),
        ];

        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record {
                a: i32,
                b: i32,
                c: i32,
            }
            let record = Record::deserialize(deserializer);
            assert!(record.is_err());
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record<'a> {
                a: i32,
                b: Option<&'a str>,
                c: Option<&'a str>,
            }
            let record = Record::deserialize(deserializer);
            assert_eq!(
                record.unwrap_err().to_string(),
                "invalid type: Option value, expected i32"
            );
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record<'a> {
                a: Option<i32>,
                b: &'a str,
                c: Option<&'a str>,
            }
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(
                record,
                Record {
                    a: None,
                    b: "",
                    c: None
                }
            );
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record<'a> {
                a: Option<i32>,
                b: Option<&'a str>,
                c: Option<&'a str>,
            }
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(
                record,
                Record {
                    a: None,
                    b: None,
                    c: None
                }
            );
        }
    }
    #[test]
    fn test_record_deserializer() {
        let fields = vec![
            Field::new("a", Ty::Int, 4),
            Field::new("b", Ty::Int, 4),
            Field::new("c", Ty::Int, 4),
        ];
        let values = vec![
            BorrowedValue::Int(1),
            BorrowedValue::Int(2),
            BorrowedValue::Int(3),
        ];

        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record {
                a: i32,
                b: i32,
                c: i32,
            } // New type struct
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(record, Record { a: 1, b: 2, c: 3 });
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record; // Unit struct
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(record, Record);
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record(serde_json::Value); // Tuple struct
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(record, Record(serde_json::json!({"a": 1, "b": 2, "c": 3})));
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            #[derive(Deserialize, PartialEq, Eq, Debug)]
            struct Record(i32, i32, i32); // Tuple struct
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(record, Record(1, 2, 3));
        }

        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            type Record = (i32, i32, i32);
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(record, (1, 2, 3));
        }

        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            type Record = [i32; 3];
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(record, [1, 2, 3]);
        }

        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);

            type Record = Vec<i32>;
            let record = Record::deserialize(deserializer).unwrap();
            assert_eq!(record, vec![1, 2, 3]);
        }

        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);
            let value: [crate::common::Value; 3] =
                serde::Deserialize::deserialize(deserializer).unwrap();
            dbg!(&value);
            assert_eq!(
                value,
                [
                    crate::common::Value::Int(1),
                    crate::common::Value::Int(2),
                    crate::common::Value::Int(3)
                ]
            );
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);
            let value: String = serde::Deserialize::deserialize(deserializer).unwrap();
            dbg!(&value);
            assert_eq!(value, "1");
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);
            let value: () = serde::Deserialize::deserialize(deserializer).unwrap();
            dbg!(&value);
            assert_eq!(value, ());
        }
        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);
            let value: Option<String> = serde::Deserialize::deserialize(deserializer).unwrap();
            dbg!(&value);
            assert_eq!(value, Some("1".to_string()));
        }

        {
            let iter = fields.iter().zip(values.iter().cloned());

            let deserializer = RecordDeserializer::from(iter);
            let value = serde_json::Value::deserialize(deserializer).unwrap();
            dbg!(&value);
            assert_eq!(value, serde_json::json!({"a": 1, "b": 2, "c": 3}));
        }
    }
}
