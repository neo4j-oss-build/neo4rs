use crate::{
    types::{
        serde::{
            date_time::BoltDateTimeVisitor,
            element::ElementDataDeserializer,
            node::BoltNodeVisitor,
            path::BoltPathVisitor,
            point::{self, BoltPointDeserializer, BoltPointVisitor},
            rel::BoltRelationVisitor,
            urel::BoltUnboundedRelationVisitor,
        },
        BoltBoolean, BoltBytes, BoltFloat, BoltInteger, BoltKind, BoltList, BoltMap, BoltNull,
        BoltString, BoltType,
    },
    DeError,
};

use std::{fmt, result::Result};

use bytes::Bytes;
use serde::{
    de::{
        value::{BorrowedStrDeserializer, MapDeserializer, SeqDeserializer},
        DeserializeSeed, Deserializer, EnumAccess, Error, IntoDeserializer, Unexpected as Unexp,
        VariantAccess, Visitor,
    },
    forward_to_deserialize_any, Deserialize,
};

impl<'de> Deserialize<'de> for BoltType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_enum(std::any::type_name::<BoltType>(), &[], BoltTypeVisitor)
    }
}

impl BoltType {
    pub(crate) fn to<'this, T>(&'this self) -> Result<T, DeError>
    where
        T: Deserialize<'this>,
    {
        T::deserialize(self.into_deserializer())
    }
}

impl BoltMap {
    pub(crate) fn to<'this, T>(&'this self) -> Result<T, DeError>
    where
        T: Deserialize<'this>,
    {
        T::deserialize(MapDeserializer::new(self.value.iter()))
    }
}

struct BoltTypeVisitor;

impl<'de> Visitor<'de> for BoltTypeVisitor {
    type Value = BoltType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid bolt type")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BoltType::Boolean(BoltBoolean::new(v)))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BoltType::Integer(BoltInteger::new(v)))
    }

    fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match i64::try_from(v) {
            Ok(v) => self.visit_i64(v),
            Err(_) => Err(E::custom(format!("i128 out of range: {}", v))),
        }
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match i64::try_from(v) {
            Ok(v) => self.visit_i64(v),
            Err(_) => Err(E::custom(format!("u64 out of range: {}", v))),
        }
    }

    fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match i64::try_from(v) {
            Ok(v) => self.visit_i64(v),
            Err(_) => Err(E::custom(format!("u128 out of range: {}", v))),
        }
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BoltType::Float(BoltFloat::new(v)))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BoltType::String(BoltString::new(v)))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BoltType::Bytes(BoltBytes::new(Bytes::copy_from_slice(v))))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BoltType::Null(BoltNull))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        ::serde::de::Deserialize::deserialize(deserializer)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BoltType::Null(BoltNull))
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.visit_some(deserializer)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: ::serde::de::SeqAccess<'de>,
    {
        let mut items = match seq.size_hint() {
            Some(s) => BoltList::with_capacity(s),
            None => BoltList::new(),
        };

        while let Some(next) = seq.next_element()? {
            items.push(next);
        }

        Ok(BoltType::List(items))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: ::serde::de::MapAccess<'de>,
    {
        let mut items = match map.size_hint() {
            Some(s) => BoltMap::with_capacity(s),
            None => BoltMap::new(),
        };

        while let Some((key, value)) = map.next_entry()? {
            items.put(key, value);
        }

        Ok(BoltType::Map(items))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BoltType::String(BoltString { value: v }))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BoltType::Bytes(BoltBytes {
            value: Bytes::from(v),
        }))
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::EnumAccess<'de>,
    {
        let (kind, variant): (BoltKind, _) = data.variant()?;
        match kind {
            BoltKind::Null => variant.tuple_variant(1, self),
            BoltKind::String => variant.tuple_variant(1, self),
            BoltKind::Boolean => variant.tuple_variant(1, self),
            BoltKind::Map => variant.tuple_variant(1, self),
            BoltKind::Integer => variant.tuple_variant(1, self),
            BoltKind::Float => variant.tuple_variant(1, self),
            BoltKind::List => variant.tuple_variant(1, self),
            BoltKind::Node => variant
                .tuple_variant(1, BoltNodeVisitor)
                .map(BoltType::Node),
            BoltKind::Relation => variant
                .tuple_variant(1, BoltRelationVisitor)
                .map(BoltType::Relation),
            BoltKind::UnboundedRelation => variant
                .tuple_variant(1, BoltUnboundedRelationVisitor)
                .map(BoltType::UnboundedRelation),
            BoltKind::Point2D => variant
                .struct_variant(
                    &point::Field::NAMES[..3],
                    BoltPointVisitor::_2d::<A::Error>(),
                )
                .map(BoltType::Point2D),
            BoltKind::Point3D => variant
                .struct_variant(point::Field::NAMES, BoltPointVisitor::_3d::<A::Error>())
                .map(BoltType::Point3D),
            BoltKind::Bytes => variant.tuple_variant(1, self),
            BoltKind::Path => variant
                .tuple_variant(1, BoltPathVisitor)
                .map(BoltType::Path),
            BoltKind::Duration => variant.tuple_variant(1, self),
            BoltKind::Date => variant.tuple_variant(1, self),
            BoltKind::Time => variant.tuple_variant(1, self),
            BoltKind::LocalTime => variant.tuple_variant(1, self),
            BoltKind::DateTime => variant
                .tuple_variant(1, BoltDateTimeVisitor)
                .map(BoltType::DateTime),
            BoltKind::LocalDateTime => variant.tuple_variant(1, self),
            BoltKind::DateTimeZoneId => variant.tuple_variant(1, self),
        }
    }
}

pub struct BoltTypeDeserializer<'de> {
    value: &'de BoltType,
}

impl<'de> Deserializer<'de> for BoltTypeDeserializer<'de> {
    type Error = DeError;

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            BoltType::List(v) => visitor.visit_seq(SeqDeserializer::new(v.value.iter())),
            BoltType::Bytes(v) => visitor.visit_seq(SeqDeserializer::new(v.value.iter().copied())),
            BoltType::Point2D(p) => p.into_deserializer().deserialize_seq(visitor),
            BoltType::Point3D(p) => p.into_deserializer().deserialize_seq(visitor),
            _ => self.unexpected(visitor),
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            BoltType::Map(v) => visitor.visit_map(MapDeserializer::new(v.value.iter())),
            BoltType::Node(v) => v.into_deserializer().deserialize_map(visitor),
            BoltType::Relation(v) => v.into_deserializer().deserialize_map(visitor),
            BoltType::UnboundedRelation(v) => v.into_deserializer().deserialize_map(visitor),
            BoltType::Path(p) => p.into_deserializer().deserialize_map(visitor),
            BoltType::Point2D(p) => p.into_deserializer().deserialize_map(visitor),
            BoltType::Point3D(p) => p.into_deserializer().deserialize_map(visitor),
            _ => self.unexpected(visitor),
        }
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            BoltType::Map(v) => visitor.visit_map(MapDeserializer::new(v.value.iter())),
            BoltType::Node(v) => v
                .into_deserializer()
                .deserialize_struct(name, fields, visitor),
            BoltType::Relation(v) => v
                .into_deserializer()
                .deserialize_struct(name, fields, visitor),
            BoltType::UnboundedRelation(v) => v
                .into_deserializer()
                .deserialize_struct(name, fields, visitor),
            BoltType::Path(p) => p
                .into_deserializer()
                .deserialize_struct(name, fields, visitor),
            BoltType::Point2D(p) => p
                .into_deserializer()
                .deserialize_struct(name, fields, visitor),
            BoltType::Point3D(p) => p
                .into_deserializer()
                .deserialize_struct(name, fields, visitor),
            _ => self.unexpected(visitor),
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            BoltType::Node(v) => v
                .into_deserializer()
                .deserialize_newtype_struct(name, visitor),
            BoltType::Relation(v) => v
                .into_deserializer()
                .deserialize_newtype_struct(name, visitor),
            BoltType::UnboundedRelation(v) => v
                .into_deserializer()
                .deserialize_newtype_struct(name, visitor),
            BoltType::Path(p) => p
                .into_deserializer()
                .deserialize_newtype_struct(name, visitor),
            BoltType::Point2D(p) => p
                .into_deserializer()
                .deserialize_newtype_struct(name, visitor),
            BoltType::Point3D(p) => p
                .into_deserializer()
                .deserialize_newtype_struct(name, visitor),
            _ => self.unexpected(visitor),
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            BoltType::List(v) if v.len() == len => {
                visitor.visit_seq(SeqDeserializer::new(v.value.iter()))
            }
            BoltType::Point2D(p) => p.into_deserializer().deserialize_tuple(len, visitor),
            BoltType::Point3D(p) => p.into_deserializer().deserialize_tuple(len, visitor),
            _ => self.unexpected(visitor),
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            BoltType::String(v) => visitor.visit_borrowed_str(&v.value),
            BoltType::DateTime(_) => self.deserialize_string(visitor),
            _ => self.unexpected(visitor),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            BoltType::String(v) => visitor.visit_string(v.value.clone()),
            BoltType::DateTime(datetime) => {
                let datetime = datetime.try_to_chrono().map_err(|_| {
                    Error::custom("Could not convert Neo4j DateTime into chrono::DateTime")
                })?;
                visitor.visit_string(datetime.to_rfc3339())
            }
            _ => self.unexpected(visitor),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let BoltType::Bytes(v) = self.value {
            visitor.visit_borrowed_bytes(&v.value)
        } else {
            self.unexpected(visitor)
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let BoltType::Bytes(v) = self.value {
            visitor.visit_byte_buf(v.value.to_vec())
        } else {
            self.unexpected(visitor)
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let BoltType::Boolean(v) = self.value {
            visitor.visit_bool(v.value)
        } else {
            self.unexpected(visitor)
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_integer(visitor)?;
        visitor.visit_i8(v)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_integer(visitor)?;
        visitor.visit_i16(v)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_integer(visitor)?;
        visitor.visit_i32(v)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_integer(visitor)?;
        visitor.visit_i64(v)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_integer(visitor)?;
        visitor.visit_u8(v)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_integer(visitor)?;
        visitor.visit_u16(v)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_integer(visitor)?;
        visitor.visit_u32(v)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_integer(visitor)?;
        visitor.visit_u64(v)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_float(visitor)?;
        visitor.visit_f32(v)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let (v, visitor) = self.read_float(visitor)?;
        visitor.visit_f64(v)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let BoltType::Null(_) = self.value {
            visitor.visit_unit()
        } else {
            self.unexpected(visitor)
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let BoltType::Null(_) = self.value {
            visitor.visit_unit()
        } else {
            self.unexpected(visitor)
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if name != std::any::type_name::<BoltType>() {
            return Err(DeError::invalid_type(Unexp::Str(name), &"BoltType"));
        }

        visitor.visit_enum(BoltEnum { value: self.value })
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.unexpected(visitor)
    }

    forward_to_deserialize_any! { char identifier }
}

impl<'de> BoltTypeDeserializer<'de> {
    fn new(value: &'de BoltType) -> Self {
        Self { value }
    }

    fn read_integer<T, E, V>(self, visitor: V) -> Result<(T, V), DeError>
    where
        V: Visitor<'de>,
        i64: TryInto<T, Error = E>,
        E: Into<std::num::TryFromIntError>,
    {
        let integer = match self.value {
            BoltType::Integer(v) => v.value,
            BoltType::DateTime(datetime) => match datetime.try_to_chrono() {
                Ok(datetime) => match std::any::type_name::<V>() {
                    "chrono::datetime::serde::MicroSecondsTimestampVisitor" => {
                        datetime.timestamp_micros()
                    }
                    "chrono::datetime::serde::MilliSecondsTimestampVisitor" => {
                        datetime.timestamp_millis()
                    }
                    "chrono::datetime::serde::SecondsTimestampVisitor" => datetime.timestamp(),
                    _ => datetime.timestamp_nanos(),
                },
                Err(_) => return Err(DeError::DateTImeOutOfBounds(std::any::type_name::<T>())),
            },
            _ => return self.unexpected(visitor),
        };

        match integer.try_into() {
            Ok(v) => Ok((v, visitor)),
            Err(e) => Err(DeError::IntegerOutOfBounds(
                e.into(),
                integer,
                std::any::type_name::<T>(),
            )),
        }
    }

    fn read_float<T, V>(self, visitor: V) -> Result<(T, V), DeError>
    where
        V: Visitor<'de>,
        T: FromFloat,
    {
        if let BoltType::Float(v) = self.value {
            Ok((T::from_float(v.value), visitor))
        } else {
            self.unexpected(visitor)
        }
    }

    fn unexpected<V, T>(self, visitor: V) -> Result<T, DeError>
    where
        V: Visitor<'de>,
    {
        let typ = match self.value {
            BoltType::String(v) => Unexp::Str(&v.value),
            BoltType::Boolean(v) => Unexp::Bool(v.value),
            BoltType::Map(_) => Unexp::Map,
            BoltType::Null(_) => Unexp::Unit,
            BoltType::Integer(v) => Unexp::Signed(v.value),
            BoltType::Float(v) => Unexp::Float(v.value),
            BoltType::List(_) => Unexp::Seq,
            BoltType::Node(_) => Unexp::Map,
            BoltType::Relation(_) => Unexp::Map,
            BoltType::UnboundedRelation(_) => Unexp::Map,
            BoltType::Point2D(_) => Unexp::Other("Point2D"),
            BoltType::Point3D(_) => Unexp::Other("Point3D"),
            BoltType::Bytes(v) => Unexp::Bytes(&v.value),
            BoltType::Path(_) => Unexp::Other("Path"),
            BoltType::Duration(_) => Unexp::Other("Duration"),
            BoltType::Date(_) => Unexp::Other("Date"),
            BoltType::Time(_) => Unexp::Other("Time"),
            BoltType::LocalTime(_) => Unexp::Other("LocalTime"),
            BoltType::DateTime(_) => Unexp::Other("DateTime"),
            BoltType::LocalDateTime(_) => Unexp::Other("LocalDateTime"),
            BoltType::DateTimeZoneId(_) => Unexp::Other("DateTimeZoneId"),
        };

        Err(DeError::invalid_type(typ, &visitor))
    }
}

struct BoltEnum<'de> {
    value: &'de BoltType,
}

impl<'de> EnumAccess<'de> for BoltEnum<'de> {
    type Error = DeError;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let kind = match self.value {
            BoltType::String(_) => BoltKind::String,
            BoltType::Boolean(_) => BoltKind::Boolean,
            BoltType::Map(_) => BoltKind::Map,
            BoltType::Null(_) => BoltKind::Null,
            BoltType::Integer(_) => BoltKind::Integer,
            BoltType::Float(_) => BoltKind::Float,
            BoltType::List(_) => BoltKind::List,
            BoltType::Node(_) => BoltKind::Node,
            BoltType::Relation(_) => BoltKind::Relation,
            BoltType::UnboundedRelation(_) => BoltKind::UnboundedRelation,
            BoltType::Point2D(_) => BoltKind::Point2D,
            BoltType::Point3D(_) => BoltKind::Point3D,
            BoltType::Bytes(_) => BoltKind::Bytes,
            BoltType::Path(_) => BoltKind::Path,
            BoltType::Duration(_) => BoltKind::Duration,
            BoltType::Date(_) => BoltKind::Date,
            BoltType::Time(_) => BoltKind::Time,
            BoltType::LocalTime(_) => BoltKind::LocalTime,
            BoltType::DateTime(_) => BoltKind::DateTime,
            BoltType::LocalDateTime(_) => BoltKind::LocalDateTime,
            BoltType::DateTimeZoneId(_) => BoltKind::DateTimeZoneId,
        };
        let val = seed.deserialize(kind.into_deserializer())?;
        Ok((val, self))
    }
}

impl<'de> VariantAccess<'de> for BoltEnum<'de> {
    type Error = DeError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Err(DeError::invalid_type(Unexp::TupleVariant, &"unit variant"))
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        Err(DeError::invalid_type(
            Unexp::TupleVariant,
            &"newtype variant",
        ))
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            BoltType::String(s) => visitor.visit_borrowed_str(&s.value),
            BoltType::Boolean(b) => visitor.visit_bool(b.value),
            BoltType::Map(m) => visitor.visit_map(MapDeserializer::new(m.value.iter())),
            BoltType::Null(_) => visitor.visit_unit(),
            BoltType::Integer(i) => visitor.visit_i64(i.value),
            BoltType::Float(f) => visitor.visit_f64(f.value),
            BoltType::List(l) => visitor.visit_seq(SeqDeserializer::new(l.value.iter())),
            BoltType::Node(n) => ElementDataDeserializer::new(n).tuple_variant(len, visitor),
            BoltType::Relation(r) => ElementDataDeserializer::new(r).tuple_variant(len, visitor),
            BoltType::UnboundedRelation(r) => {
                ElementDataDeserializer::new(r).tuple_variant(len, visitor)
            }
            BoltType::Point2D(p) => BoltPointDeserializer::new(p).deserialize_tuple(len, visitor),
            BoltType::Point3D(p) => BoltPointDeserializer::new(p).deserialize_tuple(len, visitor),
            BoltType::Bytes(b) => visitor.visit_borrowed_bytes(&b.value),
            BoltType::Path(p) => ElementDataDeserializer::new(p).tuple_variant(len, visitor),
            BoltType::Duration(_) => todo!("duration as mapaccess visit_map"),
            BoltType::Date(_) => todo!("date as mapaccess visit_map"),
            BoltType::Time(_) => todo!("time as mapaccess visit_map"),
            BoltType::LocalTime(_) => todo!("localtime as mapaccess visit_map"),
            BoltType::DateTime(datetime) => visitor.visit_map(datetime.map_access()),
            BoltType::LocalDateTime(_) => todo!("localdatetime as mapaccess visit_map"),
            BoltType::DateTimeZoneId(_) => todo!("datetimezoneid as mapaccess visit_map"),
        }
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(DeError::invalid_type(
            Unexp::TupleVariant,
            &"struct variant",
        ))
    }
}

impl<'de> IntoDeserializer<'de, DeError> for &'de BoltType {
    type Deserializer = BoltTypeDeserializer<'de>;

    fn into_deserializer(self) -> Self::Deserializer {
        BoltTypeDeserializer::new(self)
    }
}

impl<'de> IntoDeserializer<'de, DeError> for &'de BoltString {
    type Deserializer = BorrowedStrDeserializer<'de, DeError>;

    fn into_deserializer(self) -> Self::Deserializer {
        BorrowedStrDeserializer::new(&self.value)
    }
}

trait FromFloat {
    fn from_float(f: f64) -> Self;
}

impl FromFloat for f32 {
    fn from_float(f: f64) -> Self {
        f as f32
    }
}

impl FromFloat for f64 {
    fn from_float(f: f64) -> Self {
        f
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, fmt::Debug};

    use super::*;

    use crate::{
        types::{
            BoltDateTime, BoltInteger, BoltMap, BoltNode, BoltNull, BoltPoint2D, BoltPoint3D,
            BoltRelation, BoltUnboundedRelation,
        },
        EndNodeId, Id, Keys, Labels, StartNodeId, Type,
    };

    use chrono::{DateTime, FixedOffset, Utc};
    use serde::Deserialize;

    #[test]
    fn map_with_extra_fields() {
        #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
        struct Person {
            name: String,
            age: u8,
        }

        let map = [
            (BoltString::from("name"), BoltType::from("Alice")),
            (BoltString::from("age"), BoltType::from(42)),
            (BoltString::from("bar"), BoltType::from(1337)),
        ]
        .into_iter()
        .collect::<BoltMap>();
        let map = BoltType::Map(map);

        let actual = map.to::<Person>().unwrap();
        let expected = Person {
            name: "Alice".into(),
            age: 42,
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn map_with_extra_fields_fails_for_deny_unknown_fields() {
        #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Person {
            name: String,
            age: u8,
        }

        let map = [
            (BoltString::from("name"), BoltType::from("Alice")),
            (BoltString::from("age"), BoltType::from(42)),
            (BoltString::from("bar"), BoltType::from(1337)),
        ]
        .into_iter()
        .collect::<BoltMap>();
        let map = BoltType::Map(map);

        assert!(map.to::<Person>().is_err());
    }

    #[test]
    fn simple_struct() {
        #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
        struct Person {
            name: String,
            age: u8,
        }

        let map = [
            (BoltString::from("name"), BoltType::from("Alice")),
            (BoltString::from("age"), BoltType::from(42)),
        ]
        .into_iter()
        .collect::<BoltMap>();

        let actual = map.to::<Person>().unwrap();
        let expected = Person {
            name: "Alice".into(),
            age: 42,
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn tuple_struct_from_list() {
        #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
        struct Person(String, u8);

        let list = BoltType::from(vec![BoltType::from("Alice"), BoltType::from(42)]);
        let actual = list.to::<Person>().unwrap();
        let expected = Person("Alice".into(), 42);

        assert_eq!(actual, expected);
    }

    #[test]
    fn tuple_struct_from_map_fails() {
        // We do not support this since maps are unordered and
        // we cannot gurantee that the values are in the same
        // order as the tuple struct fields.
        #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
        struct Person(String, u8);

        let map = [
            (BoltString::from("name"), BoltType::from("Alice")),
            (BoltString::from("age"), BoltType::from(42)),
        ]
        .into_iter()
        .collect::<BoltMap>();

        let map = BoltType::Map(map);

        assert!(map.to::<Person>().is_err());
    }

    #[test]
    fn node() {
        #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
        struct Person {
            id: Id,
            labels: Labels,
            keys: Keys,
            name: String,
            age: u8,
        }

        let id = BoltInteger::new(1337);
        let labels = vec!["Person".into()].into();
        let properties = vec![
            ("name".into(), "Alice".into()),
            ("age".into(), 42_u16.into()),
        ]
        .into_iter()
        .collect();

        let node = BoltNode {
            id,
            labels,
            properties,
        };
        let node = BoltType::Node(node);

        let actual = node.to::<Person>().unwrap();
        let expected = Person {
            id: Id(1337),
            labels: Labels(vec!["Person".into()]),
            keys: Keys(["name".into(), "age".into()].into()),
            name: "Alice".into(),
            age: 42,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn relation() {
        #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
        struct Person {
            id: Id,
            start: StartNodeId,
            end: EndNodeId,
            typ: Type,
            keys: Keys,
            name: String,
            age: u8,
        }

        let id = BoltInteger::new(1337);
        let start_node_id = BoltInteger::new(21);
        let end_node_id = BoltInteger::new(84);
        let typ = "Person".into();
        let properties = vec![
            ("name".into(), "Alice".into()),
            ("age".into(), 42_u16.into()),
        ]
        .into_iter()
        .collect();

        let relation = BoltRelation {
            id,
            start_node_id,
            end_node_id,
            properties,
            typ,
        };
        let relation = BoltType::Relation(relation);

        let actual = relation.to::<Person>().unwrap();
        let expected = Person {
            id: Id(1337),
            start: StartNodeId(21),
            end: EndNodeId(84),
            typ: Type("Person".into()),
            keys: Keys(["name".into(), "age".into()].into()),
            name: "Alice".into(),
            age: 42,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn unbounded_relation() {
        #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
        struct Person {
            id: Id,
            typ: Type,
            keys: Keys,
            name: String,
            age: u8,
        }

        let id = BoltInteger::new(1337);
        let typ = "Person".into();
        let properties = vec![
            ("name".into(), "Alice".into()),
            ("age".into(), 42_u16.into()),
        ]
        .into_iter()
        .collect();

        let relation = BoltUnboundedRelation {
            id,
            properties,
            typ,
        };
        let relation = BoltType::UnboundedRelation(relation);

        let actual = relation.to::<Person>().unwrap();
        let expected = Person {
            id: Id(1337),
            typ: Type("Person".into()),
            keys: Keys(["name".into(), "age".into()].into()),
            name: "Alice".into(),
            age: 42,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn tuple() {
        let list = BoltType::from(vec![BoltType::from("Alice"), BoltType::from(42)]);
        let actual = list.to::<(String, u8)>().unwrap();
        let expected = ("Alice".into(), 42);

        assert_eq!(actual, expected);
    }

    #[test]
    fn borrowing_struct() {
        #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
        struct Person<'a> {
            name: &'a str,
            age: u8,
        }

        let map = [
            (BoltString::from("name"), BoltType::from("Alice")),
            (BoltString::from("age"), BoltType::from(42)),
        ]
        .into_iter()
        .collect::<BoltMap>();

        let actual = map.to::<Person>().unwrap();
        let expected = Person {
            name: "Alice",
            age: 42,
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn various_types() {
        #[derive(Clone, Debug, PartialEq, Deserialize)]
        struct Bag<'a> {
            borrowed: &'a str,
            owned: String,

            float: f64,
            int: i32,
            long: i64,

            boolean: bool,

            unit: (),
        }

        let map = [
            (
                BoltString::from("borrowed"),
                BoltType::from("I am borrowed"),
            ),
            (
                BoltString::from("owned"),
                BoltType::from("I am cloned and owned"),
            ),
            (BoltString::from("float"), BoltType::from(13.37)),
            (BoltString::from("int"), BoltType::from(42_i32)),
            (BoltString::from("long"), BoltType::from(1337_i64)),
            (BoltString::from("boolean"), BoltType::from(true)),
            (BoltString::from("unit"), BoltType::Null(BoltNull)),
        ]
        .into_iter()
        .collect::<BoltMap>();

        let actual = map.to::<Bag>().unwrap();
        let expected = Bag {
            borrowed: "I am borrowed",
            owned: "I am cloned and owned".to_owned(),
            float: 13.37,
            int: 42,
            long: 1337,
            boolean: true,
            unit: (),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn std_bytes() {
        #[derive(Clone, Debug, PartialEq, Deserialize)]
        struct Bytes<'a> {
            bytes: Vec<u8>,
            slice: &'a [u8],
        }

        let map = [
            (BoltString::from("bytes"), BoltType::from(vec![4_u8, 2])),
            (
                BoltString::from("slice"),
                BoltType::from(vec![1_u8, 3, 3, 7]),
            ),
        ]
        .into_iter()
        .collect::<BoltMap>();

        let actual = map.to::<Bytes>().unwrap();
        let expected = Bytes {
            bytes: vec![4, 2],
            slice: &[1, 3, 3, 7],
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn serde_bytes_bytes() {
        #[derive(Clone, Debug, PartialEq, Deserialize)]
        struct Bytes<'a> {
            #[serde(with = "serde_bytes")]
            bytes: Vec<u8>,
            #[serde(with = "serde_bytes")]
            slice: &'a [u8],
        }

        let map = [
            (BoltString::from("bytes"), BoltType::from(vec![4_u8, 2])),
            (
                BoltString::from("slice"),
                BoltType::from(vec![1_u8, 3, 3, 7]),
            ),
        ]
        .into_iter()
        .collect::<BoltMap>();

        let actual = map.to::<Bytes>().unwrap();
        let expected = Bytes {
            bytes: vec![4, 2],
            slice: &[1, 3, 3, 7],
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn serde_with_bytes() {
        use serde_with::{serde_as, Bytes};

        #[serde_as]
        #[derive(Clone, Debug, PartialEq, Deserialize)]
        struct AsBytes<'a> {
            #[serde_as(as = "Bytes")]
            array: [u8; 4],

            #[serde_as(as = "Bytes")]
            boxed: Box<[u8]>,

            #[serde_as(as = "Bytes")]
            #[serde(borrow)]
            cow: Cow<'a, [u8]>,

            #[serde_as(as = "Bytes")]
            #[serde(borrow)]
            cow_array: Cow<'a, [u8; 2]>,

            #[serde_as(as = "Bytes")]
            bytes: Vec<u8>,

            #[serde_as(as = "Bytes")]
            slice: &'a [u8],
        }

        let map = [
            (
                BoltString::from("array"),
                BoltType::from(vec![1_u8, 3, 3, 7]),
            ),
            (BoltString::from("boxed"), BoltType::from(vec![4_u8, 2])),
            (BoltString::from("cow"), BoltType::from(vec![1_u8, 3, 3, 7])),
            (BoltString::from("cow_array"), BoltType::from(vec![4_u8, 2])),
            (
                BoltString::from("bytes"),
                BoltType::from(vec![1_u8, 3, 3, 7]),
            ),
            (BoltString::from("slice"), BoltType::from(vec![4_u8, 2])),
        ]
        .into_iter()
        .collect::<BoltMap>();

        let actual = map.to::<AsBytes>().unwrap();
        let expected = AsBytes {
            array: [1, 3, 3, 7],
            boxed: vec![4, 2].into_boxed_slice(),
            cow: vec![1_u8, 3, 3, 7].into(),
            cow_array: Cow::Owned([4_u8, 2]),
            bytes: vec![1, 3, 3, 7],
            slice: &[4, 2],
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn nested_struct() {
        #[derive(Clone, Debug, PartialEq, Deserialize)]
        struct Person {
            name: String,
            age: u32,
        }

        #[derive(Clone, Debug, PartialEq, Deserialize)]
        struct Couple {
            p0: Person,
            p1: Person,
        }

        let map = [
            (
                BoltString::from("p0"),
                BoltType::Map(
                    [
                        (BoltString::from("name"), BoltType::from("Alice")),
                        (BoltString::from("age"), BoltType::from(42)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ),
            (
                BoltString::from("p1"),
                BoltType::Map(
                    [
                        (BoltString::from("name"), BoltType::from("Bob")),
                        (BoltString::from("age"), BoltType::from(1337)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ),
        ]
        .into_iter()
        .collect::<BoltMap>();

        let actual = map.to::<Couple>().unwrap();
        let expected = Couple {
            p0: Person {
                name: "Alice".into(),
                age: 42,
            },
            p1: Person {
                name: "Bob".into(),
                age: 1337,
            },
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn list() {
        let list = BoltType::from(vec![42_i64, 1337]);
        let actual = list.to::<Vec<i64>>().unwrap();

        assert_eq!(actual, vec![42_i64, 1337]);
    }

    #[test]
    fn nested_list() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Foo {
            bars: Vec<i64>,
        }

        let data = [(BoltString::from("bars"), BoltType::from(vec![42, 1337]))]
            .into_iter()
            .collect::<BoltMap>();
        let actual = data.to::<Foo>().unwrap();
        let expected = Foo {
            bars: vec![42, 1337],
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn datetime() {
        let expected = DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00").unwrap();

        let datetime = BoltDateTime::from(expected);
        let datetime = BoltType::DateTime(datetime);

        let actual = datetime.to::<DateTime<FixedOffset>>().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn datetime_nanoseconds() {
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(transparent)]
        struct S {
            #[serde(with = "chrono::serde::ts_nanoseconds")]
            datetime: DateTime<Utc>,
        }

        let expected = DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00").unwrap();

        let datetime = BoltDateTime::from(expected);
        let datetime = BoltType::DateTime(datetime);

        let actual = datetime.to::<S>().unwrap().datetime;
        assert_eq!(actual, expected);
    }

    #[test]
    fn datetime_opt_nanoseconds() {
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(transparent)]
        struct S {
            #[serde(with = "chrono::serde::ts_nanoseconds_option")]
            datetime: Option<DateTime<Utc>>,
        }

        let expected = DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00").unwrap();

        let datetime = BoltDateTime::from(expected);
        let datetime = BoltType::DateTime(datetime);

        let actual = datetime.to::<S>().unwrap().datetime.unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn datetime_microseconds() {
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(transparent)]
        struct S {
            #[serde(with = "chrono::serde::ts_microseconds")]
            datetime: DateTime<Utc>,
        }

        let expected = DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00").unwrap();

        let datetime = BoltDateTime::from(expected);
        let datetime = BoltType::DateTime(datetime);

        let actual = datetime.to::<S>().unwrap().datetime;
        assert_eq!(actual, expected);
    }

    #[test]
    fn datetime_opt_microseconds() {
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(transparent)]
        struct S {
            #[serde(with = "chrono::serde::ts_microseconds_option")]
            datetime: Option<DateTime<Utc>>,
        }

        let expected = DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00").unwrap();

        let datetime = BoltDateTime::from(expected);
        let datetime = BoltType::DateTime(datetime);

        let actual = datetime.to::<S>().unwrap().datetime.unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn datetime_milliseconds() {
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(transparent)]
        struct S {
            #[serde(with = "chrono::serde::ts_milliseconds")]
            datetime: DateTime<Utc>,
        }

        let expected = DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00").unwrap();

        let datetime = BoltDateTime::from(expected);
        let datetime = BoltType::DateTime(datetime);

        let actual = datetime.to::<S>().unwrap().datetime;
        assert_eq!(actual, expected);
    }

    #[test]
    fn datetime_opt_milliseconds() {
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(transparent)]
        struct S {
            #[serde(with = "chrono::serde::ts_milliseconds_option")]
            datetime: Option<DateTime<Utc>>,
        }

        let expected = DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00").unwrap();

        let datetime = BoltDateTime::from(expected);
        let datetime = BoltType::DateTime(datetime);

        let actual = datetime.to::<S>().unwrap().datetime.unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn datetime_seconds() {
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(transparent)]
        struct S {
            #[serde(with = "chrono::serde::ts_seconds")]
            datetime: DateTime<Utc>,
        }

        let expected = DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00").unwrap();

        let datetime = BoltDateTime::from(expected);
        let datetime = BoltType::DateTime(datetime);

        let actual = datetime.to::<S>().unwrap().datetime;
        assert_eq!(actual, expected);
    }

    #[test]
    fn datetime_opt_seconds() {
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(transparent)]
        struct S {
            #[serde(with = "chrono::serde::ts_seconds_option")]
            datetime: Option<DateTime<Utc>>,
        }

        let expected = DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00").unwrap();

        let datetime = BoltDateTime::from(expected);
        let datetime = BoltType::DateTime(datetime);

        let actual = datetime.to::<S>().unwrap().datetime.unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn point_2d() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct P {
            x: f64,
            y: f64,
        }

        let point = BoltType::Point2D(BoltPoint2D {
            sr_id: 420.into(),
            x: BoltFloat::new(42.0),
            y: BoltFloat::new(13.37),
        });

        let actual = point.to::<P>().unwrap();
        let expected = P { x: 42.0, y: 13.37 };

        assert_eq!(actual, expected);
    }

    #[test]
    fn point_3d() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct P {
            x: f64,
            y: f64,
            z: f64,
        }

        let point = BoltType::Point3D(BoltPoint3D {
            sr_id: 420.into(),
            x: BoltFloat::new(42.0),
            y: BoltFloat::new(13.37),
            z: BoltFloat::new(84.0),
        });
        let actual = point.to::<P>().unwrap();
        let expected = P {
            x: 42.0,
            y: 13.37,
            z: 84.0,
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn type_convert() {
        let i = BoltType::from(42);

        assert_eq!(i.to::<i8>().unwrap(), 42);
    }

    #[test]
    fn type_convert_error() {
        let i = BoltType::from(1337);

        assert_eq!(
            i.to::<i8>().unwrap_err().to_string(),
            "Could not convert the integer `1337` to the target type i8"
        );
    }

    #[test]
    fn deserialize_roundtrips() {
        let map = [
            ("age".into(), 42.into()),
            ("awesome".into(), true.into()),
            ("values".into(), vec![13.37, 42.84].into()),
            ("payload".into(), b"Hello, World!".as_slice().into()),
            ("secret".into(), BoltType::Null(BoltNull)),
            (
                "event".into(),
                DateTime::parse_from_rfc3339("1999-07-14T13:37:42+02:00")
                    .unwrap()
                    .into(),
            ),
        ]
        .into_iter()
        .collect::<BoltMap>();

        let map = BoltType::Map(map);

        let actual = map.to::<BoltType>().unwrap();
        assert_eq!(actual, map);
    }
}
