use fxhash::FxBuildHasher;
use hashlink::linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use amadeus_types::{Bson, Date, DateTime, Decimal, Enum, Group, Json, List, Time, Value};

use crate::internal::record::ParquetData;

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Predicate for [`Group`]s
pub struct MapPredicate<K, V> {
	pub(super) key: Option<K>,
	pub(super) value: Option<V>,
}
impl<K, V> MapPredicate<K, V> {
	pub fn new(key: Option<K>, value: Option<V>) -> Self {
		Self { key, value }
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Predicate for [`Group`]s
pub struct GroupPredicate(
	/// Map of field names to predicates for the fields in the group
	pub(super) LinkedHashMap<String, Option<<Value as ParquetData>::Predicate>>,
);
impl GroupPredicate {
	pub fn new<I>(fields: I) -> Self
	where
		I: IntoIterator<Item = (String, Option<<Value as ParquetData>::Predicate>)>,
	{
		Self(fields.into_iter().collect())
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Predicate for [`Value`]s
pub enum ValuePredicate {
	Bool(Option<<bool as ParquetData>::Predicate>),
	U8(Option<<u8 as ParquetData>::Predicate>),
	I8(Option<<i8 as ParquetData>::Predicate>),
	U16(Option<<u16 as ParquetData>::Predicate>),
	I16(Option<<i16 as ParquetData>::Predicate>),
	U32(Option<<u32 as ParquetData>::Predicate>),
	I32(Option<<i32 as ParquetData>::Predicate>),
	U64(Option<<u64 as ParquetData>::Predicate>),
	I64(Option<<i64 as ParquetData>::Predicate>),
	F32(Option<<f32 as ParquetData>::Predicate>),
	F64(Option<<f64 as ParquetData>::Predicate>),
	Date(Option<<Date as ParquetData>::Predicate>),
	Time(Option<<Time as ParquetData>::Predicate>),
	DateTime(Option<<DateTime as ParquetData>::Predicate>),
	Decimal(Option<<Decimal as ParquetData>::Predicate>),
	ByteArray(Option<<List<u8> as ParquetData>::Predicate>),
	Bson(Option<<Bson as ParquetData>::Predicate>),
	String(Option<<String as ParquetData>::Predicate>),
	Json(Option<<Json as ParquetData>::Predicate>),
	Enum(Option<<Enum as ParquetData>::Predicate>),
	List(Box<Option<<List<Value> as ParquetData>::Predicate>>),
	Map(Box<Option<<HashMap<Value, Value> as ParquetData>::Predicate>>),
	Group(Option<<Group as ParquetData>::Predicate>),
	Option(Box<Option<<Option<Value> as ParquetData>::Predicate>>),
}
impl ValuePredicate {
	pub fn is_list(&self) -> bool {
		matches!(self, Self::List(_))
	}
	pub fn is_map(&self) -> bool {
		matches!(self, Self::Map(_))
	}
	pub fn is_group(&self) -> bool {
		matches!(self, Self::Group(_))
	}
	pub fn as_list(&self) -> Result<&Option<<List<Value> as ParquetData>::Predicate>, ()> {
		if let Self::List(x) = self {
			Ok(x)
		} else {
			Err(())
		}
	}
	pub fn as_map(&self) -> Result<&Option<<HashMap<Value, Value> as ParquetData>::Predicate>, ()> {
		if let Self::Map(x) = self {
			Ok(x)
		} else {
			Err(())
		}
	}
	pub fn as_group(&self) -> Result<&Option<<Group as ParquetData>::Predicate>, ()> {
		if let Self::Group(x) = self {
			Ok(x)
		} else {
			Err(())
		}
	}
}
