use csv::Error as InternalCsvError;
use educe::Educe;
use futures::{pin_mut, stream, AsyncReadExt, FutureExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_closure::FnMutNamed;
use std::{
	error, fmt::{self, Display}, io::Cursor, marker::PhantomData
};

use amadeus_core::{
	file::{File, Page, Partition}, into_par_stream::IntoDistributedStream, par_stream::DistributedStream, util::{DistParStream, ResultExpandIter}, Source
};

use super::{SerdeData, SerdeDeserializeGroup};

// #[doc(inline)]
// pub type Trim = csv::Trim;
// #[doc(inline)]
// pub type Terminator = csv::Terminator;

// open (assume exists,

// open: append/overwrite (fail on not existing)
// create: append/overwrite/fail on existing

// create, fail if exists
// create, overwrite if exists
// create, append if exists

// open, fail if not exists
// open, overwrite if exists
// open, append if exists

// open fail on exist/fail on not exist/no fail overwrite/append

// fail on exist
// fail on not exist; overwrite/append
// no fail; overwrite/append

// open: must exist; append
// create: overwrite

// trait File

#[derive(Educe)]
#[educe(Clone, Debug)]
pub struct Csv<File, Row>
where
	File: amadeus_core::file::File,
	Row: SerdeData,
{
	// delimiter: u8,
	// has_headers: bool,
	// flexible: bool,
	// trim: Trim,
	// terminator: Terminator,
	// quote: u8,
	// escape: Option<u8>,
	// double_quote: bool,
	// quoting: bool,
	// comment: Option<u8>,
	partitions: Vec<File::Partition>,
	marker: PhantomData<fn() -> Row>,
}
impl<F, Row> Csv<F, Row>
where
	F: File,
	Row: SerdeData,
{
	pub async fn new(file: F) -> Result<Self, <Self as Source>::Error> {
		Ok(Self {
			partitions: file.partitions().await.map_err(CsvError::File)?,
			marker: PhantomData,
		})
	}
	// pub fn open<Row>(files: Vec<PathBuf>) -> Csv<Row> {}
	// pub fn create<Row>(files: Vec<PathBuf>) -> Csv<Row> {}
}

type Error<P, E> = CsvError<E, <P as Partition>::Error, <<P as Partition>::Page as Page>::Error>;
#[cfg(not(nightly))]
type Output<P, Row: SerdeData, E> = std::pin::Pin<Box<dyn Stream<Item = Result<Row, Error<P, E>>>>>;
#[cfg(nightly)]
type Output<P: Partition, Row: SerdeData, E> = impl Stream<Item = Result<Row, Error<P, E>>>;

FnMutNamed! {
	pub type Closure<P, Row, E> = |self|partition=> P| -> Output<P, Row, E>
	where
		P: Partition,
		Row: SerdeData,
		E: 'static
	{
		#[allow(clippy::let_and_return)]
		let ret = async move {
				Ok(stream::iter(
					partition
						.pages()
						.await
						.map_err(CsvError::Partition)?
						.into_iter(),
				)
				.flat_map(|page| {
					async move {
						let mut buf = Vec::with_capacity(10 * 1024 * 1024);
						let reader = Page::reader(page);
						pin_mut!(reader);
						let _ = reader
							.read_to_end(&mut buf)
							.await
							.map_err(InternalCsvError::from)?;
						Ok(stream::iter(
							csv::ReaderBuilder::new()
								.has_headers(false)
								.from_reader(Cursor::new(buf))
								.into_deserialize()
								.map(|x: Result<SerdeDeserializeGroup<Row>, InternalCsvError>| {
									Ok(x?.0)
								}),
						))
					}
					.map(ResultExpandIter::new)
					.flatten_stream()
				})
				.map(|row: Result<Result<Row, InternalCsvError>, Error<P, E>>| Ok(row??)))
			}
			.map(ResultExpandIter::new)
			.flatten_stream()
			.map(|row: Result<Result<Row, Error<P, E>>, Error<P, E>>| Ok(row??));
		#[cfg(not(nightly))]
		let ret = ret.boxed_local();
		ret
	}
}

impl<F, Row> Source for Csv<F, Row>
where
	F: File,
	Row: SerdeData,
{
	type Item = Row;
	#[allow(clippy::type_complexity)]
	type Error = CsvError<
		F::Error,
		<F::Partition as Partition>::Error,
		<<F::Partition as Partition>::Page as Page>::Error,
	>;

	type ParStream = DistParStream<Self::DistStream>;
	#[cfg(not(nightly))]
	#[allow(clippy::type_complexity)]
	type DistStream = amadeus_core::par_stream::FlatMap<
		amadeus_core::into_par_stream::IterDistStream<std::vec::IntoIter<F::Partition>>,
		Closure<F::Partition, Row, F::Error>,
	>;
	#[cfg(nightly)]
	type DistStream = impl DistributedStream<Item = Result<Self::Item, Self::Error>>;

	fn par_stream(self) -> Self::ParStream {
		DistParStream::new(self.dist_stream())
	}
	#[allow(clippy::let_and_return)]
	fn dist_stream(self) -> Self::DistStream {
		self.partitions.into_dist_stream().flat_map(Closure::new())
	}
}

mod csverror {
	use serde::{Deserializer, Serializer};

	pub(crate) fn serialize<T, S>(_t: &T, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		unimplemented!()
	}
	pub(crate) fn deserialize<'de, T, D>(_deserializer: D) -> Result<T, D::Error>
	where
		D: Deserializer<'de>,
	{
		unimplemented!()
	}

	// TODO
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::Error")]
	// struct Error(Box<ErrorKind>);
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::ErrorKind")]
	// enum ErrorKind {
	// 	Io(io::Error),
	// 	Utf8 {
	// 		pos: Option<Position>,
	// 		err: Utf8Error,
	// 	},
	// 	UnequalLengths {
	// 		pos: Option<Position>,
	// 		expected_len: u64,
	// 		len: u64,
	// 	},
	// 	Seek,
	// 	Serialize(String),
	// 	Deserialize {
	// 		pos: Option<Position>,
	// 		err: DeserializeError,
	// 	},
	// }
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::Position")]
	// struct Position {
	// 	byte: u64,
	// 	line: u64,
	// 	record: u64,
	// }
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::Utf8Error")]
	// struct Utf8Error {
	// 	field: usize,
	// 	valid_up_to: usize,
	// }
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::DeserializeError")]
	// struct DeserializeError {
	// 	field: Option<u64>,
	// 	kind: DeserializeErrorKind,
	// }
	// #[derive(Serialize, Deserialize)]
	// #[serde(remote = "csv::DeserializeErrorKind")]
	// enum DeserializeErrorKind {
	// 	Message(String),
	// 	Unsupported(String),
	// 	UnexpectedEndOfRow,
	// 	InvalidUtf8(str::Utf8Error),
	// 	ParseBool(str::ParseBoolError),
	// 	ParseInt(num::ParseIntError),
	// 	ParseFloat(num::ParseFloatError),
	// }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum CsvError<A, B, C> {
	File(A),
	Partition(B),
	Page(C),
	Csv(#[serde(with = "csverror")] InternalCsvError),
}
impl<A, B, C> Clone for CsvError<A, B, C>
where
	A: Clone,
	B: Clone,
	C: Clone,
{
	fn clone(&self) -> Self {
		match self {
			Self::File(err) => Self::File(err.clone()),
			Self::Partition(err) => Self::Partition(err.clone()),
			Self::Page(err) => Self::Page(err.clone()),
			Self::Csv(err) => Self::Csv(serde::ser::Error::custom(err)),
		}
	}
}
impl<A, B, C> PartialEq for CsvError<A, B, C>
where
	A: PartialEq,
	B: PartialEq,
	C: PartialEq,
{
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::File(a), Self::File(b)) => a.eq(b),
			(Self::Partition(a), Self::Partition(b)) => a.eq(b),
			(Self::Page(a), Self::Page(b)) => a.eq(b),
			(Self::Csv(a), Self::Csv(b)) => a.to_string() == b.to_string(),
			_ => false,
		}
	}
}
impl<A, B, C> error::Error for CsvError<A, B, C>
where
	A: error::Error,
	B: error::Error,
	C: error::Error,
{
}
impl<A, B, C> Display for CsvError<A, B, C>
where
	A: Display,
	B: Display,
	C: Display,
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::File(err) => Display::fmt(err, f),
			Self::Partition(err) => Display::fmt(err, f),
			Self::Page(err) => Display::fmt(err, f),
			Self::Csv(err) => Display::fmt(err, f),
		}
	}
}
impl<A, B, C> From<InternalCsvError> for CsvError<A, B, C> {
	fn from(err: InternalCsvError) -> Self {
		Self::Csv(err)
	}
}
