use educe::Educe;
use futures::{pin_mut, stream, AsyncReadExt, FutureExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_closure::FnMutNamed;
use serde_json::Error as InternalJsonError;
use std::{
	error, fmt::{self, Debug, Display}, io::{self, Cursor}, marker::PhantomData
};

use amadeus_core::{
	file::{File, Page, Partition}, into_par_stream::IntoDistributedStream, par_stream::DistributedStream, util::{DistParStream, ResultExpandIter}, Source
};

use super::{SerdeData, SerdeDeserialize};

#[derive(Educe)]
#[educe(Clone, Debug)]
pub struct Json<File, Row>
where
	File: amadeus_core::file::File,
	Row: SerdeData,
{
	partitions: Vec<File::Partition>,
	marker: PhantomData<fn() -> Row>,
}
impl<F, Row> Json<F, Row>
where
	F: File,
	Row: SerdeData,
{
	pub async fn new(file: F) -> Result<Self, <Self as Source>::Error> {
		Ok(Self {
			partitions: file.partitions().await.map_err(JsonError::File)?,
			marker: PhantomData,
		})
	}
}

type Error<P, E> = JsonError<E, <P as Partition>::Error, <<P as Partition>::Page as Page>::Error>;
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
						.map_err(JsonError::Partition)?
						.into_iter(),
				)
				.flat_map(|page| {
					async move {
						let mut buf = Vec::with_capacity(10 * 1024 * 1024);
						let reader = Page::reader(page);
						pin_mut!(reader);
						let buf = PassError::new(
							reader.read_to_end(&mut buf).await.map(|_| Cursor::new(buf)),
						);
						Ok(stream::iter(
							serde_json::Deserializer::from_reader(buf).into_iter().map(
								|x: Result<SerdeDeserialize<Row>, InternalJsonError>| Ok(x?.0),
							),
						))
					}
					.map(ResultExpandIter::new)
					.flatten_stream()
				})
				.map(|row: Result<Result<Row, InternalJsonError>, Error<P, E>>| Ok(row??)))
			}
			.map(ResultExpandIter::new)
			.flatten_stream()
			.map(|row: Result<Result<Row, Error<P, E>>, Error<P, E>>| Ok(row??));
		#[cfg(not(nightly))]
		let ret = ret.boxed_local();
		ret
	}
}

impl<F, Row> Source for Json<F, Row>
where
	F: File,
	Row: SerdeData,
{
	type Item = Row;
	#[allow(clippy::type_complexity)]
	type Error = JsonError<
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

mod jsonerror {
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
}

#[derive(Serialize, Deserialize, Debug)]
pub enum JsonError<A, B, C> {
	File(A),
	Partition(B),
	Page(C),
	Json(#[serde(with = "jsonerror")] InternalJsonError),
}
impl<A, B, C> Clone for JsonError<A, B, C>
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
			Self::Json(err) => Self::Json(serde::de::Error::custom(err)),
		}
	}
}
impl<A, B, C> PartialEq for JsonError<A, B, C>
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
			(Self::Json(a), Self::Json(b)) => a.to_string() == b.to_string(),
			_ => false,
		}
	}
}
impl<A, B, C> error::Error for JsonError<A, B, C>
where
	A: error::Error,
	B: error::Error,
	C: error::Error,
{
}
impl<A, B, C> Display for JsonError<A, B, C>
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
			Self::Json(err) => Display::fmt(err, f),
		}
	}
}
impl<A, B, C> From<InternalJsonError> for JsonError<A, B, C> {
	fn from(err: InternalJsonError) -> Self {
		Self::Json(err)
	}
}

struct PassError<R>(Result<R, Option<io::Error>>);
impl<R> PassError<R> {
	fn new(r: Result<R, io::Error>) -> Self {
		Self(r.map_err(Some))
	}
}
impl<R> io::Read for PassError<R>
where
	R: io::Read,
{
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		match &mut self.0 {
			Ok(r) => r.read(buf),
			Err(r) => Err(r.take().unwrap()),
		}
	}
}
