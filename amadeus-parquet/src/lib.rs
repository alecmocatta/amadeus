//! Harmonious distributed data processing & analysis in Rust.
//!
//! <p style="font-family: 'Fira Sans',sans-serif;padding:0.3em 0"><strong>
//! <a href="https://crates.io/crates/amadeus">📦&nbsp;&nbsp;Crates.io</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://github.com/constellation-rs/amadeus">📑&nbsp;&nbsp;GitHub</a>&nbsp;&nbsp;│&nbsp;&nbsp;<a href="https://constellation.zulipchat.com/#narrow/stream/213231-amadeus">💬&nbsp;&nbsp;Chat</a>
//! </strong></p>
//!
//! This is a support crate of [Amadeus](https://github.com/constellation-rs/amadeus) and is not intended to be used directly. These types are re-exposed in [`amadeus::source`](https://docs.rs/amadeus/0.3/amadeus/source/index.html).

#![doc(html_root_url = "https://docs.rs/amadeus-parquet/0.4.2")]
#![cfg_attr(nightly, feature(bufreader_seek_relative))]
#![cfg_attr(nightly, feature(read_initializer))]
#![cfg_attr(nightly, feature(specialization))]
#![cfg_attr(nightly, feature(type_alias_impl_trait))]
#![cfg_attr(nightly, feature(test))]
#![warn(
	// missing_copy_implementations,
	// missing_debug_implementations,
	// missing_docs,
	// trivial_numeric_casts,
	unused_import_braces,
	unused_qualifications,
	unused_results,
	// unreachable_pub,
	// clippy::pedantic,
)]
#![allow(
	clippy::module_name_repetitions,
	clippy::similar_names,
	clippy::if_not_else,
	clippy::must_use_candidate,
	clippy::missing_errors_doc,
	dead_code,
	clippy::all,
	incomplete_features
)]
// #![deny(unsafe_code)]

#[cfg(nightly)]
extern crate test;

#[cfg(nightly)]
mod internal;

#[cfg(nightly)]
mod wrap {
	use super::internal;
	use async_trait::async_trait;
	use educe::Educe;
	use futures::{pin_mut, stream, AsyncReadExt, FutureExt, StreamExt};
	use internal::{
		errors::ParquetError as InternalParquetError, file::reader::{FileReader, ParquetReader, SerializedFileReader}
	};
	use serde::{Deserialize, Serialize};
	use serde_closure::*;
	use std::{
		error, fmt::{self, Debug, Display}, io::Cursor, marker::PhantomData, ops::FnMut
	};

	use amadeus_core::{
		file::{Directory, File, Page, Partition, PathBuf}, into_par_stream::IntoDistributedStream, par_stream::DistributedStream, util::{DistParStream, ResultExpandIter}, Source
	};

	pub use internal::record::{
		predicates::{GroupPredicate, ValuePredicate}, ParquetData
	};

	#[doc(hidden)]
	pub mod derive {
		pub use super::{
			internal::{
				basic::Repetition, column::reader::ColumnReader, errors::{ParquetError, Result as ParquetResult}, record::{DisplaySchemaGroup, Reader, Schema as ParquetSchema}, schema::types::{ColumnPath, Type}
			}, ParquetData
		};
	}

	#[derive(Educe)]
	#[educe(Clone, Debug)]
	pub struct Parquet<File, Row>
	where
		File: amadeus_core::file::File,
		Row: ParquetData,
	{
		partitions: Vec<File::Partition>,
		row_predicate: Option<Row::Predicate>,
		marker: PhantomData<fn() -> Row>,
	}
	impl<F, Row> Parquet<F, Row>
	where
		F: File,
		Row: ParquetData + 'static,
	{
		pub async fn new(
			file: F, row_predicate: Option<Row::Predicate>,
		) -> Result<Self, <Self as Source>::Error> {
			Ok(Self {
				partitions: file.partitions().await.map_err(ParquetError::File)?,
				row_predicate,
				marker: PhantomData,
			})
		}
	}
	impl<F, Row> Source for Parquet<F, Row>
	where
		F: File,
		Row: ParquetData + 'static,
	{
		type Item = Row;
		#[allow(clippy::type_complexity)]
		type Error = ParquetError<
			<F as File>::Error,
			<<F as File>::Partition as Partition>::Error,
			<<<F as File>::Partition as Partition>::Page as Page>::Error,
		>;

		type ParStream =
			impl amadeus_core::par_stream::ParallelStream<Item = Result<Self::Item, Self::Error>>;
		type DistStream = impl DistributedStream<Item = Result<Self::Item, Self::Error>>;

		fn par_stream(self) -> Self::ParStream {
			DistParStream::new(self.dist_stream())
		}
		#[allow(clippy::let_and_return)]
		fn dist_stream(self) -> Self::DistStream {
			let predicate = self.row_predicate;

			self.partitions
				.into_dist_stream()
				.flat_map(FnMut!(move |partition: F::Partition| {
					let predicate = predicate.clone();
					async move {
						Ok(stream::iter(
							partition
								.pages()
								.await
								.map_err(ParquetError::Partition)?
								.into_iter(),
						)
						.flat_map(move |page| {
							let predicate = predicate.clone();
							async move {
								let mut buf = Vec::with_capacity(10 * 1024 * 1024);
								let reader = Page::reader(page);
								pin_mut!(reader);
								let buf = PassError::new(
									reader.read_to_end(&mut buf).await.map(|_| Cursor::new(buf)),
								);

								let b = predicate.clone();

								Ok(stream::iter(
									SerializedFileReader::new(buf)?.get_row_iter::<Row>(None)?,
								))
							}
							.map(ResultExpandIter::new)
							.flatten_stream()
						})
						.map(|row: Result<Result<Row, _>, Self::Error>| Ok(row??)))
					}
					.map(ResultExpandIter::new)
					.flatten_stream()
					.map(|row: Result<Result<Row, Self::Error>, Self::Error>| Ok(row??))
				}))
		}
	}

	// impl<P> ParquetReader for amadeus_core::file::Reader<P>
	// where
	// 	P: Page,
	// {
	// 	fn len(&self) -> u64 {
	// 		self.len()
	// 	}
	// }

	#[derive(Serialize, Deserialize)]
	pub struct ParquetDirectory<D> {
		directory: D,
	}
	impl<D> ParquetDirectory<D> {
		pub fn new(directory: D) -> Self {
			Self { directory }
		}
	}
	#[async_trait(?Send)]
	impl<D> File for ParquetDirectory<D>
	where
		D: Directory,
		D::Partition: Debug,
	{
		type Partition = D::Partition;
		type Error = D::Error;

		async fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
			self.partitions_filter(|_| true).await
		}
	}
	#[async_trait(?Send)]
	impl<D> Directory for ParquetDirectory<D>
	where
		D: Directory,
		D::Partition: Debug,
	{
		async fn partitions_filter<F>(
			self, mut f: F,
		) -> Result<Vec<<Self as File>::Partition>, <Self as File>::Error>
		where
			F: FnMut(&PathBuf) -> bool,
		{
			// "Logic" interpreted from https://github.com/apache/arrow/blob/927cfeff875e557e28649891ea20ca38cb9d1536/python/pyarrow/parquet.py#L705-L829
			// and https://github.com/apache/spark/blob/5a7403623d0525c23ab8ae575e9d1383e3e10635/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/InMemoryFileIndex.scala#L348-L359
			// and https://github.com/apache/spark/blob/5a7403623d0525c23ab8ae575e9d1383e3e10635/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetPartitionDiscoverySuite.scala
			self.directory
				.partitions_filter(|path| {
					let skip;
					if !path.is_file() {
						let dir_name = path.last().unwrap().to_string_lossy();

						skip = dir_name.starts_with('.') // Hidden files
							|| (dir_name.starts_with('_') && !dir_name.contains('=')) // ARROW-1079: Filter out "private" directories starting with underscore;
					} else {
						let file_name = path.file_name().unwrap().to_string_lossy();
						let extension = file_name.rfind('.').map(|offset| &file_name[offset + 1..]);
						skip = file_name.starts_with('.') // Hidden files
								|| file_name == "_metadata" || file_name == "_common_metadata" // Summary metadata
								|| file_name == "_SUCCESS" // Spark success marker
								|| extension == Some("_COPYING_") // File copy in progress; TODO: Should we error on this?
								|| extension == Some("crc") // Checksums
								|| file_name.ends_with("_$folder$"); // This is created by Apache tools on S3
					}
					!skip && f(path)
				})
				.await
		}
	}

	mod misc_serde {
		use super::internal;
		use internal::errors::ParquetError;
		use serde::{Deserialize, Deserializer, Serialize, Serializer};

		pub struct Serde<T>(T);

		impl Serialize for Serde<&ParquetError> {
			fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
			where
				S: Serializer,
			{
				<(usize, &str)>::serialize(
					&match self.0 {
						ParquetError::General(message) => (0, message),
						ParquetError::EOF(message) => (1, message),
						_ => unimplemented!(),
					},
					serializer,
				)
			}
		}
		impl<'de> Deserialize<'de> for Serde<ParquetError> {
			fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
			where
				D: Deserializer<'de>,
			{
				<(usize, String)>::deserialize(deserializer)
					.map(|(kind, message)| match kind {
						1 => ParquetError::EOF(message),
						_ => ParquetError::General(message),
					})
					.map(Self)
			}
		}

		pub fn serialize<T, S>(t: &T, serializer: S) -> Result<S::Ok, S::Error>
		where
			for<'a> Serde<&'a T>: Serialize,
			S: Serializer,
		{
			Serde(t).serialize(serializer)
		}
		pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
		where
			Serde<T>: Deserialize<'de>,
			D: Deserializer<'de>,
		{
			Serde::<T>::deserialize(deserializer).map(|x| x.0)
		}
	}

	#[derive(Serialize, Deserialize, Debug)]
	pub enum ParquetError<A, B, C> {
		File(A),
		Partition(B),
		Page(C),
		Parquet(#[serde(with = "misc_serde")] InternalParquetError),
	}
	impl<A, B, C> PartialEq for ParquetError<A, B, C>
	where
		A: PartialEq,
		B: PartialEq,
		C: PartialEq,
	{
		fn eq(&self, other: &Self) -> bool {
			match (self, other) {
				(Self::File(a), Self::File(b)) => a == b,
				(Self::Partition(a), Self::Partition(b)) => a == b,
				(Self::Page(a), Self::Page(b)) => a == b,
				(Self::Parquet(a), Self::Parquet(b)) => a == b,
				_ => false,
			}
		}
	}
	impl<A, B, C> error::Error for ParquetError<A, B, C>
	where
		A: error::Error,
		B: error::Error,
		C: error::Error,
	{
	}
	impl<A, B, C> Display for ParquetError<A, B, C>
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
				Self::Parquet(err) => Display::fmt(err, f),
			}
		}
	}
	impl<A, B, C> From<InternalParquetError> for ParquetError<A, B, C> {
		fn from(err: InternalParquetError) -> Self {
			Self::Parquet(err)
		}
	}

	use std::io;

	impl ParquetReader for PassError<Cursor<Vec<u8>>> {
		fn len(&self) -> u64 {
			self.0.as_ref().unwrap().get_ref().len() as u64
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
	impl<R> io::Seek for PassError<R>
	where
		R: io::Seek,
	{
		fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
			match &mut self.0 {
				Ok(r) => r.seek(pos),
				Err(r) => Err(r.take().unwrap()),
			}
		}
	}
}
#[cfg(nightly)]
pub use wrap::*;
