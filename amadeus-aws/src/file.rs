use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, HeadObjectRequest, S3Client, S3};
use serde::{Deserialize, Serialize};
use std::{
	convert::{TryFrom, TryInto}, future::Future, io, pin::Pin
};
use tokio_io::AsyncRead;

use amadeus_core::util::{IoError, ResultExpand};

#[derive(Serialize, Deserialize)]
pub struct S3Directory {
	region: Region,
	bucket: String,
	prefix: String,
}
impl S3Directory {
	pub fn new(region: Region, bucket: &str, prefix: &str) -> Self {
		let (bucket, prefix) = (bucket.to_owned(), prefix.to_owned());
		Self {
			region,
			bucket,
			prefix,
		}
	}
}
impl amadeus_core::file::File for S3Directory {
	type Partition = S3Partition;
	type Error = super::Error;

	fn partitions(self) -> Result<Vec<Self::Partition>, Self::Error> {
		let S3Directory {
			region,
			bucket,
			prefix,
		} = self;
		let client = S3Client::new(region.clone());
		let objects = super::list(&client, &bucket, &prefix);
		ResultExpand(objects)
			.into_iter()
			.map(|object| {
				object
					.map(|object| S3Partition {
						region: region.clone(),
						bucket: bucket.clone(),
						key: object.key.unwrap(),
						len: object.size.unwrap().try_into().unwrap(),
					})
					.map_err(From::from)
			})
			.collect()
	}
}

#[derive(Serialize, Deserialize)]
pub struct S3Partition {
	region: Region,
	bucket: String,
	key: String,
	len: u64,
}
impl amadeus_core::file::Partition for S3Partition {
	type Page = S3File;
	type Error = IoError;

	fn pages(self) -> Result<Vec<Self::Page>, Self::Error> {
		let client = S3Client::new(self.region);
		let (bucket, key, len) = (self.bucket, self.key, self.len);
		Ok(vec![S3File {
			client,
			bucket,
			key,
			len,
		}])
	}
}

pub struct S3File {
	client: S3Client,
	bucket: String,
	key: String,
	len: u64,
}
impl S3File {
	pub fn new(region: Region, bucket: &str, key: &str) -> impl Future<Output = Self> {
		let client = S3Client::new(region);
		let (bucket, key) = (bucket.to_owned(), key.to_owned());
		async move {
			let object =
				futures::compat::Compat01As03::new(client.head_object(HeadObjectRequest {
					bucket: bucket.clone(),
					key: key.clone(),
					..HeadObjectRequest::default()
				}))
				.await
				.unwrap();
			let len = object.content_length.unwrap().try_into().unwrap();
			S3File {
				client,
				bucket,
				key,
				len,
			}
		}
	}
}
impl amadeus_core::file::Page for S3File {
	type Error = IoError;

	fn len(&self) -> u64 {
		self.len
	}
	fn set_len(&self, _len: u64) -> Result<(), Self::Error> {
		unimplemented!()
	}
	fn read<'a>(
		&'a self, offset: u64, buf: &'a mut [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
		Box::pin(async move {
			let (start, end) = (offset, offset + u64::try_from(buf.len()).unwrap());
			let res =
				futures::compat::Compat01As03::new(self.client.get_object(GetObjectRequest {
					bucket: self.bucket.clone(),
					key: self.key.clone(),
					range: Some(format!("bytes={}-{}", start, end)),
					..GetObjectRequest::default()
				}))
				.await
				.unwrap();
			let len: u64 = buf.len().try_into().unwrap();
			let mut cursor = io::Cursor::new(buf);
			let mut read = res.body.unwrap().into_async_read();
			while len - cursor.position() > 0 {
				let _: usize =
					futures::compat::Compat01As03::new(tokio::prelude::future::poll_fn(|| {
						read.read_buf(&mut cursor)
					}))
					.await
					.unwrap();
			}
			Ok(())
		})
	}
	fn write<'a>(
		&'a self, _offset: u64, _buf: &'a [u8],
	) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + 'a>> {
		unimplemented!()
	}
}
