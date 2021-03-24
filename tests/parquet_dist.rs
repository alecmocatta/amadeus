#![type_length_limit = "2097152"]
#![allow(clippy::cognitive_complexity, clippy::type_complexity)]

#[cfg(feature = "constellation")]
use constellation::*;
use std::{
	collections::HashMap, path::PathBuf, time::{Duration, SystemTime}
};

use amadeus::dist::prelude::*;

fn main() {
	return;
	if cfg!(miri) {
		return;
	}
	#[cfg(feature = "constellation")]
	init(Resources::default());

	tokio::runtime::Builder::new()
		.threaded_scheduler()
		.enable_all()
		.build()
		.unwrap()
		.block_on(async {
			let thread_pool_time = {
				let thread_pool = ThreadPool::new(None).unwrap();
				run(&thread_pool).await
			};
			#[cfg(feature = "constellation")]
			let process_pool_time = {
				let process_pool = ProcessPool::new(None, None, Resources::default()).unwrap();
				run(&process_pool).await
			};
			#[cfg(not(feature = "constellation"))]
			let process_pool_time = "-";

			println!("in {:?} {:?}", thread_pool_time, process_pool_time);
		})
}

async fn run<P: amadeus_core::pool::ProcessPool>(pool: &P) -> Duration {
	let start = SystemTime::now();

	let rows = Parquet::<_, Value>::new(
		ParquetDirectory::new(PathBuf::from("amadeus-testing/parquet/cf-accesslogs/")),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		207_535
	);

	#[cfg(feature = "aws")]
	{
		let rows = Parquet::<_, Value>::new(vec![S3File::new_with(AwsRegion::UsEast1, "us-east-1.data-analytics", "cflogworkshop/optimized/cf-accesslogs/year=2018/month=11/day=03/part-00137-17868f39-cd99-4b60-bb48-8daf9072122e.c000.snappy.parquet", AwsCredentials::Anonymous);20], None).await.unwrap();
		assert_eq!(
			rows.dist_stream()
				.map(FnMut!(|row: Result<_, _>| row.unwrap()))
				.count(pool)
				.await,
			45_167 * 20
		);

		let rows = Parquet::<_, Value>::new(
			ParquetDirectory::new(S3Directory::new_with(
				AwsRegion::UsEast1,
				"us-east-1.data-analytics",
				"cflogworkshop/optimized/cf-accesslogs/",
				AwsCredentials::Anonymous,
			)),
			None,
		)
		.await
		.unwrap();
		assert_eq!(
			rows.dist_stream()
				.map(FnMut!(|row: Result<_, _>| row.unwrap()))
				.count(pool)
				.await,
			207_535
		);

		let rows = Parquet::<_, Value>::new(vec![
			S3File::new_with(AwsRegion::UsEast1, "us-east-1.data-analytics", "cflogworkshop/optimized/cf-accesslogs/year=2018/month=11/day=02/part-00176-17868f39-cd99-4b60-bb48-8daf9072122e.c000.snappy.parquet", AwsCredentials::Anonymous),
			S3File::new_with(AwsRegion::UsEast1, "us-east-1.data-analytics", "cflogworkshop/optimized/cf-accesslogs/year=2018/month=11/day=02/part-00176-ed461019-4a12-46fa-a3f3-246d58f0ee06.c000.snappy.parquet", AwsCredentials::Anonymous),
			S3File::new_with(AwsRegion::UsEast1, "us-east-1.data-analytics", "cflogworkshop/optimized/cf-accesslogs/year=2018/month=11/day=03/part-00137-17868f39-cd99-4b60-bb48-8daf9072122e.c000.snappy.parquet", AwsCredentials::Anonymous),
			S3File::new_with(AwsRegion::UsEast1, "us-east-1.data-analytics", "cflogworkshop/optimized/cf-accesslogs/year=2018/month=11/day=04/part-00173-17868f39-cd99-4b60-bb48-8daf9072122e.c000.snappy.parquet", AwsCredentials::Anonymous),
			S3File::new_with(AwsRegion::UsEast1, "us-east-1.data-analytics", "cflogworkshop/optimized/cf-accesslogs/year=2018/month=11/day=05/part-00025-17868f39-cd99-4b60-bb48-8daf9072122e.c000.snappy.parquet", AwsCredentials::Anonymous),
			S3File::new_with(AwsRegion::UsEast1, "us-east-1.data-analytics", "cflogworkshop/optimized/cf-accesslogs/year=2018/month=11/day=05/part-00025-96c249f4-3a10-4509-b6b8-693a5d90dbf3.c000.snappy.parquet", AwsCredentials::Anonymous),
			S3File::new_with(AwsRegion::UsEast1, "us-east-1.data-analytics", "cflogworkshop/optimized/cf-accesslogs/year=2018/month=11/day=06/part-00185-96c249f4-3a10-4509-b6b8-693a5d90dbf3.c000.snappy.parquet", AwsCredentials::Anonymous),
			S3File::new_with(AwsRegion::UsEast1, "us-east-1.data-analytics", "cflogworkshop/optimized/cf-accesslogs/year=2018/month=11/day=07/part-00151-96c249f4-3a10-4509-b6b8-693a5d90dbf3.c000.snappy.parquet", AwsCredentials::Anonymous),
		], None).await.unwrap();
		assert_eq!(
			rows.dist_stream()
				.map(FnMut!(|row: Result<_, _>| row.unwrap()))
				.count(pool)
				.await,
			207_535
		);

		let rows = Parquet::<_, Value>::new(
			ParquetDirectory::new(S3Directory::new_with(
				AwsRegion::UsEast1,
				"us-east-1.data-analytics",
				"cflogworkshop/optimized/cf-accesslogs/",
				AwsCredentials::Anonymous,
			)),
			None,
		)
		.await
		.unwrap();
		assert_eq!(
			rows.dist_stream()
				.map(FnMut!(|row: Result<_, _>| row.unwrap()))
				.count(pool)
				.await,
			207_535
		);
	}

	#[derive(Data, Clone, PartialEq, Debug)]
	struct StockSimulatedDerived {
		bp1: Option<f64>,
		bp2: Option<f64>,
		bp3: Option<f64>,
		bp4: Option<f64>,
		bp5: Option<f64>,
		bs1: Option<f64>,
		bs2: Option<f64>,
		bs3: Option<f64>,
		bs4: Option<f64>,
		bs5: Option<f64>,
		ap1: Option<f64>,
		ap2: Option<f64>,
		ap3: Option<f64>,
		ap4: Option<f64>,
		ap5: Option<f64>,
		as1: Option<f64>,
		as2: Option<f64>,
		as3: Option<f64>,
		as4: Option<f64>,
		as5: Option<f64>,
		valid: Option<f64>,
		__index_level_0__: Option<i64>,
	}
	let rows = Parquet::<_, StockSimulatedDerived>::new(
		PathBuf::from("amadeus-testing/parquet/stock_simulated.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		42_000
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/stock_simulated.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: StockSimulatedDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		42_000
	);

	#[derive(Data, Clone, PartialEq, Debug)]
	struct StockSimulatedDerivedProjection1 {
		bs5: Option<f64>,
		__index_level_0__: Option<i64>,
	}

	let rows = Parquet::<_, StockSimulatedDerivedProjection1>::new(
		PathBuf::from("amadeus-testing/parquet/stock_simulated.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		42_000
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/stock_simulated.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: StockSimulatedDerivedProjection1 = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		42_000
	);

	#[derive(Data, Clone, PartialEq, Debug)]
	struct StockSimulatedDerivedProjection2 {}

	let rows = Parquet::<_, StockSimulatedDerivedProjection2>::new(
		PathBuf::from("amadeus-testing/parquet/stock_simulated.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		42_000
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/stock_simulated.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: StockSimulatedDerivedProjection2 = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		42_000
	);

	type TenKayVeeTwo = (
		List<u8>,
		i32,
		i64,
		bool,
		f32,
		f64,
		List<u8>, // [u8;1024],
		DateTime,
	);

	#[derive(Data, Clone, PartialEq, Debug)]
	struct TenKayVeeTwoDerived {
		binary_field: List<u8>,
		int32_field: i32,
		int64_field: i64,
		boolean_field: bool,
		float_field: f32,
		double_field: f64,
		flba_field: List<u8>, // [u8;1024],
		int96_field: DateTime,
	}

	let rows = Parquet::<_, TenKayVeeTwo>::new(
		PathBuf::from("amadeus-testing/parquet/10k-v2.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		10_000
	);

	let rows = Parquet::<_, TenKayVeeTwoDerived>::new(
		PathBuf::from("amadeus-testing/parquet/10k-v2.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		10_000
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/10k-v2.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: TenKayVeeTwo = value.clone().downcast().unwrap();
				let _: TenKayVeeTwoDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		10_000
	);

	type AlltypesDictionary = (
		Option<i32>,
		Option<bool>,
		Option<i32>,
		Option<i32>,
		Option<i32>,
		Option<i64>,
		Option<f32>,
		Option<f64>,
		Option<List<u8>>,
		Option<List<u8>>,
		Option<DateTime>,
	);

	#[derive(Data, Clone, PartialEq, Debug)]
	struct AlltypesDictionaryDerived {
		id: Option<i32>,
		bool_col: Option<bool>,
		tinyint_col: Option<i32>,
		smallint_col: Option<i32>,
		int_col: Option<i32>,
		bigint_col: Option<i64>,
		float_col: Option<f32>,
		double_col: Option<f64>,
		date_string_col: Option<List<u8>>,
		string_col: Option<List<u8>>,
		timestamp_col: Option<DateTime>,
	}

	let rows = Parquet::<_, AlltypesDictionary>::new(
		PathBuf::from("amadeus-testing/parquet/alltypes_dictionary.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		2
	);

	let rows = Parquet::<_, AlltypesDictionaryDerived>::new(
		PathBuf::from("amadeus-testing/parquet/alltypes_dictionary.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		2
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/alltypes_dictionary.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: AlltypesDictionary = value.clone().downcast().unwrap();
				let _: AlltypesDictionaryDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		2
	);

	type AlltypesPlain = (
		Option<i32>,
		Option<bool>,
		Option<i32>,
		Option<i32>,
		Option<i32>,
		Option<i64>,
		Option<f32>,
		Option<f64>,
		Option<List<u8>>,
		Option<List<u8>>,
		Option<DateTime>,
	);

	#[derive(Data, Clone, PartialEq, Debug)]
	struct AlltypesPlainDerived {
		id: Option<i32>,
		bool_col: Option<bool>,
		tinyint_col: Option<i32>,
		smallint_col: Option<i32>,
		int_col: Option<i32>,
		bigint_col: Option<i64>,
		float_col: Option<f32>,
		double_col: Option<f64>,
		date_string_col: Option<List<u8>>,
		string_col: Option<List<u8>>,
		timestamp_col: Option<DateTime>,
	}

	let rows = Parquet::<_, AlltypesPlain>::new(
		PathBuf::from("amadeus-testing/parquet/alltypes_plain.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		8
	);

	let rows = Parquet::<_, AlltypesPlainDerived>::new(
		PathBuf::from("amadeus-testing/parquet/alltypes_plain.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		8
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/alltypes_plain.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: AlltypesPlain = value.clone().downcast().unwrap();
				let _: AlltypesPlainDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		8
	);

	type AlltypesPlainSnappy = (
		Option<i32>,
		Option<bool>,
		Option<i32>,
		Option<i32>,
		Option<i32>,
		Option<i64>,
		Option<f32>,
		Option<f64>,
		Option<List<u8>>,
		Option<List<u8>>,
		Option<DateTime>,
	);

	#[derive(Data, Clone, PartialEq, Debug)]
	struct AlltypesPlainSnappyDerived {
		id: Option<i32>,
		bool_col: Option<bool>,
		tinyint_col: Option<i32>,
		smallint_col: Option<i32>,
		int_col: Option<i32>,
		bigint_col: Option<i64>,
		float_col: Option<f32>,
		double_col: Option<f64>,
		date_string_col: Option<List<u8>>,
		string_col: Option<List<u8>>,
		timestamp_col: Option<DateTime>,
	}

	let rows = Parquet::<_, AlltypesPlainSnappy>::new(
		PathBuf::from("amadeus-testing/parquet/alltypes_plain.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		2
	);

	let rows = Parquet::<_, AlltypesPlainSnappyDerived>::new(
		PathBuf::from("amadeus-testing/parquet/alltypes_plain.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		2
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/alltypes_plain.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: AlltypesPlainSnappy = value.clone().downcast().unwrap();
				let _: AlltypesPlainSnappyDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		2
	);

	// TODO

	// type NationDictMalformed = (Option<i32>, Option<List<u8>>, Option<i32>, Option<List<u8>>);

	// let rows = Parquet::<_,NationDictMalformed>::new(vec![PathBuf::from(
	// 	"amadeus-testing/parquet/nation.dict-malformed.parquet",
	// )]).await.unwrap();
	// assert_eq!(
	// 	rows.dist_stream().collect::<Vec<_>>(pool),
	// 	[Err(amadeus::source::parquet::Error::Parquet(
	// 		amadeus_parquet::internal::errors::ParquetError::General(String::from(
	// 			"underlying IO error: failed to fill whole buffer"
	// 		))
	// 	))]
	// );

	// let rows = Parquet::<_,Value>::new(vec![PathBuf::from(
	// 	"amadeus-testing/parquet/nation.dict-malformed.parquet",
	// )]).await.unwrap();
	// assert_eq!(
	// 	rows.dist_stream().collect::<Vec<_>>(pool),
	// 	[Err(amadeus::source::parquet::Error::Parquet(
	// 		amadeus_parquet::internal::errors::ParquetError::General(String::from(
	// 			"underlying IO error: failed to fill whole buffer"
	// 		))
	// 	))]
	// );

	type NestedLists = (
		Option<List<Option<List<Option<List<Option<String>>>>>>>,
		i32,
	);
	#[derive(Data, Clone, PartialEq, Debug)]
	struct NestedListsDerived {
		a: Option<List<Option<List<Option<List<Option<String>>>>>>>,
		b: i32,
	}
	let rows = Parquet::<_, NestedLists>::new(
		PathBuf::from("amadeus-testing/parquet/nested_lists.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		3
	);

	let rows = Parquet::<_, NestedListsDerived>::new(
		PathBuf::from("amadeus-testing/parquet/nested_lists.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		3
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/nested_lists.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: NestedLists = value.clone().downcast().unwrap();
				let _: NestedListsDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		3
	);

	type NestedMaps = (
		Option<HashMap<String, Option<HashMap<i32, bool>>>>,
		i32,
		f64,
	);
	#[derive(Data, Clone, PartialEq, Debug)]
	struct NestedMapsDerived {
		a: Option<HashMap<String, Option<HashMap<i32, bool>>>>,
		b: i32,
		c: f64,
	}
	let rows = Parquet::<_, NestedMaps>::new(
		PathBuf::from("amadeus-testing/parquet/nested_maps.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		6
	);

	let rows = Parquet::<_, NestedMapsDerived>::new(
		PathBuf::from("amadeus-testing/parquet/nested_maps.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		6
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/nested_maps.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: NestedMaps = value.clone().downcast().unwrap();
				let _: NestedMapsDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		6
	);

	type Nonnullable = (
		i64,
		List<i32>,
		List<List<i32>>,
		HashMap<String, i32>,
		List<HashMap<String, i32>>,
		(
			i32,
			List<i32>,
			(List<List<(i32, String)>>,),
			HashMap<String, ((List<f64>,),)>,
		),
	);

	#[derive(Data, Clone, PartialEq, Debug)]
	struct NonnullableDerived {
		#[amadeus(name = "ID")]
		id: i64,
		#[amadeus(name = "Int_Array")]
		int_array: List<i32>,
		int_array_array: List<List<i32>>,
		#[amadeus(name = "Int_Map")]
		int_map: HashMap<String, i32>,
		int_map_array: List<HashMap<String, i32>>,
		#[amadeus(name = "nested_Struct")]
		nested_struct: NonnullableDerivedInner,
	}

	#[derive(Data, Clone, PartialEq, Debug)]
	struct NonnullableDerivedInner {
		a: i32,
		#[amadeus(name = "B")]
		b: List<i32>,
		c: NonnullableDerivedInnerInner,
		#[amadeus(name = "G")]
		g: HashMap<String, ((List<f64>,),)>,
	}

	#[derive(Data, Clone, PartialEq, Debug)]
	struct NonnullableDerivedInnerInner {
		#[amadeus(name = "D")]
		d: List<List<NonnullableDerivedInnerInnerInner>>,
	}

	#[derive(Data, Clone, PartialEq, Debug)]
	struct NonnullableDerivedInnerInnerInner {
		e: i32,
		f: String,
	}

	let rows = Parquet::<_, Nonnullable>::new(
		PathBuf::from("amadeus-testing/parquet/nonnullable.impala.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		1
	);

	let rows = Parquet::<_, NonnullableDerived>::new(
		PathBuf::from("amadeus-testing/parquet/nonnullable.impala.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		1
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/nonnullable.impala.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: Nonnullable = value.clone().downcast().unwrap();
				let _: NonnullableDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		1
	);

	type Nullable = (
		Option<i64>,
		Option<List<Option<i32>>>,
		Option<List<Option<List<Option<i32>>>>>,
		Option<HashMap<String, Option<i32>>>,
		Option<List<Option<HashMap<String, Option<i32>>>>>,
		Option<(
			Option<i32>,
			Option<List<Option<i32>>>,
			Option<(Option<List<Option<List<Option<(Option<i32>, Option<String>)>>>>>,)>,
			Option<HashMap<String, Option<(Option<(Option<List<Option<f64>>>,)>,)>>>,
		)>,
	);
	#[derive(Data, Clone, PartialEq, Debug)]
	struct NullableDerived {
		id: Option<i64>,
		int_array: Option<List<Option<i32>>>,
		#[amadeus(name = "int_array_Array")]
		int_array_array: Option<List<Option<List<Option<i32>>>>>,
		int_map: Option<HashMap<String, Option<i32>>>,
		#[amadeus(name = "int_Map_Array")]
		int_map_array: Option<List<Option<HashMap<String, Option<i32>>>>>,
		nested_struct: Option<(
			Option<i32>,
			Option<List<Option<i32>>>,
			Option<(Option<List<Option<List<Option<(Option<i32>, Option<String>)>>>>>,)>,
			Option<HashMap<String, Option<(Option<(Option<List<Option<f64>>>,)>,)>>>,
		)>,
	}
	let rows = Parquet::<_, Nullable>::new(
		PathBuf::from("amadeus-testing/parquet/nullable.impala.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		7
	);

	let rows = Parquet::<_, NullableDerived>::new(
		PathBuf::from("amadeus-testing/parquet/nullable.impala.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		7
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/nullable.impala.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: Nullable = value.clone().downcast().unwrap();
				let _: NullableDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		7
	);

	type Nulls = (Option<(Option<i32>,)>,);
	#[derive(Data, Clone, PartialEq, Debug)]
	struct NullsDerived {
		b_struct: Option<(Option<i32>,)>,
	}
	let rows = Parquet::<_, Nulls>::new(
		PathBuf::from("amadeus-testing/parquet/nulls.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		8
	);

	let rows = Parquet::<_, NullsDerived>::new(
		PathBuf::from("amadeus-testing/parquet/nulls.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		8
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/nulls.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: Nulls = value.clone().downcast().unwrap();
				let _: NullsDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		8
	);

	type Repeated = (i32, Option<(List<(i64, Option<String>)>,)>);
	#[derive(Data, Clone, PartialEq, Debug)]
	struct RepeatedDerived {
		id: i32,
		#[amadeus(name = "phoneNumbers")]
		phone_numbers: Option<(List<(i64, Option<String>)>,)>,
	}
	let rows = Parquet::<_, Repeated>::new(
		PathBuf::from("amadeus-testing/parquet/repeated_no_annotation.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		6
	);

	let rows = Parquet::<_, RepeatedDerived>::new(
		PathBuf::from("amadeus-testing/parquet/repeated_no_annotation.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		6
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/repeated_no_annotation.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: Repeated = value.clone().downcast().unwrap();
				let _: RepeatedDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		6
	);

	type TestDatapage = (Option<String>, i32, f64, bool, Option<List<i32>>);
	#[derive(Data, Clone, PartialEq, Debug)]
	struct TestDatapageDerived {
		a: Option<String>,
		b: i32,
		c: f64,
		d: bool,
		e: Option<List<i32>>,
	}
	let rows = Parquet::<_, TestDatapage>::new(
		PathBuf::from("amadeus-testing/parquet/datapage_v2.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		5
	);

	let rows = Parquet::<_, TestDatapageDerived>::new(
		PathBuf::from("amadeus-testing/parquet/datapage_v2.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		5
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/datapage_v2.snappy.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: TestDatapage = value.clone().downcast().unwrap();
				let _: TestDatapageDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		5
	);

	#[derive(Data, Clone, PartialEq, Debug)]
	struct CommitsDerived {
		id: Option<String>,
		delay: Option<i32>,
		age: Option<i32>,
		ismerge: Option<bool>,
		squashof: Option<i32>,
		author_name: Option<String>,
		author_email: Option<String>,
		committer_name: Option<String>,
		committer_email: Option<String>,
		author_time: Option<DateTime>,
		committer_time: Option<DateTime>,
		loc_d: Option<i64>,
		loc_i: Option<i64>,
		comp_d: Option<i64>,
		comp_i: Option<i64>,
		nfiles: Option<u16>,
		message: Option<String>,
		ndiffs: Option<u16>,
		author_email_dedup: Option<String>,
		author_name_dedup: Option<String>,
		committer_email_dedup: Option<String>,
		committer_name_dedup: Option<String>,
		__index_level_0__: Option<i64>,
	}

	let rows = Parquet::<_, CommitsDerived>::new(
		PathBuf::from("amadeus-testing/parquet/commits.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<_, _>| row.unwrap()))
			.count(pool)
			.await,
		14_444
	);

	let rows = Parquet::<_, Value>::new(
		PathBuf::from("amadeus-testing/parquet/commits.parquet"),
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		rows.dist_stream()
			.map(FnMut!(|row: Result<Value, _>| -> Value {
				let value = row.unwrap();
				let _: CommitsDerived = value.clone().downcast().unwrap();
				value
			}))
			.count(pool)
			.await,
		14_444
	);

	start.elapsed().unwrap()
}
