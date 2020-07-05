use either::Either;
use futures::Stream;
use std::{cmp::Ordering, hash::Hash, iter, ops::FnMut};

use crate::{pipe::Pipe, pool::ProcessSend};

use super::{par_sink::*, par_stream::*};

#[must_use]
pub trait PipeTask<Source> {
	type Item;
	type Async: Pipe<Source, Item = Self::Item>;

	fn into_async(self) -> Self::Async;
}

impl_par_dist_rename! {
	#[must_use]
	pub trait ParallelPipe<Source> {
		type Item;
		type Task: PipeTask<Source, Item = Self::Item> + Send;

		fn task(&self) -> Self::Task;

		fn inspect<F>(self, f: F) -> Inspect<Self, F>
		where
			F: FnMut(&Self::Item) + Clone + Send + 'static,
			Self: Sized,
		{
			assert_parallel_pipe(Inspect::new(self, f))
		}

		fn update<F>(self, f: F) -> Update<Self, F>
		where
			F: FnMut(&mut Self::Item) + Clone + Send + 'static,
			Self: Sized,
		{
			assert_parallel_pipe(Update::new(self, f))
		}

		fn map<B, F>(self, f: F) -> Map<Self, F>
		where
			F: FnMut(Self::Item) -> B + Clone + Send + 'static,
			Self: Sized,
		{
			assert_parallel_pipe(Map::new(self, f))
		}

		fn flat_map<B, F>(self, f: F) -> FlatMap<Self, F>
		where
			F: FnMut(Self::Item) -> B + Clone + Send + 'static,
			B: Stream,
			Self: Sized,
		{
			assert_parallel_pipe(FlatMap::new(self, f))
		}

		fn filter<F>(self, f: F) -> Filter<Self, F>
		where
			F: FnMut(&Self::Item) -> bool + Clone + Send + 'static,
			Self: Sized,
		{
			assert_parallel_pipe(Filter::new(self, f))
		}

		fn cloned<'a, T>(self) -> Cloned<Self, T, Source>
		where
			T: Clone + 'a,
			Source: 'a,
			Self: ParallelPipe<&'a Source, Item = &'a T> + Sized,
		{
			assert_parallel_pipe(Cloned::new(self))
		}

		// #[must_use]
		// fn chain<C>(self, chain: C) -> Chain<Self, C::Iter>
		// where
		// 	C: IntoParallelStream<Item = Self::Item>,
		// 	Self: Sized,
		// {
		// 	assert_parallel_pipe(Chain::new(self, chain.into_par_stream()))
		// }

		fn pipe<S>(self, sink: S) -> super::par_sink::Pipe<Self, S>
		where
			S: ParallelSink<Self::Item>,
			Self: Sized,
		{
			assert_parallel_sink(super::par_sink::Pipe::new(self, sink))
		}

		fn fork<A, B, RefAItem>(self, sink: A, sink_ref: B) -> super::par_sink::Fork<Self, A, B, &'static Self::Item>
		where
			A: ParallelSink<Self::Item>,
			B: for<'a> ParallelSink<&'a Self::Item>,
			Self: Sized,
		{
			assert_parallel_sink(super::par_sink::Fork::new(self, sink, sink_ref))
		}

		fn for_each<F>(self, f: F) -> ForEach<Self, F>
		where
			F: FnMut(Self::Item) + Clone + Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(ForEach::new(self, f))
		}

		fn fold<ID, F, B>(self, identity: ID, op: F) -> Fold<Self, ID, F, B>
		where
			ID: FnMut() -> B + Clone + Send + 'static,
			F: FnMut(B, Either<Self::Item, B>) -> B + Clone + Send + 'static,
			B: Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(Fold::new(self, identity, op))
		}

		fn group_by<S, A, B>(self, sink: S) -> GroupBy<Self, S>
		where
			A: Eq + Hash + Send + 'static,
			S: ParallelSink<B>,
			<S::Pipe as ParallelPipe<B>>::Task: Clone + Send + 'static,
			S::ReduceA: 'static,
			S::ReduceC: Clone,
			S::Output: Send + 'static,
			Self: ParallelPipe<Source, Item = (A, B)> + Sized,
		{
			assert_parallel_sink(GroupBy::new(self, sink))
		}

		fn histogram(self) -> Histogram<Self>
		where
			Self::Item: Hash + Ord + Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(Histogram::new(self))
		}

		fn count(self) -> Count<Self>
		where
			Self: Sized,
		{
			assert_parallel_sink(Count::new(self))
		}

		fn sum<B>(self) -> Sum<Self, B>
		where
			B: iter::Sum<Self::Item> + iter::Sum<B> + Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(Sum::new(self))
		}

		fn combine<F>(self, f: F) -> Combine<Self, F>
		where
			F: FnMut(Self::Item, Self::Item) -> Self::Item + Clone + Send + 'static,
			Self::Item: Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(Combine::new(self, f))
		}

		fn max(self) -> Max<Self>
		where
			Self::Item: Ord + Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(Max::new(self))
		}

		fn max_by<F>(self, f: F) -> MaxBy<Self, F>
		where
			F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + Send + 'static,
			Self::Item: Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(MaxBy::new(self, f))
		}

		fn max_by_key<F, B>(self, f: F) -> MaxByKey<Self, F>
		where
			F: FnMut(&Self::Item) -> B + Clone + Send + 'static,
			B: Ord + 'static,
			Self::Item: Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(MaxByKey::new(self, f))
		}

		fn min(self) -> Min<Self>
		where
			Self::Item: Ord + Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(Min::new(self))
		}

		fn min_by<F>(self, f: F) -> MinBy<Self, F>
		where
			F: FnMut(&Self::Item, &Self::Item) -> Ordering + Clone + Send + 'static,
			Self::Item: Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(MinBy::new(self, f))
		}

		fn min_by_key<F, B>(self, f: F) -> MinByKey<Self, F>
		where
			F: FnMut(&Self::Item) -> B + Clone + Send + 'static,
			B: Ord + 'static,
			Self::Item: Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(MinByKey::new(self, f))
		}

		fn most_frequent(self, n: usize, probability: f64, tolerance: f64) -> MostFrequent<Self>
		where
			Self::Item: Hash + Eq + Clone + Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(MostFrequent::new(self, n, probability, tolerance))
		}

		fn most_distinct<A, B>(
			self, n: usize, probability: f64, tolerance: f64, error_rate: f64,
		) -> MostDistinct<Self>
		where
			Self: ParallelPipe<Source, Item = (A, B)> + Sized,
			A: Hash + Eq + Clone + Send + 'static,
			B: Hash + 'static,
		{
			assert_parallel_sink(MostDistinct::new(
				self,
				n,
				probability,
				tolerance,
				error_rate,
			))
		}

		fn sample_unstable(self, samples: usize) -> SampleUnstable<Self>
		where
			Self::Item: Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(SampleUnstable::new(self, samples))
		}

		fn all<F>(self, f: F) -> All<Self, F>
		where
			F: FnMut(Self::Item) -> bool + Clone + Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(All::new(self, f))
		}

		fn any<F>(self, f: F) -> Any<Self, F>
		where
			F: FnMut(Self::Item) -> bool + Clone + Send + 'static,
			Self: Sized,
		{
			assert_parallel_sink(Any::new(self, f))
		}

		fn collect<B>(self) -> Collect<Self, B>
		where
			B: FromParallelStream<Self::Item>,
			Self: Sized,
		{
			assert_parallel_sink(Collect::new(self))
		}
	}
	#[inline(always)]
	pub(crate) fn assert_parallel_pipe<T, I: ParallelPipe<Source, Item = T>, Source>(i: I) -> I {
		i
	}
}
