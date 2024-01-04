// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Range;
use std::sync::Arc;

use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{InMemObjectStore, ObjectStore, ObjectStoreImpl};
use risingwave_pb::hummock::{compact_task, SstableInfo};
use risingwave_storage::hummock::compactor::compactor_runner::compact_and_build_sst;
use risingwave_storage::hummock::compactor::{
    ConcatSstableIterator, DummyCompactionFilter, TaskConfig, TaskProgress,
};
use risingwave_storage::hummock::iterator::{
    ConcatIterator, Forward, ForwardMergeRangeIterator, HummockIterator,
    UnorderedMergeIteratorInner,
};
use risingwave_storage::hummock::multi_builder::{
    CapacitySplitTableBuilder, LocalTableBuilderFactory,
};
use risingwave_storage::hummock::sstable::SstableIteratorReadOptions;
use risingwave_storage::hummock::sstable_store::SstableStoreRef;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    CachePolicy, CompactionDeleteRangeIterator, FileCache, SstableBuilder, SstableBuilderOptions,
    SstableIterator, SstableStore, SstableWriterOptions, Xor16FilterBuilder,
};
use risingwave_storage::monitor::{CompactorMetrics, StoreLocalStatistic};

pub fn mock_sstable_store() -> SstableStoreRef {
    let store = InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused()));
    let store = Arc::new(ObjectStoreImpl::InMem(store));
    let path = "test".to_string();
    Arc::new(SstableStore::new(
        store,
        path,
        64 << 20,
        128 << 20,
        0,
        64 << 20,
        FileCache::none(),
        FileCache::none(),
        None,
    ))
}

pub fn default_writer_opts() -> SstableWriterOptions {
    SstableWriterOptions {
        capacity_hint: None,
        tracker: None,
        policy: CachePolicy::Fill(CachePriority::High),
    }
}

pub fn test_key_of(idx: usize, epoch: u64) -> FullKey<Vec<u8>> {
    FullKey::for_test(
        TableId::default(),
        [
            VirtualNode::ZERO.to_be_bytes().as_slice(),
            format!("key_test_{:08}", idx * 2).as_bytes(),
        ]
        .concat(),
        epoch,
    )
}

const MAX_KEY_COUNT: usize = 128 * 1024;

async fn build_table(
    sstable_store: SstableStoreRef,
    sstable_object_id: u64,
    range: Range<u64>,
    epoch: u64,
) -> SstableInfo {
    let opt = SstableBuilderOptions {
        capacity: 32 * 1024 * 1024,
        block_capacity: 16 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.001,
        ..Default::default()
    };
    let writer = sstable_store.create_sst_writer(
        sstable_object_id,
        SstableWriterOptions {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::Fill(CachePriority::High),
        },
    );
    let mut builder =
        SstableBuilder::<_, Xor16FilterBuilder>::for_test(sstable_object_id, writer, opt);
    let value = b"1234567890123456789";
    let mut full_key = test_key_of(0, epoch);
    let table_key_len = full_key.user_key.table_key.len();
    for i in range {
        let start = (i % 8) as usize;
        let end = start + 8;
        full_key.user_key.table_key[table_key_len - 8..].copy_from_slice(&i.to_be_bytes());
        builder
            .add_for_test(full_key.to_ref(), HummockValue::put(&value[start..end]))
            .await
            .unwrap();
    }
    let output = builder.finish().await.unwrap();
    let handle = output.writer_output;
    let sst = output.sst_info.sst_info;
    handle.await.unwrap().unwrap();
    sst
}

async fn scan_all_table(info: &SstableInfo, sstable_store: SstableStoreRef) {
    let mut stats = StoreLocalStatistic::default();
    let table = sstable_store.sstable(info, &mut stats).await.unwrap();
    let default_read_options = Arc::new(SstableIteratorReadOptions::default());
    // warm up to make them all in memory. I do not use CachePolicy::Fill because it will fetch
    // block from meta.
    let mut iter = SstableIterator::new(table, sstable_store.clone(), default_read_options);
    iter.rewind().await.unwrap();
    while iter.is_valid() {
        iter.next().await.unwrap();
    }
}

fn bench_table_build(c: &mut Criterion) {
    c.bench_function("bench_table_build", |b| {
        let sstable_store = mock_sstable_store();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        b.to_async(&runtime).iter(|| async {
            build_table(sstable_store.clone(), 0, 0..(MAX_KEY_COUNT as u64), 1).await;
        });
    });
}

fn bench_table_scan(c: &mut Criterion) {
    let sstable_store = mock_sstable_store();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let info = runtime.block_on(async {
        build_table(sstable_store.clone(), 0, 0..(MAX_KEY_COUNT as u64), 1).await
    });
    // warm up to make them all in memory. I do not use CachePolicy::Fill because it will fetch
    // block from meta.
    let sstable_store1 = sstable_store.clone();
    let info1 = info.clone();
    runtime.block_on(async move {
        scan_all_table(&info1, sstable_store1).await;
    });

    c.bench_function("bench_table_iterator", |b| {
        let info1 = info.clone();
        b.to_async(FuturesExecutor)
            .iter(|| scan_all_table(&info1, sstable_store.clone()));
    });
}

async fn compact<I: HummockIterator<Direction = Forward>>(iter: I, sstable_store: SstableStoreRef) {
    let opt = SstableBuilderOptions {
        capacity: 32 * 1024 * 1024,
        block_capacity: 64 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.001,
        ..Default::default()
    };
    let mut builder =
        CapacitySplitTableBuilder::for_test(LocalTableBuilderFactory::new(32, sstable_store, opt));

    let task_config = TaskConfig {
        key_range: KeyRange::inf(),
        cache_policy: CachePolicy::Disable,
        gc_delete_keys: false,
        watermark: 0,
        stats_target_table_ids: None,
        task_type: compact_task::TaskType::Dynamic,
        use_block_based_filter: true,
        ..Default::default()
    };
    compact_and_build_sst(
        &mut builder,
        CompactionDeleteRangeIterator::new(ForwardMergeRangeIterator::new(HummockEpoch::MAX)),
        &task_config,
        Arc::new(CompactorMetrics::unused()),
        iter,
        DummyCompactionFilter,
    )
    .await
    .unwrap();
}

fn bench_merge_iterator_compactor(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let sstable_store = mock_sstable_store();
    let test_key_size = 256 * 1024;
    let info1 = runtime
        .block_on(async { build_table(sstable_store.clone(), 1, 0..test_key_size, 1).await });
    let info2 = runtime
        .block_on(async { build_table(sstable_store.clone(), 2, 0..test_key_size, 1).await });
    let level1 = vec![info1, info2];

    let info1 = runtime
        .block_on(async { build_table(sstable_store.clone(), 3, 0..test_key_size, 2).await });
    let info2 = runtime
        .block_on(async { build_table(sstable_store.clone(), 4, 0..test_key_size, 2).await });
    let level2 = vec![info1, info2];
    let read_options = Arc::new(SstableIteratorReadOptions {
        cache_policy: CachePolicy::Fill(CachePriority::High),
        prefetch_for_large_query: false,
        must_iterated_end_user_key: None,
        max_preload_retry_times: 0,
    });
    c.bench_function("bench_union_merge_iterator", |b| {
        b.to_async(FuturesExecutor).iter(|| {
            let sstable_store1 = sstable_store.clone();
            let sub_iters = vec![
                ConcatIterator::new(level1.clone(), sstable_store.clone(), read_options.clone()),
                ConcatIterator::new(level2.clone(), sstable_store.clone(), read_options.clone()),
            ];
            let iter = UnorderedMergeIteratorInner::for_compactor(sub_iters);
            async move { compact(iter, sstable_store1).await }
        });
    });
    c.bench_function("bench_merge_iterator", |b| {
        b.to_async(&runtime).iter(|| {
            let sub_iters = vec![
                ConcatSstableIterator::new(
                    vec![0],
                    level1.clone(),
                    KeyRange::inf(),
                    sstable_store.clone(),
                    Arc::new(TaskProgress::default()),
                    0,
                ),
                ConcatSstableIterator::new(
                    vec![0],
                    level2.clone(),
                    KeyRange::inf(),
                    sstable_store.clone(),
                    Arc::new(TaskProgress::default()),
                    0,
                ),
            ];
            let iter = UnorderedMergeIteratorInner::for_compactor(sub_iters);
            let sstable_store1 = sstable_store.clone();
            async move { compact(iter, sstable_store1).await }
        });
    });
}

criterion_group!(
    benches,
    bench_table_build,
    bench_table_scan,
    bench_merge_iterator_compactor
);
criterion_main!(benches);
