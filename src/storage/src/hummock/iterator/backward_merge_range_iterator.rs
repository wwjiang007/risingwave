// Copyright 2023 RisingWave Labs
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

use std::collections::{BTreeSet, BinaryHeap};
use std::future::Future;

use risingwave_hummock_sdk::key::{PointRange, UserKey};
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::iterator::concat_delete_range_iterator::BackwardConcatDeleteRangeIterator;
use crate::hummock::iterator::delete_range_iterator::{
    BackwardRangeIteratorTyped, DeleteRangeIterator,
};
use crate::hummock::iterator::MergeRangeIterator;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{BackwardSstableDeleteRangeIterator, HummockResult, TableHolder};

pub struct BackwardMergeRangeIterator {
    heap: BinaryHeap<BackwardRangeIteratorTyped>,
    unused_iters: Vec<BackwardRangeIteratorTyped>,
    tmp_buffer: Vec<BackwardRangeIteratorTyped>,
    read_epoch: HummockEpoch,
    /// The correctness of the algorithm needs to be guaranteed by "the epoch of the
    /// intervals covering each other must be different".
    current_epochs: BTreeSet<HummockEpoch>,
}

impl BackwardMergeRangeIterator {
    pub fn new(read_epoch: HummockEpoch) -> Self {
        Self {
            heap: BinaryHeap::new(),
            unused_iters: vec![],
            tmp_buffer: vec![],
            read_epoch,
            current_epochs: BTreeSet::new(),
        }
    }
}
impl MergeRangeIterator for BackwardMergeRangeIterator {
    fn add_batch_iter(&mut self, batch: &SharedBufferBatch) {
        self.unused_iters.push(BackwardRangeIteratorTyped::Batch(
            batch.backward_delete_range_iter(),
        ));
    }

    fn add_sst_iter(&mut self, table: TableHolder) {
        self.unused_iters.push(BackwardRangeIteratorTyped::Sst(
            BackwardSstableDeleteRangeIterator::new(table),
        ));
    }

    fn add_concat_iter(&mut self, sstables: Vec<SstableInfo>, sstable_store: SstableStoreRef) {
        self.unused_iters.push(BackwardRangeIteratorTyped::Concat(
            BackwardConcatDeleteRangeIterator::new(sstables, sstable_store),
        ))
    }
}

impl BackwardMergeRangeIterator {
    fn current_extended_key(&self) -> PointRange<&[u8]> {
        self.heap.peek().unwrap().current_extended_key()
    }

    pub(super) async fn next_until(
        &mut self,
        target_user_key: UserKey<&[u8]>,
    ) -> HummockResult<()> {
        let target_extended_user_key = PointRange::from_user_key(target_user_key, false);
        while self.is_valid() && self.current_extended_key().gt(&target_extended_user_key) {
            self.next().await?;
        }
        Ok(())
    }

    #[inline(always)]
    pub fn check_key_deleted(&self, key_epoch: HummockEpoch) -> bool {
        self.current_epoch() >= key_epoch
    }
}

impl DeleteRangeIterator for BackwardMergeRangeIterator {
    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn current_epoch(&self) -> HummockEpoch {
        self.current_epochs
            .range(..=self.read_epoch)
            .last()
            .map_or(HummockEpoch::MIN, |epoch| *epoch)
    }

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
            self.tmp_buffer
                .push(self.heap.pop().expect("no inner iter"));
            while let Some(node) = self.heap.peek() && node.is_valid() && node.current_extended_key() ==
                self.tmp_buffer[0].current_extended_key() {
                self.tmp_buffer.push(self.heap.pop().unwrap());
            }
            for node in &self.tmp_buffer {
                let epoch = node.current_epoch();
                if epoch != HummockEpoch::MAX {
                    self.current_epochs.remove(&epoch);
                }
            }
            // Correct because ranges in an epoch won't intersect.
            for mut node in std::mem::take(&mut self.tmp_buffer) {
                node.next().await?;
                if node.is_valid() {
                    let epoch = node.current_epoch();
                    if epoch != HummockEpoch::MAX {
                        self.current_epochs.insert(epoch);
                    }
                    self.heap.push(node);
                } else {
                    // Put back to `unused_iters`
                    self.unused_iters.push(node);
                }
            }
            Ok(())
        }
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            self.current_epochs.clear();
            self.unused_iters.extend(self.heap.drain());
            for mut node in self.unused_iters.drain(..) {
                node.rewind().await?;
                if node.is_valid() {
                    let epoch = node.current_epoch();
                    if epoch != HummockEpoch::MAX {
                        self.current_epochs.insert(epoch);
                    }
                    self.heap.push(node);
                }
            }
            Ok(())
        }
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) -> Self::SeekFuture<'_> {
        async move {
            self.current_epochs.clear();
            let mut iters = std::mem::take(&mut self.unused_iters);
            iters.extend(self.heap.drain());
            for mut node in iters {
                node.seek(target_user_key).await?;
                if node.is_valid() {
                    let epoch = node.current_epoch();
                    if epoch != HummockEpoch::MAX {
                        self.current_epochs.insert(epoch);
                    }
                    self.heap.push(node);
                } else {
                    self.unused_iters.push(node);
                }
            }
            Ok(())
        }
    }

    fn is_valid(&self) -> bool {
        self.heap
            .peek()
            .map(|node| node.is_valid())
            .unwrap_or(false)
    }
}
#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::TableKey;
    use risingwave_hummock_sdk::HummockSstableId;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::iterator::BackwardMergeRangeIterator;
    use crate::hummock::test_utils::test_user_key;
    use crate::hummock::{
        create_monotonic_events, CompactionDeleteRangesBuilder, DeleteRangeTombstone,
        SstableBuilder, SstableBuilderOptions, SstableWriterOptions,
    };
    use crate::monitor::StoreLocalStatistic;

    async fn generate_sst(
        data: Vec<DeleteRangeTombstone>,
        sstable_store: &SstableStoreRef,
        sst_id: HummockSstableId,
    ) -> HummockResult<SstableInfo> {
        let mut builder = CompactionDeleteRangesBuilder::default();
        for range in data {
            builder.add_delete_events(create_monotonic_events(vec![range]));
        }
        let compaction_delete_range = builder.build_for_compaction();
        let ranges = compaction_delete_range
            .get_tombstone_between(test_user_key(b"").as_ref(), test_user_key(b"").as_ref());
        let opts = SstableBuilderOptions::default();
        let mut builder = SstableBuilder::for_test(
            sst_id,
            sstable_store
                .clone()
                .create_sst_writer(sst_id, SstableWriterOptions::default()),
            opts,
        );
        builder.add_monotonic_deletes(ranges);
        let output = builder.finish().await?;
        output.writer_output.await.unwrap()?;
        Ok(output.sst_info.sst_info)
    }

    #[tokio::test]
    async fn test_merge_iterator() {
        let sstable_store = mock_sstable_store();
        let table_id = TableId::new(0);
        let data = vec![
            DeleteRangeTombstone::new_for_test(table_id, b"aaaa".to_vec(), b"bbbb".to_vec(), 10),
            DeleteRangeTombstone::new_for_test(table_id, b"bbbb".to_vec(), b"eeee".to_vec(), 12),
            DeleteRangeTombstone::new_for_test(table_id, b"ffff".to_vec(), b"ffgg".to_vec(), 14),
        ];
        let sst1 = generate_sst(data, &sstable_store, 1).await.unwrap();
        let data = vec![
            DeleteRangeTombstone::new_for_test(table_id, b"bbdd".to_vec(), b"cccc".to_vec(), 16),
            DeleteRangeTombstone::new_for_test(table_id, b"dddd".to_vec(), b"gggg".to_vec(), 18),
            DeleteRangeTombstone::new_for_test(table_id, b"ffff".to_vec(), b"hhhh".to_vec(), 20),
        ];
        let sst2 = generate_sst(data, &sstable_store, 2).await.unwrap();
        let data = vec![
            DeleteRangeTombstone::new_for_test(table_id, b"aaaa".to_vec(), b"bbbb".to_vec(), 22),
            DeleteRangeTombstone::new_for_test(table_id, b"cccc".to_vec(), b"dddd".to_vec(), 22),
            DeleteRangeTombstone::new_for_test(table_id, b"eeee".to_vec(), b"ffff".to_vec(), 22),
        ];
        let sst3 = generate_sst(data, &sstable_store, 3).await.unwrap();

        let mut del_iter = BackwardMergeRangeIterator::new(150);
        del_iter.add_sst_iter(
            sstable_store
                .sstable(&sst1, &mut StoreLocalStatistic::default())
                .await
                .unwrap(),
        );
        del_iter.add_sst_iter(
            sstable_store
                .sstable(&sst2, &mut StoreLocalStatistic::default())
                .await
                .unwrap(),
        );
        del_iter.add_sst_iter(
            sstable_store
                .sstable(&sst3, &mut StoreLocalStatistic::default())
                .await
                .unwrap(),
        );
        del_iter.rewind().await.unwrap();
        assert!(!del_iter.check_key_deleted(10));
        del_iter
            .next_until(UserKey::new(table_id, TableKey(b"cccc")))
            .await
            .unwrap();
        assert!(del_iter.check_key_deleted(14));

        del_iter
            .next_until(UserKey::new(table_id, TableKey(b"bbbb")))
            .await
            .unwrap();
        assert!(del_iter.check_key_deleted(12));
        assert!(!del_iter.check_key_deleted(13));

        del_iter
            .next_until(UserKey::new(table_id, TableKey(b"aaaa")))
            .await
            .unwrap();
        assert!(del_iter.check_key_deleted(20));
        assert!(!del_iter.check_key_deleted(23));

        // read key between several overlapped range tombstone
        let mut del_iter = BackwardMergeRangeIterator::new(16);
        del_iter.add_sst_iter(
            sstable_store
                .sstable(&sst1, &mut StoreLocalStatistic::default())
                .await
                .unwrap(),
        );
        del_iter.add_sst_iter(
            sstable_store
                .sstable(&sst2, &mut StoreLocalStatistic::default())
                .await
                .unwrap(),
        );
        del_iter.add_sst_iter(
            sstable_store
                .sstable(&sst3, &mut StoreLocalStatistic::default())
                .await
                .unwrap(),
        );

        del_iter.rewind().await.unwrap();
        del_iter
            .next_until(UserKey::new(table_id, TableKey(b"cccc")))
            .await
            .unwrap();
        assert!(!del_iter.check_key_deleted(14));
        del_iter
            .next_until(UserKey::new(table_id, TableKey(b"aaaa")))
            .await
            .unwrap();
        assert!(!del_iter.check_key_deleted(11));
        assert!(del_iter.check_key_deleted(10));
    }
}
