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
        while self.is_valid() && self.current_extended_key().le(&target_extended_user_key) {
            self.next().await?;
        }
        Ok(())
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
