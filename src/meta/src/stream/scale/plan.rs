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

use std::cmp::min;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use itertools::Itertools;
use num_integer::Integer;
use num_traits::abs;
use risingwave_common::hash::ParallelUnitId;

use crate::manager::WorkerId;
use crate::model::FragmentId;
use crate::storage::MetaStore;
use crate::stream::GlobalStreamManager;
use crate::MetaResult;

impl<S> GlobalStreamManager<S>
where
    S: MetaStore,
{
    pub async fn generate_rebalance_plan(
        &self,
        fragment_ids: HashSet<FragmentId>,
        _worker_ids: HashSet<WorkerId>,
    ) -> MetaResult<()> {
        let mut current_fragment_parallel_unit_mapping = HashMap::new();
        let table_fragments = self.fragment_manager.list_table_fragments().await?;
        for table_fragments in table_fragments {
            for (fragment_id, fragment) in &table_fragments.fragments {
                if !fragment_ids.contains(fragment_id) {
                    continue;
                }

                let fragment_parallel_units = fragment.get_actors().iter().map(|actor| {
                    table_fragments
                        .actor_status
                        .get(&actor.actor_id)
                        .and_then(|actor_status| actor_status.parallel_unit.clone())
                        .expect("TODO")
                });

                for parallel_unit in fragment_parallel_units {
                    current_fragment_parallel_unit_mapping
                        .entry((
                            *fragment_id as FragmentId,
                            parallel_unit.worker_node_id as WorkerId,
                        ))
                        .or_insert(HashSet::new())
                        .insert(parallel_unit.id as ParallelUnitId);
                }
            }
        }

        todo!()
    }
}

pub(crate) fn rebalance_worker_pu(
    fragment_worker_parallel_unit_map: HashMap<WorkerId, BTreeSet<ParallelUnitId>>,
    workers_to_remove: &BTreeSet<WorkerId>,
    workers_to_create: &BTreeSet<WorkerId>,
    total: usize,
) -> HashMap<WorkerId, BTreeSet<ParallelUnitId>> {
    assert!(fragment_worker_parallel_unit_map.len() >= workers_to_remove.len());

    let target_actor_count =
        fragment_worker_parallel_unit_map.len() - workers_to_remove.len() + workers_to_create.len();
    assert!(target_actor_count > 0);

    // represents the balance of each actor, used to sort later
    #[derive(Debug)]
    struct Balance {
        actor_id: WorkerId,
        balance: i32,
        parallel_unit_ids: BTreeSet<ParallelUnitId>,
    }
    let (expected, mut remain) = total.div_rem(&target_actor_count);

    tracing::debug!(
        "expected {}, remain {}, prev actors {}, target actors {}",
        expected,
        remain,
        fragment_worker_parallel_unit_map.len(),
        target_actor_count,
    );

    let (prev_expected, _) = total.div_rem(&fragment_worker_parallel_unit_map.len());

    let (mut removed, mut rest): (Vec<_>, Vec<_>) = fragment_worker_parallel_unit_map
        .into_iter()
        //.partition(|(&(a, b), _)| workers_to_remove.contains(b));
        .partition(|&(a, _)| workers_to_remove.contains(&a));

    // let order_by_parallel_unit_desc
    //     |(_, bitmap_a): &(ActorId, Bitmap), (_, bitmap_b): &(ActorId, Bitmap)| -> Ordering {
    //         bitmap_a.count_ones().cmp(&bitmap_b.count_ones()).reverse()
    //     };
    //
    // let builder_from_bitmap = |bitmap: &Bitmap| -> BitmapBuilder {
    //     let mut builder = BitmapBuilder::default();
    //     builder.append_bitmap(bitmap);
    //     builder
    // };

    let prev_remain = removed
        .iter()
        .map(|(_, parallel_unit_ids)| {
            assert!(parallel_unit_ids.len() >= prev_expected);
            parallel_unit_ids.len() - prev_expected
        })
        .sum::<usize>();

    removed.sort_by(|(_, parallel_unit_ids_a), (_, parallel_unit_ids_b)| {
        parallel_unit_ids_a
            .len()
            .cmp(&parallel_unit_ids_b.len())
            .reverse()
    });
    rest.sort_by(|(_, parallel_unit_ids_a), (_, parallel_unit_ids_b)| {
        parallel_unit_ids_a
            .len()
            .cmp(&parallel_unit_ids_b.len())
            .reverse()
    });

    let removed_balances = removed.into_iter().map(|(actor_id, bitmap)| Balance {
        actor_id,
        balance: bitmap.len() as i32,
        parallel_unit_ids: bitmap,
    });

    let mut rest_balances = rest
        .into_iter()
        .map(|(actor_id, bitmap)| Balance {
            actor_id,
            balance: bitmap.len() as i32 - expected as i32,
            parallel_unit_ids: bitmap,
        })
        .collect_vec();

    let mut created_balances = workers_to_create
        .iter()
        .map(|actor_id| Balance {
            actor_id: *actor_id,
            balance: -(expected as i32),
            parallel_unit_ids: BTreeSet::new(),
        })
        .collect_vec();

    for balance in created_balances
        .iter_mut()
        .rev()
        .take(prev_remain)
        .chain(rest_balances.iter_mut())
    {
        if remain > 0 {
            balance.balance -= 1;
            remain -= 1;
        }
    }

    // consume the rest `remain`
    for balance in &mut created_balances {
        if remain > 0 {
            balance.balance -= 1;
            remain -= 1;
        }
    }

    assert_eq!(remain, 0);

    let mut v: VecDeque<_> = removed_balances
        .chain(rest_balances)
        .chain(created_balances)
        .collect();

    // We will return the full bitmap here after rebalancing,
    // if we want to return only the changed actors, filter balance = 0 here
    let mut result = HashMap::with_capacity(target_actor_count);

    for balance in &v {
        tracing::debug!(
            "actor {:5}\tbalance {:5}\tR[{:5}]\tC[{:5}]",
            balance.actor_id,
            balance.balance,
            workers_to_remove.contains(&balance.actor_id),
            workers_to_create.contains(&balance.actor_id)
        );
    }

    while !v.is_empty() {
        if v.len() == 1 {
            let single = v.pop_front().unwrap();
            assert_eq!(single.balance, 0);
            if !workers_to_remove.contains(&single.actor_id) {
                result.insert(single.actor_id, single.parallel_unit_ids);
            }

            continue;
        }

        let mut src = v.pop_front().unwrap();
        let mut dst = v.pop_back().unwrap();

        let n = min(abs(src.balance), abs(dst.balance));

        let moving_parallel_units = src.parallel_unit_ids.iter().take(n as usize);
        dst.parallel_unit_ids.extend(moving_parallel_units);

        // for parallel_unit_id in moving_parallel_units {
        //     src.parallel_unit_ids.remove(parallel_unit_id);
        //     dst.parallel_unit_ids.insert(*parallel_unit_id);
        // }

        src.balance -= n;
        dst.balance += n;

        if src.balance != 0 {
            v.push_front(src);
        } else if !workers_to_remove.contains(&src.actor_id) {
            result.insert(src.actor_id, src.parallel_unit_ids);
        }

        if dst.balance != 0 {
            v.push_back(dst);
        } else {
            result.insert(dst.actor_id, dst.parallel_unit_ids);
        }
    }

    result
}
