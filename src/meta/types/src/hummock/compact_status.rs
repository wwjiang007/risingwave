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

use std::collections::HashSet;

use itertools::Itertools;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::{CompactTask, LevelType};

use super::LevelHandler;

pub struct CompactStatus {
    pub compaction_group_id: CompactionGroupId,
    pub level_handlers: Vec<LevelHandler>,
}

impl std::fmt::Debug for CompactStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactStatus")
            .field("compaction_group_id", &self.compaction_group_id)
            .field("level_handlers", &self.level_handlers)
            .finish()
    }
}

impl PartialEq for CompactStatus {
    fn eq(&self, other: &Self) -> bool {
        self.level_handlers.eq(&other.level_handlers)
            && self.compaction_group_id == other.compaction_group_id
    }
}

impl Clone for CompactStatus {
    fn clone(&self) -> Self {
        Self {
            compaction_group_id: self.compaction_group_id,
            level_handlers: self.level_handlers.clone(),
        }
    }
}

impl CompactStatus {
    pub fn new(compaction_group_id: CompactionGroupId, max_level: u64) -> CompactStatus {
        let mut level_handlers = vec![];
        for level in 0..=max_level {
            level_handlers.push(LevelHandler::new(level as u32));
        }
        CompactStatus {
            compaction_group_id,
            level_handlers,
        }
    }

    pub fn is_trivial_move_task(task: &CompactTask) -> bool {
        if task.input_ssts.len() != 2
            || task.input_ssts[0].level_type != LevelType::Nonoverlapping as i32
        {
            return false;
        }

        // it may be a manual compaction task
        if task.input_ssts[0].level_idx == task.input_ssts[1].level_idx
            && task.input_ssts[0].level_idx > 0
        {
            return false;
        }

        if task.input_ssts[1].level_idx == task.target_level
            && task.input_ssts[1].table_infos.is_empty()
        {
            return true;
        }

        false
    }

    pub fn is_trivial_reclaim(task: &CompactTask) -> bool {
        let exist_table_ids = HashSet::<u32>::from_iter(task.existing_table_ids.clone());
        task.input_ssts.iter().all(|level| {
            level.table_infos.iter().all(|sst| {
                sst.table_ids
                    .iter()
                    .all(|table_id| !exist_table_ids.contains(table_id))
            })
        })
    }

    /// Declares a task as either succeeded, failed or canceled.
    pub fn report_compact_task(&mut self, compact_task: &CompactTask) {
        for level in &compact_task.input_ssts {
            self.level_handlers[level.level_idx as usize].remove_task(compact_task.task_id);
        }
    }

    pub fn cancel_compaction_tasks_if<F: Fn(u64) -> bool>(&mut self, should_cancel: F) -> u32 {
        let mut count: u32 = 0;
        for level in &mut self.level_handlers {
            for pending_task_id in level.pending_tasks_ids() {
                if should_cancel(pending_task_id) {
                    level.remove_task(pending_task_id);
                    count += 1;
                }
            }
        }
        count
    }

    pub fn compaction_group_id(&self) -> CompactionGroupId {
        self.compaction_group_id
    }
}

impl From<&CompactStatus> for risingwave_pb::hummock::CompactStatus {
    fn from(status: &CompactStatus) -> Self {
        risingwave_pb::hummock::CompactStatus {
            compaction_group_id: status.compaction_group_id,
            level_handlers: status.level_handlers.iter().map_into().collect(),
        }
    }
}

impl From<CompactStatus> for risingwave_pb::hummock::CompactStatus {
    fn from(status: CompactStatus) -> Self {
        (&status).into()
    }
}

impl From<&risingwave_pb::hummock::CompactStatus> for CompactStatus {
    fn from(status: &risingwave_pb::hummock::CompactStatus) -> Self {
        CompactStatus {
            compaction_group_id: status.compaction_group_id,
            level_handlers: status.level_handlers.iter().map_into().collect(),
        }
    }
}
