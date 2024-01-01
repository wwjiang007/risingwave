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
use std::sync::Arc;

use risingwave_common::bail;
use risingwave_pb::expr::expr_node::Type::{
    GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
};
use risingwave_pb::stream_plan::DynamicFilterNode;

use super::*;
use crate::common::table::state_table::{StateTable, WatermarkCacheStateTable};
use crate::executor::DynamicFilterExecutor;

pub struct DynamicFilterExecutorBuilder;

impl ExecutorBuilder for DynamicFilterExecutorBuilder {
    type Node = DynamicFilterNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [source_l, source_r]: [_; 2] = params.input.try_into().unwrap();
        let key_l = node.get_left_key() as usize;

        let vnodes = Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for dynamic filter"),
        );

        let prost_condition = node.get_condition()?;
        let comparator = prost_condition.get_function_type()?;
        if !matches!(
            comparator,
            GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual
        ) {
            bail!(
                "`DynamicFilterExecutor` only supports comparators:\
                GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual",
            );
        }

        let condition_always_relax = node.get_condition_always_relax();

        let state_table_r =
            StateTable::from_table_catalog(node.get_right_table()?, store.clone(), None).await;

        let left_table = node.get_left_table()?;
        let cleaned_by_watermark = left_table.get_cleaned_by_watermark();

        if cleaned_by_watermark {
            let state_table_l = WatermarkCacheStateTable::from_table_catalog(
                node.get_left_table()?,
                store,
                Some(vnodes),
            )
            .await;

            Ok(Box::new(DynamicFilterExecutor::new(
                params.actor_context,
                params.info,
                source_l,
                source_r,
                key_l,
                comparator,
                state_table_l,
                state_table_r,
                params.executor_stats,
                params.env.config().developer.chunk_size,
                condition_always_relax,
                cleaned_by_watermark,
            )))
        } else {
            let state_table_l =
                StateTable::from_table_catalog(node.get_left_table()?, store, Some(vnodes)).await;

            Ok(Box::new(DynamicFilterExecutor::new(
                params.actor_context,
                params.info,
                source_l,
                source_r,
                key_l,
                comparator,
                state_table_l,
                state_table_r,
                params.executor_stats,
                params.env.config().developer.chunk_size,
                condition_always_relax,
                cleaned_by_watermark,
            )))
        }
    }
}
