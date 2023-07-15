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

use risingwave_common::error::Result;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::SortOverWindowNode;

use super::generic::PlanWindowFunction;
use super::utils::impl_distill_by_unit;
use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch,
    ToLocalBatch,
};
use crate::optimizer::property::{Order, RequiredDist};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchOverWindow {
    pub base: PlanBase,
    logical: generic::OverWindow<PlanRef>,
}

impl BatchOverWindow {
    pub fn new(logical: generic::OverWindow<PlanRef>) -> Self {
        assert!(logical.funcs_have_same_partition_and_order());

        let input = &logical.input;
        let input_dist = input.distribution().clone();

        let order = Order::new(
            logical
                .partition_key_indices()
                .into_iter()
                .map(|idx| ColumnOrder::new(idx, OrderType::default()))
                .chain(logical.order_key().iter().cloned())
                .collect(),
        );

        let base = PlanBase::new_batch_from_logical(&logical, input_dist, order);
        BatchOverWindow { base, logical }
    }

    fn expected_input_order(&self) -> Order {
        self.order().clone()
    }
}

impl_distill_by_unit!(BatchOverWindow, logical, "BatchOverWindow");

impl PlanTreeNodeUnary for BatchOverWindow {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}

impl_plan_tree_node_for_unary! { BatchOverWindow }

impl ToDistributedBatch for BatchOverWindow {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed_with_required(
            &self.expected_input_order(),
            &RequiredDist::shard_by_key(
                self.input().schema().len(),
                &self.logical.partition_key_indices(),
            ),
        )?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToLocalBatch for BatchOverWindow {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(new_input, &self.expected_input_order())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchPb for BatchOverWindow {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::SortOverWindow(SortOverWindowNode {
            calls: self
                .logical
                .window_functions()
                .iter()
                .map(PlanWindowFunction::to_protobuf)
                .collect(),
            partition_by: self
                .logical
                .partition_key_indices()
                .into_iter()
                .map(|idx| idx as _)
                .collect(),
        })
    }
}

impl ExprRewritable for BatchOverWindow {}
