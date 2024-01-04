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

use std::collections::VecDeque;
use std::ops::Range;

use futures_util::FutureExt;
use risingwave_common::array::Op;
use risingwave_common::must_match;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl, Sentinelled};
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::expr::expr_node::PbType as PbExprType;
use smallvec::{smallvec, SmallVec};

use super::range_utils::range_except;
use super::StateKey;
use crate::expr::{
    build_func, BoxedExpression, ExpressionBoxExt, InputRefExpression, LiteralExpression,
};
use crate::window_function::state::range_utils::range_diff;
use crate::window_function::{Frame, FrameBound, FrameBounds, FrameExclusion};

// TODO(): seems reusable
struct Entry<K, V> {
    key: K,
    value: V,
}

// TOOD()
fn test_order_col() -> (DataType, OrderType) {
    (DataType::Int32, OrderType::ascending())
}

/// A sliding window buffer implementation for `RANGE` frames.
pub struct RangeWindowBuffer<V: Clone> {
    frame_start_expr: Option<BoxedExpression>,
    frame_end_expr: Option<BoxedExpression>,
    frame_exclusion: FrameExclusion,
    buffer: VecDeque<Entry<StateKey, V>>, // TODO(): may store other key than StateKey
    curr_idx: usize,
    left_idx: usize,       // inclusive, note this can be > `curr_idx`
    right_excl_idx: usize, // exclusive, note this can be <= `curr_idx`
    curr_delta: Option<Vec<(Op, V)>>,
}

// TODO(): seems reusable
/// Note: A window frame can be pure preceding, pure following, or acrossing the _current row_.
pub struct CurrWindow<'a, K> {
    pub key: Option<&'a K>,
    pub preceding_saturated: bool,
    pub following_saturated: bool,
}

impl<V: Clone> RangeWindowBuffer<V> {
    pub fn new(frame: Frame, enable_delta: bool) -> Self {
        assert!(frame.bounds.validate().is_ok());

        let (frame_start, frame_end) =
            must_match!(frame.bounds, FrameBounds::Range(s, e) => (s, e));
        let frame_exclusion = frame.exclusion;

        if enable_delta {
            // TODO(rc): currently only support `FrameExclusion::NoOthers` for delta
            assert!(frame_exclusion.is_no_others());
        }

        let (data_type, order_type) = test_order_col(); // TODO()

        let (preceding_expr_type, following_expr_type) = if order_type.is_ascending() {
            // ORDER BY x ASC RANGE BETWEEN v1 PRECEDING AND v2 FOLLOWING => [x - v1, x + v2]
            (PbExprType::Subtract, PbExprType::Add)
        } else {
            // ORDER BY x DESC RANGE BETWEEN v1 PRECEDING AND v2 FOLLOWING => [x + v1, x - v2]
            (PbExprType::Add, PbExprType::Subtract)
        };

        let [frame_start_expr, frame_end_expr] = [&frame_start, &frame_end].map(|bound| {
            match bound {
                FrameBound::UnboundedPreceding | FrameBound::UnboundedFollowing => None,
                FrameBound::CurrentRow => {
                    // just use $0 here
                    Some(InputRefExpression::new(data_type.clone(), 0).boxed())
                }
                FrameBound::Preceding(offset) => Some(
                    build_func(
                        preceding_expr_type,
                        data_type.clone(),
                        vec![
                            InputRefExpression::new(data_type.clone(), 0).boxed(),
                            LiteralExpression::new(
                                data_type.clone(), // TODO(): should be offset type, maybe Constant is needed
                                Some(offset.clone().into()),
                            )
                            .boxed(),
                        ],
                    )
                    .expect("frontend should've checked the ability to add/sub")
                    .boxed(),
                ),
                FrameBound::Following(offset) => Some(
                    build_func(
                        following_expr_type,
                        data_type.clone(),
                        vec![
                            InputRefExpression::new(data_type.clone(), 0).boxed(),
                            LiteralExpression::new(
                                data_type.clone(), // TODO(): should be offset type
                                Some(offset.clone().into()),
                            )
                            .boxed(),
                        ],
                    )
                    .expect("frontend should've checked the ability to add/sub")
                    .boxed(),
                ),
            }
        });

        Self {
            frame_start_expr,
            frame_end_expr,
            frame_exclusion,
            buffer: Default::default(),
            curr_idx: 0,
            left_idx: 0,
            right_excl_idx: 0,
            curr_delta: if enable_delta {
                Some(Default::default())
            } else {
                None
            },
        }
    }

    // TODO(): seems reusable
    /// Get the key part of the current row.
    pub fn curr_key(&self) -> Option<&StateKey> {
        self.buffer.get(self.curr_idx).map(|Entry { key, .. }| key)
    }

    // TODO(): seems reusable
    /// Get the current window info.
    pub fn curr_window(&self) -> CurrWindow<'_, StateKey> {
        CurrWindow {
            key: self.curr_key(),
            preceding_saturated: self.preceding_saturated(),
            following_saturated: self.following_saturated(),
        }
    }

    // TODO(): seems reusable
    fn curr_window_outer(&self) -> Range<usize> {
        self.left_idx..self.right_excl_idx
    }

    // TODO(): seems reusable
    fn curr_window_exclusion(&self) -> Range<usize> {
        // TODO(rc): should intersect with `curr_window_outer` to be more accurate
        match self.frame_exclusion {
            FrameExclusion::CurrentRow => self.curr_idx..self.curr_idx + 1,
            FrameExclusion::NoOthers => self.curr_idx..self.curr_idx,
        }
    }

    // TODO(): seems reusable
    fn curr_window_ranges(&self) -> (Range<usize>, Range<usize>) {
        let selection = self.curr_window_outer();
        let exclusion = self.curr_window_exclusion();
        range_except(selection, exclusion)
    }

    // TODO(): seems reusable
    /// Iterate over values in the current window.
    pub fn curr_window_values(&self) -> impl Iterator<Item = &V> {
        assert!(self.left_idx <= self.right_excl_idx);
        assert!(self.right_excl_idx <= self.buffer.len());

        let (left, right) = self.curr_window_ranges();
        self.buffer
            .range(left)
            .chain(self.buffer.range(right))
            .map(|Entry { value, .. }| value)
    }

    // TODO(): seems reusable
    /// Consume the delta of values comparing the current window to the previous window.
    /// The delta is not guaranteed to be sorted, especially when frame exclusion is not `NoOthers`.
    pub fn consume_curr_window_values_delta(&mut self) -> impl Iterator<Item = (Op, V)> + '_ {
        self.curr_delta
            .as_mut()
            .expect("delta mode should be enabled")
            .drain(..)
    }

    // TODO(): seems reusable
    fn maintain_delta(&mut self, old_outer: Range<usize>, new_outer: Range<usize>) {
        debug_assert!(self.frame_exclusion.is_no_others());

        let (outer_removed, outer_added) = range_diff(old_outer.clone(), new_outer.clone());
        let delta = self.curr_delta.as_mut().unwrap();
        for idx in outer_removed.iter().cloned().flatten() {
            delta.push((Op::Delete, self.buffer[idx].value.clone()));
        }
        for idx in outer_added.iter().cloned().flatten() {
            delta.push((Op::Insert, self.buffer[idx].value.clone()));
        }
    }

    // TODO(): seems reusable
    /// Append a key-value pair to the buffer.
    pub fn append(&mut self, key: StateKey, value: V) {
        let old_outer = self.curr_window_outer();

        self.buffer.push_back(Entry { key, value });
        self.recalculate_left_right();

        if self.curr_delta.is_some() {
            self.maintain_delta(old_outer, self.curr_window_outer());
        }
    }

    // TODO(): seems reusable
    /// Get the smallest key that is still kept in the buffer.
    /// Returns `None` if there's nothing yet.
    pub fn smallest_key(&self) -> Option<&StateKey> {
        self.buffer.front().map(|Entry { key, .. }| key)
    }

    // TODO(): seems reusable
    /// Slide the current window forward.
    /// Returns the keys that are removed from the buffer.
    pub fn slide(&mut self) -> impl Iterator<Item = (StateKey, V)> + '_ {
        let old_outer = self.curr_window_outer();

        self.curr_idx += 1;
        self.recalculate_left_right();

        if self.curr_delta.is_some() {
            self.maintain_delta(old_outer, self.curr_window_outer());
        }

        let min_needed_idx = std::cmp::min(self.left_idx, self.curr_idx);
        self.curr_idx -= min_needed_idx;
        self.left_idx -= min_needed_idx;
        self.right_excl_idx -= min_needed_idx;
        self.buffer
            .drain(0..min_needed_idx)
            .map(|Entry { key, value }| (key, value))
    }

    fn preceding_saturated(&self) -> bool {
        self.curr_key().is_some() && {
            // TODO(rc): It seems that preceding saturation is not important, may remove later.
            true
        }
    }

    fn following_saturated(&self) -> bool {
        self.curr_key().is_some()
            && {
                // Left OK? (note that `left_idx` can be greater than `right_idx`)
                // The following line checks whether the left value is the last one in the buffer.
                // Here we adopt a conservative approach, which means we assume the next future value
                // is likely to be the same as the last value in the current window, in which case
                // we can't say the current window is saturated.
                self.left_idx < self.buffer.len() /* non-zero */ - 1
            }
            && {
                // Right OK? Ditto.
                self.right_excl_idx < self.buffer.len()
            }
    }

    fn recalculate_left_right(&mut self) {
        if self.buffer.is_empty() {
            self.left_idx = 0;
            self.right_excl_idx = 0;
        }

        let Some(curr_key) = self.curr_key() else {
            // If the current index has been moved to a future position, we can't touch anything
            // because the next coming key may equal to the previous one which means the left and
            // right indices will be the same.
            return;
        };

        let (data_type, order_type) = test_order_col(); // TODO()

        let curr_order_value =
            memcmp_encoding::decode_value(&data_type, &curr_key.order_key, order_type)
                .expect("no reason to fail here");
        println!("[rc] curr_order_value = {:?}", curr_order_value);
        // NOTE: only support one order column for `RANGE` frame
        let curr_order_key = OwnedRow::new(vec![curr_order_value]);

        let [start_enc, end_enc] = [self.frame_start_expr.as_ref(), self.frame_end_expr.as_ref()]
            .map(|expr_opt| {
                expr_opt.map(|expr| {
                    // TODO(): extract something like FrameBoundExpr to hide the construction and evaluation of BoxedExpression
                    let value_res = expr.eval_row(&curr_order_key).now_or_never().expect(
                        "it's just numeric/datetime Add/Substract, should finish immediately",
                    );

                    // TODO(): handle overflow
                    let value = value_res.unwrap();
                    println!("[rc] expr: {:?}, value = {:?}", expr, value);

                    memcmp_encoding::encode_value(value, order_type)
                        .expect("no reason to fail here")
                })
            });

        if let Some(start_enc) = start_enc {
            // bounded, find the start position
            self.left_idx = self
                .buffer
                .partition_point(|elem| elem.key.order_key < start_enc);
        } else {
            // unbounded frame start
            assert_eq!(
                self.left_idx, 0,
                "for unbounded start, left index should always be 0"
            );
        }

        if let Some(end_enc) = end_enc {
            // bounded, find the end position
            self.right_excl_idx = self
                .buffer
                .partition_point(|elem| elem.key.order_key <= end_enc);
        } else {
            // unbounded frame end
            self.right_excl_idx = self.buffer.len();
        }

        println!(
            "[rc] buffer: {:?}",
            self.buffer.iter().map(|elem| &elem.key).collect::<Vec<_>>()
        );
        println!(
            "[rc] left = {}, right excl = {}, curr = {}",
            self.left_idx, self.right_excl_idx, self.curr_idx
        );
    }
}
