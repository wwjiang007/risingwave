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

#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(type_alias_impl_trait)]
#![feature(drain_filter)]
#![feature(custom_test_frameworks)]
#![feature(lint_reasons)]
#![feature(map_try_insert)]
#![feature(hash_drain_filter)]
#![feature(btree_drain_filter)]
#![feature(result_option_inspect)]
#![feature(lazy_cell)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![feature(assert_matches)]
#![feature(try_blocks)]
#![cfg_attr(coverage, feature(no_coverage))]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]
#![feature(is_sorted)]
#![feature(string_leak)]
#![feature(impl_trait_in_assoc_type)]

mod etcd_meta_store;
mod etcd_retry_client;
mod mem_meta_store;
pub mod meta_store;
#[cfg(test)]
mod tests;
mod transaction;
mod wrapped_etcd_client;

pub type ColumnFamily = String;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub use etcd_meta_store::*;
pub use mem_meta_store::*;
pub use meta_store::*;
pub use transaction::*;
pub use wrapped_etcd_client::*;
