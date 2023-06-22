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

use risingwave_hummock_sdk::CompactionGroupId;
pub use risingwave_meta_types::hummock::CompactStatus;

use crate::hummock::HUMMOCK_COMPACTION_STATUS_CF_NAME;
use crate::{MetadataModel, MetadataModelResult};

impl MetadataModel for CompactStatus {
    type KeyType = CompactionGroupId;
    type PbType = risingwave_pb::hummock::CompactStatus;

    fn cf_name() -> String {
        String::from(HUMMOCK_COMPACTION_STATUS_CF_NAME)
    }

    fn to_protobuf(&self) -> Self::PbType {
        self.into()
    }

    fn from_protobuf(prost: Self::PbType) -> Self {
        (&prost).into()
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.compaction_group_id)
    }
}
