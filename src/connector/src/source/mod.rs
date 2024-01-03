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

pub mod base;
pub mod cdc;
pub mod data_gen_util;
pub mod datagen;
pub mod filesystem;
pub mod google_pubsub;
pub mod kafka;
pub mod kinesis;
pub mod monitor;
pub mod nats;
pub mod nexmark;
pub mod pulsar;
pub use base::{UPSTREAM_SOURCE_KEY, *};
pub(crate) use common::*;
pub use google_pubsub::GOOGLE_PUBSUB_CONNECTOR;
pub use kafka::KAFKA_CONNECTOR;
pub use kinesis::KINESIS_CONNECTOR;
pub use nats::NATS_CONNECTOR;
mod common;
mod manager;
pub mod test_source;

pub use manager::{SourceColumnDesc, SourceColumnType};

pub use crate::parser::additional_columns::{
    get_connector_compatible_additional_columns, CompatibleAdditionalColumnsFn,
};
pub use crate::source::filesystem::opendal_source::{
    GCS_CONNECTOR, OPENDAL_S3_CONNECTOR, POSIX_FS_CONNECTOR,
};
pub use crate::source::filesystem::S3_CONNECTOR;
pub use crate::source::nexmark::NEXMARK_CONNECTOR;
pub use crate::source::pulsar::PULSAR_CONNECTOR;

pub fn should_copy_to_format_encode_options(key: &str, connector: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "schema.registry",
        "schema.location",
        "message",
        "key.message",
        "without_header",
        "delimiter",
        // AwsAuthProps
        "region",
        "endpoint_url",
        "access_key",
        "secret_key",
        "session_token",
        "arn",
        "external_id",
        "profile",
    ];
    PREFIXES.iter().any(|prefix| key.starts_with(prefix))
        || (key == "endpoint" && !connector.eq_ignore_ascii_case(KINESIS_CONNECTOR))
}
