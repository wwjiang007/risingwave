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

#![feature(return_position_impl_trait_in_trait)]
#![feature(async_fn_in_trait)]
#![feature(lint_reasons)]
#![feature(let_chains)]
#![feature(result_option_inspect)]
#![feature(associated_type_defaults)]
#![feature(iter_from_generator)]
#![feature(generators)]
#![feature(iterator_try_collect)]
#![feature(box_into_inner)]
#![feature(type_alias_impl_trait)]
#![feature(if_let_guard)]

pub mod aws_auth;
pub mod aws_utils;
pub mod common;
pub mod kafka;
pub mod parser;
pub mod schema;
pub mod sink;

use std::time::Duration;

use duration_str::parse_std;
use risingwave_pb::connector_service::SinkPayloadFormat;
use risingwave_rpc_client::ConnectorClient;
use serde::de;

#[derive(Clone, Debug, Default)]
pub struct ConnectorParams {
    pub connector_client: Option<ConnectorClient>,
    pub sink_payload_format: SinkPayloadFormat,
}

impl ConnectorParams {
    pub fn new(
        connector_client: Option<ConnectorClient>,
        sink_payload_format: SinkPayloadFormat,
    ) -> Self {
        Self {
            connector_client,
            sink_payload_format,
        }
    }
}

pub fn deserialize_u32_from_string<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    s.parse().map_err(|_| {
        de::Error::invalid_value(
            de::Unexpected::Str(&s),
            &"integer greater than or equal to 0",
        )
    })
}

pub fn deserialize_bool_from_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    let s = s.to_ascii_lowercase();
    match s.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(de::Error::invalid_value(
            de::Unexpected::Str(&s),
            &"true or false",
        )),
    }
}

pub fn deserialize_duration_from_string<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    parse_std(&s).map_err(|_| de::Error::invalid_value(
        de::Unexpected::Str(&s),
        &"The String value unit support for one of:[“y”,“mon”,“w”,“d”,“h”,“m”,“s”, “ms”, “µs”, “ns”]",
    ))
}
