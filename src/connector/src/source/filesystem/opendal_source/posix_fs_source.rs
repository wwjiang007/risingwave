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

use std::marker::PhantomData;

use anyhow::Context;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Fs;
use opendal::Operator;

use super::opendal_enumerator::OpendalEnumerator;
use super::{OpendalSource, PosixFsProperties};

impl<Src: OpendalSource> OpendalEnumerator<Src> {
    /// create opendal posix fs source.
    pub fn new_posix_fs_source(posix_fs_properties: PosixFsProperties) -> anyhow::Result<Self> {
        // Create Fs builder.
        let mut builder = Fs::default();

        builder.root(&posix_fs_properties.root);

        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();

        let (prefix, matcher) = if let Some(pattern) = posix_fs_properties.match_pattern.as_ref() {
            // TODO(Kexiang): Currently, FsListnenr in opendal does not support a prefix. (Seems a bug in opendal)
            // So we assign prefix to empty string.
            let matcher = glob::Pattern::new(pattern)
                .with_context(|| format!("Invalid match_pattern: {}", pattern))?;
            (Some(String::new()), Some(matcher))
        } else {
            (None, None)
        };
        Ok(Self {
            op,
            prefix,
            matcher,
            marker: PhantomData,
        })
    }
}
