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

use std::ops::Deref;
use std::sync::Arc;

use super::MonitoredGlobalAlloc;
use crate::metrics::{LabelGuardedIntGauge, LabelGuardedMetricsContext, TrAdderGauge};

pub trait MemCounter: Send + Sync + 'static {
    fn add(&self, bytes: i64);
    fn get_bytes_used(&self) -> i64;
}

impl MemCounter for TrAdderGauge {
    fn add(&self, bytes: i64) {
        self.add(bytes)
    }

    fn get_bytes_used(&self) -> i64 {
        self.get()
    }
}

impl<const N: usize, C: LabelGuardedMetricsContext> MemCounter for LabelGuardedIntGauge<N, C> {
    fn add(&self, bytes: i64) {
        self.deref().add(bytes)
    }

    fn get_bytes_used(&self) -> i64 {
        self.get()
    }
}

struct MemoryContextInner {
    counter: Box<dyn MemCounter>,
    parent: Option<MemoryContext>,
}

#[derive(Clone)]
pub struct MemoryContext {
    /// Add None op mem context, so that we don't need to return [`Option`] in
    /// `BatchTaskContext`. This helps with later `Allocator` implementation.
    inner: Option<Arc<MemoryContextInner>>,
}

impl MemoryContext {
    pub fn new(parent: Option<MemoryContext>, counter: impl MemCounter) -> Self {
        let c = Box::new(counter);
        Self {
            inner: Some(Arc::new(MemoryContextInner { counter: c, parent })),
        }
    }

    /// Creates a noop memory context.
    pub fn none() -> Self {
        Self { inner: None }
    }

    pub fn root(counter: impl MemCounter) -> Self {
        Self::new(None, counter)
    }

    /// Add `bytes` memory usage. Pass negative value to decrease memory usage.
    pub fn add(&self, bytes: i64) {
        if let Some(inner) = &self.inner {
            inner.counter.add(bytes);

            if let Some(parent) = &inner.parent {
                parent.add(bytes);
            }
        }
    }

    pub fn get_bytes_used(&self) -> i64 {
        if let Some(inner) = &self.inner {
            inner.counter.get_bytes_used()
        } else {
            0
        }
    }

    /// Creates a new global allocator that reports memory usage to this context.
    pub fn global_allocator(&self) -> MonitoredGlobalAlloc {
        MonitoredGlobalAlloc::with_memory_context(self.clone())
    }
}

impl Drop for MemoryContextInner {
    fn drop(&mut self) {
        if let Some(p) = &self.parent {
            p.add(-self.counter.get_bytes_used())
        }
    }
}
