#![feature(result_option_inspect)]

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use risingwave_connector::source::SplitImpl;
use risingwave_pb::common::HostAddress;

pub mod barrier;
pub mod hummock;

/// A global, unique identifier of an actor
pub type ActorId = u32;

/// Should be used together with `ActorId` to uniquely identify a dispatcher
pub type DispatcherId = u64;

/// A global, unique identifier of a fragment
pub type FragmentId = u32;

pub type SourceId = u32;

pub type WorkerId = u32;

pub type SplitAssignment = HashMap<FragmentId, HashMap<ActorId, Vec<SplitImpl>>>;

/// The id preserved for the meta node. Note that there's no such entry in cluster manager.
pub const META_NODE_ID: u32 = 0;

#[derive(Clone, Debug)]
pub struct WorkerKey(pub HostAddress);

impl PartialEq<Self> for WorkerKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl Eq for WorkerKey {}

impl Hash for WorkerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.host.hash(state);
        self.0.port.hash(state);
    }
}
