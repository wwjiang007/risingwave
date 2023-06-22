use std::collections::HashMap;

use risingwave_connector::source::SplitImpl;

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
