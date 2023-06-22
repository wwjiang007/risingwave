use std::collections::HashMap;

use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::ActorMapping;
use risingwave_connector::source::SplitImpl;

use crate::{ActorId, DispatcherId, FragmentId};

/// [`Reschedule`] is for the `Command::RescheduleFragment`, which is used for rescheduling actors
/// in some fragment, like scaling or migrating.
#[derive(Debug, Clone)]
pub struct Reschedule {
    /// Added actors in this fragment.
    pub added_actors: Vec<ActorId>,
    /// Removed actors in this fragment.
    pub removed_actors: Vec<ActorId>,

    /// Vnode bitmap updates for some actors in this fragment.
    pub vnode_bitmap_updates: HashMap<ActorId, Bitmap>,

    /// The upstream fragments of this fragment, and the dispatchers that should be updated.
    pub upstream_fragment_dispatcher_ids: Vec<(FragmentId, DispatcherId)>,
    /// New hash mapping of the upstream dispatcher to be updated.
    ///
    /// This field exists only when there's upstream fragment and the current fragment is
    /// hash-sharded.
    pub upstream_dispatcher_mapping: Option<ActorMapping>,

    /// The downstream fragments of this fragment.
    pub downstream_fragment_ids: Vec<FragmentId>,

    /// Reassigned splits for source actors
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
}
