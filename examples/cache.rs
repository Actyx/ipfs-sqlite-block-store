use fnv::{FnvHashMap, FnvHashSet};
use libipld::Cid;
use sqlite_block_store::{cache::InMemCacheTracker, Config, SizeTargets, Store};
use std::{
    cmp::Ord, fmt::Debug, ops::DerefMut, sync::Arc, sync::Mutex, time::Duration, time::Instant,
};

fn main() -> anyhow::Result<()> {
    // a tracker that only cares about access time
    let tracker = InMemCacheTracker::new(|access, _, _| Some(access));
    let mut store = Store::memory(
        Config::default()
            .with_size_targets(SizeTargets::new(1000, 1000000))
            .with_cache_tracker(tracker),
    )?;
    Ok(())
}
