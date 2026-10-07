// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::keyspace::InternalKeyspaceId;
use core::ops::Bound;
use lsm_tree::Slice;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeBounds,
    sync::Mutex,
};

#[derive(Clone, Debug)]
enum Read {
    Single(Slice),
    Range {
        start: Bound<Slice>,
        end: Bound<Slice>,
    },
    All,
}

#[derive(Default, Debug)]
pub struct ConflictManager {
    reads: Mutex<BTreeMap<InternalKeyspaceId, Vec<Read>>>,
    conflict_keys: Mutex<BTreeMap<InternalKeyspaceId, BTreeSet<Slice>>>,
}

impl ConflictManager {
    fn push_read(&self, keyspace_id: InternalKeyspaceId, read: Read) {
        #[expect(clippy::expect_used)]
        let mut lock = self.reads.lock().expect("lock is poisoned");

        if let Some(tbl) = lock.get_mut(&keyspace_id) {
            tbl.push(read);
        } else {
            lock.entry(keyspace_id).or_default().push(read);
        }
    }

    pub fn mark_read(&self, keyspace_id: InternalKeyspaceId, key: Slice) {
        self.push_read(keyspace_id, Read::Single(key));
    }

    pub fn mark_conflict(&self, keyspace_id: InternalKeyspaceId, key: Slice) {
        #[expect(clippy::expect_used)]
        let mut lock = self.conflict_keys.lock().expect("lock is poisoned");

        if let Some(tbl) = lock.get_mut(&keyspace_id) {
            tbl.insert(key);
        } else {
            lock.entry(keyspace_id).or_default().insert(key);
        }
    }

    pub fn mark_range(&self, keyspace_id: InternalKeyspaceId, range: impl RangeBounds<Slice>) {
        let start = match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let read = if start == Bound::Unbounded && end == Bound::Unbounded {
            Read::All
        } else {
            Read::Range { start, end }
        };

        self.push_read(keyspace_id, read);
    }

    /// Returns `true` if `self` and `other` wrote the same key in the same keyspace.
    ///
    /// Only the conflict keys (writes) of both sides are compared; reads are ignored.
    ///
    /// Locks both sides' conflict keys at once, so it is only called while holding the
    /// oracle lock.
    pub fn has_write_overlap(&self, other: &Self) -> bool {
        if std::ptr::eq(self, other) {
            #[expect(clippy::expect_used)]
            let lock = self.conflict_keys.lock().expect("lock is poisoned");
            return lock.values().any(|keys| !keys.is_empty());
        }

        #[expect(clippy::expect_used)]
        let own_keys = self.conflict_keys.lock().expect("lock is poisoned");

        #[expect(clippy::expect_used)]
        let other_keys = other.conflict_keys.lock().expect("lock is poisoned");

        own_keys.iter().any(|(keyspace_id, keys)| {
            other_keys.get(keyspace_id).is_some_and(|other_keys| {
                let (smaller, larger) = if keys.len() <= other_keys.len() {
                    (keys, other_keys)
                } else {
                    (other_keys, keys)
                };
                smaller.iter().any(|key| larger.contains(key))
            })
        })
    }

    #[expect(clippy::too_many_lines, clippy::significant_drop_tightening)]
    pub fn has_conflict(&self, other: &Self) -> bool {
        #[expect(clippy::expect_used)]
        let reads_lock = self.reads.lock().expect("lock is poisoned");

        #[expect(clippy::expect_used)]
        let conflict_keys_lock = other.conflict_keys.lock().expect("lock is poisoned");

        if reads_lock.is_empty() {
            return false;
        }

        for (keyspace_name, keys) in &*reads_lock {
            if let Some(other_conflict_keys) = conflict_keys_lock.get(keyspace_name) {
                for ro in keys {
                    match ro {
                        Read::Single(k) => {
                            if other_conflict_keys.contains(k) {
                                return true;
                            }
                        }
                        Read::Range { start, end } => match (start, end) {
                            (Bound::Included(start), Bound::Included(end)) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((
                                        Bound::Included(start),
                                        Bound::Included(end),
                                    ))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Included(start), Bound::Excluded(end)) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((
                                        Bound::Included(start),
                                        Bound::Excluded(end),
                                    ))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Included(start), Bound::Unbounded) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((Bound::Included(start), Bound::Unbounded))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Excluded(start), Bound::Included(end)) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((
                                        Bound::Excluded(start),
                                        Bound::Included(end),
                                    ))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Excluded(start), Bound::Excluded(end)) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((
                                        Bound::Excluded(start),
                                        Bound::Excluded(end),
                                    ))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Excluded(start), Bound::Unbounded) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((Bound::Excluded(start), Bound::Unbounded))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Unbounded, Bound::Included(end)) => {
                                let range = ..=end;
                                for write in other_conflict_keys {
                                    if range.contains(&write) {
                                        return true;
                                    }
                                }
                            }
                            (Bound::Unbounded, Bound::Excluded(end)) => {
                                let range = ..end;
                                for write in other_conflict_keys {
                                    if range.contains(&write) {
                                        return true;
                                    }
                                }
                            }
                            (Bound::Unbounded, Bound::Unbounded) => unreachable!(),
                        },
                        Read::All => {
                            if !other_conflict_keys.is_empty() {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::ConflictManager;

    #[test]
    fn write_overlap_is_per_keyspace() {
        let a = ConflictManager::default();
        a.mark_conflict(1, "k".into());
        let b = ConflictManager::default();
        b.mark_conflict(2, "k".into());
        assert!(!a.has_write_overlap(&b));

        b.mark_conflict(1, "k".into());
        assert!(a.has_write_overlap(&b));
        assert!(b.has_write_overlap(&a));

        let reader = ConflictManager::default();
        reader.mark_read(1, "k".into());
        assert!(!reader.has_write_overlap(&a));
    }
}
