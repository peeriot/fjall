// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::conflict_manager::ConflictManager;
use crate::snapshot_tracker::SnapshotTracker;
use crate::SeqNo;
use lsm_tree::Slice;
use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

pub enum CommitOutcome<E> {
    Ok,
    Aborted(E),
    Conflicted,

    /// The transaction read or wrote a key frozen by a pending prepare
    ReservationConflicted,
}

/// Why a transaction conflicted
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConflictSource {
    /// A transaction that committed after this one's snapshot wrote a key it read
    Ssi,

    /// It read or wrote a key in the write-set of a pending prepare
    Reservation,
}

/// Everything the oracle's lock serializes
#[derive(Default)]
pub(super) struct OracleState {
    /// Conflict keys of committed transactions, by commit seqno
    pub(super) committed_txns: BTreeMap<SeqNo, ConflictManager>,

    /// Write-sets of pending prepares, by xid
    ///
    /// Never garbage collected: an entry lives until its xid is resolved.
    pub(super) reservations: crate::HashMap<Slice, ConflictManager>,
}

impl OracleState {
    /// Checks a transaction whose snapshot is at `instant` for conflicts.
    ///
    /// First the SSI check against the transactions that committed after `instant`,
    /// then, unless there are no reservations, against every pending prepare: the
    /// transaction must neither have read (check 1) nor written (check 2) a key in
    /// its write-set.
    pub(super) fn find_conflict(
        &self,
        instant: SeqNo,
        conflict_checker: &ConflictManager,
    ) -> Option<ConflictSource> {
        // If the committed_txn.ts is less than `SeqNo` that implies that the
        // committed_txn finished before the current transaction started.
        // We don't need to check for conflict in that case.
        // This change assumes linearizability. Lack of linearizability could
        // cause the read ts of a new txn to be lower than the commit ts of
        // a txn before it.
        let conflicted =
            self.committed_txns
                .range((instant + 1)..)
                .any(|(_ts, other_conflict_checker)| {
                    conflict_checker.has_conflict(other_conflict_checker)
                });

        if conflicted {
            return Some(ConflictSource::Ssi);
        }

        // NOTE: Without pending prepares, this is the only cost of 2PC on the commit path
        if self.reservations.is_empty() {
            return None;
        }

        // NOTE: Reservations are not gated by snapshot timestamps: a prepared
        // write-set is logically pending until it is resolved
        let reserved = self.reservations.values().any(|reservation| {
            conflict_checker.has_conflict(reservation)
                || conflict_checker.has_write_overlap(reservation)
        });

        reserved.then_some(ConflictSource::Reservation)
    }
}

pub struct Oracle {
    pub(super) write_serialize_lock: Mutex<OracleState>,
    pub(super) snapshot_tracker: SnapshotTracker,
}

impl Oracle {
    pub(super) fn with_commit<E, F: FnOnce() -> Result<(), E>>(
        &self,
        instant: SeqNo,
        conflict_checker: ConflictManager,
        f: F,
    ) -> crate::Result<CommitOutcome<E>> {
        let mut state = self.write_serialize_lock()?;

        let conflict = state.find_conflict(instant, &conflict_checker);

        // TODO: This can be expensive and should probably be done in a background worker, or a after a memtable rotation
        let safe_to_gc = self.snapshot_tracker.get_seqno_safe_to_gc();
        state.committed_txns.retain(|ts, _| *ts > safe_to_gc);

        match conflict {
            Some(ConflictSource::Ssi) => return Ok(CommitOutcome::Conflicted),
            Some(ConflictSource::Reservation) => return Ok(CommitOutcome::ReservationConflicted),
            None => {}
        }

        if let Err(e) = f() {
            return Ok(CommitOutcome::Aborted(e));
        }

        state
            .committed_txns
            .insert(self.snapshot_tracker.get(), conflict_checker);

        Ok(CommitOutcome::Ok)
    }

    pub(super) fn write_serialize_lock(&self) -> crate::Result<MutexGuard<'_, OracleState>> {
        self.write_serialize_lock
            .lock()
            .map_err(|_| crate::Error::Poisoned)
    }
}

#[cfg(test)]
mod tests {
    use super::ConflictSource;
    use crate::tx::optimistic::conflict_manager::ConflictManager;
    use crate::{
        Conflict, KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable,
        Slice,
    };
    use std::time::{Duration, Instant};

    #[expect(clippy::significant_drop_tightening)]
    fn run_tx(
        db: &OptimisticTxDatabase,
        tree: &OptimisticTxKeyspace,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut tx1 = db.write_tx()?;
        let mut tx2 = db.write_tx()?;
        tx1.insert(tree.inner(), "hello", "world");

        tx1.commit()??;
        assert!(tree.contains_key("hello")?);

        _ = tx2.get(tree.inner(), "hello")?;

        tx2.insert(tree.inner(), "hello", "world2");
        assert!(tx2.commit()?.is_err()); // intended to conflict

        Ok(())
    }

    #[expect(clippy::unwrap_used)]
    #[test]
    fn oracle_committed_txns_does_not_leak() -> crate::Result<()> {
        let tmpdir = tempfile::tempdir()?;
        let db = OptimisticTxDatabase::builder(tmpdir.path()).open()?;

        let part = db.keyspace("foo", KeyspaceCreateOptions::default)?;

        for _ in 0..10_000 {
            run_tx(&db, &part).unwrap();
        }

        assert!(
            dbg!(db
                .oracle
                .write_serialize_lock
                .lock()
                .unwrap()
                .committed_txns
                .len())
                < 10_000
        );

        for _ in 0..10_000 {
            run_tx(&db, &part).unwrap();
        }

        assert!(
            dbg!(db
                .oracle
                .write_serialize_lock
                .lock()
                .unwrap()
                .committed_txns
                .len())
                < 10_000
        );

        Ok(())
    }

    #[test]
    fn committing_transaction_closes_only_its_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;
        let db = OptimisticTxDatabase::builder(tmpdir.path()).open()?;
        let tree = db.keyspace("foo", KeyspaceCreateOptions::default)?;

        let mut tx1 = db.write_tx()?;
        let tx2 = db.write_tx()?;
        assert_eq!(2, db.inner().supervisor.snapshot_tracker.open_snapshots());

        tx1.insert(tree.inner(), "hello", "world");
        tx1.commit()??;
        assert_eq!(1, db.inner().supervisor.snapshot_tracker.open_snapshots());

        drop(tx2);
        assert_eq!(0, db.inner().supervisor.snapshot_tracker.open_snapshots());

        Ok(())
    }

    fn setup() -> crate::Result<(
        tempfile::TempDir,
        OptimisticTxDatabase,
        OptimisticTxKeyspace,
    )> {
        let folder = tempfile::tempdir()?;
        let db = OptimisticTxDatabase::builder(folder.path()).open()?;
        let tree = db.keyspace("foo", KeyspaceCreateOptions::default)?;
        Ok((folder, db, tree))
    }

    fn reserve(
        db: &OptimisticTxDatabase,
        xid: &str,
        keyspace: &OptimisticTxKeyspace,
        keys: &[&str],
    ) -> crate::Result<()> {
        let reservation = ConflictManager::default();
        for key in keys {
            reservation.mark_conflict(keyspace.inner().id(), Slice::from(*key));
        }
        db.oracle
            .write_serialize_lock()?
            .reservations
            .insert(Slice::from(xid), reservation);
        Ok(())
    }

    fn release(db: &OptimisticTxDatabase, xid: &str) -> crate::Result<()> {
        db.oracle
            .write_serialize_lock()?
            .reservations
            .remove(xid.as_bytes());
        Ok(())
    }

    #[test]
    fn reservation_aborts_a_reader_of_a_frozen_key() -> crate::Result<()> {
        let (_folder, db, tree) = setup()?;
        tree.insert("k", "v")?;
        reserve(&db, "p", &tree, &["k"])?;

        let mut tx = db.write_tx()?;
        tx.get(&tree, "k")?;
        tx.insert(&tree, "other", "x");
        assert_eq!(Err(ConflictSource::Reservation), tx.try_commit()?);

        release(&db, "p")?;
        let mut tx = db.write_tx()?;
        tx.get(&tree, "k")?;
        tx.insert(&tree, "other", "x");
        assert_eq!(Ok(()), tx.try_commit()?);
        Ok(())
    }

    #[test]
    fn reservation_aborts_a_blind_writer_of_a_frozen_key() -> crate::Result<()> {
        let (_folder, db, tree) = setup()?;
        reserve(&db, "p", &tree, &["k"])?;

        let mut tx = db.write_tx()?;
        tx.insert(&tree, "k", "x");
        assert!(matches!(tx.commit()?, Err(Conflict)));
        assert_eq!(None, tree.get("k")?);
        Ok(())
    }

    #[test]
    fn ssi_conflict_is_not_a_reservation_conflict() -> crate::Result<()> {
        let (_folder, db, tree) = setup()?;
        reserve(&db, "p", &tree, &["unrelated"])?;

        let mut tx1 = db.write_tx()?;
        tx1.get(&tree, "k")?;
        let mut tx2 = db.write_tx()?;
        tx2.insert(&tree, "k", "2");
        assert_eq!(Ok(()), tx2.try_commit()?);

        tx1.insert(&tree, "k", "1");
        assert_eq!(Err(ConflictSource::Ssi), tx1.try_commit()?);
        Ok(())
    }

    #[test]
    fn reservation_is_per_keyspace() -> crate::Result<()> {
        let (_folder, db, tree) = setup()?;
        let other = db.keyspace("bar", KeyspaceCreateOptions::default)?;
        reserve(&db, "p", &tree, &["k"])?;

        let mut tx = db.write_tx()?;
        tx.get(&other, "k")?;
        tx.insert(&other, "k", "x");
        assert_eq!(Ok(()), tx.try_commit()?);
        other.insert("k", "y")?;
        Ok(())
    }

    #[test]
    fn blind_helpers_return_prepared_conflict_on_a_frozen_key() -> crate::Result<()> {
        let (_folder, db, tree) = setup()?;
        tree.insert("k", "v")?;
        reserve(&db, "p", &tree, &["k"])?;

        assert!(matches!(
            tree.insert("k", "x"),
            Err(crate::Error::PreparedConflict)
        ));
        assert!(matches!(
            tree.remove("k"),
            Err(crate::Error::PreparedConflict)
        ));
        assert!(matches!(
            tree.remove_weak("k"),
            Err(crate::Error::PreparedConflict)
        ));
        assert_eq!(Some("v".as_bytes().into()), tree.get("k")?);
        tree.insert("unfrozen", "x")?;
        Ok(())
    }

    #[test]
    fn looping_helpers_return_prepared_conflict_promptly() -> crate::Result<()> {
        let (_folder, db, tree) = setup()?;
        tree.insert("k", "v")?;
        reserve(&db, "p", &tree, &["k"])?;

        let start = Instant::now();
        assert!(matches!(
            tree.fetch_update("k", |_| Some("w".into())),
            Err(crate::Error::PreparedConflict)
        ));
        assert!(matches!(
            tree.update_fetch("k", |_| Some("w".into())),
            Err(crate::Error::PreparedConflict)
        ));
        assert!(matches!(
            tree.take("k"),
            Err(crate::Error::PreparedConflict)
        ));
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "{:?}",
            start.elapsed()
        );

        release(&db, "p")?;
        assert_eq!(
            Some("v".as_bytes().into()),
            tree.fetch_update("k", |_| Some("w".into()))?
        );
        assert_eq!(
            Some("u".as_bytes().into()),
            tree.update_fetch("k", |_| Some("u".into()))?
        );
        assert_eq!(Some("u".as_bytes().into()), tree.take("k")?);
        Ok(())
    }
}
