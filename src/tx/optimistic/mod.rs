// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod conflict_manager;
mod keyspace;
mod oracle;
pub mod two_pc;
#[cfg(test)]
mod two_pc_test;
mod write_tx;

use crate::{
    batch::{item::Item, WriteBatch},
    keyspace::KeyspaceKey,
    tx::{
        optimistic::{
            conflict_manager::ConflictManager,
            oracle::Oracle,
            two_pc::{
                live_keyspaces_by_id, normalize_batch_error, read_rows, request_compaction,
                xid_prefix, PreparedRow,
            },
        },
        single_writer::Openable,
    },
    Config, Database, Keyspace, KeyspaceCreateOptions, PersistMode, Snapshot,
};
use lsm_tree::{SeqNo, Slice, ValueType};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

pub use keyspace::OptimisticTxKeyspace;
pub use two_pc::{PrepareOutcome, PreparedItemKind, Resolution};
pub use write_tx::{Conflict, UntrackedWriteTransaction, WriteTransaction};

/// Transactional database
#[derive(Clone)]
pub struct OptimisticTxDatabase {
    pub(crate) inner: Database,
    pub(super) oracle: Arc<Oracle>,
}

impl Openable for OptimisticTxDatabase {
    fn open(config: Config) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let inner = Database::create_or_recover(config)?;
        // inner.start_background_threads()?;

        Ok(Self {
            oracle: Arc::new(Oracle {
                write_serialize_lock: Mutex::default(),
                snapshot_tracker: inner.supervisor.snapshot_tracker.clone(),
            }),
            inner,
        })
    }
}

impl OptimisticTxDatabase {
    /// Creates a new database builder to create or open a database at `path`.
    pub fn builder(path: impl AsRef<Path>) -> crate::DatabaseBuilder<Self> {
        crate::DatabaseBuilder::new(path.as_ref())
    }

    /// Allows access to the inner database, allowing to
    /// escape from the transactional context.
    ///
    /// Writes through this handle bypass conflict detection, including the keys
    /// frozen by pending prepares.
    #[doc(hidden)]
    #[must_use]
    pub fn inner(&self) -> &Database {
        &self.inner
    }

    /// Starts a new writeable transaction.
    ///
    /// # Errors
    ///
    /// Will return `Err` if creation failed.
    pub fn write_tx(&self) -> crate::Result<WriteTransaction> {
        let snapshot = {
            // acquire a lock here to prevent getting a stale snapshot seqno
            // this will drain at least part of the commit queue, but ordering
            // is platform-dependent since we use std::sync::Mutex
            let _guard = self.oracle.write_serialize_lock()?;

            self.inner.supervisor.snapshot_tracker.open()
        };

        let mut write_tx =
            WriteTransaction::new(self.inner().clone(), snapshot, self.oracle.clone());

        if !self.inner.config.manual_journal_persist {
            write_tx = write_tx.durability(Some(PersistMode::Buffer));
        }

        Ok(write_tx)
    }

    /// Phase 2 of two-phase commit: applies the write-set prepared under `xid`.
    ///
    /// Idempotent: an unknown or already resolved xid is a [`Resolution::NoOp`].
    /// Never fails with a conflict. Items whose target keyspace was deleted are
    /// dropped and counted in [`Resolution::Applied`].
    ///
    /// The write-set is applied and its prepared record deleted in one atomic,
    /// fsynced batch, so a crash leaves either both or neither. A transaction that
    /// read any of the applied keys before the apply cannot commit afterwards.
    ///
    /// A `NoOp` on the FIRST resolution attempt of a branch that voted `Prepared`
    /// indicates lost prepared state or out-of-band interference: coordinators
    /// should raise an alarm.
    ///
    /// # Errors
    ///
    /// - `Error::Poisoned`: fatal; restart the process, recover, and retry.
    /// - `Error::Io` / `Error::Storage`: transient environment errors; retry.
    /// - `Error::Unrecoverable`: a prepared record cannot be decoded, or the keyspace
    ///   holding prepared write-sets was lost. Permanent; needs operator intervention.
    pub fn commit_prepared(&self, xid: impl AsRef<[u8]>) -> crate::Result<Resolution> {
        let xid = xid.as_ref();

        if self.inner.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        let mut state = self.oracle.write_serialize_lock()?;

        let Some((txn, rows)) = self.rows_to_resolve(xid)? else {
            // NOTE: Defensive; a reservation never outlives its rows
            state.reservations.remove(xid);
            return Ok(Resolution::NoOp);
        };

        // NOTE: The keyspace map's guard is released before the batch commit, which
        // takes that lock itself
        let live = live_keyspaces_by_id(&self.inner);

        let mut dropped_items = 0;

        let mut batch = WriteBatch::new(self.inner.clone()).durability(Some(PersistMode::SyncAll));
        batch.data.reserve_exact(rows.len() * 2);

        for row in &rows {
            if let Some(target) = live.get(&row.keyspace_id) {
                let payload = match row.kind {
                    PreparedItemKind::Put => row.user_value.clone(),
                    PreparedItemKind::Delete | PreparedItemKind::WeakDelete => Slice::empty(),
                };

                batch.data.push(Item::new(
                    target.clone(),
                    row.user_key.clone(),
                    payload,
                    row.kind.value_type(),
                ));
            } else {
                dropped_items += 1;
            }

            batch.data.push(Self::row_tombstone(&txn, row));
        }

        batch
            .commit()
            .map_err(|e| normalize_batch_error(&self.inner, e))?;

        // Register the applied write-set, so transactions that read the old values
        // cannot commit after the apply
        let applied = state.reservations.remove(xid).unwrap_or_else(|| {
            // NOTE: After a restart, prepares are not reserved
            let applied = ConflictManager::default();
            for row in &rows {
                applied.mark_conflict(row.keyspace_id, row.user_key.clone());
            }
            applied
        });

        // IMPORTANT: Sampled after the batch commit has published, so the key is above
        // the snapshot of every transaction that could have read the old values
        state
            .committed_txns
            .insert(self.oracle.snapshot_tracker.get(), applied);

        drop(state);

        request_compaction(&txn);

        Ok(Resolution::Applied { dropped_items })
    }

    /// Phase 2 of two-phase commit: discards the write-set prepared under `xid`.
    ///
    /// Idempotent: an unknown or already resolved xid is a [`Resolution::NoOp`].
    /// The prepared record is deleted in one fsynced batch; the target keyspaces are
    /// never touched.
    ///
    /// A `NoOp` on the FIRST resolution attempt of a branch that voted `Prepared`
    /// indicates lost prepared state or out-of-band interference: coordinators
    /// should raise an alarm.
    ///
    /// # Errors
    ///
    /// - `Error::Poisoned`: fatal; restart the process, recover, and retry.
    /// - `Error::Io` / `Error::Storage`: transient environment errors; retry.
    /// - `Error::Unrecoverable`: a prepared record cannot be decoded, or the keyspace
    ///   holding prepared write-sets was lost. Permanent; needs operator intervention.
    pub fn rollback_prepared(&self, xid: impl AsRef<[u8]>) -> crate::Result<Resolution> {
        let xid = xid.as_ref();

        if self.inner.is_poisoned() {
            return Err(crate::Error::Poisoned);
        }

        let mut state = self.oracle.write_serialize_lock()?;

        let Some((txn, rows)) = self.rows_to_resolve(xid)? else {
            // NOTE: Defensive; a reservation never outlives its rows
            state.reservations.remove(xid);
            return Ok(Resolution::NoOp);
        };

        let mut batch = WriteBatch::new(self.inner.clone()).durability(Some(PersistMode::SyncAll));
        batch.data.reserve_exact(rows.len());

        for row in &rows {
            batch.data.push(Self::row_tombstone(&txn, row));
        }

        batch
            .commit()
            .map_err(|e| normalize_batch_error(&self.inner, e))?;

        // NOTE: Nothing became visible, so nothing is registered as committed
        state.reservations.remove(xid);

        drop(state);

        request_compaction(&txn);

        Ok(Resolution::Applied { dropped_items: 0 })
    }

    /// The reserved keyspace and the rows of `xid`, read at `SeqNo::MAX`.
    ///
    /// `None` if nothing was ever prepared or `xid` has no rows. Never creates the
    /// reserved keyspace.
    fn rows_to_resolve(&self, xid: &[u8]) -> crate::Result<Option<(Keyspace, Vec<PreparedRow>)>> {
        let Some(txn) = self.inner.reserved_keyspace()? else {
            return Ok(None);
        };

        let Some(prefix) = xid_prefix(xid) else {
            return Ok(None);
        };

        // NOTE: Read at SeqNo::MAX under the oracle lock: a concurrent resolution of
        // the same xid has either not started or its tombstones are visible
        let rows = read_rows(&txn, &prefix, SeqNo::MAX)?;

        if rows.is_empty() {
            return Ok(None);
        }

        Ok(Some((txn, rows)))
    }

    /// A regular (never weak) tombstone for a prepared row.
    fn row_tombstone(txn: &Keyspace, row: &PreparedRow) -> Item {
        // NOTE: A weak tombstone could resurrect an older generation of the row
        Item::new(
            txn.clone(),
            row.row_key.clone(),
            Slice::empty(),
            ValueType::Tombstone,
        )
    }

    /// Starts a new read-only transaction (a.k.a. [`Snapshot`]).
    #[must_use]
    pub fn read_tx(&self) -> Snapshot {
        self.inner.snapshot()
    }

    /// Flushes the active journal. The durability depends on the [`PersistMode`]
    /// used.
    ///
    /// Persisting only affects durability, NOT consistency! Even without flushing
    /// data is crash-safe.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{PersistMode, OptimisticTxDatabase, KeyspaceCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// let db = OptimisticTxDatabase::builder(folder).open()?;
    /// let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;
    ///
    /// items.insert("a", "hello")?;
    ///
    /// db.persist(PersistMode::SyncAll)?;
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn persist(&self, mode: PersistMode) -> crate::Result<()> {
        self.inner.persist(mode)
    }

    /// Creates or opens a keyspace.
    ///
    /// If the keyspace does not yet exist, it will be created configured with `create_options`.
    /// Otherwise simply a handle to the existing keyspace will be returned.
    ///
    /// Keyspace names can be up to 255 characters long and can not be empty.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    ///
    /// # Panics
    ///
    /// Panics if the keyspace name is invalid.
    pub fn keyspace(
        &self,
        name: &str,
        create_options: impl FnOnce() -> KeyspaceCreateOptions,
    ) -> crate::Result<OptimisticTxKeyspace> {
        let keyspace = self.inner.keyspace(name, create_options)?;

        Ok(OptimisticTxKeyspace {
            inner: keyspace,
            db: self.clone(),
        })
    }

    /// Returns the number of keyspaces.
    #[must_use]
    pub fn keyspace_count(&self) -> usize {
        self.inner.keyspace_count()
    }

    /// Gets a list of all keyspace names in the database.
    #[must_use]
    pub fn list_keyspace_names(&self) -> Vec<KeyspaceKey> {
        self.inner.list_keyspace_names()
    }

    /// Returns `true` if the keyspace with the given name exists.
    #[must_use]
    pub fn keyspace_exists(&self, name: &str) -> bool {
        self.inner.keyspace_exists(name)
    }

    /// Returns the current write buffer size (active + sealed memtables).
    #[must_use]
    pub fn write_buffer_size(&self) -> u64 {
        self.inner.write_buffer_size()
    }

    /// Returns the number of journal fragments on disk.
    #[must_use]
    pub fn journal_count(&self) -> usize {
        self.inner.journal_count()
    }

    /// Returns the disk space usage of the entire database.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub fn disk_space(&self) -> crate::Result<u64> {
        self.inner.disk_space()
    }
}
