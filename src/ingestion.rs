// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{worker_pool::WorkerMessage, Keyspace};
use lsm_tree::{AnyIngestion, UserKey, UserValue};

pub struct Ingestion<'a> {
    keyspace: &'a Keyspace,
    inner: AnyIngestion<'a>,
}

impl<'a> Ingestion<'a> {
    pub fn new(keyspace: &'a Keyspace) -> crate::Result<Self> {
        let inner = keyspace.tree.ingestion()?;
        Ok(Self { keyspace, inner })
    }

    pub fn write<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        key: K,
        value: V,
    ) -> crate::Result<()> {
        self.inner.write(key, value).map_err(Into::into)
    }

    pub fn write_tombstone<K: Into<UserKey>>(&mut self, key: K) -> crate::Result<()> {
        self.inner.write_tombstone(key).map_err(Into::into)
    }

    #[doc(hidden)]
    pub fn write_weak_tombstone<K: Into<UserKey>>(&mut self, key: K) -> crate::Result<()> {
        self.inner.write_weak_tombstone(key).map_err(Into::into)
    }

    pub fn finish(self) -> crate::Result<()> {
        // NOTE: We hold to avoid a race condition with concurrent writes:
        //
        // write         ingest
        // lock journal
        // |
        // next seqno=1
        // |
        // --------------finish
        //                 flush
        //                 seqno=2
        //                 register
        //                 |
        // -----------------
        // |
        // insert seqno=1
        //
        // The lock is held across the whole of `finish`, so the install is
        // published before any other write can run.
        let journal_lock = self.keyspace.supervisor.journal.get_writer()?;

        self.inner.finish()?;

        // IMPORTANT: Make the ingested data visible to new snapshots before releasing the lock
        self.keyspace
            .supervisor
            .snapshot_tracker
            .publish_installs(&journal_lock);

        self.keyspace
            .worker_messager
            .try_send(WorkerMessage::Compact(self.keyspace.clone()))
            .ok();

        self.keyspace.supervisor.snapshot_tracker.gc();

        drop(journal_lock);

        Ok(())
    }
}
