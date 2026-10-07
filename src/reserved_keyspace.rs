// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! The hidden `__fjall_txn` keyspace that holds prepared (two-phase commit) write-sets.
//!
//! It does not exist until the first non-empty `prepare` creates it; a database that
//! never prepares never gets it.

use crate::{
    file::{fsync_directory, KEYSPACES_FOLDER},
    keyspace::name::TXN_KEYSPACE_NAME,
    Database, Keyspace, KeyspaceCreateOptions,
};

impl Database {
    /// Looks up the reserved `__fjall_txn` keyspace. Never creates it.
    ///
    /// Returns `Ok(None)` if it does not exist, i.e. nothing was ever prepared.
    ///
    /// # Errors
    ///
    /// Returns `Error::Unrecoverable` if the meta keyspace still maps the reserved
    /// name but no keyspace handle exists (its directory was lost), or an error if
    /// the meta keyspace could not be read.
    #[allow(dead_code, reason = "called by WriteTransaction::prepare in a later commit")]
    pub(crate) fn reserved_keyspace(&self) -> crate::Result<Option<Keyspace>> {
        // NOTE: The lost-keyspace check runs under the same lock as the map lookup,
        // so it cannot observe the mapping of a creation that the lookup missed
        #[expect(clippy::expect_used)]
        let keyspaces = self.supervisor.keyspaces.read().expect("lock is poisoned");

        if let Some(handle) = keyspaces.get(TXN_KEYSPACE_NAME) {
            return Ok(Some(handle.clone()));
        }

        self.check_reserved_keyspace_not_lost()?;

        drop(keyspaces);

        Ok(None)
    }

    /// Looks up the reserved `__fjall_txn` keyspace, creating it if it does not exist.
    ///
    /// Only `WriteTransaction::prepare` calls this.
    ///
    /// # Errors
    ///
    /// Returns `Error::Unrecoverable` if the meta keyspace still maps the reserved
    /// name but no keyspace handle exists (nothing is created then), or an error if
    /// an I/O error occurred.
    #[allow(dead_code, reason = "called by WriteTransaction::prepare in a later commit")]
    pub(crate) fn reserved_keyspace_or_create(&self) -> crate::Result<Keyspace> {
        #[expect(clippy::expect_used)]
        let keyspaces = self.supervisor.keyspaces.write().expect("lock is poisoned");

        if let Some(handle) = keyspaces.get(TXN_KEYSPACE_NAME) {
            return Ok(handle.clone());
        }

        self.check_reserved_keyspace_not_lost()?;

        let keyspace_id = self.allocate_keyspace_id()?;

        // NOTE: User compaction filters are never assigned to internal keyspaces
        let handle = Keyspace::create_new(
            keyspace_id,
            self,
            TXN_KEYSPACE_NAME.into(),
            KeyspaceCreateOptions::default(),
        )?;

        // NOTE: Make the keyspace's directory entry durable before its name mapping,
        // so a durable mapping always implies a durable directory
        fsync_directory(self.config.path.join(KEYSPACES_FOLDER))?;

        self.meta_keyspace.create_keyspace(
            keyspace_id,
            TXN_KEYSPACE_NAME,
            handle.clone(),
            keyspaces,
        )?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok(handle)
    }

    /// Fails with `Error::Unrecoverable` if the meta keyspace maps the reserved name.
    ///
    /// Only called when no handle for the reserved keyspace exists, while holding the
    /// supervisor's keyspace lock.
    fn check_reserved_keyspace_not_lost(&self) -> crate::Result<()> {
        if self.meta_keyspace.is_name_mapped(TXN_KEYSPACE_NAME)? {
            log::error!(
                "Keyspace {TXN_KEYSPACE_NAME:?} is mapped in the meta keyspace, but its directory is missing: prepared transactions were lost",
            );
            return Err(crate::Error::Unrecoverable);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db_config::CompactionFilterAssigner, keyspace::name::TXN_KEYSPACE_NAME, Database, Keyspace,
        KeyspaceCreateOptions,
    };
    use lsm_tree::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};
    use std::sync::{Arc, Barrier, Mutex};
    use test_log::test;

    /// Sorted names of the directories under `<db_folder>/keyspaces`.
    fn keyspace_dirs(db_folder: &std::path::Path) -> std::io::Result<Vec<String>> {
        let mut names = std::fs::read_dir(db_folder.join("keyspaces"))?
            .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect::<std::io::Result<Vec<_>>>()?;
        names.sort();
        Ok(names)
    }

    struct DropAll;
    impl CompactionFilter for DropAll {
        fn filter_item(&mut self, _: ItemAccessor<'_>, _: &Context) -> lsm_tree::Result<Verdict> {
            Ok(Verdict::Remove)
        }
    }
    struct DropAllFactory;
    impl Factory for DropAllFactory {
        fn name(&self) -> &str {
            "drop-all"
        }
        fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
            Box::new(DropAll)
        }
    }

    #[test]
    fn reserved_keyspace_lookup_never_creates() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::builder(&folder).open()?;
        assert!(db.reserved_keyspace()?.is_none());
        assert!(db.reserved_keyspace()?.is_none());
        assert_eq!(vec!["0"], keyspace_dirs(folder.path())?);
        Ok(())
    }

    #[test]
    fn reserved_keyspace_is_created_once_and_hidden() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        {
            let db = Database::builder(&folder).open()?;
            let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
            let txn = db.reserved_keyspace_or_create()?;
            assert_eq!(2, txn.id());
            assert_eq!(2, db.reserved_keyspace_or_create()?.id());
            assert_eq!(Some(2), db.reserved_keyspace()?.map(|k| k.id()));

            assert_eq!(1, db.keyspace_count());
            assert_eq!(vec![a.name().clone()], db.list_keyspace_names());
            assert!(!db.keyspace_exists(TXN_KEYSPACE_NAME));
            assert!(db.keyspace_exists("a"));
            assert_eq!(vec!["0", "1", "2"], keyspace_dirs(folder.path())?);
        }

        let db = Database::builder(&folder).open()?;
        assert_eq!(Some(2), db.reserved_keyspace()?.map(|k| k.id()));
        assert_eq!(1, db.keyspace_count());
        assert!(!db.keyspace_exists(TXN_KEYSPACE_NAME));
        assert_eq!(3, db.keyspace("b", KeyspaceCreateOptions::default)?.id());
        Ok(())
    }

    #[test]
    fn concurrent_creation_yields_one_keyspace() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let db = Database::builder(&folder).open()?;
        let barrier = Arc::new(Barrier::new(8));

        let creators: Vec<_> = (0..8)
            .map(|_| {
                let (db, barrier) = (db.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    db.reserved_keyspace_or_create().map(|k| k.id())
                })
            })
            .collect();
        let ids = creators
            .into_iter()
            .map(|h| h.join().expect("creator panicked"))
            .collect::<crate::Result<Vec<_>>>()?;

        assert!(ids.iter().all(|id| *id == 1), "{ids:?}");
        assert_eq!(vec!["0", "1"], keyspace_dirs(folder.path())?);
        Ok(())
    }

    #[test]
    fn lost_reserved_keyspace_is_unrecoverable_and_not_recreated() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        {
            let db = Database::builder(&folder).open()?;
            db.keyspace("a", KeyspaceCreateOptions::default)?
                .insert("k", "v")?;
            assert_eq!(2, db.reserved_keyspace_or_create()?.id());
        }
        std::fs::remove_dir_all(folder.path().join("keyspaces").join("2"))?;

        let db = Database::builder(&folder).open()?;
        assert!(matches!(
            db.reserved_keyspace(),
            Err(crate::Error::Unrecoverable)
        ));
        assert!(matches!(
            db.reserved_keyspace_or_create(),
            Err(crate::Error::Unrecoverable)
        ));
        assert_eq!(vec!["0", "1"], keyspace_dirs(folder.path())?);

        // Ordinary writes keep working
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        a.insert("k2", "v")?;
        assert_eq!(2, a.len()?);
        Ok(())
    }

    /// §9 row 1, first branch: a crash after the tree was created and before
    /// its name mapping was durable.
    #[test]
    fn unmapped_reserved_directory_is_removed_at_recovery() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        {
            let db = Database::builder(&folder).open()?;
            let orphan = Keyspace::create_new(
                1,
                &db,
                TXN_KEYSPACE_NAME.into(),
                KeyspaceCreateOptions::default(),
            )?;
            drop(orphan);
            assert_eq!(vec!["0", "1"], keyspace_dirs(folder.path())?);
        }

        let db = Database::builder(&folder).open()?;
        assert_eq!(vec!["0"], keyspace_dirs(folder.path())?);
        assert!(db.reserved_keyspace()?.is_none());
        db.reserved_keyspace_or_create()?;
        Ok(())
    }

    #[test]
    fn compaction_filter_assigner_never_sees_the_reserved_keyspace() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let assigner: CompactionFilterAssigner = {
            let seen = seen.clone();
            Arc::new(move |name: &str| {
                seen.lock().expect("lock").push(name.to_owned());
                Some(Arc::new(DropAllFactory) as Arc<dyn Factory>)
            })
        };

        {
            let db = Database::builder(&folder)
                .with_compaction_filter_factories(assigner.clone())
                .open()?;
            let txn = db.reserved_keyspace_or_create()?;
            txn.insert("k", "v")?;
            txn.rotate_memtable_and_wait()?;
            txn.major_compact()?;
            assert!(txn.contains_key("k")?);
        }

        // The recovery path must not assign it either
        let db = Database::builder(&folder)
            .with_compaction_filter_factories(assigner)
            .open()?;
        let txn = db.reserved_keyspace()?.expect("recovered");
        txn.insert("k2", "v")?;
        txn.rotate_memtable_and_wait()?;
        txn.major_compact()?;
        assert!(txn.contains_key("k")?);
        assert!(txn.contains_key("k2")?);

        assert!(seen.lock().expect("lock").is_empty(), "{seen:?}");
        Ok(())
    }
}
