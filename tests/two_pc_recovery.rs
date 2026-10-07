mod common;

use common::prepare_put;
use fjall::{
    Database, KeyspaceCreateOptions, OptimisticTxDatabase, PrepareOutcome, PreparedTxInfo,
    Resolution,
};
use test_log::test;

/// a = 1, __fjall_txn = 2 (lower than what follows), b = 3. "second" writes
/// only to b, which has no journaled writes of its own; then b is deleted
/// and its directory removed. Returns b's id.
fn prepare_into_a_deleted_keyspace(folder: &tempfile::TempDir) -> fjall::Result<u64> {
    let db = OptimisticTxDatabase::builder(folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    assert!(matches!(
        prepare_put(&db, &a, "first", "k", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("first")?
    );

    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
    let b_id = b.inner().id();
    assert_eq!(3, b_id, "premise: a = 1, __fjall_txn = 2");
    let mut tx = db.write_tx()?;
    tx.insert(&b, "k1", "v");
    tx.insert(&b, "k2", "v");
    assert!(matches!(
        tx.prepare("second")?,
        Ok(PrepareOutcome::Prepared)
    ));

    db.inner().delete_keyspace(b.inner().clone())?;
    drop(b);
    assert!(
        !folder
            .path()
            .join("keyspaces")
            .join(b_id.to_string())
            .try_exists()?,
        "premise: b's directory is gone"
    );
    Ok(b_id)
}

#[test]
fn prepared_target_keyspace_id_is_not_recycled() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let b_id = prepare_into_a_deleted_keyspace(&folder)?;

    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let c = db.keyspace("c", KeyspaceCreateOptions::default)?;
    assert_ne!(b_id, c.inner().id());
    assert_eq!(
        Resolution::Applied { dropped_items: 2 },
        db.commit_prepared("second")?
    );
    assert!(c.inner().is_empty()?);
    Ok(())
}

#[test]
fn plain_database_does_not_recycle_a_prepared_target_id() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let b_id = prepare_into_a_deleted_keyspace(&folder)?;
    {
        let db = Database::builder(&folder).open()?;
        assert_ne!(b_id, db.keyspace("c", KeyspaceCreateOptions::default)?.id());
    }
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    assert_eq!(
        Resolution::Applied { dropped_items: 2 },
        db.commit_prepared("second")?
    );
    Ok(())
}

fn info(xid: &str) -> PreparedTxInfo {
    PreparedTxInfo {
        xid: xid.into(),
        item_count: 1,
        keyspaces: vec!["a".into()],
        missing_keyspace_ids: vec![],
    }
}

fn setup(
    folder: &tempfile::TempDir,
) -> fjall::Result<(OptimisticTxDatabase, fjall::OptimisticTxKeyspace)> {
    let db = OptimisticTxDatabase::builder(folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    Ok((db, a))
}

#[test]
fn prepared_transaction_survives_restart_and_commits_once() -> fjall::Result<()> {
    // §9 rows 4, 6
    let folder = tempfile::tempdir()?;
    {
        let (db, a) = setup(&folder)?;
        assert!(matches!(
            prepare_put(&db, &a, "x", "k", "v")?,
            Ok(PrepareOutcome::Prepared)
        ));
    }
    {
        let (db, a) = setup(&folder)?;
        assert_eq!(vec![info("x")], db.pending_prepared()?);
        assert_eq!(None, a.get("k")?);
        assert_eq!(
            Resolution::Applied { dropped_items: 0 },
            db.commit_prepared("x")?
        );
    }
    let (db, a) = setup(&folder)?;
    assert_eq!(Some("v".as_bytes().into()), a.get("k")?);
    assert!(db.pending_prepared()?.is_empty());
    assert_eq!(Resolution::NoOp, db.commit_prepared("x")?);
    Ok(())
}

#[test]
fn rolled_back_prepare_stays_rolled_back_after_restart() -> fjall::Result<()> {
    // §9 row 7
    let folder = tempfile::tempdir()?;
    {
        let (db, a) = setup(&folder)?;
        assert!(matches!(
            prepare_put(&db, &a, "x", "k", "v")?,
            Ok(PrepareOutcome::Prepared)
        ));
    }
    {
        let (db, _a) = setup(&folder)?;
        assert_eq!(vec![info("x")], db.pending_prepared()?);
        assert_eq!(
            Resolution::Applied { dropped_items: 0 },
            db.rollback_prepared("x")?
        );
    }
    let (db, a) = setup(&folder)?;
    assert_eq!(None, a.get("k")?);
    assert!(db.pending_prepared()?.is_empty());
    assert_eq!(Resolution::NoOp, db.rollback_prepared("x")?);
    assert_eq!(Resolution::NoOp, db.commit_prepared("x")?);
    Ok(())
}

#[test]
fn prepare_reaches_the_journal_without_any_persist_call() -> fjall::Result<()> {
    // D21, second half
    let folder = tempfile::tempdir()?;
    let crash_image = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(&folder)
        .manual_journal_persist(true)
        .open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let mut tx = db.write_tx()?.durability(None);
    tx.insert(&a, "k", "v");
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));

    // What a crash right now would leave behind: no persist call, no drop
    common::copy_dir(folder.path(), crash_image.path())?;
    let recovered = OptimisticTxDatabase::builder(&crash_image).open()?;
    assert_eq!(vec![info("x")], recovered.pending_prepared()?);
    Ok(())
}

#[test]
fn resurrected_prepare_is_not_refrozen() -> fjall::Result<()> {
    // §3 resolve-before-traffic, documented failure
    let folder = tempfile::tempdir()?;
    {
        let (db, a) = setup(&folder)?;
        assert!(matches!(
            prepare_put(&db, &a, "p", "k", "from-p")?,
            Ok(PrepareOutcome::Prepared)
        ));
    }
    let (db, a) = setup(&folder)?;
    // Admitted before resolution: nothing freezes "k" after a restart
    a.insert("k", "from-t")?;
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("p")?
    );
    // ...and the later apply clobbers it: the lost update the spec warns about
    assert_eq!(Some("from-p".as_bytes().into()), a.get("k")?);
    Ok(())
}

#[test]
fn plain_database_open_keeps_prepared_rows_inert_and_hidden() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    {
        let (db, a) = setup(&folder)?;
        assert!(matches!(
            prepare_put(&db, &a, "x", "k", "v")?,
            Ok(PrepareOutcome::Prepared)
        ));
    }
    {
        let db = Database::builder(&folder).open()?;
        assert_eq!(1, db.keyspace_count());
        assert!(!db.keyspace_exists("__fjall_txn"));
        let names: Vec<String> = db
            .list_keyspace_names()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(vec!["a"], names);
        assert_eq!(vec!["0", "1", "2"], common::keyspace_dirs(folder.path())?);
        assert_eq!(
            None,
            db.keyspace("a", KeyspaceCreateOptions::default)?.get("k")?
        );
    }
    let (db, a) = setup(&folder)?;
    assert_eq!(vec![info("x")], db.pending_prepared()?);
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("x")?
    );
    assert_eq!(Some("v".as_bytes().into()), a.get("k")?);
    Ok(())
}
