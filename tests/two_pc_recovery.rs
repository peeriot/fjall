mod common;

use common::prepare_put;
use fjall::{Database, KeyspaceCreateOptions, OptimisticTxDatabase, PrepareOutcome, Resolution};
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
