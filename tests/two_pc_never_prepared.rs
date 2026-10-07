mod common;

use common::keyspace_dirs;
use fjall::{
    KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, PrepareOutcome, Resolution,
};
use test_log::test;

fn sorted_names(db: &OptimisticTxDatabase) -> Vec<String> {
    let mut names: Vec<String> = db
        .list_keyspace_names()
        .iter()
        .map(ToString::to_string)
        .collect();
    names.sort();
    names
}

/// Everything a database can do without a non-empty prepare.
fn exercise(
    db: &OptimisticTxDatabase,
    a: &OptimisticTxKeyspace,
    b: &OptimisticTxKeyspace,
) -> fjall::Result<()> {
    let mut tx = db.write_tx()?;
    tx.insert(a, "k", "v");
    tx.remove(b, "gone");
    assert!(tx.commit()?.is_ok());

    b.insert("x", "1")?;
    b.remove("x")?;
    b.remove_weak("y")?;
    a.fetch_update("k", |_| Some("w".into()))?;
    a.update_fetch("k", |_| Some("u".into()))?;
    a.take("k")?;

    assert!(matches!(
        db.write_tx()?.prepare("ro")?,
        Ok(PrepareOutcome::ReadOnly)
    ));
    assert_eq!(Resolution::NoOp, db.commit_prepared("ro")?);
    assert_eq!(Resolution::NoOp, db.rollback_prepared("nope")?);
    Ok(())
}

#[test]
fn never_prepared_database_is_unchanged() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = OptimisticTxDatabase::builder(&folder).open()?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
        assert_eq!((1, 2), (a.inner().id(), b.inner().id()));

        exercise(&db, &a, &b)?;

        assert_eq!(vec!["0", "1", "2"], keyspace_dirs(folder.path())?);
        assert_eq!(2, db.keyspace_count());
        assert_eq!(vec!["a", "b"], sorted_names(&db));
    }

    let db = OptimisticTxDatabase::builder(&folder).open()?;
    assert_eq!(vec!["0", "1", "2"], keyspace_dirs(folder.path())?);
    assert_eq!(2, db.keyspace_count());
    assert_eq!(vec!["a", "b"], sorted_names(&db));

    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
    exercise(&db, &a, &b)?;

    let c = db.keyspace("c", KeyspaceCreateOptions::default)?;
    assert_eq!(3, c.inner().id());
    assert_eq!(vec!["0", "1", "2", "3"], keyspace_dirs(folder.path())?);

    Ok(())
}

#[test]
#[should_panic(expected = "is_valid_keyspace_name")]
fn reserved_keyspace_name_panics() {
    let folder = tempfile::tempdir().expect("tempdir");
    let db = OptimisticTxDatabase::builder(&folder).open().expect("open");
    let _ = db.keyspace("__fjall_txn", KeyspaceCreateOptions::default);
}

#[test]
fn first_non_empty_prepare_creates_a_hidden_keyspace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    {
        let db = OptimisticTxDatabase::builder(&folder).open()?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        assert!(matches!(
            common::prepare_put(&db, &a, "x", "k", "v")?,
            Ok(PrepareOutcome::Prepared)
        ));
        assert_eq!(vec!["0", "1", "2"], keyspace_dirs(folder.path())?);
        assert_eq!(1, db.keyspace_count());
        assert_eq!(vec!["a"], sorted_names(&db));
        assert!(!db.keyspace_exists("__fjall_txn"));
    }

    let db = OptimisticTxDatabase::builder(&folder).open()?;
    assert_eq!(vec!["0", "1", "2"], keyspace_dirs(folder.path())?);
    assert_eq!(1, db.keyspace_count());
    assert_eq!(vec!["a"], sorted_names(&db));
    assert!(!db.keyspace_exists("__fjall_txn"));
    assert_eq!(
        3,
        db.keyspace("b", KeyspaceCreateOptions::default)?
            .inner()
            .id()
    );
    Ok(())
}
