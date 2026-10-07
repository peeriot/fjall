mod common;

use common::{keyspace_dirs, prepare_put};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, PrepareOutcome};
use test_log::test;

#[test]
fn lost_reserved_keyspace_fails_every_2pc_call() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    {
        let db = OptimisticTxDatabase::builder(&folder).open()?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        assert!(matches!(
            prepare_put(&db, &a, "x", "k", "v")?,
            Ok(PrepareOutcome::Prepared)
        ));
    }
    std::fs::remove_dir_all(folder.path().join("keyspaces").join("2"))?;

    // Create no keyspace before the 2PC calls: it would be handed id 2.
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    assert!(matches!(
        prepare_put(&db, &a, "y", "k", "v"),
        Err(fjall::Error::Unrecoverable)
    ));
    assert!(matches!(
        db.write_tx()?.prepare("z"),
        Err(fjall::Error::Unrecoverable)
    ));
    // Tasks 6 and 7 add the resolution and enumeration calls here.
    assert!(matches!(
        db.commit_prepared("x"),
        Err(fjall::Error::Unrecoverable)
    ));
    assert!(matches!(
        db.rollback_prepared("x"),
        Err(fjall::Error::Unrecoverable)
    ));
    assert!(matches!(
        db.pending_prepared(),
        Err(fjall::Error::Unrecoverable)
    ));
    assert!(matches!(
        db.prepared_items("x"),
        Err(fjall::Error::Unrecoverable)
    ));

    assert_eq!(vec!["0", "1"], keyspace_dirs(folder.path())?);
    let mut tx = db.write_tx()?;
    tx.insert(&a, "k", "v");
    assert!(tx.commit()?.is_ok());
    Ok(())
}
