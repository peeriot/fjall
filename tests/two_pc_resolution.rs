mod common;

use common::prepare_put;
use fjall::{
    Conflict, KeyspaceCreateOptions, KvSeparationOptions, OptimisticTxDatabase,
    OptimisticTxKeyspace, PrepareOutcome, Readable, Resolution,
};
use std::sync::{Arc, Barrier};
use test_log::test;

const APPLIED: Resolution = Resolution::Applied { dropped_items: 0 };

fn setup(
    folder: &tempfile::TempDir,
) -> fjall::Result<(OptimisticTxDatabase, OptimisticTxKeyspace)> {
    let db = OptimisticTxDatabase::builder(folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    Ok((db, a))
}

#[test]
fn commit_prepared_makes_the_write_set_visible_everywhere() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("old", "v")?;
    let mut tx = db.write_tx()?;
    tx.insert(&a, "new", "v");
    tx.remove(&a, "old");
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));
    let before = db.read_tx();

    assert_eq!(APPLIED, db.commit_prepared("x")?);

    assert_eq!(Some("v".as_bytes().into()), a.get("new")?);
    assert_eq!(None, a.get("old")?);
    assert_eq!(1, a.inner().iter().count());
    let after = db.read_tx();
    assert_eq!(Some("v".as_bytes().into()), after.get(&a, "new")?);
    assert_eq!(None, after.get(&a, "old")?);
    assert!(db.write_tx()?.untrack().contains_key(&a, "new")?);
    // An older snapshot does not change
    assert_eq!(None, before.get(&a, "new")?);
    assert_eq!(Some("v".as_bytes().into()), before.get(&a, "old")?);
    Ok(())
}

#[test]
fn rollback_prepared_discards_the_write_set() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("old", "v")?;
    let mut tx = db.write_tx()?;
    tx.insert(&a, "new", "v");
    tx.remove(&a, "old");
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));

    assert_eq!(APPLIED, db.rollback_prepared("x")?);
    assert_eq!(None, a.get("new")?);
    assert_eq!(Some("v".as_bytes().into()), a.get("old")?);
    assert_eq!(None, db.read_tx().get(&a, "new")?);
    Ok(())
}

#[test]
fn resolution_is_idempotent() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    assert!(matches!(
        prepare_put(&db, &a, "c", "k1", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    assert!(matches!(
        prepare_put(&db, &a, "r", "k2", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));

    assert_eq!(APPLIED, db.commit_prepared("c")?);
    assert_eq!(Resolution::NoOp, db.commit_prepared("c")?);
    assert_eq!(Resolution::NoOp, db.rollback_prepared("c")?);
    assert_eq!(APPLIED, db.rollback_prepared("r")?);
    assert_eq!(Resolution::NoOp, db.rollback_prepared("r")?);
    assert_eq!(Resolution::NoOp, db.commit_prepared("r")?);

    assert_eq!(Some("v".as_bytes().into()), a.get("k1")?);
    assert_eq!(None, a.get("k2")?);
    Ok(())
}

#[test]
fn concurrent_resolutions_of_one_xid_apply_once() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    assert!(matches!(
        prepare_put(&db, &a, "x", "k", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    let barrier = Arc::new(Barrier::new(2));

    let resolvers: Vec<_> = (0..2)
        .map(|_| {
            let (db, barrier) = (db.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                db.commit_prepared("x")
            })
        })
        .collect();
    let mut outcomes = resolvers
        .into_iter()
        .map(|h| h.join().expect("resolver panicked"))
        .collect::<fjall::Result<Vec<_>>>()?;
    outcomes.sort_by_key(|o| *o == Resolution::NoOp);
    assert_eq!(vec![APPLIED, Resolution::NoOp], outcomes);
    Ok(())
}

#[test]
fn unknown_and_oversize_xids_are_no_ops() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    assert!(matches!(
        prepare_put(&db, &a, "x", "k", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    assert_eq!(Resolution::NoOp, db.commit_prepared("nope")?);
    assert_eq!(Resolution::NoOp, db.rollback_prepared(vec![0u8; 300])?);
    assert_eq!(Resolution::NoOp, db.commit_prepared(vec![0u8; 70_000])?);
    assert_eq!(Resolution::NoOp, db.commit_prepared("")?);
    Ok(())
}

#[test]
fn read_only_prepare_needs_no_phase_two() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    assert!(matches!(
        prepare_put(&db, &a, "x", "k", "v")?,
        Ok(PrepareOutcome::Prepared)
    )); // __fjall_txn exists
    assert!(matches!(
        db.write_tx()?.prepare("ro")?,
        Ok(PrepareOutcome::ReadOnly)
    ));
    assert_eq!(Resolution::NoOp, db.commit_prepared("ro")?);
    Ok(())
}

#[test]
fn resolution_unfreezes_the_write_set() -> fjall::Result<()> {
    // (c)
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    assert!(matches!(
        prepare_put(&db, &a, "r", "k", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    assert!(matches!(
        prepare_put(&db, &a, "c", "j", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    assert_eq!(APPLIED, db.rollback_prepared("r")?);
    assert_eq!(APPLIED, db.commit_prepared("c")?);

    for key in ["k", "j"] {
        let mut t = db.write_tx()?;
        t.get(&a, key)?;
        t.insert(&a, key, "t");
        assert!(t.commit()?.is_ok(), "{key}");
        a.insert(key, "blind")?;
    }
    Ok(())
}

#[test]
fn reader_from_before_the_apply_conflicts() -> fjall::Result<()> {
    // (d), same run
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("k", "old")?;
    assert!(matches!(
        prepare_put(&db, &a, "p", "k", "new")?,
        Ok(PrepareOutcome::Prepared)
    ));

    let mut t = db.write_tx()?;
    assert_eq!(Some("old".as_bytes().into()), t.get(&a, "k")?);
    assert_eq!(APPLIED, db.commit_prepared("p")?);
    t.insert(&a, "elsewhere", "x");
    assert!(matches!(t.commit()?, Err(Conflict)));

    // A transaction that starts after the apply sees it and commits
    let mut u = db.write_tx()?;
    assert_eq!(Some("new".as_bytes().into()), u.get(&a, "k")?);
    u.insert(&a, "elsewhere", "y");
    assert!(u.commit()?.is_ok());
    Ok(())
}

#[test]
fn resurrected_prepare_registers_its_write_set_at_apply() -> fjall::Result<()> {
    // (d), after restart
    let folder = tempfile::tempdir()?;
    {
        let (db, a) = setup(&folder)?;
        a.insert("k", "old")?;
        assert!(matches!(
            prepare_put(&db, &a, "p", "k", "new")?,
            Ok(PrepareOutcome::Prepared)
        ));
    }
    let (db, a) = setup(&folder)?;
    let mut t = db.write_tx()?;
    assert_eq!(Some("old".as_bytes().into()), t.get(&a, "k")?);
    assert_eq!(APPLIED, db.commit_prepared("p")?);
    t.insert(&a, "elsewhere", "x");
    assert!(matches!(t.commit()?, Err(Conflict)));
    Ok(())
}

#[test]
fn items_of_a_deleted_target_keyspace_are_dropped() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let [a, b, c] = ["a", "b", "c"].map(|n| {
        db.keyspace(n, KeyspaceCreateOptions::default)
            .expect("keyspace")
    });
    let mut tx = db.write_tx()?;
    for ks in [&a, &b, &c] {
        tx.insert(ks, "k1", "v");
        tx.insert(ks, "k2", "v");
    }
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));
    db.inner().delete_keyspace(b.inner().clone())?;

    assert_eq!(
        Resolution::Applied { dropped_items: 2 },
        db.commit_prepared("x")?
    );
    for ks in [&a, &c] {
        assert_eq!(Some("v".as_bytes().into()), ks.get("k1")?);
        assert_eq!(Some("v".as_bytes().into()), ks.get("k2")?);
    }
    // Every row, b's included, was tombstoned
    assert_eq!(Resolution::NoOp, db.commit_prepared("x")?);
    Ok(())
}

#[test]
fn rollback_with_a_deleted_target_keyspace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
    let mut tx = db.write_tx()?;
    tx.insert(&a, "k", "v");
    tx.insert(&b, "k", "v");
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));
    db.inner().delete_keyspace(b.inner().clone())?;

    assert_eq!(APPLIED, db.rollback_prepared("x")?);
    assert_eq!(Resolution::NoOp, db.rollback_prepared("x")?);
    assert_eq!(None, a.get("k")?);
    Ok(())
}

#[test]
fn last_write_per_key_is_what_gets_applied() -> fjall::Result<()> {
    // Review Focus 3
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("k", "old")?;
    let mut tx = db.write_tx()?;
    tx.insert(&a, "k", "1");
    tx.insert(&a, "k", "2");
    tx.remove(&a, "k");
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));
    assert_eq!(APPLIED, db.commit_prepared("x")?);
    assert_eq!(None, a.get("k")?);
    Ok(())
}

#[test]
fn kv_separated_target_survives_restart() -> fjall::Result<()> {
    // Review Focus 1
    let folder = tempfile::tempdir()?;
    let big = "v".repeat(10_000); // above the 4 KiB journal compression threshold
    let open = |folder: &tempfile::TempDir| -> fjall::Result<(OptimisticTxDatabase, OptimisticTxKeyspace)> {
        let db = OptimisticTxDatabase::builder(folder).open()?;
        let blobs = db.keyspace("blobs", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        })?;
        Ok((db, blobs))
    };
    {
        let (db, blobs) = open(&folder)?;
        assert!(blobs.inner().is_kv_separated());
        assert!(matches!(
            prepare_put(&db, &blobs, "x", "k", &big)?,
            Ok(PrepareOutcome::Prepared)
        ));
    }
    {
        let (db, blobs) = open(&folder)?;
        assert_eq!(APPLIED, db.commit_prepared("x")?);
        assert_eq!(Some(big.as_bytes().into()), blobs.get("k")?);
        blobs.inner().rotate_memtable_and_wait()?;
        assert_eq!(Some(big.as_bytes().into()), blobs.get("k")?);
    }
    let (_db, blobs) = open(&folder)?;
    assert_eq!(Some(big.as_bytes().into()), blobs.get("k")?);
    Ok(())
}
