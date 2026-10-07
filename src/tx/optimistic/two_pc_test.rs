// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Crate-internal tests of two-phase commit.

use crate::{
    tx::optimistic::{conflict_manager::ConflictManager, two_pc::*},
    KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, PrepareOutcome, Readable,
    Slice,
};
use lsm_tree::SeqNo;
use test_log::test;

fn open(folder: &tempfile::TempDir) -> crate::Result<OptimisticTxDatabase> {
    OptimisticTxDatabase::builder(folder.path()).open()
}

fn keyspace(db: &OptimisticTxDatabase, name: &str) -> crate::Result<OptimisticTxKeyspace> {
    db.keyspace(name, KeyspaceCreateOptions::default)
}

/// Every `__fjall_txn` row, or none if the keyspace does not exist.
fn rows(db: &OptimisticTxDatabase) -> crate::Result<Vec<PreparedRow>> {
    match db.inner().reserved_keyspace()? {
        Some(txn) => read_rows(&txn, &[], SeqNo::MAX),
        None => Ok(Vec::new()),
    }
}

/// Prepares one insert under `xid` and asserts that it was prepared.
fn prepare_one(
    db: &OptimisticTxDatabase,
    ks: &OptimisticTxKeyspace,
    xid: &str,
    key: &str,
    value: &str,
) -> crate::Result<()> {
    let mut tx = db.write_tx()?;
    tx.insert(ks, key, value);
    assert!(matches!(tx.prepare(xid)?, Ok(PrepareOutcome::Prepared)));
    Ok(())
}

/// Whether the oracle holds a reservation for `xid`.
#[allow(dead_code, reason = "used by a later commit")]
fn is_reserved(db: &OptimisticTxDatabase, xid: &str) -> crate::Result<bool> {
    Ok(db
        .oracle
        .write_serialize_lock()?
        .reservations
        .contains_key(xid.as_bytes()))
}

#[test]
fn prepare_records_the_last_write_per_key() -> crate::Result<()> {
    // Review Focus 3
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    let b = keyspace(&db, "b")?;

    let mut tx = db.write_tx()?;
    tx.insert(&a, "k1", "first");
    tx.insert(&a, "k1", "last");
    tx.insert(&a, "k2", "x");
    tx.remove(&a, "k2");
    tx.remove_weak(&b, "k3");
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));

    let rows = rows(&db)?;
    let summary: Vec<_> = rows
        .iter()
        .map(|r| (&*r.xid, r.keyspace_id, &*r.user_key, r.kind, &*r.user_value))
        .collect();
    assert_eq!(
        vec![
            (
                &b"x"[..],
                1,
                &b"k1"[..],
                PreparedItemKind::Put,
                &b"last"[..]
            ),
            (&b"x"[..], 1, &b"k2"[..], PreparedItemKind::Delete, &b""[..]),
            (
                &b"x"[..],
                2,
                &b"k3"[..],
                PreparedItemKind::WeakDelete,
                &b""[..]
            ),
        ],
        summary,
    );

    // The reservation is exactly the persisted keys
    let frozen = |keyspace_id, key: &str| -> crate::Result<bool> {
        let probe = ConflictManager::default();
        probe.mark_conflict(keyspace_id, key.into());
        let state = db.oracle.write_serialize_lock()?;
        Ok(probe.has_write_overlap(state.reservations.get(b"x".as_slice()).expect("reserved")))
    };
    assert!(frozen(1, "k1")? && frozen(1, "k2")? && frozen(2, "k3")?);
    assert!(!frozen(1, "k3")? && !frozen(2, "k1")?);
    Ok(())
}

#[test]
fn prepare_releases_its_snapshot_on_every_path() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    let open_snapshots = || db.inner().supervisor.snapshot_tracker.open_snapshots();

    prepare_one(&db, &a, "x", "k", "v")?;
    assert_eq!(0, open_snapshots(), "Prepared");

    let mut tx = db.write_tx()?;
    tx.insert(&a, "k2", "v");
    assert!(matches!(tx.prepare("x"), Err(crate::Error::XidInUse)));
    assert_eq!(0, open_snapshots(), "XidInUse");

    let mut tx = db.write_tx()?;
    tx.get(&a, "r")?;
    tx.insert(&a, "k3", "v");
    a.insert("r", "changed")?;
    assert!(matches!(tx.prepare("y")?, Err(crate::Conflict)));
    assert_eq!(0, open_snapshots(), "Conflict");

    let mut tx = db.write_tx()?;
    tx.insert(&a, vec![b'k'; 65_525], "v");
    assert!(matches!(
        tx.prepare("z"),
        Err(crate::Error::PreparedItemTooLarge)
    ));
    assert_eq!(0, open_snapshots(), "PreparedItemTooLarge");

    assert!(matches!(
        db.write_tx()?.prepare("ro")?,
        Ok(PrepareOutcome::ReadOnly)
    ));
    assert_eq!(0, open_snapshots(), "ReadOnly");
    Ok(())
}

#[cfg(target_pointer_width = "64")]
#[test]
#[ignore = "allocates about 4 GiB of virtual memory"]
fn oversize_value_is_rejected_before_anything_is_written() -> crate::Result<()> {
    // D20
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;

    // 4 GiB, zero-allocated: no page is ever touched
    let huge = Slice::from(byteview::ByteView::with_size(4_294_967_294));
    let mut tx = db.write_tx()?;
    tx.insert(&a, "k", huge);
    assert!(matches!(
        tx.prepare("x"),
        Err(crate::Error::PreparedItemTooLarge)
    ));
    assert!(
        db.inner().reserved_keyspace()?.is_none(),
        "step 0b precedes 0c"
    );

    // The oracle mutex is not poisoned
    let mut tx = db.write_tx()?;
    tx.insert(&a, "k", "v");
    assert!(tx.commit()?.is_ok());
    Ok(())
}

#[test]
fn poisoned_database_refuses_every_2pc_call() -> crate::Result<()> {
    // Tasks 6 and 7 extend it
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    prepare_one(&db, &a, "x", "k", "v")?;
    db.inner().is_poisoned.poison();

    let mut tx = db.write_tx()?;
    tx.insert(&a, "k2", "v");
    assert!(matches!(tx.prepare("y"), Err(crate::Error::Poisoned)));
    assert!(matches!(
        db.write_tx()?.prepare("ro"),
        Err(crate::Error::Poisoned)
    ));
    assert!(
        matches!(db.write_tx()?.prepare("x"), Err(crate::Error::Poisoned)),
        "before the duplicate check"
    );
    assert_eq!(1, rows(&db)?.len());
    Ok(())
}

#[test]
fn poisoned_database_creates_nothing() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    db.inner().is_poisoned.poison();

    let mut tx = db.write_tx()?;
    tx.insert(&a, "k", "v");
    assert!(matches!(tx.prepare("x"), Err(crate::Error::Poisoned)));
    assert!(db.inner().reserved_keyspace()?.is_none());
    Ok(())
}
