// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Crate-internal tests of two-phase commit.

use crate::{
    tx::optimistic::{conflict_manager::ConflictManager, two_pc::*},
    KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, PrepareOutcome, Readable,
    Resolution, Slice,
};
use lsm_tree::{
    compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict},
    AbstractTree, SeqNo,
};
use std::{fs::OpenOptions, sync::Arc};
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
    assert!(matches!(
        db.commit_prepared("x"),
        Err(crate::Error::Poisoned)
    ));
    assert!(matches!(
        db.rollback_prepared("x"),
        Err(crate::Error::Poisoned)
    ));
    assert!(
        matches!(db.commit_prepared("unknown"), Err(crate::Error::Poisoned)),
        "no false NoOp"
    );
    assert!(matches!(
        db.rollback_prepared("unknown"),
        Err(crate::Error::Poisoned)
    ));
    assert_eq!(None, a.get("k")?);
    assert!(matches!(db.pending_prepared(), Err(crate::Error::Poisoned)));
    assert!(matches!(
        db.prepared_items("x"),
        Err(crate::Error::Poisoned)
    ));
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

/// Number of the `futex` system call.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const FUTEX_SYSCALL: &str = "202";
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
const FUTEX_SYSCALL: &str = "98";

/// The `/proc` directory of the calling thread.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn thread_proc_dir() -> std::io::Result<std::path::PathBuf> {
    Ok(std::path::Path::new("/proc").join(std::fs::read_link("/proc/thread-self")?))
}

/// Waits until the thread with the `/proc` directory `dir` has slept in a futex
/// wait (blocked on a lock) for several polls in a row.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn wait_until_blocked(dir: &std::path::Path) -> std::io::Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut streak = 0;
    while streak < 5 {
        assert!(
            std::time::Instant::now() < deadline,
            "{dir:?} never blocked"
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
        let stat = std::fs::read_to_string(dir.join("stat"))?;
        let syscall = std::fs::read_to_string(dir.join("syscall"))?;
        // NOTE: The state follows the parenthesized thread name
        let sleeping = stat
            .rsplit_once(") ")
            .is_some_and(|(_, rest)| rest.starts_with('S'));
        let in_futex = syscall.split_whitespace().next() == Some(FUTEX_SYSCALL);
        streak = if sleeping && in_futex { streak + 1 } else { 0 };
    }
    Ok(())
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[test]
fn poison_while_waiting_for_the_oracle_lock_is_not_missed() -> crate::Result<()> {
    // §4 poisoning discipline: the flag is checked again under the oracle lock
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let _a = keyspace(&db, "a")?;
    let read_only = db.write_tx()?;

    let guard = db.oracle.write_serialize_lock()?;
    let (sender, receiver) = std::sync::mpsc::channel();
    let spawn = |call: Box<dyn FnOnce() -> crate::Result<bool> + Send>| {
        let sender = sender.clone();
        std::thread::spawn(move || {
            sender.send(thread_proc_dir()).expect("receiver alive");
            call()
        })
    };
    let rollback = {
        let db = db.clone();
        spawn(Box::new(move || {
            db.rollback_prepared("unknown")
                .map(|r| r == Resolution::NoOp)
        }))
    };
    let commit = {
        let db = db.clone();
        spawn(Box::new(move || {
            db.commit_prepared("unknown").map(|r| r == Resolution::NoOp)
        }))
    };
    let prepare = spawn(Box::new(move || {
        read_only
            .prepare("ro")
            .map(|r| matches!(r, Ok(PrepareOutcome::ReadOnly)))
    }));
    for _ in 0..3 {
        wait_until_blocked(&receiver.recv().expect("sent")?)?;
    }

    db.inner().is_poisoned.poison(); // as by a 2PC batch that failed meanwhile
    drop(guard);

    for (name, call) in [
        ("rollback_prepared", rollback),
        ("commit_prepared", commit),
        ("read-only prepare", prepare),
    ] {
        let outcome = call.join().expect("no panic");
        assert!(
            matches!(outcome, Err(crate::Error::Poisoned)),
            "{name}: {outcome:?}"
        );
    }
    Ok(())
}

#[test]
fn undecodable_rows_are_unrecoverable() -> crate::Result<()> {
    let cases: [(Slice, Slice); 3] = [
        (encode_row_key(b"x", 1, b"k"), Slice::from(&[9u8, 0][..])), // unknown format_version
        (encode_row_key(b"x", 1, b"k"), Slice::from(&[1u8, 7][..])), // unknown value_type
        (
            Slice::from(&[0u8, 1, b'x', 0, 0][..]),
            encode_row_value(PreparedItemKind::Put, b"v"),
        ), // key too short
    ];
    for (key, value) in cases {
        let folder = tempfile::tempdir()?;
        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        let txn = db.inner().reserved_keyspace_or_create()?;
        txn.insert(key, value)?;

        assert!(matches!(
            db.commit_prepared("x"),
            Err(crate::Error::Unrecoverable)
        ));
        assert!(matches!(
            db.rollback_prepared("x"),
            Err(crate::Error::Unrecoverable)
        ));
        assert!(matches!(
            db.pending_prepared(),
            Err(crate::Error::Unrecoverable)
        ));
        assert!(matches!(
            db.prepared_items("x"),
            Err(crate::Error::Unrecoverable)
        ));
        assert_eq!(1, txn.len()?, "the row was neither skipped nor removed");
        assert_eq!(None, a.get("k")?);
    }
    Ok(())
}

#[test]
fn no_op_resolution_clears_a_stray_reservation() -> crate::Result<()> {
    for with_keyspace in [false, true] {
        let folder = tempfile::tempdir()?;
        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        if with_keyspace {
            prepare_one(&db, &a, "other", "o", "v")?;
        }
        let stray = ConflictManager::default();
        stray.mark_conflict(a.inner().id(), "k".into());
        db.oracle
            .write_serialize_lock()?
            .reservations
            .insert("ghost".into(), stray);
        assert!(matches!(
            a.insert("k", "v"),
            Err(crate::Error::PreparedConflict)
        ));

        let outcome = if with_keyspace {
            db.rollback_prepared("ghost")?
        } else {
            db.commit_prepared("ghost")?
        };
        assert_eq!(Resolution::NoOp, outcome);
        assert!(!is_reserved(&db, "ghost")?);
        a.insert("k", "v")?;
    }
    Ok(())
}

#[test]
fn resolution_writes_regular_tombstones() -> crate::Result<()> {
    // §7.2 step 3: never weak
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    prepare_one(&db, &a, "c", "k1", "v")?;
    prepare_one(&db, &a, "r", "k2", "v")?;
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("c")?
    );
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.rollback_prepared("r")?
    );

    let txn = db.inner().reserved_keyspace()?.expect("created");
    for (xid, key) in [(&b"c"[..], &b"k1"[..]), (&b"r"[..], &b"k2"[..])] {
        let entry = txn
            .tree
            .active_memtable()
            .get(&encode_row_key(xid, a.inner().id(), key), SeqNo::MAX)
            .expect("tombstone in the memtable");
        assert_eq!(lsm_tree::ValueType::Tombstone, entry.key.value_type);
    }
    Ok(())
}

#[test]
fn row_targeting_the_hidden_keyspace_is_unresolvable() -> crate::Result<()> {
    // §7.2 step 2: the hidden keyspace is never a target
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let _a = keyspace(&db, "a")?;
    let txn = db.inner().reserved_keyspace_or_create()?;
    txn.insert(
        encode_row_key(b"x", txn.id(), b"k"),
        encode_row_value(PreparedItemKind::Put, b"v"),
    )?;

    let pending = db.pending_prepared()?;
    assert_eq!(1, pending.len());
    assert!(pending[0].keyspaces.is_empty(), "{pending:?}");
    assert_eq!(vec![txn.id()], pending[0].missing_keyspace_ids);
    let items = db.prepared_items("x")?;
    assert_eq!(1, items.len());
    assert_eq!(None, items[0].keyspace);

    assert_eq!(
        Resolution::Applied { dropped_items: 1 },
        db.commit_prepared("x")?
    );
    assert!(rows(&db)?.is_empty());
    assert_eq!(0, txn.len()?);
    Ok(())
}

#[test]
fn commit_prepared_applies_the_recorded_tombstone_kind() -> crate::Result<()> {
    // §6 value_type: `remove` applies as a regular tombstone, `remove_weak` as a weak one
    use lsm_tree::ValueType::{Tombstone, Value, WeakTombstone};

    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    let mut tx = db.write_tx()?;
    tx.insert(&a, "put", "v");
    tx.remove(&a, "delete");
    tx.remove_weak(&a, "weak");
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("x")?
    );

    for (key, value_type) in [
        ("put", Value),
        ("delete", Tombstone),
        ("weak", WeakTombstone),
    ] {
        let entry = a
            .inner()
            .tree
            .active_memtable()
            .get(key.as_bytes(), SeqNo::MAX)
            .expect("applied to the memtable");
        assert_eq!(value_type, entry.key.value_type, "{key}");
    }
    Ok(())
}

#[test]
fn reused_xid_after_resolution_is_a_new_generation() -> crate::Result<()> {
    // §10 "reuse after quiescence"
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    prepare_one(&db, &a, "x", "k", "1")?;
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("x")?
    );
    prepare_one(&db, &a, "x", "k", "2")?;
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("x")?
    );
    assert_eq!(Some("2".as_bytes().into()), a.get("k")?);

    let txn = db.inner().reserved_keyspace()?.expect("created");
    txn.rotate_memtable_and_wait()?;
    txn.major_compact()?;
    assert!(rows(&db)?.is_empty());
    assert_eq!(Resolution::NoOp, db.commit_prepared("x")?);
    Ok(())
}

#[test]
fn undecodable_row_does_not_fail_open() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    {
        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        prepare_one(&db, &a, "x", "k", "v")?;
        let txn = db.inner().reserved_keyspace()?.expect("created");
        txn.insert([0xFFu8], "junk")?; // too short for its own layout
    }
    let db = open(&folder)?; // logs the row, does not fail
    assert!(matches!(
        db.pending_prepared(),
        Err(crate::Error::Unrecoverable)
    ));
    assert_eq!(1, db.prepared_items("x")?.len());
    Ok(())
}

#[test]
fn impossible_target_ids_do_not_exhaust_the_keyspace_ids() -> crate::Result<()> {
    // §7.5: ids lsm-tree's id counter can never issue
    let folder = tempfile::tempdir()?;
    {
        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        prepare_one(&db, &a, "x", "k", "v")?;
        let txn = db.inner().reserved_keyspace()?.expect("created");
        for (xid, target_id) in [
            (&b"y"[..], u64::MAX - 1),
            (&b"z"[..], 0x8000_0000_0000_0000),
            // The last id the counter can issue: raising the counter past it is just as fatal
            (&b"w"[..], 0x7FFF_FFFF_FFFF_FFFF),
        ] {
            txn.insert(
                encode_row_key(xid, target_id, b"k"),
                encode_row_value(PreparedItemKind::Put, b"v"),
            )?;
        }
    }
    let db = open(&folder)?; // logs the rows, does not fail
    let b = keyspace(&db, "b")?;
    assert_eq!(3, b.inner().id());
    b.insert("k", "v")?;
    Ok(())
}

/// Seals the active journal and deletes every journal whose writes are all flushed.
fn evict_journals(db: &OptimisticTxDatabase) -> crate::Result<()> {
    let supervisor = &db.inner().supervisor;
    let mut writer = supervisor.journal.get_writer()?;
    let seqno_map = {
        let keyspaces = supervisor.keyspaces.write().expect("lock is poisoned");
        supervisor.build_seqno_map(&keyspaces)
    };
    let mut manager = supervisor
        .journal_manager
        .write()
        .expect("lock is poisoned");
    manager.rotate_journal(&mut writer, seqno_map)?;
    drop(writer);
    manager.maintenance()
}

#[test]
fn lost_reserved_keyspace_id_is_not_reissued() -> crate::Result<()> {
    // §5 lost-keyspace check: once the prepare's journal is evicted, only the meta
    // keyspace still knows the lost keyspace's id
    let folder = tempfile::tempdir()?;
    let txn_id = {
        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        prepare_one(&db, &a, "x", "k", "v")?;
        let txn = db.inner().reserved_keyspace()?.expect("created");
        txn.rotate_memtable_and_wait()?;
        evict_journals(&db)?;
        assert_eq!(1, db.journal_count(), "the prepare's journal was evicted");
        txn.id()
    };
    std::fs::remove_dir_all(folder.path().join("keyspaces").join(txn_id.to_string()))?;

    let db = open(&folder)?;
    let c = keyspace(&db, "c")?;
    assert_ne!(txn_id, c.inner().id());

    let mut tx = db.write_tx()?;
    tx.insert(&c, "k", "v");
    assert!(matches!(tx.prepare("y"), Err(crate::Error::Unrecoverable)));
    assert!(matches!(
        db.write_tx()?.prepare("z"),
        Err(crate::Error::Unrecoverable)
    ));
    assert!(matches!(
        db.commit_prepared("x"),
        Err(crate::Error::Unrecoverable)
    ));
    assert!(matches!(
        db.rollback_prepared("x"),
        Err(crate::Error::Unrecoverable)
    ));
    assert!(matches!(
        db.pending_prepared(),
        Err(crate::Error::Unrecoverable)
    ));
    assert!(matches!(
        db.prepared_items("x"),
        Err(crate::Error::Unrecoverable)
    ));

    let mut tx = db.write_tx()?;
    tx.insert(&c, "k", "v");
    assert!(tx.commit()?.is_ok());
    assert_eq!(Some("v".as_bytes().into()), c.get("k")?);
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    JournalWrite,
    Persist,
}

fn arm(db: &OptimisticTxDatabase, fault: Fault) -> crate::Result<()> {
    let mut writer = db.inner().supervisor.journal.get_writer()?;
    match fault {
        Fault::JournalWrite => writer.fail_next_journal_write(),
        Fault::Persist => writer.fail_next_persist(),
    }
    Ok(())
}

fn open_manual(folder: &tempfile::TempDir) -> crate::Result<OptimisticTxDatabase> {
    OptimisticTxDatabase::builder(folder.path())
        .manual_journal_persist(true)
        .open()
}

#[test]
fn failed_prepare_returns_poisoned_and_applies_nothing() -> crate::Result<()> {
    // §9 row 3
    for fault in [Fault::JournalWrite, Fault::Persist] {
        let folder = tempfile::tempdir()?;
        {
            let db = open_manual(&folder)?;
            let a = keyspace(&db, "a")?;
            prepare_one(&db, &a, "warmup", "w", "v")?; // creates __fjall_txn
            arm(&db, fault)?;

            let mut tx = db.write_tx()?;
            tx.insert(&a, "k", "v");
            assert!(
                matches!(tx.prepare("x"), Err(crate::Error::Poisoned)),
                "{fault:?}"
            );
            assert!(db.inner().is_poisoned());
            assert_eq!(
                1,
                rows(&db)?.len(),
                "{fault:?}: nothing reached the memtable"
            );
            assert!(!is_reserved(&db, "x")?);

            // No false Ok, NoOp or ReadOnly in the poisoned window
            assert!(matches!(
                db.write_tx()?.prepare("ro"),
                Err(crate::Error::Poisoned)
            ));
            assert!(matches!(
                db.commit_prepared("x"),
                Err(crate::Error::Poisoned)
            ));
            assert!(matches!(
                db.rollback_prepared("x"),
                Err(crate::Error::Poisoned)
            ));
            assert!(matches!(db.pending_prepared(), Err(crate::Error::Poisoned)));
            assert!(matches!(
                db.prepared_items("x"),
                Err(crate::Error::Poisoned)
            ));
        }

        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        let resurfaced = db.pending_prepared()?.iter().any(|info| &*info.xid == b"x");
        match fault {
            // The frame never reached the journal
            Fault::JournalWrite => {
                assert!(!resurfaced);
                assert_eq!(Resolution::NoOp, db.commit_prepared("x")?);
            }
            // Written but not synced; closing the journal flushed it: it MAY resurface (§9)
            Fault::Persist => {
                assert!(resurfaced);
                assert_eq!(
                    Resolution::Applied { dropped_items: 0 },
                    db.commit_prepared("x")?
                );
                assert_eq!(Some("v".as_bytes().into()), a.get("k")?);
            }
        }
    }
    Ok(())
}

#[test]
fn failed_resolution_returns_poisoned_and_applies_nothing() -> crate::Result<()> {
    for fault in [Fault::JournalWrite, Fault::Persist] {
        for commit in [true, false] {
            let folder = tempfile::tempdir()?;
            let db = open_manual(&folder)?;
            let a = keyspace(&db, "a")?;
            prepare_one(&db, &a, "x", "k", "v")?;
            arm(&db, fault)?;

            let outcome = if commit {
                db.commit_prepared("x")
            } else {
                db.rollback_prepared("x")
            };
            assert!(
                matches!(outcome, Err(crate::Error::Poisoned)),
                "{fault:?} commit={commit}"
            );
            assert_eq!(None, a.get("k")?);
            assert_eq!(1, rows(&db)?.len());
            assert!(is_reserved(&db, "x")?);
        }
    }
    Ok(())
}

#[test]
fn prepare_fsyncs_whatever_the_durability_settings() -> crate::Result<()> {
    // D21, first half
    let folder = tempfile::tempdir()?;
    let db = open_manual(&folder)?;
    let a = keyspace(&db, "a")?;
    prepare_one(&db, &a, "warmup", "w", "v")?;
    arm(&db, Fault::Persist)?;

    // Control: an ordinary commit without durability does not persist, so the fault stays armed
    let mut tx = db.write_tx()?.durability(None);
    tx.insert(&a, "plain", "v");
    assert!(tx.commit()?.is_ok());

    let mut tx = db.write_tx()?.durability(None);
    tx.insert(&a, "k", "v");
    assert!(matches!(tx.prepare("x"), Err(crate::Error::Poisoned)));
    Ok(())
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

fn journal_end(db: &OptimisticTxDatabase) -> crate::Result<u64> {
    db.inner().supervisor.journal.get_writer()?.pos()
}

/// Cuts the last byte of the journal's last frame, as a crash mid-write would.
fn tear_last_frame(journal: &std::path::Path, end: u64) -> std::io::Result<()> {
    OpenOptions::new()
        .write(true)
        .open(journal)?
        .set_len(end - 1)
}

#[test]
fn torn_prepare_frame_is_not_prepared() -> crate::Result<()> {
    // §9 row 2
    let folder = tempfile::tempdir()?;
    let (journal, end) = {
        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        prepare_one(&db, &a, "x", "k", "v")?;
        (db.inner().supervisor.journal.path()?, journal_end(&db)?)
    };
    tear_last_frame(&journal, end)?;

    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    assert!(db.pending_prepared()?.is_empty());
    assert_eq!(Resolution::NoOp, db.commit_prepared("x")?);
    assert_eq!(None, a.get("k")?);
    prepare_one(&db, &a, "x", "k", "v")?;
    Ok(())
}

#[test]
fn torn_commit_prepared_frame_leaves_the_xid_in_doubt() -> crate::Result<()> {
    // §9 row 5
    let folder = tempfile::tempdir()?;
    let (journal, end) = {
        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        prepare_one(&db, &a, "x", "k", "v")?;
        assert_eq!(
            Resolution::Applied { dropped_items: 0 },
            db.commit_prepared("x")?
        );
        (db.inner().supervisor.journal.path()?, journal_end(&db)?)
    };
    tear_last_frame(&journal, end)?;

    {
        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        assert_eq!(1, db.prepared_items("x")?.len());
        assert_eq!(None, a.get("k")?);
        assert_eq!(
            Resolution::Applied { dropped_items: 0 },
            db.commit_prepared("x")?
        );
    }
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    assert_eq!(Some("v".as_bytes().into()), a.get("k")?);
    assert_eq!(Resolution::NoOp, db.commit_prepared("x")?);
    Ok(())
}

#[test]
fn flushed_rows_resolve_and_block_duplicates() -> crate::Result<()> {
    // §10 flush/compaction row
    let folder = tempfile::tempdir()?;
    {
        let db = open(&folder)?;
        let a = keyspace(&db, "a")?;
        prepare_one(&db, &a, "x", "k1", "v")?;
        prepare_one(&db, &a, "y", "k2", "v")?;
        let txn = db.inner().reserved_keyspace()?.expect("created");
        txn.rotate_memtable_and_wait()?;
        txn.major_compact()?;
    }
    let db = open(&folder)?;
    let a = keyspace(&db, "a")?;
    let mut tx = db.write_tx()?;
    tx.insert(&a, "k3", "v");
    assert!(matches!(tx.prepare("x"), Err(crate::Error::XidInUse)));
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("x")?
    );
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.rollback_prepared("y")?
    );

    let txn = db.inner().reserved_keyspace()?.expect("recovered");
    txn.rotate_memtable_and_wait()?;
    txn.major_compact()?;
    assert!(db.pending_prepared()?.is_empty());
    assert_eq!(Some("v".as_bytes().into()), a.get("k1")?);
    assert_eq!(None, a.get("k2")?);
    Ok(())
}

#[test]
fn catch_all_compaction_filter_leaves_prepared_rows_alone() -> crate::Result<()> {
    // §12 compaction filter
    let folder = tempfile::tempdir()?;
    let assigner: crate::db_config::CompactionFilterAssigner =
        Arc::new(|_: &str| Some(Arc::new(DropAllFactory) as Arc<dyn Factory>));
    let open_filtered = || {
        OptimisticTxDatabase::builder(folder.path())
            .with_compaction_filter_factories(assigner.clone())
            .open()
    };
    {
        let db = open_filtered()?;
        let a = keyspace(&db, "a")?;
        prepare_one(&db, &a, "x", "k", "v")?;
        let txn = db.inner().reserved_keyspace()?.expect("created");
        txn.rotate_memtable_and_wait()?;
        txn.major_compact()?;
        assert_eq!(1, db.prepared_items("x")?.len());
    }
    let db = open_filtered()?;
    let a = keyspace(&db, "a")?;
    prepare_one(&db, &a, "y", "k2", "v")?;
    let txn = db.inner().reserved_keyspace()?.expect("recovered");
    txn.rotate_memtable_and_wait()?;
    txn.major_compact()?;
    assert_eq!(1, db.prepared_items("x")?.len());
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("x")?
    );
    assert_eq!(Some("v".as_bytes().into()), a.get("k")?);
    Ok(())
}
