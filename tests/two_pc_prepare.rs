mod common;

use common::{keyspace_dirs, prepare_put};
use fjall::{
    Conflict, KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, PrepareOutcome,
    Readable,
};
use std::ops::Bound;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use test_log::test;

fn setup(
    folder: &tempfile::TempDir,
) -> fjall::Result<(OptimisticTxDatabase, OptimisticTxKeyspace)> {
    let db = OptimisticTxDatabase::builder(folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    Ok((db, a))
}

#[test]
fn prepared_writes_are_invisible() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("old", "v")?;

    let mut tx = db.write_tx()?;
    tx.insert(&a, "new", "v");
    tx.remove(&a, "old");
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));

    assert_eq!(None, a.get("new")?);
    assert_eq!(Some("v".as_bytes().into()), a.get("old")?);
    assert_eq!(1, a.inner().iter().count());
    assert_eq!(1, a.inner().range("a".."z").count());
    let snapshot = db.read_tx();
    assert_eq!(None, snapshot.get(&a, "new")?);
    assert_eq!(1, snapshot.iter(&a).count());
    let other = db.write_tx()?;
    assert_eq!(None, other.untrack().get(&a, "new")?);
    assert!(other.untrack().contains_key(&a, "old")?);
    Ok(())
}

#[test]
fn read_only_prepare_votes_without_a_trace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("k", "v")?;

    let tx = db.write_tx()?;
    assert_eq!(Some("v".as_bytes().into()), tx.get(&a, "k")?);
    assert!(matches!(tx.prepare("ro")?, Ok(PrepareOutcome::ReadOnly)));
    assert_eq!(vec!["0", "1"], keyspace_dirs(folder.path())?);

    // The xid was not remembered
    assert!(matches!(
        prepare_put(&db, &a, "ro", "k2", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    Ok(())
}

#[test]
fn read_only_prepare_validates_its_reads() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("k", "v")?;

    let voter = db.write_tx()?;
    voter.get(&a, "k")?;
    let committer = db.write_tx()?;
    committer.get(&a, "k")?;
    a.insert("k", "changed")?;

    // commit skips validation for an empty write-set; prepare does not (§7.1 step 2)
    assert!(committer.commit()?.is_ok());
    assert!(matches!(voter.prepare("ro")?, Err(Conflict)));
    Ok(())
}

#[test]
fn duplicate_prepare_is_xid_in_use_before_and_after_restart() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    {
        let (db, a) = setup(&folder)?;
        assert!(matches!(
            prepare_put(&db, &a, "x", "k", "v")?,
            Ok(PrepareOutcome::Prepared)
        ));
        assert!(matches!(
            prepare_put(&db, &a, "x", "k2", "v"),
            Err(fjall::Error::XidInUse)
        ));
        assert!(matches!(
            db.write_tx()?.prepare("x"),
            Err(fjall::Error::XidInUse)
        ));
    }
    // Only the on-disk rows remember it now
    let (db, a) = setup(&folder)?;
    assert!(matches!(
        prepare_put(&db, &a, "x", "k3", "v"),
        Err(fjall::Error::XidInUse)
    ));
    assert!(matches!(
        db.write_tx()?.prepare("x"),
        Err(fjall::Error::XidInUse)
    ));
    Ok(())
}

#[test]
fn oversize_key_is_rejected_without_creating_anything() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    let long_xid = "x".repeat(255);

    // 65,535 - 10 - xid length
    for (xid, limit) in [("x", 65_524), (long_xid.as_str(), 65_270)] {
        let mut tx = db.write_tx()?;
        tx.insert(&a, vec![b'k'; limit + 1], "v");
        assert!(matches!(
            tx.prepare(xid),
            Err(fjall::Error::PreparedItemTooLarge)
        ));
        assert_eq!(
            vec!["0", "1"],
            keyspace_dirs(folder.path())?,
            "step 0b precedes 0c"
        );
    }
    for (xid, limit) in [("x", 65_524), (long_xid.as_str(), 65_270)] {
        let mut tx = db.write_tx()?;
        tx.insert(&a, vec![b'k'; limit], "v");
        assert!(matches!(tx.prepare(xid)?, Ok(PrepareOutcome::Prepared)));
    }
    Ok(())
}

#[test]
fn first_prepare_that_conflicts_leaves_an_empty_hidden_keyspace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;

    let mut tx = db.write_tx()?;
    tx.get(&a, "k")?;
    tx.insert(&a, "k2", "v");
    a.insert("k", "v")?;
    assert!(matches!(tx.prepare("x")?, Err(Conflict)));

    assert_eq!(vec!["0", "1", "2"], keyspace_dirs(folder.path())?);
    assert_eq!(1, db.keyspace_count());
    assert!(matches!(
        prepare_put(&db, &a, "x", "k2", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    Ok(())
}

#[test]
fn racing_first_prepares_create_one_keyspace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    let barrier = Arc::new(Barrier::new(8));

    let preparers: Vec<_> = (0..8)
        .map(|i| {
            let (db, a, barrier) = (db.clone(), a.clone(), barrier.clone());
            std::thread::spawn(move || {
                let mut tx = db.write_tx()?;
                tx.insert(&a, format!("k{i}"), "v");
                barrier.wait();
                tx.prepare(format!("x{i}"))
            })
        })
        .collect();
    for preparer in preparers {
        assert!(matches!(
            preparer.join().expect("preparer panicked")?,
            Ok(PrepareOutcome::Prepared)
        ));
    }
    assert_eq!(vec!["0", "1", "2"], keyspace_dirs(folder.path())?);
    Ok(())
}

#[test]
#[should_panic(expected = "xid must be 1 to 255 bytes long")]
fn empty_xid_panics() {
    let folder = tempfile::tempdir().expect("tempdir");
    let (db, a) = setup(&folder).expect("setup");
    let _ = prepare_put(&db, &a, "", "k", "v");
}

#[test]
#[should_panic(expected = "xid must be 1 to 255 bytes long")]
fn xid_longer_than_255_bytes_panics() {
    let folder = tempfile::tempdir().expect("tempdir");
    let (db, a) = setup(&folder).expect("setup");
    let _ = prepare_put(&db, &a, &"x".repeat(256), "k", "v");
}

#[test]
fn reader_of_a_prepared_key_conflicts() -> fjall::Result<()> {
    // (a)
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("k", "old")?;
    assert!(matches!(
        prepare_put(&db, &a, "p", "k", "new")?,
        Ok(PrepareOutcome::Prepared)
    ));

    let mut t = db.write_tx()?;
    assert_eq!(Some("old".as_bytes().into()), t.get(&a, "k")?);
    t.insert(&a, "elsewhere", "x");
    assert!(matches!(t.commit()?, Err(Conflict)));
    Ok(())
}

#[test]
fn blind_writer_of_a_prepared_key_conflicts() -> fjall::Result<()> {
    // (b), the rev-1 anomaly
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    let mut p = db.write_tx()?;
    p.get(&a, "x")?;
    p.insert(&a, "z", "from-p");
    assert!(matches!(p.prepare("p")?, Ok(PrepareOutcome::Prepared)));

    let mut t = db.write_tx()?;
    t.insert(&a, "x", "t");
    t.insert(&a, "z", "t");
    assert!(matches!(t.commit()?, Err(Conflict)));
    assert_eq!(None, a.get("z")?);
    Ok(())
}

#[test]
fn writer_of_a_key_the_prepare_only_read_commits() -> fjall::Result<()> {
    // (e)
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    let mut p = db.write_tx()?;
    p.get(&a, "x")?;
    p.insert(&a, "z", "from-p");
    assert!(matches!(p.prepare("p")?, Ok(PrepareOutcome::Prepared)));

    let mut t = db.write_tx()?;
    t.insert(&a, "x", "t");
    assert!(t.commit()?.is_ok());
    assert_eq!(Some("t".as_bytes().into()), a.get("x")?);
    Ok(())
}

#[test]
fn prepare_overlapping_a_pending_prepare_conflicts() -> fjall::Result<()> {
    // (f)
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    assert!(matches!(
        prepare_put(&db, &a, "p1", "z", "1")?,
        Ok(PrepareOutcome::Prepared)
    ));

    assert!(matches!(
        prepare_put(&db, &a, "p2", "z", "2")?,
        Err(Conflict)
    ));
    let mut p3 = db.write_tx()?;
    p3.get(&a, "z")?;
    p3.insert(&a, "w", "3");
    assert!(matches!(p3.prepare("p3")?, Err(Conflict)));
    assert!(matches!(
        prepare_put(&db, &a, "p4", "w", "4")?,
        Ok(PrepareOutcome::Prepared)
    ));
    Ok(())
}

#[test]
fn helpers_return_prepared_conflict_against_a_pending_prepare() -> fjall::Result<()> {
    // (g) + livelock
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("k", "v")?;
    assert!(matches!(
        prepare_put(&db, &a, "p", "k", "new")?,
        Ok(PrepareOutcome::Prepared)
    ));

    let start = Instant::now();
    assert!(matches!(
        a.insert("k", "x"),
        Err(fjall::Error::PreparedConflict)
    ));
    assert!(matches!(a.remove("k"), Err(fjall::Error::PreparedConflict)));
    assert!(matches!(
        a.remove_weak("k"),
        Err(fjall::Error::PreparedConflict)
    ));
    assert!(matches!(
        a.fetch_update("k", |_| Some("w".into())),
        Err(fjall::Error::PreparedConflict)
    ));
    assert!(matches!(
        a.update_fetch("k", |_| Some("w".into())),
        Err(fjall::Error::PreparedConflict)
    ));
    assert!(matches!(a.take("k"), Err(fjall::Error::PreparedConflict)));
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );
    assert_eq!(Some("v".as_bytes().into()), a.get("k")?);
    Ok(())
}

#[test]
fn reservation_survives_committed_txns_gc() -> fjall::Result<()> {
    // (h)
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    assert!(matches!(
        prepare_put(&db, &a, "p", "k", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    for i in 0..10_001u32 {
        a.insert(i.to_be_bytes(), "x")?;
    }

    let mut t = db.write_tx()?;
    t.get(&a, "k")?;
    t.insert(&a, "elsewhere", "x");
    assert!(matches!(t.commit()?, Err(Conflict)));
    Ok(())
}

/// Range reads that read nothing, with bounds `BTreeSet::range` rejects: inverted, and
/// equal with both excluded. Both enclose or touch the key "m".
fn empty_range_reads() -> [(Bound<&'static str>, Bound<&'static str>); 2] {
    [
        (Bound::Included("z"), Bound::Excluded("b")),
        (Bound::Excluded("m"), Bound::Excluded("m")),
    ]
}

#[test]
fn empty_range_read_commits_after_a_concurrent_commit() -> fjall::Result<()> {
    for range in empty_range_reads() {
        let folder = tempfile::tempdir()?;
        let (db, a) = setup(&folder)?;

        let mut t = db.write_tx()?;
        assert_eq!(0, t.range::<&str, _>(&a, range).count());
        a.insert("m", "concurrent")?;
        t.insert(&a, "elsewhere", "t");
        assert!(t.commit()?.is_ok(), "{range:?}");

        // The oracle lock is not poisoned
        let mut u = db.write_tx()?;
        u.insert(&a, "after", "u");
        assert!(u.commit()?.is_ok(), "{range:?}");
    }
    Ok(())
}

#[test]
fn empty_range_read_commits_while_a_prepare_is_pending() -> fjall::Result<()> {
    for range in empty_range_reads() {
        let folder = tempfile::tempdir()?;
        let (db, a) = setup(&folder)?;
        assert!(matches!(
            prepare_put(&db, &a, "p", "m", "v")?,
            Ok(PrepareOutcome::Prepared)
        ));

        let mut t = db.write_tx()?;
        assert_eq!(0, t.range::<&str, _>(&a, range).count());
        t.insert(&a, "elsewhere", "t");
        assert!(t.commit()?.is_ok(), "{range:?}");

        // The oracle lock is not poisoned
        let mut u = db.write_tx()?;
        u.insert(&a, "after", "u");
        assert!(u.commit()?.is_ok(), "{range:?}");
    }
    Ok(())
}

#[test]
fn unchanged_fetch_update_does_not_freeze_its_key() -> fjall::Result<()> {
    // §8 reservation source
    let folder = tempfile::tempdir()?;
    let (db, a) = setup(&folder)?;
    a.insert("same", "v")?;

    let mut p = db.write_tx()?;
    p.fetch_update(&a, "same", |v| v.cloned())?; // marks a conflict, writes nothing
    p.insert(&a, "written", "v");
    assert!(matches!(p.prepare("p")?, Ok(PrepareOutcome::Prepared)));

    a.insert("same", "t")?;
    assert!(matches!(
        a.insert("written", "t"),
        Err(fjall::Error::PreparedConflict)
    ));
    Ok(())
}
