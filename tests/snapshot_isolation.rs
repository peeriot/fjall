use fjall::{Database, Keyspace, KeyspaceCreateOptions, OptimisticTxDatabase, Readable, Snapshot};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use test_log::test;

const KEYS: u32 = 200;
const RUN_FOR: Duration = Duration::from_secs(2);

/// Every value the snapshot holds in the given keyspaces, as round numbers.
fn rounds_in(snapshot: &Snapshot, keyspaces: &[&Keyspace]) -> fjall::Result<Vec<u64>> {
    let mut rounds = Vec::new();
    for keyspace in keyspaces {
        for guard in snapshot.iter(keyspace) {
            let value = guard.value()?;
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&value);
            rounds.push(u64::from_be_bytes(buf));
        }
    }
    Ok(rounds)
}

/// A snapshot must hold one round in every key, and read the same twice.
/// Returns what is wrong with it, if anything.
fn isolation_violation(first: &[u64], second: &[u64]) -> Option<&'static str> {
    if first.len() != 2 * KEYS as usize {
        Some("misses keys")
    } else if first.iter().any(|round| *round != first[0]) {
        Some("holds more than one round")
    } else if first != second {
        Some("changed while held")
    } else {
        None
    }
}

/// Alternates keyspace creation/deletion with flushes of the written keyspaces.
fn disturb(db: &Database, x: &Keyspace, y: &Keyspace, i: u64) -> fjall::Result<()> {
    match i % 3 {
        0 => {
            let tmp = db.keyspace(&format!("tmp{i}"), KeyspaceCreateOptions::default)?;
            db.delete_keyspace(tmp)
        }
        1 => x.rotate_memtable_and_wait(),
        _ => y.rotate_memtable_and_wait(),
    }
}

/// Opens snapshots for `RUN_FOR`, reading each twice, until the first violation.
///
/// Returns the number of snapshots taken and the violation, if any.
fn read_snapshots(
    open: impl Fn() -> Snapshot,
    keyspaces: &[&Keyspace],
) -> fjall::Result<(u64, Option<&'static str>)> {
    let start = Instant::now();
    let mut snapshots = 0;

    while start.elapsed() < RUN_FOR {
        let snapshot = open();
        snapshots += 1;

        let first = rounds_in(&snapshot, keyspaces)?;
        let second = rounds_in(&snapshot, keyspaces)?;

        if let Some(violation) = isolation_violation(&first, &second) {
            return Ok((snapshots, Some(violation)));
        }
    }

    Ok((snapshots, None))
}

/// Calls `disturb` in a loop until `stop` is set.
fn spawn_disturber(
    db: Database,
    x: Keyspace,
    y: Keyspace,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<fjall::Result<()>> {
    std::thread::spawn(move || {
        let mut i = 0;
        while !stop.load(Ordering::Relaxed) {
            disturb(&db, &x, &y, i)?;
            i += 1;
        }
        Ok(())
    })
}

#[test]
fn snapshot_is_isolated_under_flushes_and_keyspace_churn() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let x = db.keyspace("x", KeyspaceCreateOptions::default)?;
    let y = db.keyspace("y", KeyspaceCreateOptions::default)?;

    let write_round = |db: &Database, x: &Keyspace, y: &Keyspace, round: u64| {
        let mut batch = db.batch();
        for key in 0..KEYS {
            batch.insert(x, key.to_be_bytes(), round.to_be_bytes());
            batch.insert(y, key.to_be_bytes(), round.to_be_bytes());
        }
        batch.commit()
    };

    write_round(&db, &x, &y, 0)?;

    let stop = Arc::new(AtomicBool::new(false));

    let writers = (0..2u64)
        .map(|w| {
            let (db, x, y, stop) = (db.clone(), x.clone(), y.clone(), stop.clone());
            std::thread::spawn(move || -> fjall::Result<()> {
                let mut round = w + 1;
                while !stop.load(Ordering::Relaxed) {
                    write_round(&db, &x, &y, round)?;
                    round += 2;
                }
                Ok(())
            })
        })
        .collect::<Vec<_>>();

    let disturber = spawn_disturber(db.clone(), x.clone(), y.clone(), stop.clone());

    let result = read_snapshots(|| db.snapshot(), &[&x, &y]);

    stop.store(true, Ordering::Relaxed);
    for writer in writers {
        writer.join().expect("writer panicked")?;
    }
    disturber.join().expect("disturber panicked")?;

    let (snapshots, violation) = result?;
    assert_eq!(None, violation, "snapshot #{snapshots}");

    Ok(())
}

#[test]
fn read_tx_is_isolated_under_flushes_and_keyspace_churn() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let x = db.keyspace("x", KeyspaceCreateOptions::default)?;
    let y = db.keyspace("y", KeyspaceCreateOptions::default)?;

    let write_round = |db: &OptimisticTxDatabase,
                       x: &fjall::OptimisticTxKeyspace,
                       y: &fjall::OptimisticTxKeyspace,
                       round: u64|
     -> fjall::Result<()> {
        let mut tx = db.write_tx()?;
        for key in 0..KEYS {
            tx.insert(x, key.to_be_bytes(), round.to_be_bytes());
            tx.insert(y, key.to_be_bytes(), round.to_be_bytes());
        }
        tx.commit()?.expect("blind writes do not conflict");
        Ok(())
    };

    write_round(&db, &x, &y, 0)?;

    let stop = Arc::new(AtomicBool::new(false));

    let writer = {
        let (db, x, y, stop) = (db.clone(), x.clone(), y.clone(), stop.clone());
        std::thread::spawn(move || -> fjall::Result<()> {
            let mut round = 1;
            while !stop.load(Ordering::Relaxed) {
                write_round(&db, &x, &y, round)?;
                round += 1;
            }
            Ok(())
        })
    };

    let disturber = spawn_disturber(
        db.inner().clone(),
        x.inner().clone(),
        y.inner().clone(),
        stop.clone(),
    );

    let result = read_snapshots(|| db.read_tx(), &[x.inner(), y.inner()]);

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer panicked")?;
    disturber.join().expect("disturber panicked")?;

    let (snapshots, violation) = result?;
    assert_eq!(None, violation, "snapshot #{snapshots}");

    Ok(())
}

#[test]
fn snapshot_sees_ingestion_after_finish() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let before = db.snapshot();

    let mut ingestion = tree.start_ingestion()?;
    for key in ["a", "b", "c"] {
        ingestion.write(key, "v")?;
    }
    ingestion.finish()?;

    assert_eq!(3, db.snapshot().iter(&tree).count());
    assert_eq!(0, before.iter(&tree).count());

    Ok(())
}

#[test]
fn snapshot_sees_clear() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    for key in ["a", "b", "c"] {
        tree.insert(key, "v")?;
    }
    let before = db.snapshot();

    tree.clear()?;

    assert_eq!(0, db.snapshot().iter(&tree).count());
    assert_eq!(3, before.iter(&tree).count());

    Ok(())
}

#[test]
fn old_snapshot_survives_flush_and_compaction() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    for key in ["a", "b", "c"] {
        tree.insert(key, "old")?;
    }
    let before = db.snapshot();

    for key in ["a", "b", "c", "d"] {
        tree.insert(key, "new")?;
    }
    tree.rotate_memtable_and_wait()?;
    tree.major_compact()?;

    assert_eq!(3, before.iter(&tree).count());
    assert_eq!(Some("old".as_bytes().into()), before.get(&tree, "a")?);
    assert_eq!(4, db.snapshot().iter(&tree).count());

    Ok(())
}
