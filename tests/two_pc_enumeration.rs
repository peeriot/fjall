mod common;

use common::prepare_put;
use fjall::{
    KeyspaceCreateOptions, OptimisticTxDatabase, PrepareOutcome, PreparedItem, PreparedItemKind,
    PreparedTxInfo, Resolution,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use test_log::test;

fn info(xid: &str, item_count: usize, keyspaces: &[&str], missing: &[u64]) -> PreparedTxInfo {
    PreparedTxInfo {
        xid: xid.into(),
        item_count,
        keyspaces: keyspaces.iter().map(|k| (*k).into()).collect(),
        missing_keyspace_ids: missing.to_vec(),
    }
}

#[test]
fn pending_prepared_reports_every_in_doubt_xid() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

    let mut tx = db.write_tx()?;
    tx.insert(&b, "k2", "v");
    tx.insert(&a, "k1", "v");
    assert!(matches!(tx.prepare("bb")?, Ok(PrepareOutcome::Prepared)));
    assert!(matches!(
        prepare_put(&db, &b, "c", "k3", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    let mut tx = db.write_tx()?;
    for key in ["k4", "k5", "k6"] {
        tx.insert(&a, key, "v");
    }
    assert!(matches!(tx.prepare("aaa")?, Ok(PrepareOutcome::Prepared)));

    assert_eq!(
        vec![
            info("c", 1, &["b"], &[]),
            info("bb", 2, &["a", "b"], &[]),
            info("aaa", 3, &["a"], &[])
        ],
        db.pending_prepared()?,
    );

    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("bb")?
    );
    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.rollback_prepared("c")?
    );
    assert_eq!(vec![info("aaa", 3, &["a"], &[])], db.pending_prepared()?);
    Ok(())
}

#[test]
fn prepared_items_match_the_durable_write_set() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
    let mut tx = db.write_tx()?;
    tx.insert(&b, "k4", "w");
    tx.remove_weak(&a, "k3");
    tx.remove(&b, "k2");
    tx.insert(&a, "k1", "v");
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));

    let item = |keyspace: Option<&str>, keyspace_id, key: &str, kind| PreparedItem {
        keyspace: keyspace.map(Into::into),
        keyspace_id,
        key: key.into(),
        kind,
    };
    assert_eq!(
        vec![
            item(Some("a"), 1, "k1", PreparedItemKind::Put),
            item(Some("a"), 1, "k3", PreparedItemKind::WeakDelete),
            item(Some("b"), 2, "k2", PreparedItemKind::Delete),
            item(Some("b"), 2, "k4", PreparedItemKind::Put),
        ],
        db.prepared_items("x")?,
    );

    db.inner().delete_keyspace(b.inner().clone())?;
    assert_eq!(
        vec![
            item(Some("a"), 1, "k1", PreparedItemKind::Put),
            item(Some("a"), 1, "k3", PreparedItemKind::WeakDelete),
            item(None, 2, "k2", PreparedItemKind::Delete),
            item(None, 2, "k4", PreparedItemKind::Put),
        ],
        db.prepared_items("x")?,
    );
    assert_eq!(vec![info("x", 4, &["a"], &[2])], db.pending_prepared()?);
    Ok(())
}

#[test]
fn xids_that_prefix_each_other_stay_separate() -> fjall::Result<()> {
    // Review Focus 2
    let folder = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let xids: [Vec<u8>; 4] = [
        b"a".to_vec(),
        b"ab".to_vec(),
        vec![0x00, 0xFF],
        vec![0xFF; 255],
    ];
    for (i, xid) in xids.iter().enumerate() {
        let mut tx = db.write_tx()?;
        tx.insert(&a, format!("k{i}"), "v");
        assert!(matches!(
            tx.prepare(xid.clone())?,
            Ok(PrepareOutcome::Prepared)
        ));
    }
    for (i, xid) in xids.iter().enumerate() {
        let items = db.prepared_items(xid)?;
        assert_eq!(1, items.len(), "{xid:?}");
        assert_eq!(format!("k{i}").as_bytes(), &*items[0].key);
    }

    assert_eq!(
        Resolution::Applied { dropped_items: 0 },
        db.commit_prepared("a")?
    );
    let remaining: Vec<Vec<u8>> = db
        .pending_prepared()?
        .iter()
        .map(|i| i.xid.to_vec())
        .collect();
    // Length first, then bytes: [0x00, 0xFF] sorts before "ab"
    assert_eq!(
        vec![vec![0x00, 0xFF], b"ab".to_vec(), vec![0xFF; 255]],
        remaining
    );
    assert_eq!(Some("v".as_bytes().into()), a.get("k0")?);
    assert_eq!(None, a.get("k1")?);
    Ok(())
}

#[test]
fn prepare_into_a_keyspace_deleted_mid_transaction() -> fjall::Result<()> {
    // Review Focus 4
    let folder = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let gone = db.keyspace("gone", KeyspaceCreateOptions::default)?;
    let mut tx = db.write_tx()?;
    tx.insert(&a, "k", "v");
    tx.insert(&gone, "k", "v");
    db.inner().delete_keyspace(gone.inner().clone())?;
    assert!(matches!(tx.prepare("x")?, Ok(PrepareOutcome::Prepared)));

    assert_eq!(vec![info("x", 2, &["a"], &[2])], db.pending_prepared()?);
    assert_eq!(
        Resolution::Applied { dropped_items: 1 },
        db.commit_prepared("x")?
    );
    assert_eq!(Some("v".as_bytes().into()), a.get("k")?);
    Ok(())
}

#[test]
fn unknown_xids_have_no_items() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    assert!(matches!(
        prepare_put(&db, &a, "x", "k", "v")?,
        Ok(PrepareOutcome::Prepared)
    ));
    assert!(db.prepared_items("nope")?.is_empty());
    assert!(db.prepared_items(vec![0u8; 70_000])?.is_empty());
    Ok(())
}

const RUN_FOR: Duration = Duration::from_secs(2);
const ITEMS: usize = 5;

#[test]
fn enumeration_is_exact_under_concurrency() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = OptimisticTxDatabase::builder(&folder).open()?;
    let x = db.keyspace("x", KeyspaceCreateOptions::default)?;
    let y = db.keyspace("y", KeyspaceCreateOptions::default)?;
    let stop = Arc::new(AtomicBool::new(false));

    // Two threads prepare and resolve multi-item transactions
    let preparers: Vec<_> = (0..2)
        .map(|w| {
            let (db, x, y, stop) = (db.clone(), x.clone(), y.clone(), stop.clone());
            std::thread::spawn(move || -> fjall::Result<()> {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let xid = format!("w{w}-{i}");
                    let mut tx = db.write_tx()?;
                    for j in 0..ITEMS {
                        tx.insert(if j % 2 == 0 { &x } else { &y }, format!("{xid}-{j}"), "v");
                    }
                    assert!(matches!(
                        tx.prepare(xid.as_str())?,
                        Ok(PrepareOutcome::Prepared)
                    ));
                    let resolved = if i % 2 == 0 {
                        db.commit_prepared(&xid)?
                    } else {
                        db.rollback_prepared(&xid)?
                    };
                    assert_eq!(Resolution::Applied { dropped_items: 0 }, resolved);
                    i += 1;
                }
                Ok(())
            })
        })
        .collect();

    // A fourth creates and deletes keyspaces (version installs)
    let churner = {
        let (db, stop) = (db.clone(), stop.clone());
        std::thread::spawn(move || -> fjall::Result<()> {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let tmp = db.keyspace(&format!("tmp{i}"), KeyspaceCreateOptions::default)?;
                db.inner().delete_keyspace(tmp.inner().clone())?;
                i += 1;
            }
            Ok(())
        })
    };

    let start = Instant::now();
    let mut enumerations = 0u64;
    let mut violation = None;
    while start.elapsed() < RUN_FOR && violation.is_none() {
        violation = db.pending_prepared()?.into_iter().find(|info| {
            let names: Vec<&str> = info.keyspaces.iter().map(AsRef::as_ref).collect();
            info.item_count != ITEMS || names != ["x", "y"] || !info.missing_keyspace_ids.is_empty()
        });
        enumerations += 1;
    }

    stop.store(true, Ordering::Relaxed);
    for preparer in preparers {
        preparer.join().expect("preparer panicked")?;
    }
    churner.join().expect("churner panicked")?;
    assert_eq!(None, violation, "after {enumerations} enumerations");
    Ok(())
}
