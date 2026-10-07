use crate::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

#[test_log::test]
fn clear_recover_sealed() -> crate::Result<()> {
    use crate::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        tree.insert("a", "a")?;
        assert!(tree.contains_key("a")?);

        tree.clear()?;
        assert!(tree.is_empty()?);

        tree.rotate_memtable_and_wait()?;
        assert!(tree.is_empty()?);
        db.supervisor.journal.get_writer()?.rotate()?;

        tree.insert("b", "a")?;
        assert!(tree.contains_key("b")?);
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert!(!tree.contains_key("a")?);
        assert!(tree.contains_key("b")?);
    }

    Ok(())
}

// TODO: investigate: flaky on macOS???
#[cfg(feature = "__internal_whitebox")]
#[test]
#[ignore = "restore"]
fn whitebox_db_drop() -> crate::Result<()> {
    use crate::Database;

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        drop(db);
        assert_eq!(0, crate::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        let tree = db.keyspace("default", Default::default)?;
        assert_eq!(6, crate::drop::load_drop_counter());

        drop(tree);
        drop(db);
        assert_eq!(0, crate::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, crate::drop::load_drop_counter());
        let db = Database::builder(&folder).open()?;
        assert_eq!(5, crate::drop::load_drop_counter());

        let _tree = db.keyspace("default", Default::default)?;
        assert_eq!(6, crate::drop::load_drop_counter());

        let _tree2 = db.keyspace("different", Default::default)?;
        assert_eq!(7, crate::drop::load_drop_counter());
    }

    assert_eq!(0, crate::drop::load_drop_counter());

    Ok(())
}

#[cfg(feature = "__internal_whitebox")]
#[test]
#[ignore = "restore"]
fn whitebox_db_drop_2() -> crate::Result<()> {
    use crate::{Database, KeyspaceCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("tree", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("tree1", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a")?;
        tree2.insert("b", "b")?;

        tree.rotate_memtable_and_wait()?;
    }

    assert_eq!(0, crate::drop::load_drop_counter());

    Ok(())
}

#[test]
pub fn test_exotic_keyspace_names() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;

    for name in ["hello$world", "hello#world", "hello.world", "hello_world"] {
        let tree = db.keyspace(name, KeyspaceCreateOptions::default)?;
        tree.insert("a", "a")?;
        assert_eq!(1, tree.len()?);
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_smoke_test() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes())?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_order() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a")?;
        tree.rotate_memtable()?;

        tree.insert("a", "b")?;
        tree.rotate_memtable()?;

        tree.insert("a", "c")?;
        tree.rotate_memtable()?;
    }

    {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(b"c", &*tree.get("a")?.unwrap());
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_blob() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());

        tree.insert(i.to_be_bytes(), i.to_be_bytes().repeat(1_024))?;
        assert_eq!(i + 1, tree.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn recover_sealed_pair_1() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..3 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().max_memtable_size(1_000)
        })?;
        let tree2 = db.keyspace("default2", || {
            KeyspaceCreateOptions::default()
                .max_memtable_size(1_000)
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;

        assert_eq!(i, tree.len()?.try_into().unwrap());
        assert_eq!(i, tree2.len()?.try_into().unwrap());

        let mut batch = db.batch();
        batch.insert(&tree, i.to_be_bytes(), i.to_be_bytes());
        batch.insert(&tree2, i.to_be_bytes(), i.to_be_bytes().repeat(1_024));
        batch.commit()?;

        assert_eq!(i + 1, tree.len()?.try_into().unwrap());
        assert_eq!(i + 1, tree2.len()?.try_into().unwrap());

        tree.rotate_memtable_and_wait()?;
    }

    Ok(())
}

// Regression test for https://github.com/fjall-rs/fjall/issues/328
#[test]
fn delete_keyspace_1() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(folder.path()).open()?;
        let _keep = db.keyspace("keep", KeyspaceCreateOptions::default)?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        a.insert("stale", "written to a")?;
        db.delete_keyspace(a)?;
    }

    {
        let db = Database::builder(folder.path()).open()?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
        assert!(b.is_empty()?, "keyspace b should be empty");
    }

    {
        let db = Database::builder(folder.path()).open()?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
        assert!(b.is_empty()?, "keyspace b should still be empty");
    }

    Ok(())
}

#[test]
fn snapshot_seqno_moves_only_on_publish() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;

    let initial_seqno = db.snapshot().seqno();

    let journal_lock = db.supervisor.journal.get_writer()?;
    let batch_seqno = db.supervisor.seqno.next();
    assert_eq!(initial_seqno, db.snapshot().seqno());

    db.supervisor
        .snapshot_tracker
        .publish(&journal_lock, batch_seqno);
    assert_eq!(initial_seqno + 1, db.snapshot().seqno());

    Ok(())
}

/// A write is in flight (journal lock held, sequence number taken, nothing
/// published) while a flush and a compaction of `tree` finish.
fn assert_installs_do_not_move_snapshot_seqno(db: &Database, tree: &Keyspace) -> crate::Result<()> {
    use lsm_tree::AbstractTree;

    tree.insert("a", "a")?;

    let journal_lock = db.supervisor.journal.get_writer()?;
    let batch_seqno = db.supervisor.seqno.next();
    let before = db.snapshot().seqno();

    tree.tree.rotate_memtable();
    let flush_lock = tree.tree.get_flush_lock();
    tree.tree.flush(&flush_lock, 0)?;
    drop(flush_lock);
    assert_eq!(
        before,
        db.snapshot().seqno(),
        "flush moved the snapshot seqno"
    );

    tree.tree.major_compact(64_000_000, 0)?;
    assert_eq!(
        before,
        db.snapshot().seqno(),
        "compaction moved the snapshot seqno"
    );

    // Publishing the write also publishes the installs that happened meanwhile
    db.supervisor
        .snapshot_tracker
        .publish(&journal_lock, batch_seqno);
    drop(journal_lock);
    assert!(
        db.snapshot().seqno() > batch_seqno + 1,
        "the installs were not published"
    );

    Ok(())
}

#[test]
fn installs_do_not_move_snapshot_seqno() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    assert_installs_do_not_move_snapshot_seqno(&db, &tree)
}

#[test]
fn installs_do_not_move_snapshot_seqno_kv_separated() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default()
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    })?;
    assert!(tree.is_kv_separated());

    assert_installs_do_not_move_snapshot_seqno(&db, &tree)
}

#[test]
fn installs_do_not_move_snapshot_seqno_after_reopen() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        tree.insert("k", "v")?;
    }

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    assert_installs_do_not_move_snapshot_seqno(&db, &tree)
}

#[test]
fn keyspace_creation_does_not_move_snapshot_seqno() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;

    let journal_lock = db.supervisor.journal.get_writer()?;
    let batch_seqno = db.supervisor.seqno.next();
    let before = db.snapshot().seqno();

    let creator = {
        let db = db.clone();
        std::thread::spawn(move || {
            db.keyspace("other", KeyspaceCreateOptions::default)
                .map(|_| ())
        })
    };
    while !db.keyspace_exists("other") {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert_eq!(
        before,
        db.snapshot().seqno(),
        "keyspace creation moved the snapshot seqno"
    );

    db.supervisor
        .snapshot_tracker
        .publish(&journal_lock, batch_seqno);
    drop(journal_lock);
    creator.join().expect("creator panicked")?;
    assert!(
        db.snapshot().seqno() > batch_seqno + 1,
        "the install was not published"
    );

    Ok(())
}

#[test]
fn gap_snapshot_survives_later_installs_and_gc() -> crate::Result<()> {
    use crate::Readable;
    use lsm_tree::AbstractTree;

    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    for i in 0..100u32 {
        tree.insert(i.to_be_bytes(), "old")?;
    }

    let journal_lock = db.supervisor.journal.get_writer()?;
    let batch_seqno = db.supervisor.seqno.next();
    tree.tree.rotate_memtable();
    let flush_lock = tree.tree.get_flush_lock();
    tree.tree.flush(&flush_lock, 0)?;
    drop(flush_lock);
    tree.tree.major_compact(64_000_000, 0)?;

    // Opened between the installs and their publication
    let gap = db.snapshot();
    assert!(
        gap.seqno() <= batch_seqno,
        "premise: the installs are not published yet"
    );

    db.supervisor
        .snapshot_tracker
        .publish(&journal_lock, batch_seqno);
    drop(journal_lock);

    // Each rotation also runs version GC (`Keyspace::inner_rotate_memtable`)
    for round in 0..5u32 {
        for i in 0..100u32 {
            tree.insert(i.to_be_bytes(), round.to_be_bytes())?;
        }
        tree.rotate_memtable_and_wait()?;
        tree.major_compact()?;
    }

    assert_eq!(100, gap.iter(&tree).count());
    for guard in gap.iter(&tree) {
        assert_eq!(b"old", &*guard.value()?);
    }

    Ok(())
}

#[test]
fn ingestion_finish_holds_the_journal_lock() -> crate::Result<()> {
    use lsm_tree::AbstractTree;

    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let journal_lock = db.supervisor.journal.get_writer()?;

    let ingester = {
        let tree = tree.clone();
        std::thread::spawn(move || -> crate::Result<()> {
            let mut ingestion = tree.start_ingestion()?;
            ingestion.write("a", "v")?;
            ingestion.finish()
        })
    };

    // finish() must wait for the journal lock before it installs anything
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert!(!ingester.is_finished());
    assert_eq!(0, tree.tree.table_count());

    drop(journal_lock);
    ingester.join().expect("ingester panicked")?;
    assert!(tree.tree.table_count() > 0);

    Ok(())
}
