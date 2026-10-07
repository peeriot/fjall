use fjall::{Database, PersistMode};
use test_log::test;

#[test]
fn db_journal_recovery_no_journal_files() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    for dirent in std::fs::read_dir(&folder)? {
        let path = dirent?.path();

        if path.extension().is_some_and(|ext| ext == "jnl") {
            std::fs::remove_file(path)?;
        }
    }

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", Default::default)?;
        tree.insert("hello", "world")?;
        db.persist(PersistMode::SyncAll)?;
    }

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", Default::default)?;
        assert_eq!(Some("world".as_bytes().into()), tree.get("hello")?);
    }

    Ok(())
}

#[test]
fn db_journal_recovery_no_journal_files_restores_seqno() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let k_seqno = {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", Default::default)?;
        let k_seqno = db.seqno();
        tree.insert("k", "old")?;
        tree.rotate_memtable_and_wait()?;
        k_seqno
    };

    for dirent in std::fs::read_dir(&folder)? {
        let path = dirent?.path();

        if path.extension().is_some_and(|ext| ext == "jnl") {
            std::fs::remove_file(path)?;
        }
    }

    let db = Database::builder(&folder).open()?;
    assert!(
        db.seqno() > k_seqno,
        "seqno={} k_seqno={k_seqno}",
        db.seqno()
    );

    Ok(())
}
