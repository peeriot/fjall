use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use test_log::test;

#[test]
fn recycled_keyspace_id_survives_reopen() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let recycled_id = {
        let db = Database::builder(&folder).open()?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        for i in 0..100u32 {
            a.insert(i.to_be_bytes(), "x")?;
        }
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
        let id = b.id();
        db.delete_keyspace(b)?; // last sequence-consuming operation of this run
        id
    };

    {
        let db = Database::builder(&folder).open()?;
        let c = db.keyspace("c", KeyspaceCreateOptions::default)?;
        assert_eq!(recycled_id, c.id(), "premise: c reuses b's id");
        c.insert("k", "v")?;
        let d = db.keyspace("d", KeyspaceCreateOptions::default)?;
        db.delete_keyspace(d)?; // makes the meta keyspace compact
        db.persist(PersistMode::SyncAll)?;
    }

    let db = Database::builder(&folder).open()?;
    assert!(db.keyspace_exists("c"));
    let c = db.keyspace("c", KeyspaceCreateOptions::default)?;
    assert_eq!(Some("v".as_bytes().into()), c.get("k")?);

    Ok(())
}

#[test]
fn cleared_keyspace_id_is_not_recycled_while_journalled() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let cleared_id = {
        let db = Database::builder(&folder).open()?;
        let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
        a.insert("k", "v")?;
        let b = db.keyspace("b", KeyspaceCreateOptions::default)?;
        let id = b.id();
        b.clear()?; // journalled as "clear keyspace <id>"
        db.delete_keyspace(b)?;
        id
    };

    {
        let db = Database::builder(&folder).open()?;
        let c = db.keyspace("c", KeyspaceCreateOptions::default)?;
        assert_ne!(cleared_id, c.id());

        // Ingested data is not journalled, so a replayed clear would wipe it
        let mut ingestion = c.start_ingestion()?;
        for key in ["x", "y", "z"] {
            ingestion.write(key, "ingested")?;
        }
        ingestion.finish()?;
        assert_eq!(3, c.len()?);
    }

    let db = Database::builder(&folder).open()?;
    let c = db.keyspace("c", KeyspaceCreateOptions::default)?;
    assert_eq!(3, c.len()?);

    Ok(())
}
