#![allow(dead_code)]

/// Sorted names of the directories under `<db_folder>/keyspaces`.
pub fn keyspace_dirs(db_folder: &std::path::Path) -> std::io::Result<Vec<String>> {
    let mut names = std::fs::read_dir(db_folder.join("keyspaces"))?
        .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

/// Prepares a transaction that inserts `key` → `value` into `keyspace` under `xid`.
pub fn prepare_put(
    db: &fjall::OptimisticTxDatabase,
    keyspace: &fjall::OptimisticTxKeyspace,
    xid: &str,
    key: &str,
    value: &str,
) -> fjall::Result<Result<fjall::PrepareOutcome, fjall::Conflict>> {
    let mut tx = db.write_tx()?;
    tx.insert(keyspace, key, value);
    tx.prepare(xid)
}

/// Recursively copies the files under `from` into `to` (created as needed).
pub fn copy_dir(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}
