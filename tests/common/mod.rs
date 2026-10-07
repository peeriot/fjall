#![allow(dead_code)]

/// Sorted names of the directories under `<db_folder>/keyspaces`.
pub fn keyspace_dirs(db_folder: &std::path::Path) -> std::io::Result<Vec<String>> {
    let mut names = std::fs::read_dir(db_folder.join("keyspaces"))?
        .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}
