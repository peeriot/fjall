// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Two-phase commit: the encoding of prepared rows in the hidden `__fjall_txn` keyspace.
//!
//! One row per write-set item:
//!
//! ```text
//! key   = xid_len (u16 BE) ‖ xid ‖ target keyspace id (u64 BE) ‖ user key
//! value = format version (u8) ‖ operation tag (u8) ‖ user value
//! ```

use crate::{
    keyspace::{name::is_reserved_keyspace_name, InternalKeyspaceId, KeyspaceKey},
    worker_pool::WorkerMessage,
    Database, Keyspace,
};
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use lsm_tree::{AbstractTree, Guard, SeqNo, Slice, ValueType};

/// Outcome of a successful prepare vote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrepareOutcome {
    /// Write-set durably recorded; resolve it with `commit_prepared` or `rollback_prepared`.
    Prepared,

    /// Empty write-set: validated but nothing recorded (XA read-only optimization).
    ///
    /// Requires no phase 2; it will never be reported as pending.
    ReadOnly,
}

/// Outcome of a successful resolution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The prepared write-set was applied (commit) or discarded (rollback) by THIS call.
    Applied {
        /// Items skipped because their target keyspace no longer exists
        /// (always 0 for `rollback_prepared`)
        dropped_items: usize,
    },

    /// Nothing to do: the xid is unknown or already resolved.
    ///
    /// Safe on retries; a `NoOp` on the FIRST resolution attempt of a branch that
    /// voted `Prepared` indicates lost prepared state or out-of-band interference.
    NoOp,
}

/// An in-doubt prepared transaction, as reported by `pending_prepared`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedTxInfo {
    /// Application-supplied transaction id
    pub xid: Slice,

    /// Number of items in the write-set
    pub item_count: usize,

    /// Names of the target keyspaces that exist, sorted and deduplicated
    pub keyspaces: Vec<KeyspaceKey>,

    /// Internal ids of the target keyspaces that no longer exist (deleted since the
    /// prepare), sorted and deduplicated
    ///
    /// `commit_prepared` drops the items that target them.
    pub missing_keyspace_ids: Vec<InternalKeyspaceId>,
}

/// One item of the durable write-set of an in-doubt prepared transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedItem {
    /// Name of the target keyspace; `None` if it was deleted
    pub keyspace: Option<KeyspaceKey>,

    /// Internal id of the target keyspace
    pub keyspace_id: InternalKeyspaceId,

    /// The written key
    pub key: Slice,

    /// The kind of write
    pub kind: PreparedItemKind,
}

/// Kind of a prepared write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PreparedItemKind {
    /// A write by `insert`
    Put,

    /// A deletion by `remove`
    Delete,

    /// A weak deletion by `remove_weak`
    WeakDelete,
}

impl PreparedItemKind {
    /// The kind of a write-set item of the given value type.
    ///
    /// A transaction never writes indirections, so `ValueType::Indirection` is unreachable.
    pub(crate) fn from_value_type(value_type: ValueType) -> Self {
        match value_type {
            ValueType::Value => Self::Put,
            ValueType::Tombstone => Self::Delete,
            ValueType::WeakTombstone => Self::WeakDelete,
            ValueType::Indirection => unreachable!("a transaction never writes an indirection"),
        }
    }

    /// The value type to apply this kind of write with.
    pub(crate) fn value_type(self) -> ValueType {
        match self {
            Self::Put => ValueType::Value,
            Self::Delete => ValueType::Tombstone,
            Self::WeakDelete => ValueType::WeakTombstone,
        }
    }

    /// The operation tag stored in a row value.
    fn tag(self) -> u8 {
        match self {
            Self::Put => TAG_PUT,
            Self::Delete => TAG_DELETE,
            Self::WeakDelete => TAG_WEAK_DELETE,
        }
    }
}

/// The format version byte that starts every row value.
pub const ROW_FORMAT_VERSION: u8 = 1;

/// Operation tag of a prepared `insert`.
pub const TAG_PUT: u8 = 0;

/// Operation tag of a prepared `remove`.
pub const TAG_DELETE: u8 = 1;

/// Operation tag of a prepared `remove_weak`.
pub const TAG_WEAK_DELETE: u8 = 2;

/// Longest xid `prepare` accepts.
pub const MAX_XID_LEN: usize = 255;

/// Bytes a row key adds to xid and user key: xid length (u16) + target keyspace id (u64).
pub const ROW_KEY_OVERHEAD: usize = 10;

/// Longest key lsm-tree accepts.
pub const MAX_ROW_KEY_LEN: usize = 65_535;

/// Longest user value a row can hold: the row value adds 2 bytes and must fit a `u32` length.
pub const MAX_PREPARED_VALUE_LEN: usize = 4_294_967_293; // u32::MAX - 2

/// Lowest keyspace id that is never issued: lsm-tree's id counter reserves the most
/// significant bit and panics instead of issuing it.
pub const FIRST_IMPOSSIBLE_KEYSPACE_ID: InternalKeyspaceId = 0x8000_0000_0000_0000;

/// The decoded key of a `__fjall_txn` row.
pub struct RowKey {
    /// The prepared transaction's xid
    pub xid: Slice,

    /// Id of the keyspace the write targets
    pub keyspace_id: InternalKeyspaceId,

    /// The written key in the target keyspace
    pub user_key: Slice,
}

fn corrupt() -> crate::Error {
    crate::Error::Unrecoverable
}

/// Encodes `xid_len (u16 BE) ‖ xid`, the key prefix shared by all rows of `xid`.
///
/// Returns `None` if the xid is longer than `u16::MAX` bytes; no row can have such an xid.
pub fn xid_prefix(xid: &[u8]) -> Option<Slice> {
    let xid_len = u16::try_from(xid.len()).ok()?;

    let mut prefix = Vec::with_capacity(2 + xid.len());
    prefix.extend_from_slice(&xid_len.to_be_bytes());
    prefix.extend_from_slice(xid);

    Some(prefix.into())
}

/// Encodes a row key: `xid_len (u16 BE) ‖ xid ‖ keyspace_id (u64 BE) ‖ user_key`.
///
/// The caller guarantees that the xid is at most [`MAX_XID_LEN`] bytes long.
pub fn encode_row_key(xid: &[u8], keyspace_id: InternalKeyspaceId, user_key: &[u8]) -> Slice {
    #[expect(clippy::expect_used, reason = "prepare asserts the xid length first")]
    let xid_len = u16::try_from(xid.len()).expect("xid should be at most u16::MAX bytes long");

    let mut key = Vec::with_capacity(ROW_KEY_OVERHEAD + xid.len() + user_key.len());

    // NOTE: Writing into a Vec cannot fail
    let _ = key.write_u16::<BE>(xid_len);
    key.extend_from_slice(xid);
    let _ = key.write_u64::<BE>(keyspace_id);
    key.extend_from_slice(user_key);

    key.into()
}

/// Decodes a row key.
///
/// # Errors
///
/// Returns `Error::Unrecoverable` if the key is too short for its own layout,
/// which includes a non-empty user key.
pub fn decode_row_key(key: &[u8]) -> crate::Result<RowKey> {
    let mut reader = key;

    let xid_len = reader.read_u16::<BE>().map_err(|_| corrupt())?;

    let (xid, mut rest) = reader
        .split_at_checked(usize::from(xid_len))
        .ok_or_else(corrupt)?;

    let keyspace_id = rest.read_u64::<BE>().map_err(|_| corrupt())?;

    if rest.is_empty() {
        return Err(corrupt());
    }

    Ok(RowKey {
        xid: xid.into(),
        keyspace_id,
        user_key: rest.into(),
    })
}

/// Encodes a row value: `ROW_FORMAT_VERSION ‖ tag ‖ user_value`.
pub fn encode_row_value(kind: PreparedItemKind, user_value: &[u8]) -> Slice {
    let mut value = Vec::with_capacity(2 + user_value.len());
    value.push(ROW_FORMAT_VERSION);
    value.push(kind.tag());
    value.extend_from_slice(user_value);
    value.into()
}

/// Decodes a row value into its operation kind and user value.
///
/// # Errors
///
/// Returns `Error::Unrecoverable` if the value is shorter than 2 bytes, or has an
/// unknown format version or operation tag.
pub fn decode_row_value(value: &[u8]) -> crate::Result<(PreparedItemKind, Slice)> {
    let mut reader = value;

    let version = reader.read_u8().map_err(|_| corrupt())?;
    if version != ROW_FORMAT_VERSION {
        return Err(corrupt());
    }

    let kind = match reader.read_u8().map_err(|_| corrupt())? {
        TAG_PUT => PreparedItemKind::Put,
        TAG_DELETE => PreparedItemKind::Delete,
        TAG_WEAK_DELETE => PreparedItemKind::WeakDelete,
        _ => return Err(corrupt()),
    };

    Ok((kind, reader.into()))
}

/// Checks a write-set item against the size limits of a prepared row.
///
/// Decided on lengths alone, so it runs before any row or batch item is built.
///
/// # Errors
///
/// Returns `Error::PreparedItemTooLarge` if the row key would be longer than
/// [`MAX_ROW_KEY_LEN`] (`ROW_KEY_OVERHEAD + xid_len + user_key_len`), or the user
/// value is longer than [`MAX_PREPARED_VALUE_LEN`].
pub fn check_item_size(
    xid_len: usize,
    user_key_len: usize,
    user_value_len: usize,
) -> crate::Result<()> {
    let row_key_len = ROW_KEY_OVERHEAD
        .saturating_add(xid_len)
        .saturating_add(user_key_len);

    if row_key_len > MAX_ROW_KEY_LEN || user_value_len > MAX_PREPARED_VALUE_LEN {
        return Err(crate::Error::PreparedItemTooLarge);
    }

    Ok(())
}

/// One decoded `__fjall_txn` row.
pub struct PreparedRow {
    /// The stored composite key, tombstoned at resolution
    pub row_key: Slice,

    /// The prepared transaction's xid
    pub xid: Slice,

    /// Id of the keyspace the write targets
    pub keyspace_id: InternalKeyspaceId,

    /// The written key in the target keyspace
    pub user_key: Slice,

    /// The kind of write
    pub kind: PreparedItemKind,

    /// The written value; empty for deletions
    pub user_value: Slice,
}

/// Reads every row under `prefix` (all rows if `prefix` is empty) at `seqno`, in key order.
///
/// # Errors
///
/// Returns `Error::Unrecoverable` if a row cannot be decoded, or an error if an I/O
/// error occurred.
pub fn read_rows(txn: &Keyspace, prefix: &[u8], seqno: SeqNo) -> crate::Result<Vec<PreparedRow>> {
    let iter = if prefix.is_empty() {
        txn.tree.iter(seqno, None)
    } else {
        txn.tree.prefix(prefix, seqno, None)
    };

    let mut rows = Vec::new();

    for guard in iter {
        let (row_key, value) = guard.into_inner()?;

        let decoded = decode_row_key(&row_key)
            .and_then(|key| decode_row_value(&value).map(|value| (key, value)));

        let Ok((key, (kind, user_value))) = decoded else {
            log::error!(
                "Undecodable row in the prepared transactions keyspace (key={row_key:?}): the keyspace is corrupt",
            );
            return Err(crate::Error::Unrecoverable);
        };

        rows.push(PreparedRow {
            row_key,
            xid: key.xid,
            keyspace_id: key.keyspace_id,
            user_key: key.user_key,
            kind,
            user_value,
        });
    }

    Ok(rows)
}

/// Returns `true` if any row of `xid` exists, read at `SeqNo::MAX`. Decodes nothing.
///
/// # Errors
///
/// Returns an error if an I/O error occurred.
pub fn has_rows(txn: &Keyspace, xid: &[u8]) -> crate::Result<bool> {
    let Some(prefix) = xid_prefix(xid) else {
        return Ok(false);
    };

    match txn.tree.prefix(prefix, SeqNo::MAX, None).next() {
        Some(guard) => {
            guard.key()?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Live keyspaces by id, from the supervisor's keyspace map, without the internal
/// keyspaces: a prepared row never targets one, so it is resolved like a deleted
/// keyspace.
///
/// The map's read guard is released on return, so the result can be used across
/// a batch commit.
pub fn live_keyspaces_by_id(db: &Database) -> crate::HashMap<InternalKeyspaceId, Keyspace> {
    #[expect(clippy::expect_used)]
    let keyspaces = db.supervisor.keyspaces.read().expect("lock is poisoned");

    keyspaces
        .values()
        .filter(|keyspace| !is_reserved_keyspace_name(&keyspace.name))
        .map(|keyspace| (keyspace.id, keyspace.clone()))
        .collect()
}

/// Asks the worker pool to compact `txn`, so resolved rows are garbage collected.
///
/// Never blocks; if the request cannot be queued, it is dropped.
pub fn request_compaction(txn: &Keyspace) {
    txn.worker_messager
        .try_send(WorkerMessage::Compact(txn.clone()))
        .ok();
}

/// Highest target keyspace id of any live row, read at `SeqNo::MAX` from memtables
/// and tables.
///
/// A row whose key cannot be decoded is logged at error level and skipped: it can
/// never be applied, and failing here would make the database unopenable. So is a
/// row whose target id is within one of [`FIRST_IMPOSSIBLE_KEYSPACE_ID`] or above it:
/// raising the counter to that value would make every later keyspace creation panic,
/// and no real database gets anywhere near such an id.
///
/// # Errors
///
/// Returns an error if an I/O error occurred.
pub fn max_target_keyspace_id(txn: &Keyspace) -> crate::Result<Option<InternalKeyspaceId>> {
    let mut max_id = None;

    for guard in txn.tree.iter(SeqNo::MAX, None) {
        let row_key = guard.key()?;

        match decode_row_key(&row_key) {
            // NOTE: The caller raises the counter to `id + 1`, so the last issuable id is skipped too
            Ok(key) if key.keyspace_id < FIRST_IMPOSSIBLE_KEYSPACE_ID - 1 => {
                max_id = max_id.max(Some(key.keyspace_id));
            }
            Ok(key) => log::error!(
                "Skipping row with impossible target keyspace id {} in the prepared transactions keyspace (key={row_key:?})",
                key.keyspace_id,
            ),
            Err(_) => log::error!(
                "Skipping undecodable row in the prepared transactions keyspace (key={row_key:?})",
            ),
        }
    }

    Ok(max_id)
}

/// Maps the error of a failed 2PC batch commit to `Error::Poisoned` if the database
/// is now poisoned; otherwise returns `error` unchanged.
///
/// `Poisoned` is the only error after which a prepared xid may resurface, so
/// every failure that poisoned the database must be reported as such.
pub fn normalize_batch_error(db: &Database, error: crate::Error) -> crate::Error {
    if db.is_poisoned() {
        crate::Error::Poisoned
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;
    use PreparedItemKind::{Delete, Put, WeakDelete};

    #[test]
    fn row_key_layout() -> crate::Result<()> {
        let key = encode_row_key(b"ab", 0x0102, b"k");
        assert_eq!(&[0, 2, b'a', b'b', 0, 0, 0, 0, 0, 0, 1, 2, b'k'][..], &*key);
        assert_eq!(Some(&[0, 2, b'a', b'b'][..]), xid_prefix(b"ab").as_deref());
        Ok(())
    }

    #[test]
    fn row_key_round_trips_at_the_boundaries() -> crate::Result<()> {
        let long_xid = vec![0xFF; MAX_XID_LEN];
        for xid in [&b"x"[..], &[0u8, 1, 2][..], &long_xid[..]] {
            let longest_key = vec![7u8; MAX_ROW_KEY_LEN - ROW_KEY_OVERHEAD - xid.len()];
            for user_key in [&b"k"[..], &longest_key[..]] {
                let key = encode_row_key(xid, 42, user_key);
                assert!(key.len() <= MAX_ROW_KEY_LEN);
                assert!(key.starts_with(&*xid_prefix(xid).expect("encodable")));
                let row = decode_row_key(&key)?;
                assert_eq!(xid, &*row.xid);
                assert_eq!(42, row.keyspace_id);
                assert_eq!(user_key, &*row.user_key);
            }
        }
        Ok(())
    }

    #[test]
    fn row_value_layout_and_round_trip() -> crate::Result<()> {
        assert_eq!(&[1, 0, b'v'][..], &*encode_row_value(Put, b"v"));
        assert_eq!(&[1, 1][..], &*encode_row_value(Delete, b""));
        assert_eq!(&[1, 2][..], &*encode_row_value(WeakDelete, b""));
        for kind in [Put, Delete, WeakDelete] {
            let (decoded, value) = decode_row_value(&encode_row_value(kind, b"v"))?;
            assert_eq!(kind, decoded);
            assert_eq!(b"v", &*value);
            assert_eq!(kind, PreparedItemKind::from_value_type(kind.value_type()));
        }
        Ok(())
    }

    #[test]
    fn undecodable_bytes_are_unrecoverable() {
        use crate::Error::Unrecoverable;
        assert!(matches!(decode_row_key(&[0]), Err(Unrecoverable)));
        assert!(matches!(
            decode_row_key(&[0, 5, b'x', 0, 0, 0, 0, 0, 0, 0, 1, b'k']),
            Err(Unrecoverable)
        ));
        // no room for a user key
        assert!(matches!(
            decode_row_key(&[0, 1, b'x', 0, 0, 0, 0, 0, 0, 0, 1]),
            Err(Unrecoverable)
        ));
        assert!(matches!(decode_row_value(&[1]), Err(Unrecoverable)));
        assert!(matches!(decode_row_value(&[2, 0]), Err(Unrecoverable)));
        assert!(matches!(decode_row_value(&[1, 3]), Err(Unrecoverable)));
        assert!(matches!(decode_row_value(&[1, 4]), Err(Unrecoverable)));
    }

    #[test]
    fn item_size_limits() {
        use crate::Error::PreparedItemTooLarge;
        assert!(check_item_size(1, 65_524, 0).is_ok());
        assert!(matches!(
            check_item_size(1, 65_525, 0),
            Err(PreparedItemTooLarge)
        ));
        assert!(check_item_size(255, 65_270, 0).is_ok());
        assert!(matches!(
            check_item_size(255, 65_271, 0),
            Err(PreparedItemTooLarge)
        ));
        assert!(check_item_size(1, 1, 4_294_967_293).is_ok());
        assert!(matches!(
            check_item_size(1, 1, 4_294_967_294),
            Err(PreparedItemTooLarge)
        ));
    }

    #[test]
    fn xid_length_prefix_separates_xids() {
        let prefix_a = xid_prefix(b"a").expect("encodable");
        let prefix_ab = xid_prefix(b"ab").expect("encodable");
        assert!(!encode_row_key(b"ab", 1, b"k").starts_with(&*prefix_a));
        assert!(!encode_row_key(b"a", 1, b"k").starts_with(&*prefix_ab));
        assert!(xid_prefix(&vec![0; 65_535]).is_some());
        assert!(xid_prefix(&vec![0; 65_536]).is_none());
    }
}
