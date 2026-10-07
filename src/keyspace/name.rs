// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Keyspace names starting with this prefix are reserved for internal keyspaces.
pub const RESERVED_KEYSPACE_PREFIX: &str = "__fjall";

/// Name of the internal keyspace that holds prepared (two-phase commit) write-sets.
pub const TXN_KEYSPACE_NAME: &str = "__fjall_txn";

/// Returns `true` if the name is reserved for an internal keyspace,
/// i.e. starts with [`RESERVED_KEYSPACE_PREFIX`].
#[expect(clippy::module_name_repetitions)]
pub fn is_reserved_keyspace_name(s: &str) -> bool {
    s.starts_with(RESERVED_KEYSPACE_PREFIX)
}

/// Keyspace names can be up to 255 characters long, can not be empty and
/// can only contain alphanumerics, underscore (`_`), dash (`-`), dot (`.`), hash tag (`#`) and dollar (`$`),
/// and can not start with `__fjall`, which is reserved.
#[expect(clippy::module_name_repetitions)]
pub fn is_valid_keyspace_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    if is_reserved_keyspace_name(s) {
        return false;
    }

    u8::try_from(s.len()).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_prefix_is_rejected() {
        for name in ["__fjall", "__fjall_txn", "__fjallx", "__fjall_anything"] {
            assert!(is_reserved_keyspace_name(name), "{name}");
            assert!(!is_valid_keyspace_name(name), "{name}");
        }
        for name in ["_fjall", "__fjal", "x__fjall", "__FJALL", "default"] {
            assert!(!is_reserved_keyspace_name(name), "{name}");
            assert!(is_valid_keyspace_name(name), "{name}");
        }
        assert!(is_reserved_keyspace_name(TXN_KEYSPACE_NAME));
    }
}
