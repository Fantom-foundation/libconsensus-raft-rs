use std::fmt::{self, Display, Formatter};

use raft::{
    eraftpb::{ConfChange, ConfState, Entry, HardState, Snapshot},
    storage::Storage,
    Error,
};

pub mod cached_storage;
pub mod fs_storage;
pub mod mem_storage;

pub enum StorageType {
    Cached,
    InMemory,
    FileSystem,
}

impl Display for StorageType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            StorageType::Cached => write!(f, "Cached"),
            StorageType::InMemory => write!(f, "InMemory"),
            StorageType::FileSystem => write!(f, "FileSystem"),
        }
    }
}

pub trait StorageExt: Storage {
    // raft-rs Storage trait for version "0.5.0", added here for convenience

    /// `initial_state` is called when Raft is initialized. Returns a `RaftState` which contains `HardState`
    /// and `ConfState`. `RaftState` could be initialized or not. If it's initialized it means the `Storage`
    /// is created with a configuration, and its last index and term should be greater than 0.
    // fn initial_state(&self) -> Result<RaftState>;

    /// `entries` returns a slice of log entries in the range `[low, high)`. `max_size` limits the total
    /// size of the log entries returned, if not `None`, to have length >= 1 for entries in `[low, high)`.
    /// Panics if `high` is higher than `Storage::last_index(&self) + 1`.
    // fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>>;

    /// `term` returns the term of entry idx, which must be in the range [first_index()-1, last_index()].
    /// The term of the entry before first_index is retained for matching purpose even though the rest of
    /// that entry may not be available.
    // fn term(&self, idx: u64) -> Result<u64>;

    /// `first_index` returns the index of the first log entry in entries, which will always equal to
    /// `truncated index` plus 1. New created (but not initialized) `Storage` can be considered as
    /// truncated at 0 so that 1 will be returned in this case.
    // fn first_index(&self) -> Result<u64>;

    /// The index of the last entry replicated in the `Storage`.
    // fn last_index(&self) -> Result<u64>;

    /// `snapshot` returns the most recent snapshot. If snapshot is temporarily unavailable, it should
    /// return SnapshotTemporarilyUnavailable, so raft state machine could know that Storage needs some
    /// time to prepare snapshot and call snapshot later.
    // fn snapshot(&self) -> Result<Snapshot>;

    /// `set_hard_state` overwrites the HardState.
    fn set_hard_state(&self, hard_state: &HardState);

    /// `apply_snapshot` overwrites the contents of this Storage object with those of the given snapshot.
    fn apply_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error>;

    /// `create_snapshot` creates and applies a new snapshot, returning a clone of the created snapshot.
    fn create_snapshot(
        &self,
        index: u64,
        conf_state: Option<&ConfState>,
        conf_change: Option<ConfChange>,
        data: Vec<u8>,
    ) -> Result<Snapshot, Error>;

    /// `compact` discards all log entries prior to compact_index. It is the application's responsibility
    /// to not attempt to compact an index greater than RaftLog.applied.
    fn compact(&self, compact_index: u64) -> Result<(), Error>;

    /// `append` appends new entries to the replicated log.
    fn append(&self, entries: &[Entry]) -> Result<(), Error>;

    /// `applied` gets the index of the last raft entry that was committed.
    fn applied(&self) -> Result<u64, Error>;

    /// `set_applied` saves the index of the last raft entry that was committed.
    fn set_applied(&self, idx: u64) -> Result<(), Error>;

    /// `describe` returns a `StorageType` value describing the type of storage.
    fn describe() -> StorageType;
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use raft::{
        self,
        eraftpb::{ConfState, Entry, HardState},
        storage::{MemStorage, Storage},
    };

    const MAX: u64 = u64::max_value();

    fn create_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_term(term);
        e.set_index(index);
        e
    }

    fn create_entries(ids: Vec<u64>) -> Vec<Entry> {
        ids.into_iter().map(|i| create_entry(i, i)).collect()
    }

    fn populate_storage<S: StorageExt>(storage: &S, ids: Vec<u64>) -> Vec<Entry> {
        let entries = create_entries(ids);
        storage.append(&entries).unwrap();
        entries
    }

    pub fn test_storage_initial_state<S: StorageExt>(storage: S) {
        let raft_state = storage.initial_state().unwrap();

        assert_eq!(HardState::default(), raft_state.hard_state);
        assert_eq!(ConfState::default(), raft_state.conf_state);
    }

    pub fn test_storage_entries<S: StorageExt>(storage: S) {
        for i in 0..4 {
            for j in 0..4 {
                assert_eq!(
                    Err(raft::Error::Store(raft::StorageError::Compacted)),
                    storage.entries(i, j, MAX)
                );
            }
        }

        let entries = populate_storage(&storage, (1..4).collect());

        for i in 1..4 {
            for j in i..4 {
                assert_eq!(
                    Ok(entries[i - 1..j - 1].to_vec()),
                    storage.entries(i as u64, j as u64, MAX)
                );
            }
        }
    }

    pub fn test_storage_term<S: StorageExt>(storage: S) {
        assert_eq!(Ok(0), storage.term(0));

        let entries = populate_storage(&storage, (1..6).collect());

        for entry in entries {
            assert_eq!(Ok(entry.term), storage.term(entry.index));
        }

        storage.compact(3).unwrap();

        assert_eq!(
            Err(raft::Error::Store(raft::StorageError::Compacted)),
            storage.term(1)
        );
        assert_eq!(
            Err(raft::Error::Store(raft::StorageError::Compacted)),
            storage.term(2)
        );
        assert_eq!(Ok(3), storage.term(3));
        assert_eq!(Ok(4), storage.term(4));
    }

    pub fn test_first_and_last_index<S: StorageExt>(storage: S) {
        assert_eq!(Ok(1), storage.first_index());
        assert_eq!(Ok(0), storage.last_index());

        populate_storage(&storage, (1..6).collect());

        assert_eq!(Ok(1), storage.first_index());
        assert_eq!(Ok(5), storage.last_index());

        storage.compact(3).unwrap();

        assert_eq!(Ok(4), storage.first_index());
        assert_eq!(Ok(5), storage.last_index());
    }

    pub fn test_storage_ext_compact<S: StorageExt>(storage: S) {
        assert_eq!(
            Err(raft::Error::Store(raft::StorageError::Compacted)),
            storage.compact(0)
        );

        let entries = populate_storage(&storage, (1..11).collect());

        assert_eq!(
            Err(raft::Error::Store(raft::StorageError::Compacted)),
            storage.compact(0)
        );

        assert_eq!(Ok(()), storage.compact(2));
        assert_eq!(
            Err(raft::Error::Store(raft::StorageError::Compacted)),
            storage.entries(1, 3, MAX)
        );
        assert_eq!(Ok(entries[2..9].to_vec()), storage.entries(3, 10, MAX));

        assert_eq!(Ok(()), storage.compact(4));
        assert_eq!(
            Err(raft::Error::Store(raft::StorageError::Compacted)),
            storage.entries(2, 5, MAX)
        );
        assert_eq!(Ok(entries[4..9].to_vec()), storage.entries(5, 10, MAX));
    }

    pub fn test_last_committed_index<S: StorageExt>(storage: S) {
        assert_eq!(Ok(0), storage.applied());

        storage.set_applied(1).unwrap();
        assert_eq!(Ok(1), storage.applied());
        storage.set_applied(2).unwrap();
        assert_eq!(Ok(2), storage.applied());
    }

    pub fn test_parity<S: StorageExt>(storage: S) {
        let mem_storage = MemStorage::new();

        assert_eq!(mem_storage.term(0), storage.term(0));
        assert_eq!(mem_storage.term(1), storage.term(1));
        assert_eq!(mem_storage.term(2), storage.term(2));
        assert_eq!(mem_storage.last_index(), storage.last_index());
        assert_eq!(mem_storage.first_index(), storage.first_index());

        for i in 0..3 {
            assert_eq!(mem_storage.entries(0, i, MAX), storage.entries(0, i, MAX));
        }

        populate_storage(&mem_storage, (1..6).collect());
        populate_storage(&storage, (1..6).collect());

        assert_eq!(mem_storage.first_index(), storage.first_index());
        assert_eq!(mem_storage.last_index(), storage.last_index());

        for i in 1..6 {
            for j in i..6 {
                assert_eq!(mem_storage.entries(i, j, MAX), storage.entries(i, j, MAX));
            }
            assert_eq!(mem_storage.term(i), storage.term(i));
        }

        assert_eq!(mem_storage.snapshot(), storage.snapshot());

        let mem_snapshot = mem_storage
            .create_snapshot(3, None, None, "".into())
            .expect("MemStorage: Create snapshot failed");
        let fs_snapshot = storage
            .create_snapshot(3, None, None, "".into())
            .expect("FsStorage: Create snapshot failed");
        assert_eq!(mem_snapshot, fs_snapshot);

        assert_eq!(
            mem_storage.apply_snapshot(&mem_snapshot),
            storage.apply_snapshot(&fs_snapshot),
        );

        for i in 2..5 {
            assert_eq!(mem_storage.compact(i), storage.compact(i));

            assert_eq!(mem_storage.first_index(), storage.first_index());
            assert_eq!(mem_storage.last_index(), storage.last_index());
        }

        assert_eq!(mem_storage.snapshot(), storage.snapshot());
    }
}
