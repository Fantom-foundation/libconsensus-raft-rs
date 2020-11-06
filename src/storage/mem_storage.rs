use raft::{
    eraftpb::{ConfChange, ConfState, Entry, HardState, Snapshot},
    storage::MemStorage,
    Error,
};

use super::{StorageExt, StorageType};

impl StorageExt for MemStorage {
    fn set_hard_state(&self, hard_state: &HardState) {
        self.wl().set_hardstate(hard_state.clone())
    }

    fn create_snapshot(
        &self,
        index: u64,
        conf_state: Option<&ConfState>,
        conf_change: Option<ConfChange>,
        data: Vec<u8>,
    ) -> Result<Snapshot, Error> {
        self.wl()
            .create_snapshot(index, conf_state.map(ConfState::clone), conf_change, data)
            .map(Snapshot::clone)
    }

    fn apply_snapshot(&self, snapshot: &Snapshot) -> Result<(), Error> {
        self.wl().apply_snapshot(snapshot.clone())
    }

    fn compact(&self, compact_index: u64) -> Result<(), Error> {
        self.wl().compact(compact_index)
    }

    fn append(&self, ents: &[Entry]) -> Result<(), Error> {
        self.wl().append(ents)
    }

    fn applied(&self) -> Result<u64, Error> {
        Ok(0)
    }

    fn set_applied(&self, _idx: u64) -> Result<(), Error> {
        Ok(())
    }

    fn describe() -> StorageType {
        StorageType::InMemory
    }
}
