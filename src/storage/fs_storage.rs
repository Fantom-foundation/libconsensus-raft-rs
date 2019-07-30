use std::{
    fs, io, mem,
    ops::Range,
    path::{Path, PathBuf},
};

use protobuf::{self, Message as ProtobufMessage};

use raft::{
    self,
    eraftpb::{ConfChange, ConfState, Entry, HardState, Snapshot},
    storage::{MemStorage, Storage},
    RaftState,
};

use super::{StorageExt, StorageType};

pub struct FsStorage {
    data_dir: PathBuf,
    entries_dir: PathBuf,
}

impl FsStorage {
    pub fn with_data_dir(data_dir: PathBuf) -> io::Result<Self> {
        let entries_dir = Path::new(data_dir.as_path()).join("entries");

        fs::create_dir_all(&entries_dir)?;

        init_raft_state_if_missing(&data_dir)?;

        Ok(FsStorage {
            data_dir,
            entries_dir,
        })
    }
}

impl Storage for FsStorage {
    fn initial_state(&self) -> Result<RaftState, raft::Error> {
        Ok(read_raft_state(&self.data_dir)?)
    }

    fn entries(&self, low: u64, high: u64, _max_size: u64) -> Result<Vec<Entry>, raft::Error> {
        if low > high {
            return err_compacted();
        }

        let first_entry_index = read_first_index(&self.data_dir)?;
        let last_entry_index = read_last_index(&self.data_dir)?;

        if first_entry_index > last_entry_index {
            return err_compacted();
        }

        if low == high {
            return Ok(Vec::new());
        }

        if low < first_entry_index {
            return err_compacted();
        }

        if high > last_entry_index + 1 {
            return err_unavailable();
        }

        Ok(read_entries(&self.entries_dir, Some(low..high))?)
    }

    fn term(&self, idx: u64) -> Result<u64, raft::Error> {
        match read_entry(&self.entries_dir, idx) {
            Ok(entry) => Ok(entry.term),
            Err(err) => {
                if err.kind() == io::ErrorKind::NotFound {
                    if idx == 0 {
                        Ok(0)
                    } else if (idx + 1) == self.first_index()? {
                        match read_compacted_term(&self.data_dir)? {
                            0 => err_unavailable(),
                            compacted => Ok(compacted),
                        }
                    } else {
                        match read_compacted_term(&self.data_dir)? {
                            0 => err_unavailable(),
                            _ => err_compacted(),
                        }
                    }
                } else {
                    Err(raft::Error::from(err))
                }
            }
        }
    }

    fn first_index(&self) -> Result<u64, raft::Error> {
        Ok(read_first_index(&self.data_dir)?)
    }

    fn last_index(&self) -> Result<u64, raft::Error> {
        Ok(read_last_index(&self.data_dir)?)
    }

    fn snapshot(&self) -> Result<Snapshot, raft::Error> {
        Ok(read_snapshot(&self.data_dir)?)
    }
}

impl StorageExt for FsStorage {
    fn set_hard_state(&self, hard_state: &HardState) {
        write_hard_state(&self.data_dir, hard_state).expect("Failed to set hard_state");
    }

    fn create_snapshot(
        &self,
        index: u64,
        conf_state: Option<&ConfState>,
        _conf_change: Option<ConfChange>,
        data: Vec<u8>,
    ) -> Result<Snapshot, raft::Error> {
        let mut snapshot = self.snapshot()?;

        if index <= snapshot.get_metadata().get_index() {
            return err_snapshot_out_of_date();
        }

        let last_index = read_last_index(&self.data_dir)?;
        if index > last_index {
            panic!(
                "Tried to create snapshot with index {}, but last index is {}",
                index, last_index,
            );
        }

        snapshot.mut_metadata().set_index(index);

        let term = read_entry(&self.entries_dir, index)
            .expect("Entry log integrity error: Entry not found, but already checked bounds.")
            .get_term();

        snapshot.mut_metadata().set_term(term);

        if let Some(cs) = conf_state {
            snapshot.mut_metadata().set_conf_state(cs.clone())
        }

        snapshot.set_data(data);

        write_snapshot(&self.data_dir, &snapshot)?;

        Ok(snapshot)
    }

    fn apply_snapshot(&self, snapshot: &Snapshot) -> Result<(), raft::Error> {
        let current = self.snapshot()?;

        if current.get_metadata().get_index() >= snapshot.get_metadata().get_index() {
            return err_snapshot_out_of_date();
        }

        let compact_index = snapshot.get_metadata().get_index();
        self.compact(compact_index)?;

        write_snapshot(&self.data_dir, &snapshot)?;

        Ok(())
    }

    fn compact(&self, compact_index: u64) -> Result<(), raft::Error> {
        let first_entry_index = read_first_index(&self.data_dir)?;

        if first_entry_index > compact_index {
            return err_compacted();
        }

        let delete: Vec<Entry> = read_entries(
            &self.entries_dir,
            Some(first_entry_index..(compact_index + 1)),
        )?;

        if let Some(last) = delete.last() {
            write_compacted_term(&self.data_dir, last.get_term())?;
            write_first_index(&self.data_dir, last.get_index() + 1)?;
        }

        delete
            .into_iter()
            .map(|entry| remove_entry(&self.entries_dir, entry.index))
            .collect::<Result<(), io::Error>>()?;

        Ok(())
    }

    fn append(&self, entries: &[Entry]) -> Result<(), raft::Error> {
        entries
            .iter()
            .map(|entry| write_entry(&self.entries_dir, entry))
            .collect::<Result<Vec<()>, io::Error>>()?;

        if let Some(last_entry) = entries.last() {
            write_last_index(&self.data_dir, last_entry.get_index())?;
        }

        Ok(())
    }

    fn applied(&self) -> Result<u64, raft::Error> {
        Ok(read_applied(&self.data_dir)?)
    }

    fn set_applied(&self, idx: u64) -> Result<(), raft::Error> {
        write_applied(&self.data_dir, idx)?;
        Ok(())
    }

    fn describe() -> StorageType {
        StorageType::FileSystem
    }
}

fn init_raft_state_if_missing<P: AsRef<Path>>(data_dir: P) -> io::Result<()> {
    if let Err(err) = read_raft_state(&data_dir) {
        if err.kind() == io::ErrorKind::NotFound {
            return init_raft_state(&data_dir);
        }
    }
    Ok(())
}

fn err_compacted<T>() -> Result<T, raft::Error> {
    Err(raft::Error::Store(raft::StorageError::Compacted))
}

fn err_unavailable<T>() -> Result<T, raft::Error> {
    Err(raft::Error::Store(raft::StorageError::Unavailable))
}

fn err_snapshot_out_of_date<T>() -> Result<T, raft::Error> {
    Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate))
}

fn read_raft_state<P: AsRef<Path>>(data_dir: P) -> io::Result<RaftState> {
    let mem_storage = MemStorage::new();
    let mut raft_state = mem_storage.initial_state().unwrap();

    raft_state.hard_state = read_hard_state(&data_dir)?;
    raft_state.conf_state = read_conf_state(&data_dir)?;

    Ok(raft_state)
}

fn read_hard_state<P: AsRef<Path>>(data_dir: P) -> io::Result<HardState> {
    read_pb_from_file(data_dir.as_ref().join("hard_state"))
}

fn read_conf_state<P: AsRef<Path>>(data_dir: P) -> io::Result<ConfState> {
    if let Some(mut metadata) = read_snapshot(data_dir)?.metadata.take() {
        if let Some(conf_state) = metadata.conf_state.take() {
            return Ok(conf_state);
        }
    }

    Ok(ConfState::new())
}

fn read_snapshot<P: AsRef<Path>>(data_dir: P) -> io::Result<Snapshot> {
    read_pb_from_file(data_dir.as_ref().join("snapshot"))
}

fn read_compacted_term<P: AsRef<Path>>(data_dir: P) -> io::Result<u64> {
    read_u64_from_file(data_dir.as_ref().join("term"))
}

fn read_first_index<P: AsRef<Path>>(data_dir: P) -> io::Result<u64> {
    read_u64_from_file(data_dir.as_ref().join("first"))
}

fn read_last_index<P: AsRef<Path>>(data_dir: P) -> io::Result<u64> {
    read_u64_from_file(data_dir.as_ref().join("last"))
}

fn read_applied<P: AsRef<Path>>(data_dir: P) -> io::Result<u64> {
    read_u64_from_file(data_dir.as_ref().join("applied"))
}

fn read_entry<P: AsRef<Path>>(entries_dir: P, index: u64) -> io::Result<Entry> {
    read_pb_from_file(entries_dir.as_ref().join(format!("{}", index)))
}

fn read_entries<P: AsRef<Path>>(
    entries_dir: P,
    range: Option<Range<u64>>,
) -> io::Result<Vec<Entry>> {
    match range {
        Some(range) => range
            .map(|index| read_entry(entries_dir.as_ref(), index))
            .collect(),
        None => {
            let mut entries = read_and_map_dir(entries_dir.as_ref(), dir_entry_to_raft_entry)?;
            entries.sort_unstable_by(|a, b| a.index.cmp(&b.index));
            Ok(entries)
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn dir_entry_to_raft_entry(dir_entry: fs::DirEntry) -> io::Result<Entry> {
    read_pb_from_file(dir_entry.path())
}

fn init_raft_state<P: AsRef<Path>>(data_dir: P) -> io::Result<()> {
    write_compacted_term(&data_dir, 0)?;
    write_first_index(&data_dir, 1)?;
    write_last_index(&data_dir, 0)?;
    write_applied(&data_dir, 0)?;
    write_hard_state(&data_dir, &HardState::new())?;
    write_snapshot(&data_dir, &Snapshot::new())
}

fn write_hard_state<P: AsRef<Path>>(data_dir: P, hard_state: &HardState) -> io::Result<()> {
    write_pb_to_file(data_dir.as_ref().join("hard_state"), hard_state)
}

fn write_snapshot<P: AsRef<Path>>(data_dir: P, snapshot: &Snapshot) -> io::Result<()> {
    write_pb_to_file(data_dir.as_ref().join("snapshot"), snapshot)
}

fn write_compacted_term<P: AsRef<Path>>(data_dir: P, term: u64) -> io::Result<()> {
    write_u64_to_file(data_dir.as_ref().join("term"), term)
}

fn write_first_index<P: AsRef<Path>>(data_dir: P, first: u64) -> io::Result<()> {
    write_u64_to_file(data_dir.as_ref().join("first"), first)
}

fn write_last_index<P: AsRef<Path>>(data_dir: P, last: u64) -> io::Result<()> {
    write_u64_to_file(data_dir.as_ref().join("last"), last)
}

fn write_applied<P: AsRef<Path>>(data_dir: P, applied: u64) -> io::Result<()> {
    write_u64_to_file(data_dir.as_ref().join("applied"), applied)
}

fn write_entry<P: AsRef<Path>>(entries_dir: P, entry: &Entry) -> io::Result<()> {
    write_pb_to_file(entries_dir.as_ref().join(format!("{}", entry.index)), entry)
}

fn remove_entry<P: AsRef<Path>>(entries_dir: P, index: u64) -> io::Result<()> {
    fs::remove_file(entries_dir.as_ref().join(format!("{}", index)))
}

fn read_pb_from_file<P: AsRef<Path>, O: ProtobufMessage>(path: P) -> io::Result<O> {
    fs::read(path).and_then(|payload| {
        protobuf::parse_from_bytes(&payload)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    })
}

fn write_pb_to_file<P: AsRef<Path>, O: ProtobufMessage>(path: P, pb: &O) -> io::Result<()> {
    pb.write_to_bytes()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
        .and_then(|payload| fs::write(path, &payload))
}

fn read_and_map_dir<T, P: AsRef<Path>>(
    path: P,
    f: fn(fs::DirEntry) -> io::Result<T>,
) -> io::Result<Vec<T>> {
    Ok(fs::read_dir(path)?
        .map(|result| result.map(f)?)
        .collect::<Result<Vec<T>, io::Error>>()?)
}

fn read_u64_from_file<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    const SIZE: usize = mem::size_of::<u64>();
    let payload = fs::read(path)?;
    if payload.len() != SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "File does not contain u64",
        ));
    }
    let mut buf: [u8; SIZE] = [0; SIZE];
    (&mut buf[..]).copy_from_slice(&payload);
    Ok(u64_from_bytes(buf))
}

fn write_u64_to_file<P: AsRef<Path>>(path: P, term: u64) -> io::Result<()> {
    fs::write(path, &u64_to_bytes(term))
}

#[inline]
fn u64_to_bytes(this: u64) -> [u8; mem::size_of::<u64>()] {
    unsafe { mem::transmute(this) }
}

#[inline]
fn u64_from_bytes(bytes: [u8; mem::size_of::<u64>()]) -> u64 {
    unsafe { mem::transmute(bytes) }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::Builder;
    use tempfile::TempDir;

    use super::super::tests;

    fn create_temp_storage(name: &str) -> (TempDir, FsStorage) {
        let tmp = Builder::new().prefix(name).tempdir().unwrap();
        let storage = FsStorage::with_data_dir(tmp.path().into()).unwrap();
        (tmp, storage)
    }

    #[test]
    fn test_storage_initial_state() {
        let (_tmp, storage) = create_temp_storage("test_storage_initial_state");
        tests::test_storage_initial_state(storage);
    }

    #[test]
    fn test_storage_entries() {
        let (_tmp, storage) = create_temp_storage("test_storage_entries");
        tests::test_storage_entries(storage);
    }

    #[test]
    fn test_storage_term() {
        let (_tmp, storage) = create_temp_storage("test_storage_term");
        tests::test_storage_term(storage);
    }

    #[test]
    fn test_first_and_last_index() {
        let (_tmp, storage) = create_temp_storage("test_first_and_last_index");
        tests::test_first_and_last_index(storage);
    }

    #[test]
    fn test_storage_ext_compact() {
        let (_tmp, storage) = create_temp_storage("test_storage_ext_compact");
        tests::test_storage_ext_compact(storage);
    }

    #[test]
    fn test_last_committed_index() {
        let (_tmp, storage) = create_temp_storage("test_last_committed_index");
        tests::test_last_committed_index(storage);
    }

    #[test]
    fn test_parity() {
        let (_tmp, storage) = create_temp_storage("test_parity");
        tests::test_parity(storage);
    }
}
