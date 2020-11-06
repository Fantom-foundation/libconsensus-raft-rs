use std::{collections::HashMap, fmt, string::ToString, time::Duration};

// use serde::{
//     ser::{Serialize, Serializer, SerializeStruct},
//     de::{self, Deserialize, Deserializer, Visitor, SeqAccess, MapAccess},
// };

use hex;
use raft::{
    self,
    Config as RaftConfig,
};
use serde_json;

use super::{
    RaftConfiguration,
    path::get_path_config,
    storage::{cached_storage::CachedStorage, fs_storage::FsStorage, StorageExt},
};

use libcommon_rs::{peer::PeerList, errors::Error};
use libtransport::{Transport};
use libconsensus::PeerId;
use super::{BlockId};

pub struct RaftEngineConfig<S: StorageExt> {
    pub peers: Vec<PeerId>,
    pub period: Duration,
    pub raft: RaftConfig,
    pub storage: S,
}

impl<S: StorageExt> RaftEngineConfig<S> {
    fn new(storage: S) -> Self {
        let mut raft = RaftConfig::default();
        raft.max_size_per_msg = 1024 * 1024 * 1024;
        raft.applied = storage.applied().expect("Applied should have a value");

        RaftEngineConfig {
            peers: Vec::new(),
            period: Duration::from_millis(3_000),
            raft,
            storage,
        }
    }
}

fn create_storage() -> impl StorageExt {
    CachedStorage::new(
        FsStorage::with_data_dir(get_path_config().data_dir).expect("Failed to create FsStorage"),
    )
}

impl<S: StorageExt> fmt::Debug for RaftEngineConfig<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RaftEngineConfig {{ peers: {:?}, period: {:?}, raft: {{ election_tick: {}, heartbeat_tick: {}, applied: {} }}, storage: {} }}",
            self.peers,
            self.period,
            self.raft.election_tick,
            self.raft.heartbeat_tick,
            self.raft.applied,
            S::describe().to_string(),
        )
    }
}

pub struct RaftPeerList;

impl PeerList<PeerId, Error> for RaftPeerList {
    fn new() -> Self {

    }

    fn add(&mut self, peer: Self::P) -> std::result::Result<(), Error> {

    }

    fn get_peers_from_file(&mut self, json_peer_path: String) -> std::result::Result<(), Error> {

    }

    fn iter(&self) -> Iter<'_, Self::P> {

    }

    fn iter_mut(&mut self) -> IterMut<'_, Self::P> {

    }
}

#[allow(clippy::ptr_arg, clippy::borrowed_box)]
pub fn load_raft_config(
    peer_id: &PeerId,
    block_id: BlockId,
    transport: &mut Box<dyn Transport<PeerId, RaftConfiguration, RaftPeerList, Configuration = RaftConfiguration>,
) -> RaftEngineConfig<impl StorageExt> {
    let mut config = RaftEngineConfig::new(create_storage());
    config.raft.id = peer_id_to_raft_id(peer_id);
    config.raft.tag = format!("[{}]", config.raft.id);

    let settings_keys = vec![
        "fantom.consensus.raft.peers",
        "fantom.consensus.raft.heartbeat_tick",
        "fantom.consensus.raft.election_tick",
        "fantom.consensus.raft.period",
    ];

    let settings: HashMap<String, String> = raft_transport
        .get_settings(
            block_id,
            settings_keys.into_iter().map(String::from).collect(),
        )
        .expect("Failed to get settings keys");

    if let Some(heartbeat_tick) = settings.get("fantom.consensus.raft.heartbeat_tick") {
        let parsed: Result<usize, _> = heartbeat_tick.parse();
        if let Ok(tick) = parsed {
            config.raft.heartbeat_tick = tick;
        }
    }

    if let Some(election_tick) = settings.get("fantom.consensus.raft.election_tick") {
        let parsed: Result<usize, _> = election_tick.parse();
        if let Ok(tick) = parsed {
            config.raft.election_tick = tick;
        }
    }

    if let Some(period) = settings.get("fantom.consensus.raft.period") {
        let parsed: Result<u64, _> = period.parse();
        if let Ok(period) = parsed {
            config.period = Duration::from_millis(period);
        }
    }

    let peers = get_peers_from_settings(&settings);

    let ids: Vec<u64> = peers.iter().map(peer_id_to_raft_id).collect();

    config.peers = peers;

    config
}

#[allow(clippy::ptr_arg)]
pub fn peer_id_to_raft_id(peer_id: &PeerId) -> u64 {
    let bytes: &[u8] = peer_id.as_ref();
    assert!(bytes.len() >= 8);
    let mut u: u64 = 0;
    for i in 0..8 {
        u += (u64::from(bytes[bytes.len() - 1 - i])) << (i * 8)
    }
    u
}

pub fn get_peers_from_settings(settings: &HashMap<String, String>) -> Vec<PeerId> {
    let peers_setting_value = settings
        .get("fantom.consensus.raft.peers")
        .expect("'fantom.consensus.raft.peers' must be set to use Raft");

    let peers: Vec<String> = serde_json::from_str(peers_setting_value)
        .expect("Invalid value at 'fantom.consensus.raft.peers'");

    peers
        .into_iter()
        .map(|s| PeerId::from(hex::decode(s).expect("Peer id not valid hex")))
        .collect()
}
