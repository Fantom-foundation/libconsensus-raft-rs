use std::{
    sync::mpsc::{RecvTimeoutError},
    time::Duration,
};

use raft::{
    raw_node::RawNode,
    Peer as RaftPeer,
};

use super::{
    Update,
    StartupState,
    RaftConfiguration,
    config::{self, RaftEngineConfig},
    node::{FantomRaftNode, ReadyStatus},
    storage::StorageExt,
    ticker,
};

use libconsensus::{Consensus, errors};
use libtransport::{Transport};

pub struct RaftEngine {}

impl RaftEngine {
    pub fn new() -> Self {
        RaftEngine {}
    }
}

pub const RAFT_TIMEOUT: Duration = Duration::from_millis(100);

impl Consensus for RaftEngine
{
    type Configuration = RaftConfiguration;

    fn new(configuration: RaftConfiguration) -> Result<(), errors::Error> {
        let StartupState {
            chain_head,
            local_peer_info,
            ..
        } = configuration.startup_state;

        let cfg = config::load_raft_config(&local_peer_info.peer_id, chain_head.block_id, &mut configuration.transport);

        info!("Raft ConsensusEngine Config Loaded: {:?}", cfg);

        let RaftEngineConfig {
            peers,
            period,
            raft: raft_config,
            storage: raft_storage,
        } = cfg;

        let raft_peers: Vec<RaftPeer> = raft_config
            .peers
            .iter()
            .map(|id| RaftPeer {
                id: *id,
                context: None,
            })
            .collect();
        let raw_node = RawNode::new(&raft_config, raft_storage, raft_peers)
            .expect("Failed to create new RawNode");

        let mut node = FantomRaftNode::new(local_peer_info.peer_id, raw_node, configuration.transport, peers, period);

        let mut raft_ticker = ticker::Ticker::new(RAFT_TIMEOUT);
        let mut timeout = RAFT_TIMEOUT;

        loop {
            match configuration.updates.recv_timeout(timeout) {
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => break,
                Ok(update) => {
                    debug!("Update: {:?}", update);
                    if !handle_update(&mut node, update) {
                        break;
                    }
                }
            }

            timeout = raft_ticker.tick(|| {
                node.tick();
            });

            if let ReadyStatus::Shutdown = node.process_ready() {
                break;
            }
        }

        Ok(())
    }
}

fn handle_update<S: StorageExt>(node: &mut FantomRaftNode<S>, update: Update) -> bool {
    match update {
        Update::BlockNew(block) => node.on_block_new(block),
        Update::BlockValid(block_id) => node.on_block_valid(block_id),
        Update::BlockCommit(block_id) => node.on_block_commit(&block_id),
        Update::PeerMessage(message, _id) => node.on_peer_message(&message.content),
        Update::Shutdown => return false,

        update => warn!("Unhandled update: {:?}", update),
    }
    true
}
