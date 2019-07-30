use std::{
    sync::mpsc::{Receiver, RecvTimeoutError},
    time::Duration,
};

use raft::{raw_node::RawNode, Peer as RaftPeer};

use super::{
    config::{self, RaftEngineConfig},
    node::{FantomRaftNode, ReadyStatus},
    storage::StorageExt,
    ticker,
};

use libconsensus::{ConsensusEngine, Error, Service, StartupState, Update};

pub struct RaftEngine {}

impl RaftEngine {
    pub fn new() -> Self {
        RaftEngine {}
    }
}

pub const RAFT_TIMEOUT: Duration = Duration::from_millis(100);

impl ConsensusEngine for RaftEngine {
    fn start(
        &mut self,
        updates: Receiver<Update>,
        mut service: Box<dyn Service>,
        startup_state: StartupState,
    ) -> Result<(), Error> {
        let StartupState {
            chain_head,
            local_peer_info,
            ..
        } = startup_state;

        let cfg =
            config::load_raft_config(&local_peer_info.peer_id, chain_head.block_id, &mut service);
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

        let mut node =
            FantomRaftNode::new(local_peer_info.peer_id, raw_node, service, peers, period);

        let mut raft_ticker = ticker::Ticker::new(RAFT_TIMEOUT);
        let mut timeout = RAFT_TIMEOUT;

        loop {
            match updates.recv_timeout(timeout) {
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

    fn version(&self) -> String {
        "0.1".into()
    }

    fn name(&self) -> String {
        "raft".into()
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
