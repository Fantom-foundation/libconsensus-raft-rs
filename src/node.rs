use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
    time::{Duration, Instant},
};

use protobuf::{self, Message as ProtobufMessage};

use raft::{
    self,
    eraftpb::{ConfChange, ConfChangeType, EntryType, Message as RaftMessage},
    raw_node::RawNode,
};

use super::{
    block_queue::BlockQueue,
    config::{get_peers_from_settings, peer_id_to_raft_id},
    storage::StorageExt,
};

use libconsensus::{errors, PeerId};
use libtransport::Transport;

use super::{Block, BlockId};

enum LeaderState {
    Building(Instant),
    Publishing(BlockId),
    Validating(BlockId),
    Proposing(BlockId),
    Committing(BlockId),
    ChangingConfig,
}

enum FollowerState {
    Idle,
    Committing(BlockId),
}

pub enum ReadyStatus {
    Continue,
    Shutdown,
}

pub struct FantomRaftNode<S: StorageExt> {
    peer_id: PeerId,
    raw_node: RawNode<S>,
    transport: Box<dyn Transport<RaftMessage, PeerId>>,
    leader_state: Option<LeaderState>,
    follower_state: Option<FollowerState>,
    block_queue: BlockQueue,
    raft_id_to_peer_id: HashMap<u64, PeerId>,
    period: Duration,
}

impl<S: StorageExt> FantomRaftNode<S> {
    pub fn new(
        peer_id: PeerId,
        raw_node: RawNode<S>,
        transport: Box<dyn Transport>,
        peers: Vec<PeerId>,
        period: Duration,
    ) -> Self {
        FantomRaftNode {
            peer_id,
            raw_node,
            transport,
            leader_state: None,
            follower_state: Some(FollowerState::Idle),
            block_queue: BlockQueue::new(),
            raft_id_to_peer_id: peers
                .into_iter()
                .map(|peer_id| (peer_id_to_raft_id(&peer_id), peer_id))
                .collect(),
            period,
        }
    }

    pub fn on_block_new(&mut self, block: Block) {
        if match self.leader_state {
            Some(LeaderState::Publishing(ref block_id)) => block_id == &block.block_id,
            _ => false,
        } {
            debug!(
                "Leader({:?}) transition to Validating block {:?}",
                self.peer_id, block.block_id
            );
            self.leader_state = Some(LeaderState::Validating(block.block_id.clone()));
        }

        debug!("Block has been received: {:?}", &block);
        self.block_queue.block_new(block.clone());

        self.raft_transport
            .check_blocks(vec![block.block_id])
            .expect("Failed to send check blocks");
    }

    pub fn on_block_valid(&mut self, block_id: BlockId) {
        debug!("Block has been validated: {:?}", &block_id);
        self.block_queue.block_valid(&block_id);
        if match self.leader_state {
            Some(LeaderState::Publishing(ref expected)) => expected == &block_id,
            Some(LeaderState::Validating(ref expected)) => expected == &block_id,
            _ => false,
        } {
            debug!(
                "Leader({:?}) transition to Proposing block {:?}",
                self.peer_id, block_id
            );
            info!("Leader({:?}) proposed block {:?}", self.peer_id, block_id);
            self.raw_node
                .propose(vec![], block_id.clone())
                .expect("Failed to propose block to Raft");
            self.leader_state = Some(LeaderState::Proposing(block_id));
        }
    }

    #[allow(clippy::ptr_arg)]
    pub fn on_block_commit(&mut self, block_id: &BlockId) {
        if match self.leader_state {
            Some(LeaderState::Committing(ref committing)) => committing == block_id,
            _ => false,
        } {
            let entry = self.block_queue.block_committed();
            self.raw_node
                .raft
                .mut_store()
                .set_applied(entry)
                .expect("Failed to set last applied entry.");

            if let Some(change) = self.check_for_conf_change(block_id.clone()) {
                debug!("Leader({:?}) transition to ChangingConfig", self.peer_id);
                self.leader_state = Some(LeaderState::ChangingConfig);
                self.raw_node.propose_conf_change(vec![], change).unwrap();
            } else {
                debug!(
                    "Leader({:?}) transition to Building block {:?}",
                    self.peer_id, block_id
                );
                self.leader_state = Some(LeaderState::Building(Instant::now()));
                self.raft_transport
                    .initialize_block(None)
                    .expect("Failed to initialize block");
            }
        }

        if match self.follower_state {
            Some(FollowerState::Committing(ref committing)) => committing == block_id,
            _ => false,
        } {
            info!("Peer({:?}) committed block {:?}", self.peer_id, block_id);
            let entry = self.block_queue.block_committed();
            self.raw_node
                .raft
                .mut_store()
                .set_applied(entry)
                .expect("Failed to set last applied entry.");
            self.follower_state = Some(FollowerState::Idle);
        }
    }

    pub fn on_peer_message(&mut self, message: &[u8]) {
        let raft_message = protobuf::parse_from_bytes::<RaftMessage>(message)
            .expect("Failed to interpret bytes as Raft message");
        self.raw_node.step(raft_message).unwrap_or_else(|err| {
            warn!(
                "Peer({:?}) encountered error when stepping: {:?}",
                self.peer_id, err
            );
        });
    }

    pub fn tick(&mut self) {
        self.raw_node.tick();
        self.check_publish();
    }

    fn check_publish(&mut self) {
        if match self.leader_state {
            Some(LeaderState::Building(instant)) => instant.elapsed() >= self.period,
            _ => false,
        } {
            match self.raft_transport.summarize_block() {
                Ok(_) => {}
                Err(errors::Error::BlockNotReady) => {
                    debug!(
                        "Leader({:?}) tried to summarize block but block not ready",
                        self.peer_id
                    );
                    return;
                }
                Err(err) => panic!("Failed to summarize block: {:?}", err),
            };

            match self.raft_transport.finalize_block(vec![]) {
                Ok(block_id) => {
                    debug!(
                        "Leader({:?}) transition to Publishing block {:?}",
                        self.peer_id, block_id
                    );
                    self.leader_state = Some(LeaderState::Publishing(block_id));
                }
                Err(errors::Error::BlockNotReady) => {
                    debug!(
                        "Leader({:?}) tried to finalize block but block not ready",
                        self.peer_id
                    );
                }
                Err(err) => panic!("Failed to finalize block: {:?}", err),
            };
        }
    }

    fn send_msg(&mut self, raft_msg: &RaftMessage) {
        let msg = raft_msg
            .write_to_bytes()
            .expect("Could not serialize RaftMessage");
        if let Some(peer_id) = self.raft_id_to_peer_id.get(&raft_msg.to) {
            match self.transport.send(peer_id, "", msg.to_vec()) {
                Ok(_) => (),
                Err(errors::Error::UnknownPeer(s)) => {
                    warn!("Tried to send to disconnected peer: {}", s)
                }
                Err(err) => panic!("Failed to send to peer: {:?}", err),
            }
        } else {
            warn!("Tried to send to unknown peer: {}", raft_msg.to);
        }
    }

    pub fn process_ready(&mut self) -> ReadyStatus {
        if !self.raw_node.has_ready() {
            return ReadyStatus::Continue;
        }

        let mut ready = self.raw_node.ready();

        let is_leader = self.raw_node.raft.state == raft::StateRole::Leader;
        if is_leader {
            if self.leader_state.is_none() {
                if let Some(FollowerState::Committing(ref block_id)) = self.follower_state {
                    debug!(
                        "Follower({:?}) became leader while it was in committing state",
                        self.peer_id
                    );
                    self.leader_state = Some(LeaderState::Committing(block_id.clone()));
                } else {
                    debug!(
                        "Leader({:?}) became leader, intializing block",
                        self.peer_id
                    );
                    self.leader_state = Some(LeaderState::Building(Instant::now()));
                    self.raft_transport
                        .initialize_block(None)
                        .expect("Failed to initialize block");
                }
                self.follower_state = None;
            }
            for msg in ready.messages.drain(..) {
                debug!(
                    "Leader({:?}) wants to send message: {:?}",
                    self.peer_id, msg
                );
                self.send_msg(&msg);
            }
        }

        if !raft::is_empty_snap(&ready.snapshot()) {
            self.raw_node
                .mut_store()
                .apply_snapshot(&ready.snapshot())
                .unwrap();
        }

        if !ready.entries().is_empty() {
            self.raw_node
                .mut_store()
                .append(&ready.entries())
                .expect("Failed to append entries");
        }

        if let Some(ref hs) = ready.hs() {
            self.raw_node.mut_store().set_hard_state(hs);
        }

        if !is_leader {
            if self.leader_state.is_some() {
                match self.leader_state {
                    Some(LeaderState::Building(_)) => {
                        debug!("Leader({:?}) stepped down, cancelling block", self.peer_id);
                        self.raft_transport.cancel_block().expect("Failed to cancel block");
                    }
                    Some(LeaderState::Committing(ref block_id)) => {
                        self.follower_state = Some(FollowerState::Committing(block_id.clone()));
                    }
                    _ => (),
                }
                self.leader_state = None;
                if self.follower_state.is_none() {
                    self.follower_state = Some(FollowerState::Idle);
                }
            }
            let msgs = ready.messages.drain(..);
            for msg in msgs {
                debug!("Peer({:?}) wants to send message: {:?}", self.peer_id, msg);
                self.send_msg(&msg);
            }
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in committed_entries {
                if entry.get_data().is_empty() {
                    continue;
                }

                if entry.get_entry_type() == EntryType::EntryNormal {
                    let block_id: BlockId = BlockId::from(Vec::from(entry.get_data()));
                    self.block_queue
                        .add_block_commit(block_id, entry.get_index());
                } else if entry.get_entry_type() == EntryType::EntryConfChange
                    && entry.get_term() != 1
                {
                    let change: ConfChange = protobuf::parse_from_bytes(entry.get_data())
                        .expect("Failed to parse ConfChange");

                    if let ReadyStatus::Shutdown = self.apply_conf_change(&change) {
                        return ReadyStatus::Shutdown;
                    }

                    if let Some(LeaderState::ChangingConfig) = self.leader_state {
                        debug!("Leader({:?}) transition to Building block", self.peer_id);
                        self.leader_state = Some(LeaderState::Building(Instant::now()));
                        self.raft_transport
                            .initialize_block(None)
                            .expect("Failed to initialize block");
                    }
                }
            }
        }

        if let Some(block_id) = self.block_queue.get_next_committable() {
            self.commit_block(&block_id);
        };

        self.raw_node.advance(ready);
        ReadyStatus::Continue
    }

    #[allow(clippy::ptr_arg)]
    fn commit_block(&mut self, block_id: &BlockId) {
        if match self.leader_state {
            Some(LeaderState::Proposing(ref proposed)) => block_id == proposed,
            _ => false,
        } {
            debug!(
                "Leader({:?}) transitioning to Committing block {:?}",
                self.peer_id, block_id
            );
            self.leader_state = Some(LeaderState::Committing(block_id.clone()));
            self.raft_transport
                .commit_block(block_id.clone())
                .expect("Failed to commit block");
        }

        if let Some(FollowerState::Idle) = self.follower_state {
            debug!("Peer({:?}) committing block {:?}", self.peer_id, block_id);
            self.follower_state = Some(FollowerState::Committing(block_id.clone()));
            self.raft_transport
                .commit_block(block_id.clone())
                .expect("Failed to commit block");
        }
    }

    fn check_for_conf_change(&mut self, block_id: BlockId) -> Option<ConfChange> {
        let settings = self
            .raft_transport
            .get_settings(block_id, vec![String::from("fantom.consensus.raft.peers")])
            .expect("Failed to get settings");
        let peers = get_peers_from_settings(&settings);
        let peers: HashSet<PeerId> = HashSet::from_iter(peers);

        let old_peers: HashSet<PeerId> =
            HashSet::from_iter(self.raft_id_to_peer_id.values().cloned());
        let difference: HashSet<PeerId> = peers.symmetric_difference(&old_peers).cloned().collect();

        if !difference.is_empty() {
            if difference.len() > 1 {
                panic!("More than one node's membership has changed; only one change can be processed at a time.");
            }

            let peer_id = difference
                .iter()
                .nth(0)
                .expect("Difference cannot be empty here.");
            let mut change = ConfChange::new();
            change.set_node_id(peer_id_to_raft_id(&peer_id));

            if peers.len() > old_peers.len() {
                change.set_change_type(ConfChangeType::AddNode);
                change.set_context(Vec::from(peer_id.clone()));
            } else if peers.len() < old_peers.len() {
                change.set_change_type(ConfChangeType::RemoveNode);
            }

            info!(
                "Leader({:?}) detected configuration change {:?}",
                self.peer_id, change
            );
            return Some(change);
        }

        None
    }

    fn apply_conf_change(&mut self, change: &ConfChange) -> ReadyStatus {
        info!("Configuration change received: {:?}", change);
        let raft_id = change.get_node_id();

        match change.get_change_type() {
            ConfChangeType::RemoveNode => {
                if raft_id == peer_id_to_raft_id(&self.peer_id) {
                    return ReadyStatus::Shutdown;
                }

                self.raft_id_to_peer_id.remove(&raft_id).unwrap();
            }
            ConfChangeType::AddNode => {
                self.raft_id_to_peer_id
                    .insert(raft_id, PeerId::from(Vec::from(change.get_context())));
            }
            _ => {}
        }

        self.raw_node.apply_conf_change(&change).unwrap();

        ReadyStatus::Continue
    }
}
