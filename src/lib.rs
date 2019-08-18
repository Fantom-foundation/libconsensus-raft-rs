#[macro_use]
extern crate log;
extern crate hex;
extern crate log4rs;
extern crate log4rs_syslog;
extern crate protobuf;
extern crate raft;
extern crate serde_json;
extern crate uluru;
extern crate libconsensus;
extern crate libtransport;
extern crate libcommon_rs;

use std::{fmt, sync::mpsc::Receiver};

use libconsensus::PeerId;

use libtransport::Transport;

use raft::{Config, eraftpb::Message};

pub type BlockId = Vec<u8>;

#[derive(Clone, Default, PartialEq, Hash)]
pub struct Block {
    pub block_id: BlockId,
    pub previous_id: BlockId,
    pub signer_id: PeerId,
    pub block_num: u64,
    pub payload: Vec<u8>,
    pub summary: Vec<u8>,
}

impl Eq for Block {}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Block(block_num: {:?}, block_id: {:?}, previous_id: {:?}, signer_id: {:?}, payload: {}, summary: {})",
            self.block_num,
            self.block_id,
            self.previous_id,
            self.signer_id,
            hex::encode(&self.payload),
            hex::encode(&self.summary),
        )
    }
}

#[derive(Default, Debug, PartialEq, Hash)]
pub struct PeerInfo {
    pub peer_id: PeerId,
}

impl Eq for PeerInfo {}

#[derive(Default, Debug, Clone)]
pub struct PeerMessage {
    pub header: PeerMessageHeader,
    pub header_bytes: Vec<u8>,
    pub header_signature: Vec<u8>,
    pub content: Vec<u8>,
}

#[derive(Default, Debug, Clone)]
pub struct PeerMessageHeader {
    pub signer_id: Vec<u8>,
    pub content_sha512: Vec<u8>,
    pub message_type: String,
    pub name: String,
    pub version: String,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Update {
    PeerConnected(PeerInfo),
    PeerDisconnected(PeerId),
    PeerMessage(PeerMessage, PeerId),
    BlockNew(Block),
    BlockValid(BlockId),
    BlockInvalid(BlockId),
    BlockCommit(BlockId),
    Shutdown,
}

#[derive(Debug, Default)]
pub struct StartupState {
    pub chain_head: Block,
    pub peers: Vec<PeerInfo>,
    pub local_peer_info: PeerInfo,
}

impl Eq for Block {}

pub struct RaftConfiguration {
    pub updates: Receiver<Update>,
    pub startup_state: StartupState,
    pub raft_config: Config,
    pub raft_message: Message
}



pub mod block_queue;
pub mod config;
pub mod engine;
pub mod node;
pub mod path;
pub mod storage;
pub mod ticker;