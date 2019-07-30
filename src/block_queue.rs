use std::collections::{HashMap, VecDeque};

use libconsensus::{Block, BlockId};

struct BlockStatus {
    block: Option<Block>,
    block_valid: bool,
}

impl BlockStatus {
    fn with_block(block: Block) -> Self {
        BlockStatus {
            block: Some(block),
            block_valid: false,
        }
    }

    fn with_valid() -> Self {
        BlockStatus {
            block: None,
            block_valid: true,
        }
    }
}

pub struct BlockQueue {
    validator_backlog: HashMap<BlockId, BlockStatus>,
    commit_queue: VecDeque<(BlockId, u64)>,
}

impl BlockQueue {
    pub fn new() -> Self {
        BlockQueue {
            validator_backlog: HashMap::new(),
            commit_queue: VecDeque::new(),
        }
    }

    pub fn block_new(&mut self, block: Block) {
        let block_id = block.block_id.clone();
        self.validator_backlog
            .entry(block_id)
            .and_modify(|status| {
                status.block = Some(block.clone());
            })
            .or_insert_with(|| BlockStatus::with_block(block));
    }

    #[allow(clippy::ptr_arg)]
    pub fn block_valid(&mut self, block_id: &BlockId) {
        self.validator_backlog
            .entry(block_id.clone())
            .and_modify(|status| {
                status.block_valid = true;
            })
            .or_insert_with(BlockStatus::with_valid);
    }

    pub fn add_block_commit(&mut self, block_id: BlockId, entry: u64) {
        self.commit_queue.push_back((block_id, entry));
    }

    pub fn block_committed(&mut self) -> u64 {
        let (block_id, entry) = self.commit_queue.pop_front().expect("No blocks in queue.");
        self.validator_backlog.remove(&block_id);
        entry
    }

    pub fn get_next_committable(&mut self) -> Option<BlockId> {
        if let Some(&(ref block_id, _)) = self.commit_queue.front() {
            if let Some(status) = self.validator_backlog.get(block_id) {
                if status.block_valid {
                    return Some(block_id.clone());
                }
            }
        }
        None
    }
}
