#![cfg(test)]

use std::collections::BTreeMap;

use casper_storage::block_store::lmdb::LmdbBlockStore;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use casper_types::{
    execution::ExecutionResult, EraId, ProtocolVersion, PublicKey, SecretKey, Timestamp, U256, U512,
};
use casper_types::{execution::ExecutionResultV2, testing::TestRng, BlockHash, Digest};

pub(crate) static KEYS: Lazy<Vec<PublicKey>> = Lazy::new(|| {
    (0..10)
        .map(|i| {
            let u256 = U256::from(i);
            let mut u256_bytes = [0u8; 32];
            u256.to_big_endian(&mut u256_bytes);
            let secret_key =
                SecretKey::ed25519_from_bytes(u256_bytes).expect("should create secret key");
            PublicKey::from(&secret_key)
        })
        .collect()
});

pub struct LmdbTestFixture {
    pub tmp_dir: TempDir,
    pub block_store: LmdbBlockStore,
}

impl LmdbTestFixture {
    pub fn new() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();

        Self::from_temp_dir(tmp_dir)
    }

    pub fn destructure(self) -> (LmdbBlockStore, TempDir) {
        (self.block_store, self.tmp_dir)
    }

    pub fn from_temp_dir(tmp_dir: TempDir) -> Self {
        let block_store =
            LmdbBlockStore::new(tmp_dir.path(), 4096 * 1024).expect("can't create the block store");

        LmdbTestFixture {
            block_store,
            tmp_dir,
        }
    }
}

// This struct was created in order to generate `BlockHeaders` and then
// insert them into a mock database. Once `Block::random` becomes part
// of the public API of `casper-types`, this will no longer be needed.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct MockBlockHeader {
    pub parent_hash: BlockHash,
    pub state_root_hash: Digest,
    pub body_hash: Digest,
    pub random_bit: bool,
    pub accumulated_seed: Digest,
    pub era_end: Option<()>,
    pub timestamp: Timestamp,
    pub era_id: EraId,
    pub height: u64,
    pub protocol_version: ProtocolVersion,
}

impl Default for MockBlockHeader {
    fn default() -> Self {
        Self {
            parent_hash: Default::default(),
            state_root_hash: Default::default(),
            body_hash: Default::default(),
            random_bit: Default::default(),
            accumulated_seed: Default::default(),
            era_end: Default::default(),
            timestamp: Timestamp::now(),
            era_id: Default::default(),
            height: Default::default(),
            protocol_version: Default::default(),
        }
    }
}

pub(crate) fn success_execution_result(rng: &mut TestRng) -> ExecutionResult {
    let mut exec_result = ExecutionResultV2::random(rng);
    exec_result.error_message = None;

    exec_result.into()
}

#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct EraReport {
    equivocators: Vec<PublicKey>,
    rewards: BTreeMap<PublicKey, u64>,
    inactive_validators: Vec<PublicKey>,
}

#[derive(Clone, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct EraEnd {
    era_report: EraReport,
    pub next_era_validator_weights: BTreeMap<PublicKey, U512>,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct MockSwitchBlockHeader {
    pub parent_hash: BlockHash,
    pub state_root_hash: Digest,
    pub body_hash: Digest,
    pub random_bit: bool,
    pub accumulated_seed: Digest,
    pub era_end: Option<EraEnd>,
    pub timestamp: Timestamp,
    pub era_id: EraId,
    pub height: u64,
    pub protocol_version: ProtocolVersion,
}

impl Default for MockSwitchBlockHeader {
    fn default() -> Self {
        Self {
            parent_hash: Default::default(),
            state_root_hash: Default::default(),
            body_hash: Default::default(),
            random_bit: Default::default(),
            accumulated_seed: Default::default(),
            era_end: Some(Default::default()),
            timestamp: Timestamp::now(),
            era_id: Default::default(),
            height: Default::default(),
            protocol_version: Default::default(),
        }
    }
}
