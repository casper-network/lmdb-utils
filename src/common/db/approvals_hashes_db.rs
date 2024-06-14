use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

#[derive(Deserialize)]
struct LegacyApprovalsHashes {
    _block_hash: BlockHash,
    _approvals_hashes: Vec<ApprovalsHash>,
    _merkle_proof_approvals: TrieMerkleProof<Key, StoredValue>,
}

use casper_types::{global_state::TrieMerkleProof, ApprovalsHash, BlockHash, Key, StoredValue};
use serde::Deserialize;

use super::{Database, DeserializationError};

pub struct ApprovalsHashesDatabase;

impl Display for ApprovalsHashesDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "approvals_hashes")
    }
}

impl Database for ApprovalsHashesDatabase {
    fn db_name() -> &'static str {
        "approvals_hashes"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: LegacyApprovalsHashes = bincode::deserialize(bytes)?;
        Ok(())
    }
}
