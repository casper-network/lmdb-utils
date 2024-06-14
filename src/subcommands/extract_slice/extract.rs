use std::path::Path;

use casper_types::BlockHash;
use casper_types::Digest;

use super::{global_state, storage, Error};

pub enum SliceIdentifier {
    BlockHash(BlockHash),
    StateRootHash(Digest),
}

pub fn extract_slice<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_path: P1,
    output: P2,
    slice_identifier: SliceIdentifier,
) -> Result<(), Error> {
    storage::create_output_db_dir(&output)?;
    let state_root_hash = match slice_identifier {
        SliceIdentifier::BlockHash(block_hash) => {
            storage::transfer_block_info(&db_path, &output, block_hash)?
        }
        SliceIdentifier::StateRootHash(state_root_hash) => state_root_hash,
    };
    global_state::transfer_global_state(&db_path, &output, state_root_hash)?;
    Ok(())
}
