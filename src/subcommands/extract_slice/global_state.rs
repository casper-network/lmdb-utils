use std::{path::Path, result::Result};

use casper_storage::{data_access_layer::FlushRequest, global_state::state::StateProvider};
use casper_types::Digest;
use log::info;

use crate::subcommands::trie_compact::{
    copy_state_root, create_data_access_layer, load_data_access_layer, DEFAULT_MAX_DB_SIZE,
};

use super::Error;

/// Transfers the global state under a state root hash from a trie store to a
/// new one.
pub(crate) fn transfer_global_state<P1: AsRef<Path>, P2: AsRef<Path>>(
    source: P1,
    destination: P2,
    state_root_hash: Digest,
) -> Result<(), Error> {
    let max_db_size = DEFAULT_MAX_DB_SIZE
        .parse()
        .expect("should be able to parse max db size");

    // Load the source trie store.
    let source_state = load_data_access_layer(source, max_db_size, Digest::default(), true)
        .map_err(Error::LoadExecutionEngine)?;
    // Create the destination trie store.
    let destination_state = create_data_access_layer(destination, max_db_size, true)
        .map_err(Error::CreateExecutionEngine)?;
    info!("Starting transfer process for state root hash {state_root_hash}");
    // Copy the state root along with missing descendants over to the new trie
    // store.
    copy_state_root(state_root_hash, &source_state, &destination_state)
        .map_err(Error::StateRootTransfer)?;
    destination_state
        .flush(FlushRequest::new())
        .as_error()
        .map_err(Error::GlobalState)?;

    Ok(())
}
