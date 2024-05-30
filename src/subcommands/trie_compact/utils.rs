use std::{fs, path::Path, sync::Arc};

use casper_storage::{
    data_access_layer::{BlockStore, DataAccessLayer},
    global_state::{
        state::lmdb::LmdbGlobalState, transaction_source::lmdb::LmdbEnvironment,
        trie_store::lmdb::LmdbTrieStore,
    },
};
use log::info;

use casper_types::Digest;
use lmdb::DatabaseFlags;

use crate::common::db::TRIE_STORE_FILE_NAME;

/// LMDB max readers
///
/// The default value is chosen to be the same as the node itself.
const DEFAULT_MAX_READERS: u32 = 512;
const DEFAULT_MAX_QUERY_DEPTH: u64 = 5;

/// Loads an existing data access layer.
pub fn load_data_access_layer(
    storage_path: impl AsRef<Path>,
    default_max_db_size: usize,
    state_root_hash: Digest,
    manual_sync_enabled: bool,
) -> Result<DataAccessLayer<LmdbGlobalState>, anyhow::Error> {
    let lmdb_data_file = storage_path.as_ref().join(TRIE_STORE_FILE_NAME);
    if !storage_path.as_ref().join(TRIE_STORE_FILE_NAME).exists() {
        return Err(anyhow::anyhow!(
            "lmdb data file not found at: {}",
            lmdb_data_file.display()
        ));
    }
    let lmdb_environment =
        create_lmdb_environment(&storage_path, default_max_db_size, manual_sync_enabled)?;
    let lmdb_trie_store = Arc::new(LmdbTrieStore::open(&lmdb_environment, None)?);
    let global_state = LmdbGlobalState::new(
        Arc::clone(&lmdb_environment),
        lmdb_trie_store,
        state_root_hash,
        DEFAULT_MAX_QUERY_DEPTH,
    );
    let block_store = BlockStore::new();

    Ok(DataAccessLayer {
        state: global_state,
        block_store,
        max_query_depth: DEFAULT_MAX_QUERY_DEPTH,
    })
}

/// Create an lmdb environment at a given path.
fn create_lmdb_environment(
    lmdb_path: impl AsRef<Path>,
    default_max_db_size: usize,
    manual_sync_enabled: bool,
) -> Result<Arc<LmdbEnvironment>, anyhow::Error> {
    let lmdb_environment = Arc::new(LmdbEnvironment::new(
        &lmdb_path,
        default_max_db_size,
        DEFAULT_MAX_READERS,
        manual_sync_enabled,
    )?);
    Ok(lmdb_environment)
}

/// Creates a new data access layer.
pub fn create_data_access_layer(
    storage_path: impl AsRef<Path>,
    default_max_db_size: usize,
    manual_sync_enabled: bool,
) -> Result<DataAccessLayer<LmdbGlobalState>, anyhow::Error> {
    if !storage_path.as_ref().exists() {
        info!(
            "creating new lmdb data dir {}",
            storage_path.as_ref().display()
        );
        fs::create_dir_all(&storage_path)?;
    }
    fs::create_dir_all(&storage_path)?;
    let lmdb_environment =
        create_lmdb_environment(&storage_path, default_max_db_size, manual_sync_enabled)?;
    lmdb_environment.env().sync(true)?;

    let lmdb_trie_store = Arc::new(LmdbTrieStore::new(
        &lmdb_environment,
        None,
        DatabaseFlags::empty(),
    )?);
    let global_state = LmdbGlobalState::empty(
        Arc::clone(&lmdb_environment),
        lmdb_trie_store,
        DEFAULT_MAX_QUERY_DEPTH,
    )?;

    let block_store: BlockStore = BlockStore::new();

    Ok(DataAccessLayer {
        state: global_state,
        block_store,
        max_query_depth: DEFAULT_MAX_QUERY_DEPTH,
    })
}
