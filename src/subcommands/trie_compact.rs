mod compact;
mod helpers;

#[cfg(test)]
pub(crate) mod tests;

// All code in the `utils` mod was copied from `casper-node` because it isn't available in the
// public interface.
mod utils;

use std::{io::Error as IoError, path::PathBuf};

use anyhow::Error as AnyError;
use clap::{Arg, ArgMatches, Command};
use thiserror::Error as ThisError;

use casper_storage::block_store::BlockStoreError;
use casper_storage::global_state::error::Error as GlobalStateError;
use casper_types::Digest;

use compact::DestinationOptions;
pub use helpers::copy_state_root;
pub use utils::{create_data_access_layer, load_data_access_layer};

pub const COMMAND_NAME: &str = "compact-trie";
const APPEND: &str = "append";
const DESTINATION_TRIE_STORE_PATH: &str = "dest-trie";
const OVERWRITE: &str = "overwrite";
const MAX_DB_SIZE: &str = "max-db-size";
pub const DEFAULT_MAX_DB_SIZE: &str = "483183820800"; // 450 gb
const SOURCE_TRIE_STORE_PATH: &str = "src-trie";
const STORAGE_PATH: &str = "storage-path";

/// Possible errors caught while compacting the trie store.
#[derive(Debug, ThisError)]
pub enum Error {
    /// Error copying the state root with a specific digest.
    #[error("Error copying state root {0}: {1}")]
    CopyStateRoot(Digest, AnyError),
    /// Error creating the execution engine for the destination trie.
    #[error("Error loading the execution engine: {0}")]
    CreateDestTrie(AnyError),
    /// Error working with the destination trie path.
    #[error("Invalid destination: {0}")]
    InvalidDest(String),
    /// Path cannot be created/resolved.
    #[error("Path {0} cannot be created/resolved: {1}")]
    InvalidPath(PathBuf, IoError),
    /// Error while operating on the global state.
    #[error("Error while operating on the global state: {0}")]
    GlobalState(GlobalStateError),
    /// A block of specific height is missing from the storage.
    #[error("Storage database is missing block {0}")]
    MissingBlock(u64),
    /// Error creating the execution engine for the source trie.
    #[error("Error creating the execution engine: {0}")]
    OpenSourceTrie(AnyError),
    /// Error opening the block/deploys LMDB store.
    #[error("Error opening the block/deploy storage: {0}")]
    OpenStorage(BlockStoreError),
    /// Error while getting a block of specific height from storage.
    #[error("Storage error while trying to retrieve block {0}: {1}")]
    Storage(u64, BlockStoreError),
}

enum DisplayOrder {
    SourcePath,
    DestinationPath,
    StoragePath,
    Append,
    Overwrite,
    MaxDbSize,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Writes a compacted version of the block entries in the source trie store to the \
            destination.",
        )
        .arg(
            Arg::new(SOURCE_TRIE_STORE_PATH)
                .display_order(DisplayOrder::SourcePath as usize)
                .required(true)
                .short('s')
                .long(SOURCE_TRIE_STORE_PATH)
                .takes_value(true)
                .value_name("SOURCE_TRIE_STORE_DIR_PATH")
                .help("Path of the directory with the source `data.lmdb` file."),
        )
        .arg(
            Arg::new(DESTINATION_TRIE_STORE_PATH)
                .display_order(DisplayOrder::DestinationPath as usize)
                .required(true)
                .short('d')
                .long(DESTINATION_TRIE_STORE_PATH)
                .takes_value(true)
                .value_name("DESTINATION_TRIE_STORE_DIR_PATH")
                .help("Path of the directory where the output `data.lmdb` file will be created."),
        )
        .arg(
            Arg::new(STORAGE_PATH)
                .display_order(DisplayOrder::StoragePath as usize)
                .required(true)
                .short('b')
                .long(STORAGE_PATH)
                .takes_value(true)
                .value_name("STORAGE_DIR_PATH")
                .help(
                    "Path of the directory with the `storage.lmdb` file. Used to find all \
                    blocks' state root hashes.",
                ),
        )
        .arg(
            Arg::new(APPEND)
                .display_order(DisplayOrder::Append as usize)
                .required(false)
                .short('a')
                .long(APPEND)
                .takes_value(false)
                .conflicts_with(OVERWRITE)
                .help(
                    "Append output to an already existing output `data.lmdb` file in \
                    destination directory.",
                ),
        )
        .arg(
            Arg::new(OVERWRITE)
                .display_order(DisplayOrder::Overwrite as usize)
                .required(false)
                .short('w')
                .long(OVERWRITE)
                .takes_value(false)
                .conflicts_with(APPEND)
                .help(
                    "Overwrite an already existing output `data.lmdb` file in destination \
                    directory.",
                ),
        )
        .arg(
            Arg::new(MAX_DB_SIZE)
                .display_order(DisplayOrder::MaxDbSize as usize)
                .required(false)
                .short('m')
                .long(MAX_DB_SIZE)
                .takes_value(true)
                .default_value(DEFAULT_MAX_DB_SIZE)
                .value_name("MAX_DB_SIZE")
                .help("Maximum size the DB files are allowed to be, in bytes."),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let storage_path = matches.value_of(STORAGE_PATH).unwrap();
    let source_trie_path = matches.value_of(SOURCE_TRIE_STORE_PATH).unwrap();
    let destination_trie_path = matches.value_of(DESTINATION_TRIE_STORE_PATH).unwrap();
    // Prettier than C style if/else.
    let dest_opt = match matches {
        _ if matches.is_present(APPEND) => DestinationOptions::Append,
        _ if matches.is_present(OVERWRITE) => DestinationOptions::Overwrite,
        _ => DestinationOptions::New,
    };
    let max_db_size = matches
        .value_of(MAX_DB_SIZE)
        .unwrap()
        .parse()
        .expect("Value of \"--max-db-size\" must be an integer.");

    compact::trie_compact(
        storage_path,
        source_trie_path,
        destination_trie_path,
        dest_opt,
        max_db_size,
    )
}
