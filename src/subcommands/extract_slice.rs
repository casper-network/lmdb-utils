mod extract;
mod global_state;
mod storage;
#[cfg(test)]
mod tests;

use std::{io::Error as IoError, path::Path};

use bincode::Error as BincodeError;
use casper_storage::block_store::BlockStoreError;
use casper_storage::global_state::error::Error as GlobalStateError;
use casper_types::BlockHash;
use casper_types::Digest;
use casper_types::TransactionHash;
use clap::{Arg, ArgMatches, Command};
use thiserror::Error as ThisError;

use self::extract::SliceIdentifier;

pub const COMMAND_NAME: &str = "extract-slice";
const BLOCK_HASH: &str = "block-hash";
const STATE_ROOT_HASH: &str = "state-root-hash";
const OUTPUT: &str = "output";
const SOURCE_DB_PATH: &str = "source-db-path";

/// Errors encountered when running the `extract-slice` subcommand.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Error (de)serializing items with bincode: {0}")]
    Bincode(#[from] BincodeError),
    #[error("Error creating the destination execution engine: {0}")]
    CreateExecutionEngine(anyhow::Error),
    #[error("Error operating the database: {0}")]
    GlobalState(#[from] GlobalStateError),
    #[error("Error loading the source execution engine: {0}")]
    LoadExecutionEngine(anyhow::Error),
    #[error("Error writing output: {0}")]
    Output(#[from] IoError),
    #[error("Error transferring state root: {0}")]
    StateRootTransfer(anyhow::Error),
    #[error("Encountered an error with the block store: {0}")]
    BlockStore(#[from] BlockStoreError),
    #[error("Missing block with hash: {0}")]
    MissingBlock(BlockHash),
    #[error("Missing transaction with hash: {0}")]
    MissingTransaction(TransactionHash),
    #[error("Missing execution result for transaction with hash: {0}")]
    MissingExecutionResult(TransactionHash),
}

enum DisplayOrder {
    SourceDbPath,
    Output,
    BlockHash,
    StateRootHash,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Reads all data for a given block hash (block, deploys, execution \
                results, global state) from a storage directory and stores \
                them to a new directory in two LMDB files. If a state root \
                hash is provided instead of a block hash, only the global \
                state under that root hash will be stored in the new \
                directory",
        )
        .arg(
            Arg::new(SOURCE_DB_PATH)
                .display_order(DisplayOrder::SourceDbPath as usize)
                .required(true)
                .short('d')
                .long(SOURCE_DB_PATH)
                .takes_value(true)
                .value_name("SOURCE_DB_PATH")
                .help(
                    "Path of the directory with the `storage.lmdb` and \
                `data.lmdb` files.",
                ),
        )
        .arg(
            Arg::new(OUTPUT)
                .display_order(DisplayOrder::Output as usize)
                .short('o')
                .long(OUTPUT)
                .takes_value(true)
                .value_name("OUTPUT_DB_PATH")
                .help(
                    "Path of the directory where the program will output the \
                    two newly created `storage.lmdb` and `data.lmdb` files. \
                    The directory must not exist when running this command.",
                ),
        )
        .arg(
            Arg::new(BLOCK_HASH)
                .display_order(DisplayOrder::BlockHash as usize)
                .short('b')
                .long(BLOCK_HASH)
                .takes_value(true)
                .value_name("BLOCK_HASH")
                .conflicts_with(STATE_ROOT_HASH)
                .help("Hash of the block which defines the slice."),
        )
        .arg(
            Arg::new(STATE_ROOT_HASH)
                .display_order(DisplayOrder::StateRootHash as usize)
                .short('s')
                .long(STATE_ROOT_HASH)
                .takes_value(true)
                .value_name("STATE_ROOT_HASH")
                .help("State root hash to be copied over to the new database."),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let path = Path::new(
        matches
            .value_of(SOURCE_DB_PATH)
            .expect("should have db-path arg"),
    );
    let output = Path::new(matches.value_of(OUTPUT).expect("should have output arg"));
    let slice_identifier = matches
        .value_of(BLOCK_HASH)
        .map(|block_hash_str| {
            let block_hash: BlockHash = Digest::from_hex(block_hash_str)
                .expect("should parse block hash to hex format")
                .into();
            SliceIdentifier::BlockHash(block_hash)
        })
        .unwrap_or_else(|| {
            matches
                .value_of(STATE_ROOT_HASH)
                .map(|state_root_hash_str| {
                    let state_root_hash = Digest::from_hex(state_root_hash_str)
                        .expect("should parse state root hash to hex format");
                    SliceIdentifier::StateRootHash(state_root_hash)
                })
                .expect("should have either BLOCK_HASH or STATE_ROOT_HASH arg")
        });

    extract::extract_slice(path, output, slice_identifier)
}
