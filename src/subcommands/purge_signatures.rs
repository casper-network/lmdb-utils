mod purge;
mod signatures;
#[cfg(test)]
mod tests;

use std::{collections::BTreeSet, path::Path};

use casper_storage::block_store::BlockStoreError;
use casper_types::BlockHash;
use casper_types::EraId;
use clap::{Arg, ArgMatches, Command};
use lmdb::Error as LmdbError;
use thiserror::Error as ThisError;

pub const COMMAND_NAME: &str = "purge-signatures";
const DB_PATH: &str = "db-path";
const NO_FINALITY: &str = "no-finality";
const WEAK_FINALITY: &str = "weak-finality";

/// Errors encountered when operating on the storage database.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Block list is empty")]
    EmptyBlockList,
    #[error("No blocks found in the block header database")]
    EmptyDatabase,
    /// Database operation error.
    #[error("Error operating the database: {0}")]
    Database(#[from] LmdbError),
    /// Block store error.
    #[error("Error encountered with block store: {0}")]
    BlockStore(#[from] BlockStoreError),
    #[error("Found duplicate block header with height {0}")]
    DuplicateBlock(u64),
    #[error("Missing switch block with weights for era {0}")]
    MissingEraWeights(EraId),
    /// Missing block header from database.
    #[error("Missing block header from database at hash: {0}")]
    MissingBlockHeader(BlockHash),
}

enum DisplayOrder {
    DbPath,
    WeakFinality,
    NoFinality,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Purges the signatures for a given block list from a storage \
            database.",
        )
        .arg(
            Arg::new(DB_PATH)
                .display_order(DisplayOrder::DbPath as usize)
                .required(true)
                .short('d')
                .long(DB_PATH)
                .takes_value(true)
                .value_name("DB_PATH")
                .help("Path of the directory with the `storage.lmdb` file."),
        )
        .arg(
            Arg::new(WEAK_FINALITY)
                .display_order(DisplayOrder::WeakFinality as usize)
                .required_unless_present(NO_FINALITY)
                .short('w')
                .long(WEAK_FINALITY)
                .takes_value(true)
                .value_name("BLOCK_HEIGHT_LIST")
                .help(
                    "List of block heights separated by ',' for which \
                    signatures will be stripped until weak finality is \
                    reached.",
                ),
        )
        .arg(
            Arg::new(NO_FINALITY)
                .display_order(DisplayOrder::NoFinality as usize)
                .required_unless_present(WEAK_FINALITY)
                .short('n')
                .long(NO_FINALITY)
                .takes_value(true)
                .value_name("BLOCK_HEIGHT_LIST")
                .help(
                    "List of block heights separated by ',' for which \
                    all signatures will be stripped.",
                ),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let path = Path::new(matches.value_of(DB_PATH).expect("should have db-path arg"));
    let weak_finality_block_list: BTreeSet<u64> = matches
        .value_of(WEAK_FINALITY)
        .map(|height_list| height_list.split(','))
        .map(|height_str| {
            height_str.map(|height| {
                height
                    .parse()
                    .unwrap_or_else(|_| panic!("{height} is not a valid block height"))
            })
        })
        .map(|list| list.collect())
        .unwrap_or_default();
    let no_finality_block_list: BTreeSet<u64> = matches
        .value_of(NO_FINALITY)
        .map(|height_list| height_list.split(','))
        .map(|height_str| {
            height_str.map(|height| {
                height
                    .parse()
                    .unwrap_or_else(|_| panic!("{height} is not a valid block height"))
            })
        })
        .map(|list| list.collect())
        .unwrap_or_default();
    purge::purge_signatures(path, weak_finality_block_list, no_finality_block_list)
}
