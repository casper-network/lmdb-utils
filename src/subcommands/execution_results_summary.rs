mod read_db;
mod summary;
#[cfg(test)]
mod tests;

use std::{io::Error as IoError, path::Path};

use bincode::Error as BincodeError;
use casper_storage::block_store::BlockStoreError;
use clap::{Arg, ArgMatches, Command};
use serde_json::Error as JsonSerializationError;
use thiserror::Error as ThisError;

pub const COMMAND_NAME: &str = "execution-results-summary";
const DB_PATH: &str = "db-path";
const OVERWRITE: &str = "overwrite";
const OUTPUT: &str = "output";

/// Errors encountered when operating on the storage database.
#[derive(Debug, ThisError)]
pub enum Error {
    /// Database operation error.
    #[error("Error serializing output: {0}")]
    JsonSerialize(#[from] JsonSerializationError),
    #[error("Error writing output: {0}")]
    Output(#[from] IoError),
    #[error("Error serializing execution results: {0}")]
    Serialize(#[from] BincodeError),
    #[error("Block store error: {0}")]
    BlockStore(#[from] BlockStoreError),
    #[error("Empty database.")]
    EmptyDatabase,
}

enum DisplayOrder {
    DbPath,
    Output,
    Overwrite,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Outputs information about the execution results in a storage \
            database in JSON format.",
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
            Arg::new(OUTPUT)
                .display_order(DisplayOrder::Output as usize)
                .short('o')
                .long(OUTPUT)
                .takes_value(true)
                .value_name("FILE_PATH")
                .help(
                    "Path to where the program will output the summary. \
                    If unspecified, defaults to standard output.",
                ),
        )
        .arg(
            Arg::new(OVERWRITE)
                .display_order(DisplayOrder::Overwrite as usize)
                .required(false)
                .short('w')
                .long(OVERWRITE)
                .takes_value(false)
                .requires(OUTPUT)
                .help(
                    "Overwrite an already existing output file in destination \
                    directory.",
                ),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let path = Path::new(matches.value_of(DB_PATH).expect("should have db-path arg"));
    let output = matches.value_of(OUTPUT).map(Path::new);
    let overwrite = matches.is_present(OVERWRITE);
    read_db::execution_results_summary(path, output, overwrite)
}
