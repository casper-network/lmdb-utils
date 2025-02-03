mod block_info;
mod read_db;
#[cfg(test)]
mod tests;

use std::{array::TryFromSliceError, io::Error as IoError, path::Path};

use bincode::Error as BincodeError;
use clap::{Arg, ArgMatches, Command};
use lmdb::Error as LmdbError;
use serde_json::Error as SerializationError;
use thiserror::Error as ThisError;

pub const COMMAND_NAME: &str = "latest-block-summary";
const DB_PATH: &str = "db-path";
const OVERWRITE: &str = "overwrite";
const OUTPUT: &str = "output";
const AT_ERA_END: &str = "at-era-end";

/// Errors encountered when operating on the storage database.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error("No blocks found in the block header database")]
    EmptyDatabase,
    /// Parsing error on entry at index in the database.
    #[error("Error parsing element {0}: {1}")]
    Parsing(usize, BincodeError),
    /// Database operation error.
    #[error("Error operating the database: {0}")]
    Database(#[from] LmdbError),
    #[error("Error serializing output: {0}")]
    Serialize(#[from] SerializationError),
    #[error("Error writing output: {0}")]
    Output(#[from] IoError),
    #[error("Invalid block hash {err:?} {val}")]
    InvalidBlockHash { err: TryFromSliceError, val: String },
}

enum DisplayOrder {
    DbPath,
    Output,
    Overwrite,
    AtEraEnd,
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about(
            "Outputs information about the latest block in a storage database \
            in JSON format.",
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
                    "Path to where the program will output the metadata. \
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
        .arg(
            Arg::new(AT_ERA_END)
                .display_order(DisplayOrder::AtEraEnd as usize)
                .required(false)
                .short('e')
                .long(AT_ERA_END)
                .takes_value(false)
                .help("Find latest era end (switch) block."),
        )
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let path = Path::new(matches.value_of(DB_PATH).expect("should have db-path arg"));
    let output = matches.value_of(OUTPUT).map(Path::new);
    let overwrite = matches.is_present(OVERWRITE);
    let must_be_era_end = matches.is_present(AT_ERA_END);
    read_db::latest_block_summary(path, output, overwrite, must_be_era_end)
}
